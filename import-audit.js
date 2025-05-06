/**
 * Import Audit Module
 * 
 * This module audits guild logs after exportguild operations are complete,
 * tracking role changes and member join/leave events.
 * - Adds role history to role_history table
 * - Creates and populates join-leave table with member join/leave events
 */

const { Client, GatewayIntentBits, AuditLogEvent, PermissionsBitField } = require('discord.js');
const monitor = require('./monitor'); // Use your existing monitor module

// Configuration
const config = {
  batchSize: 100,
  maxConcurrentRequests: 5,
};

/**
 * Process guild audit logs - can be called directly from command
 * @param {Object} message - Discord message object with guild context
 * @param {Client} client - Discord client
 * @returns {Promise<void>}
 */
async function auditGuildCommand(message, client) {
  const guild = message.guild;
  
  // Check permissions first
  if (!guild.members.me.permissions.has(PermissionsBitField.Flags.ViewAuditLog)) {
    await message.channel.send('⚠️ Bot is missing required permission: View Audit Log');
    return;
  }
  
  // Send initial status message
  const statusMessage = await message.channel.send('Starting guild audit log processing...');
  
  try {
    console.log(`[${getCurrentTime()}] Starting audit logs processing for guild ${guild.name} (${guild.id})`);
    
    // Get database connection from monitor
    const db = monitor.getDatabase();
    if (!db) {
      throw new Error('Database not initialized. Please run !exportguild first.');
    }
    
    // Update status message
    await statusMessage.edit('Creating and verifying required database tables...');
    
    // Create tables
    await createTablesIfNotExist(db);
    
    // Update status message
    await statusMessage.edit('Processing role changes from audit log...');
    
    // Define the audit log types we're interested in
    const roleUpdateType = AuditLogEvent.MemberRoleUpdate;
    const memberAddType = AuditLogEvent.MemberAdd;
    const memberRemoveType = AuditLogEvent.MemberRemove;
    
    // Process role changes
    await statusMessage.edit('Fetching role update audit logs...');
    const roleChangeLogs = await fetchLogsOfType(guild, roleUpdateType);
    console.log(`[${getCurrentTime()}] Fetched ${roleChangeLogs.length} role update logs`);
    
    // Process the role changes
    const roleChanges = await processRoleChangeLogs(roleChangeLogs, db);
    await statusMessage.edit(`Processing role changes... (${roleChanges.roleAddCount} adds, ${roleChanges.roleRemoveCount} removes)`);
    
    // Process member join events
    await statusMessage.edit('Fetching member join audit logs...');
    const memberJoinLogs = await fetchLogsOfType(guild, memberAddType);
    console.log(`[${getCurrentTime()}] Fetched ${memberJoinLogs.length} member join logs`);
    
    // Process member leave events
    await statusMessage.edit('Fetching member leave audit logs...');
    const memberLeaveLogs = await fetchLogsOfType(guild, memberRemoveType);
    console.log(`[${getCurrentTime()}] Fetched ${memberLeaveLogs.length} member leave logs`);
    
    // Process join/leave events
    const memberEvents = await processMemberEvents(memberJoinLogs, memberLeaveLogs, db);
    
    // Store metadata
    await monitor.storeGuildMetadata('audit_log_processed_at', new Date().toISOString());
    await monitor.storeGuildMetadata('role_changes_processed', (roleChanges.roleAddCount + roleChanges.roleRemoveCount).toString());
    await monitor.storeGuildMetadata('member_events_processed', (memberEvents.joinCount + memberEvents.leaveCount).toString());
    
    // Final status update
    await statusMessage.edit(`✅ Guild audit log processing complete!\n\n` +
      `**Role Changes**\n` +
      `- Added: ${roleChanges.roleAddCount}\n` +
      `- Removed: ${roleChanges.roleRemoveCount}\n\n` +
      `**Member Events**\n` +
      `- Joins: ${memberEvents.joinCount}\n` +
      `- Leaves: ${memberEvents.leaveCount}\n\n` +
      `All data has been saved to the database.`);
      
    console.log('[${getCurrentTime()}] Guild audit log processing completed successfully');
    
  } catch (error) {
    console.error('[${getCurrentTime()}] Error during guild audit processing:', error);
    await statusMessage.edit(`❌ Error during guild audit processing: ${error.message}`);
    
    // Store error metadata
    try {
      await monitor.storeGuildMetadata('audit_log_processing_error', error.message);
      await monitor.storeGuildMetadata('audit_log_processing_error_time', new Date().toISOString());
    } catch (metadataError) {
      console.error('[${getCurrentTime()}] Error storing error metadata:', metadataError);
    }
  }
}

/**
 * Process audit logs after exportguild
 * @param {string} guildId - Discord guild ID
 * @param {Client} client - Discord client
 * @returns {Promise<Object>} - Results of processing
 */
async function auditAfterExport(guildId, client) {
  try {
    console.log(`[${getCurrentTime()}] Starting audit after export for guild ${guildId}`);
    
    // Fetch the guild
    const guild = await client.guilds.fetch(guildId);
    if (!guild) {
      throw new Error(`Guild with ID ${guildId} not found`);
    }
    
    // Check permissions
    if (!guild.members.me.permissions.has(PermissionsBitField.Flags.ViewAuditLog)) {
      console.error(`[${getCurrentTime()}] Bot lacks View Audit Log permission in guild ${guild.name}`);
      return {
        success: false,
        error: "Missing View Audit Log permission"
      };
    }
    
    // Get database connection
    const db = monitor.getDatabase();
    if (!db) {
      throw new Error('Database not initialized');
    }
    
    // Create necessary tables
    await createTablesIfNotExist(db);
    
    // Define the audit log types we're interested in
    const roleUpdateType = AuditLogEvent.MemberRoleUpdate;
    const memberAddType = AuditLogEvent.MemberAdd;
    const memberRemoveType = AuditLogEvent.MemberRemove;
    
    // Fetch all relevant logs
    console.log(`[${getCurrentTime()}] Fetching audit logs for role updates`);
    const roleChangeLogs = await fetchLogsOfType(guild, roleUpdateType);
    
    console.log(`[${getCurrentTime()}] Fetching audit logs for member joins`);
    const memberJoinLogs = await fetchLogsOfType(guild, memberAddType);
    
    console.log(`[${getCurrentTime()}] Fetching audit logs for member leaves`);
    const memberLeaveLogs = await fetchLogsOfType(guild, memberRemoveType);
    
    // Process the logs
    console.log(`[${getCurrentTime()}] Processing role change logs (${roleChangeLogs.length} entries)`);
    const roleChanges = await processRoleChangeLogs(roleChangeLogs, db);
    
    console.log(`[${getCurrentTime()}] Processing member events (${memberJoinLogs.length} joins, ${memberLeaveLogs.length} leaves)`);
    const memberEvents = await processMemberEvents(memberJoinLogs, memberLeaveLogs, db);
    
    // Record metadata
    await monitor.storeGuildMetadata('audit_log_processed_at', new Date().toISOString());
    await monitor.storeGuildMetadata('role_changes_processed', (roleChanges.roleAddCount + roleChanges.roleRemoveCount).toString());
    await monitor.storeGuildMetadata('member_events_processed', (memberEvents.joinCount + memberEvents.leaveCount).toString());
    
    console.log(`[${getCurrentTime()}] Audit processing complete`);
    return {
      success: true,
      roleChanges,
      memberEvents
    };
  } catch (error) {
    console.error(`[${getCurrentTime()}] Error in auditAfterExport:`, error);
    
    // Try to record the error
    try {
      await monitor.storeGuildMetadata('audit_log_processing_error', error.message);
      await monitor.storeGuildMetadata('audit_log_processing_error_time', new Date().toISOString());
    } catch (metadataError) {
      console.error(`[${getCurrentTime()}] Failed to store error metadata:`, metadataError);
    }
    
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Create necessary tables if they don't exist
 * @param {Object} db - SQLite database connection
 * @returns {Promise<void>}
 */
async function createTablesIfNotExist(db) {
  return new Promise((resolve, reject) => {
    try {
      console.log(`[${getCurrentTime()}] Creating tables if they don't exist...`);
      
      // Check if join-leave table exists, create if not
      db.run(`
        CREATE TABLE IF NOT EXISTS "join-leave" (
          id TEXT NOT NULL,
          event TEXT CHECK (event IN ('join', 'leave')),
          timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (id, event, timestamp)
        )
      `, function(err) {
        if (err) {
          console.error(`[${getCurrentTime()}] Error creating join-leave table:`, err);
          reject(err);
          return;
        }
        
        // Ensure role_history table exists
        db.run(`
          CREATE TABLE IF NOT EXISTS role_history (
            memberId TEXT NOT NULL,
            roleId TEXT NOT NULL,
            action TEXT CHECK (action IN ('add', 'remove')),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (memberId, roleId, action, timestamp)
          )
        `, function(err) {
          if (err) {
            console.error(`[${getCurrentTime()}] Error creating role_history table:`, err);
            reject(err);
            return;
          }
          
          console.log(`[${getCurrentTime()}] Tables verified`);
          resolve();
        });
      });
    } catch (error) {
      console.error(`[${getCurrentTime()}] Error creating tables:`, error);
      reject(error);
    }
  });
}

/**
 * Fetch logs of a specific type
 * @param {Guild} guild - Discord guild
 * @param {number} type - Audit log type (from AuditLogEvent)
 * @returns {Promise<Array>} - Array of processed log entries
 */
async function fetchLogsOfType(guild, type) {
  // Find the corresponding name for logging purposes
  const typeName = Object.keys(AuditLogEvent).find(key => 
    AuditLogEvent[key] === type
  ) || type;
  
  console.log(`[${getCurrentTime()}] Fetching logs of type: ${typeName} (${type})`);
  
  const logs = [];
  let lastId = null;
  let hasMore = true;
  let attemptCount = 0;
  
  try {
    while (hasMore) {
      attemptCount++;
      const fetchOptions = { 
        limit: config.batchSize,
        type: type
      };
      
      if (lastId) {
        fetchOptions.before = lastId;
      }
      
      console.log(`[${getCurrentTime()}] Fetching batch ${attemptCount} with options: ${JSON.stringify(fetchOptions)}`);
      
      const fetchedLogs = await guild.fetchAuditLogs(fetchOptions);
      
      if (fetchedLogs.entries.size === 0) {
        console.log(`[${getCurrentTime()}] No more entries for ${typeName} (${type})`);
        hasMore = false;
      } else {
        console.log(`[${getCurrentTime()}] Processing ${fetchedLogs.entries.size} entries for ${typeName} (${type})`);
        
        for (const [id, entry] of fetchedLogs.entries) {
          logs.push({
            id: entry.id,
            type: entry.action,
            actionType: typeName,
            executor: entry.executor ? {
              id: entry.executor.id,
              tag: entry.executor.tag
            } : null,
            target: entry.target ? {
              id: entry.target.id,
              type: entry.targetType,
              tag: entry.target.tag || null
            } : null,
            reason: entry.reason || null,
            changes: entry.changes || [],
            createdTimestamp: entry.createdTimestamp,
            createdAt: entry.createdAt.toISOString(),
            rawEntry: entry // Include the raw entry for direct access
          });
        }
        
        // Get last ID for pagination
        lastId = fetchedLogs.entries.last()?.id;
        
        if (fetchedLogs.entries.size < config.batchSize) {
          console.log(`[${getCurrentTime()}] Batch size ${fetchedLogs.entries.size} < ${config.batchSize}, stopping pagination`);
          hasMore = false;
        }
      }
      
      // Safety check - don't loop forever
      if (attemptCount >= 10) {
        console.log(`[${getCurrentTime()}] Reached maximum fetch attempts (10) for ${typeName}, stopping for safety`);
        hasMore = false;
      }
    }
    
    console.log(`[${getCurrentTime()}] Fetched ${logs.length} logs of type: ${typeName} (${type})`);
    return logs;
  } catch (error) {
    console.error(`[${getCurrentTime()}] Error fetching logs of type ${typeName} (${type}):`, error);
    return []; // Return empty array on error to continue with others
  }
}

/**
 * Process role change audit logs
 * @param {Array} logs - Array of role change audit logs
 * @param {Object} db - SQLite database connection
 * @returns {Promise<Object>} - Statistics about role changes processed
 */
async function processRoleChangeLogs(logs, db) {
  return new Promise((resolve, reject) => {
    try {
      console.log(`[${getCurrentTime()}] Processing ${logs.length} role change logs`);
      
      let roleAddCount = 0;
      let roleRemoveCount = 0;
      
      // Begin transaction
      db.run('BEGIN TRANSACTION', (transErr) => {
        if (transErr) {
          console.error(`[${getCurrentTime()}] Error beginning transaction:`, transErr);
          reject(transErr);
          return;
        }
        
        // Use prepared statement for better performance
        const roleStmt = db.prepare(`
          INSERT OR IGNORE INTO role_history (memberId, roleId, action, timestamp)
          VALUES (?, ?, ?, ?)
        `);
        
        // Process each log entry
        for (const log of logs) {
          if (!log.target || !log.target.id) continue;
          
          const userId = log.target.id;
          const timestamp = log.createdAt;
          const entry = log.rawEntry;
          
          // Process added roles
          if (entry.changes && entry.changes.find(change => change.key === '$add')) {
            const addedRoles = entry.changes.find(change => change.key === '$add').new;
            
            if (Array.isArray(addedRoles)) {
              for (const role of addedRoles) {
                try {
                  roleStmt.run(userId, role.id, 'add', timestamp);
                  roleAddCount++;
                } catch (err) {
                  console.error(`[${getCurrentTime()}] Error inserting role add:`, err);
                }
              }
            }
          }
          
          // Process removed roles
          if (entry.changes && entry.changes.find(change => change.key === '$remove')) {
            const removedRoles = entry.changes.find(change => change.key === '$remove').new;
            
            if (Array.isArray(removedRoles)) {
              for (const role of removedRoles) {
                try {
                  roleStmt.run(userId, role.id, 'remove', timestamp);
                  roleRemoveCount++;
                } catch (err) {
                  console.error(`[${getCurrentTime()}] Error inserting role remove:`, err);
                }
              }
            }
          }
        }
        
        // Finalize statement
        roleStmt.finalize();
        
        // Commit the transaction
        db.run('COMMIT', (commitErr) => {
          if (commitErr) {
            console.error(`[${getCurrentTime()}] Error committing transaction:`, commitErr);
            db.run('ROLLBACK');
            reject(commitErr);
            return;
          }
          
          console.log(`[${getCurrentTime()}] Role changes processed: ${roleAddCount} adds, ${roleRemoveCount} removes`);
          resolve({ roleAddCount, roleRemoveCount });
        });
      });
    } catch (error) {
      console.error(`[${getCurrentTime()}] Error processing role changes:`, error);
      
      // Try to rollback if there was an error
      try {
        db.run('ROLLBACK');
      } catch (e) {
        console.error(`[${getCurrentTime()}] Error during rollback:`, e);
      }
      
      reject(error);
    }
  });
}

/**
 * Process member join and leave events
 * @param {Array} joinLogs - Array of member join audit logs
 * @param {Array} leaveLogs - Array of member leave audit logs
 * @param {Object} db - SQLite database connection
 * @returns {Promise<Object>} - Statistics about member events processed
 */
async function processMemberEvents(joinLogs, leaveLogs, db) {
  return new Promise((resolve, reject) => {
    try {
      console.log(`[${getCurrentTime()}] Processing member events: ${joinLogs.length} joins, ${leaveLogs.length} leaves`);
      
      let joinCount = 0;
      let leaveCount = 0;
      
      // Begin transaction
      db.run('BEGIN TRANSACTION', (transErr) => {
        if (transErr) {
          console.error(`[${getCurrentTime()}] Error beginning transaction:`, transErr);
          reject(transErr);
          return;
        }
        
        // Use prepared statement for better performance
        const eventStmt = db.prepare(`
          INSERT OR IGNORE INTO "join-leave" (id, event, timestamp)
          VALUES (?, ?, ?)
        `);
        
        // Process join events
        for (const log of joinLogs) {
          if (!log.target || !log.target.id) continue;
          
          try {
            eventStmt.run(log.target.id, 'join', log.createdAt);
            joinCount++;
          } catch (err) {
            console.error(`[${getCurrentTime()}] Error inserting join event:`, err);
          }
        }
        
        // Process leave events
        for (const log of leaveLogs) {
          if (!log.target || !log.target.id) continue;
          
          try {
            eventStmt.run(log.target.id, 'leave', log.createdAt);
            leaveCount++;
          } catch (err) {
            console.error(`[${getCurrentTime()}] Error inserting leave event:`, err);
          }
        }
        
        // Finalize statement
        eventStmt.finalize();
        
        // Commit the transaction
        db.run('COMMIT', (commitErr) => {
          if (commitErr) {
            console.error(`[${getCurrentTime()}] Error committing transaction:`, commitErr);
            db.run('ROLLBACK');
            reject(commitErr);
            return;
          }
          
          console.log(`[${getCurrentTime()}] Member events processed: ${joinCount} joins, ${leaveCount} leaves`);
          resolve({ joinCount, leaveCount });
        });
      });
    } catch (error) {
      console.error(`[${getCurrentTime()}] Error processing member events:`, error);
      
      // Try to rollback if there was an error
      try {
        db.run('ROLLBACK');
      } catch (e) {
        console.error(`[${getCurrentTime()}] Error during rollback:`, e);
      }
      
      reject(error);
    }
  });
}

/**
 * Get current time in ISO format
 * @returns {string} - ISO formatted timestamp
 */
function getCurrentTime() {
  return new Date().toISOString();
}

// Module exports
module.exports = {
  auditGuildCommand,
  auditAfterExport
};