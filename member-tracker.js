// member-tracker.js - Module to track member stats, roles, and role changes
const { PermissionFlagsBits } = require('discord.js');
const config = require('./config');
const monitor = require('./monitor');

// Initialize the database tables for member tracking
async function initializeMemberDatabase(db) {
  return new Promise((resolve, reject) => {
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    console.log('Initializing member tracking database tables...');
    
    // Create members table to store basic member information
db.run(`
  CREATE TABLE IF NOT EXISTS guild_members (
    id TEXT PRIMARY KEY,
    username TEXT,
    avatarURL TEXT,
    joinedAt TEXT,
    joinedTimestamp INTEGER,
    bot INTEGER DEFAULT 0,
    lastUpdated INTEGER,
    leftGuild INTEGER DEFAULT 0,
    leftTimestamp INTEGER,
    rejoinTimestamp INTEGER
  )
`, (err) => {
  if (err) {
    console.error('Error creating guild_members table:', err);
    reject(err);
    return;
  }
      
      // Create member_roles table to track current roles
      db.run(`
        CREATE TABLE IF NOT EXISTS member_roles (
          memberId TEXT,
          roleId TEXT,
          roleName TEXT,
          addedAt INTEGER,
          PRIMARY KEY (memberId, roleId)
        )
      `, (err) => {
        if (err) {
          console.error('Error creating member_roles table:', err);
          reject(err);
          return;
        }
        
        // Create role_history table to track role changes over time
        db.run(`
          CREATE TABLE IF NOT EXISTS role_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memberId TEXT,
            roleId TEXT,
            action TEXT,
            timestamp INTEGER,
            FOREIGN KEY (memberId) REFERENCES guild_members(id)
          )
        `, (err) => {
          if (err) {
            console.error('Error creating role_history table:', err);
            reject(err);
            return;
          }
          
          // Create guild_roles table to track roles and their hierarchy
          db.run(`
            CREATE TABLE IF NOT EXISTS guild_roles (
              id TEXT PRIMARY KEY,
              name TEXT,
              color TEXT,
              position INTEGER,
              permissions TEXT,
              mentionable INTEGER,
              hoist INTEGER,
              managed INTEGER,
              createdAt TEXT,
              createdTimestamp INTEGER,
              updatedAt TEXT,
              updatedTimestamp INTEGER,
              deleted INTEGER DEFAULT 0,
              deletedAt TEXT,
              deletedTimestamp INTEGER
            )
          `, (err) => {
            if (err) {
              console.error('Error creating guild_roles table:', err);
              reject(err);
              return;
            }
            
            // Create processing_checkpoints table for efficient member fetching
            db.run(`
              CREATE TABLE IF NOT EXISTS processing_checkpoints (
                guild_id TEXT PRIMARY KEY,
                last_member_id TEXT,
                timestamp INTEGER
              )
            `, (err) => {
              if (err) {
                console.error('Error creating processing_checkpoints table:', err);
                reject(err);
                return;
              }
              
              // Create indexes for better performance
              db.run(`CREATE INDEX IF NOT EXISTS idx_member_roles_member_id ON member_roles(memberId)`, (err) => {
                if (err) {
                  console.error('Error creating member_roles index:', err);
                  // Non-fatal, continue
                }
                
                console.log('Member tracking database tables initialized successfully');
                resolve(true);
              });
            });
          });
        });
      });
    });
  });
}

// Save member to database
async function storeMemberInDb(member) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    // Only process if we can get valid member data
    if (!member || !member.id) {
      reject(new Error("Invalid member object"));
      return;
    }
    
    const currentTime = Date.now();
    const joinedTimestamp = member.joinedTimestamp || null;
    const joinedAt = joinedTimestamp ? new Date(joinedTimestamp).toISOString() : null;
    
    // Store member data
    const sql = `
      INSERT OR REPLACE INTO guild_members (
        id, username, avatarURL, joinedAt, joinedTimestamp, 
        bot, lastUpdated, leftGuild
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    db.run(sql, [
      member.id,
      member.user.username,
      member.user.displayAvatarURL(),
      joinedAt,
      joinedTimestamp,
      member.user.bot ? 1 : 0,
      currentTime,
      0 // Not left guild
    ], function(err) {
      if (err) {
        console.error(`Error storing member ${member.id} in database:`, err);
        reject(err);
        return;
      }
      
      console.log(`Stored or updated member ${member.user.username} (${member.id}) in database`);
      resolve(this.changes);
    });
  });
}

// Store member roles in database
async function storeMemberRolesInDb(member) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    // Only process if we have a valid member with roles
    if (!member || !member.roles || !member.roles.cache) {
      reject(new Error("Invalid member object or roles collection"));
      return;
    }
    
    const currentTime = Date.now();
    const roles = Array.from(member.roles.cache.values());
    
    // Skip @everyone role
    const filteredRoles = roles.filter(role => role.id !== member.guild.id);
    
    if (filteredRoles.length === 0) {
      console.log(`Member ${member.user.username} has no roles to store (besides @everyone)`);
      resolve(0);
      return;
    }
    
    // Get current role IDs for comparison
    const newRoleIds = new Set(filteredRoles.map(role => role.id));
    
    // First get existing roles
    db.all(`SELECT roleId, roleName, addedAt FROM member_roles WHERE memberId = ?`, [member.id], (err, existingRoles) => {
      if (err) {
        console.error(`Error fetching existing roles for member ${member.id}:`, err);
        reject(err);
        return;
      }
      
      // Create set of existing role IDs and a map of role ID to timestamp
      const existingRoleIds = new Set();
      const roleTimestamps = {};
      
      existingRoles.forEach(role => {
        existingRoleIds.add(role.roleId);
        roleTimestamps[role.roleId] = role.addedAt;
      });
      
      // Identify roles to remove (in existing but not in new set)
      const rolesToRemove = Array.from(existingRoleIds).filter(id => !newRoleIds.has(id));
      
      // Identify roles to add (in new but not in existing set)
      const rolesToAdd = filteredRoles.filter(role => !existingRoleIds.has(role.id));
      
      // Check if we need to make any changes
      if (rolesToRemove.length === 0 && rolesToAdd.length === 0) {
        console.log(`No role changes needed for member ${member.user.username}`);
        resolve(0);
        return;
      }

      // First check if we're already in a transaction
      db.get("SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_master'", function(checkErr, row) {
        if (checkErr) {
          // Error checking transaction status, proceed with caution
          console.error(`Error checking transaction status: ${checkErr}. Will try without transaction.`);
          processRoleChanges(false);
        } else {
          // If we can query sqlite_master, we're not in a transaction
          // Otherwise, the error above would occur
          processRoleChanges(true);
        }
      });
      
      // Process role changes with or without a transaction
      function processRoleChanges(useTransaction) {
        let successCount = 0;
        let promiseChain = Promise.resolve();
        
        // Start transaction if needed
        if (useTransaction) {
          promiseChain = promiseChain.then(() => {
            return new Promise((resolveBegin, rejectBegin) => {
              db.run('BEGIN TRANSACTION', function(beginErr) {
                if (beginErr) {
                  console.error(`Error beginning transaction for member ${member.id}:`, beginErr);
                  rejectBegin(beginErr);
                  return;
                }
                resolveBegin();
              });
            });
          });
        }
        
        // Remove roles that are no longer assigned
        if (rolesToRemove.length > 0) {
          const placeholders = rolesToRemove.map(() => '?').join(',');
          const deleteQuery = `DELETE FROM member_roles WHERE memberId = ? AND roleId IN (${placeholders})`;
          
          promiseChain = promiseChain.then(() => {
            return new Promise((resolveDelete, rejectDelete) => {
              db.run(deleteQuery, [member.id, ...rolesToRemove], function(deleteErr) {
                if (deleteErr) {
                  console.error(`Error removing old roles for member ${member.user.username}:`, deleteErr);
                  rejectDelete(deleteErr);
                  return;
                }
                console.log(`Removed ${this.changes} old roles for ${member.user.username}`);
                resolveDelete();
              });
            });
          });
        }
        
        // Add new roles
        if (rolesToAdd.length > 0) {
          const insertStmt = db.prepare(`
            INSERT INTO member_roles (memberId, roleId, roleName, addedAt)
            VALUES (?, ?, ?, ?)
          `);
          
          promiseChain = promiseChain.then(() => {
            return new Promise((resolveInserts, rejectInserts) => {
              let insertPromises = [];
              
              for (const role of rolesToAdd) {
                insertPromises.push(new Promise((resolveInsert) => {
                  insertStmt.run([
                    member.id,
                    role.id,
                    role.name,
                    currentTime // New roles get current timestamp
                  ], function(insertErr) {
                    if (insertErr) {
                      console.error(`Error adding new role ${role.name} for member ${member.user.username}:`, insertErr);
                    } else {
                      successCount++;
                    }
                    resolveInsert();
                  });
                }));
              }
              
              Promise.all(insertPromises)
                .then(() => {
                  insertStmt.finalize();
                  resolveInserts();
                })
                .catch(err => rejectInserts(err));
            });
          });
        }
        
        // Commit transaction if we started one
        if (useTransaction) {
          promiseChain = promiseChain.then(() => {
            return new Promise((resolveCommit, rejectCommit) => {
              db.run('COMMIT', commitErr => {
                if (commitErr) {
                  console.error(`Error committing transaction for member ${member.id}:`, commitErr);
                  db.run('ROLLBACK');
                  rejectCommit(commitErr);
                  return;
                }
                console.log(`Successfully updated roles for ${member.user.username}: added ${rolesToAdd.length}, removed ${rolesToRemove.length}`);
                resolveCommit();
              });
            });
          });
        } else {
          // No transaction, just add a log message
          promiseChain = promiseChain.then(() => {
            console.log(`Successfully updated roles for ${member.user.username}: added ${rolesToAdd.length}, removed ${rolesToRemove.length}`);
            return Promise.resolve();
          });
        }
        
        // Final resolution
        promiseChain
          .then(() => {
            resolve(successCount);
          })
          .catch(finalErr => {
            if (useTransaction) {
              db.run('ROLLBACK');
            }
            reject(finalErr);
          });
      }
    });
  });
}

// Also modify storeMemberRolesInDbBatch to use the same approach
async function storeMemberRolesInDbBatch(members) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    let totalRolesAdded = 0;
    let totalRolesRemoved = 0;
    let processedMembers = 0;
    
    // Process each member sequentially to avoid transaction conflicts
    const processMember = async (index) => {
      if (index >= members.length) {
        // All members processed
        console.log(`Successfully processed roles for ${processedMembers} members: ${totalRolesAdded} added, ${totalRolesRemoved} removed`);
        resolve(totalRolesAdded);
        return;
      }
      
      const member = members[index];
      
      if (!member || !member.roles || !member.roles.cache) {
        console.log(`Skipping invalid member`);
        processMember(index + 1);
        return;
      }
      
      try {
        // First, get existing roles for this member
        const existingRoles = await new Promise((resolveQuery, rejectQuery) => {
          db.all(`SELECT roleId FROM member_roles WHERE memberId = ?`, [member.id], (err, rows) => {
            if (err) rejectQuery(err);
            else resolveQuery(rows || []);
          });
        });
        
        const existingRoleIds = new Set(existingRoles.map(row => row.roleId));
        
        // Get current roles (excluding @everyone)
        const currentRoles = Array.from(member.roles.cache.values())
          .filter(role => role.id !== member.guild.id);
        
        const currentRoleIds = new Set(currentRoles.map(role => role.id));
        
        // Find roles to add and remove
        const rolesToAdd = currentRoles.filter(role => !existingRoleIds.has(role.id));
        const roleIdsToRemove = [...existingRoleIds].filter(id => !currentRoleIds.has(id));
        
        // Process removals
        for (const roleId of roleIdsToRemove) {
          await new Promise((resolveRemove, rejectRemove) => {
            db.run(`DELETE FROM member_roles WHERE memberId = ? AND roleId = ?`, 
                   [member.id, roleId], 
                   err => err ? rejectRemove(err) : resolveRemove());
          });
          totalRolesRemoved++;
        }
        
        // Process additions
        for (const role of rolesToAdd) {
          await new Promise((resolveAdd, rejectAdd) => {
            db.run(`INSERT INTO member_roles (memberId, roleId, roleName, addedAt) VALUES (?, ?, ?, ?)`,
                   [member.id, role.id, role.name, currentTime],
                   err => err ? rejectAdd(err) : resolveAdd());
          });
          totalRolesAdded++;
        }
        
        processedMembers++;
        processMember(index + 1);
      } catch (error) {
        console.error(`Error processing roles for member ${member?.id}:`, error);
        // Continue with next member despite errors
        processMember(index + 1);
      }
    };
    
    // Start processing with the first member
    processMember(0);
  });
}

// Store multiple members' roles in a batch transaction - FIXED VERSION
async function storeMemberRolesInDbBatch(members) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    let totalRolesAdded = 0;
    
    // No transaction here - rely on the outer transaction
    
    // Prepare delete statement for reuse
    const deleteStmt = db.prepare(`DELETE FROM member_roles WHERE memberId = ?`);
    
    // Prepare insert statement for reuse
    const insertStmt = db.prepare(`
      INSERT INTO member_roles (
        memberId, roleId, roleName, addedAt
      ) VALUES (?, ?, ?, ?)
    `);
    
    // Process each member
    for (const member of members) {
      if (!member || !member.roles || !member.roles.cache) {
        console.log(`Skipping invalid member`);
        continue;
      }
      
      const roles = Array.from(member.roles.cache.values());
      const filteredRoles = roles.filter(role => role.id !== member.guild.id);
      
      if (filteredRoles.length === 0) {
        console.log(`Member ${member.user.username} has no roles to store (besides @everyone)`);
        continue;
      }
      
      // Insert each role
      for (const role of filteredRoles) {
        insertStmt.run(
          member.id,
          role.id,
          role.name,
          currentTime
        );
        totalRolesAdded++;
      }
    }
    
    // Finalize statements
    deleteStmt.finalize();
    insertStmt.finalize();
    
    // No commit or rollback here - rely on outer transaction management
    
    console.log(`Successfully stored ${totalRolesAdded} roles for ${members.length} members in batch`);
    resolve(totalRolesAdded);
  });
}

// Add role history entry
async function addRoleHistoryEntry(memberId, roleId, roleName, action) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    
    const sql = `
      INSERT INTO role_history (
        memberId, roleId, action, timestamp
      ) VALUES (?, ?, ?, ?)
    `;
    
    db.run(sql, [
      memberId,
      roleId,
      action, // 'added' or 'removed'
      currentTime
    ], function(err) {
      if (err) {
        console.error(`Error adding role history entry for ${memberId}:`, err);
        reject(err);
        return;
      }
      
      console.log(`Added role history entry: ${action} role ${roleName} for member ${memberId}`);
      resolve(this.lastID);
    });
  });
}

// Mark member as having left the guild
async function markMemberLeftGuild(memberId, username) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    
    // Modified SQL to keep rejoinTimestamp intact
    const sql = `
      UPDATE guild_members 
      SET leftGuild = 1, leftTimestamp = ? 
      WHERE id = ?
    `;
    
    db.run(sql, [currentTime, memberId], function(err) {
      if (err) {
        console.error(`[${getFormattedDateTime()}] Error marking member ${memberId} as left:`, err);
        reject(err);
        return;
      }
      
      if (this.changes > 0) {
        console.log(`[${getFormattedDateTime()}] Marked member ${username} (${memberId}) as having left the guild`);
      } else {
        console.log(`[${getFormattedDateTime()}] Member ${memberId} not found in database or already marked as left`);
      }
      resolve(this.changes);
    });
  });
}

// Store a role in the database
async function storeRoleInDb(role) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    const currentTimeIso = new Date(currentTime).toISOString();
    const createdTimestamp = role.createdTimestamp;
    const createdAt = new Date(createdTimestamp).toISOString();
    
    const sql = `
      INSERT OR REPLACE INTO guild_roles (
        id, name, color, position, permissions, mentionable, hoist, managed,
        createdAt, createdTimestamp, updatedAt, updatedTimestamp, deleted
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    db.run(sql, [
      role.id,
      role.name,
      role.hexColor,
      role.position,
      role.permissions.bitfield.toString(),
      role.mentionable ? 1 : 0,
      role.hoist ? 1 : 0,
      role.managed ? 1 : 0,
      createdAt,
      createdTimestamp,
      currentTimeIso,
      currentTime,
      0 // not deleted
    ], function(err) {
      if (err) {
        console.error(`Error storing role ${role.name} (${role.id}) in database:`, err);
        reject(err);
        return;
      }
      
      console.log(`Stored or updated role ${role.name} (${role.id}) in database`);
      resolve(this.changes);
    });
  });
}

// Mark a role as deleted
async function markRoleDeleted(role) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    const currentTimeIso = new Date(currentTime).toISOString();
    
    const sql = `
      UPDATE guild_roles
      SET deleted = 1, deletedAt = ?, deletedTimestamp = ?
      WHERE id = ?
    `;
    
    db.run(sql, [
      currentTimeIso,
      currentTime,
      role.id
    ], function(err) {
      if (err) {
        console.error(`Error marking role ${role.name} (${role.id}) as deleted:`, err);
        reject(err);
        return;
      }
      
      console.log(`Marked role ${role.name} (${role.id}) as deleted`);
      resolve(this.changes);
    });
  });
}

// Fetch and store all guild roles
async function fetchAndStoreGuildRoles(guild) {
  try {
    const db = monitor.getDatabase();
    if (!db) {
      console.error("Cannot fetch roles: Database not initialized");
      return { success: false, error: "Database not initialized" };
    }
    
    console.log(`Starting to fetch all roles for guild ${guild.name} (${guild.id})`);
    
    // Ensure we have fetched all roles
    await guild.roles.fetch();
    
    const roles = Array.from(guild.roles.cache.values());
    let roleCount = 0;
    
    // Process each role
    for (const role of roles) {
      try {
        // Skip @everyone role if desired
        // if (role.id === guild.id) continue;
        
        // Store the role data
        await storeRoleInDb(role);
        roleCount++;
      } catch (roleError) {
        console.error(`Error storing role ${role.name}:`, roleError);
      }
    }
    
    console.log(`Completed storing ${roleCount} roles for guild ${guild.name}`);
    
    return {
      success: true,
      roleCount
    };
  } catch (error) {
    console.error(`Error in fetchAndStoreGuildRoles:`, error);
    return {
      success: false,
      error: error.message
    };
  }
}

// Store checkpoint for efficient member processing
async function saveProcessingCheckpoint(guildId, lastProcessedMemberId) {
  const db = monitor.getDatabase();
  return new Promise((resolve, reject) => {
    db.run(
      'INSERT OR REPLACE INTO processing_checkpoints (guild_id, last_member_id, timestamp) VALUES (?, ?, ?)',
      [guildId, lastProcessedMemberId, Date.now()],
      function(err) {
        if (err) reject(err);
        else resolve(this.changes);
      }
    );
  });
}

// Get last checkpoint for resuming member processing
async function getProcessingCheckpoint(guildId) {
  const db = monitor.getDatabase();
  return new Promise((resolve, reject) => {
    db.get(
      'SELECT last_member_id FROM processing_checkpoints WHERE guild_id = ?',
      [guildId],
      (err, row) => {
        if (err) reject(err);
        else resolve(row ? row.last_member_id : null);
      }
    );
  });
}

/**
 * Memory-efficient guild member fetching for large Discord servers
 * Date: 2025-04-25
 */
async function fetchMembersInChunks(guild, statusMessage) {
  // Initialize tracking variables
  let lastId = null;
  let done = false;
  let fetchCount = 0;
  let totalFetched = 0;
  
  // Memory usage tracking
  const initialMemory = process.memoryUsage().heapUsed;
  const memoryThresholdMB = 1000; // 1GB threshold - adjust based on your environment
  const memoryThresholdBytes = memoryThresholdMB * 1024 * 1024;
  
  // Initialize database connection once outside the loop
  const db = monitor.getDatabase();
  if (!db) {
    throw new Error("Database not initialized");
  }
  
  // Enable WAL mode temporarily for this bulk operation
  console.log(`[${new Date().toISOString()}] Enabling WAL mode for bulk member fetch operation`);
  db.run('PRAGMA journal_mode = WAL');
  db.run('PRAGMA synchronous = NORMAL');
  db.run('PRAGMA cache_size = 10000');
  db.run('PRAGMA temp_store = MEMORY');
  
  // Prepare statements for better performance
  const memberStmt = db.prepare(`
    INSERT OR REPLACE INTO guild_members (
      id, username, avatarURL, joinedAt, joinedTimestamp, 
      bot, lastUpdated, leftGuild
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  
  const deleteRolesStmt = db.prepare(`
    DELETE FROM member_roles WHERE memberId = ?
  `);
  
  // Change to "INSERT OR IGNORE" to prevent unique constraint errors
  const roleStmt = db.prepare(`
    INSERT OR IGNORE INTO member_roles (
      memberId, roleId, roleName, addedAt
    ) VALUES (?, ?, ?, ?)
  `);
  
  console.log(`[${new Date().toISOString()}] Starting memory-efficient member fetch for guild ${guild.name} (${guild.id})`);
  await statusMessage.edit(`Member Database Import Status\n` +
    `🔄 Starting memory-efficient member fetch for ${guild.name}...`);
  
  // First fetch and store guild roles to ensure they're available for member role assignments
  try {
    await statusMessage.edit(`Member Database Import Status\n` +
      `🔄 Fetching roles for ${guild.name}...`);
    
    const roleResult = await fetchAndStoreGuildRoles(guild);
    await statusMessage.edit(`Member Database Import Status\n` +
      `✅ Stored ${roleResult.roleCount} roles\n` +
      `🔄 Now fetching members (this may take a while for large guilds)...`);
  } catch (roleError) {
    console.error(`[${new Date().toISOString()}] Error fetching roles:`, roleError);
  }
  
  // Create a Map for role caching to avoid repeated object creation
  const roleCache = new Map();
  
  // Track processed members to avoid duplication
  const processedMemberIds = new Set();
  
  // Helper function to execute a database transaction
  async function executeTransaction(operations) {
    return new Promise((resolve, reject) => {
      db.serialize(() => {
        db.run('BEGIN TRANSACTION', (beginErr) => {
          if (beginErr) {
            return reject(beginErr);
          }
          
          try {
            // Execute the operations
            const result = operations();
            
            // Commit the transaction
            db.run('COMMIT', (commitErr) => {
              if (commitErr) {
                console.error('Error committing transaction:', commitErr);
                db.run('ROLLBACK', () => reject(commitErr));
              } else {
                resolve(result);
              }
            });
          } catch (operationErr) {
            console.error('Error during transaction operations:', operationErr);
            db.run('ROLLBACK', () => reject(operationErr));
          }
        });
      });
    });
  }
  
  // Fetch members in chunks and process them immediately
  while (!done) {
    try {
      // Check memory usage and perform garbage collection if available
      const currentMemory = process.memoryUsage().heapUsed;
      const memoryUsageMB = Math.round(currentMemory / 1024 / 1024);
      
      console.log(`[${new Date().toISOString()}] Memory usage: ${memoryUsageMB}MB`);
      
      if (currentMemory - initialMemory > memoryThresholdBytes) {
        console.log(`[${new Date().toISOString()}] Memory threshold reached, forcing garbage collection`);
        if (global.gc) {
          global.gc();
          await new Promise(resolve => setTimeout(resolve, 500)); // Give GC time to work
        }
      }
      
      // Build fetch options - using Discord's REST pagination
      const options = { limit: 1000 }; // Max allowed by Discord API
      if (lastId) options.after = lastId;
      
      fetchCount++;
      console.log(`[${new Date().toISOString()}] Fetching member chunk #${fetchCount} (after ID: ${lastId || 'start'})`);
      
      // Update status message periodically
      if (fetchCount % 5 === 0 || fetchCount === 1) {
        await statusMessage.edit(`Member Database Import Status\n` +
          `🔄 Fetched ${totalFetched} members so far...\n` +
          `💾 Memory usage: ${memoryUsageMB}MB\n` +
          `⏱️ Fetch operation #${fetchCount}`);
      }
      
      // Use REST API directly for better pagination control
      let response;
      
      try {
        // Use Discord.js's REST client for proper rate limit handling
        response = await guild.client.rest.get(
          `/guilds/${guild.id}/members?limit=1000${lastId ? `&after=${lastId}` : ''}`
        );
      } catch (apiError) {
        console.error(`[${new Date().toISOString()}] API Error:`, apiError);
        
        // Handle rate limiting explicitly
        if (apiError.httpStatus === 429) {
          const retryAfter = apiError.retryAfter || 5; // Default to 5 seconds if not specified
          console.log(`[${new Date().toISOString()}] Rate limited, waiting ${retryAfter}s before retry`);
          
          await statusMessage.edit(`Member Database Import Status\n` +
            `⏳ Rate limited by Discord. Waiting ${retryAfter}s before continuing...\n` +
            `🔄 Fetched ${totalFetched} members so far`);
          
          await new Promise(resolve => setTimeout(resolve, retryAfter * 1000 + 100));
          continue; // Try again
        }
        
        // For other errors, wait a bit and try again, but only up to 3 times
        if (apiError.httpStatus >= 500 && fetchCount < 3) {
          console.log(`[${new Date().toISOString()}] Server error, retrying in 5s...`);
          await new Promise(resolve => setTimeout(resolve, 5000));
          continue;
        }
        
        // If we get here, it's a serious error we can't recover from
        throw apiError;
      }
      
      // If no members returned, we're done
      if (!response || response.length === 0) {
        console.log(`[${new Date().toISOString()}] No more members returned, fetch complete`);
        done = true;
        continue;
      }
      
      // Batch processing with immediate storage
      console.log(`[${new Date().toISOString()}] Processing ${response.length} members from chunk #${fetchCount}`);
      
      // Use the transaction helper for proper transaction management
      let processedInBatch = 0;
      
      await executeTransaction(() => {
        const currentTime = Date.now();
        
        for (const memberData of response) {
          // Skip if invalid data
          if (!memberData || !memberData.user || !memberData.user.id) {
            continue;
          }
          
          // Extract just what we need to minimize memory usage
          const user = memberData.user;
          const memberId = user.id;
          
          // Skip if we've already processed this member
          if (processedMemberIds.has(memberId)) {
            console.log(`[${new Date().toISOString()}] Skipping already processed member: ${memberId}`);
            continue;
          }
          
          // Add to processed set
          processedMemberIds.add(memberId);
          
          const joinedTimestamp = memberData.joined_at ? new Date(memberData.joined_at).getTime() : null;
          const joinedAt = joinedTimestamp ? new Date(joinedTimestamp).toISOString() : null;
          
          // Store member data
          memberStmt.run([
            memberId,
            user.username,
            user.avatar ? `https://cdn.discordapp.com/avatars/${user.id}/${user.avatar}.png` : null,
            joinedAt,
            joinedTimestamp,
            user.bot ? 1 : 0,
            currentTime,
            0 // Not left guild
          ]);
                   
          // Track roles we've inserted for this member to avoid duplicates
          const insertedRoles = new Set();
          
          // Then add current roles
          if (memberData.roles && Array.isArray(memberData.roles)) {
            // Skip @everyone role which isn't included in the roles array from API
            // Process each role
            for (const roleId of memberData.roles) {
              // Skip if we've already processed this role for this member
              if (insertedRoles.has(roleId)) {
                continue;
              }
              
              // Get role data from cache if available
              let role = roleCache.get(roleId);
              
              // If not in cache, get from guild
              if (!role) {
                const guildRole = guild.roles.cache.get(roleId);
                if (guildRole) {
                  // Minimize the data we store in memory
                  role = {
                    id: guildRole.id,
                    name: guildRole.name
                  };
                  // Store in cache
                  roleCache.set(roleId, role);
                }
              }
              
              // Insert role if we have data
              if (role) {
                roleStmt.run([
                  memberId,
                  role.id,
                  role.name,
                  currentTime
                ]);
                
                // Mark this role as inserted for this member
                insertedRoles.add(roleId);
              }
            }
          }
          
          processedInBatch++;
          totalFetched++;
          
          // Update lastId to highest ID seen
          if (!lastId || BigInt(memberId) > BigInt(lastId)) {
            lastId = memberId;
          }
        }
        
        return processedInBatch;
      });
      
      console.log(`[${new Date().toISOString()}] Successfully committed ${processedInBatch} members to database`);
      
      // If we got fewer members than requested, we've reached the end
      if (response.length < 1000) {
        console.log(`[${new Date().toISOString()}] Reached end of member list (${response.length} < 1000)`);
        done = true;
      }
      
      // Add a small delay to prevent rate limits and allow GC to work
      await new Promise(resolve => setTimeout(resolve, 300));
      
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error in member processing loop:`, error);
      
      // Update status message with error
      await statusMessage.edit(`Member Database Import Status\n` +
        `⚠️ Error encountered: ${error.message}\n` +
        `🔄 Fetched ${totalFetched} members before error\n` +
        `🔄 Attempting to continue...`);
      
      // If we've been fetching for a while, try to continue
      if (fetchCount > 3) {
        await new Promise(resolve => setTimeout(resolve, 5000));
      } else {
        // If we're at the very beginning, this is fatal
        done = true;
        throw error;
      }
    }
  }
  
  // Success, update status
  console.log(`[${new Date().toISOString()}] Completed member fetch: ${totalFetched} members`);
  await statusMessage.edit(`Member Database Import Status\n` +
    `✅ Successfully fetched ${totalFetched} members from ${guild.name}\n` +
    `💾 Final memory usage: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);
  
  // Clean up prepared statements
  memberStmt.finalize();
  deleteRolesStmt.finalize();
  roleStmt.finalize();
  
  // Perform a full checkpoint and then switch back to DELETE mode
  console.log(`[${new Date().toISOString()}] Performing WAL checkpoint and reverting to DELETE journal mode...`);
  
  // First checkpoint to ensure all changes are in the main DB
  await new Promise((resolve, reject) => {
    db.run('PRAGMA wal_checkpoint(FULL)', function(err) {
      if (err) {
        console.error(`[${new Date().toISOString()}] Error during WAL checkpoint:`, err);
        reject(err);
      } else {
        console.log(`[${new Date().toISOString()}] WAL checkpoint completed successfully.`);
        resolve();
      }
    });
  });
  
  // Then switch back to DELETE mode which will remove the WAL file
  await new Promise((resolve, reject) => {
    db.run('PRAGMA journal_mode = DELETE', function(err) {
      if (err) {
        console.error(`[${new Date().toISOString()}] Error switching journal mode:`, err);
        reject(err);
      } else {
        console.log(`[${new Date().toISOString()}] Successfully switched to DELETE journal mode.`);
        resolve();
      }
    });
  });
  
  // Return summary
  return {
    success: true,
    memberCount: totalFetched,
    fetchOperations: fetchCount
  };
}

// Fetch all members for a guild and store them in database (for exportguild command)
async function fetchAndStoreMembersForGuild(guild, statusMessage) {
  try {
    const db = monitor.getDatabase();
    if (!db) {
      console.error("Cannot fetch members: Database not initialized");
      return { success: false, error: "Database not initialized" };
    }
    
    // Enable WAL mode temporarily for this bulk operation
    console.log(`[${new Date().toISOString()}] Enabling WAL mode for bulk member fetch operation`);
    db.run('PRAGMA journal_mode = WAL');
    db.run('PRAGMA synchronous = NORMAL');
    db.run('PRAGMA cache_size = 10000');
    db.run('PRAGMA temp_store = MEMORY');
    db.run('CREATE INDEX IF NOT EXISTS idx_member_roles_member_id ON member_roles(memberId)');
    
    // Update status message if provided
    if (statusMessage) {
      await statusMessage.edit(`Member Database Import Status\n` +
                           `🔄 Fetching members for ${guild.name}...`);
    }
    
    // Check if the guild is large and use the memory-efficient approach
    const isLargeGuild = guild.memberCount > 10000; // Threshold for large guilds
    
    if (isLargeGuild) {
      console.log(`Guild ${guild.name} has ${guild.memberCount} members - using memory-efficient processing`);
      return await fetchMembersInChunks(guild, statusMessage);
    } else {
      console.log(`Guild ${guild.name} has ${guild.memberCount} members - using standard processing`);
    }
    
    // Fetch and store all roles first
    try {
      await statusMessage.edit(`Member Database Import Status\n` +
                           `🔄 Fetching roles for ${guild.name}...`);
      
      const roleResult = await fetchAndStoreGuildRoles(guild);
      if (roleResult.success) {
        console.log(`Successfully stored ${roleResult.roleCount} roles for guild ${guild.name}`);
        await statusMessage.edit(`Member Database Import Status\n` +
                              `✅ Stored ${roleResult.roleCount} roles\n` +
                              `🔄 Now fetching members...`);
      } else {
        console.error('Error storing roles:', roleResult.error);
        await statusMessage.edit(`Member Database Import Status\n` +
                              `⚠️ Error storing roles: ${roleResult.error}\n` +
                              `🔄 Proceeding with member fetch...`);
      }
    } catch (roleError) {
      console.error('Error fetching roles:', roleError);
    }
    
    console.log(`Starting to fetch all members for guild ${guild.name} (${guild.id})`);
    
    let memberCount = 0;
    let roleCount = 0;
    
    try {
      await guild.members.fetch();
      console.log(`Fetched ${guild.members.cache.size} members from ${guild.name}`);
    } catch (error) {
      console.error(`Error fetching members for guild ${guild.name}:`, error);
      if (statusMessage) {
        await statusMessage.edit(`Member Database Import Status\n` +
                             `❌ Error fetching members: ${error.message}\n` +
                             `⚠️ Will proceed with ${guild.members.cache.size} cached members`);
      }
    }
    
    // Process members in parallel batches
    const members = Array.from(guild.members.cache.values());
    const batchSize = config.getConfig('memberBatchSize', 'MEMBER_BATCH_SIZE') || 100;
    const concurrentBatchCount = config.getConfig('concurrentBatches', 'CONCURRENT_BATCHES') || 5;
    
    // START A TRANSACTION HERE FOR ALL BATCHES
    db.run('BEGIN TRANSACTION');
    let transactionActive = true;
    
    try {
      // Process batches of members concurrently
      for (let i = 0; i < members.length; i += (batchSize * concurrentBatchCount)) {
        const batchPromises = [];
        
        for (let j = 0; j < concurrentBatchCount; j++) {
          const startIndex = i + (j * batchSize);
          if (startIndex >= members.length) break;
          
          const endIndex = Math.min(startIndex + batchSize, members.length);
          const currentBatch = members.slice(startIndex, endIndex);
          
          if (currentBatch.length > 0) {
            batchPromises.push(processMemberBatch(currentBatch));
          }
        }
        
        if (batchPromises.length > 0) {
          const batchResults = await Promise.all(batchPromises);
          
          // Sum up the results
          for (const result of batchResults) {
            memberCount += result.members;
            roleCount += result.roles;
          }
          
          // Update status message
          if (statusMessage) {
            await statusMessage.edit(`Member Database Import Status\n` +
                                 `🔄 Processed ${memberCount}/${members.length} members with ${roleCount} roles...`);
          }
        }
      }
      
      // COMMIT TRANSACTION AFTER ALL BATCHES
      db.run('COMMIT');
      transactionActive = false;
      
      // Final status update
      console.log(`Completed storing ${memberCount} members with ${roleCount} roles for guild ${guild.name}`);
      
      if (statusMessage) {
        await statusMessage.edit(`Member Database Import Status\n` +
                           `✅ Completed! Stored data for ${memberCount} members with ${roleCount} total roles`);
      }
      
      // Perform a full checkpoint and then switch back to DELETE mode
      console.log(`Performing WAL checkpoint and reverting to DELETE journal mode...`);
      
      // First checkpoint to ensure all changes are in the main DB
      await new Promise((resolve, reject) => {
        db.run('PRAGMA wal_checkpoint(FULL)', function(err) {
          if (err) {
            console.error(`Error during WAL checkpoint:`, err);
            reject(err);
          } else {
            console.log(`WAL checkpoint completed successfully.`);
            resolve();
          }
        });
      });
      
      // Then switch back to DELETE mode which will remove the WAL file
      await new Promise((resolve, reject) => {
        db.run('PRAGMA journal_mode = DELETE', function(err) {
          if (err) {
            console.error(`Error switching journal mode:`, err);
            reject(err);
          } else {
            console.log(`Successfully switched to DELETE journal mode.`);
            resolve();
          }
        });
      });
      
      return {
        success: true,
        memberCount,
        roleCount
      };
    } catch (error) {
      // ROLLBACK TRANSACTION ON ERROR
      if (transactionActive) {
        db.run('ROLLBACK');
      }
      
      console.error(`Error in fetchAndStoreMembersForGuild:`, error);
      if (statusMessage) {
        await statusMessage.edit(`Member Database Import Status\n` +
                           `❌ Error: ${error.message}`);
      }
      return {
        success: false,
        error: error.message
      };
    }
  } catch (error) {
    console.error(`Error in fetchAndStoreMembersForGuild:`, error);
    if (statusMessage) {
      await statusMessage.edit(`Member Database Import Status\n` +
                           `❌ Error: ${error.message}`);
    }
    return {
      success: false,
      error: error.message
    };
  }
  
  // Helper function to process a batch of members
  async function processMemberBatch(memberBatch) {
    let batchMemberCount = 0;
    let batchRoleCount = 0;
    
    // Store all members first
    const memberInsertPromises = memberBatch.map(member => storeMemberInDb(member));
    await Promise.all(memberInsertPromises);
    batchMemberCount += memberBatch.length;
    
    // Group members for batch role processing
    const batchesOf50 = [];
    for (let i = 0; i < memberBatch.length; i += 50) {
      batchesOf50.push(memberBatch.slice(i, i + 50));
    }
    
    // Process roles in smaller batches
    for (const smallBatch of batchesOf50) {
      try {
        // Use the new batch processing function
        const roleResult = await storeMemberRolesInDbBatch(smallBatch);
        batchRoleCount += roleResult;
      } catch (error) {
        console.error('Error in batch role processing:', error);
        
        // Fallback to individual processing
        for (const member of smallBatch) {
          try {
            const roleResult = await storeMemberRolesInDb(member);
            batchRoleCount += roleResult;
          } catch (memberError) {
            console.error(`Error storing roles for member ${member.user.username}:`, memberError);
          }
        }
      }
    }
    
    return { members: batchMemberCount, roles: batchRoleCount };
  }
}

/**
 * Cleans up WAL files by checkpointing and switching journal mode
 * Can be called anytime WAL files need to be removed
 */
async function cleanupWalFiles() {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    console.log(`[${new Date().toISOString()}] Cleaning up WAL files...`);
    
    // First checkpoint
    db.run('PRAGMA wal_checkpoint(FULL)', function(checkpointErr) {
      if (checkpointErr) {
        console.error(`[${new Date().toISOString()}] Error during checkpoint:`, checkpointErr);
      }
      
      // Switch to DELETE mode to remove the WAL file
      db.run('PRAGMA journal_mode = DELETE', function(modeErr) {
        if (modeErr) {
          console.error(`[${new Date().toISOString()}] Error switching journal mode:`, modeErr);
          reject(modeErr);
          return;
        }
        
        console.log(`[${new Date().toISOString()}] Successfully removed WAL files`);
        resolve(true);
      });
    });
  });
}

// Initialize member tracking
function initializeMemberTracking(client) {
  console.log(`[${getFormattedDateTime()}] Member tracking initialized`);
  
  // Listen for guildMemberAdd events
client.on('guildMemberAdd', async (member) => {
  try {
    // Only process if database is initialized for this guild
    const dbExists = monitor.checkDatabaseExists(member.guild);
    if (!dbExists) {
      console.log(`[${getFormattedDateTime()}] Skipping new member ${member.user.username}: no database for guild ${member.guild.name}`);
      return;
    }
    
    console.log(`[${getFormattedDateTime()}] New member detected: ${member.user.username} (${member.id})`);
    
    // Get the database
    const db = monitor.getDatabase();
    
    // First, check if this member was previously in the database and had left
    db.get(
      "SELECT leftGuild, joinedTimestamp FROM guild_members WHERE id = ?", 
      [member.id], 
      async (err, row) => {
        if (err) {
          console.error(`[${getFormattedDateTime()}] Error checking if member previously left:`, err);
          // Continue with normal member creation
          await storeMemberInDb(member);
          await storeMemberRolesInDb(member);
          return;
        }
        
        const currentTime = Date.now();
        
        if (row && row.leftGuild === 1) {
          // This member previously left the guild (we have a record with leftGuild=1)
          console.log(`[${getFormattedDateTime()}] Member ${member.user.username} (${member.id}) is rejoining the guild`);
          
          // Update only the necessary fields:
          // - Set leftGuild back to 0 (they're no longer "left")
          // - Set rejoinTimestamp to current time
          // - Update username, avatar URL and lastUpdated (these may have changed)
          // - Preserve joinedTimestamp and leftTimestamp
          db.run(`
            UPDATE guild_members 
            SET leftGuild = 0,
                rejoinTimestamp = ?,
                username = ?,
                avatarURL = ?,
                lastUpdated = ?
            WHERE id = ?
          `, [
            currentTime, // Current time for rejoinTimestamp
            member.user.username,
            member.user.displayAvatarURL(),
            currentTime, // lastUpdated
            member.id
          ], function(updateErr) {
            if (updateErr) {
              console.error(`[${getFormattedDateTime()}] Error updating rejoin timestamp:`, updateErr);
            } else {
              console.log(`[${getFormattedDateTime()}] Updated rejoin data for ${member.user.username}`);
            }
          });
        } else {
          // Either a brand new member or one that didn't have leftGuild=1
          // Use the standard function to store them
          await storeMemberInDb(member);
        }
        
        // Always store the member's current roles
        await storeMemberRolesInDb(member);
      }
    );
  } catch (error) {
    console.error(`[${getFormattedDateTime()}] Error processing new member:`, error);
  }
});

client.on('guildMemberRemove', async (member) => {
  try {
    // Only process if database is initialized for this guild
    const dbExists = monitor.checkDatabaseExists(member.guild);
    if (!dbExists) {
      console.log(`[${getFormattedDateTime()}] Skipping member leave ${member.user.username}: no database for guild ${member.guild.name}`);
      return;
    }
    
    console.log(`[${getFormattedDateTime()}] Member left: ${member.user.username} (${member.id})`);
    
    // Mark member as having left the guild - only update leftGuild and leftTimestamp
    const db = monitor.getDatabase();
    const currentTime = Date.now();
    
    db.run(`
      UPDATE guild_members 
      SET leftGuild = 1, leftTimestamp = ? 
      WHERE id = ?
    `, [currentTime, member.id], function(err) {
      if (err) {
        console.error(`[${getFormattedDateTime()}] Error marking member as left:`, err);
        return;
      }
      
      if (this.changes > 0) {
        console.log(`[${getFormattedDateTime()}] Marked member ${member.user.username} (${member.id}) as having left the guild`);
      } else {
        console.log(`[${getFormattedDateTime()}] Member ${member.id} not found in database or already marked as left`);
      }
    });
    
  } catch (error) {
    console.error(`[${getFormattedDateTime()}] Error processing member leave:`, error);
  }
});
  
  // Listen for guildMemberUpdate events to track role changes
client.on('guildMemberUpdate', async (oldMember, newMember) => {
  // Log every time the event fires, regardless of database status
  console.log(`[${getFormattedDateTime()}] MEMBER_UPDATE_EVENT received for ${newMember.user.username}`);
  
  try {
    // Only process if database is initialized for this guild
    const dbExists = monitor.checkDatabaseExists(newMember.guild);
    if (!dbExists) {
      console.log(`[${getFormattedDateTime()}] Database not initialized for guild ${newMember.guild.name} - skipping role tracking`);
      return;
    }
    
    // Check for role changes
    const oldRoles = oldMember.roles.cache;
    const newRoles = newMember.roles.cache;
    
    // Log role collections for debugging
    console.log(`[${getFormattedDateTime()}] Old roles: ${Array.from(oldRoles.values()).map(r => r.name).join(', ')}`);
    console.log(`[${getFormattedDateTime()}] New roles: ${Array.from(newRoles.values()).map(r => r.name).join(', ')}`);
    
    // Find added roles (in new but not in old)
    for (const [roleId, role] of newRoles) {
      // Skip @everyone role
      if (roleId === newMember.guild.id) continue;
      
      if (!oldRoles.has(roleId)) {
        console.log(`[${getFormattedDateTime()}] Role added to ${newMember.user.username}: ${role.name}`);
        
        // Add to role history
        await addRoleHistoryEntry(newMember.id, roleId, role.name, 'added');
      }
    }
    
    // Find removed roles (in old but not in new)
    for (const [roleId, role] of oldRoles) {
      // Skip @everyone role
      if (roleId === newMember.guild.id) continue;
      
      if (!newRoles.has(roleId)) {
        console.log(`[${getFormattedDateTime()}] Role removed from ${newMember.user.username}: ${role.name}`);
        
        // Add to role history
        await addRoleHistoryEntry(newMember.id, roleId, role.name, 'removed');
      }
    }
    
    // Update member data in database with any changes in username
    if (oldMember.user.username !== newMember.user.username) {
      await storeMemberInDb(newMember);
    }
    
    // Update only the changed roles (using our new function)
    await storeMemberRolesInDb(newMember);
    
  } catch (error) {
    console.error(`[${getFormattedDateTime()}] Error processing member update:`, error);
  }
});
  
  client.on('debug', info => {
  if (info.includes('GUILD_MEMBER_UPDATE')) {
    console.log('Debug - Member Update Event:', info);
  }
});
  
    // Listen for roleCreate events
  client.on('roleCreate', async (role) => {
    try {
      // Only process if database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(role.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping new role ${role.name}: no database for guild ${role.guild.name}`);
        return;
      }
      
      console.log(`[${getFormattedDateTime()}] New role created: ${role.name} (${role.id})`);
      
      // Store the role in database
      await storeRoleInDb(role);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing new role:`, error);
    }
  });
  
  // Listen for roleDelete events
  client.on('roleDelete', async (role) => {
    try {
      // Only process if database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(role.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping role deletion ${role.name}: no database for guild ${role.guild.name}`);
        return;
      }
      
      console.log(`[${getFormattedDateTime()}] Role deleted: ${role.name} (${role.id})`);
      
      // Mark the role as deleted in database
      await markRoleDeleted(role);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing role deletion:`, error);
    }
  });
  
  // Listen for roleUpdate events
  client.on('roleUpdate', async (oldRole, newRole) => {
    try {
      // Only process if database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(newRole.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping role update ${newRole.name}: no database for guild ${newRole.guild.name}`);
        return;
      }
      
      console.log(`[${getFormattedDateTime()}] Role updated: ${newRole.name} (${newRole.id})`);
      
      // Store the updated role in database
      await storeRoleInDb(newRole);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing role update:`, error);
    }
  });
}

// Helper function to get formatted date and time
function getFormattedDateTime() {
  const now = new Date();
  return now.toISOString();
}

// Export functions
module.exports = {
  initializeMemberTracking,
  initializeMemberDatabase,
  fetchAndStoreMembersForGuild,
  fetchMembersInChunks,
  storeMemberInDb,
  storeMemberRolesInDb,
  storeMemberRolesInDbBatch,
  addRoleHistoryEntry,
  markMemberLeftGuild,
  storeRoleInDb,
  markRoleDeleted,
  fetchAndStoreGuildRoles,
  cleanupWalFiles
};