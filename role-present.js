// role-present.js - Module to handle adding "present" role history entries during exportguild
const monitor = require('./monitor');

/**
 * Process "present" role entries for members
 * Adds entries to role_history for each role a member currently has
 * @param {Array} members - Array of Discord.js GuildMember objects
 * @returns {Promise<Object>} - Results of the operation
 */
async function processCurrentRoles(members) {
  return new Promise(async (resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }

    const currentTime = Date.now();
    let addedEntries = 0;
    let skippedEntries = 0;
    let errorCount = 0;
    let processedMembers = 0;

    console.log(`[2025-05-06 17:16:28] Processing "present" role entries for ${members.length} members...`);

    try {
      // Begin transaction for better performance
      await new Promise((resolveBegin, rejectBegin) => {
        db.run('BEGIN TRANSACTION', function(beginErr) {
          if (beginErr) {
            console.error(`[2025-05-06 17:16:28] Error beginning transaction:`, beginErr);
            rejectBegin(beginErr);
            return;
          }
          resolveBegin();
        });
      });

      // Use prepared statement for better performance
      const insertStmt = db.prepare(`
        INSERT INTO role_history (
          memberId, roleId, action, timestamp
        ) VALUES (?, ?, ?, ?)
      `);

      // Process each member sequentially
      for (const member of members) {
        if (!member || !member.roles || !member.roles.cache) {
          console.log(`[2025-05-06 17:16:28] Skipping invalid member`);
          continue;
        }

        try {
          // Get all the member's current roles
          const roles = Array.from(member.roles.cache.values());
          // Skip @everyone role
          const filteredRoles = roles.filter(role => role.id !== member.guild.id);

          if (filteredRoles.length === 0) {
            console.log(`[2025-05-06 17:16:28] Member ${member.user.username} has no roles to record (besides @everyone)`);
            continue;
          }

          // Add a role_history entry for each role with action "present"
          for (const role of filteredRoles) {
            try {
              // Check if there's already a "present" entry for this member and role
              const existingEntry = await new Promise((resolveCheck, rejectCheck) => {
                db.get(
                  `SELECT id FROM role_history WHERE memberId = ? AND roleId = ? AND action = 'present' LIMIT 1`,
                  [member.id, role.id],
                  (err, row) => {
                    if (err) {
                      console.error(`[2025-05-06 17:16:28] Error checking existing entries:`, err);
                      rejectCheck(err);
                      return;
                    }
                    resolveCheck(row);
                  }
                );
              });

              if (existingEntry) {
                // Skip this entry as it already exists
                skippedEntries++;
                continue;
              }

              await new Promise((resolveInsert, rejectInsert) => {
                insertStmt.run([
                  member.id,
                  role.id,
                  'present', // Action is "present" indicating role was present during export
                  currentTime
                ], function(insertErr) {
                  if (insertErr) {
                    console.error(`[2025-05-06 17:16:28] Error adding role history entry for ${member.user.username}, role ${role.name}:`, insertErr);
                    errorCount++;
                    rejectInsert(insertErr);
                    return;
                  }
                  addedEntries++;
                  resolveInsert();
                });
              });
            } catch (roleError) {
              console.error(`[2025-05-06 17:16:28] Error processing role ${role.name} for ${member.user.username}:`, roleError);
              // Continue with next role despite errors
            }
          }

          processedMembers++;
          
          // Log progress periodically
          if (processedMembers % 100 === 0) {
            console.log(`[2025-05-06 17:16:28] Processed ${processedMembers}/${members.length} members for "present" role entries...`);
          }
        } catch (memberError) {
          console.error(`[2025-05-06 17:16:28] Error processing member ${member?.id}:`, memberError);
          errorCount++;
          // Continue with next member despite errors
        }
      }

      // Finalize statement
      insertStmt.finalize();

      // Commit the transaction
      await new Promise((resolveCommit, rejectCommit) => {
        db.run('COMMIT', commitErr => {
          if (commitErr) {
            console.error(`[2025-05-06 17:16:28] Error committing transaction:`, commitErr);
            db.run('ROLLBACK'); // Rollback on error
            rejectCommit(commitErr);
            return;
          }
          resolveCommit();
        });
      });

      console.log(`[2025-05-06 17:16:28] Successfully added ${addedEntries} "present" role history entries for ${processedMembers} members (${skippedEntries} entries already existed)`);
      resolve({
        success: true,
        addedEntries,
        skippedEntries,
        processedMembers,
        errorCount
      });
    } catch (error) {
      // Rollback on error
      db.run('ROLLBACK');
      console.error(`[2025-05-06 17:16:28] Error processing "present" role entries:`, error);
      reject(error);
    }
  });
}

/**
 * Process member_roles table after exportguild command has fetched members
 * Adds entries to role_history with action "present" for each role
 * @param {Object} guild - Discord.js Guild object 
 * @returns {Promise<Object>} - Results of the operation
 */
async function processRolesFromMemberRolesTable(guild) {
  return new Promise(async (resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }

    const currentTime = Date.now();
    let addedEntries = 0;
    let skippedEntries = 0;
    let errorCount = 0;

    console.log(`[2025-05-06 17:16:28] Processing "present" role entries from member_roles table...`);

    try {
      // Begin transaction for better performance
      await new Promise((resolveBegin, rejectBegin) => {
        db.run('BEGIN TRANSACTION', function(beginErr) {
          if (beginErr) {
            console.error(`[2025-05-06 17:16:28] Error beginning transaction:`, beginErr);
            rejectBegin(beginErr);
            return;
          }
          resolveBegin();
        });
      });

      // Get all entries from member_roles table
      const memberRoles = await new Promise((resolveQuery, rejectQuery) => {
        db.all(`SELECT memberId, roleId, roleName FROM member_roles`, [], (err, rows) => {
          if (err) {
            console.error(`[2025-05-06 17:16:28] Error querying member_roles:`, err);
            rejectQuery(err);
            return;
          }
          resolveQuery(rows || []);
        });
      });

      console.log(`[2025-05-06 17:16:28] Found ${memberRoles.length} entries in member_roles table`);

      // Bulk check for existing entries in role_history to improve performance
      // Create a lookup map of existing memberId+roleId combinations with "present" action
      const existingLookup = new Map();
      
      const existingEntries = await new Promise((resolveCheck, rejectCheck) => {
        db.all(
          `SELECT memberId, roleId FROM role_history WHERE action = 'present'`,
          [],
          (err, rows) => {
            if (err) {
              console.error(`[2025-05-06 17:16:28] Error checking existing entries:`, err);
              rejectCheck(err);
              return;
            }
            resolveCheck(rows || []);
          }
        );
      });
      
      // Build lookup map for faster checking
      for (const entry of existingEntries) {
        const key = `${entry.memberId}-${entry.roleId}`;
        existingLookup.set(key, true);
      }
      
      console.log(`[2025-05-06 17:16:28] Found ${existingEntries.length} existing "present" entries in role_history`);

      // Use prepared statement for better performance
      const insertStmt = db.prepare(`
        INSERT INTO role_history (
          memberId, roleId, action, timestamp
        ) VALUES (?, ?, ?, ?)
      `);

      // Process all member_roles entries
      for (const entry of memberRoles) {
        try {
          // Check if there's already a "present" entry for this member and role
          const key = `${entry.memberId}-${entry.roleId}`;
          if (existingLookup.has(key)) {
            skippedEntries++;
            continue;
          }

          await new Promise((resolveInsert, rejectInsert) => {
            insertStmt.run([
              entry.memberId,
              entry.roleId,
              'present', // Action is "present" indicating role was present during export
              currentTime
            ], function(insertErr) {
              if (insertErr) {
                console.error(`[2025-05-06 17:16:28] Error adding role history entry for ${entry.memberId}, role ${entry.roleName}:`, insertErr);
                errorCount++;
                rejectInsert(insertErr);
                return;
              }
              addedEntries++;
              resolveInsert();
            });
          });

          // Log progress periodically
          if ((addedEntries + skippedEntries) % 1000 === 0) {
            console.log(`[2025-05-06 17:16:28] Processed ${addedEntries + skippedEntries}/${memberRoles.length} role entries (${addedEntries} added, ${skippedEntries} skipped)...`);
          }
        } catch (entryError) {
          console.error(`[2025-05-06 17:16:28] Error processing member_roles entry:`, entryError);
          errorCount++;
          // Continue with next entry despite errors
        }
      }

      // Finalize statement
      insertStmt.finalize();

      // Commit the transaction
      await new Promise((resolveCommit, rejectCommit) => {
        db.run('COMMIT', commitErr => {
          if (commitErr) {
            console.error(`[2025-05-06 17:16:28] Error committing transaction:`, commitErr);
            db.run('ROLLBACK'); // Rollback on error
            rejectCommit(commitErr);
            return;
          }
          resolveCommit();
        });
      });

      // Store metadata about the operation
      try {
        await monitor.storeGuildMetadata('role_present_entries_added', addedEntries.toString());
        await monitor.storeGuildMetadata('role_present_entries_skipped', skippedEntries.toString());
        await monitor.storeGuildMetadata('role_present_processed_at', new Date().toISOString());
        await monitor.storeGuildMetadata('role_present_processed_by', 'noname9006');
      } catch (metadataError) {
        console.error(`[2025-05-06 17:16:28] Error storing metadata:`, metadataError);
      }

      console.log(`[2025-05-06 17:16:28] Successfully added ${addedEntries} "present" role history entries from member_roles table (${skippedEntries} entries already existed)`);
      resolve({
        success: true,
        addedEntries,
        skippedEntries,
        errorCount
      });
    } catch (error) {
      // Rollback on error
      db.run('ROLLBACK');
      console.error(`[2025-05-06 17:16:28] Error processing "present" role entries from member_roles table:`, error);
      reject(error);
    }
  });
}

// Export functions
module.exports = {
  processCurrentRoles,
  processRolesFromMemberRolesTable
};