// member-left.js - Module to process left members based on message history
const monitor = require('./monitor');
const sqlite3 = require('sqlite3').verbose(); // Adding .verbose() for better error reporting

// Cache for roles to avoid repeated lookups
const roleCache = new Map();

// Prepared statements
let addMemberStmt = null;
let addRoleStmt = null;

/**
 * Identifies users from messages who are not present in the guild anymore
 * and adds them to the guild_members table with appropriate timestamps
 * @param {Object} guild - The Discord guild object
 * @param {Number} batchSize - Number of members to process in parallel (default: 100)
 * @param {Boolean} skipRoles - Skip role processing entirely (default: true)
 */
async function processLeftMembers(guild, batchSize = 100, skipRoles = true) {
    console.log(`[${getFormattedDateTime()}] Processing left members for guild ${guild.name} (${guild.id})`);
    const db = monitor.getDatabase();
    if (!db) {
        console.error("Cannot process left members: Database not initialized");
        return { success: false, error: "Database not initialized" };
    }
    try {
        // First ensure all required tables exist
        await ensureTablesExist(db);

        // Prepare statements for reuse
        prepareStatements(db);

        // Add required indexes if they don't exist
        await ensureDatabaseIndexes(db);

        // Get all message authors not in guild_members
        const startTime = Date.now();
        const missingMembers = await findMissingMembers(db);
        console.log(`[${getFormattedDateTime()}] Found ${missingMembers.length} users in messages who might have left the guild (query took ${Date.now() - startTime}ms)`);

        // Process in larger batches
        let addedCount = 0;
        let processedCount = 0;
        let errorCount = 0;
        const currentTime = Date.now();

        // Process in chunks of batchSize
        for (let i = 0; i < missingMembers.length; i += batchSize) {
            const batchStartTime = Date.now();
            const memberBatch = missingMembers.slice(i, i + batchSize);
            const memberIds = memberBatch.map(m => m.authorId);

            console.log(`[${getFormattedDateTime()}] Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(missingMembers.length / batchSize)} (${memberBatch.length} members)`);

            try {
                // Begin transaction
                await beginTransaction(db);

                // Preload all timestamp data in a single query
                const timestampsMap = await preloadMemberTimestamps(db, memberIds);

                // Prepare members for bulk insert
                const membersToAdd = [];
                for (const member of memberBatch) {
                    const timestamps = timestampsMap[member.authorId];
                    if (!timestamps) continue;
                    const memberInfo = {
                        id: member.authorId,
                        username: member.authorUsername,
                        joinedTimestamp: timestamps.firstMessageTime,
                        leftTimestamp: timestamps.lastMessageTime
                    };
                    membersToAdd.push(memberInfo);
                }

                // Bulk add members
                const result = await addLeftMembersToBulk(db, membersToAdd, currentTime);
                addedCount += result;
                processedCount += memberBatch.length;

                // Commit transaction
                await commitTransaction(db);

                const batchTime = Date.now() - batchStartTime;
                console.log(`[${getFormattedDateTime()}] Batch completed in ${batchTime}ms: ${result}/${memberBatch.length} members added successfully (${(batchTime / memberBatch.length).toFixed(2)}ms)`);
            } catch (batchError) {
                // Rollback on error
                await rollbackTransaction(db);
                console.error(`[${getFormattedDateTime()}] Batch failed, rolled back:`, batchError);
                errorCount += memberBatch.length;
            }
        }

        console.log(`[${getFormattedDateTime()}] Added ${addedCount}/${processedCount} left members to database (${errorCount} errors) in ${(Date.now() - startTime)/1000} seconds`);
        return { success: true, addedCount, errorCount };
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error processing left members:`, error);
        return { success: false, error: error.message };
    } finally {
        // Clean up prepared statements
        finalizeStatements();
    }
}

/**
 * Ensures required tables exist in the database
 * @param {Object} db - Database connection
 * @returns {Promise<Boolean>} Whether tables were created or already exist
 */
async function ensureTablesExist(db) {
    return new Promise((resolve, reject) => {
        // Check if the required tables exist
        db.get("SELECT COUNT(*) as tableCount FROM sqlite_master WHERE type='table' AND name IN ('guild_members', 'member_roles', 'role_history')", (err, row) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error checking for tables:`, err);
                reject(err);
                return;
            }
            // If all three tables exist, we're good
            if (row && row.tableCount === 3) {
                resolve(true);
                return;
            }
            // Import the necessary table creation function from member-tracker
            try {
                const memberTracker = require('./member-tracker');
                memberTracker.initializeMemberDatabase(db)
                    .then(() => {
                        console.log(`[${getFormattedDateTime()}] Member tracking tables created successfully`);
                        resolve(true);
                    })
                    .catch(error => {
                        console.error(`[${getFormattedDateTime()}] Error creating member tracking tables:`, error);
                        reject(error);
                    });
            } catch (error) {
                console.error(`[${getFormattedDateTime()}] Error importing member-tracker module:`, error);
                reject(error);
            }
        });
    });
}

/**
 * Prepare SQLite statements for reuse
 */
function prepareStatements(db) {
    try {
        if (addMemberStmt === null) {
            addMemberStmt = db.prepare(`
                INSERT OR REPLACE INTO guild_members (
                    id, username, avatarURL, joinedAt, joinedTimestamp,
                    bot, lastUpdated, leftGuild, leftTimestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);
        }
        if (addRoleStmt === null) {
            addRoleStmt = db.prepare(`
                INSERT OR IGNORE INTO member_roles (
                    memberId, roleId, roleName, addedAt
                ) VALUES (?, ?, ?, ?)
            `);
        }
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error preparing statements:`, error);
    }
}

/**
 * Clean up prepared statements
 */
function finalizeStatements() {
    try {
        if (addMemberStmt) {
            addMemberStmt.finalize();
            addMemberStmt = null;
        }
        if (addRoleStmt) {
            addRoleStmt.finalize();
            addRoleStmt = null;
        }
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error finalizing statements:`, error);
    }
}

/**
 * Ensures necessary database indexes exist
 * @param {Object} db - Database connection
 */
async function ensureDatabaseIndexes(db) {
    return new Promise((resolve, reject) => {
        // First ensure the message author index exists
        db.run(`CREATE INDEX IF NOT EXISTS idx_messages_author_id ON messages(authorId)`, (err) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error creating messages index:`, err);
            }

            // Then add index on leftGuild for faster queries
            db.run(`CREATE INDEX IF NOT EXISTS idx_guild_members_left ON guild_members(leftGuild)`, (err) => {
                if (err) {
                    console.error(`[${getFormattedDateTime()}] Error creating guild_members index:`, err);
                }

                // Create index on role_history if it doesn't exist
                db.run(`CREATE INDEX IF NOT EXISTS idx_role_history_member_id ON role_history(memberId)`, (err) => {
                    if (err) {
                        console.error(`[${getFormattedDateTime()}] Error creating role_history index:`, err);
                    }

                    // Create index on member_roles if it doesn't exist
                    db.run(`CREATE INDEX IF NOT EXISTS idx_member_roles_member_id ON member_roles(memberId)`, (err) => {
                        if (err) {
                            console.error(`[${getFormattedDateTime()}] Error creating member_roles index:`, err);
                        }
                        resolve();
                    });
                });
            });
        });
    });
}

/**
 * Preload member message timestamps in a single query for a batch
 * @param {Object} db - Database connection
 * @param {Array} memberIds - Array of member IDs
 * @returns {Object} Map of member IDs to timestamp objects
 */
async function preloadMemberTimestamps(db, memberIds) {
    return new Promise((resolve, reject) => {
        if (!memberIds.length) {
            resolve({});
            return;
        }
        const placeholders = memberIds.map(() => '?').join(',');
        const sql = `
            SELECT authorId,
                   MIN(timestamp) as firstMessageTime,
                   MAX(timestamp) as lastMessageTime
            FROM messages
            WHERE authorId IN (${placeholders})
            GROUP BY authorId
        `;
        db.all(sql, memberIds, (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error preloading timestamps:`, err);
                resolve({});
                return;
            }
            const result = {};
            rows.forEach(row => {
                result[row.authorId] = {
                    firstMessageTime: row.firstMessageTime,
                    lastMessageTime: row.lastMessageTime
                };
            });
            resolve(result);
        });
    });
}

/**
 * Add multiple left members to the database in a single operation
 * @param {Object} db - Database connection
 * @param {Array} members - Array of member objects with id, username, etc.
 * @param {Number} currentTime - Current timestamp
 * @returns {Promise<Number>} Number of members added
 */
function addLeftMembersToBulk(db, members, currentTime) {
    return new Promise((resolve, reject) => {
        if (!members || members.length === 0) {
            resolve(0);
            return;
        }

        // Create a single SQL statement with multiple VALUES clauses
        let sql = `
            INSERT OR REPLACE INTO guild_members 
            (id, username, avatarURL, joinedAt, joinedTimestamp, bot, lastUpdated, leftGuild, leftTimestamp)
            VALUES `;
        const values = [];
        const params = [];

        members.forEach(member => {
            const joinedAt = member.joinedTimestamp ? new Date(member.joinedTimestamp).toISOString() : null;
            const leftTimestamp = member.leftTimestamp || currentTime;
            values.push(`(?, ?, ?, ?, ?, ?, ?, ?, ?)`);
            params.push(
                member.id,
                member.username,
                null,
                joinedAt,
                member.joinedTimestamp,
                0,
                currentTime,
                1,
                leftTimestamp
            );
        });

        sql += values.join(', ');

        db.run(sql, params, function(err) {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error bulk adding ${members.length} left members:`, err);
                reject(err);
                return;
            }

            // Log added members in batches to reduce console output
            if (members.length > 20) {
                console.log(`[${getFormattedDateTime()}] Bulk added ${this.changes} left members to database`);
            } else {
                members.forEach(m => {
                    const joinedAt = m.joinedTimestamp ? new Date(m.joinedTimestamp).toISOString() : null;
                    const leftAt = m.leftTimestamp ? new Date(m.leftTimestamp).toISOString() : new Date(currentTime).toISOString();
                    console.log(`[${getFormattedDateTime()}] Added left member ${m.username} (${m.id}) to database (estimated join: ${joinedAt}, left: ${leftAt})`);
                });
            }

            resolve(this.changes || members.length);
        });
    });
}

/**
 * Finds message authors who aren't in the guild_members table
 * @param {Object} db - Database connection
 * @returns {Array} Array of member objects with authorId and authorUsername
 */
function findMissingMembers(db) {
    return new Promise((resolve, reject) => {
        // Optimized query with limit and filter for non-bot users
        const sql = `
            SELECT DISTINCT authorId, authorUsername, authorBot
            FROM messages
            WHERE authorBot = 0
            AND authorId NOT IN (
                SELECT id FROM guild_members
            )
            LIMIT 10000
        `;
        db.all(sql, [], (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error finding missing members:`, err);
                reject(err);
                return;
            }
            resolve(rows);
        });
    });
}

/**
 * Begins a database transaction
 * @param {Object} db - Database connection
 */
function beginTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run('BEGIN TRANSACTION', (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

/**
 * Commits a database transaction
 * @param {Object} db - Database connection
 */
function commitTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run('COMMIT', (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

/**
 * Rolls back a database transaction
 * @param {Object} db - Database connection
 */
function rollbackTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run('ROLLBACK', (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

/**
 * Utility function to get formatted date-time string
 * @returns {String} Formatted date-time string
 */
function getFormattedDateTime() {
    const now = new Date();
    return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

/**
 * For backward compatibility - simple wrapper around processLeftMembers
 * @deprecated Use processLeftMembers with explicit parameters instead
 * @param {Object} guild - The Discord guild object
 * @returns {Promise<Object>} Processing result
 */
async function processLeftMembersLegacy(guild) {
    return processLeftMembers(guild, 100, true); // Changed to true to always skip role processing
}

// Export the module functions
module.exports = {
    processLeftMembers,
    processLeftMembersLegacy,
    ensureTablesExist
};