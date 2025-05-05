// channel-monitor.js - Module to monitor channel creation and deletion events
const { ChannelType, AuditLogEvent } = require('discord.js');
const config = require('./config');
const monitor = require('./monitor');

/**
 * Initialize channel monitoring
 * @param {Client} client - Discord client
 */
function initializeChannelMonitoring(client) {
  console.log(`[${getFormattedDateTime()}] Channel monitoring initialized`);

  // Listen for channel creation events
  client.on('channelCreate', async (channel) => {
    try {
      // Only process channels in guilds (ignore DMs)
      if (!channel.guild) return;

      // Only process text-based channels
      if (!isTextBasedChannel(channel)) {
        console.log(`[${getFormattedDateTime()}] Ignoring new non-text channel: ${channel.name} (${channel.id})`);
        return;
      }

      // Check if channel is excluded
      if (config.excludedChannels.includes(channel.id)) {
        console.log(`[${getFormattedDateTime()}] Ignoring excluded channel: ${channel.name} (${channel.id})`);
        return;
      }

      // Check if bot has access to channel
      if (!canAccessChannel(channel)) {
        console.log(`[${getFormattedDateTime()}] Cannot access new channel: ${channel.name} (${channel.id})`);
        return;
      }

      console.log(`[${getFormattedDateTime()}] New channel detected: ${channel.name} (${channel.id})`);
      console.log(`Channel type: ${channel.type}, Parent ID: ${channel.parentId || 'None'}`);

      // Only process if a database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(channel.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping new channel tracking - no database for guild ${channel.guild.name}`);
        return;
      }

      // Add channel to monitoring - explicitly pass the full channel object
      await monitor.markChannelFetchingStarted(channel.id, channel.name, channel);
      await monitor.markChannelFetchingCompleted(channel.id);
      console.log(`[${getFormattedDateTime()}] Added new channel to monitoring: ${channel.name} (${channel.id})`);

      // Optionally send a notification to a designated log channel
      await sendNotification(client, channel, 'added');

    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing new channel:`, error);
    }
  });

  // Listen for channel deletion events
  client.on('channelDelete', async (channel) => {
    try {
      // Only process channels in guilds (ignore DMs)
      if (!channel.guild) return;

      console.log(`[${getFormattedDateTime()}] Channel deleted: ${channel.name} (${channel.id})`);
      
      // Mark the channel as deleted in the database
      await markChannelAsDeleted(channel.id, channel.name);
      
      // Optionally send a notification to a designated log channel
      await sendNotification(client, channel, 'deleted');
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing deleted channel:`, error);
    }
  });

  // Listen for thread creation events
  client.on('threadCreate', async (thread) => {
    try {
      // Check if thread is excluded
      if (config.excludedChannels.includes(thread.id)) {
        console.log(`[${getFormattedDateTime()}] Ignoring excluded thread: ${thread.name} (${thread.id})`);
        return;
      }

      // Check if bot has access to thread
      if (!canAccessChannel(thread)) {
        console.log(`[${getFormattedDateTime()}] Cannot access new thread: ${thread.name} (${thread.id})`);
        return;
      }

      console.log(`[${getFormattedDateTime()}] New thread detected: ${thread.name} (${thread.id})`);
      console.log(`Thread type: ${thread.type}, Parent ID: ${thread.parentId || 'None'}`);

      // Only process if a database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(thread.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping new thread tracking - no database for guild ${thread.guild.name}`);
        return;
      }

      // Add thread to monitoring - explicitly pass the full thread object
      await monitor.markChannelFetchingStarted(thread.id, thread.name, thread);
      await monitor.markChannelFetchingCompleted(thread.id);
      console.log(`[${getFormattedDateTime()}] Added new thread to monitoring: ${thread.name} (${thread.id})`);

      // Optionally send a notification to a designated log channel
      await sendNotification(client, thread, 'added', true);

    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing new thread:`, error);
    }
  });

  // NEW: Listen for channel update events
  client.on('channelUpdate', async (oldChannel, newChannel) => {
    try {
      // Only process channels in guilds (ignore DMs)
      if (!newChannel.guild) return;

      // Only process text-based channels
      if (!isTextBasedChannel(newChannel)) {
        console.log(`[${getFormattedDateTime()}] Ignoring updated non-text channel: ${newChannel.name} (${newChannel.id})`);
        return;
      }

      // Check if channel is excluded
      if (config.excludedChannels.includes(newChannel.id)) {
        console.log(`[${getFormattedDateTime()}] Ignoring excluded channel update: ${newChannel.name} (${newChannel.id})`);
        return;
      }

      console.log(`[${getFormattedDateTime()}] Channel updated: ${newChannel.name} (${newChannel.id})`);
      
      // Detect what changed
      const changes = detectChannelChanges(oldChannel, newChannel);
      
      if (Object.keys(changes).length > 0) {
        // Log the changes
        console.log(`[${getFormattedDateTime()}] Changes detected for channel ${newChannel.id}:`, JSON.stringify(changes));
        
        // Update channel in the database using the existing schema
        await updateChannelInDatabaseSafe(newChannel, changes);
        
        // Optionally send a notification to a designated log channel
        await sendUpdateNotification(client, oldChannel, newChannel, changes);
      }
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing channel update:`, error);
    }
  });

  // NEW: Listen for thread update events
  client.on('threadUpdate', async (oldThread, newThread) => {
    try {
      // Check if thread is excluded
      if (config.excludedChannels.includes(newThread.id)) {
        console.log(`[${getFormattedDateTime()}] Ignoring excluded thread update: ${newThread.name} (${newThread.id})`);
        return;
      }

      console.log(`[${getFormattedDateTime()}] Thread updated: ${newThread.name} (${newThread.id})`);
      
      // Detect what changed
      const changes = detectChannelChanges(oldThread, newThread);
      
      if (Object.keys(changes).length > 0) {
        // Log the changes
        console.log(`[${getFormattedDateTime()}] Changes detected for thread ${newThread.id}:`, JSON.stringify(changes));
        
        // Update thread in the database using the existing schema
        await updateChannelInDatabaseSafe(newThread, changes);
        
        // Optionally send a notification to a designated log channel
        await sendUpdateNotification(client, oldThread, newThread, changes, true);
      }
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing thread update:`, error);
    }
  });

  // NEW: Listen for thread delete events
  client.on('threadDelete', async (thread) => {
    try {
      // Only process threads in guilds
      if (!thread.guild) return;

      console.log(`[${getFormattedDateTime()}] Thread deleted: ${thread.name} (${thread.id})`);
      
      // Mark the thread as deleted in the database
      await markChannelAsDeleted(thread.id, thread.name);
      
      // Optionally send a notification to a designated log channel
      await sendNotification(client, thread, 'deleted', true);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing deleted thread:`, error);
    }
  });

  // NEW: Listen for thread list sync events (when client gains access to more threads)
  client.on('threadListSync', async (threads) => {
    try {
      console.log(`[${getFormattedDateTime()}] Thread list sync received with ${threads.length} threads`);
      
      for (const thread of threads) {
        // Skip if this thread is already in the database
        const exists = await checkChannelExistsInDatabase(thread.id);
        if (exists) {
          console.log(`[${getFormattedDateTime()}] Thread already tracked: ${thread.name} (${thread.id})`);
          continue;
        }
        
        // Check if thread is excluded
        if (config.excludedChannels.includes(thread.id)) {
          console.log(`[${getFormattedDateTime()}] Ignoring excluded thread from sync: ${thread.name} (${thread.id})`);
          continue;
        }
        
        // Check if bot has access to thread
        if (!canAccessChannel(thread)) {
          console.log(`[${getFormattedDateTime()}] Cannot access synced thread: ${thread.name} (${thread.id})`);
          continue;
        }
        
        // Only process if a database is initialized for this guild
        const dbExists = monitor.checkDatabaseExists(thread.guild);
        if (!dbExists) {
          console.log(`[${getFormattedDateTime()}] Skipping synced thread tracking - no database for guild ${thread.guild.name}`);
          continue;
        }
        
        // Add thread to monitoring
        await monitor.markChannelFetchingStarted(thread.id, thread.name, thread);
        await monitor.markChannelFetchingCompleted(thread.id);
        console.log(`[${getFormattedDateTime()}] Added synced thread to monitoring: ${thread.name} (${thread.id})`);
      }
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing thread list sync:`, error);
    }
  });
  
  // NEW: Listen for channel pins update events
  client.on('channelPinsUpdate', async (channel, date) => {
    try {
      // Only process channels in guilds (ignore DMs)
      if (!channel.guild) return;
      
      // Check if channel is excluded
      if (config.excludedChannels.includes(channel.id)) {
        console.log(`[${getFormattedDateTime()}] Ignoring pins update in excluded channel: ${channel.name} (${channel.id})`);
        return;
      }
      
      console.log(`[${getFormattedDateTime()}] Pins updated in channel: ${channel.name} (${channel.id}) at ${date}`);
      
      // Just log the event instead of trying to update the database
      // We'll add proper database support for this in a future update
      console.log(`[${getFormattedDateTime()}] Pin timestamp update for channel ${channel.id}: ${date.toISOString()}`);
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing channel pins update:`, error);
    }
  });
}

/**
 * Detect what changed between old and new channel
 * @param {Channel} oldChannel - The old channel state
 * @param {Channel} newChannel - The new channel state
 * @returns {Object} Object containing the changes
 */
function detectChannelChanges(oldChannel, newChannel) {
  const changes = {};
  
  // Check for name change
  if (oldChannel.name !== newChannel.name) {
    changes.name = {
      old: oldChannel.name,
      new: newChannel.name
    };
  }
  
  // Check for parent (category) change
  if (oldChannel.parentId !== newChannel.parentId) {
    changes.parentId = {
      old: oldChannel.parentId,
      new: newChannel.parentId
    };
  }
  
  // Check for permission overwrites changes
  const oldPermOverwrites = oldChannel.permissionOverwrites?.cache;
  const newPermOverwrites = newChannel.permissionOverwrites?.cache;
  
  if (oldPermOverwrites && newPermOverwrites) {
    if (oldPermOverwrites.size !== newPermOverwrites.size) {
      changes.permissions = {
        description: 'Permissions were modified'
      };
    } else {
      // Check if any of the overwrites changed
      let permissionsChanged = false;
      
      newPermOverwrites.forEach((overwrite, id) => {
        const oldOverwrite = oldPermOverwrites.get(id);
        if (!oldOverwrite || 
            oldOverwrite.allow.bitfield !== overwrite.allow.bitfield || 
            oldOverwrite.deny.bitfield !== overwrite.deny.bitfield) {
          permissionsChanged = true;
        }
      });
      
      if (permissionsChanged) {
        changes.permissions = {
          description: 'Permissions were modified'
        };
      }
    }
  }
  
  // For thread-specific properties
  if (oldChannel.threadMetadata && newChannel.threadMetadata) {
    // Check for archive state change
    if (oldChannel.threadMetadata.archived !== newChannel.threadMetadata.archived) {
      changes.archived = {
        old: oldChannel.threadMetadata.archived,
        new: newChannel.threadMetadata.archived
      };
    }
    
    // Check for auto-archive duration change
    if (oldChannel.threadMetadata.autoArchiveDuration !== newChannel.threadMetadata.autoArchiveDuration) {
      changes.autoArchiveDuration = {
        old: oldChannel.threadMetadata.autoArchiveDuration,
        new: newChannel.threadMetadata.autoArchiveDuration
      };
    }
  }
  
  return changes;
}

/**
 * Update channel in database when it changes, using only existing columns
 * This is a safe version that handles databases that don't have the new columns yet
 * @param {Channel} channel - The updated channel
 * @param {Object} changes - The detected changes
 * @returns {Promise<boolean>} Whether the update was successful
 */
async function updateChannelInDatabaseSafe(channel, changes) {
  try {
    const db = monitor.getDatabase();
    if (!db) {
      console.log(`No database available to update channel ${channel.name} (${channel.id})`);
      return false;
    }
    
    return new Promise((resolve, reject) => {
      // First get the current record
      const selectSql = `SELECT * FROM channels WHERE id = ?`;
      
      db.get(selectSql, [channel.id], (err, row) => {
        if (err) {
          console.error(`Error retrieving channel ${channel.id} from database:`, err);
          reject(err);
          return;
        }
        
        if (!row) {
          console.log(`Channel ${channel.id} not found in database, creating new record`);
          // Channel doesn't exist in database, create it
          monitor.markChannelFetchingStarted(channel.id, channel.name, channel)
            .then(() => monitor.markChannelFetchingCompleted(channel.id))
            .then(() => resolve(true))
            .catch(reject);
          return;
        }
        
        // Check if we need to update the name
        if (changes.name && row.name !== changes.name.new) {
          // Simple name update
          const updateNameSql = `UPDATE channels SET name = ? WHERE id = ?`;
          db.run(updateNameSql, [changes.name.new, channel.id], function(err) {
            if (err) {
              console.error(`Error updating channel name for ${channel.id}:`, err);
              reject(err);
              return;
            }
            
            console.log(`Updated name for channel ${channel.id} to "${changes.name.new}"`);
          });
        }
        
        // If parent ID changed, update it based on whether it's a thread or channel
        if (changes.parentId) {
          const isThread = (
            channel.type === ChannelType.PublicThread || 
            channel.type === ChannelType.PrivateThread ||
            channel.isThread === true
          );
          
          let parentChannelId = null;
          let parentCatId = null;
          
          if (isThread) {
            parentChannelId = changes.parentId.new;
            
            // Try to get the category from the parent channel
            const parentChannel = channel.guild.channels.cache.get(changes.parentId.new);
            if (parentChannel && parentChannel.parent) {
              parentCatId = parentChannel.parent.id;
            }
          } else if (changes.parentId.new) {
            // For regular channels, parentId is the category
            parentCatId = changes.parentId.new;
          }
          
          if (parentChannelId !== null || parentCatId !== null) {
            let updateParentSql = 'UPDATE channels SET ';
            const params = [];
            
            if (parentChannelId !== null) {
              updateParentSql += 'parentChannelId = ?';
              params.push(parentChannelId);
              
              if (parentCatId !== null) {
                updateParentSql += ', parentCatId = ?';
                params.push(parentCatId);
              }
            } else if (parentCatId !== null) {
              updateParentSql += 'parentCatId = ?';
              params.push(parentCatId);
            }
            
            updateParentSql += ' WHERE id = ?';
            params.push(channel.id);
            
            db.run(updateParentSql, params, function(err) {
              if (err) {
                console.error(`Error updating channel parent for ${channel.id}:`, err);
                // Don't reject here, just log the error
                console.log(`Update parent SQL: ${updateParentSql}`);
                console.log(`Update parent params: ${JSON.stringify(params)}`);
              } else {
                console.log(`Updated parent for channel ${channel.id} (parentChannelId: ${parentChannelId}, parentCatId: ${parentCatId})`);
              }
            });
          }
        }
        
        // Custom fields for thread metadata
        if (changes.archived !== undefined || changes.autoArchiveDuration !== undefined) {
          // Store these in the database notes field as JSON if possible
          try {
            // Check if notes column exists
            const checkNotesSql = `PRAGMA table_info(channels)`;
            db.all(checkNotesSql, (err, rows) => {
              if (err) {
                console.error(`Error checking for notes column:`, err);
                return;
              }
              
              const hasNotesColumn = rows.some(row => row.name === 'notes');
              if (hasNotesColumn) {
                // First get the current notes
                db.get(`SELECT notes FROM channels WHERE id = ?`, [channel.id], (err, row) => {
                  if (err) {
                    console.error(`Error retrieving notes for ${channel.id}:`, err);
                    return;
                  }
                  
                  let notes = {};
                  if (row && row.notes) {
                    try {
                      notes = JSON.parse(row.notes);
                    } catch (e) {
                      // If not valid JSON, start fresh
                      notes = {};
                    }
                  }
                  
                  // Update with thread metadata
                  if (!notes.threadMetadata) {
                    notes.threadMetadata = {};
                  }
                  
                  if (changes.archived !== undefined) {
                    notes.threadMetadata.archived = changes.archived.new;
                  }
                  
                  if (changes.autoArchiveDuration !== undefined) {
                    notes.threadMetadata.autoArchiveDuration = changes.autoArchiveDuration.new;
                  }
                  
                  // Save updated notes
                  const updateNotesSql = `UPDATE channels SET notes = ? WHERE id = ?`;
                  db.run(updateNotesSql, [JSON.stringify(notes), channel.id], function(err) {
                    if (err) {
                      console.error(`Error updating notes for ${channel.id}:`, err);
                    } else {
                      console.log(`Updated thread metadata in notes for ${channel.id}`);
                    }
                  });
                });
              } else {
                console.log(`No notes column found, skipping thread metadata storage`);
              }
            });
          } catch (e) {
            console.error(`Error handling thread metadata:`, e);
          }
        }
        
        resolve(true);
      });
    });
  } catch (error) {
    console.error(`Error in updateChannelInDatabaseSafe for ${channel.id}:`, error);
    return false;
  }
}

/**
 * Check if a channel exists in the database
 * @param {string} channelId - The channel ID
 * @returns {Promise<boolean>} Whether the channel exists
 */
async function checkChannelExistsInDatabase(channelId) {
  try {
    const db = monitor.getDatabase();
    if (!db) {
      console.log(`No database available to check if channel ${channelId} exists`);
      return false;
    }
    
    return new Promise((resolve, reject) => {
      const sql = `SELECT id FROM channels WHERE id = ?`;
      
      db.get(sql, [channelId], (err, row) => {
        if (err) {
          console.error(`Error checking if channel ${channelId} exists:`, err);
          reject(err);
          return;
        }
        
        resolve(!!row);
      });
    });
  } catch (error) {
    console.error(`Error in checkChannelExistsInDatabase for ${channelId}:`, error);
    return false;
  }
}

async function markChannelAsDeleted(channelId, channelName) {
  try {
    const db = monitor.getDatabase();
    if (!db) {
      console.log(`No database available to mark channel ${channelName} (${channelId}) as deleted`);
      return false;
    }
    
    return new Promise((resolve, reject) => {
      // Update the channel record to mark it as deleted
      const sql = `
        UPDATE channels 
        SET deleted = 1, deletedAt = ? 
        WHERE id = ?
      `;
      
      db.run(sql, [Date.now(), channelId], function(err) {
        if (err) {
          console.error(`Error marking channel ${channelId} as deleted:`, err);
          reject(err);
          return;
        }
        
        if (this.changes > 0) {
          console.log(`Marked channel ${channelName} (${channelId}) as deleted in database`);
          resolve(true);
        } else {
          console.log(`Channel ${channelId} not found in database or already marked as deleted`);
          resolve(false);
        }
      });
    });
  } catch (error) {
    console.error(`Error in markChannelAsDeleted for ${channelId}:`, error);
    return false;
  }
}

/**
 * Check if a channel is text-based and suitable for monitoring
 * @param {Channel} channel - The Discord channel object
 * @returns {boolean} Whether channel is text-based
 */
function isTextBasedChannel(channel) {
  return channel.type === ChannelType.GuildText || 
         channel.type === ChannelType.GuildForum ||
         channel.type === ChannelType.PublicThread ||
         channel.type === ChannelType.PrivateThread;
}

/**
 * Check if the bot can access the channel
 * @param {Channel} channel - The Discord channel object
 * @returns {boolean} Whether bot can access channel
 */
function canAccessChannel(channel) {
  // Skip if channel isn't viewable
  if (!channel.viewable) return false;

  // Check for message history permission
  const permissions = channel.permissionsFor(channel.guild.members.me);
  if (!permissions || !permissions.has('ReadMessageHistory')) return false;

  return true;
}

/**
 * Send notification about channel changes to a log channel
 * @param {Client} client - Discord client
 * @param {Channel} channel - The affected channel
 * @param {string} action - The action performed (added/deleted)
 * @param {boolean} isThread - Whether the channel is a thread
 */
async function sendNotification(client, channel, action, isThread = false) {
  // Check if log channel is configured
  const logChannelId = config.getConfig('channelMonitorLogChannel', 'CHANNEL_MONITOR_LOG');
  if (!logChannelId) return;

  try {
    const logChannel = await client.channels.fetch(logChannelId);
    if (!logChannel) return;

    const channelType = isThread ? 'Thread' : 'Channel';
    const emoji = action === 'added' ? '➕' : '🗑️';
    const color = action === 'added' ? '#00FF00' : '#FF0000';
    
    let parentInfo = '';
    if (isThread && channel.parent) {
      parentInfo = `\n• Parent Channel: <#${channel.parentId}> (${channel.parent.name})`;
    } else if (channel.parent) {
      parentInfo = `\n• Category: ${channel.parent.name}`;
    }

    await logChannel.send({
      embeds: [{
        title: `${emoji} ${channelType} ${action === 'added' ? 'Created' : 'Deleted'}`,
        description: `${channelType} has been ${action} and ${action === 'added' ? 'added to' : 'removed from'} monitoring.`,
        color: color,
        fields: [
          { name: 'Name', value: channel.name, inline: true },
          { name: 'ID', value: channel.id, inline: true },
          { name: 'Guild', value: channel.guild.name, inline: true }
        ],
        footer: { text: `Time: ${getFormattedDateTime()}` }
      }]
    });
  } catch (error) {
    console.error(`[${getFormattedDateTime()}] Error sending notification:`, error);
  }
}

/**
 * Send notification about channel updates to a log channel
 * @param {Client} client - Discord client
 * @param {Channel} oldChannel - The channel before update
 * @param {Channel} newChannel - The channel after update
 * @param {Object} changes - The detected changes
 * @param {boolean} isThread - Whether the channel is a thread
 */
async function sendUpdateNotification(client, oldChannel, newChannel, changes, isThread = false) {
  // Check if log channel is configured
  const logChannelId = config.getConfig('channelMonitorLogChannel', 'CHANNEL_MONITOR_LOG');
  if (!logChannelId) return;

  try {
    const logChannel = await client.channels.fetch(logChannelId);
    if (!logChannel) return;

    const channelType = isThread ? 'Thread' : 'Channel';
    const emoji = '🔄';
    const color = '#FFAA00';
    
    // Build the fields array based on what changed
    const fields = [
      { name: 'Name', value: newChannel.name, inline: true },
      { name: 'ID', value: newChannel.id, inline: true },
      { name: 'Guild', value: newChannel.guild.name, inline: true }
    ];
    
    // Add fields for each changed property
    if (changes.name) {
      fields.push({ 
        name: 'Name Changed', 
        value: `From: ${changes.name.old}\nTo: ${changes.name.new}`,
        inline: false 
      });
    }
    
    if (changes.parentId) {
      // For parent ID changes, try to resolve names
      let oldParentName = changes.parentId.old || 'None';
      let newParentName = changes.parentId.new || 'None';
      
      try {
        if (changes.parentId.old) {
          const oldParent = await newChannel.guild.channels.fetch(changes.parentId.old).catch(() => null);
          if (oldParent) oldParentName = oldParent.name;
        }
        
        if (changes.parentId.new) {
          const newParent = await newChannel.guild.channels.fetch(changes.parentId.new).catch(() => null);
          if (newParent) newParentName = newParent.name;
        }
      } catch (error) {
        // Ignore errors in resolving names
      }
      
      fields.push({ 
        name: isThread ? 'Parent Channel Changed' : 'Category Changed',
        value: `From: ${oldParentName}\nTo: ${newParentName}`, 
        inline: false 
      });
    }
    
    if (changes.permissions) {
      fields.push({ 
        name: 'Permissions Changed',
        value: changes.permissions.description,
        inline: false 
      });
    }
    
    if (changes.archived !== undefined) {
      fields.push({ 
        name: 'Archive Status Changed',
        value: changes.archived.new ? 'Thread was archived' : 'Thread was unarchived',
        inline: false 
      });
    }
    
    if (changes.autoArchiveDuration !== undefined) {
      fields.push({ 
        name: 'Auto Archive Duration Changed',
        value: `From: ${changes.autoArchiveDuration.old} minutes\nTo: ${changes.autoArchiveDuration.new} minutes`,
        inline: false 
      });
    }

    await logChannel.send({
      embeds: [{
        title: `${emoji} ${channelType} Updated`,
        description: `${channelType} has been updated and changes have been recorded in the database.`,
        color: color,
        fields: fields,
        footer: { text: `Time: ${getFormattedDateTime()}` }
      }]
    });
  } catch (error) {
    console.error(`[${getFormattedDateTime()}] Error sending update notification:`, error);
  }
}

/**
 * Format date and time for logs (UTC, YYYY-MM-DD HH:MM:SS format)
 * @returns {string} Formatted date time string
 */
function getFormattedDateTime() {
  const now = new Date();
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

module.exports = {
  initializeChannelMonitoring,
  markChannelAsDeleted,
  checkChannelExistsInDatabase // Export this for potential use in other modules
};