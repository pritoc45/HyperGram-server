import crypto from 'crypto';
import { MSG } from '../../shared/protocol.js';
import { hashPassword, generateToken, genId } from '../utils/helpers.js';
import { users, history, groups, persist } from '../storage/db.js';

let wss = null;
const activeSockets = {};
const sessions = {};
const uploads = {};
const MAX_MEDIA = 80 * 1024 * 1024;
const HISTORY_CAP = 300;

export function init(server) {
  wss = server;
}

export function getActiveSockets() {
  return activeSockets;
}

export function handleConnection(ws) {
  let myUsername = null;

  ws.on('message', (message) => {
    try {
      const p = JSON.parse(message.toString());
      handleMessage(ws, p, myUsername, (name) => { myUsername = name; });
    } catch (err) {
      console.error('Parse error:', err.message);
    }
  });

  ws.on('close', () => {
    if (myUsername) {
      const arr = activeSockets[myUsername] || [];
      const i = arr.indexOf(ws);
      if (i >= 0) arr.splice(i, 1);
      if (!arr.length) {
        delete activeSockets[myUsername];
        if (users[myUsername]) {
          users[myUsername].online = false;
          users[myUsername].lastSeen = Date.now();
        }
        persist();
      }
      broadcastUserList();
    }
  });
}

function handleMessage(ws, p, myUsername, setMyUsername) {
  if (p.type === MSG.CLIENT.REGISTER) {
    handleRegister(ws, p);
  } else if (p.type === MSG.CLIENT.LOGIN) {
    handleLogin(ws, p, setMyUsername);
  } else if (p.type === MSG.CLIENT.GET_HISTORY) {
    handleGetHistory(ws, myUsername);
  } else if (p.type === MSG.CLIENT.UPDATE_PROFILE) {
    handleUpdateProfile(ws, myUsername, p);
  } else if (p.type === MSG.CLIENT.CHANGE_PASSWORD) {
    handleChangePassword(ws, myUsername, p);
  } else if (p.type === MSG.CLIENT.MESSAGE) {
    if (myUsername) deliverMessage(myUsername, p, undefined);
  } else if (p.type === MSG.CLIENT.MEDIA_START) {
    uploads[p.uploadId] = { meta: p, parts: [], size: 0 };
  } else if (p.type === MSG.CLIENT.MEDIA_CHUNK) {
    const u = uploads[p.uploadId];
    if (u) {
      u.parts.push(p.data);
      u.size += p.data.length;
      if (u.size > MAX_MEDIA) {
        delete uploads[p.uploadId];
        ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.MEDIA, text: 'File too large' }));
      }
    }
  } else if (p.type === MSG.CLIENT.MEDIA_END) {
    const u = uploads[p.uploadId];
    if (u && myUsername) {
      delete uploads[p.uploadId];
      deliverMessage(myUsername, u.meta, u.parts.join(''));
    }
  } else if (p.type === MSG.CLIENT.EDIT_MESSAGE) {
    if (myUsername) {
      const key = branchKey(myUsername, p.to, p.groupId);
      const m = (history[key] || []).find(x => x.id === p.messageId);
      if (m) {
        m.text = p.newText;
        m.edited = true;
        persist();
      }
      const payload = { type: MSG.SERVER.MSG_EDITED, from: myUsername, to: p.to, groupId: p.groupId, messageId: p.messageId, newText: p.newText };
      audience(p.to, myUsername).forEach(u => sendToAll(u, payload));
    }
  } else if (p.type === MSG.CLIENT.TYPING) {
    if (myUsername) {
      audience(p.to, myUsername).forEach(u => {
        if (u !== myUsername) sendToAll(u, { type: MSG.SERVER.TYPING, from: myUsername, groupId: p.groupId });
      });
    }
  } else if (p.type === MSG.CLIENT.REACTION) {
    if (myUsername) {
      const key = branchKey(myUsername, p.to, p.groupId);
      const m = (history[key] || []).find(x => x.id === p.messageId);
      if (m) {
        m.reactions = m.reactions || {};
        m.reactions[p.reaction] = (m.reactions[p.reaction] || 0) + 1;
        persist();
      }
      const payload = { type: MSG.SERVER.NEW_REACTION, from: myUsername, to: p.to, groupId: p.groupId, messageId: p.messageId, reaction: p.reaction };
      audience(p.to, myUsername).forEach(u => sendToAll(u, payload));
    }
  } else if (p.type === MSG.CLIENT.PIN_MESSAGE) {
    if (myUsername) {
      const payload = { type: MSG.SERVER.MESSAGE_PINNED, from: myUsername, to: p.to, groupId: p.groupId, messageId: p.messageId, text: p.text };
      audience(p.to, myUsername).forEach(u => sendToAll(u, payload));
    }
  } else if (p.type === MSG.CLIENT.CALL_OFFER || p.type === MSG.CLIENT.VIDEO_OFFER) {
    const t = socketsOf(p.to);
    if (t.length) {
      p.from = myUsername;
      const s = JSON.stringify(p);
      t.forEach(x => { if (x.readyState === 1) x.send(s); });
    } else {
      ws.send(JSON.stringify({ type: MSG.SERVER.CALL_END, from: p.to, reason: 'offline' }));
    }
  } else if (p.type === MSG.CLIENT.CALL_ANSWER || p.type === MSG.CLIENT.ICE_CANDIDATE || p.type === MSG.CLIENT.CALL_END) {
    p.from = myUsername;
    const s = JSON.stringify(p);
    socketsOf(p.to).forEach(x => { if (x.readyState === 1) x.send(s); });
  } else if (p.type === MSG.CLIENT.GROUP_CREATED) {
    if (myUsername) {
      groups[p.groupId] = p.groupData;
      persist();
      (p.groupData.members || []).forEach(m => {
        if (m !== myUsername) sendToAll(m, { type: MSG.SERVER.GROUP_INVITE, from: myUsername, groupId: p.groupId, groupData: p.groupData });
      });
    }
  }
}

function handleRegister(ws, p) {
  const username = (p.username || '').trim().toLowerCase();
  const password = p.password || '';
  const displayName = (p.displayName || p.username || '').trim();
  
  if (!username || username.length < 3) {
    return ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.REGISTER, text: 'Username min 3 chars' }));
  }
  if (!password || password.length < 4) {
    return ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.REGISTER, text: 'Password min 4 chars' }));
  }
  if (!/^[a-z0-9_\.]+$/.test(username)) {
    return ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.REGISTER, text: 'Only latin, numbers, _ and .' }));
  }
  if (users[username]) {
    return ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.REGISTER, text: 'Username taken' }));
  }
  
  const salt = crypto.randomBytes(16).toString('hex');
  users[username] = {
    displayName: displayName || username,
    avatar: null,
    bio: 'Using HyperGram',
    passwordHash: hashPassword(password, salt),
    salt,
    createdAt: Date.now(),
    online: false,
    lastSeen: null
  };
  persist();
  ws.send(JSON.stringify({ type: MSG.SERVER.REGISTER_SUCCESS, username }));
}

function handleLogin(ws, p, setMyUsername) {
  if (p.token) {
    const uname = sessions[p.token];
    if (!uname || !users[uname]) {
      return ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.AUTH, text: 'Session expired' }));
    }
    doLogin(ws, uname, p.token, setMyUsername);
    return;
  }
  
  const username = (p.username || '').trim().toLowerCase();
  if (!users[username]) {
    return ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.AUTH, text: 'User not found' }));
  }
  
  const { passwordHash, salt } = users[username];
  if (hashPassword(p.password || '', salt) !== passwordHash) {
    return ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.AUTH, text: 'Wrong password' }));
  }
  
  const token = generateToken();
  sessions[token] = username;
  doLogin(ws, username, token, setMyUsername);
}

function handleGetHistory(ws, myUsername) {
  if (!myUsername) return;
  const out = [];
  Object.keys(history).forEach(k => {
    if (k === myUsername + '|saved') out.push(...history[k]);
    else if (groups[k] && (groups[k].members || []).includes(myUsername)) out.push(...history[k]);
    else if (k.split('|').includes(myUsername)) out.push(...history[k]);
  });
  ws.send(JSON.stringify({ type: MSG.SERVER.HISTORY, messages: out }));
}

function handleUpdateProfile(ws, myUsername, p) {
  if (!myUsername) return;
  if (p.displayName) users[myUsername].displayName = p.displayName;
  if (p.bio !== undefined) users[myUsername].bio = p.bio;
  if (p.avatar !== undefined) users[myUsername].avatar = p.avatar;
  persist();
  ws.send(JSON.stringify({ type: MSG.SERVER.PROFILE_UPDATED, profile: getProfile(myUsername) }));
  broadcastUserList();
}

function handleChangePassword(ws, myUsername, p) {
  if (!myUsername) return;
  const { passwordHash, salt } = users[myUsername];
  if (hashPassword(p.oldPassword || '', salt) !== passwordHash) {
    return ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.PASSWORD, text: 'Wrong old password' }));
  }
  if (!p.newPassword || p.newPassword.length < 4) {
    return ws.send(JSON.stringify({ type: MSG.SERVER.ERROR, code: MSG.ERROR_CODES.PASSWORD, text: 'New password min 4 chars' }));
  }
  const newSalt = crypto.randomBytes(16).toString('hex');
  users[myUsername].passwordHash = hashPassword(p.newPassword, newSalt);
  users[myUsername].salt = newSalt;
  persist();
  ws.send(JSON.stringify({ type: MSG.SERVER.PASSWORD_CHANGED }));
}

function doLogin(ws, username, token, setMyUsername) {
  (activeSockets[username] = activeSockets[username] || []).push(ws);
  setMyUsername(username);
  users[username].online = true;
  ws.send(JSON.stringify({ type: MSG.SERVER.AUTH_SUCCESS, username, token, profile: getProfile(username) }));
  broadcastUserList();
}

function deliverMessage(from, p, mediaOverride) {
  const msg = {
    type: MSG.SERVER.NEW_MESSAGE,
    id: genId(),
    ts: Date.now(),
    from,
    to: p.to,
    groupId: p.groupId,
    groupName: p.groupName,
    text: p.text || '',
    media: mediaOverride !== undefined ? mediaOverride : p.media,
    mediaType: p.mediaType,
    fileName: p.fileName,
    voice: p.voice,
    replyTo: p.replyTo,
    replyText: p.replyText,
    time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    reactions: {}
  };
  
  const key = branchKey(from, p.to, p.groupId);
  (history[key] = history[key] || []).push(msg);
  if (history[key].length > HISTORY_CAP) history[key] = history[key].slice(-HISTORY_CAP);
  persist();
  
  audience(p.to, from).forEach(u => sendToAll(u, msg));
}

function socketsOf(u) {
  return activeSockets[u] || [];
}

function sendToAll(u, data) {
  const s = JSON.stringify(data);
  socketsOf(u).forEach(ws => { if (ws.readyState === 1) ws.send(s); });
}

function getProfile(u) {
  const x = users[u];
  return { displayName: x.displayName, avatar: x.avatar, bio: x.bio };
}

function audience(to, from) {
  if (to === 'Favorites' || to === from) return [from];
  if (groups[to]) return groups[to].members || [];
  return [to, from];
}

function branchKey(from, to, groupId) {
  if (groupId) return groupId;
  if (to === 'Favorites') return from + '|saved';
  return [from, to].sort().join('|');
}

function broadcastUserList() {
  if (!wss) return;
  const list = Object.keys(users).map(u => ({
    username: u,
    displayName: users[u].displayName,
    avatar: users[u].avatar,
    bio: users[u].bio,
    online: socketsOf(u).length > 0,
    lastSeen: users[u].lastSeen
  }));
  const s = JSON.stringify({ type: MSG.SERVER.USER_LIST, users: list });
  wss.clients.forEach(ws => { if (ws.readyState === 1) ws.send(s); });
}