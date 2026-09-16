const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const port = process.env.PORT || 8080;
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'db.json');
const MAX_PAYLOAD = 8 * 1024 * 1024;          // лимит одного WS-кадра (чанки мелкие)
const HISTORY_CAP = 300;                       // сообщений на ветку чата
const MAX_MEDIA = 80 * 1024 * 1024;           // лимит base64-файла (~60 МБ)

// ── БД на диске (переживает sleep/wake Render) ──
let db = { users: {}, history: {}, groups: {} };
try { if (fs.existsSync(DB_FILE)) db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch (e) { console.error('DB load fail:', e.message); }
const users   = db.users   || (db.users = {});
const history = db.history || (db.history = {});
const groups  = db.groups  || (db.groups = {});

const activeSockets = {};   // username → [ws, ws, ...] (МУЛЬТИ-ДЕВАЙС)
const sessions = {};        // token → username
const uploads = {};         // uploadId → { meta, parts[], size }

let saveTimer = null;
function persist() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try { fs.writeFileSync(DB_FILE, JSON.stringify(db)); } catch (e) { console.error('DB save fail:', e.message); }
  }, 800);
}

const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, users: Object.keys(users).length, online: Object.keys(activeSockets).length }));
  }
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end('AyuGram Pro Server OK');
});
const wss = new WebSocket.Server({ server: httpServer, maxPayload: MAX_PAYLOAD });

console.log(`🚀 AyuGram Pro сервер запущен на порту ${port}`);

setInterval(() => {
  http.get(`http://localhost:${port}/health`, () => {});
  wss.clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.ping(); });
}, 14 * 60 * 1000);

function hashPassword(password, salt) { return crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex'); }
function generateToken() { return crypto.randomBytes(32).toString('hex'); }
function genId() { return Date.now().toString(36) + Math.random().toString(36).substr(2, 5); }
function socketsOf(u) { return activeSockets[u] || []; }
function sendToAll(u, data) { const s = JSON.stringify(data); socketsOf(u).forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(s); }); }
function broadcastUserList() {
  const list = Object.keys(users).map(u => ({ username: u, displayName: users[u].displayName, avatar: users[u].avatar, bio: users[u].bio, online: socketsOf(u).length > 0, lastSeen: users[u].lastSeen }));
  const s = JSON.stringify({ type: 'user_list', users: list });
  wss.clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(s); });
}
function getProfile(u) { const x = users[u]; return { displayName: x.displayName, avatar: x.avatar, bio: x.bio }; }
function audience(to, from) {
  if (to === 'Избранное' || to === from) return [from];
  if (groups[to]) return groups[to].members || [];
  return [to, from];
}
function branchKey(from, to, groupId) {
  if (groupId) return groupId;
  if (to === 'Избранное') return from + '|saved';
  return [from, to].sort().join('|');
}
function deliverMessage(from, p, mediaOverride) {
  const msg = {
    type: 'new_message', id: genId(), ts: Date.now(),
    from, to: p.to, groupId: p.groupId, groupName: p.groupName,
    text: p.text || '', media: mediaOverride !== undefined ? mediaOverride : p.media,
    mediaType: p.mediaType, fileName: p.fileName, voice: p.voice,
    replyTo: p.replyTo, replyText: p.replyText,
    time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    reactions: {}
  };
  const key = branchKey(from, p.to, p.groupId);
  (history[key] = history[key] || []).push(msg);
  if (history[key].length > HISTORY_CAP) history[key] = history[key].slice(-HISTORY_CAP);
  persist();
  audience(p.to, from).forEach(u => sendToAll(u, msg));   // всем устройствам обеих сторон
}

wss.on('connection', (ws) => {
  let myUsername = null;
  ws.on('pong', () => {});

  ws.on('message', (message) => {
    try {
      const p = JSON.parse(message.toString());

      if (p.type === 'register') {
        const username = (p.username || '').trim().toLowerCase();
        const password = p.password || '';
        const displayName = (p.displayName || p.username || '').trim();
        if (!username || username.length < 3) return ws.send(JSON.stringify({ type: 'error', code: 'register', text: 'Имя пользователя минимум 3 символа' }));
        if (!password || password.length < 4) return ws.send(JSON.stringify({ type: 'error', code: 'register', text: 'Пароль минимум 4 символа' }));
        if (!/^[a-z0-9_\.]+$/.test(username)) return ws.send(JSON.stringify({ type: 'error', code: 'register', text: 'Только латиница, цифры, _ и .' }));
        if (users[username]) return ws.send(JSON.stringify({ type: 'error', code: 'register', text: 'Имя пользователя занято' }));
        const salt = crypto.randomBytes(16).toString('hex');
        users[username] = { displayName: displayName || username, avatar: null, bio: 'Использую AyuGram', passwordHash: hashPassword(password, salt), salt, createdAt: Date.now(), online: false, lastSeen: null };
        persist();
        console.log(`✅ Зарегистрирован: ${username}`);
        ws.send(JSON.stringify({ type: 'register_success', username }));
      }

      if (p.type === 'login') {
        if (p.token) {
          const uname = sessions[p.token];
          if (!uname || !users[uname]) return ws.send(JSON.stringify({ type: 'error', code: 'auth', text: 'Сессия истекла, войдите снова' }));
          return doLogin(ws, uname, p.token);
        }
        const username = (p.username || '').trim().toLowerCase();
        if (!users[username]) return ws.send(JSON.stringify({ type: 'error', code: 'auth', text: 'Пользователь не найден' }));
        const { passwordHash, salt } = users[username];
        if (hashPassword(p.password || '', salt) !== passwordHash) return ws.send(JSON.stringify({ type: 'error', code: 'auth', text: 'Неверный пароль' }));
        const token = generateToken(); sessions[token] = username;
        doLogin(ws, username, token);
      }

      if (p.type === 'get_history') {
        if (!myUsername) return;
        const out = [];
        Object.keys(history).forEach(k => {
          if (k === myUsername + '|saved') out.push(...history[k]);
          else if (groups[k] && (groups[k].members || []).includes(myUsername)) out.push(...history[k]);
          else if (k.split('|').includes(myUsername)) out.push(...history[k]);
        });
        ws.send(JSON.stringify({ type: 'history', messages: out }));
      }

      if (p.type === 'update_profile') {
        if (!myUsername) return;
        if (p.displayName) users[myUsername].displayName = p.displayName;
        if (p.bio !== undefined) users[myUsername].bio = p.bio;
        if (p.avatar !== undefined) users[myUsername].avatar = p.avatar;
        persist();
        ws.send(JSON.stringify({ type: 'profile_updated', profile: getProfile(myUsername) }));
        broadcastUserList();
      }

      if (p.type === 'change_password') {
        if (!myUsername) return;
        const { passwordHash, salt } = users[myUsername];
        if (hashPassword(p.oldPassword || '', salt) !== passwordHash) return ws.send(JSON.stringify({ type: 'error', code: 'password', text: 'Неверный старый пароль' }));
        if (!p.newPassword || p.newPassword.length < 4) return ws.send(JSON.stringify({ type: 'error', code: 'password', text: 'Новый пароль минимум 4 символа' }));
        const newSalt = crypto.randomBytes(16).toString('hex');
        users[myUsername].passwordHash = hashPassword(p.newPassword, newSalt);
        users[myUsername].salt = newSalt; persist();
        ws.send(JSON.stringify({ type: 'password_changed' }));
      }

      // ── СООБЩЕНИЕ (текст/голос/мелкий файл) ──
      if (p.type === 'message') { if (myUsername) deliverMessage(myUsername, p, undefined); }

      // ── ЧАНКОВАЯ ЗАГРУЗКА БОЛЬШИХ ФАЙЛОВ (видео) ──
      if (p.type === 'media_start') { uploads[p.uploadId] = { meta: p, parts: [], size: 0 }; }
      if (p.type === 'media_chunk') {
        const u = uploads[p.uploadId];
        if (u) {
          u.parts.push(p.data); u.size += p.data.length;
          if (u.size > MAX_MEDIA) { delete uploads[p.uploadId]; ws.send(JSON.stringify({ type: 'error', code: 'media', text: 'Файл слишком большой' })); }
        }
      }
      if (p.type === 'media_end') {
        const u = uploads[p.uploadId];
        if (!u || !myUsername) return;
        delete uploads[p.uploadId];
        deliverMessage(myUsername, u.meta, u.parts.join(''));
      }

      if (p.type === 'edit_message') {
        if (!myUsername) return;
        const key = branchKey(myUsername, p.to, p.groupId);
        const m = (history[key] || []).find(x => x.id === p.messageId);
        if (m) { m.text = p.newText; m.edited = true; persist(); }
        const payload = { type: 'msg_edited', from: myUsername, to: p.to, groupId: p.groupId, messageId: p.messageId, newText: p.newText };
        audience(p.to, myUsername).forEach(u => sendToAll(u, payload));
      }

      if (p.type === 'typing') {
        if (!myUsername) return;
        audience(p.to, myUsername).forEach(u => { if (u !== myUsername) sendToAll(u, { type: 'typing', from: myUsername, groupId: p.groupId }); });
      }

      if (p.type === 'reaction') {
        if (!myUsername) return;
        const key = branchKey(myUsername, p.to, p.groupId);
        const m = (history[key] || []).find(x => x.id === p.messageId);
        if (m) { m.reactions = m.reactions || {}; m.reactions[p.reaction] = (m.reactions[p.reaction] || 0) + 1; persist(); }
        const payload = { type: 'new_reaction', from: myUsername, to: p.to, groupId: p.groupId, messageId: p.messageId, reaction: p.reaction };
        audience(p.to, myUsername).forEach(u => sendToAll(u, payload));
      }

      if (p.type === 'pin_message') {
        if (!myUsername) return;
        const payload = { type: 'message_pinned', from: myUsername, to: p.to, groupId: p.groupId, messageId: p.messageId, text: p.text };
        audience(p.to, myUsername).forEach(u => sendToAll(u, payload));
      }

      // ── ЗВОНКИ: сигналы всем устройствам цели ──
      if (['call_offer', 'video_offer'].includes(p.type)) {
        const t = socketsOf(p.to);
        if (t.length) { p.from = myUsername; const s = JSON.stringify(p); t.forEach(x => { if (x.readyState === WebSocket.OPEN) x.send(s); }); }
        else ws.send(JSON.stringify({ type: 'call_end', from: p.to, reason: 'offline' }));
      }
      if (['call_answer', 'ice_candidate', 'call_end'].includes(p.type)) {
        p.from = myUsername;
        const s = JSON.stringify(p);
        socketsOf(p.to).forEach(x => { if (x.readyState === WebSocket.OPEN) x.send(s); });
      }

      if (p.type === 'group_created') {
        if (!myUsername) return;
        groups[p.groupId] = p.groupData; persist();
        (p.groupData.members || []).forEach(m => { if (m !== myUsername) sendToAll(m, { type: 'group_invite', from: myUsername, groupId: p.groupId, groupData: p.groupData }); });
      }
    } catch (err) { console.error('Ошибка пакета:', err.message); }
  });

  ws.on('close', () => {
    if (myUsername) {
      const arr = activeSockets[myUsername] || [];
      const i = arr.indexOf(ws); if (i >= 0) arr.splice(i, 1);
      if (!arr.length) {
        delete activeSockets[myUsername];
        if (users[myUsername]) { users[myUsername].online = false; users[myUsername].lastSeen = Date.now(); }
        persist();
      }
      console.log(`💤 Отключился: ${myUsername} (осталось устройств: ${arr.length})`);
      broadcastUserList();
    }
  });
  ws.on('error', err => console.error('WS error:', err.message));

  function doLogin(ws, username, token) {
    // БЕЗ кика: несколько устройств на одном аккаунте
    (activeSockets[username] = activeSockets[username] || []).push(ws);
    myUsername = username;
    users[username].online = true;
    console.log(`👤 В сети: ${username} (устройств: ${socketsOf(username).length})`);
    ws.send(JSON.stringify({ type: 'auth_success', username, token, profile: getProfile(username) }));
    broadcastUserList();
  }
});

httpServer.listen(port, () => console.log(`HTTP + WS на порту ${port}`));
