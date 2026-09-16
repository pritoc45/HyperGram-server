const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const port = process.env.PORT || 8080;
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'db.json');

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end('AyuGram Pro Server OK');
});
const wss = new WebSocket.Server({ server: httpServer, maxPayload: 100 * 1024 * 1024 });

// ── БД на диске: пользователи + история + оффлайн-очередь ──
let db = { users: {}, history: {}, offline: {} };
try { if (fs.existsSync(DB_FILE)) db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch (e) { console.error('DB load fail:', e.message); }
const users = db.users || (db.users = {});
const history = db.history || (db.history = {});
const offline = db.offline || (db.offline = {});
let saveTimer = null;
function saveDB() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => { try { fs.writeFileSync(DB_FILE, JSON.stringify(db)); } catch (e) { console.error('DB save fail:', e.message); } }, 400);
}

const activeSockets = {};
const sessions = {};
console.log(`🚀 AyuGram Pro сервер запущен на порту ${port}`);

setInterval(() => {
  http.get(`http://localhost:${port}`, () => {});
  wss.clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.ping(); });
}, 14 * 60 * 1000);

function hashPassword(password, salt) { return crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex'); }
function generateToken() { return crypto.randomBytes(32).toString('hex'); }
function broadcast(data) { const p = JSON.stringify(data); wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(p); }); }
function broadcastUserList() {
  broadcast({ type: 'user_list', users: Object.keys(users).map(u => ({ username: u, displayName: users[u].displayName, avatar: users[u].avatar, bio: users[u].bio, online: !!activeSockets[u], lastSeen: users[u].lastSeen })) });
}
function sendTo(username, data) {
  const sock = activeSockets[username];
  if (sock && sock.readyState === WebSocket.OPEN) { sock.send(JSON.stringify(data)); return true; }
  return false;
}
function histKey(me, other) { return other === 'Избранное' ? me + '|saved' : [me, other].sort().join('|'); }
function pushHistory(msg) {
  const key = histKey(msg.from, msg.to);
  (history[key] = history[key] || []).push(msg);
  if (history[key].length > 300) history[key] = history[key].slice(-300);
  saveDB();
}
function queueOffline(username, msg) {
  (offline[username] = offline[username] || []).push(msg);
  if (offline[username].length > 100) offline[username] = offline[username].slice(-100);
  saveDB();
}

wss.on('connection', (ws) => {
  let myUsername = null;
  ws.on('pong', () => {});

  ws.on('message', (message) => {
    try {
      const p = JSON.parse(message.toString());
      if (p.type === 'ping') return;

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
        saveDB();
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
        const password = p.password || '';
        if (!users[username]) return ws.send(JSON.stringify({ type: 'error', code: 'auth', text: 'Пользователь не найден' }));
        const { passwordHash, salt } = users[username];
        if (hashPassword(password, salt) !== passwordHash) return ws.send(JSON.stringify({ type: 'error', code: 'auth', text: 'Неверный пароль' }));
        const token = generateToken();
        sessions[token] = username;
        doLogin(ws, username, token);
      }

      if (p.type === 'update_profile') {
        if (!myUsername) return;
        if (p.displayName) users[myUsername].displayName = p.displayName;
        if (p.bio !== undefined) users[myUsername].bio = p.bio;
        if (p.avatar !== undefined) users[myUsername].avatar = p.avatar;
        saveDB();
        ws.send(JSON.stringify({ type: 'profile_updated', profile: getProfile(myUsername) }));
        broadcastUserList();
      }

      if (p.type === 'change_password') {
        if (!myUsername) return;
        const { oldPassword, newPassword } = p;
        const { passwordHash, salt } = users[myUsername];
        if (hashPassword(oldPassword, salt) !== passwordHash) return ws.send(JSON.stringify({ type: 'error', code: 'password', text: 'Неверный старый пароль' }));
        if (!newPassword || newPassword.length < 4) return ws.send(JSON.stringify({ type: 'error', code: 'password', text: 'Новый пароль минимум 4 символа' }));
        const newSalt = crypto.randomBytes(16).toString('hex');
        users[myUsername].passwordHash = hashPassword(newPassword, newSalt);
        users[myUsername].salt = newSalt;
        saveDB();
        ws.send(JSON.stringify({ type: 'password_changed' }));
      }

      if (p.type === 'get_history') {
        if (!myUsername) return;
        const key = histKey(myUsername, p.with);
        ws.send(JSON.stringify({ type: 'history', with: p.with, messages: history[key] || [] }));
      }

      if (p.type === 'message') {
        if (!myUsername) return;
        const msg = {
          type: 'new_message',
          id: Date.now().toString(36) + Math.random().toString(36).substr(2, 5),
          from: myUsername, to: p.to,
          groupId: p.groupId, groupName: p.groupName,
          text: p.text, media: p.media, mediaType: p.mediaType, fileName: p.fileName, voice: p.voice,
          replyTo: p.replyTo, replyText: p.replyText,
          time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
          reactions: {}
        };
        pushHistory(msg);
        if (p.to === 'Избранное') {
          ws.send(JSON.stringify(msg));
        } else if (p.groupId) {
          // групповое: шлём всем участникам, оффлайн-юзерам — в очередь
          (groups_members(p.groupId, myUsername)).forEach(m => { if (!sendTo(m, msg)) queueOffline(m, msg); });
          ws.send(JSON.stringify(msg));
        } else {
          if (!sendTo(p.to, msg)) queueOffline(p.to, msg);
          ws.send(JSON.stringify(msg));
        }
      }

      if (p.type === 'edit_message') {
        if (!myUsername) return;
        const key = histKey(myUsername, p.to);
        const m = (history[key] || []).find(x => x.id === p.messageId);
        if (m) { m.text = p.newText; m.edited = true; saveDB(); }
        const payload = { type: 'msg_edited', from: myUsername, to: p.to, messageId: p.messageId, newText: p.newText };
        sendTo(p.to, payload); ws.send(JSON.stringify(payload));
      }

      if (p.type === 'typing') { if (myUsername) sendTo(p.to, { type: 'typing', from: myUsername, groupId: p.groupId }); }

      if (p.type === 'reaction') {
        if (!myUsername) return;
        const key = histKey(myUsername, p.to);
        const m = (history[key] || []).find(x => x.id === p.messageId);
        if (m) { m.reactions = m.reactions || {}; m.reactions[p.reaction] = (m.reactions[p.reaction] || 0) + 1; saveDB(); }
        const payload = { type: 'new_reaction', from: myUsername, to: p.to, messageId: p.messageId, reaction: p.reaction };
        if (p.to === 'Избранное') ws.send(JSON.stringify(payload));
        else { sendTo(p.to, payload); ws.send(JSON.stringify(payload)); }
      }

      if (p.type === 'pin_message') {
        if (!myUsername) return;
        const payload = { type: 'message_pinned', from: myUsername, to: p.to, messageId: p.messageId, text: p.text };
        if (p.to === 'Избранное') ws.send(JSON.stringify(payload));
        else { sendTo(p.to, payload); ws.send(JSON.stringify(payload)); }
      }

      if (['call_offer','video_offer','call_answer','ice_candidate','call_end'].includes(p.type)) {
        const target = activeSockets[p.to];
        if (target && target.readyState === WebSocket.OPEN) { p.from = myUsername; target.send(JSON.stringify(p)); }
        else if (['call_offer','video_offer'].includes(p.type)) ws.send(JSON.stringify({ type: 'call_end', from: p.to, reason: 'offline' }));
      }

      if (p.type === 'group_created') {
        if (!myUsername) return;
        (p.groupData && p.groupData.members || []).forEach(m => {
          if (m !== myUsername) sendTo(m, { type: 'group_invite', from: myUsername, groupId: p.groupId, groupData: p.groupData });
        });
      }
    } catch (err) { console.error('Ошибка пакета:', err.message); }
  });

  ws.on('close', () => {
    if (myUsername) {
      console.log(`💤 Отключился: ${myUsername}`);
      delete activeSockets[myUsername];
      if (users[myUsername]) { users[myUsername].online = false; users[myUsername].lastSeen = Date.now(); saveDB(); }
      broadcastUserList();
    }
  });
  ws.on('error', err => console.error('WS error:', err.message));

  function doLogin(ws, username, token) {
    if (activeSockets[username] && activeSockets[username] !== ws) {
      activeSockets[username].send(JSON.stringify({ type: 'kicked', text: 'Вы вошли с другого устройства' }));
      activeSockets[username].close();
    }
    myUsername = username;
    activeSockets[username] = ws;
    users[username].online = true;
    console.log(`👤 В сети: ${username}`);
    ws.send(JSON.stringify({ type: 'auth_success', username, token, profile: getProfile(username) }));
    // ✅ выдаём накопленные оффлайн-сообщения
    const q = offline[username];
    if (q && q.length) { q.forEach(m => ws.send(JSON.stringify(m))); delete offline[username]; saveDB(); console.log(`📬 Выдано из оффлайна: ${q.length} → ${username}`); }
    broadcastUserList();
  }
});

function groups_members(groupId, exclude) {
  // члены группы приходят в group_created; храним на лету
  if (!global._groups) global._groups = {};
  return (global._groups[groupId] || []).filter(m => m !== exclude);
}
// запоминаем состав групп из group_created
const _origOn = wss.on.bind(wss);
wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    try {
      const p = JSON.parse(raw.toString());
      if (p.type === 'group_created' && p.groupData && p.groupData.members) {
        if (!global._groups) global._groups = {};
        global._groups[p.groupId] = p.groupData.members;
      }
    } catch (e) {}
  });
});

function getProfile(username) { const u = users[username]; return { displayName: u.displayName, avatar: u.avatar, bio: u.bio }; }
httpServer.listen(port, () => console.log(`HTTP + WS на порту ${port}`));
