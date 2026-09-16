// =====================================================================
// AyuGram Pro Server — Render-ready edition
// Персистентность (db.json) + оффлайн-очередь + история + rate-limit
// =====================================================================
const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const port = process.env.PORT || 8080;
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'db.json');
const MAX_PAYLOAD = 8 * 1024 * 1024;   // 8 MB — медиа без OOM на free-инстансе
const RATE_LIMIT = 25;                 // сообщений/сек на сокет
const START_TIME = Date.now();

// ── HTTP: health-check + keep-alive ──
const httpServer = http.createServer((req, res) => {
  const cors = { 'Access-Control-Allow-Origin': '*' };
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', ...cors });
    return res.end(JSON.stringify({
      ok: true,
      uptime: Math.round((Date.now() - START_TIME) / 1000),
      users: Object.keys(users).length,
      online: Object.keys(activeSockets).length
    }));
  }
  res.writeHead(200, { 'Content-Type': 'text/plain', ...cors });
  res.end('AyuGram Pro Server OK');
});

const wss = new WebSocket.Server({ server: httpServer, maxPayload: MAX_PAYLOAD });

// ── «БД» ──
let users = {};      // username → { passwordHash, salt, displayName, avatar, bio, createdAt, online, lastSeen }
let history = {};    // "a|b" → [messages]
let offline = {};    // username → [messages] (ждут логина)
const activeSockets = {};
const sessions = {}; // token → username

console.log(`🚀 AyuGram Pro сервер запущен на порту ${port}`);

// ── Персистентность ──
let saveTimer = null;
function saveDB() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try { fs.writeFileSync(DB_FILE, JSON.stringify({ users, history })); }
    catch (e) { console.error('DB save error:', e.message); }
  }, 500);
}
(function loadDB() {
  try {
    if (fs.existsSync(DB_FILE)) {
      const d = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
      users = d.users || {};
      history = d.history || {};
      console.log(`💾 БД загружена: ${Object.keys(users).length} юзеров`);
    }
  } catch (e) { console.error('DB load error:', e.message); }
})();
function shutdown() {
  try { fs.writeFileSync(DB_FILE, JSON.stringify({ users, history })); } catch {}
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);   // Render останавливает инстанс через SIGTERM

// ── Keep-alive: самопинг каждые 14 мин + ping клиентов ──
setInterval(() => {
  http.get(`http://localhost:${port}/health`, () => {}).on('error', () => {});
  wss.clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.ping(); });
}, 14 * 60 * 1000);

// ── Хелперы ──
const now = () => new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
}
function generateToken() { return crypto.randomBytes(32).toString('hex'); }
function broadcast(data) {
  const packet = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(packet); });
}
function broadcastUserList() {
  const userList = Object.keys(users).map(u => ({
    username: u,
    displayName: users[u].displayName,
    avatar: users[u].avatar,
    bio: users[u].bio,
    online: !!activeSockets[u],
    lastSeen: users[u].lastSeen
  }));
  broadcast({ type: 'user_list', users: userList });
}
function sendTo(username, data) {
  const sock = activeSockets[username];
  if (sock && sock.readyState === WebSocket.OPEN) sock.send(JSON.stringify(data));
}
function getProfile(username) {
  const u = users[username];
  return { displayName: u.displayName, avatar: u.avatar, bio: u.bio };
}
function chatKey(a, b) { return [a, b].sort().join('|'); }
function historyKey(from, to) { return to === 'Избранное' ? from + '|saved' : chatKey(from, to); }
function pushHistory(msg) {
  const key = historyKey(msg.from, msg.to);
  (history[key] = history[key] || []).push(msg);
  if (history[key].length > 500) history[key] = history[key].slice(-500);
}

// =====================================================================
wss.on('connection', (ws) => {
  let myUsername = null;
  ws._rc = { t: Date.now(), n: 0 };
  ws.on('pong', () => {});

  ws.on('message', (message) => {
    try {
      const p = JSON.parse(message.toString());

      // rate-limit на активные типы
      if (p.type === 'message' || p.type === 'typing' || p.type === 'reaction') {
        const t = Date.now();
        if (t - ws._rc.t > 1000) ws._rc = { t, n: 0 };
        if (++ws._rc.n > RATE_LIMIT) return;
      }

      // ── REGISTER ──
      if (p.type === 'register') {
        const username = (p.username || '').trim().toLowerCase();
        const password = p.password || '';
        const displayName = (p.displayName || p.username || '').trim();
        if (!username || username.length < 3)
          return ws.send(JSON.stringify({ type: 'error', code: 'register', text: 'Имя пользователя минимум 3 символа' }));
        if (!password || password.length < 4)
          return ws.send(JSON.stringify({ type: 'error', code: 'register', text: 'Пароль минимум 4 символа' }));
        if (!/^[a-z0-9_\.]+$/.test(username))
          return ws.send(JSON.stringify({ type: 'error', code: 'register', text: 'Только латиница, цифры, _ и .' }));
        if (users[username])
          return ws.send(JSON.stringify({ type: 'error', code: 'register', text: 'Имя пользователя занято' }));
        const salt = crypto.randomBytes(16).toString('hex');
        users[username] = {
          displayName: displayName || username,
          avatar: null,
          bio: 'Использую AyuGram',
          passwordHash: hashPassword(password, salt), salt,
          createdAt: Date.now(), online: false, lastSeen: null
        };
        saveDB();
        console.log(`✅ Зарегистрирован: ${username}`);
        ws.send(JSON.stringify({ type: 'register_success', username }));
      }

      // ── LOGIN ──
      if (p.type === 'login') {
        if (p.token) {
          const uname = sessions[p.token];
          if (!uname || !users[uname])
            return ws.send(JSON.stringify({ type: 'error', code: 'auth', text: 'Сессия истекла, войдите снова' }));
          return doLogin(ws, uname, p.token);
        }
        const username = (p.username || '').trim().toLowerCase();
        const password = p.password || '';
        if (!users[username])
          return ws.send(JSON.stringify({ type: 'error', code: 'auth', text: 'Пользователь не найден' }));
        const { passwordHash, salt } = users[username];
        if (hashPassword(password, salt) !== passwordHash)
          return ws.send(JSON.stringify({ type: 'error', code: 'auth', text: 'Неверный пароль' }));
        const token = generateToken();
        sessions[token] = username;
        doLogin(ws, username, token);
      }

      // ── UPDATE PROFILE ──
      if (p.type === 'update_profile') {
        if (!myUsername) return;
        if (p.displayName) users[myUsername].displayName = p.displayName;
        if (p.bio !== undefined) users[myUsername].bio = p.bio;
        if (p.avatar !== undefined) users[myUsername].avatar = p.avatar;
        saveDB();
        ws.send(JSON.stringify({ type: 'profile_updated', profile: getProfile(myUsername) }));
        broadcastUserList();
      }

      // ── CHANGE PASSWORD ──
      if (p.type === 'change_password') {
        if (!myUsername) return;
        const { oldPassword, newPassword } = p;
        const { passwordHash, salt } = users[myUsername];
        if (hashPassword(oldPassword, salt) !== passwordHash)
          return ws.send(JSON.stringify({ type: 'error', code: 'password', text: 'Неверный старый пароль' }));
        if (!newPassword || newPassword.length < 4)
          return ws.send(JSON.stringify({ type: 'error', code: 'password', text: 'Новый пароль минимум 4 символа' }));
        const newSalt = crypto.randomBytes(16).toString('hex');
        users[myUsername].passwordHash = hashPassword(newPassword, newSalt);
        users[myUsername].salt = newSalt;
        saveDB();
        ws.send(JSON.stringify({ type: 'password_changed' }));
      }

      // ── GET HISTORY (новый тип, опционален для клиента) ──
      if (p.type === 'get_history') {
        if (!myUsername) return;
        const key = historyKey(myUsername, p.with);
        ws.send(JSON.stringify({ type: 'history', with: p.with, messages: history[key] || [] }));
      }

      // ── MESSAGE ──
      if (p.type === 'message') {
        if (!myUsername) return;
        const msg = {
          type: 'new_message',
          id: Date.now().toString(36) + Math.random().toString(36).substr(2, 5),
          from: myUsername, to: p.to,
          text: p.text, media: p.media, mediaType: p.mediaType,
          fileName: p.fileName, voice: p.voice,
          replyTo: p.replyTo, replyText: p.replyText,
          time: now(), reactions: {}
        };
        pushHistory(msg);
        saveDB();
        if (p.to === 'Избранное') {
          ws.send(JSON.stringify(msg));
        } else {
          const target = activeSockets[p.to];
          if (target && target.readyState === WebSocket.OPEN) {
            target.send(JSON.stringify(msg));
          } else {
            (offline[p.to] = offline[p.to] || []).push(msg);   // ✅ оффлайн-очередь
          }
          ws.send(JSON.stringify(msg));
        }
      }

      // ── EDIT ──
      if (p.type === 'edit_message') {
        if (!myUsername) return;
        const key = historyKey(myUsername, p.to);
        const m = (history[key] || []).find(x => x.id === p.messageId);
        if (m) { m.text = p.newText; m.edited = true; saveDB(); }
        const payload = { type: 'msg_edited', from: myUsername, to: p.to, messageId: p.messageId, newText: p.newText };
        sendTo(p.to, payload);
        ws.send(JSON.stringify(payload));
      }

      // ── TYPING ──
      if (p.type === 'typing') {
        if (!myUsername) return;
        sendTo(p.to, { type: 'typing', from: myUsername });
      }

      // ── REACTION ──
      if (p.type === 'reaction') {
        if (!myUsername) return;
        const key = historyKey(myUsername, p.to);
        const m = (history[key] || []).find(x => x.id === p.messageId);
        if (m) { m.reactions = m.reactions || {}; m.reactions[p.reaction] = (m.reactions[p.reaction] || 0) + 1; saveDB(); }
        const payload = { type: 'new_reaction', from: myUsername, to: p.to, messageId: p.messageId, reaction: p.reaction };
        if (p.to === 'Избранное') ws.send(JSON.stringify(payload));
        else { sendTo(p.to, payload); ws.send(JSON.stringify(payload)); }
      }

      // ── PIN ──
      if (p.type === 'pin_message') {
        if (!myUsername) return;
        const payload = { type: 'message_pinned', from: myUsername, to: p.to, messageId: p.messageId, text: p.text };
        if (p.to === 'Избранное') ws.send(JSON.stringify(payload));
        else { sendTo(p.to, payload); ws.send(JSON.stringify(payload)); }
      }

      // ── CALLS (WebRTC signaling) ──
      if (['call_offer', 'video_offer', 'call_answer', 'ice_candidate', 'call_end'].includes(p.type)) {
        const target = activeSockets[p.to];
        if (target && target.readyState === WebSocket.OPEN) {
          p.from = myUsername;
          target.send(JSON.stringify(p));
        } else if (p.type === 'call_offer' || p.type === 'video_offer') {
          ws.send(JSON.stringify({ type: 'call_end', from: p.to, reason: 'offline' }));
        }
      }

      // ── GROUP ──
      if (p.type === 'group_created') {
        sendTo(p.to, { type: 'group_invite', from: myUsername, groupId: p.groupId, groupData: p.groupData });
      }
    } catch (err) {
      console.error('Ошибка пакета:', err.message);
    }
  });

  ws.on('close', () => {
    if (myUsername) {
      console.log(`💤 Отключился: ${myUsername}`);
      delete activeSockets[myUsername];
      if (users[myUsername]) {
        users[myUsername].online = false;
        users[myUsername].lastSeen = Date.now();
        saveDB();
      }
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
    // ✅ отдаём накопленные оффлайн-сообщения
    const queue = offline[username];
    if (queue && queue.length) {
      queue.forEach(m => ws.send(JSON.stringify(m)));
      delete offline[username];
    }
    broadcastUserList();
  }
});

httpServer.listen(port, () => console.log(`HTTP + WS на порту ${port}`));
