// =====================================================================
// AyuGram Pro Server — финальная версия
// Персистентность + оффлайн-очередь + история + группы + rate-limit
// =====================================================================
const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const port = process.env.PORT || 8080;
const DB_FILE = process.env.DB_FILE || path.join(__dirname, 'db.json');
const MAX_PAYLOAD = 100 * 1024 * 1024;   // медиа в base64
const RATE_LIMIT = 30;                    // сообщений/сек на сокет
const HISTORY_CAP = 500;                  // сообщений на чат
const OFFLINE_CAP = 200;                  // сообщений в оффлайн-очереди
const TEXT_LIMIT = 20000;                 // символов в тексте
const START_TIME = Date.now();

// ── Состояние ──
let users = {};      // username → { passwordHash, salt, displayName, avatar, bio, createdAt, online, lastSeen }
let groups = {};     // groupId → { name, type, members[] }
let history = {};    // key → [messages]   key: "a|b" (sorted) | "user|saved" | groupId
let offline = {};    // username → [messages]
const activeSockets = {};
const sessions = {}; // token → username

// ── Загрузка БД ──
(function loadDB() {
  try {
    if (fs.existsSync(DB_FILE)) {
      const d = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
      users = d.users || {};
      groups = d.groups || {};
      history = d.history || {};
      offline = d.offline || {};
      console.log(`💾 БД загружена: ${Object.keys(users).length} юзеров, ${Object.keys(groups).length} групп`);
    }
  } catch (e) { console.error('DB load error:', e.message); }
})();

// ── Сохранение БД (debounce 500ms) ──
let saveTimer = null;
function saveDB() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try { fs.writeFileSync(DB_FILE, JSON.stringify({ users, groups, history, offline })); }
    catch (e) { console.error('DB save error:', e.message); }
  }, 500);
}

// ── HTTP сервер: health check + keep-alive ──
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

console.log(`🚀 AyuGram Pro сервер запущен на порту ${port}`);

// Keep-alive: пинг клиентов + самопинг каждые 14 минут
setInterval(() => {
  http.get(`http://localhost:${port}/health`, () => {}).on('error', () => {});
  wss.clients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.ping(); });
}, 14 * 60 * 1000);

// ── Хелперы ──
function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
}
function generateToken() { return crypto.randomBytes(32).toString('hex'); }
function genId() { return Date.now().toString(36) + Math.random().toString(36).substr(2, 5); }
function nowTime() { return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
function getProfile(username) {
  const u = users[username];
  return { displayName: u.displayName, avatar: u.avatar, bio: u.bio };
}
function broadcast(data) {
  const packet = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(packet); });
}
function broadcastUserList() {
  broadcast({
    type: 'user_list',
    users: Object.keys(users).map(u => ({
      username: u, displayName: users[u].displayName, avatar: users[u].avatar,
      bio: users[u].bio, online: !!activeSockets[u], lastSeen: users[u].lastSeen
    }))
  });
}
function sendTo(username, data) {
  const sock = activeSockets[username];
  if (sock && sock.readyState === WebSocket.OPEN) sock.send(JSON.stringify(data));
}
// Доставка: онлайн → сразу, оффлайн → в очередь
function deliver(username, msg) {
  const sock = activeSockets[username];
  if (sock && sock.readyState === WebSocket.OPEN) {
    sock.send(JSON.stringify(msg));
  } else if (users[username]) {
    (offline[username] = offline[username] || []).push(msg);
    if (offline[username].length > OFFLINE_CAP) offline[username] = offline[username].slice(-OFFLINE_CAP);
    saveDB();
  }
}
// Ключ истории чата
function historyKey(from, to) {
  if (groups[to]) return to;                       // групповой чат
  if (to === 'Избранное') return from + '|saved'; // избранное
  return [from, to].sort().join('|');              // личный чат
}
function pushHistory(key, msg) {
  (history[key] = history[key] || []).push(msg);
  if (history[key].length > HISTORY_CAP) history[key] = history[key].slice(-HISTORY_CAP);
  saveDB();
}
// Кто получатели (с учётом групп)
function resolveRecipients(to, from) {
  if (to === 'Избранное') return [from];
  const g = groups[to];
  if (g) return (g.members || []).filter(m => m !== from);
  return [to];
}

// =====================================================================
wss.on('connection', (ws) => {
  let myUsername = null;
  ws._rc = { t: Date.now(), n: 0 }; // rate-limit счётчик
  ws.on('pong', () => {});

  // rate-limit для активных типов
  function rateLimited() {
    const t = Date.now();
    if (t - ws._rc.t > 1000) ws._rc = { t, n: 0 };
    return ++ws._rc.n > RATE_LIMIT;
  }

  ws.on('message', (message) => {
    try {
      const p = JSON.parse(message.toString());

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
          displayName: displayName || username, avatar: null, bio: 'Использую AyuGram',
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

      // ── GET HISTORY ──
      if (p.type === 'get_history') {
        if (!myUsername) return;
        const key = historyKey(myUsername, p.with);
        ws.send(JSON.stringify({ type: 'history', with: p.with, messages: history[key] || [] }));
      }

      // ── MESSAGE ──
      if (p.type === 'message') {
        if (!myUsername) return;
        if (rateLimited()) return;
        if (p.text && p.text.length > TEXT_LIMIT)
          return ws.send(JSON.stringify({ type: 'error', code: 'message', text: 'Сообщение слишком длинное' }));

        const isGroupAddress = !!groups[p.to];           // прислали сразу на группу
        const msg = {
          type: 'new_message',
          id: genId(),
          from: myUsername,
          to: p.to,
          groupId: p.groupId || (isGroupAddress ? p.to : undefined),
          groupName: p.groupName || (isGroupAddress ? groups[p.to].name : undefined),
          text: p.text || '',
          media: p.media, mediaType: p.mediaType, fileName: p.fileName, voice: p.voice,
          replyTo: p.replyTo, replyText: p.replyText,
          time: nowTime(),
          reactions: {}
        };

        // история: только личные/избранное/группа-адрес (пер-member дубли не пишем)
        if (!p.groupId) pushHistory(historyKey(myUsername, p.to), msg);

        const recipients = resolveRecipients(p.to, myUsername);
        recipients.forEach(u => deliver(u, msg));
        if (!recipients.includes(myUsername)) ws.send(JSON.stringify(msg)); // echo отправителю
      }

      // ── EDIT ──
      if (p.type === 'edit_message') {
        if (!myUsername) return;
        const key = historyKey(myUsername, p.to);
        const m = (history[key] || []).find(x => x.id === p.messageId);
        if (m) { m.text = p.newText; m.edited = true; saveDB(); }
        const payload = { type: 'msg_edited', from: myUsername, to: p.to, groupId: p.groupId, messageId: p.messageId, newText: p.newText };
        resolveRecipients(p.to, myUsername).forEach(u => sendTo(u, payload));
        ws.send(JSON.stringify(payload));
      }

      // ── TYPING ──
      if (p.type === 'typing') {
        if (!myUsername) return;
        if (rateLimited()) return;
        const payload = { type: 'typing', from: myUsername, groupId: p.groupId };
        resolveRecipients(p.to, myUsername).forEach(u => sendTo(u, payload));
      }

      // ── REACTION ──
      if (p.type === 'reaction') {
        if (!myUsername) return;
        if (rateLimited()) return;
        const key = historyKey(myUsername, p.to);
        const m = (history[key] || []).find(x => x.id === p.messageId);
        if (m) { m.reactions = m.reactions || {}; m.reactions[p.reaction] = (m.reactions[p.reaction] || 0) + 1; saveDB(); }
        const payload = { type: 'new_reaction', from: myUsername, to: p.to, groupId: p.groupId, messageId: p.messageId, reaction: p.reaction };
        resolveRecipients(p.to, myUsername).forEach(u => sendTo(u, payload));
        ws.send(JSON.stringify(payload));
      }

      // ── PIN ──
      if (p.type === 'pin_message') {
        if (!myUsername) return;
        const payload = { type: 'message_pinned', from: myUsername, to: p.to, groupId: p.groupId, messageId: p.messageId, text: p.text };
        resolveRecipients(p.to, myUsername).forEach(u => sendTo(u, payload));
        ws.send(JSON.stringify(payload));
      }

      // ── CALLS (WebRTC signaling) ──
      if (['call_offer', 'video_offer', 'call_answer', 'ice_candidate', 'call_end'].includes(p.type)) {
        const target = activeSockets[p.to];
        if (target && target.readyState === WebSocket.OPEN) {
          p.from = myUsername;
          target.send(JSON.stringify(p));
        } else if (['call_offer', 'video_offer'].includes(p.type)) {
          ws.send(JSON.stringify({ type: 'call_end', from: p.to, reason: 'offline' }));
        }
      }

      // ── GROUP CREATED: реестр групп + инвайт ──
      if (p.type === 'group_created') {
        if (!myUsername) return;
        const gd = p.groupData || {};
        groups[p.groupId] = {
          name: gd.name || 'Группа',
          type: gd.type || 'group',
          members: Array.from(new Set([...(gd.members || []), myUsername]))
        };
        saveDB();
        sendTo(p.to, { type: 'group_invite', from: myUsername, groupId: p.groupId, groupData: gd });
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
    // ✅ выдаём оффлайн-очередь
    const queue = offline[username];
    if (queue && queue.length) {
      queue.forEach(m => ws.send(JSON.stringify(m)));
      delete offline[username];
      saveDB();
      console.log(`📥 Доставлено из оффлайна: ${queue.length} → ${username}`);
    }
    broadcastUserList();
  }
});

httpServer.listen(port, () => console.log(`HTTP + WS на порту ${port}`));
