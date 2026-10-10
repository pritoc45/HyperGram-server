import http from 'http';
import { WebSocketServer } from 'ws';
import { handleConnection, init, getActiveSockets } from './src/ws/handlers.js';
import { users } from './src/storage/db.js';

const port = process.env.PORT || 8080;
const MAX_PAYLOAD = 8 * 1024 * 1024;

const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, users: Object.keys(users).length, online: Object.keys(getActiveSockets()).length }));
  }
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end('HyperGram Server OK');
});

const wss = new WebSocketServer({ server: httpServer, maxPayload: MAX_PAYLOAD });
init(wss);

wss.on('connection', handleConnection);

httpServer.listen(port, () => console.log(`HyperGram server running on port ${port}`));

setInterval(() => {
  http.get(`http://localhost:${port}/health`, () => {});
  wss.clients.forEach(ws => { if (ws.readyState === 1) ws.ping(); });
}, 14 * 60 * 1000);