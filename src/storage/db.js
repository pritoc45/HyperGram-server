import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DB_FILE = process.env.DB_FILE || path.join(__dirname, '../../db.json');

let db = { users: {}, history: {}, groups: {} };

try {
  if (fs.existsSync(DB_FILE)) {
    db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  }
} catch (e) {
  console.error('DB load fail:', e.message);
}

export const users = db.users || (db.users = {});
export const history = db.history || (db.history = {});
export const groups = db.groups || (db.groups = {});

let saveTimer = null;

export function persist() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try {
      fs.writeFileSync(DB_FILE, JSON.stringify(db));
    } catch (e) {
      console.error('DB save fail:', e.message);
    }
  }, 800);
}

export { db };