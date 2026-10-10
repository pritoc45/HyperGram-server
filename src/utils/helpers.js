import crypto from 'crypto';

export function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha512').toString('hex');
}

export function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

export function genId() {
  return Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
}