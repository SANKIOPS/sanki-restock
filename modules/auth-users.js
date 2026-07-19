// ═══════════════════════════════════════════════════════════════
// modules/auth-users.js — Phase 0: per-person logins + roles
//
// Turns the single shared password into a real multi-user system.
// Each employee gets their own username + password; their ROLE decides
// which departments (pages) they can open. Admin sees everything and
// manages users from /admin-users.html.
//
// No new dependencies and no database — users live in a JSON file on the
// same /data volume the rest of the app uses (survives redeploys).
// Sessions are stateless signed cookies (HMAC), so there's nothing to
// store server-side and nothing to expire manually.
//
// NO-LOCKOUT MIGRATION: on boot, if there are no users yet, we seed the
// first Admin from the existing DASH_USER / DASH_PASS env vars. So the
// credentials you already use keep working — now via the login page — and
// the raw Basic-auth header still works for scripts/automation (gate.js).
//
// Exports:
//   router            → login/logout/me + admin user-management endpoints
//   seedAdminIfEmpty  → call once at boot
//   verifySession     → (req) -> {username, role} | null   (used by auth.js)
//   roleCanAccessPath → (role, path) -> bool               (used by auth.js)
//   landingFor        → (role) -> path                     (used by auth.js)
//   ROLES             → [{id,label}] for the admin UI
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const crypto  = require('crypto');

const router = express.Router();

// ── Paths & secret (on the persistent volume) ────────────────────
const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const USERS_PATH = process.env.USERS_PATH || path.join(DATA_DIR, 'users.json');
const SECRET_PATH = path.join(DATA_DIR, 'session_secret.key');
const SESSION_TTL_MS = 12 * 60 * 60 * 1000; // 12h
const COOKIE = 'sanki_session';

// Session-signing secret: prefer env; else persist a random one on the
// volume so it stays stable across restarts (a changed secret just logs
// everyone out — safe, not destructive).
const SECRET = (function () {
  if (process.env.SESSION_SECRET) return process.env.SESSION_SECRET;
  try { const s = fs.readFileSync(SECRET_PATH, 'utf8').trim(); if (s) return s; } catch { /* none yet */ }
  const s = crypto.randomBytes(32).toString('hex');
  try { fs.writeFileSync(SECRET_PATH, s); } catch (e) { console.error('[auth] could not persist session secret:', e.message); }
  return s;
})();

// ── Roles → which pages each role may open, and where they land ──
const ROLES = [
  { id: 'admin',       label: 'Admin (full access)' },
  { id: 'inventory',   label: 'Inventory' },
  { id: 'sales',       label: 'Sales' },
  { id: 'procurement', label: 'Fresh Procurement' },
  { id: 'revenue',     label: 'Revenue / Accounts' },
  { id: 'warehouse',   label: 'Warehouse / Rack' },
  { id: 'stocksearch', label: 'Stock Search' }
];
const ROLE_IDS = ROLES.map(r => r.id);

const ROLE_HOME = {
  admin:       '/index.html',
  inventory:   '/showroom-replenishment.html',
  sales:       '/analytics.html',
  procurement: '/coming-soon.html?m=Fresh%20Procurement',
  revenue:     '/coming-soon.html?m=Revenue%20%2F%20Accounts',
  warehouse:   '/rack-locations.html',
  stocksearch: '/rack-locations.html'
};
// '*' = all pages. Otherwise an allow-list of exact page paths.
const ROLE_PAGES = {
  admin:       '*',
  inventory:   ['/showroom-replenishment.html', '/inventory-stats.html', '/rack-locations.html'],
  sales:       ['/analytics.html', '/velocity.html'],
  procurement: ['/coming-soon.html'],
  revenue:     ['/coming-soon.html'],
  warehouse:   ['/rack-locations.html'],
  stocksearch: ['/rack-locations.html']
};

function landingFor(role) { return ROLE_HOME[role] || '/login.html'; }
function allowedPagesFor(role) { return ROLE_PAGES[role] || []; }
function roleCanAccessPath(role, p) {
  const a = ROLE_PAGES[role];
  if (a === '*') return true;
  if (!a) return false;
  return a.includes(p);
}

// ── User store (atomic JSON on the volume) ───────────────────────
function atomicWrite(filePath, data) {
  const tmp = filePath + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filePath);
}
function loadUsers() {
  try { const s = JSON.parse(fs.readFileSync(USERS_PATH, 'utf8')); if (!Array.isArray(s.users)) s.users = []; return s; }
  catch { return { users: [] }; }
}
function saveUsers(store) { atomicWrite(USERS_PATH, JSON.stringify(store, null, 2)); }

// ── Password hashing (scrypt, built-in — no deps) ────────────────
function hashPassword(pw) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(String(pw), salt, 64).toString('hex');
  return salt + ':' + hash;
}
function verifyPassword(pw, stored) {
  if (!stored || stored.indexOf(':') < 0) return false;
  const [salt, hash] = stored.split(':');
  let test;
  try { test = crypto.scryptSync(String(pw), salt, 64).toString('hex'); } catch { return false; }
  const a = Buffer.from(test, 'hex'), b = Buffer.from(hash, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// ── Sessions (stateless signed cookie) ───────────────────────────
function parseCookies(req) {
  const out = {}; const h = req.headers.cookie; if (!h) return out;
  h.split(';').forEach(part => {
    const i = part.indexOf('=');
    if (i > 0) { out[part.slice(0, i).trim()] = decodeURIComponent(part.slice(i + 1).trim()); }
  });
  return out;
}
function signSession(payload) {
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig = crypto.createHmac('sha256', SECRET).update(body).digest('base64url');
  return body + '.' + sig;
}
function readSession(token) {
  if (!token) return null;
  const dot = token.lastIndexOf('.');
  if (dot < 0) return null;
  const body = token.slice(0, dot), sig = token.slice(dot + 1);
  const expect = crypto.createHmac('sha256', SECRET).update(body).digest('base64url');
  const a = Buffer.from(sig), b = Buffer.from(expect);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  let p;
  try { p = JSON.parse(Buffer.from(body, 'base64url').toString('utf8')); } catch { return null; }
  if (!p || !p.exp || Date.now() > p.exp) return null;
  return p;
}
// verifySession also confirms the user still exists / current role, so a
// deleted or role-changed user can't ride an old cookie.
function verifySession(req) {
  const p = readSession(parseCookies(req)[COOKIE]);
  if (!p) return null;
  const u = loadUsers().users.find(x => x.username === p.u);
  if (!u) return null;
  return { username: u.username, role: u.role };
}

// ── Boot seed: first Admin from existing DASH_USER / DASH_PASS ────
function seedAdminIfEmpty() {
  const store = loadUsers();
  if (store.users.length) return;
  const u = process.env.DASH_USER, p = process.env.DASH_PASS;
  if (!u || !p) { console.warn('[auth] no users and DASH_USER/DASH_PASS unset — set them to seed the first admin'); return; }
  store.users.push({ username: u, role: 'admin', password: hashPassword(p), createdAt: Date.now(), seeded: true });
  saveUsers(store);
  console.log('[auth] seeded initial admin user "' + u + '" from DASH_USER');
}

// ── Public auth routes (login/logout are exempted in auth.js) ────
router.post('/api/auth/login', (req, res) => {
  const { username, password } = (req.body || {});
  if (!username || !password) return res.json({ success: false, error: 'Enter username and password' });
  const u = loadUsers().users.find(x => x.username === String(username));
  if (!u || !verifyPassword(password, u.password)) return res.json({ success: false, error: 'Invalid username or password' });
  const token = signSession({ u: u.username, r: u.role, exp: Date.now() + SESSION_TTL_MS });
  res.cookie(COOKIE, token, { httpOnly: true, secure: true, sameSite: 'lax', maxAge: SESSION_TTL_MS });
  res.json({ success: true, username: u.username, role: u.role, home: landingFor(u.role) });
});

router.post('/api/auth/logout', (req, res) => {
  res.clearCookie(COOKIE);
  res.json({ success: true });
});

router.get('/api/auth/me', (req, res) => {
  const user = req.user || verifySession(req);
  if (!user) return res.status(401).json({ success: false, error: 'Not logged in' });
  res.json({
    success: true,
    username: user.username,
    role: user.role,
    isAdmin: user.role === 'admin',
    home: landingFor(user.role),
    allowedPages: allowedPagesFor(user.role)
  });
});

// ── Admin user management (gate already restricts /api/admin to admin) ──
function requireAdmin(req, res, next) {
  if (!req.user || req.user.role !== 'admin') return res.status(403).json({ success: false, error: 'Admin only' });
  next();
}

router.get('/api/admin/users', requireAdmin, (req, res) => {
  const users = loadUsers().users.map(u => ({ username: u.username, role: u.role, createdAt: u.createdAt || null }));
  res.json({ success: true, users, roles: ROLES });
});

router.post('/api/admin/users', requireAdmin, (req, res) => {
  const { username, password, role } = (req.body || {});
  const uname = String(username || '').trim();
  if (!uname) return res.json({ success: false, error: 'Username required' });
  if (!password || String(password).length < 4) return res.json({ success: false, error: 'Password must be at least 4 characters' });
  if (!ROLE_IDS.includes(role)) return res.json({ success: false, error: 'Pick a valid role' });
  const store = loadUsers();
  if (store.users.some(u => u.username === uname)) return res.json({ success: false, error: 'That username already exists' });
  store.users.push({ username: uname, role, password: hashPassword(password), createdAt: Date.now() });
  saveUsers(store);
  res.json({ success: true, username: uname, role });
});

router.post('/api/admin/users/reset', requireAdmin, (req, res) => {
  const { username, password } = (req.body || {});
  if (!password || String(password).length < 4) return res.json({ success: false, error: 'Password must be at least 4 characters' });
  const store = loadUsers();
  const u = store.users.find(x => x.username === String(username));
  if (!u) return res.json({ success: false, error: 'User not found' });
  u.password = hashPassword(password);
  saveUsers(store);
  res.json({ success: true });
});

router.post('/api/admin/users/role', requireAdmin, (req, res) => {
  const { username, role } = (req.body || {});
  if (!ROLE_IDS.includes(role)) return res.json({ success: false, error: 'Pick a valid role' });
  const store = loadUsers();
  const u = store.users.find(x => x.username === String(username));
  if (!u) return res.json({ success: false, error: 'User not found' });
  // Don't allow removing the last admin's admin role (avoid lockout).
  if (u.role === 'admin' && role !== 'admin' && store.users.filter(x => x.role === 'admin').length <= 1)
    return res.json({ success: false, error: 'Cannot change the only admin — create another admin first' });
  u.role = role;
  saveUsers(store);
  res.json({ success: true, username: u.username, role });
});

router.post('/api/admin/users/delete', requireAdmin, (req, res) => {
  const { username } = (req.body || {});
  const store = loadUsers();
  const u = store.users.find(x => x.username === String(username));
  if (!u) return res.json({ success: false, error: 'User not found' });
  if (u.role === 'admin' && store.users.filter(x => x.role === 'admin').length <= 1)
    return res.json({ success: false, error: 'Cannot delete the only admin' });
  if (req.user && req.user.username === u.username)
    return res.json({ success: false, error: 'You cannot delete your own account while logged in' });
  store.users = store.users.filter(x => x.username !== u.username);
  saveUsers(store);
  res.json({ success: true, removed: u.username });
});

module.exports = { router, seedAdminIfEmpty, verifySession, roleCanAccessPath, landingFor, ROLES };
