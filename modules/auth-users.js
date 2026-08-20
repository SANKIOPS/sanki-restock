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
const OWNER_USER = process.env.OWNER_USER || 'gaganlambasanki';

// Session-signing secret: prefer env; else persist a random one on the
// volume so it stays stable across restarts (a changed secret just logs
// everyone out — safe, not destructive).
const SECRET = (function () {
  if (process.env.SESSION_SECRET) return process.env.SESSION_SECRET;
  // Railway may briefly serve more than one instance during a deploy, and a
  // volume-backed random key is not guaranteed to be identical in every
  // instance. The existing bootstrap credentials are stable deployment
  // secrets, so derive a dedicated session key from them when no explicit
  // SESSION_SECRET is configured. No credential value is stored in the token.
  if (process.env.DASH_USER && process.env.DASH_PASS) {
    return crypto.createHash('sha256')
      .update('sanki-session-v1\0' + process.env.DASH_USER + '\0' + process.env.DASH_PASS)
      .digest('hex');
  }
  try { const s = fs.readFileSync(SECRET_PATH, 'utf8').trim(); if (s) return s; } catch { /* none yet */ }
  const s = crypto.randomBytes(32).toString('hex');
  try { fs.writeFileSync(SECRET_PATH, s); } catch (e) { console.error('[auth] could not persist session secret:', e.message); }
  return s;
})();

// ── Roles → which pages each role may open, and where they land ──
const ROLES = [
  { id: 'admin',       label: 'Admin (full access)' },
  { id: 'owner',       label: 'Owner (private Money Picture + accounting)' },
  { id: 'inventory',   label: 'Inventory' },
  { id: 'sales',       label: 'Sales' },
  { id: 'procurement', label: 'Fresh Procurement' },
  { id: 'accounting',  label: 'Accounting' },
  { id: 'claimant',    label: 'Claimant (log expenses only)' },
  { id: 'revenue',     label: 'Revenue / Accounts' },
  { id: 'warehouse',   label: 'Warehouse / Rack' },
  { id: 'stocksearch', label: 'Stock Search' }
];
const ROLE_IDS = ROLES.map(r => r.id);

// Everyone now lands on the clean launcher; it self-filters the tiles a
// role may see via /api/modules. (The old per-role landing pages are all
// still reachable — they're just not the front door anymore.)
const ROLE_HOME = {
  admin:       '/dashboard.html',
  owner:       '/owner.html',
  inventory:   '/dashboard.html',
  sales:       '/dashboard.html',
  procurement: '/dashboard.html',
  accounting:  '/dashboard.html',
  claimant:    '/expenses.html',
  revenue:     '/dashboard.html',
  warehouse:   '/dashboard.html',
  stocksearch: '/dashboard.html'
};
// '*' = all pages. Otherwise an allow-list of exact page paths. This is the
// DEFAULT (seed) map — the admin can override it per role (persisted in the
// user store under `rolePages`); admin always stays '*'.
const DEFAULT_ROLE_PAGES = {
  admin:       '*',
  owner:       '*',
  inventory:   ['/showroom-replenishment.html', '/inventory-stats.html', '/rack-locations.html', '/procurement.html'],
  sales:       ['/orders.html', '/sales.html', '/analytics.html', '/velocity.html'],
  procurement: ['/procurement.html'],
  accounting:  ['/accounting.html', '/expenses.html'],
  claimant:    ['/expenses.html'],
  revenue:     ['/orders.html', '/coming-soon.html'],
  warehouse:   ['/orders.html', '/rack-locations.html'],
  stocksearch: ['/rack-locations.html']
};

// The module registry pushes its catalog here at boot (avoids a circular
// require). Used to (a) seed each role's default pages from the modules that
// list that role, and (b) build the admin permissions grid.
let MODULE_CATALOG = [];
function registerModules(mods) { MODULE_CATALOG = Array.isArray(mods) ? mods : []; }
function modulePath(href) { return String(href || '').split('#')[0]; }
// Deduped catalog of pages the admin can grant, from the module registry.
function pageCatalog() {
  const seen = {}, out = [];
  MODULE_CATALOG.forEach(m => {
    const p = modulePath(m.href);
    if (!p || seen[p]) return;
    seen[p] = 1;
    out.push({ path: p, label: m.title || p, section: m.section || '' });
  });
  return out;
}
// Default pages for a role = its DEFAULT_ROLE_PAGES entry UNION every module
// path that lists the role. Reproduces current access + tiles before any edit.
function seedPagesForRole(role) {
  const set = new Set(Array.isArray(DEFAULT_ROLE_PAGES[role]) ? DEFAULT_ROLE_PAGES[role] : []);
  MODULE_CATALOG.forEach(m => { if ((m.roles || []).includes(role)) set.add(modulePath(m.href)); });
  return Array.from(set);
}
// The effective role→pages map (admin overrides layered over the seed).
function getRolePages() {
  const store = loadUsers();
  const overrides = store.rolePages || {};
  const out = {};
  ROLE_IDS.forEach(r => {
    if (r === 'admin' || r === 'owner') { out[r] = '*'; return; }
    out[r] = Array.isArray(overrides[r]) ? overrides[r] : seedPagesForRole(r);
  });
  return out;
}

// ── Multi-role helpers ───────────────────────────────────────────
// A user may hold SEVERAL roles; their access is the UNION. We keep a single
// `role` (the primary, = admin if held else the first) for legacy checks that
// still read one role, plus the full `roles` array.
function normalizeRoles(u) {
  let roles = Array.isArray(u.roles) ? u.roles.slice() : (u.role ? [u.role] : []);
  roles = roles.filter(r => ROLE_IDS.includes(r));
  return roles.length ? Array.from(new Set(roles)) : [];
}
function primaryRole(roles) { return roles.includes('owner') ? 'owner' : (roles.includes('admin') ? 'admin' : (roles[0] || '')); }
function repairMissingRoles(store, u) {
  let roles = normalizeRoles(u);
  // The confirmed founder must retain the distinct Owner role. Owner is a
  // superuser and unlocks the private Money Picture / OD dashboard.
  if (u.username === OWNER_USER && !roles.includes('owner')) {
    roles = Array.from(new Set(['owner'].concat(roles.filter(r => r !== 'admin'))));
    u.roles = roles.slice();
    delete u.role;
    saveUsers(store);
    console.warn('[auth] restored owner role');
    return roles;
  }
  if (roles.length) return roles;
  // Legacy users created before role enforcement can exist with a blank or
  // obsolete role. Restore the configured owner as admin; give every other
  // such account the least-privileged useful role instead of elevating it.
  roles = [u.username === OWNER_USER ? 'owner' : (u.username === process.env.DASH_USER ? 'admin' : 'claimant')];
  u.roles = roles.slice();
  delete u.role;
  saveUsers(store);
  console.warn('[auth] repaired missing role for an existing user');
  return roles;
}
function rolesOf(user) {
  if (!user) return [];
  if (Array.isArray(user.roles) && user.roles.length) return user.roles;
  return user.role ? [user.role] : [];
}

function landingFor(role) { return ROLE_HOME[role] || '/dashboard.html'; }
function allowedPagesFor(role) { const a = getRolePages()[role]; return a === '*' ? '*' : (a || []); }
function roleCanAccessPath(role, p) {
  // The launcher is the shared front door — every authenticated role may
  // open it (it filters its own contents by role via /api/modules).
  if (p === '/dashboard.html') return true;
  const a = getRolePages()[role];
  if (a === '*') return true;
  if (!a) return false;
  return a.includes(p);
}
// Union access across all of a user's roles.
function userCanAccessPath(user, p) { return rolesOf(user).some(r => roleCanAccessPath(r, p)); }
function allowedPagesForUser(user) {
  const roles = rolesOf(user);
  if (roles.some(r => getRolePages()[r] === '*')) return '*';
  const set = new Set();
  roles.forEach(r => { const a = getRolePages()[r]; if (Array.isArray(a)) a.forEach(x => set.add(x)); });
  return Array.from(set);
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
  const store = loadUsers();
  const u = store.users.find(x => x.username === p.u);
  if (!u) return null;
  const roles = repairMissingRoles(store, u);
  return { username: u.username, role: primaryRole(roles), roles };
}

// Privacy-safe live diagnostic for authentication incidents. It never returns
// the cookie, username, token payload, password data, or signing material.
function sessionDiagnostics(req) {
  const token = parseCookies(req)[COOKIE];
  const payload = readSession(token);
  const user = payload ? loadUsers().users.find(x => x.username === payload.u) : null;
  const roles = user ? normalizeRoles(user) : [];
  return {
    cookieReceived: !!token,
    signatureValid: !!payload,
    userFound: !!user,
    roleValid: roles.length > 0
  };
}

// ── Boot seed: first Admin from existing DASH_USER / DASH_PASS ────
function seedAdminIfEmpty() {
  const store = loadUsers();
  if (store.users.length) return;
  const u = process.env.DASH_USER, p = process.env.DASH_PASS;
  if (!u || !p) { console.warn('[auth] no users and DASH_USER/DASH_PASS unset — set them to seed the first admin'); return; }
  store.users.push({ username: u, roles: ['admin'], password: hashPassword(p), createdAt: Date.now(), seeded: true });
  saveUsers(store);
  console.log('[auth] seeded initial admin user "' + u + '" from DASH_USER');
}

// ── Public auth routes (login/logout are exempted in auth.js) ────
router.post('/api/auth/login', (req, res) => {
  const { username, password } = (req.body || {});
  if (!username || !password) return res.json({ success: false, error: 'Enter username and password' });
  const store = loadUsers();
  const u = store.users.find(x => x.username === String(username));
  if (!u || !verifyPassword(password, u.password)) return res.json({ success: false, error: 'Invalid username or password' });
  const roles = repairMissingRoles(store, u);
  const primary = primaryRole(roles);
  const token = signSession({ u: u.username, r: primary, exp: Date.now() + SESSION_TTL_MS });
  res.cookie(COOKIE, token, {
    httpOnly: true,
    secure: true,
    sameSite: 'lax',
    path: '/',
    maxAge: SESSION_TTL_MS
  });
  res.json({ success: true, username: u.username, role: primary, roles, home: landingFor(primary) });
});

router.post('/api/auth/logout', (req, res) => {
  res.clearCookie(COOKIE, { httpOnly: true, secure: true, sameSite: 'lax', path: '/' });
  res.json({ success: true });
});

router.get('/api/auth/me', (req, res) => {
  const user = req.user || verifySession(req);
  if (!user) return res.status(401).json({ success: false, error: 'Not logged in' });
  const roles = rolesOf(user);
  res.json({
    success: true,
    username: user.username,
    role: user.role,
    roles,
    isAdmin: roles.includes('admin'),
    home: landingFor(user.role),
    allowedPages: allowedPagesForUser(user)
  });
});

router.get('/api/auth/diagnostic', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json({ success: true, session: sessionDiagnostics(req) });
});

// ── Admin user management (gate already restricts /api/admin to admin) ──
function requireAdmin(req, res, next) {
  const roles = rolesOf(req.user);
  if (!roles.includes('admin') && !roles.includes('owner')) return res.status(403).json({ success: false, error: 'Admin or owner only' });
  next();
}

// Accept either a `roles` array or a single `role`; return a clean valid list.
function rolesFromBody(body) {
  let roles = Array.isArray(body.roles) ? body.roles : (body.role ? [body.role] : []);
  roles = roles.filter(r => ROLE_IDS.includes(r));
  return Array.from(new Set(roles));
}
function adminCount(store) { return store.users.filter(u => normalizeRoles(u).includes('admin')).length; }

// Safe internal lookup for modules that need to target an existing user
// without exposing password hashes or the underlying user store.
function userSummary(username) {
  const wanted = String(username || '').trim().toLowerCase();
  const user = loadUsers().users.find(u => String(u.username || '').toLowerCase() === wanted);
  if (!user) return null;
  const roles = normalizeRoles(user);
  return { username: user.username, role: primaryRole(roles), roles };
}

router.get('/api/admin/users', requireAdmin, (req, res) => {
  const users = loadUsers().users.map(u => {
    const roles = normalizeRoles(u);
    return { username: u.username, role: primaryRole(roles), roles, createdAt: u.createdAt || null };
  });
  res.json({ success: true, users, roles: ROLES, rolePages: getRolePages(), pageCatalog: pageCatalog() });
});

router.post('/api/admin/users', requireAdmin, (req, res) => {
  const b = req.body || {};
  const uname = String(b.username || '').trim();
  const roles = rolesFromBody(b);
  if (!uname) return res.json({ success: false, error: 'Username required' });
  if (!b.password || String(b.password).length < 4) return res.json({ success: false, error: 'Password must be at least 4 characters' });
  if (!roles.length) return res.json({ success: false, error: 'Pick at least one role' });
  const store = loadUsers();
  if (store.users.some(u => u.username === uname)) return res.json({ success: false, error: 'That username already exists' });
  store.users.push({ username: uname, roles, password: hashPassword(b.password), createdAt: Date.now() });
  saveUsers(store);
  res.json({ success: true, username: uname, roles });
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

// Set a user's FULL role set (multi-role). Accepts `roles` array (or single
// `role`). Blocks removing admin from the only admin (avoid lockout).
router.post('/api/admin/users/roles', requireAdmin, (req, res) => {
  const b = req.body || {};
  const roles = rolesFromBody(b);
  if (!roles.length) return res.json({ success: false, error: 'Pick at least one role' });
  const store = loadUsers();
  const u = store.users.find(x => x.username === String(b.username));
  if (!u) return res.json({ success: false, error: 'User not found' });
  const wasAdmin = normalizeRoles(u).includes('admin');
  if (wasAdmin && !roles.includes('admin') && adminCount(store) <= 1)
    return res.json({ success: false, error: 'Cannot remove admin from the only admin — create another admin first' });
  u.roles = roles; delete u.role;   // migrate to the array model
  saveUsers(store);
  res.json({ success: true, username: u.username, roles });
});

// Back-compat: single-role change → treated as the full set [role].
router.post('/api/admin/users/role', requireAdmin, (req, res) => {
  const { username, role } = (req.body || {});
  if (!ROLE_IDS.includes(role)) return res.json({ success: false, error: 'Pick a valid role' });
  const store = loadUsers();
  const u = store.users.find(x => x.username === String(username));
  if (!u) return res.json({ success: false, error: 'User not found' });
  if (normalizeRoles(u).includes('admin') && role !== 'admin' && adminCount(store) <= 1)
    return res.json({ success: false, error: 'Cannot change the only admin — create another admin first' });
  u.roles = [role]; delete u.role;
  saveUsers(store);
  res.json({ success: true, username: u.username, roles: u.roles });
});

// Edit a ROLE's page permissions. Body: { role, pages:[paths] }. Admin is
// locked to full access. Pages must be from the known page catalogue.
router.post('/api/admin/permissions', requireAdmin, (req, res) => {
  const b = req.body || {};
  const role = String(b.role || '');
  if (!ROLE_IDS.includes(role)) return res.json({ success: false, error: 'Unknown role' });
  if (role === 'admin') return res.json({ success: false, error: 'Admin always has full access — it cannot be limited.' });
  const known = pageCatalog().map(p => p.path);
  const pages = Array.from(new Set((Array.isArray(b.pages) ? b.pages : []).map(String).filter(p => known.includes(p))));
  const store = loadUsers();
  store.rolePages = store.rolePages || {};
  store.rolePages[role] = pages;
  saveUsers(store);
  res.json({ success: true, role, pages, rolePages: getRolePages() });
});

router.post('/api/admin/users/delete', requireAdmin, (req, res) => {
  const { username } = (req.body || {});
  const store = loadUsers();
  const u = store.users.find(x => x.username === String(username));
  if (!u) return res.json({ success: false, error: 'User not found' });
  if (normalizeRoles(u).includes('admin') && adminCount(store) <= 1)
    return res.json({ success: false, error: 'Cannot delete the only admin' });
  if (req.user && req.user.username === u.username)
    return res.json({ success: false, error: 'You cannot delete your own account while logged in' });
  store.users = store.users.filter(x => x.username !== u.username);
  saveUsers(store);
  res.json({ success: true, removed: u.username });
});

module.exports = {
  router, seedAdminIfEmpty, verifySession, landingFor, ROLES,
  roleCanAccessPath, userCanAccessPath,
  allowedPagesForUser, rolesOf, registerModules, userSummary
};
