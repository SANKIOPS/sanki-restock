// ═══════════════════════════════════════════════════════════════
// auth.js — authentication gate for SANKI Business OS
// Fixes audit findings #1–#4:
//   #1 unauthenticated /api/* (catalog, orders, customers leaked)
//   #2 public POST /api/inventory/adjust (writes live Shopify stock)
//   #3 /api/debug/* data-leaking endpoints
//   #4 over-sharing /api/health
//
// USAGE in server.js: add `const { gate } = require('./auth');`
// then `app.use(gate);` IMMEDIATELY BEFORE `app.use(express.static(...))`.
//
// Railway → Variables:
//   DASH_USER  = username you choose
//   DASH_PASS  = strong password you choose
//   API_KEY    = long random string (openssl rand -hex 24) for scripts/automation
//
// Behaviour:
//   • Pages + /api/* in the browser → HTTP Basic Auth (the browser auto-sends
//     the saved credentials to same-origin fetch calls, so the dashboard's
//     fetch('/api/products') keeps working with no client changes).
//   • Scripts/automation → send header  x-api-key: <API_KEY>.
//   • EXEMPT (no auth): /api/webhooks/* (Shopify/Velocity call these and
//     authenticate via their own signatures) and /api/health + /healthz
//     (used by the keep-alive self-ping and uptime monitors).
//   • /api/debug/* is always 404 in production.
//   • If DASH_USER/DASH_PASS are unset the gate FAILS CLOSED (denies all)
//     so the app can never ship wide-open again.
// ═══════════════════════════════════════════════════════════════
const crypto = require('crypto');
const path   = require('path');
// Phase 0: per-person logins + roles live in modules/auth-users. The gate
// stays the single chokepoint but now also accepts a valid session cookie
// and enforces which pages each role may open. Basic-auth + x-api-key are
// kept as admin fallbacks so scripts/automation and the bootstrap admin
// never get locked out.
const { verifySession, userCanAccessPath, rolesOf, landingFor } = require('./modules/auth-users');

// Static asset extensions any logged-in user may fetch regardless of role
// (JS/CSS/images the shared pages need). Page/navigation requests are gated.
const ASSET_EXT = new Set(['.js', '.css', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.woff', '.woff2', '.ttf', '.map', '.webp', '.webmanifest']);
function isAssetPath(p) {
  const m = p.match(/\.[a-z0-9]+$/i);
  return m ? ASSET_EXT.has(m[0].toLowerCase()) : false;
}

// Normalize the request path before making any auth decision. Express does NOT
// collapse `..` or decode `%2f` in req.path, so a request like
// `/api/webhooks/..%2fproducts` would otherwise keep the `/api/webhooks/` prefix
// (→ wrongly exempted) while routing/static resolves elsewhere. We percent-decode
// and POSIX-normalize so exemption/blocking decisions match what actually gets served.
function normalizedPath(req) {
  let p = req.path || '/';
  try { p = decodeURIComponent(p); } catch { /* malformed % — fall through; won't match exemptions */ }
  p = p.replace(/\\/g, '/');                 // treat backslashes as separators
  p = path.posix.normalize(p);               // collapse ../ and ./
  if (!p.startsWith('/')) p = '/' + p;       // normalize() can drop leading slash on '..'
  return p;
}

function safeEqual(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return crypto.timingSafeEqual(ba, bb);
}

function checkBasic(req) {
  const USER = process.env.DASH_USER;
  const PASS = process.env.DASH_PASS;
  if (!USER || !PASS) return false; // fail closed
  const header = req.headers.authorization || '';
  if (!header.startsWith('Basic ')) return false;
  let decoded;
  try { decoded = Buffer.from(header.slice(6), 'base64').toString('utf8'); }
  catch { return false; }
  const idx = decoded.indexOf(':');
  if (idx < 0) return false;
  return safeEqual(decoded.slice(0, idx), USER) && safeEqual(decoded.slice(idx + 1), PASS);
}

function checkApiKey(req) {
  const KEY = process.env.API_KEY;
  if (!KEY) return false;
  const provided = req.headers['x-api-key'] || '';
  return provided.length > 0 && safeEqual(provided, KEY);
}

// API authorization is deliberately centralized here so adding a page cannot
// accidentally expose its backend to every logged-in employee. Rules are
// checked in order; more-specific paths must come before broader prefixes.
const API_ROLE_RULES = [
  { path: '/api/auth/me',                 roles: '*' },
  { path: '/api/modules',                 roles: '*' },
  { prefix: '/api/admin',                 roles: ['admin'] },
  { prefix: '/api/setup/',                roles: ['admin'] },
  { prefix: '/api/expenses',              roles: ['admin', 'accounting', 'claimant'] },
  { prefix: '/api/pl/',                   roles: ['admin', 'accounting', 'revenue'] },
  { prefix: '/api/procurement/',          roles: ['admin', 'procurement'] },
  { prefix: '/api/fresh/',                roles: ['admin', 'procurement'] },
  { prefix: '/api/casuals/',              roles: ['admin', 'procurement'] },
  { prefix: '/api/sizetracker/',          roles: ['admin', 'procurement'] },
  { prefix: '/api/sales',                 roles: ['admin', 'sales'] },
  { prefix: '/api/racks',                 roles: ['admin', 'inventory', 'warehouse', 'stocksearch'] },
  { prefix: '/api/stock-search',          roles: ['admin', 'inventory', 'warehouse', 'stocksearch'] },
  { prefix: '/api/showroom/',             roles: ['admin', 'inventory', 'warehouse'] },
  { prefix: '/api/velocity/',             roles: ['admin', 'sales'] },
  { prefix: '/api/orders-ledger',         roles: ['admin', 'sales', 'revenue', 'warehouse'] },
  { prefix: '/api/orders',                roles: ['admin', 'sales', 'revenue', 'warehouse'] },
  { path: '/api/shopify/customers',       roles: ['admin', 'sales'] },
  { path: '/api/inventory/adjust',        roles: ['admin', 'inventory', 'warehouse'] },
  { prefix: '/api/whatsapp/',             roles: ['admin', 'sales'] },
  { path: '/api/data/save',               roles: ['admin', 'inventory'] },
  { path: '/api/data/load',               roles: ['admin', 'inventory', 'warehouse'] },
  { path: '/api/products',                roles: ['admin', 'inventory', 'sales', 'procurement', 'warehouse', 'stocksearch'] },
  { path: '/api/inventory',               roles: ['admin', 'inventory', 'sales', 'warehouse', 'stocksearch'] },
  { path: '/api/collections',             roles: ['admin', 'inventory', 'sales', 'procurement'] },
  { path: '/api/identify-product',        roles: ['admin', 'inventory', 'procurement'] },
  { path: '/api/health/full',             roles: ['admin'] }
];

function apiRuleFor(p) {
  return API_ROLE_RULES.find(rule =>
    (rule.path && p === rule.path) ||
    (rule.prefix && p.startsWith(rule.prefix))
  ) || null;
}

function apiAllowedForUser(user, p) {
  const userRoles = rolesOf(user);
  if (userRoles.includes('admin')) return true;
  const rule = apiRuleFor(p);
  if (!rule) return false; // new APIs must opt in instead of silently opening
  if (rule.roles === '*') return true;
  return rule.roles.some(role => userRoles.includes(role));
}

function gate(req, res, next) {
  // Decide on the NORMALIZED path so encoded/`..` traversal can't bypass the gate.
  const p = normalizedPath(req);

  // Always block debug endpoints in production (finding #3)
  if (p.startsWith('/api/debug')) {
    return res.status(404).json({ success: false, error: 'Not found' });
  }

  // PUBLIC (no auth): external webhooks (signature-authed), health/keep-alive,
  // and the login page + login/logout endpoints (so people can get in).
  if (p.startsWith('/api/webhooks/') ||
      p === '/api/health' ||
      p === '/healthz' ||
      p === '/login.html' ||
      p === '/api/auth/login' ||
      p === '/api/auth/logout') {
    return next();
  }

  // Identify the caller: a valid session cookie, else an admin fallback
  // (x-api-key for automation, Basic-auth for the bootstrap/admin login).
  let user = verifySession(req);
  if (!user) {
    if (checkApiKey(req)) user = { username: 'automation', role: 'admin', roles: ['admin'] };
    else if (checkBasic(req)) user = { username: process.env.DASH_USER || 'admin', role: 'admin', roles: ['admin'] };
  }

  if (!user) {
    // API callers get a clean 401; browsers are sent to the login page
    // (instead of the old native Basic-auth popup).
    if (p.startsWith('/api/')) return res.status(401).json({ success: false, error: 'Unauthorized' });
    return res.redirect(302, '/login.html');
  }
  req.user = user;

  // API permissions are enforced independently of page visibility. Unknown
  // endpoints fail closed for non-admin users until a rule is added above.
  if (p.startsWith('/api/')) {
    if (!apiAllowedForUser(user, p)) {
      return res.status(403).json({ success: false, error: 'Forbidden for this role' });
    }
    return next();
  }

  // ── Page/navigation gating by role (admin sees everything) ──
  if (rolesOf(user).includes('admin')) return next();
  if (isAssetPath(p)) return next();                 // shared JS/CSS/images
  if (p === '/') return res.redirect(302, landingFor(user.role));
  if (userCanAccessPath(user, p)) return next();     // union across all roles
  // Any other page/route this role isn't allowed → send to their home.
  return res.redirect(302, landingFor(user.role));
}

module.exports = { gate, apiRuleFor, apiAllowedForUser };
