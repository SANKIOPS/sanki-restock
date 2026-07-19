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
const { verifySession, roleCanAccessPath, landingFor } = require('./modules/auth-users');

// Static asset extensions any logged-in user may fetch regardless of role
// (JS/CSS/images the shared pages need). Page/navigation requests are gated.
const ASSET_EXT = new Set(['.js', '.css', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.woff', '.woff2', '.ttf', '.map', '.webp']);
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
    if (checkApiKey(req)) user = { username: 'automation', role: 'admin' };
    else if (checkBasic(req)) user = { username: process.env.DASH_USER || 'admin', role: 'admin' };
  }

  if (!user) {
    // API callers get a clean 401; browsers are sent to the login page
    // (instead of the old native Basic-auth popup).
    if (p.startsWith('/api/')) return res.status(401).json({ success: false, error: 'Unauthorized' });
    return res.redirect(302, '/login.html');
  }
  req.user = user;

  // Admin-only management APIs.
  if (p.startsWith('/api/admin')) {
    if (user.role !== 'admin') return res.status(403).json({ success: false, error: 'Admin only' });
    return next();
  }

  // Other APIs: any authenticated user for now (Phase 0). Sensitive future
  // modules (revenue/accounts) will add their own per-role checks.
  if (p.startsWith('/api/')) return next();

  // ── Page/navigation gating by role (admin sees everything) ──
  if (user.role === 'admin') return next();
  if (isAssetPath(p)) return next();                 // shared JS/CSS/images
  if (p === '/') return res.redirect(302, landingFor(user.role));
  if (roleCanAccessPath(user.role, p)) return next();
  // Any other page/route this role isn't allowed → send to their home.
  return res.redirect(302, landingFor(user.role));
}

module.exports = { gate };
