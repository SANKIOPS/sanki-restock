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
  // Always block debug endpoints in production (finding #3)
  if (req.path.startsWith('/api/debug')) {
    return res.status(404).json({ success: false, error: 'Not found' });
  }

  // Exempt external webhooks (authenticated by signature) and health/keep-alive
  if (req.path.startsWith('/api/webhooks/') ||
      req.path === '/api/health' ||
      req.path === '/healthz') {
    return next();
  }

  if (checkApiKey(req) || checkBasic(req)) return next();

  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }
  res.set('WWW-Authenticate', 'Basic realm="SANKI Business OS", charset="UTF-8"');
  return res.status(401).send('Authentication required.');
}

module.exports = { gate };
