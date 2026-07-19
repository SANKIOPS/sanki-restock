// ═══════════════════════════════════════════════════════════════
// modules/module-registry.js — the ONE source of truth for what the
// user sees. Both the launcher dashboard and the sidebar read this via
// GET /api/modules, so the two can never drift apart again.
//
// Nothing here deletes or disables backend routes — every page/API is
// still mounted and protected by the role gate (auth.js). This registry
// only controls VISIBILITY in the UI:
//     status: 'live'   → shown as an active tile / menu item
//             'coming' → shown greyed as "coming soon"
//             'hidden' → not shown at all (default for un-launched work)
//
// To surface a module: flip its status to 'live' and redeploy. That's the
// "reveal one by one" workflow — the backend was ready all along.
//
// roles = who may SEE it (admin always sees everything that isn't hidden).
// The server gate still enforces access independently, so this is purely
// about a clean, curated front door.
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const router = express.Router();

let verifySession;
try { verifySession = require('./auth-users').verifySession; } catch { verifySession = () => null; }

// Section order controls how groups stack in the sidebar / dashboard.
const SECTION_ORDER = ['Main', 'Sales', 'Store Ops', 'Procurement', 'Accounts', 'Marketing', 'Admin'];

// ── The registry ────────────────────────────────────────────────
// Everything starts 'hidden' (blank launcher). We turn modules 'live'
// deliberately, one at a time, as we build/bless them together.
const MODULES = [
  { key: 'restock',      title: 'Restock Planner',       desc: 'Reorder fast-movers before they run out',            icon: '🔄', href: '/index.html#restock',            section: 'Main',      status: 'hidden', roles: ['admin', 'inventory'] },
  { key: 'velocity',     title: 'Velocity Intelligence', desc: 'How fast every product is selling',                  icon: '⚡', href: '/velocity.html',                  section: 'Main',      status: 'hidden', roles: ['admin', 'sales', 'inventory'] },
  { key: 'analytics',    title: 'Analytics',             desc: 'Sales & performance dashboards',                     icon: '📊', href: '/analytics.html',                 section: 'Main',      status: 'hidden', roles: ['admin', 'sales'] },
  { key: 'inv-stats',    title: 'Inventory Statistics',  desc: 'Stock levels & valuation',                           icon: '📈', href: '/inventory-stats.html',           section: 'Main',      status: 'hidden', roles: ['admin', 'inventory'] },
  { key: 'stock-search', title: 'Stock Search',          desc: 'Find any SKU and where it is',                       icon: '🔍', href: '/rack-locations.html',            section: 'Main',      status: 'hidden', roles: ['admin', 'stocksearch', 'warehouse'] },

  { key: 'sales',        title: 'Record a Sale',         desc: 'Log walk-in / IG / WhatsApp / phone sales',          icon: '🧾', href: '/sales.html',                     section: 'Sales',     status: 'hidden', roles: ['admin', 'sales'] },

  { key: 'showroom',     title: 'Showroom Replenishment', desc: 'Refill the showroom front from the back',           icon: '🛍️', href: '/showroom-replenishment.html',   section: 'Store Ops', status: 'hidden', roles: ['admin', 'inventory', 'warehouse'] },
  { key: 'racks',        title: 'Rack Locations',        desc: 'Where every SKU sits on the racks',                  icon: '🗄️', href: '/rack-locations.html',           section: 'Store Ops', status: 'hidden', roles: ['admin', 'warehouse'] },

  { key: 'users',        title: 'Users & Roles',         desc: 'Add staff and set what they can access',             icon: '👥', href: '/admin-users.html',               section: 'Admin',     status: 'live',   roles: ['admin'] }
];

// ── Visibility resolution ───────────────────────────────────────
function visibleFor(user) {
  const role = user && user.role;
  const isAdmin = role === 'admin';
  return MODULES
    .filter(m => m.status !== 'hidden')
    .filter(m => isAdmin || (m.roles || []).includes(role))
    .map(m => ({ key: m.key, title: m.title, desc: m.desc, icon: m.icon, href: m.href, section: m.section, status: m.status }))
    .sort((a, b) => {
      const sa = SECTION_ORDER.indexOf(a.section), sb = SECTION_ORDER.indexOf(b.section);
      return (sa < 0 ? 99 : sa) - (sb < 0 ? 99 : sb);
    });
}

router.get('/api/modules', (req, res) => {
  const user = req.user || verifySession(req);
  if (!user) return res.status(401).json({ success: false, error: 'Not logged in' });
  res.json({ success: true, sectionOrder: SECTION_ORDER, modules: visibleFor(user) });
});

module.exports = { router, MODULES, visibleFor };
