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

let verifySession, allowedPagesForUser, rolesOf, registerModules;
try {
  const au = require('./auth-users');
  verifySession = au.verifySession;
  allowedPagesForUser = au.allowedPagesForUser;
  rolesOf = au.rolesOf;
  registerModules = au.registerModules;
} catch {
  verifySession = () => null;
  allowedPagesForUser = () => [];
  rolesOf = (u) => (u && u.roles) || (u && u.role ? [u.role] : []);
  registerModules = () => {};
}

// Section order controls how groups stack in the sidebar / dashboard.
const SECTION_ORDER = ['Main', 'Sales', 'Store Ops', 'Purchases', 'Accounts', 'Marketing', 'Admin'];

// ── The registry ────────────────────────────────────────────────
// Everything starts 'hidden' (blank launcher). We turn modules 'live'
// deliberately, one at a time, as we build/bless them together.
const MODULES = [
  { key: 'restock',      title: 'Restock Planner',       desc: 'Reorder fast-movers before they run out',            icon: '🔄', href: '/index.html#restock',            section: 'Main',      status: 'live', roles: ['admin', 'inventory'] },
  { key: 'velocity',     title: 'Velocity Intelligence', desc: 'How fast every product is selling',                  icon: '⚡', href: '/velocity.html',                  section: 'Main',      status: 'live', roles: ['admin', 'sales', 'inventory'] },
  { key: 'analytics',    title: 'Analytics',             desc: 'Sales & performance dashboards',                     icon: '📊', href: '/analytics.html',                 section: 'Main',      status: 'live', roles: ['admin', 'sales'] },
  { key: 'inv-stats',    title: 'Inventory Statistics',  desc: 'Stock levels & valuation',                           icon: '📈', href: '/inventory-stats.html',           section: 'Main',      status: 'live', roles: ['admin', 'inventory'] },
  { key: 'stock-search', title: 'Stock Search',          desc: 'Find any SKU and where it is',                       icon: '🔍', href: '/rack-locations.html',            section: 'Main',      status: 'live', roles: ['admin', 'stocksearch', 'warehouse'] },

  { key: 'orders',       title: 'Orders',                desc: 'Every Shopify sale (POS + Website) + dispatch tracking', icon: '🧾', href: '/orders.html',                section: 'Sales',     status: 'live',   roles: ['admin', 'sales', 'revenue', 'warehouse'] },
  // Retired: manual POS entry duplicated Shopify POS. Superseded by Orders (read-only Shopify mirror). Kept hidden.
  { key: 'sales',        title: 'Record a Sale',         desc: 'Log walk-in / IG / WhatsApp / phone sales',          icon: '📝', href: '/sales.html',                     section: 'Sales',     status: 'hidden', roles: ['admin', 'sales'] },

  { key: 'procurement', title: 'Purchases',              desc: 'Generate SKUs, landed cost, create products / add stock', icon: '📦', href: '/procurement.html',           section: 'Purchases', status: 'live',   roles: ['admin', 'procurement', 'inventory'] },
  { key: 'fresh-proc',  title: 'Fresh Procurement',      desc: 'Decide what NEW fashion to source — funkiness tiers + live market trends', icon: '🧠', href: '/fresh-procurement.html', section: 'Purchases', status: 'live',   roles: ['admin', 'procurement'] },
  { key: 'size-tracker', title: 'Size Tracker',           desc: 'Match vendor (China) size charts to SANKI target sizes before every PO', icon: '📏', href: '/size-tracker.html',      section: 'Purchases', status: 'live',   roles: ['admin', 'procurement'] },

  { key: 'pl',           title: 'Accounting',            desc: 'Live P&L — Sales & COGS split POS / Website / Combined', icon: '💰', href: '/accounting.html',            section: 'Accounts',  status: 'live',   roles: ['admin', 'accounting', 'revenue'] },
  { key: 'expenses',     title: 'Expenses',              desc: 'Log → approve (bill proof) → pay (payment proof); cash balances', icon: '🧾', href: '/expenses.html',    section: 'Accounts',  status: 'live',   roles: ['admin', 'accounting', 'claimant'] },

  { key: 'showroom',     title: 'Showroom Replenishment', desc: 'Refill the showroom front from the back',           icon: '🛍️', href: '/showroom-replenishment.html',   section: 'Store Ops', status: 'live', roles: ['admin', 'inventory', 'warehouse'] },
  { key: 'racks',        title: 'Rack Locations',        desc: 'Where every SKU sits on the racks',                  icon: '🗄️', href: '/rack-locations.html',           section: 'Store Ops', status: 'live', roles: ['admin', 'warehouse'] },

  { key: 'users',        title: 'Users & Roles',         desc: 'Add staff and set what they can access',             icon: '👥', href: '/admin-users.html',               section: 'Admin',     status: 'live',   roles: ['admin'] }
];

// Hand the module list to auth-users so role-permission seeding knows which
// page each module lives at (union'd with DEFAULT_ROLE_PAGES). Safe no-op if
// auth-users wasn't loaded.
registerModules(MODULES);

// Strip any '#anchor' so a module href maps to the underlying page path the
// permission map is keyed on (e.g. '/index.html#restock' → '/index.html').
function modulePath(href) { return String(href || '').split('#')[0]; }

// ── Visibility resolution ───────────────────────────────────────
function visibleFor(user) {
  const isAdmin = rolesOf(user).includes('admin');
  // Non-admins: visibility follows the (editable) role→pages permission map,
  // union'd across all of the user's roles. A module shows when its page path
  // is in that allow-list. Admin sees everything not hidden.
  const allowed = isAdmin ? '*' : allowedPagesForUser(user);
  const canSee = (m) => isAdmin || (Array.isArray(allowed) && allowed.includes(modulePath(m.href)));
  return MODULES
    .filter(m => m.status !== 'hidden')
    .filter(canSee)
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
