// ═══════════════════════════════════════════════════════════════
// modules/orders.js — THE SALES SPINE
//
// A strictly READ-ONLY mirror of every Shopify order (POS + Website),
// from the very first order onward, into a permanent local ledger on the
// /data volume. This is the database every downstream calculation stands
// on — accounting, restocking, fresh procurement, unit-economics.
//
//   • Backfill: pull ALL history via Link-header pagination.
//   • Stay synced: a lightweight internal poller runs an incremental sync
//     (updated_at_min) every few minutes, plus a manual "Sync now".
//   • NEVER writes back to Shopify and NEVER touches inventory. Read-only.
//
// It also absorbs the old "Manual POS Dispatch" Google Sheet: each order
// carries an editable in-app DISPATCH layer (courier, tracking URL,
// packing status, order confirmation, COD paid/due, remarks) so every
// order is tracked end-to-end on ONE page. Order core = read-only from
// Shopify; dispatch layer = in-app CRUD (the app is the source of truth,
// retiring the dispatch sheet).
//
// Endpoints (all behind the auth gate). NOTE the /api/orders-ledger/* base:
// server.js already owns a legacy GET/POST /api/orders (the velocity/analytics
// COD cache, different shape) that is registered first and would otherwise
// shadow these. Keeping this module on its own namespace avoids the clash.
//   POST /api/orders-ledger/sync         → backfill (first run) or incremental
//   GET  /api/orders-ledger/sync/status  → last sync meta
//   GET  /api/orders-ledger              → filtered list + summary
//   GET  /api/orders-ledger/:id          → one order (with dispatch layer)
//   POST /api/orders-ledger/:id/dispatch → update the editable dispatch fields
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const fetch   = require('node-fetch');

const router = express.Router();

const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
const SHOPIFY_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

const ORDERS_PATH = process.env.ORDERS_PATH ||
  path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'),
            'orders.json');

// Fields we pull from Shopify (keep the payload lean but complete).
const ORDER_FIELDS = [
  'id', 'name', 'order_number', 'created_at', 'updated_at', 'processed_at', 'cancelled_at',
  'source_name', 'financial_status', 'fulfillment_status', 'currency',
  'subtotal_price', 'total_discounts', 'total_tax', 'total_price', 'total_shipping_price_set',
  'discount_codes', 'payment_gateway_names', 'customer', 'email', 'contact_email', 'phone',
  'shipping_address', 'billing_address', 'note', 'line_items', 'refunds'
].join(',');

// ── JSON store (atomic write) ────────────────────────────────────
function atomicWrite(filePath, data) {
  const tmp = filePath + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filePath);
}
function loadStore() {
  // { orders:   { [id]: normalizedOrder },
  //   dispatch: { [id]: { courier, trackingUrl, packingStatus, orderConfirmation,
  //                       paymentMode, paidAmount, dueAmount, remarks, updatedAt, updatedBy } },
  //   sync:     { lastSyncedAt, lastCursorUpdatedAt, count, oldestAt, running } }
  try {
    const s = JSON.parse(fs.readFileSync(ORDERS_PATH, 'utf8'));
    if (!s.orders) s.orders = {};
    if (!s.dispatch) s.dispatch = {};
    if (!s.sync) s.sync = {};
    return s;
  } catch { return { orders: {}, dispatch: {}, sync: {} }; }
}
function saveStore(store) { atomicWrite(ORDERS_PATH, JSON.stringify(store)); }

// ── Shopify paginated fetch (read-only) ──────────────────────────
async function shopifyFetchAll(startUrl) {
  let all = [], url = startUrl;
  while (url) {
    const r = await fetch(url, {
      headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json' }
    });
    if (!r.ok) {
      const body = await r.text().catch(() => '');
      const err = new Error('Shopify ' + r.status + ': ' + body.slice(0, 300));
      err.status = r.status;
      throw err;
    }
    const d = await r.json();
    const key = Object.keys(d).find(k => Array.isArray(d[k]));
    if (key) all = all.concat(d[key]);
    const link = r.headers.get('Link') || '';
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  return all;
}

// ── Normalisation: raw Shopify order → lean ledger record ────────
function channelOf(sourceName) {
  const s = String(sourceName || '').toLowerCase();
  if (s === 'pos') return 'POS';
  return 'Website'; // web / gokwik / online-store / anything else
}
function num(v) { const n = parseFloat(v); return isNaN(n) ? 0 : n; }
function shippingTotal(o) {
  try { return num(((o.total_shipping_price_set || {}).shop_money || {}).amount); } catch { return 0; }
}
function refundTotal(o) {
  if (!Array.isArray(o.refunds)) return 0;
  let sum = 0;
  o.refunds.forEach(rf => (rf.transactions || []).forEach(t => { if (t.kind === 'refund') sum += num(t.amount); }));
  return sum;
}
function addressObj(a) {
  a = a || {};
  return {
    name: a.name || [a.first_name, a.last_name].filter(Boolean).join(' ').trim() || '',
    phone: a.phone || '',
    address1: a.address1 || '', address2: a.address2 || '',
    city: a.city || '', province: a.province || '', zip: a.zip || '',
    country: a.country || ''
  };
}
function normalizeOrder(o) {
  const cust = o.customer || {};
  const ship = o.shipping_address || {};
  const custName = [cust.first_name, cust.last_name].filter(Boolean).join(' ').trim()
    || ship.name || '';
  const email = cust.email || o.email || o.contact_email || '';
  const lineItems = (o.line_items || []).map(li => ({
    sku: li.sku || '', title: li.title || '', variantTitle: li.variant_title || '',
    qty: li.quantity || 0, price: num(li.price)
  }));
  return {
    id: String(o.id),
    name: o.name || ('#' + (o.order_number || '')),
    number: o.order_number || null,
    createdAt: o.created_at || null,
    updatedAt: o.updated_at || null,
    processedAt: o.processed_at || null,
    cancelledAt: o.cancelled_at || null,
    channel: channelOf(o.source_name),
    sourceName: o.source_name || '',
    customer: { name: custName, phone: o.phone || cust.phone || ship.phone || '', email },
    city: ship.city || '',
    shipAddress: addressObj(ship),
    billAddress: addressObj(o.billing_address),
    note: o.note || '',
    lineItems,
    itemCount: lineItems.reduce((s, li) => s + (li.qty || 0), 0),
    currency: o.currency || 'INR',
    subtotal: num(o.subtotal_price),
    discount: num(o.total_discounts),
    tax: num(o.total_tax),
    shipping: shippingTotal(o),
    total: num(o.total_price),
    refundAmount: refundTotal(o),
    financialStatus: o.financial_status || '',
    fulfillmentStatus: o.fulfillment_status || 'unfulfilled',
    paymentGateways: o.payment_gateway_names || [],
    couponCodes: (o.discount_codes || []).map(d => d.code).filter(Boolean)
  };
}

// Derive a Prepaid/COD guess so the dispatch layer has a sensible default.
function paymentModeGuess(n) {
  const g = (n.paymentGateways || []).join(' ').toLowerCase();
  if (g.includes('cod') || g.includes('cash on delivery')) return 'COD';
  if (n.financialStatus === 'paid') return 'Prepaid';
  if (n.financialStatus === 'pending') return 'COD';
  return '';
}

// ── Sync (backfill or incremental) ───────────────────────────────
let _syncing = false;
async function runSync(opts = {}) {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) throw new Error('Shopify env not configured');
  if (_syncing) return { skipped: 'already running' };
  _syncing = true;
  try {
    const store = loadStore();
    // Full backfill when: forced (opts.full), OR nothing stored yet. A forced full
    // re-pulls the ENTIRE history (no updated_at_min) so orders that predate the
    // first sync get imported — this is what "Backfill all history" triggers.
    const isBackfill = opts.full === true || Object.keys(store.orders).length === 0;
    // Incremental syncs re-pull anything updated since a small safety margin
    // before the last sync (catches edits/refunds/fulfilment changes).
    let url = `https://${SHOPIFY_STORE}/admin/api/2024-01/orders.json?status=any&limit=250&fields=${ORDER_FIELDS}`;
    if (!isBackfill && store.sync.lastSyncedAt) {
      const since = new Date(Date.now() - 10 * 60 * 1000).toISOString(); // last 10 min margin
      const min = store.sync.lastSyncedAt < since ? store.sync.lastSyncedAt : since;
      url += `&updated_at_min=${encodeURIComponent(min)}`;
    }
    const raw = await shopifyFetchAll(url);
    let imported = 0;
    raw.forEach(o => { store.orders[String(o.id)] = normalizeOrder(o); imported++; });

    // Recompute meta.
    const ids = Object.keys(store.orders);
    let oldest = null;
    ids.forEach(id => { const c = store.orders[id].createdAt; if (c && (!oldest || c < oldest)) oldest = c; });
    store.sync = {
      lastSyncedAt: new Date().toISOString(),
      count: ids.length,
      oldestAt: oldest,
      lastImported: imported,
      mode: isBackfill ? 'backfill' : 'incremental'
    };
    saveStore(store);
    return { imported, total: ids.length, oldestAt: oldest, mode: store.sync.mode };
  } finally { _syncing = false; }
}

// ── Read + filter ────────────────────────────────────────────────
function mergedList(store) {
  return Object.values(store.orders).map(o => {
    const d = store.dispatch[o.id] || {};
    return Object.assign({}, o, {
      dispatch: {
        courier: d.courier || '',
        trackingUrl: d.trackingUrl || '',
        packingStatus: d.packingStatus || 'pending',
        orderConfirmation: d.orderConfirmation || 'pending',
        paymentMode: d.paymentMode || paymentModeGuess(o),
        paidAmount: d.paidAmount != null ? d.paidAmount : null,
        dueAmount: d.dueAmount != null ? d.dueAmount : null,
        remarks: d.remarks || '',
        updatedAt: d.updatedAt || null,
        updatedBy: d.updatedBy || null
      }
    });
  });
}

router.get('/api/orders-ledger/sync/status', (req, res) => {
  const s = loadStore();
  res.json({ success: true, sync: s.sync || {}, running: _syncing });
});

router.post('/api/orders-ledger/sync', async (req, res) => {
  try {
    const full = req.query.full === '1' || !!(req.body && req.body.full);
    const out = await runSync({ full });
    res.json({ success: true, ...out });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

router.get('/api/orders-ledger', (req, res) => {
  const store = loadStore();
  let list = mergedList(store);
  const q = (req.query.q || '').toString().trim().toLowerCase();
  const channel = (req.query.channel || '').toString();
  const from = (req.query.from || '').toString();
  const to = (req.query.to || '').toString();
  const packing = (req.query.packing || '').toString();
  const fulfillment = (req.query.fulfillment || '').toString();

  if (channel) list = list.filter(o => o.channel === channel);
  if (from) list = list.filter(o => o.createdAt && o.createdAt >= from);
  if (to) list = list.filter(o => o.createdAt && o.createdAt <= to + 'T23:59:59');
  if (packing) list = list.filter(o => o.dispatch.packingStatus === packing);
  if (fulfillment) list = list.filter(o => (o.fulfillmentStatus || 'unfulfilled') === fulfillment);
  if (q) list = list.filter(o =>
    (o.name || '').toLowerCase().includes(q) ||
    (o.customer.name || '').toLowerCase().includes(q) ||
    (o.customer.phone || '').toLowerCase().includes(q) ||
    (o.customer.email || '').toLowerCase().includes(q) ||
    (o.city || '').toLowerCase().includes(q) ||
    o.lineItems.some(li => (li.sku || '').toLowerCase().includes(q) || (li.title || '').toLowerCase().includes(q))
  );

  list.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));

  // Summary over the filtered set (before paging).
  const summary = { count: list.length, revenue: 0, byChannel: {}, units: 0 };
  list.forEach(o => {
    if (o.cancelledAt) return;
    summary.revenue += o.total;
    summary.units += o.itemCount;
    summary.byChannel[o.channel] = (summary.byChannel[o.channel] || 0) + o.total;
  });

  const limit = Math.min(parseInt(req.query.limit, 10) || 200, 1000);
  const offset = parseInt(req.query.offset, 10) || 0;
  res.json({ success: true, total: list.length, summary, orders: list.slice(offset, offset + limit) });
});

router.get('/api/orders-ledger/:id', (req, res) => {
  const store = loadStore();
  const o = store.orders[String(req.params.id)];
  if (!o) return res.status(404).json({ success: false, error: 'Order not found' });
  const merged = mergedList({ orders: { [o.id]: o }, dispatch: store.dispatch })[0];
  res.json({ success: true, order: merged });
});

const PACKING_STATES = ['pending', 'packed', 'booked', 'dispatched', 'delivered', 'rto', 'cancelled'];
const CONFIRM_STATES = ['pending', 'confirmed', 'unreachable', 'cancelled'];

router.post('/api/orders-ledger/:id/dispatch', (req, res) => {
  const store = loadStore();
  const id = String(req.params.id);
  if (!store.orders[id]) return res.status(404).json({ success: false, error: 'Order not found' });
  const b = req.body || {};
  const cur = store.dispatch[id] || {};
  const next = {
    courier:           b.courier != null ? String(b.courier).trim() : (cur.courier || ''),
    trackingUrl:       b.trackingUrl != null ? String(b.trackingUrl).trim() : (cur.trackingUrl || ''),
    packingStatus:     PACKING_STATES.includes(b.packingStatus) ? b.packingStatus : (cur.packingStatus || 'pending'),
    orderConfirmation: CONFIRM_STATES.includes(b.orderConfirmation) ? b.orderConfirmation : (cur.orderConfirmation || 'pending'),
    paymentMode:       b.paymentMode != null ? String(b.paymentMode).trim() : (cur.paymentMode || ''),
    paidAmount:        b.paidAmount != null && b.paidAmount !== '' ? num(b.paidAmount) : (cur.paidAmount != null ? cur.paidAmount : null),
    dueAmount:         b.dueAmount != null && b.dueAmount !== '' ? num(b.dueAmount) : (cur.dueAmount != null ? cur.dueAmount : null),
    remarks:           b.remarks != null ? String(b.remarks).trim() : (cur.remarks || ''),
    updatedAt:         new Date().toISOString(),
    updatedBy:         (req.user && req.user.username) || 'system'
  };
  store.dispatch[id] = next;
  saveStore(store);
  res.json({ success: true, dispatch: next });
});

// ── Auto-sync poller (kept lightweight; incremental only) ────────
function startAutoSync() {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) { console.warn('[orders] Shopify env unset — auto-sync disabled'); return; }
  const every = parseInt(process.env.ORDERS_SYNC_INTERVAL_MS, 10) || 10 * 60 * 1000; // 10 min
  const tick = () => runSync().then(r => {
    if (r && r.imported) console.log(`[orders] sync: +${r.imported} (${r.mode}), total ${r.total}`);
  }).catch(e => console.error('[orders] sync error:', e.message));
  setTimeout(tick, 15 * 1000);        // first run shortly after boot
  setInterval(tick, every);
}

module.exports = { router, startAutoSync, runSync };
