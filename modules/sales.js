// ═══════════════════════════════════════════════════════════════
// modules/sales.js — Phase 1: POS / offline sales recording
//
// Online sales (the website) already flow in from Shopify. The gap is
// OFFLINE sales — walk-ins, Instagram/WhatsApp/DM, phone orders — which
// live nowhere. This module is the ledger for those: a salesperson picks
// products (via the existing Stock Search cache), sets qty + price +
// payment mode, and records the sale. Each sale is attributed to the
// logged-in staff member. Recording a sale also decrements FRONT-of-store
// inventory in Shopify so stock stays truthful.
//
// Self-contained, same pattern as rack-locations.js: own env reads, own
// atomic JSON store on the /data volume, mounted with one line in server.js
//   app.use(require('./modules/sales').router);
//
// Endpoints (behind the same auth gate; sales role + admin):
//   POST /api/sales/record   { items:[{variantId,inventoryItemId,sku,title,
//                              variantTitle,qty,unitPrice}], paymentMode,
//                              channel?, customer?, discount?, note?,
//                              adjustInventory? }  → saves + adjusts stock
//   GET  /api/sales          ?from=YYYY-MM-DD&to=&channel=&staff=&limit=
//   GET  /api/sales/today    → today's (IST) rollup: count, gross, by mode/staff
//   POST /api/sales/void     { id }  → void a sale (admin, or own same-day),
//                              re-adds the inventory it took
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const fetch   = require('node-fetch');
const { shopifyClient } = require('./shopify-client');

const router = express.Router();

const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
const SHOPIFY_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

// Persist beside the rest of the app's data on the mounted volume.
const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const SALES_PATH = process.env.SALES_PATH || path.join(DATA_DIR, 'sales.json');
// Front/back location config is owned by the showroom module; we only read it.
const SHOWROOM_SETTINGS_PATH = path.join(DATA_DIR, 'showroom-settings.json');

// ── Store (atomic JSON, mirrors the other modules) ───────────────
function atomicWrite(filePath, data) {
  const tmp = filePath + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filePath);
}
function loadSales() {
  try { const s = JSON.parse(fs.readFileSync(SALES_PATH, 'utf8')); if (!Array.isArray(s.sales)) s.sales = []; return s; }
  catch { return { sales: [] }; }
}
function saveSales(store) { atomicWrite(SALES_PATH, JSON.stringify(store, null, 2)); }

function loadFrontLocationId() {
  try { return String(JSON.parse(fs.readFileSync(SHOWROOM_SETTINGS_PATH, 'utf8')).frontLocationId || ''); }
  catch { return ''; }
}

// ── Helpers ──────────────────────────────────────────────────────
// Business is in India; "today" must be IST, not the container's UTC.
function istDay(d) {
  const t = new Date((d instanceof Date ? d.getTime() : Date.now()) + 5.5 * 3600 * 1000);
  return t.toISOString().slice(0, 10); // YYYY-MM-DD
}
function round2(n) { return Math.round((Number(n) || 0) * 100) / 100; }
function newId() { return 'S' + Date.now() + Math.random().toString(36).slice(2, 6); }

const CHANNELS = ['walk-in', 'instagram', 'whatsapp', 'phone', 'other'];
const PAYMENTS = ['cash', 'upi', 'card', 'other'];

// Adjust a single inventory item at the front location by `delta` (can be
// negative to sell, positive to restock on void). Best-effort; returns a
// small result object so the caller can report per-line success.
async function adjustFront(inventoryItemId, locationId, delta) {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) return { ok: false, error: 'Shopify not configured' };
  if (!locationId) return { ok: false, error: 'No front location configured' };
  if (!inventoryItemId) return { ok: false, error: 'Missing inventory item' };
  try {
    const r = await shopifyClient.request(`https://${SHOPIFY_STORE}/admin/api/2024-01/inventory_levels/adjust.json`, {
      method: 'POST',
      body: JSON.stringify({
        location_id: Number(locationId),
        inventory_item_id: Number(inventoryItemId),
        available_adjustment: Number(delta)
      })
    });
    if (!r.ok) { const b = await r.text().catch(() => ''); return { ok: false, error: 'Shopify ' + r.status + ': ' + b.slice(0, 160) }; }
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

// Normalise + validate an incoming line item.
function cleanItem(raw) {
  const qty = Math.max(1, Math.floor(Number(raw.qty) || 0));
  const unitPrice = round2(raw.unitPrice);
  return {
    variantId: raw.variantId != null ? String(raw.variantId) : '',
    inventoryItemId: raw.inventoryItemId != null ? String(raw.inventoryItemId) : '',
    sku: String(raw.sku || ''),
    title: String(raw.title || ''),
    variantTitle: String(raw.variantTitle || ''),
    qty,
    unitPrice,
    lineTotal: round2(qty * unitPrice)
  };
}

// ── Routes ────────────────────────────────────────────────────────
router.post('/api/sales/record', async (req, res) => {
  try {
    const b = req.body || {};
    const rawItems = Array.isArray(b.items) ? b.items : [];
    const items = rawItems.map(cleanItem).filter(it => it.qty > 0 && (it.variantId || it.sku));
    if (!items.length) return res.json({ success: false, error: 'Add at least one product' });

    const paymentMode = PAYMENTS.includes(String(b.paymentMode)) ? String(b.paymentMode) : 'cash';
    const channel = CHANNELS.includes(String(b.channel)) ? String(b.channel) : 'walk-in';
    const discount = Math.max(0, round2(b.discount));
    const note = String(b.note || '').slice(0, 500);
    const customer = (b.customer && (b.customer.name || b.customer.phone))
      ? { name: String(b.customer.name || '').slice(0, 120), phone: String(b.customer.phone || '').slice(0, 20) }
      : null;

    const subtotal = round2(items.reduce((s, it) => s + it.lineTotal, 0));
    const total = round2(Math.max(0, subtotal - discount));

    // Decrement front-of-store stock unless the caller opts out.
    const adjustInventory = b.adjustInventory !== false;
    const frontLoc = loadFrontLocationId();
    const invResults = [];
    let invAdjusted = false;
    if (adjustInventory && frontLoc) {
      for (const it of items) {
        const r = await adjustFront(it.inventoryItemId, frontLoc, -it.qty);
        invResults.push({ variantId: it.variantId, sku: it.sku, qty: it.qty, ok: r.ok, error: r.error || null });
        if (r.ok) invAdjusted = true;
      }
    }

    const now = new Date();
    const sale = {
      id: newId(),
      ts: now.toISOString(),
      day: istDay(now),
      staff: (req.user && req.user.username) || 'unknown',
      channel,
      paymentMode,
      customer,
      items,
      subtotal,
      discount,
      total,
      inventory: { requested: adjustInventory, location: frontLoc || null, adjusted: invAdjusted, results: invResults },
      note,
      voided: false
    };

    const store = loadSales();
    store.sales.push(sale);
    saveSales(store);

    res.json({ success: true, sale, inventoryWarning: (adjustInventory && !frontLoc)
      ? 'Sale recorded, but front location is not configured in Settings — stock was NOT reduced.' : null });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

router.get('/api/sales', (req, res) => {
  try {
    const from    = String(req.query.from || '').trim();   // YYYY-MM-DD (IST day)
    const to      = String(req.query.to || '').trim();
    const channel = String(req.query.channel || '').trim().toLowerCase();
    const staff   = String(req.query.staff || '').trim().toLowerCase();
    const limit   = Math.min(1000, Math.max(1, parseInt(req.query.limit, 10) || 200));

    let rows = loadSales().sales.slice();
    if (from)    rows = rows.filter(s => s.day >= from);
    if (to)      rows = rows.filter(s => s.day <= to);
    if (channel) rows = rows.filter(s => (s.channel || '').toLowerCase() === channel);
    if (staff)   rows = rows.filter(s => (s.staff || '').toLowerCase() === staff);

    rows.sort((a, b) => (a.ts < b.ts ? 1 : -1)); // newest first

    const live = rows.filter(s => !s.voided);
    const totals = {
      count: live.length,
      gross: round2(live.reduce((n, s) => n + (s.total || 0), 0)),
      units: live.reduce((n, s) => n + s.items.reduce((m, it) => m + it.qty, 0), 0)
    };
    res.json({ success: true, totals, returned: Math.min(rows.length, limit), rows: rows.slice(0, limit) });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

router.get('/api/sales/today', (req, res) => {
  try {
    const today = istDay(new Date());
    const rows = loadSales().sales.filter(s => s.day === today && !s.voided);
    const byMode = {}, byStaff = {}, byChannel = {};
    let gross = 0, units = 0;
    for (const s of rows) {
      gross += (s.total || 0);
      units += s.items.reduce((m, it) => m + it.qty, 0);
      byMode[s.paymentMode]  = round2((byMode[s.paymentMode] || 0) + (s.total || 0));
      byStaff[s.staff]       = round2((byStaff[s.staff] || 0) + (s.total || 0));
      byChannel[s.channel]   = round2((byChannel[s.channel] || 0) + (s.total || 0));
    }
    res.json({ success: true, day: today, count: rows.length, gross: round2(gross), units, byMode, byStaff, byChannel });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

router.post('/api/sales/void', async (req, res) => {
  try {
    const id = String((req.body || {}).id || '');
    if (!id) return res.json({ success: false, error: 'Missing sale id' });
    const store = loadSales();
    const sale = store.sales.find(s => s.id === id);
    if (!sale) return res.json({ success: false, error: 'Sale not found' });
    if (sale.voided) return res.json({ success: false, error: 'Already voided' });

    const isAdmin = req.user && req.user.role === 'admin';
    const isOwnSameDay = req.user && req.user.username === sale.staff && sale.day === istDay(new Date());
    if (!isAdmin && !isOwnSameDay)
      return res.json({ success: false, error: 'Only an admin can void this (or the same salesperson, same day)' });

    // Put the stock back for whatever we actually took.
    const restock = [];
    if (sale.inventory && sale.inventory.adjusted && sale.inventory.location) {
      for (const it of sale.items) {
        const took = (sale.inventory.results || []).find(r => r.variantId === it.variantId);
        if (took && took.ok) {
          const r = await adjustFront(it.inventoryItemId, sale.inventory.location, it.qty);
          restock.push({ variantId: it.variantId, ok: r.ok, error: r.error || null });
        }
      }
    }

    sale.voided = true;
    sale.voidedAt = new Date().toISOString();
    sale.voidedBy = (req.user && req.user.username) || 'unknown';
    sale.voidRestock = restock;
    saveSales(store);
    res.json({ success: true, id, restocked: restock.filter(r => r.ok).length });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

module.exports = { router };
