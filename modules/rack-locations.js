// ═══════════════════════════════════════════════════════════════
// modules/rack-locations.js — Rack Identification + Stock Search
//
// First module of the "Business OS" split. Self-contained on purpose:
// it reads its own env vars and has its own tiny Shopify + JSON-store
// helpers, so mounting it is a single line in server.js and it cannot
// regress any existing route. Later modules can share an extracted
// lib/shopify.js; this one proves the pattern with zero blast radius.
//
// MOUNT in server.js (immediately before the `app.get('*')` catch-all):
//   app.use(require('./modules/rack-locations').router);
//
// Endpoints (all behind the same auth gate as everything else):
//   GET  /api/racks              → all rack assignments
//   POST /api/racks/set          → { variantId, sku?, rack } assign/update
//   POST /api/racks/delete       → { variantId } remove assignment
//   GET  /api/stock-search?q=...&vendor=&type=&hasRack=&inStock=&limit=
//                                → variant rows matching ANY parameter,
//                                  each annotated with its rack code
//   GET  /api/stock-search/refresh → force-refresh the catalog cache
//
// The catalog scan is cached in-memory (TTL below) because a full
// products.json crawl is slow; search then filters that snapshot in
// memory, which is instant and lets you match on any field at once.
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const fetch   = require('node-fetch');

const router = express.Router();

const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
const SHOPIFY_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
// Persist on the same volume the rest of the app uses. On Railway, DATA_PATH
// points at the mounted /data volume; writing beside it means rack assignments
// (typed in by hand) survive redeploys instead of living on the ephemeral
// container filesystem. Falls back to the repo dir for local runs.
const RACKS_PATH = process.env.RACKS_PATH ||
  path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'),
            'rack_locations.json');

// ── JSON store (atomic write, mirrors server.js atomicWrite) ──────
function atomicWrite(filePath, data) {
  const tmp = filePath + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, filePath);
}
function loadRacks() {
  try { return JSON.parse(fs.readFileSync(RACKS_PATH, 'utf8')); }
  catch { return { racks: {} }; }   // { racks: { [variantId]: { rack, sku, updatedAt } } }
}
function saveRacks(store) {
  atomicWrite(RACKS_PATH, JSON.stringify(store, null, 2));
}

// ── Shopify catalog crawl (paginated) with in-memory TTL cache ────
const CATALOG_TTL_MS = 5 * 60 * 1000;
let _catalog = { at: 0, variants: [] };
let _inflight = null;

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

// Flatten products → one row per variant, carrying product-level fields
// we want to be searchable (vendor, type, tags).
async function buildCatalog() {
  const url = `https://${SHOPIFY_STORE}/admin/api/2024-01/products.json?limit=250&fields=id,title,vendor,product_type,tags,variants,image,images`;
  const products = await shopifyFetchAll(url);
  const variants = [];
  for (const p of products) {
    const img = (p.image && p.image.src) || (p.images && p.images[0] && p.images[0].src) || null;
    for (const v of (p.variants || [])) {
      variants.push({
        productId: p.id,
        productTitle: p.title || '',
        vendor: p.vendor || '',
        productType: p.product_type || '',
        tags: p.tags || '',
        image: img,
        variantId: v.id,
        sku: v.sku || '',
        variantTitle: v.title || '',
        price: v.price || '',
        inventoryItemId: v.inventory_item_id || null,
        inventoryQuantity: (typeof v.inventory_quantity === 'number') ? v.inventory_quantity : null
      });
    }
  }
  return variants;
}

async function getCatalog(force) {
  const fresh = !force && (Date.now() - _catalog.at) < CATALOG_TTL_MS && _catalog.variants.length;
  if (fresh) return _catalog.variants;
  if (_inflight) return _inflight;               // coalesce concurrent callers → one crawl
  _inflight = (async () => {
    try {
      const variants = await buildCatalog();
      _catalog = { at: Date.now(), variants };
      return variants;
    } finally { _inflight = null; }
  })();
  return _inflight;
}

// ── Routes ────────────────────────────────────────────────────────
router.get('/api/racks', (req, res) => {
  const store = loadRacks();
  res.json({ success: true, racks: store.racks || {}, count: Object.keys(store.racks || {}).length });
});

router.post('/api/racks/set', (req, res) => {
  try {
    const { variantId, sku, rack } = (req.body || {});
    if (!variantId) return res.json({ success: false, error: 'Missing variantId' });
    const code = String(rack == null ? '' : rack).trim();
    const store = loadRacks();
    if (!store.racks) store.racks = {};
    if (!code) {                                   // empty rack clears the assignment
      delete store.racks[String(variantId)];
    } else {
      store.racks[String(variantId)] = { rack: code, sku: sku ? String(sku) : '', updatedAt: Date.now() };
    }
    saveRacks(store);
    res.json({ success: true, variantId: String(variantId), rack: code });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

router.post('/api/racks/delete', (req, res) => {
  try {
    const { variantId } = (req.body || {});
    if (!variantId) return res.json({ success: false, error: 'Missing variantId' });
    const store = loadRacks();
    const existed = store.racks && store.racks[String(variantId)];
    if (store.racks) delete store.racks[String(variantId)];
    saveRacks(store);
    res.json({ success: true, removed: !!existed });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

router.get('/api/stock-search/refresh', async (req, res) => {
  try {
    const variants = await getCatalog(true);
    res.json({ success: true, refreshed: true, variantCount: variants.length, cachedAt: _catalog.at });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

router.get('/api/stock-search', async (req, res) => {
  try {
    const q       = String(req.query.q || '').trim().toLowerCase();
    const vendor  = String(req.query.vendor || '').trim().toLowerCase();
    const type    = String(req.query.type || '').trim().toLowerCase();
    const hasRack = String(req.query.hasRack || '').toLowerCase();   // 'true' | 'false' | ''
    const inStock = String(req.query.inStock || '').toLowerCase();   // 'true' | 'false' | ''
    const limit   = Math.min(500, Math.max(1, parseInt(req.query.limit, 10) || 200));

    const [variants, store] = [await getCatalog(false), loadRacks()];
    const racks = store.racks || {};

    let rows = variants.map(v => ({ ...v, rack: (racks[String(v.variantId)] || {}).rack || '' }));

    if (vendor)  rows = rows.filter(r => r.vendor.toLowerCase() === vendor);
    if (type)    rows = rows.filter(r => r.productType.toLowerCase() === type);
    if (hasRack === 'true')  rows = rows.filter(r => !!r.rack);
    if (hasRack === 'false') rows = rows.filter(r => !r.rack);
    if (inStock === 'true')  rows = rows.filter(r => (r.inventoryQuantity || 0) > 0);
    if (inStock === 'false') rows = rows.filter(r => (r.inventoryQuantity || 0) <= 0);

    if (q) {
      // "any parameter" — match the query against every searchable field.
      rows = rows.filter(r =>
        r.sku.toLowerCase().includes(q) ||
        r.productTitle.toLowerCase().includes(q) ||
        r.variantTitle.toLowerCase().includes(q) ||
        r.vendor.toLowerCase().includes(q) ||
        r.productType.toLowerCase().includes(q) ||
        String(r.tags).toLowerCase().includes(q) ||
        r.rack.toLowerCase().includes(q) ||
        String(r.variantId).includes(q)
      );
    }

    const total = rows.length;
    res.json({
      success: true,
      total,
      returned: Math.min(total, limit),
      cachedAt: _catalog.at,
      stale: (Date.now() - _catalog.at) > CATALOG_TTL_MS,
      rows: rows.slice(0, limit)
    });
  } catch (e) { res.json({ success: false, error: e.message }); }
});

module.exports = { router };
