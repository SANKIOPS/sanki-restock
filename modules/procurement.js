// ═══════════════════════════════════════════════════════════════
// modules/procurement.js — PROCUREMENT / INVENTORY INTAKE
//
// Mirrors SANKI's real China-sourcing flow as two states of ONE PO:
//   • ADVANCE  — from the vendor (RMB) invoice: enter lines, SKUs are
//                GENERATED in-app, landed cost is provisional.
//   • FINAL    — after weighing: freight (₹/g) allocated, landed cost
//                locked, then the gated Shopify write.
//
// This is the FIRST module that WRITES to Shopify, and every write is
// behind a mandatory preview/approval gate:
//   • NEW sku  → CREATE a draft product (one product per colour, sizes
//                as variants) — never published until the user activates.
//   • EXISTING → ADD the received quantity to that variant's inventory
//                at the WAREHOUSE location.
//
// SKU + all SEO/GEO/AEO fields (title, handle, meta title/description,
// image alt-text, tags) are generated here so listings are born clean.
//
// The app is the source of truth. Lookup tables are seeded ONCE from the
// decoded sheet, then owned in-app. The running serial is derived LIVE
// from Shopify (never from a sheet).
//
// Endpoints (all behind the auth gate):
//   GET  /api/procurement/settings          → rates + warehouse location
//   POST /api/procurement/settings
//   GET  /api/procurement/lookups           → brand/product/colour/size tables
//   POST /api/procurement/lookups
//   GET  /api/procurement/next-serial       → next serial (from Shopify)
//   POST /api/procurement/preview           → compute SKU+cost+SEO+NEW/EXISTING
//   POST /api/procurement/commit            → execute the approved writes
//   GET  /api/procurement/pos               → intake history
//   GET  /api/procurement/pos/:id
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const fetch   = require('node-fetch');

const router = express.Router();

const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
const SHOPIFY_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const API = '2024-01';

const STORE_PATH = process.env.PROCUREMENT_PATH ||
  path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'),
            'procurement.json');

// ── Seeds (decoded once from the sheet; editable in-app thereafter) ──
const SEED = {
  brand: 'SA',
  products: { // product type → numeric code
    'Shirt': 1, 'T-Shirt': 2, 'Jeans': 10, 'Trouser': 11, 'Lower': 12,
    'Shorts': 13, 'Jogger': 14, 'Coord Set': 15, 'Jorts': 16, 'Sando': 17,
    'Bag': 18, 'Denim Joggers': 19
  },
  colours: { // colour → numeric code
    'Black': 1, 'Blue': 2, 'Brown': 3, 'Cream': 4, 'Green': 5, 'Grey': 6,
    'Maroon': 7, 'Orange': 8, 'Pink': 9, 'Purple': 10, 'Red': 11, 'White': 12,
    'Yellow': 13, 'Beige': 14, 'Sky Blue': 16, 'Olive': 17, 'Khaki': 18,
    'Golden': 20, 'Silver': 21
  },
  sizes: { // size label → suffix code
    'Free Size': 'FS', 'Medium': 'M', 'Large': 'L', 'Extra Large': 'XL',
    'Double Extra Large': 'XXL', 'Triple Extra Large': '3XL', 'Four Extra Large': '4XL',
    'Waist 36': '36'
  },
  settings: {
    exRate: 15,           // ₹ per RMB
    freightPerGram: 0.42, // ₹ per gram (≈ ₹420/kg)
    gstLowThreshold: 2500,// < ₹2500 → 5% GST, else 18% (per unit-economics)
    gstLow: 0.05,
    gstHigh: 0.18,
    warehouseLocationId: '' // set from the UI (Shopify location)
  }
};

// Known size suffix tokens — used to parse the serial out of an existing SKU.
const SIZE_TOKENS = ['FS','XS','S','M','L','XL','XXL','3XL','4XL','5XL','36','38','40','28','30','32','34'];

// ── JSON store (atomic) ──────────────────────────────────────────
function atomicWrite(fp, data) {
  const tmp = fp + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data); fs.renameSync(tmp, fp);
}
function loadStore() {
  let s;
  try { s = JSON.parse(fs.readFileSync(STORE_PATH, 'utf8')); } catch { s = {}; }
  if (!s.brand)    s.brand = SEED.brand;
  if (!s.products) s.products = { ...SEED.products };
  if (!s.colours)  s.colours = { ...SEED.colours };
  if (!s.sizes)    s.sizes = { ...SEED.sizes };
  if (!s.settings) s.settings = { ...SEED.settings };
  else s.settings = { ...SEED.settings, ...s.settings };
  if (!s.pos)      s.pos = {};      // { [poId]: PO }
  if (!s.seq)      s.seq = 0;       // internal PO counter
  return s;
}
function saveStore(s) { atomicWrite(STORE_PATH, JSON.stringify(s)); }

// ── small helpers ────────────────────────────────────────────────
function num(v) { const n = parseFloat(v); return isNaN(n) ? 0 : n; }
function slugify(s) {
  return String(s || '').toLowerCase().trim()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/-{2,}/g, '-');
}
function truncate(s, n) { s = String(s || ''); return s.length <= n ? s : s.slice(0, n - 1).trim() + '…'; }
function titleCase(s) { return String(s || '').replace(/\w\S*/g, w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()); }

// ── Serial parsing / next-serial (Shopify-sourced) ───────────────
// Current SKU format: SA<digits><ALPHA><digits><SIZE>. The single alpha
// letter delimits the numeric prefix from the running serial number; the
// trailing token is a known size code. Old eras (two letters, or no size
// suffix) don't match and are ignored.
function parseSerial(sku) {
  const m = String(sku || '').toUpperCase().match(/^SA\d+([A-Z])(\d{1,4})([A-Z0-9]{1,3})$/);
  if (!m) return null;
  const [, alpha, numStr, size] = m;
  if (!SIZE_TOKENS.includes(size)) return null;
  return { alpha, num: parseInt(numStr, 10) };
}
// Compare two serials: alpha first (A<B…), then number.
function serialGt(a, b) {
  if (!b) return true;
  if (a.alpha !== b.alpha) return a.alpha > b.alpha;
  return a.num > b.num;
}
function nextSerial(cur) {
  // cur = { alpha, num }; roll J→K→… at 999.
  if (!cur) return { alpha: 'J', num: 1 };
  if (cur.num < 999) return { alpha: cur.alpha, num: cur.num + 1 };
  const nextAlpha = String.fromCharCode(cur.alpha.charCodeAt(0) + 1);
  return { alpha: nextAlpha, num: 1 };
}

// ── Shopify variant catalogue (cached; used for serial + classify) ──
let _catalogue = null;      // { skuMap: {SKU: {productId, variantId, inventoryItemId}}, maxSerial, fetchedAt }
async function loadCatalogue(force) {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) throw new Error('Shopify env not configured');
  if (_catalogue && !force && (Date.now() - _catalogue.fetchedAt) < 5 * 60 * 1000) return _catalogue;
  let url = `https://${SHOPIFY_STORE}/admin/api/${API}/products.json?limit=250&fields=id,variants`;
  const skuMap = {}; let maxSerial = null;
  while (url) {
    const r = await fetch(url, { headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json' } });
    if (!r.ok) { const b = await r.text().catch(() => ''); throw new Error('Shopify ' + r.status + ': ' + b.slice(0, 200)); }
    const d = await r.json();
    (d.products || []).forEach(p => (p.variants || []).forEach(v => {
      const sku = (v.sku || '').toUpperCase();
      if (!sku) return;
      skuMap[sku] = { productId: String(p.id), variantId: String(v.id), inventoryItemId: String(v.inventory_item_id || '') };
      const s = parseSerial(sku);
      if (s && serialGt(s, maxSerial)) maxSerial = s;
    }));
    const link = r.headers.get('Link') || '';
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  _catalogue = { skuMap, maxSerial, fetchedAt: Date.now() };
  return _catalogue;
}

// ── SKU builder ──────────────────────────────────────────────────
function buildSku(store, productType, colour, sizeLabel, serial) {
  const pc = store.products[productType];
  const cc = store.colours[colour];
  const sc = store.sizes[sizeLabel];
  const missing = [];
  if (pc == null) missing.push('product "' + productType + '"');
  if (cc == null) missing.push('colour "' + colour + '"');
  if (sc == null) missing.push('size "' + sizeLabel + '"');
  if (missing.length) return { error: 'Unknown ' + missing.join(', ') + ' — add it to the lookup tables first.' };
  return { sku: `${store.brand}${pc}${cc}${serial.alpha}${serial.num}${sc}` };
}

// ── Landed cost + suggested MRP (per pc) ─────────────────────────
function landedCost(line, settings) {
  const inrValue    = num(line.perPcsYuan) * num(settings.exRate);
  const freightPerPc = num(line.weightGrams) * num(settings.freightPerGram);
  const landed      = inrValue + freightPerPc;
  // MRP ≈ 2×landed + GST; GST tier depends on the resulting price.
  let mrpRaw = 2 * landed * (1 + settings.gstLow);
  if (mrpRaw >= settings.gstLowThreshold) mrpRaw = 2 * landed * (1 + settings.gstHigh);
  const mrp = charmPrice(mrpRaw);
  return {
    inrValue: round2(inrValue),
    freightPerPc: round2(freightPerPc),
    landed: round2(landed),
    suggestedMrp: mrp
  };
}
function round2(n) { return Math.round(n * 100) / 100; }
function charmPrice(x) { const up = Math.ceil(x / 100) * 100; return Math.max(up - 1, 0); } // → …99

// ── SEO / GEO / AEO field generation ─────────────────────────────
// Matches SANKI's newer, keyword-first title style (em-dash, fit words,
// colour) rather than the old code-first names. Everything is a starting
// draft the user edits in the preview before anything is written.
function genSeo(g) {
  const designName  = titleCase(g.designName || '');
  const productType = g.productType || '';
  const colour      = titleCase(g.colour || '');
  const fit         = titleCase(g.fit || '');
  const audience    = g.audience || 'Men';           // 'Men' | 'Women' | 'Unisex'
  const sizeList    = (g.sizeLabels || []).map(l => (g.sizeCodeOf ? g.sizeCodeOf(l) : l)).join(', ');

  // Customer-facing product title (the H1 / storefront name).
  const descriptor  = [fit ? fit + ' Fit' : '', colour].filter(Boolean).join(', ');
  const titleCore   = [designName, productType].filter(Boolean).join(' ').trim();
  const title       = descriptor ? `${titleCore} — ${descriptor}` : titleCore;

  // URL handle: clean, keyword-rich, no code up front.
  const handle = slugify([designName, productType, colour, fit].filter(Boolean).join(' '));

  // SEO <title> (global.title_tag) — keep ~60 chars, brand at the end.
  const metaTitle = truncate(
    `${[fit, colour].filter(Boolean).join(' ')} ${productType} – ${designName} | SANKI`.replace(/\s+/g, ' ').trim(),
    60
  );

  // Meta description (global.description_tag) — natural, AEO-friendly, ~155 chars.
  const audienceWord = audience === 'Unisex' ? 'unisex' : (audience === 'Women' ? "women's" : "men's");
  const metaDescription = truncate(
    `Shop the ${designName} ${String(productType).toLowerCase()} in ${colour.toLowerCase()} by SANKI — premium ${audienceWord} streetwear` +
    (fit ? `, ${fit.toLowerCase()} fit` : '') +
    `. ${sizeList ? 'Sizes ' + sizeList + '. ' : ''}COD available. Limited drop.`,
    160
  );

  // Image alt-text (accessibility + image SEO). One per product image slot.
  const imageAlt = [colour, designName, productType, 'by SANKI',
    fit ? '— ' + fit.toLowerCase() : '', audienceWord, 'streetwear']
    .filter(Boolean).join(' ').replace(/\s+/g, ' ').trim();

  // Search tags.
  const tags = Array.from(new Set([
    productType, colour, fit, designName, 'SANKI', 'Streetwear',
    audience === 'Unisex' ? 'Unisex' : audience,
    `${colour} ${productType}`.trim(),
    fit ? `${fit} ${productType}`.trim() : ''
  ].filter(Boolean)));

  // Body / description HTML (schema-friendly plain prose + feature list).
  const bodyHtml =
    `<p>The <strong>${title}</strong> from SANKI — premium ${audienceWord} streetwear` +
    `${fit ? ', ' + fit.toLowerCase() + ' fit' : ''}, in ${colour.toLowerCase()}.</p>` +
    `<ul>` +
    `<li>Design: ${designName || productType}</li>` +
    `<li>Colour: ${colour || '—'}</li>` +
    (fit ? `<li>Fit: ${fit}</li>` : '') +
    (sizeList ? `<li>Available sizes: ${sizeList}</li>` : '') +
    `<li>Cash on Delivery available · Limited drop</li>` +
    `</ul>`;

  return { title, handle, metaTitle, metaDescription, imageAlt, tags, bodyHtml };
}

// ── Grouping: intake lines → products (one product per design×colour) ──
// A "group key" = brand+design+colour+productType+fit+audience. Each group
// becomes one Shopify product; its lines (sizes) become the variants.
function groupKey(l) {
  return [l.designName || '', l.productType || '', l.colour || '', l.fit || '', l.audience || 'Men']
    .map(x => String(x).trim().toLowerCase()).join('|');
}

// Compute a full preview for a set of intake lines (no writes).
async function computePreview(store, body) {
  const settings = { ...store.settings };
  if (body.exRate != null && body.exRate !== '')       settings.exRate = num(body.exRate);
  if (body.freightPerGram != null && body.freightPerGram !== '') settings.freightPerGram = num(body.freightPerGram);

  const cat = await loadCatalogue(!!body.refresh);
  const sizeCodeOf = (label) => store.sizes[label] || label;

  // Serial cursor starts from the live Shopify max; hand out consecutive
  // serials as we walk the lines (each size variant gets its own).
  let cursor = cat.maxSerial ? { ...cat.maxSerial } : null;

  const lines = (body.lines || []).map(raw => {
    const line = {
      designName:  (raw.designName || '').trim(),
      productType: (raw.productType || '').trim(),
      colour:      (raw.colour || '').trim(),
      sizeLabel:   (raw.sizeLabel || '').trim(),
      fit:         (raw.fit || '').trim(),
      audience:    (raw.audience || 'Men').trim(),
      vendor:      (raw.vendor || '').trim(),
      designCode:  (raw.designCode || '').trim(),
      qty:         Math.max(0, Math.round(num(raw.qty))),
      perPcsYuan:  num(raw.perPcsYuan),
      weightGrams: num(raw.weightGrams)
    };
    const cost = landedCost(line, settings);

    // Classify against Shopify: does this exact SKU already exist?
    // We generate the candidate SKU using the NEXT serial, but if the user
    // supplied an explicit existing SKU we honour it for the EXISTING path.
    let sku = (raw.sku || '').toUpperCase().trim();
    let serialUsed = null, skuError = null;
    const existingByGiven = sku && cat.skuMap[sku];
    if (!sku) {
      cursor = nextSerial(cursor);
      serialUsed = { ...cursor };
      const built = buildSku(store, line.productType, line.colour, line.sizeLabel, serialUsed);
      if (built.error) skuError = built.error; else sku = built.sku;
    }
    const existing = sku ? cat.skuMap[sku] : null;
    return Object.assign(line, {
      sku, serialUsed, skuError,
      ...cost,
      classification: existing ? 'EXISTING' : 'NEW',
      existing: existing || null
    });
  });

  // Build the SEO/product preview for each NEW-product group.
  const groups = {};
  lines.forEach(l => {
    if (l.classification !== 'NEW') return;
    const k = groupKey(l);
    if (!groups[k]) groups[k] = { key: k, lines: [], productType: l.productType, colour: l.colour, designName: l.designName, fit: l.fit, audience: l.audience, vendor: l.vendor, designCode: l.designCode };
    groups[k].lines.push(l);
  });
  const newProducts = Object.values(groups).map(g => {
    const seo = genSeo({
      designName: g.designName, productType: g.productType, colour: g.colour,
      fit: g.fit, audience: g.audience, sizeLabels: g.lines.map(l => l.sizeLabel), sizeCodeOf
    });
    return {
      key: g.key, vendor: g.vendor, designCode: g.designCode,
      colour: g.colour, productType: g.productType,
      seo,
      variants: g.lines.map(l => ({
        sku: l.sku, sizeLabel: l.sizeLabel, sizeCode: sizeCodeOf(l.sizeLabel),
        qty: l.qty, landed: l.landed, price: l.suggestedMrp, skuError: l.skuError
      }))
    };
  });

  const existingAdds = lines.filter(l => l.classification === 'EXISTING').map(l => ({
    sku: l.sku, qty: l.qty, landed: l.landed,
    productId: l.existing.productId, variantId: l.existing.variantId, inventoryItemId: l.existing.inventoryItemId
  }));

  return {
    settings, warehouseLocationId: store.settings.warehouseLocationId,
    lines, newProducts, existingAdds,
    counts: { total: lines.length, newProducts: newProducts.length, existingAdds: existingAdds.length,
              errors: lines.filter(l => l.skuError).length }
  };
}

// ── Shopify writes ───────────────────────────────────────────────
async function shopifyPost(pathUrl, payload) {
  const r = await fetch(`https://${SHOPIFY_STORE}/admin/api/${API}/${pathUrl}`, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const text = await r.text();
  let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }
  if (!r.ok) { const e = new Error('Shopify ' + r.status + ': ' + JSON.stringify(json.errors || json).slice(0, 300)); e.status = r.status; throw e; }
  return json;
}

async function createDraftProduct(np, warehouseLocationId) {
  const sizes = np.variants.map(v => v.sizeCode);
  const payload = {
    product: {
      title: np.seo.title,
      body_html: np.seo.bodyHtml,
      vendor: 'SANKI',
      product_type: np.productType,
      handle: np.seo.handle,
      status: 'draft',              // never live until the user activates
      tags: np.seo.tags.join(', '),
      options: [
        { name: 'Color', values: [np.colour || 'Default'] },
        { name: 'Size',  values: sizes }
      ],
      variants: np.variants.map(v => ({
        option1: np.colour || 'Default',
        option2: v.sizeCode,
        sku: v.sku,
        price: String(v.price),
        inventory_management: 'shopify',
        inventory_policy: 'deny'
      })),
      metafields: [
        { namespace: 'global', key: 'title_tag',       value: np.seo.metaTitle,       type: 'single_line_text_field' },
        { namespace: 'global', key: 'description_tag', value: np.seo.metaDescription, type: 'multi_line_text_field' }
      ]
    }
  };
  const created = await shopifyPost('products.json', payload).then(d => d.product);

  // Stock each variant at the warehouse location with its received qty.
  const stocked = [];
  for (let i = 0; i < (created.variants || []).length; i++) {
    const cv = created.variants[i];
    const src = np.variants.find(v => (v.sku || '').toUpperCase() === (cv.sku || '').toUpperCase()) || np.variants[i];
    const qty = src ? src.qty : 0;
    if (warehouseLocationId && cv.inventory_item_id) {
      try {
        await shopifyPost('inventory_levels/set.json', {
          location_id: Number(warehouseLocationId),
          inventory_item_id: cv.inventory_item_id,
          available: qty,
          disconnect_if_necessary: true
        });
        stocked.push({ sku: cv.sku, qty });
      } catch (e) {
        stocked.push({ sku: cv.sku, qty, stockError: e.message });
      }
    }
  }
  return { productId: String(created.id), handle: created.handle, title: created.title, variants: stocked };
}

async function addExistingInventory(ea, warehouseLocationId) {
  if (!ea.inventoryItemId) return { sku: ea.sku, error: 'No inventory_item_id' };
  if (!warehouseLocationId) return { sku: ea.sku, error: 'Warehouse location not set' };
  const d = await shopifyPost('inventory_levels/adjust.json', {
    location_id: Number(warehouseLocationId),
    inventory_item_id: Number(ea.inventoryItemId),
    available_adjustment: Number(ea.qty)
  });
  const lvl = d.inventory_level;
  return { sku: ea.sku, added: ea.qty, newAvailable: lvl ? lvl.available : null };
}

// ═════════════════════════ ROUTES ═══════════════════════════════
router.get('/api/procurement/settings', (req, res) => {
  const s = loadStore();
  res.json({ success: true, settings: s.settings });
});
router.post('/api/procurement/settings', (req, res) => {
  const s = loadStore();
  const b = req.body || {};
  if (b.exRate != null)          s.settings.exRate = num(b.exRate);
  if (b.freightPerGram != null)  s.settings.freightPerGram = num(b.freightPerGram);
  if (b.warehouseLocationId != null) s.settings.warehouseLocationId = String(b.warehouseLocationId);
  if (b.gstLowThreshold != null) s.settings.gstLowThreshold = num(b.gstLowThreshold);
  saveStore(s);
  res.json({ success: true, settings: s.settings });
});

router.get('/api/procurement/lookups', (req, res) => {
  const s = loadStore();
  res.json({ success: true, brand: s.brand, products: s.products, colours: s.colours, sizes: s.sizes });
});
router.post('/api/procurement/lookups', (req, res) => {
  const s = loadStore();
  const b = req.body || {};
  // Add or update a single entry, or replace a whole table.
  if (b.table && b.label && b.code !== undefined) {
    const t = { products: 'products', colours: 'colours', sizes: 'sizes' }[b.table];
    if (!t) return res.status(400).json({ success: false, error: 'Unknown table' });
    if (b.remove) delete s[t][b.label];
    else s[t][b.label] = (t === 'sizes') ? String(b.code).toUpperCase() : Math.round(num(b.code));
    saveStore(s);
    return res.json({ success: true, [t]: s[t] });
  }
  if (b.products) s.products = b.products;
  if (b.colours)  s.colours = b.colours;
  if (b.sizes)    s.sizes = b.sizes;
  if (b.brand)    s.brand = String(b.brand).toUpperCase();
  saveStore(s);
  res.json({ success: true, brand: s.brand, products: s.products, colours: s.colours, sizes: s.sizes });
});

router.get('/api/procurement/next-serial', async (req, res) => {
  try {
    const cat = await loadCatalogue(req.query.refresh === '1');
    const next = nextSerial(cat.maxSerial);
    res.json({ success: true, current: cat.maxSerial, next, skuCount: Object.keys(cat.skuMap).length });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.post('/api/procurement/preview', async (req, res) => {
  try {
    const s = loadStore();
    const preview = await computePreview(s, req.body || {});
    res.json({ success: true, ...preview });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// The gated write. Body carries the user-approved plan (edited SEO allowed).
router.post('/api/procurement/commit', async (req, res) => {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) return res.status(400).json({ success: false, error: 'Shopify env not configured' });
  const s = loadStore();
  const b = req.body || {};
  if (!b.approve) return res.status(400).json({ success: false, error: 'Missing approval flag' });
  const warehouseLocationId = String(b.warehouseLocationId || s.settings.warehouseLocationId || '');
  if (!warehouseLocationId) return res.status(400).json({ success: false, error: 'Warehouse location not set — choose it in Settings first.' });

  const results = { created: [], adjusted: [], errors: [] };
  // 1) Create new draft products.
  for (const np of (b.newProducts || [])) {
    try {
      const r = await createDraftProduct(np, warehouseLocationId);
      results.created.push(r);
    } catch (e) { results.errors.push({ kind: 'create', product: np.seo && np.seo.title, error: e.message }); }
  }
  // 2) Add received qty to existing variants.
  for (const ea of (b.existingAdds || [])) {
    try {
      const r = await addExistingInventory(ea, warehouseLocationId);
      results.adjusted.push(r);
    } catch (e) { results.errors.push({ kind: 'adjust', sku: ea.sku, error: e.message }); }
  }

  // Record the PO in history.
  s.seq += 1;
  const poId = 'PO-' + String(s.seq).padStart(4, '0');
  s.pos[poId] = {
    id: poId,
    createdAt: new Date().toISOString(),
    createdBy: (req.user && req.user.username) || 'system',
    vendor: b.vendor || '',
    billNo: b.billNo || '',
    warehouseLocationId,
    newProducts: b.newProducts || [],
    existingAdds: b.existingAdds || [],
    results
  };
  saveStore(s);
  _catalogue = null; // invalidate cache so new SKUs are seen next time
  res.json({ success: true, poId, results });
});

router.get('/api/procurement/pos', (req, res) => {
  const s = loadStore();
  const list = Object.values(s.pos).sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  res.json({ success: true, pos: list });
});
router.get('/api/procurement/pos/:id', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  res.json({ success: true, po });
});

module.exports = { router, genSeo, buildSku, landedCost, parseSerial, nextSerial };
