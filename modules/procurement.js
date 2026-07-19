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
const crypto  = require('crypto');
const multer  = require('multer');
const fetch   = require('node-fetch');

const router = express.Router();

const SHOPIFY_STORE = process.env.SHOPIFY_STORE;
const SHOPIFY_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const API = '2024-01';

const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const STORE_PATH = process.env.PROCUREMENT_PATH || path.join(DATA_DIR, 'procurement.json');

// ── Raw product photos (mandatory per SKU) ───────────────────────
// These are the source images the AI image module will judge to generate the
// AI photos + SEO. Stored on the persistent volume so they survive redeploys.
const PHOTO_DIR = path.join(DATA_DIR, 'procurement-photos');
try { fs.mkdirSync(PHOTO_DIR, { recursive: true }); } catch { /* exists */ }
const photoUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, PHOTO_DIR),
    filename: (req, file, cb) => {
      const ext = (path.extname(file.originalname || '') || '.jpg').toLowerCase().replace(/[^.a-z0-9]/g, '');
      cb(null, Date.now() + '-' + crypto.randomBytes(6).toString('hex') + (ext || '.jpg'));
    }
  }),
  limits: { fileSize: 15 * 1024 * 1024 },
  fileFilter: (req, file, cb) => cb(null, /^image\//.test(file.mimetype))
});

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
  sizes: { // size label → suffix code (letter sizes for uppers, waist for bottoms)
    'Free Size': 'FS', 'Medium': 'M', 'Large': 'L', 'Extra Large': 'XL',
    'Double Extra Large': 'XXL', 'Triple Extra Large': '3XL', 'Four Extra Large': '4XL',
    'Waist 28': '28', 'Waist 30': '30', 'Waist 32': '32', 'Waist 34': '34',
    'Waist 36': '36', 'Waist 38': '38', 'Waist 40': '40', 'Waist 42': '42', 'Waist 44': '44'
  },
  vendors: [ // known China vendors (from the sheet); ALWAYS stored UPPERCASE
    'CHAOUFI', 'YOK', 'HK', 'NTVG', 'AMAZE VARIETY', 'EU FUN2 TNC', 'TANG',
    'WR+FUNK', 'HS1', 'FUNK', 'BM BAGS', 'TG'
  ],
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
const SIZE_TOKENS = ['FS','XS','S','M','L','XL','XXL','3XL','4XL','5XL','28','30','32','34','36','38','40','42','44'];
// Trailing size embedded in the serial regex (longest-first) so the greedy
// number group backtracks to a VALID size token — critical for numeric waist
// sizes (…33 could otherwise split as num=…3, size='3').
const SIZE_ALT = SIZE_TOKENS.slice().sort((a, b) => b.length - a.length).join('|');
// The running serial number rolls to the next letter at 999, so it is ALWAYS
// 1-3 digits. Bounding the number group to \d{1,3} lets it hand the extra
// leading digit to sizes like 3XL/4XL (e.g. …5633XL = serial 563 + size 3XL,
// not serial 5633 + size XL).
const SERIAL_RE = new RegExp('^SA\\d+([A-Z])(\\d{1,3})(' + SIZE_ALT + ')$');

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
  if (!Array.isArray(s.vendors)) s.vendors = [ ...SEED.vendors ];
  else { // normalize any older mixed-case entries to UPPERCASE + dedupe
    const seen = {}; s.vendors = s.vendors.map(v => String(v).toUpperCase().trim())
      .filter(v => v && !seen[v] && (seen[v] = 1));
  }
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
  const m = String(sku || '').toUpperCase().match(SERIAL_RE);
  if (!m) return null;
  return { alpha: m[1], num: parseInt(m[2], 10) };
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
  // Resolve size: a mapped label, or a raw code already sent by the UI (e.g. '32', 'XL').
  const sc = store.sizes[sizeLabel] ||
    (SIZE_TOKENS.includes(String(sizeLabel).toUpperCase()) ? String(sizeLabel).toUpperCase() : null);
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
  const designCode  = String(g.designCode || '').toUpperCase().trim();
  // Design name is OPTIONAL — fall back to the design code for the title/name.
  const nameForTitle = designName || designCode;
  const productType = g.productType || '';
  const colour      = titleCase(g.colour || '');
  const fit         = titleCase(g.fit || '');
  const fitBase     = fit.replace(/\s*fit$/i, '').trim();   // strip trailing "Fit" so we never double it
  const audience    = g.audience || 'Men';           // 'Men' | 'Women' | 'Unisex'
  const sizeList    = (g.sizeLabels || []).map(l => (g.sizeCodeOf ? g.sizeCodeOf(l) : l)).join(', ');
  const nm          = nameForTitle ? nameForTitle + ' ' : '';

  // Customer-facing product title (the H1 / storefront name).
  const descriptor  = [fitBase ? fitBase + ' Fit' : '', colour].filter(Boolean).join(', ');
  const titleCore   = [nameForTitle, productType].filter(Boolean).join(' ').trim() || productType;
  const title       = descriptor ? `${titleCore} — ${descriptor}` : titleCore;

  // URL handle: clean, keyword-rich. Always fold in the design code (when
  // present) so two same-named products can never collide on the same URL.
  const handleCode = (designCode && designCode.toUpperCase() !== nameForTitle.toUpperCase()) ? designCode : '';
  const handle = slugify([nameForTitle, productType, colour, fitBase, handleCode].filter(Boolean).join(' '))
    || slugify([productType, colour].filter(Boolean).join(' '));

  // SEO <title> (global.title_tag) — keep ~60 chars, brand at the end.
  const metaTitle = truncate(
    `${[fitBase, colour].filter(Boolean).join(' ')} ${productType}${nameForTitle ? ' – ' + nameForTitle : ''} | SANKI`.replace(/\s+/g, ' ').trim(),
    60
  );

  // Meta description (global.description_tag) — natural, AEO-friendly, ~155 chars.
  const audienceWord = audience === 'Unisex' ? 'unisex' : (audience === 'Women' ? "women's" : "men's");
  const metaDescription = truncate(
    `Shop the ${nm}${String(productType).toLowerCase()} in ${colour.toLowerCase()} by SANKI — premium ${audienceWord} streetwear` +
    (fitBase ? `, ${fitBase.toLowerCase()} fit` : '') +
    `. ${sizeList ? 'Sizes ' + sizeList + '. ' : ''}COD available. Limited drop.`,
    160
  );

  // Image alt-text (accessibility + image SEO). One per product image slot.
  const imageAlt = [colour, nameForTitle, productType, 'by SANKI',
    fitBase ? '— ' + fitBase.toLowerCase() + ' fit' : '', audienceWord, 'streetwear']
    .filter(Boolean).join(' ').replace(/\s+/g, ' ').trim();

  // Search tags.
  const tags = Array.from(new Set([
    productType, colour, fitBase ? fitBase + ' Fit' : '', nameForTitle, 'SANKI', 'Streetwear',
    audience === 'Unisex' ? 'Unisex' : audience,
    `${colour} ${productType}`.trim(),
    fitBase ? `${fitBase} ${productType}`.trim() : ''
  ].filter(Boolean)));

  // Body / description HTML (schema-friendly plain prose + feature list).
  const bodyHtml =
    `<p>The <strong>${title}</strong> from SANKI — premium ${audienceWord} streetwear` +
    `${fitBase ? ', ' + fitBase.toLowerCase() + ' fit' : ''}, in ${colour.toLowerCase()}.</p>` +
    `<ul>` +
    `<li>Design: ${nameForTitle || productType}</li>` +
    `<li>Colour: ${colour || '—'}</li>` +
    (fitBase ? `<li>Fit: ${fitBase} Fit</li>` : '') +
    (sizeList ? `<li>Available sizes: ${sizeList}</li>` : '') +
    `<li>Cash on Delivery available · Limited drop</li>` +
    `</ul>`;

  return { title, handle, metaTitle, metaDescription, imageAlt, tags, bodyHtml };
}

// ── Grouping: intake lines → products (one product per design×colour) ──
// A "group key" identifies one product whose sizes become the variants.
// The DESIGN CODE (sheet's "CODE AS PER PRODUCT") is the primary identifier;
// design name is optional and only used for the SEO title. We fall back to
// the name if no code is given.
function groupKey(l) {
  const design = (l.designCode || l.designName || '').trim().toLowerCase();
  return [design, l.productType || '', l.colour || '', l.fit || '', l.audience || 'Men']
    .map(x => String(x).trim().toLowerCase()).join('|');
}

// Normalize a raw intake line into a clean, storable shape. Weight is optional
// at the ADVANCE stage (product not yet received / weighed).
function normalizeLine(raw, body) {
  return {
    designName:  (raw.designName || '').trim(),
    productType: (raw.productType || '').trim(),
    colour:      (raw.colour || '').trim(),
    sizeLabel:   (raw.sizeLabel || '').trim(),   // Indian size — used for the SKU
    chinaSize:   (raw.chinaSize || '').trim(),   // China size — recorded only
    fit:         (raw.fit || '').trim(),
    audience:    (raw.audience || 'Men').trim(),
    vendor:      (raw.vendor || (body && body.vendor) || '').trim(),  // vendor comes from the bill
    designCode:  (raw.designCode || '').trim(),
    photoUrl:    (raw.photoUrl || '').trim(),        // mandatory raw image → AI pipeline
    sku:         (raw.sku || '').toUpperCase().trim(),
    qty:         Math.max(0, Math.round(num(raw.qty))),
    perPcsYuan:  num(raw.perPcsYuan),
    weightGrams: num(raw.weightGrams)               // 0 until received & weighed
  };
}

// Highest serial already RESERVED by not-yet-posted POs. Advance POs generate
// SKUs immediately but only reach Shopify at the receive/post stage, so their
// serials aren't in the live catalogue yet — we must not hand them out twice.
function pendingSerialMax(store) {
  let best = null;
  Object.values(store.pos || {}).forEach(po => {
    if (po.status === 'posted') return;   // already on Shopify → counted via catalogue
    (po.lines || []).forEach(l => {
      if (l.serialUsed && serialGt(l.serialUsed, best)) best = { ...l.serialUsed };
    });
  });
  return best;
}

// Compute a full preview for a set of intake lines (no writes).
async function computePreview(store, body) {
  const settings = { ...store.settings };
  if (body.exRate != null && body.exRate !== '')       settings.exRate = num(body.exRate);
  if (body.freightPerGram != null && body.freightPerGram !== '') settings.freightPerGram = num(body.freightPerGram);

  const cat = await loadCatalogue(!!body.refresh);
  const sizeCodeOf = (label) => store.sizes[label] || label;

  // Serial cursor starts from the live Shopify max, but also clears any serials
  // already reserved by pending advance POs so two advance purchases can never
  // collide. Lines that already carry a SKU (from the advance stage) keep it.
  let cursor = cat.maxSerial ? { ...cat.maxSerial } : null;
  const pend = pendingSerialMax(store);
  if (pend && serialGt(pend, cursor)) cursor = { ...pend };

  const lines = (body.lines || []).map(raw => {
    const line = normalizeLine(raw, body);
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
      designName: g.designName, designCode: g.designCode, productType: g.productType, colour: g.colour,
      fit: g.fit, audience: g.audience, sizeLabels: g.lines.map(l => l.sizeLabel), sizeCodeOf
    });
    // Ambiguous if it has neither a design code nor a design name — two truly
    // different products could otherwise merge into one.
    const ambiguous = !((g.designCode || '').trim() || (g.designName || '').trim());
    return {
      key: g.key, vendor: g.vendor, designCode: g.designCode, designName: g.designName,
      colour: g.colour, productType: g.productType, ambiguous,
      seo,
      variants: g.lines.map(l => ({
        sku: l.sku, sizeLabel: l.sizeLabel, sizeCode: sizeCodeOf(l.sizeLabel), chinaSize: l.chinaSize,
        qty: l.qty, landed: l.landed, price: l.suggestedMrp, skuError: l.skuError
      }))
    };
  });

  const existingAdds = lines.filter(l => l.classification === 'EXISTING').map(l => ({
    sku: l.sku, qty: l.qty, landed: l.landed, chinaSize: l.chinaSize,
    productId: l.existing.productId, variantId: l.existing.variantId, inventoryItemId: l.existing.inventoryItemId
  }));

  return {
    settings, warehouseLocationId: store.settings.warehouseLocationId,
    lines, newProducts, existingAdds,
    counts: { total: lines.length, newProducts: newProducts.length, existingAdds: existingAdds.length,
              errors: lines.filter(l => l.skuError).length,
              ambiguous: newProducts.filter(p => p.ambiguous).length }
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
      ].concat(
        // Vendor's own product code (from the invoice) — stored for future
        // identification / restock traceability. Never generated by us.
        np.designCode ? [{ namespace: 'custom', key: 'vendor_code', value: String(np.designCode), type: 'single_line_text_field' }] : []
      ).concat(
        np.vendor ? [{ namespace: 'custom', key: 'vendor', value: String(np.vendor), type: 'single_line_text_field' }] : []
      )
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

// ── Role helpers: SEO is ADMIN-ONLY ──────────────────────────────
// Inventory managers create advance POs, weigh on arrival, and REQUEST
// approval — but they never see the generated SEO and cannot post to Shopify.
// The admin reviews the SEO and is the only one who can post.
function isAdmin(req) { return !!(req.user && req.user.role === 'admin'); }
function publicPo(po, req) {
  if (isAdmin(req)) return po;
  const clone = JSON.parse(JSON.stringify(po));
  delete clone.seoDraft;                                   // hide SEO drafts
  (clone.newProducts || []).forEach(np => { delete np.seo; });
  return clone;
}
function stripPreviewForRole(preview, req) {
  if (isAdmin(req)) return preview;
  (preview.newProducts || []).forEach(np => { delete np.seo; });
  return preview;
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

// ── Photo upload / serve (mandatory raw image per SKU) ───────────
router.post('/api/procurement/photo', photoUpload.single('photo'), (req, res) => {
  if (!req.file) return res.status(400).json({ success: false, error: 'No image received (JPG/PNG/WebP only).' });
  res.json({ success: true, file: req.file.filename, url: '/api/procurement/photo/' + req.file.filename });
});
router.get('/api/procurement/photo/:file', (req, res) => {
  // Guard against path traversal — only serve plain filenames from PHOTO_DIR.
  const name = path.basename(String(req.params.file || ''));
  const fp = path.join(PHOTO_DIR, name);
  if (!fp.startsWith(PHOTO_DIR) || !fs.existsSync(fp)) return res.status(404).end();
  res.sendFile(fp);
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

// ── Vendors (persisted list; dropdown + add-new) ─────────────────
router.get('/api/procurement/vendors', (req, res) => {
  const s = loadStore();
  res.json({ success: true, vendors: s.vendors });
});
router.post('/api/procurement/vendors', (req, res) => {
  const s = loadStore();
  const b = req.body || {};
  const name = String(b.name || '').toUpperCase().trim();   // vendors are ALWAYS uppercase
  if (!name) return res.status(400).json({ success: false, error: 'Vendor name required.' });
  if (b.remove) {
    s.vendors = s.vendors.filter(v => v !== name);
  } else if (!s.vendors.includes(name)) {
    s.vendors.push(name);
  }
  saveStore(s);
  res.json({ success: true, vendors: s.vendors });
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

// ── Stage 1: save an ADVANCE purchase — SKUs generated NOW, no Shopify write ──
// The product hasn't arrived, so there is no weight / freight / final landed
// cost yet. But SKUs ARE assigned here (and reserved via pendingSerialMax) so
// photos / AI images can be prepared against a real SKU during the lead time.
router.post('/api/procurement/advance', async (req, res) => {
  try {
    const s = loadStore();
    const b = req.body || {};
    if (!(b.lines || []).length) return res.status(400).json({ success: false, error: 'Add at least one line before saving.' });
    // Photo is mandatory per SKU — it is what the AI image module will judge.
    const missingPhoto = (b.lines || []).filter(l => !(l.photoUrl || '').trim()).length;
    if (missingPhoto) return res.status(400).json({ success: false, error: 'Every line needs a product photo (' + missingPhoto + ' missing).' });
    // Run the preview engine to assign SKUs + classify NEW vs EXISTING (weight
    // is 0 at this stage, so any landed figure is provisional and unused here).
    const preview = await computePreview(s, { lines: b.lines, vendor: b.vendor, exRate: b.exRate });
    const lines = preview.lines.map(l => ({
      designName: l.designName, productType: l.productType, colour: l.colour,
      sizeLabel: l.sizeLabel, chinaSize: l.chinaSize, fit: l.fit, audience: l.audience,
      vendor: l.vendor, designCode: l.designCode, photoUrl: l.photoUrl,
      qty: l.qty, perPcsYuan: l.perPcsYuan,
      weightGrams: 0,                                   // filled at receive
      sku: l.sku, serialUsed: l.serialUsed || null, skuError: l.skuError || null,
      classification: l.classification                 // NEW = create; EXISTING = restock
    }));
    s.seq += 1;
    const poId = 'PO-' + String(s.seq).padStart(4, '0');
    s.pos[poId] = {
      id: poId,
      status: 'advance',               // advance → received → awaiting_approval → posted
      createdAt: new Date().toISOString(),
      createdBy: (req.user && req.user.username) || 'system',
      vendor: String(b.vendor || '').toUpperCase().trim(),
      billNo: b.billNo || '',
      datePurchase: b.datePurchase || '',
      dateReceive: '',
      leadTimeDays: b.leadTimeDays != null && b.leadTimeDays !== '' ? Math.max(0, Math.round(num(b.leadTimeDays))) : null,
      // Expected arrival = purchase date + lead time (China→India transit). Used
      // in the Receive tab to show a live countdown to delivery.
      expectedReceiveDate: (function(){
        const lt = b.leadTimeDays != null && b.leadTimeDays !== '' ? Math.max(0, Math.round(num(b.leadTimeDays))) : null;
        const base = b.datePurchase ? new Date(b.datePurchase) : new Date();
        if (lt == null || isNaN(base.getTime())) return '';
        base.setDate(base.getDate() + lt);
        return base.toISOString().slice(0, 10);
      })(),
      exRate: b.exRate != null && b.exRate !== '' ? num(b.exRate) : s.settings.exRate,
      // Freight rate captured at advance time so it can be shown (and edited) at receive.
      freightPerGram: b.freightPerGram != null && b.freightPerGram !== '' ? num(b.freightPerGram) : s.settings.freightPerGram,
      lines,                           // intake lines WITH generated SKUs
      // SEO drafts generated at SKU time (admin-only). Keyed by product group.
      // Placeholder text drafts for now — the AI image module will regenerate
      // these by judging the uploaded photos. Never shown to non-admins.
      seoDraft: (preview.newProducts || []).map(np => ({ key: np.key, designCode: np.designCode, colour: np.colour, productType: np.productType, seo: np.seo })),
      results: null
    };
    // Remember any newly-typed vendor so it appears in the dropdown next time.
    const vn = String(b.vendor || '').toUpperCase().trim();
    if (vn && !s.vendors.includes(vn)) s.vendors.push(vn);
    saveStore(s);
    // Stage 1 must NEVER expose SEO — those drafts are for the admin at stage 2
    // only. Strip seoDraft from the advance-save response entirely.
    const out = publicPo(s.pos[poId], req);
    delete out.seoDraft;
    res.json({ success: true, poId, po: out, lines: out.lines });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ── Stage 2a: receive an advance PO — attach weights, compute the preview ──
// Merges the per-line weights the user recorded on arrival, then generates
// SKUs + landed cost + draft SEO for approval (still no Shopify write).
router.post('/api/procurement/pos/:id/receive', async (req, res) => {
  try {
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    if (po.status === 'posted') return res.status(400).json({ success: false, error: 'This PO is already posted to Shopify.' });
    const b = req.body || {};
    const weights = b.weights || {};            // { lineIndex: grams }
    po.lines = (po.lines || []).map((l, i) => {
      const w = weights[i] != null ? num(weights[i]) : num(l.weightGrams);
      return { ...l, weightGrams: w };
    });
    if (b.dateReceive) po.dateReceive = b.dateReceive;
    if (b.freightPerGram != null && b.freightPerGram !== '') po.freightPerGram = num(b.freightPerGram);
    if (b.exRate != null && b.exRate !== '') po.exRate = num(b.exRate);
    if (po.status === 'advance') po.status = 'received';
    saveStore(s);
    const preview = await computePreview(s, {
      lines: po.lines, vendor: po.vendor,
      exRate: po.exRate, freightPerGram: po.freightPerGram
    });
    // Overlay the SEO drafts saved at advance so the admin edits persist.
    if (isAdmin(req) && Array.isArray(po.seoDraft)) {
      (preview.newProducts || []).forEach(np => {
        const d = po.seoDraft.find(x => x.key === np.key);
        if (d && d.seo) np.seo = d.seo;
      });
    }
    res.json({ success: true, poId: po.id, po: publicPo(po, req), ...stripPreviewForRole(preview, req) });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ── Edit a PO's header + drop lines (not posted) ─────────────────
// Header fields (vendor / bill / dates / lead time) can be corrected any time
// before the PO is posted. Individual lines may be removed. Remaining lines
// keep their frozen SKUs. Recomputes the expected arrival from the new inputs.
router.patch('/api/procurement/pos/:id', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (po.status === 'posted') return res.status(400).json({ success: false, error: 'A posted PO can no longer be edited.' });
  const b = req.body || {};
  if (b.vendor != null)       po.vendor = String(b.vendor).toUpperCase().trim();
  if (b.billNo != null)       po.billNo = String(b.billNo).trim();
  if (b.datePurchase != null) po.datePurchase = String(b.datePurchase);
  if (b.leadTimeDays != null && b.leadTimeDays !== '') po.leadTimeDays = Math.max(0, Math.round(num(b.leadTimeDays)));
  // Recompute expected arrival = purchase date (or today) + lead time.
  if (po.leadTimeDays != null) {
    const base = po.datePurchase ? new Date(po.datePurchase) : new Date();
    if (!isNaN(base.getTime())) { base.setDate(base.getDate() + po.leadTimeDays); po.expectedReceiveDate = base.toISOString().slice(0, 10); }
  }
  // Drop selected line indexes (from the ORIGINAL ordering).
  if (Array.isArray(b.removeLineIndexes) && b.removeLineIndexes.length) {
    const drop = new Set(b.removeLineIndexes.map(Number));
    po.lines = (po.lines || []).filter((_, i) => !drop.has(i));
    po.seoDraft = (po.seoDraft || []).filter(d => (po.lines || []).some(l => groupKey(l) === d.key));
  }
  if (b.vendor) { const vn = String(b.vendor).toUpperCase().trim(); if (vn && !s.vendors.includes(vn)) s.vendors.push(vn); }
  saveStore(s);
  const out = publicPo(po, req); delete out.seoDraft;
  res.json({ success: true, poId: po.id, po: out });
});

// ── Delete a PO entirely (not posted) ────────────────────────────
router.delete('/api/procurement/pos/:id', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (po.status === 'posted') return res.status(400).json({ success: false, error: 'A posted PO can no longer be deleted.' });
  delete s.pos[req.params.id];
  saveStore(s);
  res.json({ success: true, deleted: req.params.id });
});

// ── Stage 2b: inventory REQUESTS admin approval (no Shopify write) ──
// Once weights are in, the inventory manager sends the PO to the admin. Only
// the admin can then post it to Shopify (see commit, admin-gated).
router.post('/api/procurement/pos/:id/request-approval', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (po.status === 'posted') return res.status(400).json({ success: false, error: 'Already posted.' });
  const unweighed = (po.lines || []).filter(l => !(num(l.weightGrams) > 0)).length;
  if (unweighed) return res.status(400).json({ success: false, error: 'Enter weight for every line first (' + unweighed + ' missing).' });
  po.status = 'awaiting_approval';
  po.approvalRequestedBy = (req.user && req.user.username) || 'system';
  po.approvalRequestedAt = new Date().toISOString();
  saveStore(s);
  res.json({ success: true, poId: po.id, status: po.status });
});

// The gated write. Body carries the user-approved plan (edited SEO allowed).
router.post('/api/procurement/commit', async (req, res) => {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) return res.status(400).json({ success: false, error: 'Shopify env not configured' });
  // ADMIN ONLY: posting to Shopify is the admin's approval step.
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Only an admin can approve and post to Shopify.' });
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

  // Finalize the PO. If a poId is supplied (Stage-2 receive→post flow) we mark
  // that advance PO as posted; otherwise we record a fresh one (legacy path).
  let poId, po;
  if (b.poId && s.pos[b.poId]) {
    poId = b.poId; po = s.pos[poId];
    po.status = 'posted';
    po.postedAt = new Date().toISOString();
    po.dateReceive = b.dateReceive || po.dateReceive || '';
    po.warehouseLocationId = warehouseLocationId;
    po.newProducts = b.newProducts || [];
    po.existingAdds = b.existingAdds || [];
    po.results = results;
  } else {
    s.seq += 1;
    poId = 'PO-' + String(s.seq).padStart(4, '0');
    po = s.pos[poId] = {
      id: poId,
      status: 'posted',
      createdAt: new Date().toISOString(),
      postedAt: new Date().toISOString(),
      createdBy: (req.user && req.user.username) || 'system',
      vendor: b.vendor || '',
      billNo: b.billNo || '',
      datePurchase: b.datePurchase || '',
      dateReceive: b.dateReceive || '',
      warehouseLocationId,
      newProducts: b.newProducts || [],
      existingAdds: b.existingAdds || [],
      results
    };
  }
  saveStore(s);
  _catalogue = null; // invalidate cache so new SKUs are seen next time
  res.json({ success: true, poId, results });
});

router.get('/api/procurement/pos', (req, res) => {
  const s = loadStore();
  let list = Object.values(s.pos);
  if (req.query.status) { const want = String(req.query.status).split(','); list = list.filter(p => want.includes(p.status)); }
  list = list.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  res.json({ success: true, pos: list.map(p => publicPo(p, req)) });
});
router.get('/api/procurement/pos/:id', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  res.json({ success: true, po: publicPo(po, req) });
});

module.exports = { router, genSeo, buildSku, landedCost, parseSerial, nextSerial };
