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

// ── Invoice auto-fill (Chinese vendor invoice → structured lines) ──
// The invoice is held in memory (base64 → AI vision), never persisted. Accepts
// an image OR a PDF. Requires ANTHROPIC_API_KEY; model is overridable.
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const AI_MODEL = process.env.PROCUREMENT_AI_MODEL || 'claude-sonnet-4-6';
const invoiceUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 },
  fileFilter: (req, file, cb) => cb(null, /^image\/|application\/pdf/.test(file.mimetype))
});

// ── AI product images (Google Gemini image generation) ───────────
// Feed the raw invoice/garment photo to Gemini and get back polished listing
// images: a clean model shot, front-only, back-only and a studio product shot.
// Requires GEMINI_API_KEY (or GOOGLE_API_KEY). Model is overridable.
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
const IMAGE_MODEL = process.env.PROCUREMENT_IMAGE_MODEL || 'gemini-2.5-flash-image';
// The four shots we make for every product. `prompt` is suffixed with the
// product context (type / colour / audience) at generation time.
const AI_IMAGE_SPECS = [
  { type: 'female', label: 'Female model', aspect: '4:5',
    prompt: 'Generate a clean e-commerce photo of a real FEMALE human model wearing THIS EXACT garment. Keep the garment identical — same colour, print, graphics, cut and details as the reference image; do not redesign it. Neutral light-grey studio background, soft even lighting, natural relaxed pose, streetwear styling. Show EXACTLY ONE model, the full body from head to shoes fully inside the frame with even margin above the head and below the shoes. MANDATORY: the model must face the camera with the COMPLETE FACE fully visible and unobstructed — clear, sharp facial features; never hide, crop, turn away, blur or obscure the face; hair, hands or props must not cover the face; do NOT crop the head out of frame; the full head and face must always be inside the picture. Photorealistic, high resolution. Do NOT place anything below the shoes; do NOT add any duplicated, repeated, tiled or extra copies of the garment; no second image, no split frame, no collage, no text or watermark.' },
  { type: 'male',   label: 'Male model', aspect: '4:5',
    prompt: 'Generate a clean e-commerce photo of a real MALE human model wearing THIS EXACT garment. Keep the garment identical — same colour, print, graphics, cut and details as the reference image; do not redesign it. Neutral light-grey studio background, soft even lighting, natural relaxed pose, streetwear styling. Show EXACTLY ONE model, the full body from head to shoes fully inside the frame with even margin above the head and below the shoes. MANDATORY: the model must face the camera with the COMPLETE FACE fully visible and unobstructed — clear, sharp facial features; never hide, crop, turn away, blur or obscure the face; hair, hands or props must not cover the face; do NOT crop the head out of frame; the full head and face must always be inside the picture. Photorealistic, high resolution. Do NOT place anything below the shoes; do NOT add any duplicated, repeated, tiled or extra copies of the garment; no second image, no split frame, no collage, no text or watermark.' },
  { type: 'front',  label: 'Product front', aspect: '4:5',
    prompt: 'Create a BRAND-NEW professional e-commerce INVISIBLE-MANNEQUIN (hollow-man / ghost-mannequin) packshot of this garment. This is NOT a copy task — do NOT return, crop, filter, recolour or re-frame the reference photo; re-photograph the garment completely from scratch as a fresh studio image. Preserve the garment\'s DESIGN and SHAPE with total fidelity — EXACTLY the same colour, plaid/print pattern, graphics, stripes, badges, panels, buttons, pockets, collar, sleeve length, hem, overall cut and proportions as the reference; do NOT distort, warp, stretch, rearrange, redesign or invent any part of it. Present the FRONT of the garment as if worn on a body then the body digitally removed, so it holds a clean, filled-out, symmetric 3D worn shape (shoulders, chest and sleeves naturally shaped), smooth and wrinkle-free, like a premium catalogue product. CRITICAL — the mannequin is INVISIBLE: do NOT render any visible mannequin, dummy, tailor\'s form, dress form, bust, torso, neck, skin, head, shoulders-as-body, hands, arms or legs. The inside of the collar and neck opening must be HOLLOW — you see straight THROUGH it to the pure-white background behind; there must be NO cream, grey, white or skin-toned neck, plug or filler showing in the collar opening. Just the empty garment floating in its worn shape. Completely REMOVE everything in the reference that is not this single garment — any background, table, floor, hanger, packaging, plastic, paper, tags, stickers, price labels, pins, clips, accessories or other objects must NOT appear. Center the garment on a seamless pure-white studio background, even soft lighting, sharp product detail, no human model, no props, no visible hanger, no text or watermark. Do NOT show any inner neck label, brand tag, size tag or care label. Show a SINGLE garment fully in frame — no duplicated or repeated copies, no collage, no second view. IGNORE the reference photo\'s own edges, corners, border, vignette and background colour; do NOT reproduce any coloured or dark rectangle, rounded corner, drop shadow or padding around it. The pure-white background must fill the ENTIRE frame and bleed to ALL FOUR CORNERS — absolutely no black bars, letterboxing, borders, rounded edges, vignette or leftover colour from the source photo.' },
  { type: 'back',   label: 'Product back',
    prompt: 'Generate a clean FLAT-LAY / ghost-mannequin photo showing the BACK (reverse side) of this exact garment, centered on a pure white background. Keep the same colour, fabric, cut and length as the reference. The large graphics, numbers or prints that appear on the FRONT must NOT be shown on the back — render the rear panel as it would realistically look (usually plainer). Do NOT show any inner neck label, brand tag, size tag or care label — the collar/neckline must be clean with no visible tag. Even studio lighting, no model, no props, no text or watermark. Show a SINGLE garment fully in frame; no duplicated or repeated copies, no collage. The background must be pure white filling the ENTIRE frame to all four edges — absolutely no black bars, letterboxing, borders or coloured padding.' }
];

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
// opts.origin === 'india' → the goods are bought locally in ₹: `perPcsYuan`
// holds the ₹ unit cost (no exchange-rate conversion) and freight is a flat
// per-piece transport share (opts.transportPerPc) instead of weight×rate.
// Anything else (default) is the China model: ¥ × exRate + weight × freight.
function landedCost(line, settings, opts) {
  const india = opts && opts.origin === 'india';
  const inrValue    = india ? num(line.perPcsYuan)
                            : num(line.perPcsYuan) * num(settings.exRate);
  const freightPerPc = india ? num(opts.transportPerPc)
                             : num(line.weightGrams) * num(settings.freightPerGram);
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
// Strip internal warehouse codes out of any CUSTOMER-FACING string. Vendor
// design numbers (e.g. "71383") and SKU fragments (e.g. "SA-2-11-FS") must never
// appear in a storefront name, title, meta or alt text — they hurt SEO/AEO/GEO
// and read like noise to shoppers and to answer/generative engines.
function stripInternalCodes(str, designCode) {
  if (!str) return '';
  let out = String(str);
  if (designCode) {
    const esc = String(designCode).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    out = out.replace(new RegExp('\\b' + esc + '\\b', 'ig'), ' ');
  }
  out = out
    .replace(/\bSA[-\s]?\d+(?:[-\s]?\w+)*\b/ig, ' ')   // brand SKU fragments (SA-2-11-FS)
    .replace(/\b\d{3,}\b/g, ' ')                        // bare vendor codes (71383)
    .replace(/\s{2,}/g, ' ')                            // collapse gaps left by removals
    .replace(/\s+,/g, ',')                              // no space before a comma
    .replace(/,(?=\s*(?:,|$))/g, '')                    // drop empty comma segments
    .replace(/\s*([—–|])\s*/g, ' $1 ')                 // single space around en/em-dash & pipe (leave hyphens in T-Shirt alone)
    .replace(/^[\s—–|,]+|[\s—–|,]+$/g, '')             // trim stray leading/trailing separators
    .replace(/\s{2,}/g, ' ')
    .trim();
  return out;
}
function genSeo(g) {
  const designCode  = String(g.designCode || '').toUpperCase().trim();
  // Clean the vendor design name of any embedded codes; if nothing meaningful
  // is left, we simply omit the name and let colour + product type carry it.
  const designName  = stripInternalCodes(titleCase(g.designName || ''), designCode);
  // Customer-facing copy NEVER falls back to the raw design code.
  const nameForTitle = designName;
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

  // Origin of the whole PO: 'india' (bought locally in ₹, flat transport split
  // by piece count) or 'china' (default: ¥ × exRate + weight × per-gram freight).
  const origin = body.origin === 'india' ? 'india' : 'china';
  const totalQty = (body.lines || []).reduce((s, l) => s + (num(l.qty) || 0), 0);
  const transportPerPc = (origin === 'india' && totalQty > 0)
    ? num(body.transportTotal) / totalQty : 0;
  const costOpts = { origin, transportPerPc };

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
    const cost = landedCost(line, settings, costOpts);

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
    // A representative uploaded photo (first line that has one) so the studio
    // can show what the product actually is.
    const repLine = g.lines.find(l => (l.photoUrl || '').trim());
    return {
      key: g.key, vendor: g.vendor, designCode: g.designCode, designName: g.designName,
      colour: g.colour, productType: g.productType, ambiguous,
      photoUrl: repLine ? repLine.photoUrl : '',
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
    origin, transportTotal: origin === 'india' ? num(body.transportTotal) : 0,
    transportPerPc: round2(transportPerPc),
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
  // Attach the approved AI images (base64) so the listing is born with photos.
  // Shopify can't fetch our private URLs, so we upload each as an attachment.
  const imgs = Array.isArray(np.images) ? np.images : [];
  const attachments = imgs.map(im => {
    const src = readStoredPhoto(im.url);
    return src ? { attachment: src.buf.toString('base64'), alt: (im.alt || np.seo.imageAlt || '').slice(0, 512) } : null;
  }).filter(Boolean);
  if (attachments.length) payload.product.images = attachments;
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
// Attach (or clear) a real BACK-view reference photo for a product group so the
// "Product back" shot is generated from the true reverse instead of guessing.
// The image itself is uploaded via /api/procurement/photo first; we just store
// its URL against the group on the PO.
router.post('/api/procurement/pos/:id/back-ref', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  const b = req.body || {};
  if (!b.groupKey) return res.status(400).json({ success: false, error: 'groupKey required.' });
  po.backRefs = po.backRefs || {};
  if (b.url) po.backRefs[b.groupKey] = String(b.url);
  else delete po.backRefs[b.groupKey];
  saveStore(s);
  res.json({ success: true, groupKey: b.groupKey, url: po.backRefs[b.groupKey] || '' });
});

// ── AI image helpers (Gemini) ────────────────────────────────────
// Persist a generated image buffer to the photo volume, return its URL.
function savePhotoBuffer(buf, ext) {
  const name = Date.now() + '-' + crypto.randomBytes(6).toString('hex') + (ext || '.jpg');
  fs.writeFileSync(path.join(PHOTO_DIR, name), buf);
  return { file: name, url: '/api/procurement/photo/' + name };
}
// Read a stored /api/procurement/photo/<file> URL back into a buffer + mime.
function readStoredPhoto(url) {
  const name = path.basename(String(url || ''));
  const fp = path.join(PHOTO_DIR, name);
  if (!fp.startsWith(PHOTO_DIR) || !fs.existsSync(fp)) return null;
  const ext = path.extname(name).toLowerCase();
  const mime = ext === '.png' ? 'image/png' : ext === '.webp' ? 'image/webp' : 'image/jpeg';
  return { buf: fs.readFileSync(fp), mime };
}
// Call Gemini image generation: reference image + instruction → new image buffer.
async function geminiGenerateImage(baseB64, baseMime, prompt, aspect) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${IMAGE_MODEL}:generateContent?key=${GEMINI_API_KEY}`;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 120000);
  // Pin the output aspect ratio so full-body model shots don't come back as an
  // over-tall canvas that Gemini fills by tiling a duplicate of the garment.
  const generationConfig = { responseModalities: ['IMAGE'] };
  if (aspect) generationConfig.imageConfig = { aspectRatio: aspect };
  let r;
  try {
    r = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        contents: [{ parts: [{ inline_data: { mime_type: baseMime, data: baseB64 } }, { text: prompt }] }],
        generationConfig
      }),
      signal: ctrl.signal
    });
  } catch (err) {
    clearTimeout(timer);
    if (err.name === 'AbortError') { const e = new Error('Image generation timed out.'); e.timeout = true; throw e; }
    throw err;
  }
  clearTimeout(timer);
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error('Gemini ' + r.status + ': ' + (((j.error && j.error.message) || '').slice(0, 200) || 'image error'));
  const parts = ((((j.candidates || [])[0] || {}).content) || {}).parts || [];
  const img = parts.find(p => p.inlineData || p.inline_data);
  const blob = img && (img.inlineData || img.inline_data);
  const data = blob && blob.data;
  if (!data) throw new Error('The model returned no image (try again or use a clearer source photo).');
  const mime = (blob.mimeType || blob.mime_type || 'image/png').toLowerCase();
  return { buf: Buffer.from(data, 'base64'), mime };
}
// Map an image mime type to the file extension Shopify (and the browser) expect.
function extForMime(mime) {
  if (mime === 'image/png') return '.png';
  if (mime === 'image/webp') return '.webp';
  if (mime === 'image/gif') return '.gif';
  return '.jpg';
}

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

// ── Parse a (Chinese) vendor invoice into vendor-bill + intake lines ──
// Vision-LLM OCR + translate + structured extraction. Returns a DRAFT the user
// reviews/edits in the normal Lines table before saving the advance PO. Nothing
// is written — this only pre-fills the form to save manual typing.
function pickClosest(value, options) {
  const v = String(value || '').trim().toLowerCase();
  if (!v) return '';
  const exact = options.find(o => o.toLowerCase() === v);
  if (exact) return exact;
  const part = options.find(o => o.toLowerCase().includes(v) || v.includes(o.toLowerCase()));
  return part || '';
}
function extractJsonBlock(text) {
  if (!text) return null;
  // Prefer a fenced ```json block; else the first {...} span.
  const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const raw = fence ? fence[1] : text;
  const start = raw.indexOf('{'); const end = raw.lastIndexOf('}');
  if (start < 0 || end < 0 || end <= start) return null;
  try { return JSON.parse(raw.slice(start, end + 1)); } catch { return null; }
}
router.post('/api/procurement/parse-invoice', invoiceUpload.single('invoice'), async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'Invoice auto-fill is not enabled. Set ANTHROPIC_API_KEY in Railway to turn it on.' });
    if (!req.file) return res.status(400).json({ success: false, error: 'No invoice received — attach a photo or PDF of the vendor invoice.' });
    const s = loadStore();
    const products = Object.keys(s.products);
    const colours = Object.keys(s.colours);
    const sizes = ['FS', 'M', 'L', 'XL', 'XXL', '3XL', '4XL', '28', '30', '32', '34', '36', '38', '40', '42', '44'];
    const fits = ['Oversized', 'Drop Shoulder', 'Boxy Fit', 'Relaxed Fit', 'Regular Fit', 'Slim Fit', 'Muscle Fit',
                  'Baggy Fit', 'Straight Fit', 'Tapered Fit', 'Skinny Fit', 'Narrow Fit', 'Wide Leg', 'Bootcut', 'Cargo Fit'];
    const b64 = req.file.buffer.toString('base64');
    const isPdf = /pdf/.test(req.file.mimetype);
    const media = isPdf
      ? { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: b64 } }
      : { type: 'image', source: { type: 'base64', media_type: req.file.mimetype, data: b64 } };
    const prompt =
`You are reading a garment supplier INVOICE for an Indian streetwear brand. ` +
`It may be PRINTED or HANDWRITTEN, and in Chinese, English, or a mix. ` +
`Read handwriting carefully. OCR it, translate any Chinese to English, and extract EVERY line item. ` +
`Do your best on messy or handwritten bills — infer product/colour/size/qty/price from whatever is legible rather than giving up.\n\n` +
`Return STRICT JSON ONLY (no prose) in exactly this shape:\n` +
`{"vendor":"","billNo":"","datePurchase":"YYYY-MM-DD","lines":[{"designName":"","designCode":"","productType":"","colour":"","fit":"","sizeLabel":"","chinaSize":"","qty":0,"perPcsYuan":0,"photoBox":null}]}\n\n` +
`Rules:\n` +
`- vendor = supplier/company name (romanise if Chinese). billNo = invoice/order number. datePurchase = the invoice date.\n` +
`- designCode = the vendor's product/style code printed on the invoice (e.g. A611). designName = a short English working name for the garment.\n` +
`- productType: map to the CLOSEST of [${products.join(', ')}] or "" if unclear.\n` +
`- colour: map to the CLOSEST of [${colours.join(', ')}] or "" if unclear.\n` +
`- fit: map to the CLOSEST of [${fits.join(', ')}] or "" if not stated.\n` +
`- sizeLabel = the Indian/global size, one of [${sizes.join(', ')}] ("FS" = free size). chinaSize = the size as printed on the invoice (M/L/XL… or "FS").\n` +
`- If a row lists several sizes, output ONE line PER size with its own qty.\n` +
`- qty = pieces (integer). perPcsYuan = unit price in RMB/¥ (number only).\n` +
`- photoBox = if this line has a PRODUCT PHOTO/THUMBNAIL on the invoice, its bounding box as [x0,y0,x1,y1] normalised 0..1 (left,top,right,bottom of the whole page). Use null if there is no product image for the line. Lines sharing one photo may repeat the same box.\n` +
`- Never invent data — leave a field "" or 0 (or null for photoBox) if the invoice does not show it.`;
    // Guard the vision call with a hard timeout so a slow/hung upstream returns
    // a clean JSON error to the browser instead of the platform's plain-text
    // "upstream error" (which the frontend can't JSON.parse).
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 90000);
    let r;
    try {
      r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
        body: JSON.stringify({ model: AI_MODEL, max_tokens: 4000, messages: [{ role: 'user', content: [media, { type: 'text', text: prompt }] }] }),
        signal: ctrl.signal
      });
    } catch (err) {
      clearTimeout(timer);
      if (err.name === 'AbortError') return res.status(504).json({ success: false, error: 'The invoice reader timed out. Try a smaller/clearer photo, or enter the lines manually.' });
      return res.status(502).json({ success: false, error: 'Could not reach the invoice reader: ' + err.message });
    }
    clearTimeout(timer);
    const j = await r.json().catch(() => ({}));
    if (!r.ok) return res.status(502).json({ success: false, error: 'AI error: ' + ((j.error && j.error.message) || ('HTTP ' + r.status)) });
    const text = (j.content || []).map(c => c.text || '').join('');
    const parsed = extractJsonBlock(text);
    if (!parsed) return res.status(502).json({ success: false, error: 'Could not read the invoice. Try a clearer photo, or enter the lines manually.' });
    const lines = (parsed.lines || []).map(l => ({
      designName: String(l.designName || '').trim(),
      designCode: String(l.designCode || '').trim(),
      productType: pickClosest(l.productType, products),
      colour: pickClosest(l.colour, colours),
      fit: pickClosest(l.fit, fits),
      sizeLabel: pickClosest(l.sizeLabel, sizes) || String(l.sizeLabel || '').trim(),
      chinaSize: String(l.chinaSize || '').trim(),
      audience: 'Men',
      qty: Math.max(0, Math.round(num(l.qty))),
      perPcsYuan: num(l.perPcsYuan),
      // Normalised [x0,y0,x1,y1] 0..1 of the product thumbnail on the invoice, if any.
      photoBox: (Array.isArray(l.photoBox) && l.photoBox.length === 4 &&
                 l.photoBox.every(n => typeof n === 'number' && n >= 0 && n <= 1)) ? l.photoBox : null
    }));
    res.json({
      success: true,
      vendor: String(parsed.vendor || '').toUpperCase().trim(),
      billNo: String(parsed.billNo || '').trim(),
      datePurchase: String(parsed.datePurchase || '').trim(),
      // The frontend crops photoBoxes from THIS image (canvas), so tell it whether cropping is possible.
      canCropPhotos: !isPdf,
      lines
    });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
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
    // Vendor is mandatory — it identifies who the goods were bought from and
    // is shown on every advance / receive / post line.
    if (!String(b.vendor || '').trim()) return res.status(400).json({ success: false, error: 'Pick or type a vendor name before saving the PO.' });
    // Photo is mandatory per SKU — it is what the AI image module will judge.
    const missingPhoto = (b.lines || []).filter(l => !(l.photoUrl || '').trim()).length;
    if (missingPhoto) return res.status(400).json({ success: false, error: 'Every line needs a product photo (' + missingPhoto + ' missing).' });
    // Run the preview engine to assign SKUs + classify NEW vs EXISTING (weight
    // is 0 at this stage, so any landed figure is provisional and unused here).
    const origin = b.origin === 'india' ? 'india' : 'china';
    const transportTotal = origin === 'india' ? num(b.transportTotal) : 0;
    const preview = await computePreview(s, { lines: b.lines, vendor: b.vendor, exRate: b.exRate, origin, transportTotal });
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
      // Origin of the whole PO: 'china' (¥ × exRate + weight×freight) or 'india'
      // (₹ unit cost + flat transport share, no exchange rate / per-kg freight).
      origin,
      transportTotal,                  // ₹ total transport for the shipment (india only)
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
    const qtys    = b.qtys || {};               // { lineIndex: actual received qty }
    po.lines = (po.lines || []).map((l, i) => {
      const w = weights[i] != null ? num(weights[i]) : num(l.weightGrams);
      // Received qty can differ from what was ordered — honour an edited value.
      const q = (qtys[i] != null && qtys[i] !== '') ? Math.max(0, Math.round(num(qtys[i]))) : num(l.qty);
      return { ...l, weightGrams: w, qty: q };
    });
    if (b.dateReceive) po.dateReceive = b.dateReceive;
    if (b.freightPerGram != null && b.freightPerGram !== '') po.freightPerGram = num(b.freightPerGram);
    if (b.exRate != null && b.exRate !== '') po.exRate = num(b.exRate);
    // India POs: the total shipment transport can be adjusted at receive time
    // (e.g. once the actual courier bill is known). Ignored for China POs.
    if (po.origin === 'india' && b.transportTotal != null && b.transportTotal !== '') po.transportTotal = num(b.transportTotal);
    // NOTE: computing landed cost is a PREVIEW only — it no longer flips the PO
    // to "received". Arrival is confirmed explicitly via /mark-received so that
    // previewing costs during the advance/lead-time stage never mis-marks a PO.
    saveStore(s);
    const preview = await computePreview(s, {
      lines: po.lines, vendor: po.vendor,
      exRate: po.exRate, freightPerGram: po.freightPerGram,
      origin: po.origin, transportTotal: po.transportTotal
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

// ── Explicitly confirm the physical goods arrived (advance → received) ──
// Kept SEPARATE from computing landed cost so previewing costs during the
// lead-time stage never mis-marks a PO as received. Reversible while not posted
// via /mark-received {undo:true}.
router.post('/api/procurement/pos/:id/mark-received', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (po.status === 'posted') return res.status(400).json({ success: false, error: 'Already posted.' });
  const b = req.body || {};
  if (b.undo) {
    // Roll back an accidental "received" (or a stale awaiting_approval) to advance.
    po.status = 'advance';
    delete po.receivedBy; delete po.receivedAt;
  } else {
    if (po.status === 'advance') po.status = 'received';
    if (b.dateReceive) po.dateReceive = b.dateReceive;
    else if (!po.dateReceive) po.dateReceive = new Date().toISOString().slice(0, 10);
    po.receivedBy = (req.user && req.user.username) || 'system';
    po.receivedAt = new Date().toISOString();
  }
  saveStore(s);
  res.json({ success: true, poId: po.id, status: po.status, dateReceive: po.dateReceive });
});

// ── Replace/attach the RAW source photo of a single PO line (pre-post) ──
// The AI studio feeds off the group's first line that has a photo, so swapping
// the photo here re-feeds the image generator on the next studio render.
router.post('/api/procurement/pos/:id/line-photo', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (po.status === 'posted') return res.status(400).json({ success: false, error: 'Already posted.' });
  const b = req.body || {};
  const i = Number(b.lineIndex);
  if (!Array.isArray(po.lines) || !(i >= 0 && i < po.lines.length)) return res.status(400).json({ success: false, error: 'Bad line index.' });
  po.lines[i].photoUrl = String(b.url || '').trim();
  saveStore(s);
  res.json({ success: true, lineIndex: i, url: po.lines[i].photoUrl });
});

// ── Edit a PO's header + drop lines (not posted) ─────────────────
// Header fields (vendor / bill / dates / lead time) can be corrected any time
// before the PO is posted. Individual lines may be removed. Remaining lines
// keep their frozen SKUs. Recomputes the expected arrival from the new inputs.
router.patch('/api/procurement/pos/:id', async (req, res) => {
  try {
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    if (po.status === 'posted') return res.status(400).json({ success: false, error: 'A posted PO can no longer be edited.' });
    const b = req.body || {};
    if (b.vendor != null)       po.vendor = String(b.vendor).toUpperCase().trim();
    if (b.billNo != null)       po.billNo = String(b.billNo).trim();
    if (b.datePurchase != null) po.datePurchase = String(b.datePurchase);
    if (b.leadTimeDays != null && b.leadTimeDays !== '') po.leadTimeDays = Math.max(0, Math.round(num(b.leadTimeDays)));
    if (b.exRate != null && b.exRate !== '')         po.exRate = num(b.exRate);
    if (b.freightPerGram != null && b.freightPerGram !== '') po.freightPerGram = num(b.freightPerGram);
    // Origin can be corrected before posting; transportTotal applies to india only.
    if (b.origin != null) po.origin = b.origin === 'india' ? 'india' : 'china';
    if (po.origin === 'india' && b.transportTotal != null && b.transportTotal !== '') po.transportTotal = num(b.transportTotal);
    else if (po.origin !== 'india') po.transportTotal = 0;
    // Recompute expected arrival = purchase date (or today) + lead time.
    if (po.leadTimeDays != null) {
      const base = po.datePurchase ? new Date(po.datePurchase) : new Date();
      if (!isNaN(base.getTime())) { base.setDate(base.getDate() + po.leadTimeDays); po.expectedReceiveDate = base.toISOString().slice(0, 10); }
    }
    // FULL line edit: caller sends the complete edited line set. Lines that keep
    // their existing SKU keep it (frozen); brand-new lines (no sku) get the next
    // serial. computePreview honours an explicit sku, so pre-filling each line's
    // sku preserves it. Weight is carried through if the line already had one.
    if (Array.isArray(b.lines)) {
      const preview = await computePreview(s, { lines: b.lines, vendor: po.vendor, exRate: po.exRate, freightPerGram: po.freightPerGram, origin: po.origin, transportTotal: po.transportTotal });
      po.lines = preview.lines.map(l => ({
        designName: l.designName, productType: l.productType, colour: l.colour,
        sizeLabel: l.sizeLabel, chinaSize: l.chinaSize, fit: l.fit, audience: l.audience,
        vendor: l.vendor || po.vendor, designCode: l.designCode, photoUrl: l.photoUrl,
        qty: l.qty, perPcsYuan: l.perPcsYuan,
        weightGrams: num(l.weightGrams),
        sku: l.sku, serialUsed: l.serialUsed || null, skuError: l.skuError || null,
        classification: l.classification
      }));
      po.seoDraft = (preview.newProducts || []).map(np => ({ key: np.key, designCode: np.designCode, colour: np.colour, productType: np.productType, seo: np.seo }));
    } else if (Array.isArray(b.removeLineIndexes) && b.removeLineIndexes.length) {
      // Legacy path: just drop selected line indexes (from the ORIGINAL ordering).
      const drop = new Set(b.removeLineIndexes.map(Number));
      po.lines = (po.lines || []).filter((_, i) => !drop.has(i));
      po.seoDraft = (po.seoDraft || []).filter(d => (po.lines || []).some(l => groupKey(l) === d.key));
    }
    if (b.vendor) { const vn = String(b.vendor).toUpperCase().trim(); if (vn && !s.vendors.includes(vn)) s.vendors.push(vn); }
    saveStore(s);
    const out = publicPo(po, req); delete out.seoDraft;
    res.json({ success: true, poId: po.id, po: out });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
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
  // India POs price on a flat transport split, not weight — so weights aren't
  // required. China POs still need a weight per line to compute per-kg freight.
  if (po.origin !== 'india') {
    const unweighed = (po.lines || []).filter(l => !(num(l.weightGrams) > 0)).length;
    if (unweighed) return res.status(400).json({ success: false, error: 'Enter weight for every line first (' + unweighed + ' missing).' });
  }
  po.status = 'awaiting_approval';
  po.approvalRequestedBy = (req.user && req.user.username) || 'system';
  po.approvalRequestedAt = new Date().toISOString();
  saveStore(s);
  res.json({ success: true, poId: po.id, status: po.status });
});

// ── AI IMAGE STUDIO (admin) ──────────────────────────────────────
// The listing-image + image-judged-SEO pipeline that sits between "received"
// and "posted": generate 4 shots per NEW product → admin approves → generate
// display name + SEO by JUDGING the approved photo → admin approves → post.

// NEW-product groups of a PO (design×colour), each with a representative raw
// photo to feed the image model. Only NEW products need images (EXISTING ones
// already have a Shopify listing we only add stock to).
async function newGroupsOf(s, po) {
  const preview = await computePreview(s, { lines: po.lines, vendor: po.vendor, exRate: po.exRate, freightPerGram: po.freightPerGram, origin: po.origin, transportTotal: po.transportTotal });
  return (preview.newProducts || []).map(np => {
    const line = (po.lines || []).find(l => groupKey(l) === np.key && (l.photoUrl || '').trim());
    return { key: np.key, colour: np.colour, productType: np.productType, designName: np.designName,
             designCode: np.designCode, audience: (line && line.audience) || 'Men',
             fit: (line && line.fit) || '', sizeLabels: np.variants.map(v => v.sizeLabel),
             photoUrl: line ? line.photoUrl : '' };
  });
}

// Read-only: the NEW-product groups of a PO so the AI studio can be prepared
// at the ADVANCE stage — during the shipping lead time, before goods arrive.
// No status change and no weights required (weights don't affect imaging/SEO).
router.get('/api/procurement/pos/:id/studio', async (req, res) => {
  try {
    if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    const preview = await computePreview(s, { lines: po.lines, vendor: po.vendor, exRate: po.exRate, freightPerGram: po.freightPerGram, origin: po.origin, transportTotal: po.transportTotal });
    res.json({ success: true, newProducts: preview.newProducts || [], po: publicPo(po, req) });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Generate the AI shots for ONE product group (or specific `types`).
router.post('/api/procurement/pos/:id/generate-images', async (req, res) => {
  try {
    if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Only an admin can generate AI images.' });
    if (!GEMINI_API_KEY) return res.status(400).json({ success: false, error: 'AI images are not enabled. Set GEMINI_API_KEY in Railway to turn it on.' });
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    if (po.status === 'posted') return res.status(400).json({ success: false, error: 'This PO is already posted.' });
    const b = req.body || {};
    if (!b.groupKey) return res.status(400).json({ success: false, error: 'groupKey required.' });
    const groups = await newGroupsOf(s, po);
    const g = groups.find(x => x.key === b.groupKey);
    if (!g) return res.status(404).json({ success: false, error: 'Product group not found on this PO.' });
    const src = readStoredPhoto(g.photoUrl);
    if (!src) return res.status(400).json({ success: false, error: 'No source photo for this product — add one first.' });
    const baseB64 = src.buf.toString('base64');
    // Optional explicit fit/length chosen in the studio (e.g. "oversized",
    // "three-quarter (3/4)") — the strongest lever for getting the cut right.
    const fitDesc = String(b.fit || '').trim();
    const context = ` The product is a ${g.colour} ${g.productType}${g.fit ? ' (' + g.fit + ' fit)' : ''} for ${g.audience}. Match this colour exactly.` +
      (fitDesc ? ` This garment is a ${fitDesc.toUpperCase()} — render it with exactly that cut and length in every shot; do not change it.` : '') +
      // Lock the garment's real proportions — Gemini otherwise lengthens 3/4 or
      // cropped pieces into full-length ones.
      ` Preserve the garment's EXACT length, hemline, proportions and silhouette exactly as shown in the reference — if it is cropped, three-quarter, calf-length or ankle-length, keep that same length; never lengthen or shorten it.` +
      // Shopify product-image rules: high-res, clean, centered, no text/watermark/border.
      // (Model shots get a 4:5 ratio via imageConfig; flat-lays keep their natural white-bg framing.)
      ` Output a high-resolution image formatted for a Shopify product listing: the subject centered and fully in frame with even margins, sharp focus, plain uncluttered background, no text, logos, watermarks, borders or UI overlays.`;
    const wantTypes = Array.isArray(b.types) && b.types.length ? b.types : AI_IMAGE_SPECS.map(s2 => s2.type);
    po.aiImages = po.aiImages || {};
    const existing = Array.isArray(po.aiImages[g.key]) ? po.aiImages[g.key] : [];
    const errors = [];
    // Optional per-product styling for the model shots only (e.g. pair a top with
    // jeans/trousers). Front/back flat-lays have no model so it's ignored there.
    const styling = String(b.styling || '').trim();
    // If the admin uploaded a REAL back-view photo for this group, the "back"
    // shot is generated from it (accurate) instead of guessed from the front.
    const backRef = (po.backRefs && po.backRefs[g.key]) ? readStoredPhoto(po.backRefs[g.key]) : null;
    const backRefB64 = backRef ? backRef.buf.toString('base64') : null;
    for (const spec of AI_IMAGE_SPECS) {
      if (wantTypes.indexOf(spec.type) < 0) continue;
      // Never fabricate a back: the back shot is only produced from a real
      // uploaded back photo. Without one we simply skip it (no error).
      if (spec.type === 'back' && !backRefB64) continue;
      try {
        const styleAdd = (styling && (spec.type === 'female' || spec.type === 'male'))
          ? ` Style the model wearing this exact garment ${styling}. Keep any paired clothing understated and choose colours that complement and flatter THIS garment tastefully, so it stays the clear hero of the photo.`
          : '';
        const useBackRef = spec.type === 'back' && backRefB64;
        const promptText = useBackRef
          ? 'Generate a clean FLAT-LAY / ghost-mannequin photo of the BACK of this exact garment, reproducing the reference image faithfully — same colour, print, graphics, cut and length; do not redesign it. Centered on a pure white background, even studio lighting, no model, no props, no text or watermark, sharp product detail. Do NOT show any inner neck label, brand tag, size tag or care label — the collar/neckline must be clean with no visible tag. Show a SINGLE garment fully in frame; no duplicated copies, no collage. The background must be pure white filling the ENTIRE frame to all four edges — absolutely no black bars, letterboxing, borders or coloured padding.' + context
          : spec.prompt + styleAdd + context;
        const out = await geminiGenerateImage(useBackRef ? backRefB64 : baseB64, useBackRef ? backRef.mime : src.mime, promptText, spec.aspect);
        const saved = savePhotoBuffer(out.buf, extForMime(out.mime));
        const idx = existing.findIndex(x => x.type === spec.type);
        const rec = { type: spec.type, label: spec.label, url: saved.url, approved: false };
        if (idx >= 0) existing[idx] = rec; else existing.push(rec);
      } catch (e) { errors.push({ type: spec.type, error: e.message }); }
    }
    // keep a stable model→front→back→studio order
    existing.sort((a, c) => AI_IMAGE_SPECS.findIndex(x => x.type === a.type) - AI_IMAGE_SPECS.findIndex(x => x.type === c.type));
    po.aiImages[g.key] = existing;
    saveStore(s);
    res.json({ success: true, groupKey: g.key, images: existing, errors });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Persist image approve/unapprove (and manual replacements) from the studio.
router.post('/api/procurement/pos/:id/images', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (po.status === 'posted') return res.status(400).json({ success: false, error: 'Already posted.' });
  const b = req.body || {};
  if (b.aiImages && typeof b.aiImages === 'object') {
    po.aiImages = po.aiImages || {};
    Object.keys(b.aiImages).forEach(k => {
      po.aiImages[k] = (b.aiImages[k] || []).map(x => ({ type: String(x.type || ''), label: String(x.label || x.type || ''), url: String(x.url || ''), approved: !!x.approved })).filter(x => x.url);
    });
    saveStore(s);
  }
  res.json({ success: true, aiImages: po.aiImages || {} });
});

// Judge the APPROVED product photo(s) with vision → display name + SEO.
router.post('/api/procurement/pos/:id/generate-seo', async (req, res) => {
  try {
    if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI SEO is not enabled. Set ANTHROPIC_API_KEY in Railway.' });
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    if (po.status === 'posted') return res.status(400).json({ success: false, error: 'Already posted.' });
    const b = req.body || {};
    if (!b.groupKey) return res.status(400).json({ success: false, error: 'groupKey required.' });
    const groups = await newGroupsOf(s, po);
    const g = groups.find(x => x.key === b.groupKey);
    if (!g) return res.status(404).json({ success: false, error: 'Product group not found.' });
    // Prefer an approved AI image; fall back to the model/studio shot, then the raw photo.
    const imgs = (po.aiImages && po.aiImages[g.key]) || [];
    const chosen = imgs.find(x => x.approved && (x.type === 'female' || x.type === 'male'))
                || imgs.find(x => x.approved) || null;
    const src = readStoredPhoto(chosen ? chosen.url : g.photoUrl);
    if (!src) return res.status(400).json({ success: false, error: 'No photo available to judge — generate/approve an image first.' });
    const sizeCodeOf = (label) => s.sizes[label] || label;
    const sizeList = (g.sizeLabels || []).map(sizeCodeOf).join(', ');
    const prompt =
`You are an expert e-commerce SEO/AEO/GEO copywriter naming and writing listing copy for an Indian premium streetwear product for the brand SANKI, based on the product PHOTO shown.\n` +
`Known facts: product type = ${g.productType || 'garment'}; colour = ${g.colour || 'as shown'}; audience = ${g.audience}; ${g.fit ? 'fit = ' + g.fit + '; ' : ''}available sizes = ${sizeList || 'as listed'}.\n` +
`Look at the actual garment in the photo (graphics, print, silhouette, vibe) and write copy that fits WHAT YOU SEE.\n\n` +
`Optimise for three things at once:\n` +
`- SEO (Google): natural, keyword-rich phrasing built around real search terms a shopper types (e.g. "baggy red cargo pants men").\n` +
`- AEO (answer engines / voice): clear, factual, self-contained sentences that directly answer "what is this product?" so it can be quoted as a snippet.\n` +
`- GEO (ChatGPT/Perplexity/Gemini): state the product entity plainly — brand SANKI + product type + colour + fit + key visible detail — so generative engines can confidently cite it.\n\n` +
`Return STRICT JSON ONLY:\n` +
`{"displayName":"","title":"","metaTitle":"","metaDescription":"","imageAlt":"","tags":[""],"bodyHtml":""}\n\n` +
`Rules:\n` +
`- CRITICAL: never include any internal codes, vendor design numbers, SKUs, or bare numbers (e.g. "71383", "SA-2-11-FS") in ANY field. These are warehouse-only. Names must read like real retail product names.\n` +
`- displayName = a short, catchy customer-facing product name (2-4 words), inspired by what the garment looks like. No brand, no colour, no numbers.\n` +
`- title = storefront H1: "<displayName> <productType> — <Fit>, <Colour>" style, natural and clean.\n` +
`- metaTitle <= 60 chars, front-loads the main keyword, ends with " | SANKI".\n` +
`- metaDescription <= 155 chars: one natural sentence that states what it is (brand + colour + fit + product type), mentions premium streetwear, COD and limited drop. Written to answer a search query directly.\n` +
`- imageAlt = concise, literal description of the garment as seen (colour + key visible feature + product type).\n` +
`- tags = 5-8 short, real-world search tags (mix of head + long-tail keywords). No codes.\n` +
`- bodyHtml = 2-3 sentences of product description in simple HTML (<p>…</p>), premium streetwear tone; lead with a plain factual sentence (great for AEO/GEO) then describe what is visibly distinctive.\n` +
`- Never invent sizes/prices. No markdown, JSON only.`;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 90000);
    let r;
    try {
      r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
        body: JSON.stringify({ model: AI_MODEL, max_tokens: 1500, messages: [{ role: 'user', content: [
          { type: 'image', source: { type: 'base64', media_type: src.mime, data: src.buf.toString('base64') } },
          { type: 'text', text: prompt }
        ] }] }),
        signal: ctrl.signal
      });
    } catch (err) {
      clearTimeout(timer);
      if (err.name === 'AbortError') return res.status(504).json({ success: false, error: 'The SEO writer timed out — try again.' });
      return res.status(502).json({ success: false, error: 'Could not reach the SEO writer: ' + err.message });
    }
    clearTimeout(timer);
    const j = await r.json().catch(() => ({}));
    if (!r.ok) return res.status(502).json({ success: false, error: 'AI error: ' + ((j.error && j.error.message) || ('HTTP ' + r.status)) });
    const parsed = extractJsonBlock((j.content || []).map(c => c.text || '').join('')) || {};
    // Deterministic fields still come from our own generator (handle uniqueness etc).
    // Clean the model's suggested name of any codes BEFORE using it as a fallback,
    // so genSeo's deterministic title/meta are built on clean copy too.
    const cleanDisplay = stripInternalCodes(String(parsed.displayName || '').trim(), g.designCode);
    const base = genSeo({ designName: cleanDisplay || g.designName, designCode: g.designCode, productType: g.productType, colour: g.colour, fit: g.fit, audience: g.audience, sizeLabels: g.sizeLabels, sizeCodeOf });
    // Belt-and-braces: strip codes from every customer-facing field the model returned.
    const clean = (v, fb) => stripInternalCodes(String(v || '').trim(), g.designCode) || fb;
    const seo = {
      displayName: cleanDisplay || base.title.split('—')[0].trim(),
      title: clean(parsed.title, base.title),
      handle: base.handle,
      metaTitle: clean(parsed.metaTitle, base.metaTitle).slice(0, 70),
      metaDescription: clean(parsed.metaDescription, base.metaDescription).slice(0, 320),
      imageAlt: clean(parsed.imageAlt, base.imageAlt),
      tags: (Array.isArray(parsed.tags) && parsed.tags.length ? parsed.tags : base.tags)
        .map(t => stripInternalCodes(String(t).trim(), g.designCode)).filter(Boolean),
      bodyHtml: stripInternalCodes(String(parsed.bodyHtml || base.bodyHtml), g.designCode)
    };
    // Persist onto the PO's seoDraft (keyed by group) so it survives reloads/posts.
    po.seoDraft = Array.isArray(po.seoDraft) ? po.seoDraft : [];
    const di = po.seoDraft.findIndex(d => d.key === g.key);
    const rec = { key: g.key, designCode: g.designCode, colour: g.colour, productType: g.productType, seo, seoApproved: false };
    if (di >= 0) po.seoDraft[di] = rec; else po.seoDraft.push(rec);
    saveStore(s);
    res.json({ success: true, groupKey: g.key, seo });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Persist edited/approved SEO from the studio (before posting).
router.post('/api/procurement/pos/:id/seo', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (po.status === 'posted') return res.status(400).json({ success: false, error: 'Already posted.' });
  const b = req.body || {};
  if (!b.groupKey || !b.seo) return res.status(400).json({ success: false, error: 'groupKey and seo required.' });
  po.seoDraft = Array.isArray(po.seoDraft) ? po.seoDraft : [];
  const di = po.seoDraft.findIndex(d => d.key === b.groupKey);
  const prev = di >= 0 ? po.seoDraft[di] : { key: b.groupKey };
  const rec = Object.assign({}, prev, { key: b.groupKey, seo: Object.assign({}, prev.seo, b.seo), seoApproved: !!b.seoApproved });
  if (di >= 0) po.seoDraft[di] = rec; else po.seoDraft.push(rec);
  saveStore(s);
  res.json({ success: true, seoDraft: po.seoDraft });
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

  // Approved AI images live on the PO (keyed by product group). Attach them
  // to each new product at post time so the listing is born with photos.
  const poForImages = (b.poId && s.pos[b.poId]) ? s.pos[b.poId] : null;
  const results = { created: [], adjusted: [], errors: [] };
  // 1) Create new draft products.
  for (const np of (b.newProducts || [])) {
    try {
      if (!np.images && poForImages && poForImages.aiImages && poForImages.aiImages[np.key]) {
        np.images = poForImages.aiImages[np.key].filter(x => x.approved).map(x => ({ url: x.url, alt: np.seo && np.seo.imageAlt }));
      }
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
