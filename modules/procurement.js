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
const pdfParse = require('pdf-parse');
const { createWorker } = require('tesseract.js');
const tesseractChinese = require('@tesseract.js-data/chi_sim');
const { shopifyClient } = require('./shopify-client');
const { purchasePaymentStatus } = require('./purchase-payment-status');
const { invoiceAmounts, allocateAmount, finalizedByPo } = require('./lg-invoices');

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
const INVOICE_DIR = path.join(DATA_DIR, 'procurement-invoices');
try { fs.mkdirSync(INVOICE_DIR, { recursive: true }); } catch { /* exists */ }
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
// The invoice is held in memory and never persisted. Reading uses local
// Simplified-Chinese OCR, so this workflow does not depend on paid AI credits.
// Images and text-based PDFs are supported; the buyer reviews every extracted
// field before the purchase is saved.
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY; // optional SEO generation only
const AI_MODEL = process.env.PROCUREMENT_AI_MODEL || 'claude-sonnet-4-6';
const invoiceUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 },
  fileFilter: (req, file, cb) => cb(null, /^image\/|application\/pdf/.test(file.mimetype))
});
function persistInvoice(file) {
  const ext=(path.extname(file.originalname||'')||(/pdf/.test(file.mimetype)?'.pdf':'.jpg')).toLowerCase().replace(/[^.a-z0-9]/g,'');
  const name=Date.now()+'-'+crypto.randomBytes(6).toString('hex')+(ext||'.bin');
  fs.writeFileSync(path.join(INVOICE_DIR,name),file.buffer);
  return { file:name, url:'/api/procurement/invoice/'+name, originalName:path.basename(file.originalname||'Vendor bill'), mime:file.mimetype||'', uploadedAt:new Date().toISOString() };
}

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
    'Waist 24': '24', 'Waist 26': '26', 'Waist 28': '28', 'Waist 30': '30', 'Waist 32': '32', 'Waist 34': '34',
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
const SIZE_TOKENS = ['FS','XS','S','M','L','XL','XXL','3XL','4XL','5XL','24','26','28','30','32','34','36','38','40','42','44'];
// Trailing size embedded in the serial regex (longest-first) so the greedy
// number group backtracks to a VALID size token — critical for numeric waist
// sizes (…33 could otherwise split as num=…3, size='3').
const SIZE_ALT = SIZE_TOKENS.slice().sort((a, b) => b.length - a.length).join('|');
// The running serial number rolls to the next Excel-style letter at 999, so it is ALWAYS
// 1-3 digits. Bounding the number group to \d{1,3} lets it hand the extra
// leading digit to sizes like 3XL/4XL (e.g. …5633XL = serial 563 + size 3XL,
// not serial 5633 + size XL).
const SERIAL_RE = new RegExp('^SA\\d+([A-Z]+)(\\d{1,3})(' + SIZE_ALT + ')$');

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
  else s.sizes = { ...SEED.sizes, ...s.sizes };
  if (!Array.isArray(s.vendors)) s.vendors = [ ...SEED.vendors ];
  else { // normalize any older mixed-case entries to UPPERCASE + dedupe
    const seen = {}; s.vendors = s.vendors.map(v => String(v).toUpperCase().trim())
      .filter(v => v && !seen[v] && (seen[v] = 1));
  }
  if (!s.settings) s.settings = { ...SEED.settings };
  else s.settings = { ...SEED.settings, ...s.settings };
  if (!s.pos)      s.pos = {};      // { [poId]: PO }
  if (!s.combinedVendorInvoices) s.combinedVendorInvoices = {};
  if (!s.seq)      s.seq = 0;       // internal PO counter
  // Repair the old Z999 rollover bug. String.fromCharCode('Z' + 1) produced
  // '[' and reserved malformed SKUs such as SA111[134 on unposted POs.
  // The intended Excel-style sequence continues Z999 → AA1.
  let repairedInvalidSerials = false;
  Object.values(s.pos).forEach(po => {
    if (!po || po.status === 'posted' || po.status === 'posting_partial') return;
    (po.lines || []).forEach(line => {
      const serial = line && line.serialUsed;
      if (!serial || serial.alpha !== '[') return;
      serial.alpha = 'AA';
      if (typeof line.sku === 'string' && line.sku.includes('[')) {
        line.sku = line.sku.replace('[', 'AA');
      }
      if (line.ordered && typeof line.ordered.sku === 'string' && line.ordered.sku.includes('[')) {
        line.ordered.sku = line.ordered.sku.replace('[', 'AA');
      }
      repairedInvalidSerials = true;
    });
  });
  if (repairedInvalidSerials) atomicWrite(STORE_PATH, JSON.stringify(s));
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
// Current SKU format: SA<digits><ALPHA><digits><SIZE>. One or more Excel-style
// alpha letters delimit the numeric prefix from the running serial number.
function parseSerial(sku) {
  const m = String(sku || '').toUpperCase().match(SERIAL_RE);
  if (!m) return null;
  return { alpha: m[1], num: parseInt(m[2], 10) };
}
// Keep the article's running serial, but rebuild all classification-derived
// parts whenever product type, colour or size changes.
function rebuildLineSku(store, line, previousSku) {
  const serial = line.serialUsed || parseSerial(previousSku || line.sku);
  if (!serial) return { sku: String(line.sku || '').toUpperCase().trim(), serialUsed: null, error: 'Could not retain the SKU serial.' };
  const built = buildSku(store, line.productType, line.colour, line.sizeLabel, serial);
  return { sku: built.sku || '', serialUsed: { ...serial }, error: built.error || null };
}
// Compare two serials: alpha first (A<B…), then number.
function serialGt(a, b) {
  if (!b) return true;
  if (a.alpha !== b.alpha) {
    if (a.alpha.length !== b.alpha.length) return a.alpha.length > b.alpha.length;
    return a.alpha > b.alpha;
  }
  return a.num > b.num;
}
function nextSerial(cur) {
  // cur = { alpha, num }; roll J→K→…→Z→AA→AB using Excel-style letters.
  if (!cur) return { alpha: 'J', num: 1 };
  if (cur.num < 999) return { alpha: cur.alpha, num: cur.num + 1 };
  const chars = String(cur.alpha || 'J').toUpperCase().split('');
  let i = chars.length - 1;
  while (i >= 0 && chars[i] === 'Z') { chars[i] = 'A'; i--; }
  if (i < 0) chars.unshift('A');
  else chars[i] = String.fromCharCode(chars[i].charCodeAt(0) + 1);
  return { alpha: chars.join(''), num: 1 };
}

// ── Shopify variant catalogue (cached; used for serial + classify) ──
let _catalogue = null;      // { skuMap: {SKU: {productId, variantId, inventoryItemId}}, maxSerial, fetchedAt }
let _shopifyPurchaseHistory = null;
async function loadCatalogue(force) {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) throw new Error('Shopify env not configured');
  if (_catalogue && !force && (Date.now() - _catalogue.fetchedAt) < 5 * 60 * 1000) return _catalogue;
  let url = `https://${SHOPIFY_STORE}/admin/api/${API}/products.json?limit=250&fields=id,variants`;
  const skuMap = {}; let maxSerial = null;
  while (url) {
    const r = await shopifyClient.request(url);
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

// Recover the purchase-era history that survived in Shopify even when the
// corresponding old procurement.json records did not. These rows remain
// separate from real POs: Shopify can verify product/date/SKU, but not the
// original bill, quantity or landed cost.
async function loadShopifyPurchaseHistory(force) {
  if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) return [];
  if (_shopifyPurchaseHistory && !force && (Date.now() - _shopifyPurchaseHistory.fetchedAt) < 5 * 60 * 1000) {
    return _shopifyPurchaseHistory.rows;
  }
  let url = `https://${SHOPIFY_STORE}/admin/api/${API}/products.json?limit=250&created_at_min=2026-07-19T00:00:00%2B05:30&fields=id,title,created_at,vendor,product_type,status,variants`;
  const products = [];
  while (url) {
    const r = await shopifyClient.request(url);
    if (!r.ok) { const b = await r.text().catch(() => ''); throw new Error('Shopify ' + r.status + ': ' + b.slice(0, 200)); }
    const d = await r.json();
    (d.products || []).forEach(p => {
      // Historical SKUs include formats that pre-date today's strict parser;
      // requiring parseSerial() here would silently erase legitimate old buys.
      const skus = (p.variants || []).map(v => String(v.sku || '').toUpperCase()).filter(Boolean);
      products.push({
        productId: String(p.id), title: p.title || '(untitled)', type: p.product_type || '',
        vendor: p.vendor || '', status: p.status || '', createdAt: p.created_at || '', skus
      });
    });
    const link = r.headers.get('Link') || '';
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  const byDate = {};
  products.forEach(p => {
    const date = String(p.createdAt).slice(0, 10);
    if (date) (byDate[date] || (byDate[date] = [])).push(p);
  });
  const rows = Object.keys(byDate).sort().reverse().map(date => ({
    id: 'HIST-' + date.replace(/-/g, ''), historical: true, source: 'shopify-recovery',
    status: 'posted', datePurchase: date, createdAt: date + 'T00:00:00.000Z',
    vendor: 'Recovered from Shopify', billNo: '', products: byDate[date],
    productCount: byDate[date].length,
    skuCount: byDate[date].reduce((n, p) => n + p.skus.length, 0),
    quantityKnown: false, valueKnown: false
  }));
  _shopifyPurchaseHistory = { rows, fetchedAt: Date.now() };
  return rows;
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
function poCostBreakdown(po, defaults) {
  defaults = defaults || {};
  const india = po.origin === 'india';
  const exRate = num(po.exRate != null ? po.exRate : defaults.exRate);
  const freightPerGram = num(po.freightPerGram != null ? po.freightPerGram : defaults.freightPerGram);
  const totalQty = (po.lines || []).reduce((n, l) => n + num(l.qty), 0);
  const transportTotal = india ? num(po.transportTotal) : 0;
  const transportPerPc = india && totalQty ? transportTotal / totalQty : 0;
  const lines = (po.lines || []).map((line, index) => {
    const qty = num(line.qty), unitPrice = num(line.perPcsYuan), weightGrams = num(line.weightGrams);
    const goodsPerPc = india ? unitPrice : unitPrice * exRate;
    const freightPerPc = india ? transportPerPc : weightGrams * freightPerGram;
    const landedPerPc = goodsPerPc + freightPerPc;
    return { index, sku: line.sku || '', designName: line.designName || '', qty,
      unitPrice: round2(unitPrice), weightGrams: round2(weightGrams), goodsPerPc: round2(goodsPerPc),
      freightPerPc: round2(freightPerPc), landedPerPc: round2(landedPerPc), lineTotal: round2(landedPerPc * qty) };
  });
  return { origin: india ? 'india' : 'china', exRate: round2(exRate), freightPerGram: round2(freightPerGram),
    transportTotal: round2(transportTotal), totalQty: round2(totalQty),
    goodsTotal: round2(lines.reduce((n, l) => n + l.goodsPerPc * l.qty, 0)),
    freightTotal: round2(lines.reduce((n, l) => n + l.freightPerPc * l.qty, 0)),
    landedTotal: round2(lines.reduce((n, l) => n + l.lineTotal, 0)), lines,
    formula: india ? 'Landed/pc = INR price/pc + (total transport / total quantity)'
      : 'Landed/pc = (Yuan price/pc x exchange rate) + (weight g/pc x freight rate/g)' };
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
  // Vendor names often already end in the product type ("Casuals T-shirt").
  // Do not produce customer-facing names such as "T-shirt T-Shirt".
  const nameForTitle = designName.replace(new RegExp('\\s+' + String(g.productType || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '$', 'i'), '').trim();
  const audience    = g.audience || 'Men';           // 'Men' | 'Women' | 'Unisex'
  const winter = /^winter$/i.test(g.season || '') || /^(hoodie|sweatshirt|sweater|cardigan|pullover|jacket|coat)$/i.test(g.productType || '');
  const productType = audience === 'Women' && !winter && /^t[ -]?shirt$/i.test(g.productType || '')
    ? 'Top' // Basic copy cannot verify a polo collar; only photo-based AI copy may say that.
    : (g.productType || '');
  const colour      = titleCase(g.colour || '');
  const fit         = audience === 'Women' && !winter && /\bmuscle\s*fit\b/i.test(g.fit || '') ? '' : titleCase(g.fit || '');
  const fitBase     = fit.replace(/\s*fit$/i, '').trim();   // strip trailing "Fit" so we never double it
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

  return { displayName: nameForTitle, title, handle, metaTitle, metaDescription, imageAlt, tags, bodyHtml };
}

function seoNeedsReview(seo) {
  return !seo || !seo.displayName || !seo.title || !seo.metaTitle || !seo.metaDescription || !seo.imageAlt ||
    !seo.handle || !seo.bodyHtml || !Array.isArray(seo.tags) || !seo.tags.length ||
    /(t[ -]?shirt|shirt|trouser|jeans|lower|shorts)\s+\1/i.test(seo.title);
}

// A trailing SIZE token on a design name ("FY5002 Black S", "Cargo 32") — the
// vendor sheet often appends the size to the per-row design name, which would
// otherwise make each size read as a different product. Strip ONE trailing
// size-looking token so all sizes of a colourway share the same product name.
const SIZE_SUFFIX_RE = /[\s,|-]+(?:XXXXL|XXXL|XXL|XXS|XS|[2-6]XL|[SML]|XL|\d{2})\s*$/i;
function stripSizeSuffix(name) {
  let s = String(name || '').trim();
  const stripped = s.replace(SIZE_SUFFIX_RE, '').trim();
  // Only strip if something meaningful is left (don't reduce "S" → "").
  return stripped || s;
}

// Product LINE tag for a PO: SANKI Funky vs SANKI Casuals (two separate ranges).
// Anything unrecognised → '' (Unclassified), so old/untagged POs stay neutral
// until bulk-tagged. Only these two values are ever stored.
function normLine(v) {
  const x = String(v || '').toLowerCase().trim();
  return (x === 'funky' || x === 'casuals') ? x : '';
}

// ── Post-order correction audit trail ───────────────────────────────
// When goods arrive the actual product can differ from what was ordered
// (wrong colour, short/over qty, mis-classified size…). We freeze each
// line's ORDERED values once, at PO creation, in `l.ordered`. Any later
// edit that makes a tracked field differ from `l.ordered` is a discrepancy
// the UI highlights — so a bill audited months later still shows what was
// corrected. The baseline is written ONCE and never overwritten.
const ORDERED_FIELDS = ['qty', 'colour', 'productType', 'sizeLabel', 'chinaSize', 'designName', 'designCode', 'sku', 'fit', 'audience'];
function orderedSnapshot(l) {
  const o = {};
  ORDERED_FIELDS.forEach(k => { o[k] = l[k] == null ? '' : l[k]; });
  return o;
}

// ── Grouping: intake lines → products (one product per design×colour) ──
// A "group key" identifies one product whose sizes become the variants. The
// DESIGN CODE (sheet's "CODE AS PER PRODUCT") is the primary identifier; if
// absent we fall back to the size-stripped design name. Grouping is ONLY by
// design × colour — a single colourway is ONE product for photos, regardless
// of which sizes it spans. Fit / product-type / audience are NOT part of the
// key: they vary per size-row in real vendor data (S/M/L tagged "regular",
// XL "oversized", XXL blank) and would wrongly split one tee into many. Those
// fields are reconciled to a representative value per group in computePreview.
function groupKey(l) {
  const design = ((l.designCode || '').trim() || stripSizeSuffix(l.designName || '')).toLowerCase();
  return [design, l.colour || '']
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
    audience:    (raw.audience || '').trim(),
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
    let serialUsed = raw.serialUsed || null, skuError = raw.skuError || null;
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
    if (!groups[k]) groups[k] = { key: k, lines: [], productType: '', colour: l.colour, designName: '', fit: '', audience: '', vendor: '', designCode: '' };
    const g = groups[k];
    g.lines.push(l);
    // Reconcile the group's descriptive fields to the FIRST non-empty value across
    // its size-rows, so a blank field on one size (e.g. XXL with no product type)
    // doesn't degrade the shared product's SEO. The design name is size-stripped.
    if (!g.productType && l.productType) g.productType = l.productType;
    if (!g.fit && l.fit) g.fit = l.fit;
    if (!g.audience && l.audience) g.audience = l.audience;
    if (!g.vendor && l.vendor) g.vendor = l.vendor;
    if (!g.designCode && l.designCode) g.designCode = l.designCode;
    if (!g.designName && l.designName) g.designName = stripSizeSuffix(l.designName);
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
    const bySize = new Map();
    g.lines.forEach(l => {
      const size = String(sizeCodeOf(l.sizeLabel) || '').trim().toUpperCase();
      if (!bySize.has(size)) bySize.set(size, []);
      bySize.get(size).push(l.sku);
    });
    const variantConflicts = [...bySize].filter(([, skus]) => skus.length > 1)
      .map(([size, skus]) => ({ size, skus }));
    return {
      key: g.key, vendor: g.vendor, designCode: g.designCode, designName: g.designName,
      colour: g.colour, productType: g.productType, ambiguous, variantConflicts,
      photoUrl: repLine ? repLine.photoUrl : '',
      seo,
      variants: g.lines.map(l => ({
        sku: l.sku, sizeLabel: l.sizeLabel, sizeCode: sizeCodeOf(l.sizeLabel), chinaSize: l.chinaSize,
        qty: l.qty, weightGrams:l.weightGrams, landed: l.landed, price: l.suggestedMrp, skuError: l.skuError
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
  const r = await shopifyClient.request(`https://${SHOPIFY_STORE}/admin/api/${API}/${pathUrl}`, {
    method: 'POST',
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

// ── Role helpers ─────────────────────────────────────────────────
// Owner and authorised procurement users retain the complete historical
// workflow: intake → AI photos/SEO → final preview → Shopify draft posting.
function isAdmin(req) { return !!(req.user && req.user.role === 'admin'); }
function canManagePurchases(req) {
  const roles = (req.user && (Array.isArray(req.user.roles) && req.user.roles.length
    ? req.user.roles : (req.user.role ? [req.user.role] : []))) || [];
  return roles.map(r => String(r).toLowerCase()).some(r =>
    r === 'admin' || r === 'owner' || r === 'procurement' || r === 'inventory');
}
function canReconcileVendorBill(req) {
  const roles = (req.user && (Array.isArray(req.user.roles) ? req.user.roles : []).concat([req.user.role])) || [];
  return canManagePurchases(req) || roles.some(role => String(role).toLowerCase() === 'accounting');
}
function isLockedPo(po) { return po.status === 'posted' || po.status === 'posting_partial'; }
function publicPo(po, req) {
  if (canManagePurchases(req)) return po;
  const clone = JSON.parse(JSON.stringify(po));
  delete clone.seoDraft;                                   // hide SEO drafts
  (clone.newProducts || []).forEach(np => { delete np.seo; });
  return clone;
}
function stripPreviewForRole(preview, req) {
  if (canManagePurchases(req)) return preview;
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
router.get('/api/procurement/invoice/:file', (req, res) => {
  const name=path.basename(String(req.params.file||'')),fp=path.join(INVOICE_DIR,name);
  if(!fp.startsWith(INVOICE_DIR)||!fs.existsSync(fp))return res.status(404).end();
  res.sendFile(fp);
});
router.post('/api/procurement/pos/:id/invoice', invoiceUpload.single('invoice'), (req,res) => {
  const role=String(req.user&&req.user.role||'').toLowerCase();
  if(!req.file)return res.status(400).json({success:false,error:'Choose the original vendor bill.'});
  const s=loadStore(),po=s.pos[req.params.id];if(!po)return res.status(404).json({success:false,error:'PO not found.'});
  if (po.status === 'posted' ? (!isAdmin(req) && role !== 'owner') : !canReconcileVendorBill(req))
    return res.status(403).json({success:false,error:'Purchases access is required; only the Owner can replace a posted bill.'});
  const invoice=persistInvoice(req.file);invoice.uploadedBy=(req.user&&req.user.username)||'owner';po.invoice=invoice;
  po.invoiceHistory=Array.isArray(po.invoiceHistory)?po.invoiceHistory:[];po.invoiceHistory.push({...invoice,reason:'Original bill attached to PO'});saveStore(s);
  res.json({success:true,invoice,po:publicPo(po,req)});
});
// Disk reclaim: every AI-image generation writes a NEW random-named file and
// never deletes the version it replaced, so regenerated images pile up on the
// /data volume as orphans no PO references. Collect every photo filename still
// referenced across all POs (aiImages urls, backRefs, line photoUrls) and
// report/delete the rest. Dry-run by default; ?apply=1 actually deletes.
// Admin-only — it touches the shared volume.
function collectReferencedPhotos(s) {
  const keep = new Set();
  const add = url => { const n = path.basename(String(url || '')); if (n) keep.add(n); };
  Object.values(s.pos || {}).forEach(po => {
    Object.values(po.aiImages || {}).forEach(arr => (arr || []).forEach(x => add(x && x.url)));
    Object.values(po.qaRejected || {}).forEach(arr => (arr || []).forEach(x => add(x && x.url)));
    Object.values(po.backRefs || {}).forEach(add);
    (po.lines || []).forEach(l => add(l && l.photoUrl));
  });
  return keep;
}
function sweepOrphanPhotos(apply) {
  const s = loadStore();
  const keep = collectReferencedPhotos(s);
  let files = []; try { files = fs.readdirSync(PHOTO_DIR); } catch {}
  let orphanBytes = 0, orphanCount = 0, keptCount = 0, removed = 0, freed = 0;
  files.forEach(fn => {
    const fp = path.join(PHOTO_DIR, fn);
    let size = 0; try { const st = fs.statSync(fp); if (!st.isFile()) return; size = st.size; } catch { return; }
    if (keep.has(fn)) { keptCount++; return; }
    orphanCount++; orphanBytes += size;
    if (apply) { try { fs.unlinkSync(fp); removed++; freed += size; } catch {} }
  });
  return { applied: !!apply, totalFiles: files.length,
    referenced: keep.size, keptOnDisk: keptCount,
    orphanCount, orphanMB: +(orphanBytes / 1048576).toFixed(2),
    removed, freedMB: +(freed / 1048576).toFixed(2) };
}
router.post('/api/procurement/photos/sweep', (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ success: false, error: 'Admin only.' });
  const apply = String((req.query && req.query.apply) || '') === '1';
  res.json({ success: true, ...sweepOrphanPhotos(apply) });
});
// Run once at boot so regenerated-image orphans can't silently fill the /data
// volume across deploys (mirrors the casuals candidate sweep). Also reaps any
// stale atomic-write .tmp-* files left behind by a crash mid-write.
try {
  const r = sweepOrphanPhotos(true);
  if (r.removed) console.log('[procurement] swept ' + r.removed + ' orphan photo(s), freed ' + r.freedMB + ' MB');
} catch {}
try {
  let reaped = 0;
  for (const fn of fs.readdirSync(DATA_DIR)) {
    if (!/\.tmp-\d+-\d+$/.test(fn)) continue;
    const fp = path.join(DATA_DIR, fn);
    try { const st = fs.statSync(fp); if (st.isFile() && Date.now() - st.mtimeMs > 3600000) { fs.unlinkSync(fp); reaped++; } } catch {}
  }
  if (reaped) console.log('[procurement] reaped ' + reaped + ' stale .tmp write file(s)');
} catch {}
// Attach (or clear) a real BACK-view reference photo for a product group so the
// "Product back" shot is generated from the true reverse instead of guessing.
// The image itself is uploaded via /api/procurement/photo first; we just store
// its URL against the group on the PO.
router.post('/api/procurement/pos/:id/back-ref', (req, res) => {
  if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  const b = req.body || {};
  if (!b.groupKey) return res.status(400).json({ success: false, error: 'groupKey required.' });
  if (isLockedPo(po)) return res.status(409).json({ success: false, error: 'Posted purchases cannot change image references.' });
  if (!(po.lines||[]).some(line=>groupKey(line)===b.groupKey)) return res.status(404).json({ success:false,error:'Product group not found.' });
  if (b.url && (!String(b.url).startsWith('/api/procurement/photo/') || !readStoredPhoto(b.url))) return res.status(400).json({ success:false,error:'Upload a readable back photo first.' });
  po.backRefs = po.backRefs || {};
  if (po.backRefs[b.groupKey] !== (b.url || '')) {
    // A draft created from a different back reference must not remain approvable.
    po.aiImages=po.aiImages||{};
    po.aiImages[b.groupKey]=(po.aiImages[b.groupKey]||[]).filter(image=>image.type!=='back');
  }
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
const sleep = (ms) => new Promise(res => setTimeout(res, ms));
// Gemini's image model returns 503 ("overloaded / high demand") and 429 (rate)
// intermittently — they are transient, so a short exponential backoff usually
// clears them without the user seeing a thing. 500 is likewise retried.
const IMG_RETRY_STATUS = new Set([429, 500, 503]);
const IMG_MAX_ATTEMPTS = 4;

// One raw attempt at Gemini image generation. Attaches `.status` on HTTP errors
// so the caller can decide whether to retry.
async function geminiImageAttempt(baseB64, baseMime, prompt, aspect) {
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
  if (!r.ok) {
    const e = new Error('Gemini ' + r.status + ': ' + (((j.error && j.error.message) || '').slice(0, 200) || 'image error'));
    e.status = r.status;
    throw e;
  }
  const parts = ((((j.candidates || [])[0] || {}).content) || {}).parts || [];
  const img = parts.find(p => p.inlineData || p.inline_data);
  const blob = img && (img.inlineData || img.inline_data);
  const data = blob && blob.data;
  if (!data) throw new Error('The model returned no image (try again or use a clearer source photo).');
  const mime = (blob.mimeType || blob.mime_type || 'image/png').toLowerCase();
  return { buf: Buffer.from(data, 'base64'), mime };
}

// Call Gemini image generation with automatic backoff on transient overload
// (503/429/500). Waits ~2s, 4s, 8s between attempts so a demand spike recovers
// silently instead of surfacing an error to the user.
async function geminiGenerateImage(baseB64, baseMime, prompt, aspect) {
  let lastErr;
  for (let attempt = 1; attempt <= IMG_MAX_ATTEMPTS; attempt++) {
    try {
      return await geminiImageAttempt(baseB64, baseMime, prompt, aspect);
    } catch (err) {
      lastErr = err;
      const retryable = IMG_RETRY_STATUS.has(err.status);
      if (!retryable || attempt === IMG_MAX_ATTEMPTS) throw err;
      await sleep(1000 * Math.pow(2, attempt)); // 2s, 4s, 8s
    }
  }
  throw lastErr;
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
let invoiceOcrWorkerPromise;
let invoiceOcrQueue = Promise.resolve();
function localInvoiceOcr(buffer) {
  // One warm local worker; serialize jobs because a Tesseract worker cannot
  // safely run two recognitions at once. chi_sim also recognises Latin/digits.
  const job = invoiceOcrQueue.then(async () => {
    if (!invoiceOcrWorkerPromise) {
      invoiceOcrWorkerPromise = createWorker(tesseractChinese.code, 1, {
        langPath: tesseractChinese.langPath, gzip: tesseractChinese.gzip, cacheMethod: 'none'
      });
    }
    const worker = await invoiceOcrWorkerPromise;
    const result = await worker.recognize(buffer);
    return String(result && result.data && result.data.text || '');
  });
  invoiceOcrQueue = job.catch(() => {});
  return job;
}
function normalInvoiceText(value) {
  return String(value || '').normalize('NFKC').replace(/\r/g, '').replace(/[，]/g, ',').replace(/[：]/g, ':');
}
function localInvoiceDate(text) {
  const raw = normalInvoiceText(text);
  const m = raw.match(/(?:20\d{2})[年\/.-]\s*\d{1,2}[月\/.-]\s*\d{1,2}日?/) ||
            raw.match(/\d{1,2}[\/.-]\s*\d{1,2}[\/.-]\s*(?:20)?\d{2}/);
  if (!m) return '';
  const nums = m[0].match(/\d+/g).map(Number);
  let y, month, day;
  if (nums[0] > 1900) [y, month, day] = nums;
  else { [day, month, y] = nums; if (y < 100) y += 2000; }
  if (month < 1 || month > 12 || day < 1 || day > 31) return '';
  return String(y).padStart(4, '0') + '-' + String(month).padStart(2, '0') + '-' + String(day).padStart(2, '0');
}
function localInvoiceBillNo(text) {
  const m = normalInvoiceText(text).match(/(?:invoice|bill|order|单据|单号|订单|票据)\s*(?:no\.?|number|编号|号码|#)?\s*[:#-]?\s*([A-Z0-9][A-Z0-9_\/-]{2,})/i);
  return m ? m[1].replace(/[.,;:]+$/, '') : '';
}
function localInvoiceProduct(line, products) {
  const rules = [
    ['Denim Joggers', /denim\s*jogger|牛仔束脚/i], ['Coord Set', /coord|co-ord|套装/i],
    ['T-Shirt', /t[\s-]?shirt|tee\b|polo|T恤|短袖/i], ['Shirt', /\bshirt\b|衬衫/i],
    ['Jeans', /\bjeans?\b|牛仔裤/i], ['Trouser', /trouser|pants?|长裤|裤子|西裤|阔腿裤/i],
    ['Jogger', /jogger|束脚裤/i], ['Shorts', /shorts?|短裤/i], ['Jorts', /jorts?/i],
    ['Sando', /sando|背心/i], ['Lower', /lower/i], ['Bag', /\bbag\b|包/i]
  ];
  const hit = rules.find(r => r[1].test(line) && products.includes(r[0]));
  return hit ? hit[0] : '';
}
function localInvoiceColour(line, colours) {
  const rules = [
    ['Sky Blue', /sky\s*blue|天蓝/i], ['Blue', /navy|blue|蓝|藏青/i], ['Black', /black|黑/i],
    ['White', /white|白/i], ['Brown', /brown|coffee|咖啡|棕|褐/i], ['Cream', /cream|off[ -]?white|米白|奶油/i],
    ['Green', /green|绿/i], ['Grey', /gr[ae]y|灰/i], ['Maroon', /maroon|酒红/i], ['Orange', /orange|橙|桔/i],
    ['Pink', /pink|粉/i], ['Purple', /purple|紫/i], ['Red', /red|红/i], ['Yellow', /yellow|黄/i],
    ['Beige', /beige|杏|米色/i], ['Olive', /olive|军绿/i], ['Khaki', /khaki|卡其/i],
    ['Golden', /gold(?:en)?|金色/i], ['Silver', /silver|银色/i]
  ];
  const hit = rules.find(r => r[1].test(line) && colours.includes(r[0]));
  return hit ? hit[0] : '';
}
function localInvoiceFit(line, fits) {
  const rules = [
    ['Wide Leg', /wide\s*leg|阔腿/i], ['Oversized', /oversiz|超大/i], ['Relaxed Fit', /relaxed|宽松/i],
    ['Slim Fit', /slim|修身/i], ['Skinny Fit', /skinny|紧身/i], ['Straight Fit', /straight|直筒/i],
    ['Baggy Fit', /baggy/i], ['Tapered Fit', /tapered|锥形/i], ['Bootcut', /bootcut|喇叭/i],
    ['Cargo Fit', /cargo|工装/i], ['Drop Shoulder', /drop\s*shoulder|落肩/i], ['Boxy Fit', /boxy/i],
    ['Regular Fit', /regular/i], ['Muscle Fit', /muscle/i], ['Narrow Fit', /narrow/i]
  ];
  const hit = rules.find(r => r[1].test(line) && fits.includes(r[0]));
  return hit ? hit[0] : '';
}
function localInvoiceSize(line, sizes) {
  const matches = normalInvoiceText(line).toUpperCase().match(/(?:^|[^A-Z0-9])(FS|4XL|3XL|XXL|XL|L|M|S|(?:2[468]|3[02468]|4[024]))(?:[^A-Z0-9]|$)/g) || [];
  for (const match of matches) {
    const size = match.replace(/[^A-Z0-9]/g, '');
    if (sizes.includes(size)) return size;
  }
  return '';
}
function localInvoiceNumbers(line, designCode) {
  const clean = normalInvoiceText(line)
    .replace(/(?:20\d{2})[年\/.-]\s*\d{1,2}[月\/.-]\s*\d{1,2}日?/g, ' ')
    .replace(/\b\d{7,}\b/g, ' ');
  const values = [];
  for (const m of clean.matchAll(/(?:^|[^A-Z0-9])(?:¥|￥|RMB|CNY)?\s*(\d+(?:\.\d+)?)(?=$|[^A-Z0-9])/gi)) {
    if (designCode && m[1] === designCode) continue;
    values.push(Number(m[1]));
  }
  if (/^\s*\d{1,3}[.)、]\s/.test(clean) && values.length > 2) values.shift();
  let qty = 0, price = 0;
  for (let i = values.length - 3; i >= 0; i--) {
    const q = values[i], p = values[i + 1], total = values[i + 2];
    if (Number.isInteger(q) && q > 0 && q <= 10000 && p > 0 && Math.abs(q * p - total) <= Math.max(2, total * 0.03)) {
      qty = q; price = p; break;
    }
  }
  if (!qty && values.length >= 2) {
    const q = values[values.length - 2], p = values[values.length - 1];
    if (Number.isInteger(q) && q > 0 && q <= 10000 && p > 0) { qty = q; price = p; }
  }
  return { qty, price };
}
function parseLocalInvoiceText(rawText, store) {
  const text = normalInvoiceText(rawText);
  const products = Object.keys(store.products || {}), colours = Object.keys(store.colours || {});
  const sizes = ['FS', 'M', 'L', 'XL', 'XXL', '3XL', '4XL', '24', '26', '28', '30', '32', '34', '36', '38', '40', '42', '44'];
  const fits = ['Oversized', 'Drop Shoulder', 'Boxy Fit', 'Relaxed Fit', 'Regular Fit', 'Slim Fit', 'Muscle Fit',
                'Baggy Fit', 'Straight Fit', 'Tapered Fit', 'Skinny Fit', 'Narrow Fit', 'Wide Leg', 'Bootcut', 'Cargo Fit'];
  const knownVendor = (store.vendors || []).find(v => text.toLowerCase().includes(String(v).toLowerCase()));
  const textLines = text.split('\n').map(x => x.replace(/\s+/g, ' ').trim()).filter(Boolean);
  const vendorLine = textLines.slice(0, 12).find(x => /公司|商行|服饰|服装|档口|供应商|supplier|vendor/i.test(x));
  const lines = [];
  textLines.forEach((line, index) => {
    if (/合计|总计|小计|运费|税额|折扣|应付|实付|收款|电话|地址|日期|单号|订单号|subtotal|grand\s*total|freight|discount|tax/i.test(line)) return;
    const tokens = line.match(/[A-Z]*\d[A-Z0-9_-]{2,}/gi) || [];
    let designCode = tokens.find(x => /[A-Z]/i.test(x) && /\d/.test(x)) || '';
    // OCR can glue the printed row number to an alphanumeric style code
    // ("1 A611" → "1A611"). Separate that harmlessly.
    if (/^\d{1,3}[A-Z]\d/i.test(designCode) && line.trim().startsWith(designCode)) {
      designCode = designCode.replace(/^\d{1,3}(?=[A-Z]\d)/i, '');
    }
    if (!designCode) {
      const numericCode = line.match(/(?:^|\s)(\d{4,8})(?=\s|$)/);
      if (numericCode) designCode = numericCode[1];
    }
    const productType = localInvoiceProduct(line, products);
    const colour = localInvoiceColour(line, colours);
    const sizeLabel = localInvoiceSize(line, sizes);
    const amounts = localInvoiceNumbers(line, designCode);
    if (!amounts.qty || (!productType && !colour && !sizeLabel && !designCode)) return;
    const fit = localInvoiceFit(line, fits);
    const sourceName = line.replace(/[¥￥]/g, ' ').replace(/\b\d+(?:\.\d+)?\b/g, ' ').replace(/\s+/g, ' ').trim();
    const baseName = [productType, colour].filter(Boolean).join(' ');
    lines.push({
      designName: (baseName || sourceName || ('Invoice item ' + (index + 1))).slice(0, 80),
      designCode: String(designCode).slice(0, 40), productType, colour, fit,
      sizeLabel, chinaSize: sizeLabel, audience: 'Men', qty: amounts.qty,
      perPcsYuan: amounts.price, photoBox: null
    });
  });
  const fallbackVendor = textLines.slice(0, 8).find(x =>
    !/(?:invoice|bill|order|单据|单号|订单|票据|date|日期|电话|phone)/i.test(x) &&
    /[A-Z\u3400-\u9fff]/i.test(x) && !/\d{4,}/.test(x)
  );
  return {
    vendor: String(knownVendor || vendorLine || fallbackVendor || '').replace(/^(?:供应商|vendor|supplier)\s*[:：-]?\s*/i, '').toUpperCase().trim().slice(0, 100),
    billNo: localInvoiceBillNo(text), datePurchase: localInvoiceDate(text), lines
  };
}
router.post('/api/procurement/parse-invoice', invoiceUpload.single('invoice'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: 'No invoice received — attach a photo or PDF of the vendor invoice.' });
    const s = loadStore(), invoice = persistInvoice(req.file);
    const isPdf = /pdf/.test(req.file.mimetype);
    const text = isPdf ? String((await pdfParse(req.file.buffer)).text || '') : await localInvoiceOcr(req.file.buffer);
    if (!text.trim()) return res.status(422).json({ success: false, error: isPdf
      ? 'This PDF has no readable text. Upload a clear JPG/PNG photo of each page instead.'
      : 'No readable invoice text was found. Retake the photo straight-on in good light and try again.' });
    const parsed = parseLocalInvoiceText(text, s);
    if (!parsed.lines.length) return res.status(422).json({ success: false,
      error: 'The invoice text was read, but no complete quantity-and-price rows were found. Use a clearer straight-on photo, or add the lines manually.' });
    res.json({
      success: true,
      vendor: String(parsed.vendor || '').toUpperCase().trim(),
      billNo: String(parsed.billNo || '').trim(),
      datePurchase: String(parsed.datePurchase || '').trim(),
      canCropPhotos: false,
      reader: 'local-ocr',
      invoice,
      lines: parsed.lines
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
      classification: l.classification,                // NEW = create; EXISTING = restock
      ordered: orderedSnapshot(l)                       // frozen baseline for the discrepancy audit trail
    }));
    s.seq += 1;
    const poId = 'PO-' + String(s.seq).padStart(4, '0');
    s.pos[poId] = {
      id: poId,
      status: 'advance',               // advance → received → posted
      // Origin of the whole PO: 'china' (¥ × exRate + weight×freight) or 'india'
      // (₹ unit cost + flat transport share, no exchange rate / per-kg freight).
      origin,
      transportTotal,                  // ₹ total transport for the shipment (india only)
      createdAt: new Date().toISOString(),
      createdBy: (req.user && req.user.username) || 'system',
      vendor: String(b.vendor || '').toUpperCase().trim(),
      line: normLine(b.line),          // 'funky' | 'casuals' | '' (unclassified)
      // Exact bridge back to Fresh Procurement. This avoids fuzzy matching on
      // batch names, fit words or product titles when calculating open-to-buy.
      sourceBatchId: normLine(b.line) ? String(b.sourceBatchId || '').trim().slice(0, 80) : '',
      sourceBatchName: normLine(b.line) ? String(b.sourceBatchName || '').trim().slice(0, 120) : '',
      billNo: b.billNo || '',
      invoice: b.invoice && b.invoice.url ? b.invoice : null,
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
router.patch('/api/procurement/pos/:id/weights', (req, res) => {
  if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Only authorised Purchases users can save weights.' });
  const s = loadStore(), po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found.' });
  if (po.status === 'posted' || po.status === 'posting_partial') return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
  const weights = (req.body || {}).weights;
  if (!weights || typeof weights !== 'object' || Array.isArray(weights) || !Object.keys(weights).length)
    return res.status(400).json({ success: false, error: 'Enter at least one weight.' });
  for (const [index, weight] of Object.entries(weights)) {
    if (!/^(0|[1-9]\d*)$/.test(index) || !(po.lines || [])[index] ||
        (typeof weight !== 'number' && typeof weight !== 'string') || String(weight).trim() === '' || !Number.isFinite(Number(weight)) || Number(weight) <= 0)
      return res.status(400).json({ success: false, error: 'Each weight must be a positive number in grams per piece.' });
  }
  po.weightHistory = po.weightHistory || [];
  po.weightHistory.push({ at: new Date().toISOString(), by: (req.user || {}).username || '',
    changes: Object.entries(weights).map(([index, weight]) => ({ index: Number(index), before: num(po.lines[index].weightGrams), after: Number(weight) })) });
  Object.entries(weights).forEach(([index, weight]) => { po.lines[index].weightGrams = Number(weight); });
  saveStore(s);
  res.json({ success: true, po: publicPo(po, req) });
});

// Merges the per-line weights the user recorded on arrival, then generates
// SKUs + landed cost + draft SEO for approval (still no Shopify write).
router.post('/api/procurement/pos/:id/receive', async (req, res) => {
  try {
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
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
      lines: po.lines.filter(line => num(line.qty) > 0), vendor: po.vendor,
      exRate: po.exRate, freightPerGram: po.freightPerGram,
      origin: po.origin, transportTotal: po.transportTotal
    });
    // Overlay the SEO drafts saved at advance so the admin edits persist.
    if (canManagePurchases(req) && Array.isArray(po.seoDraft)) {
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
  if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
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
  if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
  const b = req.body || {};
  const i = Number(b.lineIndex);
  if (!Array.isArray(po.lines) || !(i >= 0 && i < po.lines.length)) return res.status(400).json({ success: false, error: 'Bad line index.' });
  const url = String(b.url || '').trim();
  if (!url.startsWith('/api/procurement/photo/') || !readStoredPhoto(url)) return res.status(400).json({ success: false, error: 'Upload a readable product photo first.' });
  const key = groupKey(po.lines[i]);
  if (po.lines[i].photoUrl !== url) {
    ((po.aiImages || {})[key] || []).forEach(image => { image.approved = false; });
    (po.seoDraft || []).filter(d => d.key === key).forEach(d => { d.seoApproved = false; });
  }
  po.lines[i].photoUrl = url;
  saveStore(s);
  res.json({ success: true, lineIndex: i, url: po.lines[i].photoUrl });
});

// Reconcile the physical delivery without rewriting the vendor's bill. A
// missing line remains in the PO at zero received pieces; an extra line gets
// an ordered baseline of zero and its own audit entry. Neither action writes
// to Shopify until the normal received/approved posting gate.
router.post('/api/procurement/pos/:id/receipt-missing', (req, res) => {
  if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
  const s = loadStore(), po = s.pos[req.params.id], index = Number((req.body || {}).lineIndex);
  if (!po) return res.status(404).json({ success: false, error: 'PO not found.' });
  if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
  if (!Number.isInteger(index) || index < 0 || index >= (po.lines || []).length)
    return res.status(400).json({ success: false, error: 'Choose a valid bill line.' });
  const line = po.lines[index], before = num(line.qty);
  if (!line.ordered || typeof line.ordered !== 'object') line.ordered = orderedSnapshot(line);
  line.qty = 0;
  po.receiptHistory = Array.isArray(po.receiptHistory) ? po.receiptHistory : [];
  po.receiptHistory.push({ action: 'not-received', lineIndex: index, sku: line.sku,
    before, after: 0, at: new Date().toISOString(), by: (req.user || {}).username || 'system' });
  saveStore(s);
  res.json({ success: true, po: publicPo(po, req) });
});

router.post('/api/procurement/pos/:id/receipt-add', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    const s = loadStore(), po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found.' });
    if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
    const raw = normalizeLine((req.body || {}).line || {}, { vendor: po.vendor });
    if (!raw.designName || !raw.designCode || !raw.productType || !raw.colour || !raw.sizeLabel || !raw.audience || !raw.fit ||
        !Number.isInteger(raw.qty) || raw.qty <= 0 || !Number.isFinite(raw.perPcsYuan) || raw.perPcsYuan <= 0)
      return res.status(400).json({ success: false, error: 'Complete the product, colour, size, fit, audience, positive quantity and unit cost.' });
    if (!raw.photoUrl.startsWith('/api/procurement/photo/') || !readStoredPhoto(raw.photoUrl))
      return res.status(400).json({ success: false, error: 'Upload a readable original product photo first.' });
    const preview = await computePreview(s, { lines: [raw], vendor: po.vendor, origin: po.origin,
      exRate: po.exRate, freightPerGram: po.freightPerGram, transportTotal: po.transportTotal });
    const line = preview.lines[0];
    if (line.skuError || !line.sku) return res.status(400).json({ success: false, error: line.skuError || 'Could not assign a SKU.' });
    if ((po.lines || []).some(existing => String(existing.sku || '').toUpperCase() === line.sku))
      return res.status(409).json({ success: false, error: 'This SKU is already on the PO. Correct its received quantity instead.' });
    // The vendor bill had zero of this article; the actual delivery has qty.
    line.ordered = { ...orderedSnapshot(line), qty: 0 };
    line.receiptAdded = { at: new Date().toISOString(), by: (req.user || {}).username || 'system' };
    po.lines = Array.isArray(po.lines) ? po.lines : [];
    po.lines.push(line);
    po.receiptHistory = Array.isArray(po.receiptHistory) ? po.receiptHistory : [];
    po.receiptHistory.push({ action: 'added', lineIndex: po.lines.length - 1, sku: line.sku,
      before: 0, after: line.qty, at: line.receiptAdded.at, by: line.receiptAdded.by });
    for (const np of preview.newProducts) {
      if (!(po.seoDraft || []).some(d => d.key === np.key)) {
        po.seoDraft = Array.isArray(po.seoDraft) ? po.seoDraft : [];
        po.seoDraft.push({ key: np.key, designCode: np.designCode, colour: np.colour,
          productType: np.productType, seo: np.seo, seoApproved: false, source: 'product-details' });
      }
    }
    saveStore(s);
    res.json({ success: true, po: publicPo(po, req), lineIndex: po.lines.length - 1, sku: line.sku });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// ── Inline line corrections during receiving/audit (pre-post) ────────
// The receiving grid lets staff fix the actual product that arrived —
// colour, SKU, category, size, design, qty — without reopening the whole
// advance form. Each touched line freezes its ORDERED baseline the first
// time it's edited (so pre-existing POs start tracking from now), then any
// field that ends up differing from `ordered` is a highlighted discrepancy.
// A direct SKU correction remains possible, but changing product type, colour
// or size automatically rebuilds the SKU while retaining its article serial.
const LINE_EDIT_FIELDS = ['designName', 'designCode', 'productType', 'colour', 'sizeLabel', 'chinaSize', 'fit', 'audience', 'sku'];
const MODEL_IMAGE_TYPES = new Set(['female','male','model-front','model-side','model-side-female','model-side-male']);
function retireAudienceModelImages(po, key, previousAudience, nextAudience) {
  const images = (po.aiImages || {})[key] || [];
  const rejected = (po.qaRejected || {})[key] || [];
  const retiring = images.filter(image => MODEL_IMAGE_TYPES.has(image.type));
  if (retiring.length || rejected.length) {
    po.audienceImageHistory = Array.isArray(po.audienceImageHistory) ? po.audienceImageHistory : [];
    po.audienceImageHistory.push({groupKey:key,previousAudience,nextAudience,at:new Date().toISOString(),images:retiring,rejected});
    po.aiImages[key] = images.filter(image => !MODEL_IMAGE_TYPES.has(image.type));
    if (po.qaRejected) po.qaRejected[key] = [];
  }
}
router.post('/api/procurement/pos/:id/line-edits', async (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
  const b = req.body || {};
  const edits = b.edits || {};      // { lineIndex: { field: value } }
  const qtys  = b.qtys  || {};      // { lineIndex: qty }
  const who = (req.user && req.user.username) || 'system';
  const now = new Date().toISOString();
  let touched = 0;
  const copyChanged = new Set();
  const audienceChanged = new Map();
  (po.lines || []).forEach((l, i) => {
    const e = edits[i];
    const hasQty = qtys[i] != null && qtys[i] !== '';
    if (!e && !hasQty) return;
    // Freeze the baseline once, BEFORE applying this edit, so the very edit
    // that introduces a difference is captured against the prior values.
    if (!l.ordered || typeof l.ordered !== 'object') l.ordered = orderedSnapshot(l);
    let changed = false;
    const oldGroup = groupKey(l);
    const priorAudience = l.audience;
    const oldCopy = ['designName', 'designCode', 'productType', 'colour', 'sizeLabel', 'fit', 'audience'].map(k => String(l[k] || ''));
    const priorSku = l.sku;
    const priorIdentity = [l.productType, l.colour, l.sizeLabel].map(v => String(v == null ? '' : v));
    if (e) LINE_EDIT_FIELDS.forEach(k => {
      if (e[k] == null) return;
      let v = String(e[k]).trim();
      if (k === 'sku') v = v.toUpperCase();
      if (v !== String(l[k] == null ? '' : l[k])) { l[k] = v; changed = true; }
    });
    if (hasQty) {
      const q = Math.max(0, Math.round(num(qtys[i])));
      if (q !== (num(l.qty) || 0)) { l.qty = q; changed = true; }
    }
    const nextIdentity = [l.productType, l.colour, l.sizeLabel].map(v => String(v == null ? '' : v));
    if (priorIdentity.some((v, idx) => v !== nextIdentity[idx])) {
      const rebuilt = rebuildLineSku(s, l, priorSku);
      l.sku = rebuilt.sku;
      l.serialUsed = rebuilt.serialUsed;
      l.skuError = rebuilt.error;
    }
    if (oldCopy.some((v, idx) => v !== ['designName', 'designCode', 'productType', 'colour', 'sizeLabel', 'fit', 'audience'].map(k => String(l[k] || ''))[idx])) {
      copyChanged.add(oldGroup);
      copyChanged.add(groupKey(l));
    }
    if (priorAudience !== l.audience) audienceChanged.set(oldGroup, {previous:priorAudience,next:l.audience});
    if (changed) { l.editedAt = now; l.editedBy = who; touched++; }
  });
  for (const [key, audiences] of audienceChanged) retireAudienceModelImages(po,key,audiences.previous,audiences.next);
  if(audienceChanged.size){
    try {
      const groups=await newGroupsOf(s,po);
      for(const key of audienceChanged.keys()){
        const group=groups.find(g=>g.key===key);
        if(group)for(const image of ((po.aiImages||{})[key]||[]))if(['front','back'].includes(image.type))image.sourceFingerprint=codexBatch.fingerprint(group,(po.backRefs||{})[key]);
      }
    }catch(e){return res.status(500).json({success:false,error:e.message});}
  }
  // Keep a fresh, editable SEO/AEO/GEO draft aligned with corrected product
  // facts. Corrections invalidate prior copy approval; quantity-only edits do not.
  if (copyChanged.size || (po.seoDraft || []).some(d => seoNeedsReview(d.seo))) {
    po.seoDraft = Array.isArray(po.seoDraft) ? po.seoDraft : [];
    const refreshKeys = new Set([...copyChanged, ...po.seoDraft.filter(d => seoNeedsReview(d.seo)).map(d => d.key)]);
    for (const key of refreshKeys) {
      const lines = (po.lines || []).filter(l => groupKey(l) === key);
      if (!lines.length) continue;
      const l = lines[0];
      const seo = genSeo({ designName: stripSizeSuffix(l.designName), designCode: l.designCode,
        productType: l.productType, colour: l.colour, fit: l.fit, audience: l.audience,
        sizeLabels: lines.map(x => x.sizeLabel), sizeCodeOf: label => s.sizes[label] || label });
      const rec = { key, seo, seoApproved: false, source: 'product-details' };
      const at = po.seoDraft.findIndex(x => x.key === key);
      if (at >= 0) po.seoDraft[at] = rec; else po.seoDraft.push(rec);
    }
  }
  saveStore(s);
  res.json({ success: true, poId: po.id, touched, po: publicPo(po, req) });
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
    if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
    const b = req.body || {};
    if (b.vendor != null)       po.vendor = String(b.vendor).toUpperCase().trim();
    if (b.line != null)         po.line = normLine(b.line);
    if (b.sourceBatchId != null) po.sourceBatchId = po.line ? String(b.sourceBatchId).trim().slice(0, 80) : '';
    if (b.sourceBatchName != null) po.sourceBatchName = po.line ? String(b.sourceBatchName).trim().slice(0, 120) : '';
    if (!po.line) { po.sourceBatchId = ''; po.sourceBatchName = ''; }
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
    // FULL line edit: retain the serial, but rebuild the SKU when its product,
    // colour or size components changed. Brand-new lines receive a new serial.
    if (Array.isArray(b.lines)) {
      // Carry each line's frozen "ordered" baseline across a full-form edit. The
      // preview rebuild drops unknown fields, so we re-attach by (stable) SKU.
      const prevOrdered = {}, prevReceiptAdded = {};
      (po.lines || []).forEach(l => {
        if (l.sku && l.ordered) prevOrdered[l.sku] = l.ordered;
        if (l.sku && l.receiptAdded) prevReceiptAdded[l.sku] = l.receiptAdded;
      });
      const preparedLines = b.lines.map((raw, idx) => {
        const incoming = { ...raw };
        const old = (po.lines || [])[idx];
        if (!old) return incoming;
        const identityChanged = ['productType', 'colour', 'sizeLabel'].some(k => String(incoming[k] == null ? '' : incoming[k]) !== String(old[k] == null ? '' : old[k]));
        if (identityChanged) {
          const rebuilt = rebuildLineSku(s, incoming, old.sku);
          incoming.sku = rebuilt.sku;
          incoming.serialUsed = rebuilt.serialUsed;
          incoming.skuError = rebuilt.error;
        }
        return incoming;
      });
      const preview = await computePreview(s, { lines: preparedLines, vendor: po.vendor, exRate: po.exRate, freightPerGram: po.freightPerGram, origin: po.origin, transportTotal: po.transportTotal });
      po.lines = preview.lines.map(l => ({
        designName: l.designName, productType: l.productType, colour: l.colour,
        sizeLabel: l.sizeLabel, chinaSize: l.chinaSize, fit: l.fit, audience: l.audience,
        vendor: l.vendor || po.vendor, designCode: l.designCode, photoUrl: l.photoUrl,
        qty: l.qty, perPcsYuan: l.perPcsYuan,
        weightGrams: num(l.weightGrams),
        sku: l.sku, serialUsed: l.serialUsed || null, skuError: l.skuError || null,
        classification: l.classification,
        // Preserve the frozen ordered baseline: carried on the line, else matched
        // by SKU from before the edit, else seeded fresh so tracking still starts.
        ordered: (l.ordered && typeof l.ordered === 'object') ? l.ordered : (prevOrdered[l.sku] || orderedSnapshot(l)),
        receiptAdded: prevReceiptAdded[l.sku] || null
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

// ── Tag a PO's product line (Funky / Casuals) — works on ANY status ──
// Bulk-tagging needs to reach posted/live POs too, so this is deliberately
// NOT gated on status (unlike edit/delete). Accepts one id or ?ids / body.ids
// for a batch. line = 'funky' | 'casuals' | '' (clear → Unclassified).
router.post('/api/procurement/pos/:id/line', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  po.line = normLine((req.body || {}).line);
  // This quick tagging endpoint does not choose a batch. Clear any previous
  // bridge so moving a PO between lines cannot leave it attached to the wrong one.
  po.sourceBatchId = ''; po.sourceBatchName = '';
  saveStore(s);
  res.json({ success: true, poId: po.id, line: po.line });
});

// ── Delete a PO entirely (not posted) ────────────────────────────
router.delete('/api/procurement/pos/:id', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be deleted.' });
  delete s.pos[req.params.id];
  saveStore(s);
  res.json({ success: true, deleted: req.params.id });
});

// ── AI IMAGE STUDIO (admin) ──────────────────────────────────────
// The listing-image + image-judged-SEO pipeline that sits between "received"
// and "posted": generate 4 shots per NEW product → admin approves → generate
// display name + SEO by JUDGING the approved photo → admin approves → post.

// NEW-product groups of a PO (design×colour), each with a representative raw
// photo to feed the image model. Only NEW products need images (EXISTING ones
// already have a Shopify listing we only add stock to).
async function newGroupsOf(s, po) {
  const preview = await computePreview(s, { lines: (po.lines || []).filter(line => num(line.qty) > 0), vendor: po.vendor, exRate: po.exRate, freightPerGram: po.freightPerGram, origin: po.origin, transportTotal: po.transportTotal });
  return (preview.newProducts || []).map(np => {
    const line = (po.lines || []).find(l => groupKey(l) === np.key && (l.photoUrl || '').trim());
    const details = (po.lines || []).find(l => groupKey(l) === np.key) || {};
    const audiences = [...new Set((po.lines || []).filter(l => groupKey(l) === np.key).map(l => String(l.audience || '').trim()))];
    const audience = audiences.length === 1 && ['Men','Women','Unisex'].includes(audiences[0]) ? audiences[0] : '';
    return { key: np.key, colour: np.colour, productType: np.productType, designName: np.designName,
             designCode: np.designCode, audience, line: po.line || '', season: details.season || po.season || '',
             fit: details.fit || '', sizeLabels: np.variants.map(v => v.sizeLabel),
             photoUrl: line ? line.photoUrl : '' };
  });
}

// Read-only: the NEW-product groups of a PO so the AI studio can be prepared
// at the ADVANCE stage — during the shipping lead time, before goods arrive.
// No status change and no weights required (weights don't affect imaging/SEO).
router.get('/api/procurement/pos/:id/studio', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    const preview = await computePreview(s, { lines: po.lines, vendor: po.vendor, exRate: po.exRate, freightPerGram: po.freightPerGram, origin: po.origin, transportTotal: po.transportTotal });
    const groups = await newGroupsOf(s, po);
    const byKey = new Map(groups.map(g => [g.key, g]));
    res.json({ success: true, newProducts: (preview.newProducts || []).map(np => ({...np,
      audience: byKey.get(np.key)?.audience || '', fit: byKey.get(np.key)?.fit || '', line: po.line || '', season:byKey.get(np.key)?.season||''})), po: publicPo(po, req) });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// The listing audience is a product-level decision. Save it to every size
// line, and retain any earlier model drafts in audit history, never in the
// active set that can be approved or posted under the new audience.
router.post('/api/procurement/pos/:id/group-audience', async (req,res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({success:false,error:'Purchases access required.'});
    const s=loadStore(),po=s.pos[req.params.id],key=String((req.body||{}).groupKey||'');
    const audience=String((req.body||{}).audience||'');
    if (!po || isLockedPo(po)) return res.status(409).json({success:false,error:'Editable PO required.'});
    if (!['Men','Women','Unisex'].includes(audience)) return res.status(400).json({success:false,error:'Select Men, Women or Unisex.'});
    const lines=(po.lines||[]).filter(l=>groupKey(l)===key);
    if (!lines.length) return res.status(404).json({success:false,error:'Product group not found.'});
    // A browser or deployment interruption can leave an old paid attempt saved
    // as "running". Expire it here too, so it cannot permanently block the
    // user from retiring an incorrect model draft.
    expireStalePaidAttempts(po);
    if (((po.openaiPilot||{}).attempts||[]).some(a=>a.status==='running')) return res.status(409).json({success:false,error:'Wait for the PO’s current image generation to finish before changing a product audience.'});
    const oldAudiences=[...new Set(lines.map(l=>String(l.audience||'').trim()))];
    const sameAudience=oldAudiences.length===1&&oldAudiences[0]===audience;
    if(sameAudience&&(req.body||{}).resetModels!==true) return res.json({success:true,audience,changed:false});
    const now=new Date().toISOString(),who=(req.user||{}).username||'system';
    if(!sameAudience)for(const line of lines){
      if(!line.ordered||typeof line.ordered!=='object')line.ordered=orderedSnapshot(line);
      line.audience=audience;line.editedAt=now;line.editedBy=who;
    }
    retireAudienceModelImages(po,key,oldAudiences.join(' / '),audience);
    if(!sameAudience){
      const updatedGroup=(await newGroupsOf(s,po)).find(g=>g.key===key);
      if(updatedGroup)for(const image of ((po.aiImages||{})[key]||[]))if(['front','back'].includes(image.type))image.sourceFingerprint=codexBatch.fingerprint(updatedGroup,(po.backRefs||{})[key]);
      const first=lines[0];
      const seo=genSeo({designName:stripSizeSuffix(first.designName),designCode:first.designCode,productType:first.productType,
        colour:first.colour,fit:first.fit,audience,sizeLabels:lines.map(l=>l.sizeLabel),sizeCodeOf:label=>s.sizes[label]||label});
      po.seoDraft=Array.isArray(po.seoDraft)?po.seoDraft:[];
      const idx=po.seoDraft.findIndex(d=>d.key===key),draft={key,seo,seoApproved:false,source:'product-details'};
      if(idx>=0)po.seoDraft[idx]=draft;else po.seoDraft.push(draft);
    }
    saveStore(s);
    res.json({success:true,audience,changed:true,retiredViews:'Previous model photos were kept in audit history and removed from active posting.'});
  }catch(e){res.status(500).json({success:false,error:e.message});}
});

// Included-usage generation happens in a user-started Codex session, not Railway.
const codexBatch = require('./procurement-codex-batch');
const openaiPilot = require('./procurement-openai-pilot');
router.post('/api/procurement/pos/:id/image-styling', async (req,res) => {
  try {
  if (!canManagePurchases(req)) return res.status(403).json({success:false,error:'Purchases access required.'});
  const s=loadStore(),po=s.pos[req.params.id],key=String((req.body||{}).groupKey||'');
  if(!po||isLockedPo(po))return res.status(409).json({success:false,error:'Editable PO required.'});
  const group=(await newGroupsOf(s,po)).find(g=>g.key===key);
  if(!group)return res.status(404).json({success:false,error:'Product group not found.'});
  const styling=openaiPilot.normalizeStyling((req.body||{}).styling,group);
  po.imageStyling=po.imageStyling||{};
  const previous=openaiPilot.normalizeStyling(po.imageStyling[key],group);
  const changedFields=Object.keys(styling).filter(field=>previous[field]!==styling[field]);
  po.imageStyling[key]=styling;
  if(changedFields.length) for(const image of ((po.aiImages||{})[key]||[])) {
    if(image.source==='openai-pilot' && image.styling && ['female','male','model-front','model-side','model-side-female','model-side-male'].includes(image.type)) {
      const gender= image.type==='female'||image.type==='model-side-female'?'female'
        :image.type==='male'||image.type==='model-side-male'?'male':group.audience==='Men'?'male':'female';
      if(changedFields.every(field=>field===(gender==='female'?'maleComplexion':'femaleComplexion')))continue;
      image.approved=false;
      image.qa={...(image.qa||{}),status:'needs-review',issues:['Model styling changed after this image was generated. Regenerate this view.']};
    }
  }
  saveStore(s);
  res.json({success:true,groupKey:key,styling,images:(po.aiImages||{})[key]||[]});
  } catch(e) { res.status(500).json({success:false,error:e.message}); }
});
const paidPilotInFlight = new Set();
function expireStalePaidAttempts(po) {
  let changed=false;
  for(const attempt of ((po.openaiPilot||{}).attempts||[])) {
    if(attempt.status==='running' && Date.now()-Date.parse(attempt.startedAt||'')>90*60*1000) {
      attempt.status='interrupted';attempt.completedAt=new Date().toISOString();
      attempt.errors=Array.isArray(attempt.errors)?attempt.errors:[];
      attempt.errors.push({type:'job',error:'Generation stopped or lost contact. Saved drafts remain; review them before an explicit retry.'});
      changed=true;
    }
  }
  return changed;
}
function canStartPaidPilot(req) {
  const roles = (req.user && Array.isArray(req.user.roles) && req.user.roles.length)
    ? req.user.roles : [req.user && req.user.role];
  return roles.some(r => ['owner','admin','inventory'].includes(String(r).toLowerCase()));
}
function canReviewPaidImage(req) {
  const roles=(req.user && Array.isArray(req.user.roles) && req.user.roles.length)
    ? req.user.roles : [req.user && req.user.role];
  return roles.some(r=>['owner','admin'].includes(String(r).toLowerCase()));
}
function imageCheckAccepted(image) {
  return !image.qa || ['pass','manual-reviewed'].includes(image.qa.status);
}
function invalidateDependentSides(images,frontType) {
  const sideType={'model-front':'model-side',female:'model-side-female',male:'model-side-male'}[frontType];
  if(!sideType)return;
  const side=images.find(image=>image.type===sideType);
  if(side){side.approved=false;side.qa={...(side.qa||{}),status:'needs-review',issues:['Matching front image changed. Regenerate this three-quarter view.']};}
}
router.get('/api/procurement/openai-pilot-status', (req,res) => {
  if (!canManagePurchases(req)) return res.status(403).json({success:false,error:'Purchases access required.'});
  res.json({success:true,configured:!!process.env.OPENAI_API_KEY,
    imageModel:process.env.PROCUREMENT_OPENAI_IMAGE_MODEL||'gpt-image-1.5',
    maxGroups:Math.min(1000,Math.max(1,Number(process.env.PROCUREMENT_OPENAI_MAX_GROUPS)||30))});
});
router.get('/api/procurement/pos/:id/openai-pilot-status', (req,res) => {
  if (!canStartPaidPilot(req)) return res.status(403).json({success:false,error:'Paid image-generation access required.'});
  const s=loadStore(),po=s.pos[req.params.id],key=String(req.query.groupKey||'');
  if (!po) return res.status(404).json({success:false,error:'PO not found.'});
  if(expireStalePaidAttempts(po))saveStore(s);
  const record=((po.openaiPilot||{}).attempts||[]).slice().reverse().find(x=>x.groupKey===key);
  if (!record) return res.status(404).json({success:false,error:'No pilot attempt for this article.'});
  res.json({success:true,pilot:record,images:(po.aiImages||{})[key]||[],rejectedImages:((po.qaRejected||{})[key]||[]).filter(x=>!x.supersededBy),seo:(po.seoDraft||[]).find(x=>x.key===key)||null});
});
// A mistaken visual-check verdict can be resolved without another paid image
// call, but only by an owner/admin who inspects the held draft and records why.
router.post('/api/procurement/pos/:id/qa-review', async (req,res) => {
  try {
    if(!canReviewPaidImage(req))return res.status(403).json({success:false,error:'Owner or admin review required.'});
    const s=loadStore(),po=s.pos[req.params.id],key=String((req.body||{}).groupKey||''),url=String((req.body||{}).url||'');
    const reason=String((req.body||{}).reason||'').trim();
    if(!po||isLockedPo(po))return res.status(409).json({success:false,error:'Editable PO required.'});
    if(reason.length<12||reason.length>500)return res.status(400).json({success:false,error:'Give a short, specific reason (12–500 characters) for accepting this image.'});
    const group=(await newGroupsOf(s,po)).find(g=>g.key===key);
    const rejected=(po.qaRejected||{})[key]||[];
    const candidate=rejected.find(image=>image.url===url&&image.type===(req.body||{}).type&&!image.supersededBy);
    if(!group||!candidate||!readStoredPhoto(candidate.url))return res.status(404).json({success:false,error:'Held image not found.'});
    const attempt=((po.openaiPilot||{}).attempts||[]).filter(item=>item.groupKey===key&&item.startedAt<=candidate.at).slice(-1)[0];
    const fingerprint=candidate.sourceFingerprint||attempt?.sourceFingerprint;
    const styling=candidate.styling||attempt?.styling;
    if(!fingerprint||fingerprint!==codexBatch.fingerprint(group,(po.backRefs||{})[key]))return res.status(409).json({success:false,error:'The product reference changed. Generate a new image before review.'});
    if(!styling||JSON.stringify(openaiPilot.normalizeStyling(styling,group))!==JSON.stringify(openaiPilot.normalizeStyling((po.imageStyling||{})[key],group)))return res.status(409).json({success:false,error:'The styling changed. Generate a new image before review.'});
    po.aiImages=po.aiImages||{};
    const images=po.aiImages[key]||[];
    const rec={type:candidate.type,label:(AI_IMAGE_SPECS.find(x=>x.type===candidate.type)||{}).label||candidate.type,url:candidate.url,approved:false,source:'openai-pilot',
      qa:{...candidate.qa,status:'manual-reviewed',issues:[],manualReview:{reason,by:String(req.user?.username||req.user?.role||'owner'),at:new Date().toISOString()}},sourceFingerprint:fingerprint,
      styling:['female','male','model-front','model-side','model-side-female','model-side-male'].includes(candidate.type)?styling:null};
    const idx=images.findIndex(image=>image.type===rec.type);if(idx>=0)images[idx]=rec;else images.push(rec);
    invalidateDependentSides(images,rec.type);
    po.aiImages[key]=images;
    po.qaRejected[key]=rejected.filter(image=>image!==candidate);
    saveStore(s);
    res.json({success:true,images,rejectedImages:po.qaRejected[key]});
  } catch(e){res.status(500).json({success:false,error:e.message});}
});
// Explicit, authorised-user-started pilot. One colourway per request; the user
// authorizes at most two billed image attempts per view before the job starts.
// No automatic approvals or Shopify writes.
router.post('/api/procurement/pos/:id/openai-pilot', async (req,res) => {
  const lockKey=req.params.id;
  if (!canStartPaidPilot(req)) return res.status(403).json({success:false,error:'Paid image-generation access required.'});
  if (!process.env.OPENAI_API_KEY) return res.status(503).json({success:false,error:'Set OPENAI_API_KEY in Railway before starting the paid pilot.'});
  if (paidPilotInFlight.has(lockKey)) return res.status(409).json({success:false,error:'Another paid generation is running for this PO.'});
  paidPilotInFlight.add(lockKey);
  try {
    const s=loadStore(),po=s.pos[req.params.id],key=String((req.body||{}).groupKey||'');
    if (!po || isLockedPo(po)) return res.status(400).json({success:false,error:'Editable PO required.'});
    if(expireStalePaidAttempts(po))saveStore(s);
    const g=(await newGroupsOf(s,po)).find(x=>x.key===key);
    const source=g&&readStoredPhoto(g.photoUrl);
    if (!g || !source) return res.status(400).json({success:false,error:'Product group with original photo required.'});
    if (!['Men','Women','Unisex'].includes(g.audience)) return res.status(400).json({success:false,error:'Set Men, Women or Unisex before paid image generation.'});
    const backSource=(po.backRefs||{})[key] ? readStoredPhoto(po.backRefs[key]) : null;
    const maxGroups=Math.min(1000,Math.max(1,Number(process.env.PROCUREMENT_OPENAI_MAX_GROUPS)||30));
    po.openaiPilot=po.openaiPilot||{attempts:[]};
    const priorAttempts=po.openaiPilot.attempts.filter(x=>x.groupKey===key),retry=(req.body||{}).retry===true;
    if (priorAttempts.length&&!retry) return res.status(409).json({success:false,error:'This article has a previous paid attempt. Confirm a missing-drafts retry in the app.'});
    if (priorAttempts.length&&priorAttempts[priorAttempts.length-1].status==='running') return res.status(409).json({success:false,error:'This article is still generating. Reopen the PO to check progress.'});
    if (!priorAttempts.length&&retry) return res.status(409).json({success:false,error:'No earlier paid attempt exists for this article. Reload the PO before generating.'});
    if (new Set(po.openaiPilot.attempts.map(x=>x.groupKey)).size>=maxGroups&&!priorAttempts.length) return res.status(409).json({success:false,error:`Pilot limit of ${maxGroups} articles reached for this PO.`});
    const fingerprint=codexBatch.fingerprint(g,(po.backRefs||{})[key]);
    const allowedTypes=openaiPilot.pilotTypes(g,!!backSource);
    const savedImages=((po.aiImages||{})[key]||[]);
    const requestedRegeneration=(req.body||{}).regenerateTypes;
    if(requestedRegeneration!==undefined && (!Array.isArray(requestedRegeneration)||!requestedRegeneration.length||requestedRegeneration.length>6||new Set(requestedRegeneration).size!==requestedRegeneration.length||requestedRegeneration.some(type=>!allowedTypes.includes(type)||!savedImages.some(image=>image.type===type&&image.url)))) {
      return res.status(400).json({success:false,error:'Select existing, supported image views to regenerate.'});
    }
    const regenerateTypes=requestedRegeneration||[];
    const sideToFront={ 'model-side':'model-front', 'model-side-female':'female', 'model-side-male':'male' };
    const invalidTypes=allowedTypes.filter(type=>savedImages.some(x=>x.type===type&&x.url&&x.qa&&!imageCheckAccepted(x)));
    const neededTypes=allowedTypes.filter(type=>!savedImages.some(x=>x.type===type&&x.url&&(!x.qa||imageCheckAccepted(x))));
    const existingSeo=(po.seoDraft||[]).find(x=>x.key===key);
    // A deterministic draft made when corrections were saved is NOT AI-written.
    // Preserve approved manual copy; replace unapproved placeholders with AI copy.
    const needsSeo=!regenerateTypes.length&&!(existingSeo&&existingSeo.seo&&(existingSeo.seoApproved||existingSeo.source==='openai-pilot'));
    const types=regenerateTypes.length?regenerateTypes:neededTypes;
    const replaceTypes=new Set(regenerateTypes.concat(invalidTypes));
    if(types.some(type=>sideToFront[type]&&!types.includes(sideToFront[type])&&!savedImages.some(image=>image.type===sideToFront[type]&&image.url&&imageCheckAccepted(image)))) {
      return res.status(409).json({success:false,error:'Generate a visually checked front model image before its three-quarter view.'});
    }
    if (!types.length&&!needsSeo) return res.status(409).json({success:false,error:'All image and SEO drafts already exist. Review and approve them; no paid retry was started.'});
    // An already-open browser tab may still run the older one-attempt UI after a
    // deployment. Do not spend credits under that stale confirmation dialog.
    if((req.body||{}).maxImageAttempts!==2) return res.status(409).json({success:false,error:'Purchases page is out of date. Refresh the page and reopen this PO before generating photos. No paid call was made.'});
    const styling=openaiPilot.normalizeStyling((req.body||{}).styling||(po.imageStyling||{})[key],g);
    po.imageStyling=po.imageStyling||{};
    if(!po.imageStyling[key])po.imageStyling[key]=styling;
    else if(JSON.stringify(po.imageStyling[key])!==JSON.stringify(styling))return res.status(409).json({success:false,error:'Styling changed or is still saving. Wait for it to save, then retry.'});
    const maxImageAttempts=2;
    const attempt={groupKey:key,sourceFingerprint:fingerprint,styling,regenerateTypes,maxImageAttempts,startedAt:new Date().toISOString(),status:'running',retry,views:[],errors:[]};
    po.openaiPilot.attempts.push(attempt);saveStore(s);
    res.status(202).json({success:true,groupKey:key,pilot:attempt});
    const imageModel=process.env.PROCUREMENT_OPENAI_IMAGE_MODEL||'gpt-image-1.5';
    const textModel=process.env.PROCUREMENT_OPENAI_TEXT_MODEL||'gpt-4.1-mini';
    const checkModel=process.env.PROCUREMENT_OPENAI_CHECK_MODEL||'gpt-4.1-mini';
    let preflightBlocked=false,photoStyling=styling;
    try {
      const fitPreflight=types.length?await openaiPilot.preflightFit({key:process.env.OPENAI_API_KEY,group:g,source,styling,model:checkModel}):{status:'not-required',reason:'SEO-only job'};
      const fresh=loadStore(),current=fresh.pos[req.params.id],item=current.openaiPilot.attempts.slice().reverse().find(x=>x.groupKey===key);
      item.fitPreflight=fitPreflight;
      const freshGroup=(await newGroupsOf(fresh,current)).find(x=>x.key===key);
      if(!freshGroup||codexBatch.fingerprint(freshGroup,(current.backRefs||{})[key])!==fingerprint||JSON.stringify(openaiPilot.normalizeStyling((current.imageStyling||{})[key],freshGroup))!==JSON.stringify(styling)) {
        preflightBlocked=true;item.errors.push({type:'preflight',error:'Product photo or styling changed during the source check. No image call was made.'});
      } else if(fitPreflight.status==='conflict') {
        photoStyling=openaiPilot.stylingForPhoto(styling,fitPreflight);
        item.photoStyling=photoStyling;
        item.warnings=[`The selected ${styling.fit} fit conflicts with the original photo (${fitPreflight.reason}). Images will follow the photographed cut instead. Correct the purchase fit before posting.`];
      }
      saveStore(fresh);
    } catch(e) {
      preflightBlocked=true;
      const fresh=loadStore(),item=((fresh.pos[req.params.id]||{}).openaiPilot||{}).attempts?.slice().reverse().find(x=>x.groupKey===key);
      if(item){item.errors.push({type:'preflight',error:'Could not check the original fit: '+e.message+'. No image call was made.'});saveStore(fresh);}
    }
    if(!preflightBlocked)for(const type of types){
      try {
        if (!replaceTypes.has(type)&&((po.aiImages||{})[key]||[]).some(x=>x.type===type && x.url)) continue;
        const matchingFrontType=type==='model-side'?'model-front':type==='model-side-female'?'female':type==='model-side-male'?'male':'';
        const currentForReference=matchingFrontType?loadStore().pos[req.params.id]:null;
        if(matchingFrontType&&types.includes(matchingFrontType)&&!currentForReference.openaiPilot.attempts.slice().reverse().find(x=>x.groupKey===key).views.some(view=>view.type===matchingFrontType)) {
          throw new Error('The new front model view did not pass its visual check. Three-quarter generation was skipped so it cannot reuse an older outfit.');
        }
        const matchingFront=matchingFrontType&&((currentForReference?.aiImages||{})[key]||[]).find(image=>image.type===matchingFrontType&&image.url&&imageCheckAccepted(image));
        const continuitySource=matchingFront?readStoredPhoto(matchingFront.url):null;
        if(matchingFrontType&&!continuitySource) throw new Error('Generate the matching front model view first so the three-quarter view can keep the same outfit.');
        let repairFields=[];
        for(let imageAttempt=1;imageAttempt<=maxImageAttempts;imageAttempt++){
        const before=loadStore(),started=before.pos[req.params.id].openaiPilot.attempts.slice().reverse().find(x=>x.groupKey===key);
        started.imageCalls=started.imageCalls||[];
        started.imageCalls.push({type,attempt:imageAttempt,startedAt:new Date().toISOString()});saveStore(before);
        const generated=await openaiPilot.generateImage({key:process.env.OPENAI_API_KEY,group:g,source:type==='back'?backSource:source,continuitySource,type,styling:photoStyling,repairFields,model:imageModel});
        let check;
        try {check=await openaiPilot.verifyImage({key:process.env.OPENAI_API_KEY,group:g,source:type==='back'?backSource:source,generated:generated.buffer,continuitySource,type,styling:photoStyling,model:checkModel});}
        catch(e){check={status:'unavailable',failed:['verification'],issues:['Visual check unavailable: '+e.message.slice(0,140)]};}
        const fresh=loadStore(),current=fresh.pos[req.params.id];
        const freshGroup=current&&(await newGroupsOf(fresh,current)).find(x=>x.key===key);
        if(!freshGroup || codexBatch.fingerprint(freshGroup,(current.backRefs||{})[key])!==fingerprint) throw new Error('Product details changed during generation; result was discarded.');
        if(JSON.stringify(openaiPilot.normalizeStyling((current.imageStyling||{})[key],freshGroup))!==JSON.stringify(styling)) throw new Error('Styling changed during generation; result was discarded. Regenerate with the saved settings.');
        current.aiImages=current.aiImages||{};const images=current.aiImages[key]||[];
        const prior=images.find(x=>x.type===type);
        if(replaceTypes.has(type) && prior?.url!==savedImages.find(image=>image.type===type)?.url) throw new Error('This view changed during regeneration; result was discarded.');
        if(prior && prior.url && !replaceTypes.has(type)) throw new Error('An image already exists for this view; generated draft was not attached.');
        const saved=savePhotoBuffer(generated.buffer,'.png');
        if(check.status!=='pass') {
          current.qaRejected=current.qaRejected||{};
          current.qaRejected[key]=Array.isArray(current.qaRejected[key])?current.qaRejected[key]:[];
          current.qaRejected[key].push({type,url:saved.url,qa:check,sourceFingerprint:fingerprint,styling:photoStyling,at:new Date().toISOString()});
          current.qaRejected[key]=current.qaRejected[key].slice(-12);
          const item=current.openaiPilot.attempts.slice().reverse().find(x=>x.groupKey===key);
          const canRepair=openaiPilot.shouldRetryImageCheck(check,imageAttempt,maxImageAttempts);
          if(!canRepair)item.errors.push({type,error:'Visual check after '+imageAttempt+' image attempt(s): '+(check.issues.join('; ')||check.failed.join(', '))+'. Earlier image kept; inspect the held draft.'});
          saveStore(fresh);
          if(canRepair){repairFields=check.failed;continue;}
          break;
        }
        const rec={type,label:(AI_IMAGE_SPECS.find(x=>x.type===type)||{}).label||type,url:saved.url,approved:false,source:'openai-pilot',qa:check,
          sourceFingerprint:fingerprint,styling:['female','male','model-front','model-side','model-side-female','model-side-male'].includes(type)?photoStyling:null};
        const idx=images.findIndex(x=>x.type===type);if(idx>=0)images[idx]=rec;else images.push(rec);
        for(const held of ((current.qaRejected||{})[key]||[]))if(held.type===type&&!held.supersededBy){held.supersededBy=rec.url;held.supersededAt=new Date().toISOString();}
        invalidateDependentSides(images,type);
        current.aiImages[key]=images;
        const item=current.openaiPilot.attempts.slice().reverse().find(x=>x.groupKey===key);item.views.push({type,model:imageModel,attempts:imageAttempt,usage:generated.usage||null});
        saveStore(fresh);
        break;
        }
      }catch(e){
        const fresh=loadStore(),item=((fresh.pos[req.params.id]||{}).openaiPilot||{}).attempts?.slice().reverse().find(x=>x.groupKey===key);
        if(item){item.errors.push({type,error:e.message});saveStore(fresh);}
      }
    }
    if (!preflightBlocked&&needsSeo) try {
      const generated=await openaiPilot.generateSeo({key:process.env.OPENAI_API_KEY,group:g,source,model:textModel});
      const fresh=loadStore(),current=fresh.pos[req.params.id];
      const freshGroup=current&&(await newGroupsOf(fresh,current)).find(x=>x.key===key);
      if(!freshGroup || codexBatch.fingerprint(freshGroup,(current.backRefs||{})[key])!==fingerprint) throw new Error('Product details changed during generation; SEO was discarded.');
      const base=genSeo({...g,sizeCodeOf:label=>fresh.sizes[label]||label});
      const clean=(value,fallback)=>stripInternalCodes(String(value||'').trim(),g.designCode)||fallback;
      const draft=generated.seo;
      const retail=openaiPilot.retailFacts(g);
      const seo={displayName:clean(draft.displayName,base.displayName),title:clean(draft.title,base.title),
        handle:slugify([draft.displayName,g.colour,retail.productType,g.designCode].filter(Boolean).join(' ')) || base.handle,
        metaTitle:clean(draft.metaTitle,base.metaTitle).slice(0,70),metaDescription:clean(draft.metaDescription,base.metaDescription).slice(0,320),
        imageAlt:clean(draft.imageAlt,base.imageAlt),tags:draft.tags.map(x=>clean(x,'')).filter(Boolean),
        bodyHtml:clean(draft.bodyHtml,base.bodyHtml).replace(/<([^>]+)>/g,(tag,inside)=>/^\/?p$/i.test(inside.trim())?tag:'')};
      if(seoNeedsReview(seo)) throw new Error('Generated SEO was incomplete or repetitive.');
      if(openaiPilot.seoCopyNeedsReview(seo,g)) throw new Error('Generated SEO did not meet the women’s top/fit and distinctive-name rules. The earlier draft was kept for review.');
      current.seoDraft=current.seoDraft||[];
      const rec={key,designCode:g.designCode,colour:g.colour,productType:g.productType,seo,seoApproved:false,source:'openai-pilot'};
      const idx=current.seoDraft.findIndex(x=>x.key===key);if(idx>=0)current.seoDraft[idx]=rec;else current.seoDraft.push(rec);
      const item=current.openaiPilot.attempts.slice().reverse().find(x=>x.groupKey===key);item.seo={model:textModel,usage:generated.usage||null};
      saveStore(fresh);
    }catch(e){
      const fresh=loadStore(),item=((fresh.pos[req.params.id]||{}).openaiPilot||{}).attempts?.slice().reverse().find(x=>x.groupKey===key);
      if(item){item.errors.push({type:'seo',error:e.message});saveStore(fresh);}
    }
    const done=loadStore(),donePo=done.pos[req.params.id],record=donePo.openaiPilot.attempts.slice().reverse().find(x=>x.groupKey===key);
    record.status=record.errors.length?(record.views.length||record.seo?'partial':'failed'):'drafts-ready';
    record.completedAt=new Date().toISOString();saveStore(done);
  }catch(e){
    if (!res.headersSent) res.status(500).json({success:false,error:e.message});
    else {
      const s=loadStore(),record=(((s.pos[req.params.id]||{}).openaiPilot||{}).attempts||[]).slice().reverse().find(x=>x.status==='running');
      if(record){record.errors.push({type:'job',error:e.message});record.status='failed';record.completedAt=new Date().toISOString();saveStore(s);}
    }
  }
  finally{paidPilotInFlight.delete(lockKey);}
});
router.post('/api/procurement/pos/:id/codex-batch', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({success:false,error:'Purchases access required.'});
    const s=loadStore(), po=s.pos[req.params.id];
    if (!po || isLockedPo(po)) return res.status(400).json({success:false,error:'Editable purchase required.'});
    const g=(await newGroupsOf(s,po)).find(g=>g.key===(req.body||{}).groupKey);
    if (!g || !readStoredPhoto(g.photoUrl)) return res.status(400).json({success:false,error:'Product group with source photo required.'});
    const batch=codexBatch.prepare(po,g); batch.poId=req.params.id;
    po.codexBatches=po.codexBatches||{}; po.codexBatches[batch.id]=batch; saveStore(s);
    res.json({success:true,batch});
  } catch(e) {res.status(400).json({success:false,error:e.message});}
});
router.post('/api/procurement/pos/:id/codex-batch/:batchId/results', async (req,res)=>{
  try {
    if (!canManagePurchases(req)) return res.status(403).json({success:false,error:'Purchases access required.'});
    const s=loadStore(),po=s.pos[req.params.id],batch=po&&(po.codexBatches||{})[req.params.batchId];
    if(!batch || isLockedPo(po)) throw new Error('Editable batch not found.');
    if((req.body||{}).groupKey!==batch.groupKey) throw new Error('Batch belongs to a different product/colour.');
    const g=(await newGroupsOf(s,po)).find(g=>g.key===batch.groupKey);
    if(!g) throw new Error('Product group no longer exists.');
    const submitted=req.body||{};
    if(!submitted.seo && (!Array.isArray(submitted.images) || !submitted.images.length)) throw new Error('Return images or listing copy.');
    const images=Array.isArray(submitted.images) && submitted.images.length
      ? codexBatch.accept(batch,g,(po.backRefs||{})[g.key],submitted.images,readStoredPhoto) : [];
    const seo=submitted.seo ? codexBatch.acceptSeo(batch,g,(po.backRefs||{})[g.key],submitted.seo) : null;
    if(seo && seoNeedsReview(seo)) throw new Error('Review incomplete or repetitive listing copy.');
    po.aiImages=po.aiImages||{}; const existing=po.aiImages[g.key]||[];
    for(const img of images){const old=existing.find(x=>x.type===img.type);if(old && old.url!==img.url) throw new Error('View already exists; use the existing replacement controls.');}
    for(const img of images) if(!existing.some(x=>x.type===img.type)) existing.push(img);
    po.aiImages[g.key]=existing; batch.status='returned'; batch.returnedAt=new Date().toISOString();saveStore(s);
    if(seo){
      po.seoDraft=Array.isArray(po.seoDraft)?po.seoDraft:[];
      const rec={key:g.key,designCode:g.designCode,colour:g.colour,productType:g.productType,seo,seoApproved:false,source:'codex-batch',codexBatchId:batch.id};
      const at=po.seoDraft.findIndex(x=>x.key===g.key);
      if(at>=0)po.seoDraft[at]=rec;else po.seoDraft.push(rec);
    }
    saveStore(s);
    res.json({success:true,images:existing,seo});
  }catch(e){res.status(400).json({success:false,error:e.message});}
});

// The source photograph is a reference only and must never become a listing view.
router.post('/api/procurement/pos/:id/use-original-photo', async (req, res) => {
  res.status(409).json({ success: false, error: 'The original photograph is a private reference and cannot be posted to Shopify. Approve generated listing views instead.' });
});

// Generate the AI shots for ONE product group (or specific `types`).
router.post('/api/procurement/pos/:id/generate-images', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    if (!GEMINI_API_KEY) return res.status(400).json({ success: false, error: 'AI images are not enabled. Set GEMINI_API_KEY in Railway to turn it on.' });
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
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
  if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
  const b = req.body || {};
  if (b.aiImages && typeof b.aiImages === 'object') {
    po.aiImages = po.aiImages || {};
    const updates={};
    for(const [k,submitted] of Object.entries(b.aiImages)) {
      if(!Array.isArray(submitted))return res.status(400).json({success:false,error:'Image approvals must be a list.'});
      const existing=po.aiImages[k]||[];
      if(submitted.length!==existing.length)return res.status(409).json({success:false,error:'Images changed. Reopen the PO before approving.'});
      if(new Set(submitted.map(x=>x.type+'\n'+x.url)).size!==submitted.length)return res.status(400).json({success:false,error:'Duplicate image approval entries are not allowed.'});
      const next=[];
      for(const x of submitted) {
        const saved=existing.find(image=>image.type===x.type&&image.url===x.url);
        if(!saved)return res.status(409).json({success:false,error:'An image changed. Reopen the PO before approving.'});
        if(x.approved && !imageCheckAccepted(saved))return res.status(409).json({success:false,error:'This image has not passed visual verification or owner review.'});
        next.push({...saved,approved:!!x.approved});
      }
      updates[k]=next;
    }
    Object.assign(po.aiImages,updates);
    saveStore(s);
  }
  res.json({ success: true, aiImages: po.aiImages || {} });
});

// Judge the APPROVED product photo(s) with vision → display name + SEO.
router.post('/api/procurement/pos/:id/generate-seo', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI SEO is not enabled. Set ANTHROPIC_API_KEY in Railway.' });
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
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
  if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
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
  try {
    if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) return res.status(400).json({ success: false, error: 'Shopify env not configured' });
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    const s = loadStore(), b = req.body || {}, po = s.pos[b.poId];
    if (!b.approve || !po) return res.status(400).json({ success: false, error: 'A saved, approved purchase is required.' });
    if (po.status !== 'received') return res.status(409).json({ success: false, error: 'Purchase must be received and not already posted or partially posted.' });
    const warehouseLocationId = String(s.settings.warehouseLocationId || '');
    if (!warehouseLocationId) return res.status(400).json({ success: false, error: 'Warehouse location not set — save it in Settings first.' });
    // Zero-quantity bill lines stay in the PO for audit, but never create a
    // Shopify product/variant or adjust existing stock.
    const receivedLines = (po.lines || []).filter(line => num(line.qty) > 0);
    if (!receivedLines.length) return res.status(400).json({ success: false, error: 'No received pieces to post.' });
    const preview = await computePreview(s, { lines: receivedLines, vendor: po.vendor, exRate: po.exRate,
      freightPerGram: po.freightPerGram, origin: po.origin, transportTotal: po.transportTotal });
    if (preview.counts.errors || preview.counts.ambiguous) return res.status(400).json({ success: false, error: 'Fix SKU or product-group errors before posting.' });
    const conflicts = preview.newProducts.flatMap(p => (p.variantConflicts || []).map(c => `${p.designCode || p.designName} / ${p.colour} / ${c.size}: ${c.skus.join(', ')}`));
    if (conflicts.length) return res.status(400).json({ success: false, error: 'Different SKUs have the same product, colour and size. Decide whether they are one article or separate products before posting: ' + conflicts.join('; ') });
    const allSkus = preview.lines.map(l => l.sku).filter(Boolean);
    if (allSkus.length !== preview.lines.length || new Set(allSkus).size !== allSkus.length) return res.status(400).json({ success: false, error: 'Every purchase line needs a unique SKU.' });
    for (const np of preview.newProducts) {
      const draft = (po.seoDraft || []).find(x => x.key === np.key);
      const seo = draft && draft.seo;
      const group = (await newGroupsOf(s, po)).find(g => g.key === np.key);
      if (!draft || !draft.seoApproved || seoNeedsReview(seo) || (group&&openaiPilot.seoCopyNeedsReview(seo,group))) {
        return res.status(400).json({ success: false, error: 'Approve complete, non-repetitive listing copy for every new product.' });
      }
      const required = openaiPilot.pilotTypes(group || {}, !!(po.backRefs || {})[np.key]);
      const currentFingerprint=group?codexBatch.fingerprint(group,(po.backRefs||{})[np.key]):'';
      const approved = ((po.aiImages || {})[np.key] || []).filter(x => x.approved && imageCheckAccepted(x) && (!x.sourceFingerprint || x.sourceFingerprint===currentFingerprint) && x.type !== 'original' && x.url !== group?.photoUrl);
      if (!required.length || required.some(type => !approved.some(x => x.type === type && readStoredPhoto(x.url)))) return res.status(400).json({ success: false, error: 'Approve all required product and matching model views for each new product. The original reference photo cannot be posted.' });
      np.seo = seo;
      np.images = approved.map(x => ({ url: x.url, alt: seo.imageAlt }));
    }
    // Reserve the PO before the first external write. Any uncertain/partial
    // result needs manual reconciliation, never a blind retry that duplicates stock.
    const results = { created: [], adjusted: [], errors: [] };
    po.status = 'posting_partial';
    po.postingStartedAt = new Date().toISOString();
    po.results = results;
    saveStore(s);
    for (const np of preview.newProducts) {
      try {
        const result = await createDraftProduct(np, warehouseLocationId);
        results.created.push(result);
        saveStore(s);
        const stockError = (result.variants || []).find(v => v.stockError);
        if (stockError) throw new Error('Product created, but stock failed for ' + stockError.sku + ': ' + stockError.stockError);
      } catch (e) { results.errors.push({ kind: 'create', product: np.seo.title, error: e.message }); break; }
    }
    if (!results.errors.length) for (const ea of preview.existingAdds) {
      try {
        const result = await addExistingInventory(ea, warehouseLocationId);
        if (result.error) throw new Error(result.error);
        results.adjusted.push(result);
        saveStore(s);
      } catch (e) { results.errors.push({ kind: 'adjust', sku: ea.sku, error: e.message }); break; }
    }
    po.newProducts = preview.newProducts;
    po.existingAdds = preview.existingAdds;
    po.warehouseLocationId = warehouseLocationId;
    if (!results.errors.length) {
      po.status = 'posted';
      po.postedAt = new Date().toISOString();
    }
    saveStore(s);
    _catalogue = null;
    if (results.errors.length) return res.status(409).json({ success: false, poId: po.id, results,
      error: 'Shopify posting stopped after an error. This PO is locked as partially posted; inspect Shopify and the saved results before any retry. ' + results.errors[0].error });
    res.json({ success: true, poId: po.id, results });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

router.get('/api/procurement/pos', (req, res) => {
  const s = loadStore();
  let list = Object.values(s.pos);
  if (req.query.status) { const want = String(req.query.status).split(','); list = list.filter(p => want.includes(p.status)); }
  list = list.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  res.json({ success: true, pos: list.map(p => publicPo(p, req)) });
});
router.patch('/api/procurement/pos/:id/cost-calculation', (req, res) => {
  const role = String(req.user && req.user.role || '').toLowerCase();
  if (!isAdmin(req) && role !== 'owner') return res.status(403).json({ success: false, error: 'Only the Owner can edit a posted purchase calculation.' });
  const s = loadStore(), po = s.pos[req.params.id], b = req.body || {};
  if (!po) return res.status(404).json({ success: false, error: 'PO not found.' });
  if (po.status !== 'posted') return res.status(400).json({ success: false, error: 'Use the normal PO editor until this purchase is posted.' });
  const reason = String(b.reason || '').trim();
  if (!reason) return res.status(400).json({ success: false, error: 'A correction reason is required.' });
  const rateForExtras = Number(b.exRate != null ? b.exRate : po.exRate != null ? po.exRate : (s.settings || {}).exRate);
  const localForExtras = Number(b.localTransportYuan != null ? b.localTransportYuan : po.localTransportYuan) || 0;
  const otherForExtras = Number(b.otherCostsYuan != null ? b.otherCostsYuan : po.otherCostsYuan) || 0;
  if ((localForExtras + otherForExtras) > 0 && !(rateForExtras > 0))
    return res.status(400).json({ success: false, error: 'An exchange rate is required for local transport and other Yuan costs.' });
  const validPostedNumber = (v, integer) => v == null || (v !== '' && Number.isFinite(Number(v)) && Number(v) >= 0 && (!integer || Number.isInteger(Number(v))));
  if (['exRate', 'freightPerGram', 'transportTotal', 'localTransportYuan', 'otherCostsYuan'].some(k => !validPostedNumber(b[k], false)) ||
      (Array.isArray(b.lines) && b.lines.some(l => !validPostedNumber(l.qty, true) || !validPostedNumber(l.unitPrice, false) || !validPostedNumber(l.weightGrams, false))))
    return res.status(400).json({ success: false, error: 'Enter valid non-negative quantities, prices, weights and rates.' });
  const before = poCostBreakdown(po, s.settings);
  if (b.exRate != null && b.exRate !== '') po.exRate = Math.max(0, num(b.exRate));
  if (b.freightPerGram != null && b.freightPerGram !== '') po.freightPerGram = Math.max(0, num(b.freightPerGram));
  if (b.transportTotal != null && b.transportTotal !== '') po.transportTotal = Math.max(0, num(b.transportTotal));
  if (b.localTransportYuan != null && b.localTransportYuan !== '') po.localTransportYuan = Math.max(0, num(b.localTransportYuan));
  if (b.otherCostsYuan != null && b.otherCostsYuan !== '') po.otherCostsYuan = Math.max(0, num(b.otherCostsYuan));
  if (Array.isArray(b.lines)) b.lines.forEach((edit, i) => {
    const line = (po.lines || [])[i]; if (!line) return;
    if (edit.qty != null && edit.qty !== '') line.qty = Math.max(0, Math.round(num(edit.qty)));
    if (edit.unitPrice != null && edit.unitPrice !== '') line.perPcsYuan = Math.max(0, num(edit.unitPrice));
    if (edit.weightGrams != null && edit.weightGrams !== '') line.weightGrams = Math.max(0, num(edit.weightGrams));
  });
  const after = poCostBreakdown(po, s.settings), by = (req.user && req.user.username) || 'owner';
  const bySku = new Map(after.lines.map(x => [String(x.sku), x]));
  (po.newProducts || []).forEach(p => (p.variants || []).forEach(v => { const x = bySku.get(String(v.sku)); if (x) { v.qty = x.qty; v.landed = x.landedPerPc; } }));
  (po.existingAdds || []).forEach(v => { const x = bySku.get(String(v.sku)); if (x) { v.qty = x.qty; v.landed = x.landedPerPc; } });
  po.costCorrectionHistory = Array.isArray(po.costCorrectionHistory) ? po.costCorrectionHistory : [];
  po.costCorrectionHistory.push({ correctedAt: new Date().toISOString(), correctedBy: by, reason, before, after });
  saveStore(s);
  res.json({ success: true, po: publicPo(po, req), breakdown: after, warning: 'Accounting cost was corrected. Shopify inventory was not changed.' });
});
// Correct a pending PO's bill calculation in place. The ordered/receipt audit
// baseline and Shopify state are deliberately left untouched.
router.patch('/api/procurement/pos/:id/summary-calculation', (req, res) => {
  if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
  const s = loadStore(), po = s.pos[req.params.id], b = req.body || {};
  if (!po) return res.status(404).json({ success: false, error: 'PO not found.' });
  if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted purchases require an Owner correction with a reason.' });
  if (!Array.isArray(b.lines) || b.lines.length !== (po.lines || []).length)
    return res.status(400).json({ success: false, error: 'Submit every PO line in its original order.' });
  const validNumber = (v, integer) => v !== '' && v != null && Number.isFinite(Number(v)) && Number(v) >= 0 && (!integer || Number.isInteger(Number(v)));
  if (['exRate', 'freightPerGram', 'transportTotal', 'localTransportYuan', 'otherCostsYuan'].some(k => b[k] != null && !validNumber(b[k], false)) ||
      b.lines.some(l => !validNumber(l.qty, true) || !validNumber(l.unitPrice, false) || !validNumber(l.weightGrams, false)))
    return res.status(400).json({ success: false, error: 'Enter valid non-negative quantities, prices, weights and rates.' });
  const effectiveRate = Number(b.exRate != null ? b.exRate : po.exRate != null ? po.exRate : (s.settings || {}).exRate);
  const extraYuan = (Number(b.localTransportYuan != null ? b.localTransportYuan : po.localTransportYuan) || 0) + (Number(b.otherCostsYuan != null ? b.otherCostsYuan : po.otherCostsYuan) || 0);
  if (extraYuan > 0 && !(effectiveRate > 0)) return res.status(400).json({ success: false, error: 'An exchange rate is required for local transport and other Yuan costs.' });
  const before = poCostBreakdown(po, s.settings);
  ['exRate', 'freightPerGram', 'transportTotal', 'localTransportYuan', 'otherCostsYuan'].forEach(k => { if (b[k] != null) po[k] = Number(b[k]); });
  b.lines.forEach((edit, i) => { const line = po.lines[i]; line.qty = Number(edit.qty); line.perPcsYuan = Number(edit.unitPrice); line.weightGrams = Number(edit.weightGrams); });
  po.costCorrectionHistory = Array.isArray(po.costCorrectionHistory) ? po.costCorrectionHistory : [];
  po.costCorrectionHistory.push({ correctedAt: new Date().toISOString(), correctedBy: (req.user || {}).username || 'system', reason: 'Purchase Summary inline correction', before, after: poCostBreakdown(po, s.settings) });
  saveStore(s);
  res.json({ success: true, po: publicPo(po, req) });
});
// A combined invoice is a parent record. Its child POs remain
// untouched, including their accounting balances and individual payment trail.
router.post('/api/procurement/combined-invoices', (req, res) => {
  if (!canReconcileVendorBill(req)) return res.status(403).json({ success: false, error: 'Purchases or accounting access required.' });
  const s = loadStore(), ids = (req.body || {}).poIds;
  if (!Array.isArray(ids) || ids.length < 1 || ids.length !== new Set(ids).size || ids.some(id => typeof id !== 'string'))
    return res.status(400).json({ success: false, error: 'Select one or more distinct purchase bills.' });
  const pos = ids.map(id => s.pos[id]);
  if (pos.some(po => !po || po.historical))
    return res.status(400).json({ success: false, error: 'Every selected bill must be a visible purchase PO.' });
  const vendors = [...new Map(pos.map(po => {
    const name = String(po.vendor || 'Not recorded').trim() || 'Not recorded';
    return [name.toLowerCase(), name];
  })).values()];
  const alreadyGrouped = new Set(Object.values(s.combinedVendorInvoices).flatMap(invoice => invoice.poIds || []));
  if (ids.some(id => alreadyGrouped.has(id)))
    return res.status(409).json({ success: false, error: 'One or more bills already belong to a combined invoice.' });
  const id = 'CVI-' + Date.now().toString(36).toUpperCase() + '-' + Math.random().toString(36).slice(2, 6).toUpperCase();
  const invoice = { id, vendor: vendors.length === 1 ? vendors[0] : 'Multiple vendors', vendors,
    origin: pos.every(po => po.origin === pos[0].origin) ? pos[0].origin : 'mixed',
    poIds: ids, childBills: {}, combined: {},
    createdAt: new Date().toISOString(), createdBy: (req.user || {}).username || 'system' };
  s.combinedVendorInvoices[id] = invoice;
  saveStore(s);
  res.status(201).json({ success: true, invoice });
});
function validLgDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value || '')) return false;
  const date = new Date(value + 'T00:00:00Z');
  return !Number.isNaN(date.getTime()) && date.toISOString().slice(0, 10) === value;
}
router.patch('/api/procurement/combined-invoices/:id', (req, res) => {
  if (!canReconcileVendorBill(req)) return res.status(403).json({ success: false, error: 'Purchases or accounting access required.' });
  const s = loadStore(), invoice = s.combinedVendorInvoices[req.params.id], body = req.body || {};
  if (!invoice) return res.status(404).json({ success: false, error: 'Combined invoice not found.' });
  if (invoice.finalized) return res.status(409).json({ success: false, error: 'This LG bill is finalized and cannot be edited.' });
  if (!body.childBills || typeof body.childBills !== 'object' || Array.isArray(body.childBills) || !body.combined || typeof body.combined !== 'object')
    return res.status(400).json({ success: false, error: 'Enter bill-level and combined vendor figures.' });
  if (Object.keys(body.childBills).some(id => !invoice.poIds.includes(id)))
    return res.status(400).json({ success: false, error: 'A bill is not linked to this combined invoice.' });
  const numeric = (value, integer) => value === '' || value == null ? null :
    Number.isFinite(Number(value)) && Number(value) >= 0 && (!integer || Number.isInteger(Number(value))) ? Number(value) : undefined;
  const childBills = {};
  for (const id of invoice.poIds) {
    const child = body.childBills[id] || {}, quantity = numeric(child.totalQuantity, true), value = numeric(child.billValueYuan, false);
    if (quantity === undefined || value === undefined) return res.status(400).json({ success: false, error: 'Bill quantities and Yuan values must be non-negative numbers.' });
    childBills[id] = { billNumber: String(child.billNumber || '').trim().slice(0, 160), totalQuantity: quantity, billValueYuan: value };
  }
  const combined = {};
  for (const key of ['totalWeightGrams', 'localTransportationYuan', 'fixedTransportationYuan', 'extraChargesYuan', 'combinedFreightYuan', 'combinedFreightInr', 'exchangeRate']) {
    const value = numeric(body.combined[key], false);
    if (value === undefined) return res.status(400).json({ success: false, error: 'Combined costs, weight and rate must be non-negative numbers.' });
    combined[key] = value;
  }
  if (combined.exchangeRate === 0) return res.status(400).json({ success: false, error: 'Exchange rate must be greater than zero.' });
  const lgDate = String(body.lgDate || '').trim(), lgBillNumber = String(body.lgBillNumber || '').trim().slice(0, 120);
  if (lgDate && !validLgDate(lgDate)) return res.status(400).json({ success: false, error: 'Enter a valid LG date.' });
  invoice.history = Array.isArray(invoice.history) ? invoice.history : [];
  if (invoice.updatedAt) invoice.history.push({ childBills: invoice.childBills, combined: invoice.combined, updatedAt: invoice.updatedAt, updatedBy: invoice.updatedBy });
  invoice.childBills = childBills; invoice.combined = combined; invoice.lgDate = lgDate; invoice.lgBillNumber = lgBillNumber;
  invoice.updatedAt = new Date().toISOString(); invoice.updatedBy = (req.user || {}).username || 'system';
  saveStore(s);
  res.json({ success: true, invoice });
});
router.post('/api/procurement/combined-invoices/:id/finalize', (req, res) => {
  if (!canReconcileVendorBill(req)) return res.status(403).json({ success: false, error: 'Purchases or accounting access required.' });
  const s = loadStore(), invoice = s.combinedVendorInvoices[req.params.id], basis = String((req.body || {}).basis || '');
  if (!invoice) return res.status(404).json({ success: false, error: 'Invoice calculation not found.' });
  if (invoice.finalized) return res.status(409).json({ success: false, error: 'This LG bill is already finalized.' });
  if (!['purchase', 'vendor'].includes(basis)) return res.status(400).json({ success: false, error: 'Choose Purchase Summary or vendor invoice as the payable amount.' });
  if (!invoice.lgBillNumber || !validLgDate(invoice.lgDate))
    return res.status(400).json({ success: false, error: 'Save the LG bill number and LG date before finalizing.' });
  if (Object.values(s.combinedVendorInvoices).some(other => other.id !== invoice.id && other.finalized && String(other.lgBillNumber).toLowerCase() === invoice.lgBillNumber.toLowerCase()))
    return res.status(409).json({ success: false, error: 'This LG bill number is already finalized.' });
  const amounts = invoiceAmounts(invoice, s.pos, s.settings);
  if (!amounts || amounts.vendorAmountInr == null) return res.status(400).json({ success: false, error: 'Save complete vendor bill values, charges and exchange rate first.' });
  const accountingPath = path.join(DATA_DIR, 'expenses.json');
  let accounting;
  try { accounting = JSON.parse(fs.readFileSync(accountingPath, 'utf8')); }
  catch { return res.status(503).json({ success: false, error: 'Accounting payment history is unavailable; LG bill was not finalized.' }); }
  const paymentsByPo = ((accounting.procurementAccounting || {}).paymentsByPo || {});
  if (invoice.poIds.some(id => ((paymentsByPo[id] || {}).payments || []).length))
    return res.status(409).json({ success: false, error: 'Existing PO payments must be reconciled before finalizing this LG bill.' });
  const amountInr = basis === 'vendor' ? amounts.vendorAmountInr : amounts.purchaseAmountInr;
  if (!(amountInr > 0)) return res.status(400).json({ success: false, error: 'The finalized bill amount must be greater than zero.' });
  invoice.finalized = { basis, amountInr, purchaseAmountInr: amounts.purchaseAmountInr, vendorAmountInr: amounts.vendorAmountInr,
    vendorBillYuan: amounts.vendorBillYuan, vendorTotalYuan: amounts.vendorTotalYuan,
    allocations: allocateAmount(invoice.poIds, amounts.purchaseAmounts, amountInr),
    finalizedAt: new Date().toISOString(), finalizedBy: (req.user || {}).username || 'system' };
  saveStore(s);
  res.json({ success: true, invoice });
});
router.post('/api/procurement/combined-invoices/:id/reopen', (req, res) => {
  if (!canReconcileVendorBill(req)) return res.status(403).json({ success: false, error: 'Purchases or accounting access required.' });
  const s = loadStore(), invoice = s.combinedVendorInvoices[req.params.id];
  if (!invoice) return res.status(404).json({ success: false, error: 'LG bill not found.' });
  if (!invoice.finalized) return res.status(409).json({ success: false, error: 'This LG bill is not finalized.' });
  if (String((req.body || {}).lgBillNumber || '').trim() !== invoice.lgBillNumber)
    return res.status(400).json({ success: false, error: 'LG bill number does not match.' });
  let accounting;
  try { accounting = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'expenses.json'), 'utf8')); }
  catch { return res.status(503).json({ success: false, error: 'Accounting payment history is unavailable; LG bill was not reopened.' }); }
  const paymentsByPo = ((accounting.procurementAccounting || {}).paymentsByPo || {});
  if (invoice.poIds.some(id => ((paymentsByPo[id] || {}).payments || []).length))
    return res.status(409).json({ success: false, error: 'This LG bill has recorded payments and cannot be reopened.' });
  invoice.finalizationHistory = Array.isArray(invoice.finalizationHistory) ? invoice.finalizationHistory : [];
  invoice.finalizationHistory.push({ ...invoice.finalized, reopenedAt: new Date().toISOString(), reopenedBy: (req.user || {}).username || 'system' });
  delete invoice.finalized;
  saveStore(s);
  res.json({ success: true, invoice });
});
router.get('/api/procurement/history', (req, res) => {
  const s = loadStore();
  const finalized = finalizedByPo(s);
  let accounting = null;
  try { accounting = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'expenses.json'), 'utf8')); } catch { /* Unavailable history must not imply unpaid. */ }
  // Stored POs are the purchase record, including early posted purchases.
  // Shopify-recovered placeholders remain excluded because they do not retain
  // the original bill, quantity, weights, or landed-cost calculation.
  const history = Object.values(s.pos)
    .filter(p => !p.historical)
    .map(p => ({ ...publicPo(p, req), paymentSummary: purchasePaymentStatus(p, accounting, canReconcileVendorBill(req), s.settings, finalized[p.id] && finalized[p.id].amount) }))
    .sort((a, b) => String(b.datePurchase || b.createdAt || '').localeCompare(String(a.datePurchase || a.createdAt || '')) || String(b.id).localeCompare(String(a.id)));
  res.json({ success: true, history, combinedInvoices: Object.values(s.combinedVendorInvoices), completePurchases: history.length, recoveredBatches: 0, recoveredProducts: 0 });
});
router.get('/api/procurement/pos/:id', (req, res) => {
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  res.json({ success: true, po: publicPo(po, req) });
});

// ── Purchases summary — what's been bought, by garment type ──────
// Two buckets: DONE (posted → live on Shopify) and PENDING/in-process
// (advance/received/awaiting_approval → money committed, not yet live), plus
// the Casuals "on order" planning quantities folded in as queued (pending).
// Per garment type we roll up pieces + ₹ landed value, drillable to each buy.
const SUMMARY_DONE_STATUSES    = ['posted'];
const SUMMARY_PENDING_STATUSES = ['advance', 'received', 'awaiting_approval'];
// ₹ landed per piece for a PO line, honouring the PO's own rates/origin.
function summaryLinePerPc(po, line, settings) {
  const lineSettings = {
    ...settings,
    exRate:         po.exRate != null ? po.exRate : settings.exRate,
    freightPerGram: po.freightPerGram != null ? po.freightPerGram : settings.freightPerGram
  };
  const totalQty = (po.lines || []).reduce((a, l) => a + num(l.qty), 0);
  const transportPerPc = (po.origin === 'india' && totalQty > 0) ? num(po.transportTotal) / totalQty : 0;
  return landedCost(line, lineSettings, { origin: po.origin, transportPerPc }).landed;
}
router.get('/api/procurement/summary', (req, res) => {
  const s = loadStore();
  const cats = {};
  const catOf = t => (cats[t] || (cats[t] = {
    type: t, done: { pieces: 0, cost: 0 }, pending: { pieces: 0, cost: 0 }, items: []
  }));
  const totals = { done: { pieces: 0, cost: 0 }, pending: { pieces: 0, cost: 0 } };

  // 1) Real purchase orders.
  Object.values(s.pos).forEach(po => {
    let bucket = null;
    if (SUMMARY_DONE_STATUSES.includes(po.status)) bucket = 'done';
    else if (SUMMARY_PENDING_STATUSES.includes(po.status)) bucket = 'pending';
    if (!bucket) return;
    (po.lines || []).forEach(line => {
      const qty = num(line.qty);
      if (qty <= 0) return;
      const perPc = summaryLinePerPc(po, line, s.settings);
      const cost  = Math.round(perPc * qty);
      const c = catOf(line.productType || 'Uncategorised');
      c[bucket].pieces += qty; c[bucket].cost += cost;
      totals[bucket].pieces += qty; totals[bucket].cost += cost;
      c.items.push({
        bucket, source: 'po', poId: po.id, status: po.status,
        line: po.line || '',                 // 'funky' | 'casuals' | '' (unclassified)
        name: line.designName || line.designCode || '(unnamed)',
        code: line.designCode || '',         // vendor product code
        colour: line.colour || '', fit: line.fit || '', size: line.sizeLabel || '',
        chinaSize: line.chinaSize || '',     // vendor (China) size off the bill — shown beside the Indian size
        vendor: line.vendor || po.vendor || '',
        photoUrl: (line.photoUrl || '').trim(),
        date: po.datePurchase || (po.createdAt || '').slice(0, 10),
        dateReceive: po.dateReceive || '',
        qty, cost, perPc: Math.round(perPc)
      });
    });
  });

  // 2) Casuals "on order" — planning-level queued pieces (count as pending).
  try {
    const cz = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'casuals.json'), 'utf8'));
    const czCats = (cz.settings && cz.settings.categories) || {};
    const norm = { 'T-shirt': 'T-Shirt', 'Shirt': 'Shirt', 'Trouser': 'Trouser' };
    Object.keys(czCats).forEach(catKey => {
      const cc = czCats[catKey] || {};
      const onOrder = cc.onOrder || {}, onCost = cc.onOrderCost || {};
      const type = norm[catKey] || catKey;
      Object.keys(onOrder).forEach(key => {
        const qty = num(onOrder[key]);
        if (qty <= 0) return;
        const perPc = num(onCost[key]) || num(cc.avgCost) || 0;
        const cost  = Math.round(perPc * qty);
        const c = catOf(type);
        c.pending.pieces += qty; c.pending.cost += cost;
        totals.pending.pieces += qty; totals.pending.cost += cost;
        c.items.push({
          bucket: 'pending', source: 'casuals', poId: null, status: 'queued',
          line: 'casuals', name: 'Casuals queue', code: '', colour: '', fit: String(key).replace('::', ' · '), size: '',
          vendor: '', photoUrl: '', date: '', dateReceive: '', qty, cost, perPc: Math.round(perPc)
        });
      });
    });
  } catch { /* no casuals store yet — skip */ }

  const categories = Object.values(cats).map(c => {
    c.total = { pieces: c.done.pieces + c.pending.pieces, cost: c.done.cost + c.pending.cost };
    c.items.sort((a, b) => (a.bucket === b.bucket ? b.qty - a.qty : (a.bucket === 'done' ? -1 : 1)));
    return c;
  }).sort((a, b) => b.total.pieces - a.total.pieces);

  // Same rows, re-grouped by VENDOR (parallel view to the by-category one).
  const vends = {};
  const vendOf = v => (vends[v] || (vends[v] = {
    type: v, done: { pieces: 0, cost: 0 }, pending: { pieces: 0, cost: 0 }, items: []
  }));
  categories.forEach(c => (c.items || []).forEach(it => {
    const v = vendOf(it.vendor && String(it.vendor).trim() ? it.vendor : 'Unassigned');
    const b = it.bucket === 'done' ? 'done' : 'pending';
    v[b].pieces += it.qty; v[b].cost += it.cost;
    v.items.push({ ...it, category: c.type });
  }));
  const vendors = Object.values(vends).map(v => {
    v.total = { pieces: v.done.pieces + v.pending.pieces, cost: v.done.cost + v.pending.cost };
    v.items.sort((a, b) => (a.bucket === b.bucket ? b.qty - a.qty : (a.bucket === 'done' ? -1 : 1)));
    return v;
  }).sort((a, b) => b.total.pieces - a.total.pieces);

  res.json({ success: true, totals, categories, vendors, generatedAt: new Date().toISOString() });
});

module.exports = { router, genSeo, buildSku, rebuildLineSku, landedCost, parseSerial, nextSerial, canManagePurchases, canStartPaidPilot, parseLocalInvoiceText, retireAudienceModelImages };
