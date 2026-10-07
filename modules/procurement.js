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
const purchaseCosts = require('../public/purchase-costs');
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
const { buildRecoveryPlan, publicRecoveryPlan } = require('./procurement-shopify-recovery');

const router = express.Router();
router.use('/api/procurement/pos/:id', (req,res,next)=>{
  if(req.method==='POST' && paidPilotInFlight.has(req.params.id) &&
    /^\/(?:image-styling|back-ref|line-photo|line-edits|group-audience|receive|receipt-|discard-|split-|merge-|images|qa-review|codex-batch|mark-received)/.test(req.path))
    return res.status(409).json({success:false,error:'Image generation is running for this PO. Stop it or wait for completion before changing its product details or reviewing images.'});
  next();
});


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
    'Shirt': 1, 'T-Shirt': 2, 'T-Shirt Hood': 21, 'Jeans': 10, 'Trouser': 11, 'Lower': 12,
    'Shorts': 13, 'Jogger': 14, 'Coord Set': 15, 'Jorts': 16, 'Sando': 17,
    'Bag': 18, 'Denim Joggers': 19, 'Top': 20, 'Perfumes': 22, 'Belts': 23
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
  try { fs.writeFileSync(tmp, data); fs.renameSync(tmp, fp); }
  finally { try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch {} }
}
const storeSnapshots = new WeakMap();
function loadStore() {
  let s;
  try { s = JSON.parse(fs.readFileSync(STORE_PATH, 'utf8')); } catch { s = {}; }
  if (!s.brand)    s.brand = SEED.brand;
  if (!s.products) s.products = { ...SEED.products };
  else s.products = { ...SEED.products, ...s.products };
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
  let repairedPurchaseMetadata = false;
  Object.values(s.pos).forEach(po => {
    if (!po) return;
    (po.lines || []).forEach(line => {
      // Raw references are permanent purchase records. `photoUrl` remains the
      // working source used by the studio; `rawPhotoUrl` is the recovery copy
      // that cannot be lost during recalculation, line edits or regeneration.
      if (!line.rawPhotoUrl && line.photoUrl) { line.rawPhotoUrl = line.photoUrl; repairedPurchaseMetadata = true; }
      if (!line.photoUrl && line.rawPhotoUrl) { line.photoUrl = line.rawPhotoUrl; repairedPurchaseMetadata = true; }
      if (!line.designId) {
        line.designId = ((line.designCode || '').trim() || stripSizeSuffix(line.designName || '') || crypto.randomUUID()).toUpperCase();
        repairedPurchaseMetadata = true;
      }
      if (!line.designOverrides || typeof line.designOverrides !== 'object' || Array.isArray(line.designOverrides)) {
        line.designOverrides = {};
        repairedPurchaseMetadata = true;
      }
      if (po.status === 'posted' || po.status === 'posting_partial') return;
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
  if (repairedInvalidSerials || repairedPurchaseMetadata) atomicWrite(STORE_PATH, JSON.stringify(s));
  storeSnapshots.set(s, JSON.parse(JSON.stringify(s)));
  return s;
}
function reclaimRejectedPhotoStorage(s) {
  const protectedFiles=new Set(),add=url=>{const name=path.basename(String(url||''));if(name)protectedFiles.add(name);};
  for(const po of Object.values(s.pos||{})){
    for(const images of Object.values(po.aiImages||{}))for(const image of images||[])add(image&&image.url);
    for(const url of Object.values(po.backRefs||{}))add(url);
    for(const line of po.lines||[]){add(line&&line.photoUrl);add(line&&line.rawPhotoUrl);}
    for(const image of po.imageRejectionHistory||[])add(image&&image.url);
    for(const record of po.referenceImageHistory||[])for(const image of record.images||[])add(image&&image.url);
  }
  let removed=0,freed=0;
  for(const po of Object.values(s.pos||{}))for(const rejected of Object.values(po.qaRejected||{})){
    const latestUnresolvedByType={};
    for(const candidate of rejected||[])if(candidate&&candidate.url&&!candidate.supersededBy)latestUnresolvedByType[candidate.type||'generated']=candidate;
    for(const candidate of rejected||[]){
      if(!candidate||!candidate.url||latestUnresolvedByType[candidate.type||'generated']===candidate)continue;
      const name=path.basename(candidate.url),fp=path.join(PHOTO_DIR,name);
      if(!protectedFiles.has(name))try{const size=fs.statSync(fp).size;fs.unlinkSync(fp);removed++;freed+=size;}catch{}
      candidate.fileArchivedAt=new Date().toISOString();delete candidate.url;
    }
  }
  return {removed,freed};
}
function saveStore(s) {
  const baseline = storeSnapshots.get(s);
  const next = baseline ? loadStore() : s;
  const equal = (a,b) => JSON.stringify(a) === JSON.stringify(b);
  if (baseline) for (const key of new Set([...Object.keys(baseline), ...Object.keys(s)])) {
    if (equal(baseline[key], s[key])) continue;
    // Merge independent POs/settings after an await. A changed record itself
    // must be reloaded rather than overwriting a newer quantity or approval.
    if (['pos','settings','combinedVendorInvoices'].includes(key)) {
      next[key] = next[key] || {};
      for (const id of new Set([...Object.keys(baseline[key] || {}), ...Object.keys(s[key] || {})])) {
        const before = (baseline[key] || {})[id], after = (s[key] || {})[id];
        if (equal(before, after)) continue;
        if (!equal(next[key][id], before) && !equal(next[key][id], after))
          throw new Error('Purchase data changed while saving ('+key+': '+id+'). Reopen the PO and review the latest data.');
        if (after === undefined) delete next[key][id]; else next[key][id] = after;
      }
    } else {
      if (!equal(next[key], baseline[key]) && !equal(next[key], s[key]))
        throw new Error('Purchase data changed while saving ('+key+'). Reload before retrying.');
      if (s[key] === undefined) delete next[key]; else next[key] = s[key];
    }
  }
  try { atomicWrite(STORE_PATH, JSON.stringify(next)); }
  catch(error){
    if(error&&error.code==='ENOSPC'){
      reclaimRejectedPhotoStorage(next);
      atomicWrite(STORE_PATH, JSON.stringify(next));
    } else throw error;
  }
  storeSnapshots.set(s, JSON.parse(JSON.stringify(s)));
}

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
  // The owner requested recovery of the Shopify-backed purchase trail from
  // August 2026 onward. Shopify can prove the product, SKU, vendor label and
  // creation date, but it cannot recreate the supplier bill or landed cost.
  let url = `https://${SHOPIFY_STORE}/admin/api/${API}/products.json?limit=250&created_at_min=2026-08-01T00:00:00%2B05:30&fields=id,title,created_at,vendor,product_type,status,variants,images,image`;
  const products = [];
  while (url) {
    const r = await shopifyClient.request(url);
    if (!r.ok) { const b = await r.text().catch(() => ''); throw new Error('Shopify ' + r.status + ': ' + b.slice(0, 200)); }
    const d = await r.json();
    (d.products || []).forEach(p => {
      // Historical SKUs include formats that pre-date today's strict parser;
      // requiring parseSerial() here would silently erase legitimate old buys.
      const variantDetails = (p.variants || []).map(v => ({
        sku: String(v.sku || '').toUpperCase(),
        inventoryItemId: String(v.inventory_item_id || ''),
        sellingPrice: v.price == null || v.price === '' ? null : Number(v.price),
        historicalReceivedQuantity: null,
        grams: Number(v.grams) || 0,
        weight: Number(v.weight) || 0,
        weightUnit: String(v.weight_unit || '')
      })).filter(v => v.sku);
      const skus = variantDetails.map(v => v.sku);
      products.push({
        productId: String(p.id), title: p.title || '(untitled)', type: p.product_type || '',
        vendor: p.vendor || '', status: p.status || '', createdAt: p.created_at || '', skus,
        imageUrl: String((p.image && p.image.src) || (p.images && p.images[0] && p.images[0].src) || ''),
        variantDetails
      });
    });
    const link = r.headers.get('Link') || '';
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  // Shopify stores the merchant-entered per-item cost on InventoryItem, not
  // ProductVariant. Fetch it separately and keep failures non-fatal: selling
  // price and weight are still useful recovery evidence without this scope.
  const inventoryIds = Array.from(new Set(products.flatMap(p => p.variantDetails.map(v => v.inventoryItemId)).filter(Boolean)));
  const costByInventoryId = {};
  for (let offset = 0; offset < inventoryIds.length; offset += 100) {
    const ids = inventoryIds.slice(offset, offset + 100);
    try {
      const r = await shopifyClient.request(`https://${SHOPIFY_STORE}/admin/api/${API}/inventory_items.json?ids=${ids.join(',')}`);
      if (!r.ok) continue;
      const d = await r.json();
      (d.inventory_items || []).forEach(item => {
        if (item && item.id != null && item.cost != null && item.cost !== '') costByInventoryId[String(item.id)] = Number(item.cost);
      });
    } catch { /* Cost recovery is optional; never hide the product history. */ }
  }
  products.forEach(product => product.variantDetails.forEach(variant => {
    variant.recordedCost = Object.prototype.hasOwnProperty.call(costByInventoryId, variant.inventoryItemId)
      ? costByInventoryId[variant.inventoryItemId] : null;
  }));
  // A single Shopify creation date can contain products from several sourcing
  // vendors. Keep those as separate historical purchase rows so the vendor
  // column represents one supplier instead of an amalgamated batch.
  const byDateAndVendor = {};
  products.forEach(p => {
    const date = String(p.createdAt).slice(0, 10);
    const vendor = String(p.vendor || '').trim() || 'Vendor not recorded';
    const key = date + '\u0000' + vendor;
    if (date) (byDateAndVendor[key] || (byDateAndVendor[key] = { date, vendor, products: [] })).products.push(p);
  });
  const rows = Object.values(byDateAndVendor)
    .sort((a, b) => b.date.localeCompare(a.date) || a.vendor.localeCompare(b.vendor))
    .map(group => {
      const vendorKey = encodeURIComponent(group.vendor.toUpperCase()) || 'UNKNOWN';
      return {
        id: 'HIST-' + group.date.replace(/-/g, '') + '-' + vendorKey,
        historical: true, source: 'shopify-recovery', status: 'posted',
        manualVendorBill: group.date.startsWith('2026-08-'),
        datePurchase: group.date, createdAt: group.date + 'T00:00:00.000Z',
        vendor: group.vendor, vendorNames: [group.vendor], billNo: '', products: group.products,
        productCount: group.products.length,
        skuCount: group.products.reduce((n, p) => n + p.skus.length, 0),
        historicalPieces: group.products.reduce((total, p) => total + p.variantDetails.reduce((n, v) =>
          n + (Number.isFinite(v.historicalReceivedQuantity) ? v.historicalReceivedQuantity : 0), 0), 0),
        historicalPiecesKnown: group.products.every(p => p.variantDetails.every(v => Number.isFinite(v.historicalReceivedQuantity))),
        quantityKnown: false, valueKnown: false
      };
    });
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
  const calculatedMrp = charmPrice(mrpRaw);
  const manualMrp = num(line.manualMrp) > 0 ? Math.round(num(line.manualMrp)) : 0;
  return {
    inrValue: round2(inrValue),
    freightPerPc: round2(freightPerPc),
    landed: round2(landed),
    calculatedMrp,
    suggestedMrp: manualMrp || calculatedMrp,
    mrpOverridden: !!manualMrp
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

// One automatic retail price per vendor/design/category, across colour and size.
// Received variants determine the price; missing variants cannot inflate it.
function uniformDesignMrps(lines) {
  const keyOf = (line, index) => JSON.stringify([
    String(line.vendor || '').trim().toLowerCase(), String(line.productType || '').trim().toLowerCase(),
    String(line.designCode || stripSizeSuffix(line.designName || '') || ('unnamed-row-' + index)).trim().toLowerCase()
  ]);
  const maxima = new Map();
  lines.forEach((line, index) => {
    if (num(line.qty) > 0) maxima.set(keyOf(line, index), Math.max(maxima.get(keyOf(line, index)) || 0, line.calculatedMrp));
  });
  return lines.map((line, index) => {
    const calculatedMrp = maxima.get(keyOf(line, index)) ?? line.calculatedMrp;
    return {...line, variantCalculatedMrp:line.calculatedMrp, calculatedMrp,
      suggestedMrp:num(line.manualMrp) > 0 ? Math.round(num(line.manualMrp)) : calculatedMrp};
  });
}

function pricedLinesForPo(store, po) {
  const settings = {...store.settings};
  if (po.exRate != null && po.exRate !== '') settings.exRate = num(po.exRate);
  if (po.freightPerGram != null && po.freightPerGram !== '') settings.freightPerGram = num(po.freightPerGram);
  const totalQty = (po.lines || []).reduce((sum, line) => sum + num(line.qty), 0);
  return uniformDesignMrps((po.lines || []).map(raw => {
    const line = normalizeLine(raw, po);
    return {...line, ...landedCost(line, settings, {origin:po.origin, transportPerPc:totalQty ? num(po.transportTotal) / totalQty : 0})};
  }));
}

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
  const productType = audience === 'Women' && !winter && /^t[ -]?shirt(?: hood)?$/i.test(g.productType || '')
    ? (/ hood$/i.test(g.productType || '') ? 'Hooded Top' : 'Top') // Basic copy cannot verify a polo collar; only photo-based AI copy may say that.
    : (g.productType || '');
  const colour      = titleCase(g.colour || '');
  const fit         = openaiPilot.productProfile(g).productOnly || audience === 'Women' && !winter && /\bmuscle\s*fit\b/i.test(g.fit || '') ? '' : titleCase(g.fit || '');
  const fitBase     = fit.replace(/\s*fit$/i, '').trim();   // strip trailing "Fit" so we never double it
  const sizeList    = (g.sizeLabels || []).map(l => (g.sizeCodeOf ? g.sizeCodeOf(l) : l)).join(', ');
  const nm          = nameForTitle ? nameForTitle + ' ' : '';

  // Customer-facing product title (the H1 / storefront name).
  const titleCore   = [nameForTitle, productType].filter(Boolean).join(' ').trim() || productType;
  const audienceSuffix = audience === 'Women' ? 'for Women' : audience === 'Men' ? 'for Men' : 'Unisex';
  const title       = ['SANKI', colour, fitBase ? fitBase + ' Fit' : '', titleCore, audienceSuffix].filter(Boolean).join(' ');

  // URL handle: clean, keyword-rich. Always fold in the design code (when
  // present) so two same-named products can never collide on the same URL.
  const handleCode = (designCode && designCode.toUpperCase() !== nameForTitle.toUpperCase()) ? designCode : '';
  const handle = slugify([nameForTitle, productType, colour, fitBase, handleCode].filter(Boolean).join(' '))
    || slugify([productType, colour].filter(Boolean).join(' '));

  // SEO <title> (global.title_tag) — keep ~60 chars, brand at the end.
  const metaTitle = truncate(
    `${[colour, fitBase ? fitBase + ' Fit' : '', nameForTitle, productType, audienceSuffix].filter(Boolean).join(' ')} | SANKI`.replace(/\s+/g, ' ').trim(),
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

function normalizeSeoStyle(value, group = {}) {
  let out = stripInternalCodes(String(value || ''), group.designCode)
    .replace(/\bSANKI\b/ig, ' ').replace(/\bfor\s+(?:women|men)\b/ig, ' ')
    .replace(/\b(?:women|men)'?s\b/ig, ' ').replace(/\bV[\s-]?neck(?:ed)?\b/ig, 'V-Neck')
    .replace(/\b(?:with\s+)?button(?:ed)?[\s-]*(?:placket|trim|details?)\b/ig, 'Button-Detail');
  [group.colour, group.fit].filter(Boolean).forEach(part => {
    const esc = String(part).replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\\ /g, '\\s+');
    out = out.replace(new RegExp('\\b' + esc + '\\b', 'ig'), ' ');
  });
  const productType = String(group.productType || '').trim();
  if (productType) {
    const esc = productType.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\\ /g, '\\s+');
    out = out.replace(new RegExp('\\b' + esc + 's?\\b', 'ig'), ' ');
  }
  if (String(group.audience || '').toLowerCase() === 'women') out = out.replace(/\b(?:t[ -]?shirts?|tops?)\b/ig, ' ');
  out = stripSizeSuffix(out.replace(/[—–|,]/g, ' ').replace(/\s+/g, ' ').trim())
    .replace(/\b(?:FS|XXXS|XXS|XS|S|M|L|XL|XXL|XXXL|[2-6]XL|\d{2})\b$/i, '').replace(/\s+/g, ' ').trim();
  return titleCase(out).replace(/\bV-neck(?:ed)?\b/ig, 'V-Neck').replace(/\bButton-detail\b/ig, 'Button-Detail');
}

function canonicalSeoNaming(seo, group, preferredStyle = '') {
  const audience = group.audience || 'Unisex';
  const winter = /^winter$/i.test(group.season || '') || /^(hoodie|sweatshirt|sweater|cardigan|pullover|jacket|coat)$/i.test(group.productType || '');
  const productType = audience === 'Women' && !winter && /^t[ -]?shirt(?: hood)?$/i.test(group.productType || '') ? (/ hood$/i.test(group.productType || '') ? 'Hooded Top' : 'Top') : titleCase(group.productType || 'Product');
  const colour = titleCase(group.colour || '');
  const fit = openaiPilot.productProfile(group).productOnly || audience === 'Women' && !winter && /\bmuscle\s*fit\b/i.test(group.fit || '') ? '' : titleCase(group.fit || '').replace(/\s*fit$/i, '').trim();
  const style = normalizeSeoStyle(preferredStyle, group) || normalizeSeoStyle(seo.displayName || seo.title, group) || normalizeSeoStyle(group.designName, group);
  const audienceSuffix = audience === 'Women' ? 'for Women' : audience === 'Men' ? 'for Men' : 'Unisex';
  const descriptiveType = [style, productType].filter(Boolean).join(' ').replace(/\b(Top|T-Shirt|Shirt|Trouser|Jeans)\s+\1\b/ig, '$1');
  const title = ['SANKI', colour, fit ? fit + ' Fit' : '', descriptiveType, audienceSuffix].filter(Boolean).join(' ').replace(/\s+/g, ' ').trim();
  const metaTitle = truncate([colour, fit ? fit + ' Fit' : '', descriptiveType, audienceSuffix, '| SANKI'].filter(Boolean).join(' ').replace(/\s+/g, ' ').trim(), 60);
  return { ...seo, displayName: descriptiveType, title, metaTitle, styleDescriptor: style };
}

function siblingSeoStyle(po, group) {
  const code = String(group.designCode || '').trim().toLowerCase();
  if (!code) return '';
  const sibling = (po.seoDraft || []).find(d => d.key !== group.key && String(d.designCode || '').trim().toLowerCase() === code && d.seo);
  return sibling ? (sibling.styleDescriptor || normalizeSeoStyle(sibling.seo.displayName || sibling.seo.title, { ...group, colour: sibling.colour })) : '';
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
const ORDERED_FIELDS = ['qty', 'colour', 'productType', 'sizeLabel', 'chinaSize', 'designName', 'designCode', 'sku', 'fit', 'audience', 'perPcsYuan'];
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

// Paid studio work is stored against the editable design/colour key. Keep it
// safe when purchase details are corrected: a one-to-one rename may move the
// bundle, but a split/merge is ambiguous and must never duplicate photographs
// onto another article. Ambiguous bundles remain available in recovery
// history with their source photographs and original key.
function reconcileStudioKeysAfterLineEdit(po, beforeLines, afterLines) {
  const beforeBySku = new Map((beforeLines || []).filter(l => l && l.sku).map(l => [String(l.sku), l]));
  const transitions = new Map();
  (afterLines || []).forEach((line, index) => {
    const previous = (line && line.sku && beforeBySku.get(String(line.sku))) || (beforeLines || [])[index];
    if (!previous) return;
    const oldKey = groupKey(previous), nextKey = groupKey(line);
    if (oldKey === nextKey) return;
    if (!transitions.has(oldKey)) transitions.set(oldKey, new Set());
    transitions.get(oldKey).add(nextKey);
  });
  const activeKeys = new Set((afterLines || []).map(groupKey));
  const maps = ['aiImages', 'imageStyling', 'backRefs', 'qaRejected'];
  po.seoDraft = Array.isArray(po.seoDraft) ? po.seoDraft : [];
  let changed = false;
  for (const [oldKey, targets] of transitions) {
    if (activeKeys.has(oldKey)) continue;
    const targetList = [...targets].filter(Boolean);
    const hasBundle = maps.some(name => po[name] && po[name][oldKey] != null) || po.seoDraft.some(d => d.key === oldKey);
    if (!hasBundle) continue;
    if (targetList.length === 1) {
      const nextKey = targetList[0];
      for (const name of maps) {
        if (!po[name] || po[name][oldKey] == null || po[name][nextKey] != null) continue;
        po[name][nextKey] = po[name][oldKey];
        delete po[name][oldKey];
      }
      const oldSeo = po.seoDraft.find(d => d.key === oldKey);
      if (oldSeo && !po.seoDraft.some(d => d.key === nextKey)) oldSeo.key = nextKey;
      changed = true;
      continue;
    }
    po.orphanedStudioDrafts = Array.isArray(po.orphanedStudioDrafts) ? po.orphanedStudioDrafts : [];
    if (!po.orphanedStudioDrafts.some(x => x.key === oldKey)) {
      po.orphanedStudioDrafts.push({
        key: oldKey, targetKeys: targetList, preservedAt: new Date().toISOString(),
        sourcePhotos: (beforeLines || []).filter(l => groupKey(l) === oldKey).map(l => l.photoUrl).filter(Boolean),
        images: (po.aiImages || {})[oldKey] || [], styling: (po.imageStyling || {})[oldKey] || null,
        backRef: (po.backRefs || {})[oldKey] || '', rejected: (po.qaRejected || {})[oldKey] || [],
        seo: po.seoDraft.find(d => d.key === oldKey) || null
      });
      changed = true;
    }
  }
  return changed;
}

// Normalize a raw intake line into a clean, storable shape. Weight is optional
// at the ADVANCE stage (product not yet received / weighed).
function normalizeLine(raw, body) {
  const rawPhotoUrl = (raw.rawPhotoUrl || raw.photoUrl || '').trim();
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
    photoUrl:    (raw.photoUrl || rawPhotoUrl).trim(), // working studio source
    rawPhotoUrl,                                      // permanent purchase reference
    designId: String(raw.designId || raw.designCode || stripSizeSuffix(raw.designName || '') || crypto.randomUUID()).trim().toUpperCase(),
    designOverrides: raw.designOverrides && typeof raw.designOverrides === 'object' && !Array.isArray(raw.designOverrides) ? { ...raw.designOverrides } : {},
    sku:         (raw.sku || '').toUpperCase().trim(),
    qty:         Math.max(0, Math.round(num(raw.qty))),
    perPcsYuan:  num(raw.perPcsYuan),
    weightGrams: num(raw.weightGrams),              // 0 until received & weighed
    manualMrp: num(raw.manualMrp) > 0 ? Math.round(num(raw.manualMrp)) : 0
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
  return computePreviewWithCatalogue(store, body, await loadCatalogue(!!body.refresh));
}
function computePreviewWithCatalogue(store, body, cat) {
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

  const sizeCodeOf = (label) => store.sizes[label] || label;

  // Serial cursor starts from the live Shopify max, but also clears any serials
  // already reserved by pending advance POs so two advance purchases can never
  // collide. Lines that already carry a SKU (from the advance stage) keep it.
  let cursor = cat.maxSerial ? { ...cat.maxSerial } : null;
  const pend = pendingSerialMax(store);
  if (pend && serialGt(pend, cursor)) cursor = { ...pend };

  const lines = uniformDesignMrps((body.lines || []).map(raw => {
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
  }));

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
async function shopifyPost(pathUrl, payload, options = {}) {
  const r = await shopifyClient.request(`https://${SHOPIFY_STORE}/admin/api/${API}/${pathUrl}`, {
    method: 'POST',
    body: JSON.stringify(payload), ...options
  });
  const text = await r.text();
  let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }
  if (!r.ok) { const e = new Error('Shopify ' + r.status + ': ' + JSON.stringify(json.errors || json).slice(0, 300)); e.status = r.status; throw e; }
  return json;
}

async function createDraftProduct(np, warehouseLocationId, recoveryOptions = {}) {
  const imgs = Array.isArray(np.images) ? np.images : [];
  if (!imgs.length) throw new Error('No readable approved listing photos were supplied. Shopify product creation was cancelled.');
  const attachments = await Promise.all(imgs.map(async im => {
    const src = await readListingPhoto(im.url);
    if (!src) throw new Error('Approved listing photo is no longer readable: ' + String(im.url || '(missing URL)') + '. Shopify product creation was cancelled.');
    return { attachment: src.buf.toString('base64'), alt: (im.alt || np.seo.imageAlt || '').slice(0, 512) };
  }));
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
  payload.product.images = attachments;
  if (recoveryOptions.poId) assertNoImageGeneration(loadStore().pos[recoveryOptions.poId], 'posting to Shopify');
  if (recoveryOptions.beforeCreate) recoveryOptions.beforeCreate();
  const created = await shopifyPost('products.json', payload, recoveryOptions.noCreateRetries ? {maxRetries:0} : {}).then(d => d.product);
  const uploadedCount = Array.isArray(created.images) ? created.images.length : 0;
  if (recoveryOptions.onCreated) recoveryOptions.onCreated({productId:String(created.id), handle:created.handle,
    title:created.title, imagesUploaded:uploadedCount, variants:[]});
  if (uploadedCount < attachments.length) throw new Error('Shopify created the product but confirmed only ' + uploadedCount + ' of ' + attachments.length + ' listing photos. Reconcile this product before continuing.');

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
  return { productId: String(created.id), handle: created.handle, title: created.title, imagesUploaded: uploadedCount, variants: stocked };
}

async function addExistingInventory(ea, warehouseLocationId) {
  if (!ea.inventoryItemId) return { sku: ea.sku, error: 'No inventory_item_id' };
  if (!warehouseLocationId) return { sku: ea.sku, error: 'Warehouse location not set' };
  const d = await shopifyPost('inventory_levels/adjust.json', {
    location_id: Number(warehouseLocationId),
    inventory_item_id: Number(ea.inventoryItemId),
    available_adjustment: Number(ea.qty)
  }, { maxRetries: 0 }); // An uncertain increment must never be sent twice.
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
    (po.imageRejectionHistory || []).forEach(x => add(x && x.url));
    (po.newProducts || []).forEach(product => (product.images || []).forEach(image => add(image && image.url)));
    Object.values(po.backRefs || {}).forEach(add);
    (po.lines || []).forEach(l => { add(l && l.photoUrl); add(l && l.rawPhotoUrl); });
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
  const s=loadStore(),r=reclaimRejectedPhotoStorage(s);
  if(r.removed){saveStore(s);console.log('[procurement] archived '+r.removed+' older rejected photo file(s), freed '+(r.freed/1048576).toFixed(2)+' MB');}
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
router.post('/api/procurement/pos/:id/back-ref', async (req, res) => {
  try {
  if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
  const s = loadStore();
  const po = s.pos[req.params.id];
  if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
  const b = req.body || {};
  if (!b.groupKey) return res.status(400).json({ success: false, error: 'groupKey required.' });
  if (isLockedPo(po)) return res.status(409).json({ success: false, error: 'Posted purchases cannot change image references.' });
  if (!(po.lines||[]).some(line=>groupKey(line)===b.groupKey)) return res.status(404).json({ success:false,error:'Product group not found.' });
  if (b.url && (!String(b.url).startsWith('/api/procurement/photo/') || !readStoredPhoto(b.url))) return res.status(400).json({ success:false,error:'Upload a readable back photo first.' });
  const snapshot=JSON.stringify(po),group=(await newGroupsOf(s,po)).find(group=>group.key===b.groupKey);
  if(!group)return res.status(404).json({success:false,error:'Product group not found.'});
  if(JSON.stringify(loadStore().pos[req.params.id])!==snapshot||paidPilotInFlight.has(req.params.id))return res.status(409).json({success:false,error:'The PO changed while saving its back reference. Reopen it and try again.'});
  po.backRefs = po.backRefs || {};
  if (po.backRefs[b.groupKey] !== (b.url || '')) {
    const before=codexBatch.fingerprint(group,po.backRefs[b.groupKey]),after=codexBatch.fingerprint(group,b.url||'');
    po.aiImages=po.aiImages||{};po.qaRejected=po.qaRejected||{};
    const active=po.aiImages[b.groupKey]||[],held=po.qaRejected[b.groupKey]||[];
    const backImages=active.concat(held).filter(image=>image.type==='back');
    if(backImages.length){po.referenceImageHistory=po.referenceImageHistory||[];po.referenceImageHistory.push({groupKey:b.groupKey,at:new Date().toISOString(),reason:'back-reference-changed',images:backImages});}
    po.aiImages[b.groupKey]=active.filter(image=>image.type!=='back');
    po.qaRejected[b.groupKey]=held.filter(image=>image.type!=='back');
    for(const image of po.aiImages[b.groupKey].concat(po.qaRejected[b.groupKey]))if(image.sourceFingerprint===before)image.sourceFingerprint=after;
  }
  if (b.url) po.backRefs[b.groupKey] = String(b.url);
  else delete po.backRefs[b.groupKey];
  saveStore(s);
  res.json({ success: true, groupKey: b.groupKey, url: po.backRefs[b.groupKey] || '' });
  }catch(error){res.status(409).json({success:false,error:error.message});}
});

// ── AI image helpers (Gemini) ────────────────────────────────────
// Persist a generated image buffer to the photo volume, return its URL.
function savePhotoBuffer(buf, ext) {
  const name = Date.now() + '-' + crypto.randomBytes(6).toString('hex') + (ext || '.jpg');
  try { fs.writeFileSync(path.join(PHOTO_DIR, name), buf); }
  catch(error){
    if(!error||error.code!=='ENOSPC')throw error;
    const store=loadStore();reclaimRejectedPhotoStorage(store);saveStore(store);
    fs.writeFileSync(path.join(PHOTO_DIR, name), buf);
  }
  return { file: name, url: '/api/procurement/photo/' + name };
}
// Read a stored /api/procurement/photo/<file> URL back into a buffer + mime.
function readStoredPhoto(url) {
  if(typeof url!=='string'||!url.startsWith('/api/procurement/photo/'))return null;
  const name = path.basename(String(url || ''));
  const fp = path.join(PHOTO_DIR, name);
  if (!name || !fp.startsWith(PHOTO_DIR) || !fs.existsSync(fp) || !fs.statSync(fp).isFile()) return null;
  const ext = path.extname(name).toLowerCase();
  const mime = ext === '.png' ? 'image/png' : ext === '.webp' ? 'image/webp' : 'image/jpeg';
  return { buf: fs.readFileSync(fp), mime };
}
// Existence is insufficient: a truncated/empty file must never pass approval
// or reach Shopify. Decode the actual bytes; conversion leaves originals intact.
async function readListingPhoto(url) {
  const source = readStoredPhoto(url);
  if (!source) return null;
  try { return await require('./procurement-image-source').normalizeSource(source); }
  catch {
    const error = new Error('Saved listing photo is damaged or cannot be decoded: ' + String(url) + '. Restore or replace this image before approving or posting.');
    error.status = 409;
    throw error;
  }
}
async function validateListingPhotos(images) {
  for (const image of images) if (!await readListingPhoto(image.url)) {
    const error = new Error('Approved listing photo is no longer readable: ' + String(image.url) + '. Restore the saved image before posting.');
    error.status = 409;
    throw error;
  }
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
    await worker.setParameters({ tessedit_pageseg_mode: '6', preserve_interword_spaces: '1' });
    const result = await worker.recognize(buffer);
    return String(result && result.data && result.data.text || '');
  });
  invoiceOcrQueue = job.catch(() => {});
  return job;
}
function normalInvoiceText(value) {
  let text = String(value || '').normalize('NFKC').replace(/\r/g, '').replace(/[，]/g, ',').replace(/[：]/g, ':');
  // Chinese Tesseract often inserts a space between every Han character
  // ("单 价", "咖 啡 色").  Those spaces prevent every label/colour rule
  // below from matching.  Remove only CJK-to-CJK spaces; Latin words and the
  // numeric table columns keep their separators.
  let previous;
  do {
    previous = text;
    text = text.replace(/([\u3400-\u9fff])[ \t]+(?=[\u3400-\u9fff])/g, '$1');
  } while (text !== previous);
  return text;
}
function localInvoiceDate(text) {
  const raw = normalInvoiceText(text);
  const m = raw.match(/(?:20\d{2})[年\/.-]\s*\d{1,2}[月\/.-]\s*\d{1,2}日?/) ||
            raw.match(/(?:日期|时间|date|time)\s*[:：-]?\s*(\d{2})[\/.-]\s*(\d{1,2})[\/.-]\s*(\d{1,2})/i) ||
            raw.match(/\d{1,2}[\/.-]\s*\d{1,2}[\/.-]\s*(?:20)?\d{2}/);
  if (!m) return '';
  const nums = m[0].match(/\d+/g).map(Number);
  let y, month, day;
  if (nums[0] > 1900) [y, month, day] = nums;
  else if (/(?:日期|时间|date|time)/i.test(m[0]) && nums[0] < 100) { [y, month, day] = nums; y += 2000; }
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
    ['Belts', /\bbelts?\b|腰带|皮带/i],
    ['Perfumes', /\bperfumes?\b|\bcologne\b|\beau\s+de\s+(?:parfum|toilette)\b|香水/i],
    ['Denim Joggers', /denim\s*jogger|牛仔束脚/i], ['Coord Set', /coord|co-ord|套装/i],
    ['T-Shirt Hood', /\bt[\s-]?shirt\s*hood(?:ed)?\b|\bhooded\s*(?:t[\s-]?shirt|tee)\b|连帽\s*(?:T恤|短袖)/i],
    ['T-Shirt', /t[\s-]?shirt|tee\b|polo|T恤|短袖/i], ['Shirt', /\bshirt\b|衬衫/i],
    ['Top', /\btop\b|上衣|女上装|针织衫/i],
    ['Jeans', /\bjeans?\b|牛仔裤/i], ['Trouser', /trouser|pants?|长裤|裤子|西裤|阔腿裤/i],
    ['Jogger', /jogger|束脚裤/i], ['Shorts', /shorts?|短裤/i], ['Jorts', /jorts?/i],
    ['Sando', /sando|背心/i], ['Lower', /lower/i], ['Bag', /\bbag\b|包/i]
  ];
  const hit = rules.find(r => r[1].test(line) && products.includes(r[0]));
  return hit ? hit[0] : '';
}
function localInvoiceColour(line, colours) {
  const rules = [
    ['Sky Blue', /sky\s*blue|天蓝|浅蓝/i], ['Blue', /navy|blue|蓝|藏青|宝蓝/i], ['Black', /black|黑/i],
    ['White', /white|白/i], ['Brown', /brown|coffee|咖啡|棕|褐/i], ['Cream', /cream|off[ -]?white|米白|奶油/i],
    ['Green', /green|绿/i], ['Grey', /gr[ae]y|灰/i], ['Maroon', /maroon|酒红/i], ['Orange', /orange|橙|桔/i],
    ['Pink', /pink|粉/i], ['Purple', /purple|紫/i], ['Red', /red|红/i], ['Yellow', /yellow|黄/i],
    ['Beige', /beige|杏|米色/i], ['Olive', /olive|军绿/i], ['Khaki', /khaki|卡其|卡色/i],
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
  const explicit = normalInvoiceText(line).match(/(?:size|尺码|码数)\s*[:：-]?\s*(FREE\s*SIZE|均码|FS|4XL|3XL|XXL|XL|L|M|S|(?:2[468]|3[02468]|4[024]))/i);
  if (explicit) {
    const value = /FREE\s*SIZE|均码/i.test(explicit[1]) ? 'FS' : explicit[1].toUpperCase();
    if (sizes.includes(value)) return value;
  }
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
  const labelledQty = clean.match(/(?:qty|quantity|数量|件数)\s*[:：-]?\s*(\d+)/i);
  const labelledPrice = clean.match(/(?:unit\s*price|price|单价|售价)\s*[:：-]?\s*(?:¥|￥|RMB|CNY)?\s*(\d+(?:\.\d+)?)/i);
  if (labelledQty && labelledPrice && Number(labelledQty[1]) > 0 && Number(labelledPrice[1]) > 0) {
    return { qty: Number(labelledQty[1]), price: Number(labelledPrice[1]) };
  }
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
function invoiceSizeToken(value) {
  const token = String(value || '').toUpperCase().replace(/\s+/g, '');
  return ({ 'FREE': 'FS', 'FREESIZE': 'FS', '均码': 'FS', '2X': 'XXL', '2XL': 'XXL', '3X': '3XL' })[token] || token;
}
function invoiceRowCode(line) {
  const labelled = line.match(/(?:货号|款号|货品编码|商品编码|style|article|item|vendor\s*(?:code|sku))\s*[:#-]?\s*([A-Z0-9_\/-]{2,})/i);
  if (labelled) return labelled[1];
  const numbered = line.match(/^\s*\d{1,3}[.)、]?\s+([A-Z]*\d[A-Z0-9_\/-]{1,}|\d{3,8})\b/i);
  if (numbered) return numbered[1];
  const start = line.match(/^\s*([A-Z]*\d[A-Z0-9_\/-]{1,}|\d{3,8}(?:[#/][A-Z0-9\u3400-\u9fff]+)?)\b/i);
  return start ? start[1].replace(/#$/, '') : '';
}
function invoiceTableSizes(line, validSizes) {
  if (!/(?:颜色|colour|color)/i.test(line) || !/(?:数量|qty|quantity)/i.test(line)) return [];
  const middle = line.split(/(?:颜色|colour|color)/i)[1].split(/(?:数量|qty|quantity)/i)[0];
  const raw = middle.toUpperCase().match(/(?:FREE\s*SIZE|均码|FS|XS|S|M|L|2XL|3XL|4XL|XXL|XL|2X|3X|(?:2[468]|3[02468]|4[024]))/g) || [];
  return raw.map(invoiceSizeToken).filter((size, index, all) => validSizes.includes(size) && all.indexOf(size) === index);
}
function parseLocalInvoiceText(rawText, store) {
  const text = normalInvoiceText(rawText);
  const products = Object.keys(store.products || {}), colours = Object.keys(store.colours || {});
  const sizes = ['FS', 'S', 'M', 'L', 'XL', 'XXL', '3XL', '4XL', '24', '26', '28', '30', '32', '34', '36', '38', '40', '42', '44'];
  const fits = ['Oversized', 'Drop Shoulder', 'Boxy Fit', 'Relaxed Fit', 'Regular Fit', 'Slim Fit', 'Muscle Fit',
                'Baggy Fit', 'Straight Fit', 'Tapered Fit', 'Skinny Fit', 'Narrow Fit', 'Wide Leg', 'Bootcut', 'Cargo Fit'];
  const knownVendor = (store.vendors || []).find(v => text.toLowerCase().includes(String(v).toLowerCase()));
  const textLines = text.split('\n').map(x => x.replace(/\s+/g, ' ').trim()).filter(Boolean);
  const vendorLine = textLines.slice(0, 12).find(x => /公司|商行|服饰|服装|档口|供应商|supplier|vendor/i.test(x));
  const lines = [], warnings = [];
  let current = { code: '', name: '', productType: '', fit: '', size: '', price: 0, expectedQty: 0 };
  let tableSizes = [];
  const addLine = (data, sourceLine) => {
    const qty = Number(data.qty || 0), price = Number(data.price || 0);
    if (!(qty > 0)) return;
    const productType = data.productType || current.productType || '';
    const colour = data.colour || '';
    const sizeLabel = invoiceSizeToken(data.sizeLabel || current.size || '');
    const designCode = String(data.designCode || current.code || '').replace(/#$/, '').slice(0, 40);
    const missing = [];
    if (!designCode) missing.push('design code');
    if (!productType) missing.push('product type');
    if (!colour) missing.push('colour');
    if (!sizeLabel) missing.push('size');
    if (!(price > 0)) missing.push('unit price');
    const sourceName = String(data.sourceName || current.name || sourceLine || '').replace(/[¥￥]/g, ' ').replace(/\b\d+(?:\.\d+)?\b/g, ' ').replace(/\s+/g, ' ').trim();
    const baseName = [productType, colour].filter(Boolean).join(' ');
    lines.push({
      designName: (baseName || sourceName || ('Invoice item ' + (lines.length + 1))).slice(0, 80),
      designCode, productType, colour, sourceColour: data.sourceColour || '', fit: data.fit || current.fit || '',
      sizeLabel, chinaSize: sizeLabel, audience: 'Unisex', qty,
      perPcsYuan: price, photoBox: null, reviewRequired: missing.length > 0, reviewReasons: missing
    });
  };

  for (let index = 0; index < textLines.length; index++) {
    const line = textLines[index];
    if (/^(?:客户|销售|单号|时间|日期|批次|店员|本单|未付|款数|开单时间|联系电话)\s*[:：]/i.test(line)) continue;
    const headerSizes = invoiceTableSizes(line, sizes);
    if (headerSizes.length) { tableSizes = headerSizes; continue; }
    if (/合计|总计|小计|运费|税额|折扣|应付|实付|收款|销售\b|电话|地址|subtotal|grand\s*total|freight|discount|tax/i.test(line)) continue;

    const rowCode = invoiceRowCode(line);
    const productType = localInvoiceProduct(line, products);
    const fit = localInvoiceFit(line, fits);
    const colour = localInvoiceColour(line, colours);
    const size = localInvoiceSize(line, sizes);
    const amounts = localInvoiceNumbers(line, rowCode || current.code);
    const priceTimesQty = line.match(/(\d+(?:\.\d+)?)\s*元\s*[x×*]\s*(\d+)\s*件/i);

    // Product headings in printed table invoices occur once, followed by many
    // colour rows.  Retain the code/name/type until the next heading.
    if (rowCode) {
      current.code = rowCode;
      current.name = line.replace(rowCode, '').replace(/^\s*[#/:.-]+/, '').trim() || current.name;
      current.productType = productType || current.productType;
      current.fit = fit || current.fit;
      current.size = size || '';
    }
    if (!amounts.qty && current.code) {
      current.name = productType || fit || colour ? line : current.name;
      current.productType = productType || current.productType;
      current.fit = fit || current.fit;
      current.size = size || current.size;
    }
    if (priceTimesQty) {
      current.price = Number(priceTimesQty[1]);
      current.expectedQty = Number(priceTimesQty[2]);
      current.productType = productType || current.productType;
      current.fit = fit || current.fit;
      continue;
    }
    if (/^(?:均码|FS|S|M|L|XL|XXL|2XL|3XL|4XL)$/i.test(line)) {
      current.size = invoiceSizeToken(line);
      continue;
    }

    // Size-grid row: code/colour + one quantity per header + row qty/price/total.
    if (amounts.qty && tableSizes.length && (colour || rowCode)) {
      const numeric = [];
      const withoutCode = rowCode ? line.replace(rowCode, ' ') : line;
      for (const match of withoutCode.matchAll(/(?:^|\s)(\d+(?:\.\d+)?)(?=\s|$)/g)) numeric.push(Number(match[1]));
      let qtyIndex = -1;
      for (let i = numeric.length - 3; i >= 0; i--) {
        if (numeric[i] === amounts.qty && Math.abs(numeric[i] * numeric[i + 1] - numeric[i + 2]) <= Math.max(2, numeric[i + 2] * .03)) { qtyIndex = i; break; }
      }
      const sizeQty = qtyIndex >= tableSizes.length ? numeric.slice(qtyIndex - tableSizes.length, qtyIndex) : [];
      if (sizeQty.length === tableSizes.length && sizeQty.reduce((sum, qty) => sum + qty, 0) === amounts.qty) {
        tableSizes.forEach((sizeLabel, i) => { if (sizeQty[i] > 0) addLine({ designCode: rowCode, productType, colour, fit, sizeLabel, qty: sizeQty[i], price: amounts.price }, line); });
        continue;
      }
    }

    if (amounts.qty && (rowCode || colour || productType || size || current.code)) {
      addLine({ designCode: rowCode, productType, colour, fit, sizeLabel: size, qty: amounts.qty, price: amounts.price }, line);
      continue;
    }

    // Mobile receipt cards state the unit price/total first, then list one or
    // more colour variants below it.  Reuse that price for each variant row.
    if (current.price > 0 && (colour || /均色/.test(line))) {
      const nums = line.match(/\d+/g) || [];
      const qty = Number(nums[nums.length - 1] || 0);
      if (qty > 0) addLine({ colour, sourceColour: colour ? '' : line.replace(/\d+/g, '').trim(), qty, price: current.price, sizeLabel: size }, line);
    }
  }

  const fallbackVendor = textLines.slice(0, 8).find(x =>
    !/(?:invoice|bill|order|单据|单号|订单|票据|date|日期|时间|电话|phone)/i.test(x) &&
    !/(?:名称|商品|颜色|数量|单价|小计)/i.test(x) && /[A-Z\u3400-\u9fff]/i.test(x) && !/\d{4,}/.test(x) && x.replace(/\s/g,'').length > 1
  );
  const invoiceQtyMatch = text.match(/(?:合计\s*)?数量\s*:\s*(\d+)/i) || text.match(/销售\s*:\s*(\d+)/i);
  const totalLines=textLines.filter(line=>/(?:合计|总计|总额|金额|销售)/i.test(line));
  const invoiceAmountMatch = totalLines.join('\n').match(/(?:总计|总额|金额)\s*[:：]?\s*[¥￥#Y]?\s*(\d+(?:\.\d+)?)/i);
  const extractedQty = lines.reduce((sum, line) => sum + Number(line.qty || 0), 0);
  const extractedAmount = lines.reduce((sum, line) => sum + Number(line.qty || 0) * Number(line.perPcsYuan || 0), 0);
  const invoiceQty = invoiceQtyMatch ? Number(invoiceQtyMatch[1]) : 0;
  const invoiceAmount = invoiceAmountMatch ? Number(invoiceAmountMatch[1]) : 0;
  if (invoiceQty && extractedQty !== invoiceQty) warnings.push('Invoice says ' + invoiceQty + ' pieces; OCR extracted ' + extractedQty + '. Review missing or misread rows.');
  if (invoiceAmount && Math.abs(extractedAmount - invoiceAmount) > Math.max(1, invoiceAmount * .01)) warnings.push('Invoice total is ¥' + invoiceAmount + '; extracted lines total ¥' + extractedAmount + '. Review highlighted fields.');
  const reviewCount = lines.filter(line => line.reviewRequired).length;
  if (reviewCount) warnings.push(reviewCount + ' extracted line(s) have fields that need review before saving.');
  return {
    vendor: String(knownVendor || vendorLine || fallbackVendor || '').replace(/^(?:供应商|vendor|supplier)\s*[:：-]?\s*/i, '').toUpperCase().trim().slice(0, 100),
    billNo: localInvoiceBillNo(text), datePurchase: localInvoiceDate(text), lines, warnings,
    totals: { invoiceQty, invoiceAmount, extractedQty, extractedAmount }
  };
}

function normalizedBillNumber(value) {
  return String(value || '').normalize('NFKC').trim().replace(/\s+/g, '').toUpperCase();
}
function duplicateBillPo(store, billNo, exceptPoId) {
  const wanted = normalizedBillNumber(billNo);
  if (!wanted) return null;
  return Object.values((store && store.pos) || {}).find(po => po && po.id !== exceptPoId && normalizedBillNumber(po.billNo) === wanted) || null;
}

function articleWeightKey(line) {
  const code = String(line && line.designCode || '').normalize('NFKC').trim().toUpperCase();
  if (code) return 'CODE:' + code;
  const nameParts = [line && line.designName, line && line.productType]
    .map(value => String(value || '').normalize('NFKC').trim().replace(/\s+/g, ' ').toUpperCase());
  if (nameParts.some(Boolean)) return 'NAME:' + nameParts.join('|');
  return 'SKU:' + String(line && line.sku || '').trim().toUpperCase();
}
function expandArticleWeights(lines, weights) {
  const selected = new Map();
  Object.entries(weights || {}).forEach(([index, weight]) => {
    const line = (lines || [])[Number(index)];
    if (line) selected.set(articleWeightKey(line), Number(weight));
  });
  const expanded = {};
  (lines || []).forEach((line, index) => {
    const key = articleWeightKey(line);
    if (selected.has(key)) expanded[index] = selected.get(key);
  });
  return expanded;
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
    let parsed = parseLocalInvoiceText(text, s), reader = 'local-ocr';
    if (!isPdf && process.env.OPENAI_API_KEY) {
      try {
        const vision = await openaiPilot.extractInvoice({ key: process.env.OPENAI_API_KEY, buffer: req.file.buffer, mime: req.file.mimetype || 'image/jpeg' });
        const visionLines=(vision.lines||[]).filter(line=>Number(line.qty)>0).map(line=>{
          const sourceColour=String(line.sourceColour||''), colour=pickClosest(line.colour,Object.keys(s.colours||{}))||localInvoiceColour(sourceColour,Object.keys(s.colours||{}));
          const chinaSize=String(line.chinaSize||line.sizeLabel||''), sizeLabel=invoiceSizeToken(chinaSize||line.sizeLabel);
          const productType=pickClosest(line.productType,Object.keys(s.products||{}));
          return {
            designName:String(line.designName||line.sourceDescription||'Invoice item').slice(0,80), designCode:String(line.designCode||'').slice(0,40),
            productType, colour, sourceColour, fit:String(line.fit||''), sizeLabel, chinaSize, audience:'Unisex',
            qty:Number(line.qty), perPcsYuan:Number(line.perPcsYuan||0), photoBox:null,
            reviewRequired:line.confidence!=='high'||!!line.reviewReason||!line.designCode||!productType||!colour||!sizeLabel||!(Number(line.perPcsYuan)>0),
            reviewReasons:[String(line.reviewReason||''),!line.designCode?'design code':'',!productType?'product type':'',!colour?'colour':'',!sizeLabel?'size':'',!(Number(line.perPcsYuan)>0)?'unit price':''].filter(Boolean)
          };
        });
        if (visionLines.length) {
          const extractedQty=visionLines.reduce((sum,line)=>sum+line.qty,0), extractedAmount=visionLines.reduce((sum,line)=>sum+line.qty*line.perPcsYuan,0);
          const warnings=[...(vision.warnings||[])];
          if(Number(vision.invoiceQty)>0&&extractedQty!==Number(vision.invoiceQty))warnings.push('Invoice says '+vision.invoiceQty+' pieces; extracted '+extractedQty+'. Review missing or misread rows.');
          if(Number(vision.invoiceAmount)>0&&Math.abs(extractedAmount-Number(vision.invoiceAmount))>Math.max(1,Number(vision.invoiceAmount)*.01))warnings.push('Invoice total is ¥'+vision.invoiceAmount+'; extracted lines total ¥'+extractedAmount+'. Review highlighted fields.');
          const vendorCandidate=String(vision.vendor||'').trim(), vendorNumbers=vendorCandidate.match(/\d+(?:\.\d+)?/g)||[];
          const visionVendor=/^(?:量|名称|商品|颜色|客户|销售)$/i.test(vendorCandidate)||vendorNumbers.length>1||/(?:黑色|白色|灰色|粉色|蓝色|绿色|咖啡色)\s*\d/i.test(vendorCandidate)?'':vendorCandidate;
          parsed={vendor:visionVendor||parsed.vendor,billNo:vision.billNo||parsed.billNo,datePurchase:vision.datePurchase||parsed.datePurchase,lines:visionLines,warnings,totals:{invoiceQty:Number(vision.invoiceQty||0),invoiceAmount:Number(vision.invoiceAmount||0),extractedQty,extractedAmount}};
          reader='openai-vision';
        }
      } catch (visionError) {
        parsed.warnings=[...(parsed.warnings||[]),'OpenAI invoice reading was unavailable; local Chinese OCR was used. '+String(visionError.message||visionError).slice(0,160)];
      }
    }
    if (!parsed.lines.length) return res.status(422).json({ success: false,
      error: 'The invoice text was read, but no complete quantity-and-price rows were found. Use a clearer straight-on photo, or add the lines manually.' });
    res.json({
      success: true,
      vendor: String(parsed.vendor || '').toUpperCase().trim(),
      billNo: String(parsed.billNo || '').trim(),
      datePurchase: String(parsed.datePurchase || '').trim(),
      canCropPhotos: false,
      reader,
      invoice,
      lines: parsed.lines,
      warnings: parsed.warnings || [],
      totals: parsed.totals || {}
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
let advanceSaveTail = Promise.resolve();
const activePurchasePostings = new Set();
// Persist only this locked PO after an external await; other purchases and
// settings may have been saved while Shopify was responding.
function persistPostingPo(po) {
  const latest = loadStore();
  latest.pos[po.id] = po;
  saveStore(latest);
}
router.post('/api/procurement/advance', async (req, res) => {
  const previous = advanceSaveTail;
  let releaseSave;
  advanceSaveTail = new Promise(resolve => { releaseSave = resolve; });
  await previous;
  try {
    // Finish network reads before loading the allocation state. From the fresh
    // store read through SKU/PO allocation and save there is no async gap.
    const catalogue = await loadCatalogue(true);
    const s = loadStore();
    const b = req.body || {};
    if (!(b.lines || []).length) return res.status(400).json({ success: false, error: 'Add at least one line before saving.' });
    const billNo = String(b.billNo || '').trim();
    if (!billNo) return res.status(400).json({ success: false, error: 'Enter the vendor bill number before saving.' });
    const existingBill = duplicateBillPo(s, billNo);
    if (existingBill) return res.status(409).json({ success: false, error: 'Bill number "' + billNo + '" already exists in ' + existingBill.id + '. Duplicate bills are not allowed.' });
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
    const preview = computePreviewWithCatalogue(s, { lines: b.lines, vendor: b.vendor, exRate: b.exRate, origin, transportTotal }, catalogue);
    const lines = preview.lines.map(l => ({
      designName: l.designName, productType: l.productType, colour: l.colour,
      sizeLabel: l.sizeLabel, chinaSize: l.chinaSize, fit: l.fit, audience: l.audience,
      vendor: l.vendor, designCode: l.designCode, photoUrl: l.photoUrl, rawPhotoUrl: l.rawPhotoUrl || l.photoUrl,
      designId: l.designId, designOverrides: l.designOverrides || {},
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
      billNo,
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
    const out = { ...publicPo(s.pos[poId], req) };
    delete out.seoDraft;
    res.json({ success: true, poId, po: out, lines: out.lines });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
  finally { releaseSave(); }
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
  const expandedWeights = expandArticleWeights(po.lines || [], weights);
  po.weightHistory = po.weightHistory || [];
  po.weightHistory.push({ at: new Date().toISOString(), by: (req.user || {}).username || '',
    changes: Object.entries(expandedWeights).map(([index, weight]) => ({ index: Number(index), before: num(po.lines[index].weightGrams), after: Number(weight) })) });
  Object.entries(expandedWeights).forEach(([index, weight]) => { po.lines[index].weightGrams = Number(weight); });
  saveStore(s);
  res.json({ success: true, po: publicPo(po, req) });
});

// Accounts may replace the formula MRP before Shopify posting. The override is
// stored per SKU/line, survives every recalculation and is the variant price
// used by computePreview/createDraftProduct.
router.patch('/api/procurement/pos/:id/selling-prices', (req, res) => {
  if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
  const s = loadStore(), po = s.pos[req.params.id], prices = (req.body || {}).prices;
  if (!po) return res.status(404).json({ success: false, error: 'PO not found.' });
  if (isLockedPo(po)) return res.status(409).json({ success: false, error: 'Selling prices must be finalized before Shopify posting.' });
  if (!prices || typeof prices !== 'object' || Array.isArray(prices)) return res.status(400).json({ success: false, error: 'Selling prices are required.' });
  const automaticLines = pricedLinesForPo(s, po);
  const changes = [];
  for (const [sku, value] of Object.entries(prices)) {
    const index = (po.lines || []).findIndex(line => String(line.sku || '').toUpperCase() === String(sku).toUpperCase());
    if (index < 0) return res.status(400).json({ success: false, error: 'Selling-price SKU no longer matches this PO: ' + sku });
    const reset = value === null || value === '';
    const price = reset ? 0 : Number(value);
    if (!reset && (!Number.isFinite(price) || price <= 0 || Math.round(price) !== price)) return res.status(400).json({ success: false, error: 'Each selling price must be a positive whole rupee amount.' });
    const line = po.lines[index], before = num(line.manualMrp) || 0;
    // Saving unchanged automatic fields must not freeze them as overrides.
    const after = !before && price === automaticLines[index].calculatedMrp ? 0 : price;
    line.manualMrp = after;
    if (before !== after) changes.push({ index, sku: line.sku, before, after });
  }
  po.sellingPriceHistory = Array.isArray(po.sellingPriceHistory) ? po.sellingPriceHistory : [];
  if (changes.length) po.sellingPriceHistory.push({ at: new Date().toISOString(), by: (req.user && req.user.username) || 'system', changes });
  saveStore(s);
  res.json({ success: true, changed: changes.length, po: publicPo(po, req) });
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
    const weights = expandArticleWeights(po.lines || [], b.weights || {}); // one entered article weight applies to every colour/size
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
    // Recalculating weights or saving selling prices replaces lastReceive in
    // the page. Keep the same group metadata as /studio so its approved model
    // views still satisfy the posting gate (and conflicting audiences stay blocked).
    preview.newProducts = (preview.newProducts || []).map(np => {
      const group = studioSourceGroup(po, np);
      return { ...np, audience: group.audience, fit: group.fit, line: group.line, season: group.season };
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
  po.lines[i].rawPhotoUrl = url;
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
const LINE_EDIT_FIELDS = ['designName', 'designCode', 'productType', 'colour', 'sizeLabel', 'chinaSize', 'fit', 'audience', 'sku', 'perPcsYuan'];
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
  const overrides = b.overrides || {}; // { lineIndex: { field: true } }
  const who = (req.user && req.user.username) || 'system';
  const now = new Date().toISOString();
  const beforeLines = (po.lines || []).map(line => ({ ...line }));
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
      let v = k === 'perPcsYuan' ? Math.max(0, num(e[k])) : String(e[k]).trim();
      if (k === 'sku') v = v.toUpperCase();
      const oldValue = k === 'perPcsYuan' ? Math.max(0, num(l[k])) : String(l[k] == null ? '' : l[k]);
      if (v !== oldValue) { l[k] = v; changed = true; }
    });
    if (hasQty) {
      const q = Math.max(0, Math.round(num(qtys[i])));
      if (q !== (num(l.qty) || 0)) { l.qty = q; changed = true; }
    }
    if (overrides[i] && typeof overrides[i] === 'object') {
      l.designOverrides = {};
      for (const [field, enabled] of Object.entries(overrides[i])) if (enabled) l.designOverrides[field] = true;
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
  reconcileStudioKeysAfterLineEdit(po, beforeLines, po.lines || []);
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

// Split articles that were accidentally merged under a generic design name.
// Rows with the same original photo remain size variants; different originals
// become separate products. Existing paid work stays with the first article.
router.post('/api/procurement/pos/:id/split-group-by-photo', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success:false, error:'Purchases access required.' });
    const s=loadStore(),po=s.pos[req.params.id],oldKey=String((req.body||{}).groupKey||'');
    if(!po||isLockedPo(po))return res.status(409).json({success:false,error:'An editable purchase is required.'});
    const lines=(po.lines||[]).filter(line=>groupKey(line)===oldKey);
    if(!oldKey||lines.length<2)return res.status(404).json({success:false,error:'Merged product group not found.'});
    if(lines.some(line=>!String(line.photoUrl||'').trim()))return res.status(409).json({success:false,error:'Every merged SKU needs its original photo before it can be split safely.'});
    const byPhoto=new Map();
    for(const line of lines){const photo=String(line.photoUrl).trim();if(!byPhoto.has(photo))byPhoto.set(photo,[]);byPhoto.get(photo).push(line);}
    if(byPhoto.size<2)return res.status(409).json({success:false,error:'These SKUs use the same original photo. Give each separate design its own vendor code in the editable purchase table.'});
    const sizeCodeOf=label=>s.sizes[label]||label;
    for(const photoLines of byPhoto.values()){
      const seen=new Set();
      for(const line of photoLines){const size=String(sizeCodeOf(line.sizeLabel)||'').trim().toUpperCase();if(seen.has(size))return res.status(409).json({success:false,error:'Two SKUs with size '+size+' still share one original photo. Assign their vendor codes manually.'});seen.add(size);}
    }
    const oldImages=(po.aiImages||{})[oldKey],oldStyle=(po.imageStyling||{})[oldKey],oldBack=(po.backRefs||{})[oldKey],oldRejected=(po.qaRejected||{})[oldKey];
    po.seoDraft=Array.isArray(po.seoDraft)?po.seoDraft:[];
    const oldSeo=po.seoDraft.find(d=>d.key===oldKey),base=(po.id+'-ARTICLE').replace(/[^A-Z0-9-]/gi,'').toUpperCase();
    let article=0,primaryKey='';
    for(const photoLines of byPhoto.values()){
      const code=base+'-'+String.fromCharCode(65+article++);
      for(const line of photoLines){line.designCode=code;line.editedAt=new Date().toISOString();line.editedBy=(req.user&&req.user.username)||'system';}
      if(!primaryKey)primaryKey=groupKey(photoLines[0]);
    }
    for(const name of ['aiImages','imageStyling','backRefs','qaRejected'])if(po[name])delete po[name][oldKey];
    po.seoDraft=po.seoDraft.filter(d=>d.key!==oldKey);
    if(oldImages){po.aiImages=po.aiImages||{};po.aiImages[primaryKey]=oldImages;}
    if(oldStyle){po.imageStyling=po.imageStyling||{};po.imageStyling[primaryKey]=oldStyle;}
    if(oldBack){po.backRefs=po.backRefs||{};po.backRefs[primaryKey]=oldBack;}
    if(oldRejected){po.qaRejected=po.qaRejected||{};po.qaRejected[primaryKey]=oldRejected;}
    if(oldSeo)po.seoDraft.push({...oldSeo,key:primaryKey,designCode:lines[0].designCode,seoApproved:false});
    if(po.openaiPilot&&Array.isArray(po.openaiPilot.attempts))for(const attempt of po.openaiPilot.attempts)if(attempt.groupKey===oldKey)attempt.groupKey=primaryKey;
    const groups=await newGroupsOf(s,po),newKeys=new Set([...byPhoto.values()].map(photoLines=>groupKey(photoLines[0])));
    for(const group of groups.filter(g=>newKeys.has(g.key))){
      const fingerprint=codexBatch.fingerprint(group,(po.backRefs||{})[group.key]);
      for(const image of ((po.aiImages||{})[group.key]||[]))image.sourceFingerprint=fingerprint;
      if(!po.seoDraft.some(d=>d.key===group.key)){
        const seo=genSeo({...group,sizeCodeOf});
        po.seoDraft.push({key:group.key,designCode:group.designCode,colour:group.colour,productType:group.productType,seo,seoApproved:false,source:'product-details'});
      }
    }
    saveStore(s);
    res.json({success:true,articles:byPhoto.size,preservedGroupKey:primaryKey});
  }catch(error){res.status(500).json({success:false,error:error.message});}
});

// Resolve repeated variants that are the same photographed article and size.
// One active SKU keeps the combined received quantity; the other rows remain
// in the PO at quantity zero so the original purchase and SKU trail are kept.
router.post('/api/procurement/pos/:id/merge-duplicate-variants', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success:false, error:'Purchases access required.' });
    const s=loadStore(),po=s.pos[req.params.id],key=String((req.body||{}).groupKey||'');
    if(!po||isLockedPo(po))return res.status(409).json({success:false,error:'An editable purchase is required.'});
    const lines=(po.lines||[]).filter(line=>num(line.qty)>0&&key&&groupKey(line)===key);
    if(!lines.length)return res.status(404).json({success:false,error:'Received product group not found. Refresh the PO and try again.'});
    const sizeCodeOf=label=>s.sizes[label]||label,bySize=new Map();
    for(const line of lines){const size=String(sizeCodeOf(line.sizeLabel)||'').trim().toUpperCase();if(!bySize.has(size))bySize.set(size,[]);bySize.get(size).push(line);}
    const duplicates=[...bySize].filter(([,sameSize])=>sameSize.length>1);
    if(!duplicates.length)return res.json({success:true,mergedRows:0,alreadyResolved:true});
    for(const [size,sameSize] of duplicates){
      const photos=new Set(sameSize.map(line=>String(line.photoUrl||'').trim()).filter(Boolean));
      if(photos.size!==1||sameSize.some(line=>!String(line.photoUrl||'').trim()))return res.status(409).json({success:false,error:'Size '+size+' does not have one shared original photo. Use separate vendor codes for separate designs.'});
    }
    const at=new Date().toISOString(),by=(req.user&&req.user.username)||'system',mergedSizes=[];
    po.variantMergeHistory=Array.isArray(po.variantMergeHistory)?po.variantMergeHistory:[];
    for(const [size,sameSize] of duplicates){
      const keeper=sameSize[0],removed=sameSize.slice(1),totalQty=sameSize.reduce((sum,line)=>sum+num(line.qty),0);
      for(const line of sameSize)if(!line.ordered||typeof line.ordered!=='object')line.ordered=orderedSnapshot(line);
      keeper.qty=totalQty;keeper.editedAt=at;keeper.editedBy=by;
      for(const line of removed){line.qty=0;line.mergedIntoSku=keeper.sku;line.mergedDuplicateAt=at;line.mergedDuplicateBy=by;line.editedAt=at;line.editedBy=by;}
      const record={type:'duplicate-variant-merge',groupKey:key,size,keptSku:keeper.sku,mergedSkus:removed.map(line=>line.sku).filter(Boolean),totalQty,at,by};
      po.variantMergeHistory.push(record);mergedSizes.push(record);
    }
    // Removing an identical duplicate size changes the technical group
    // fingerprint, but not the photographed article. Carry the already paid,
    // reviewed images onto that corrected fingerprint.
    const correctedGroup=(await newGroupsOf(s,po)).find(group=>group.key===key);
    if(correctedGroup){
      const fingerprint=codexBatch.fingerprint(correctedGroup,(po.backRefs||{})[key]);
      for(const image of ((po.aiImages||{})[key]||[]))image.sourceFingerprint=fingerprint;
    }
    saveStore(s);
    res.json({success:true,mergedRows:mergedSizes.reduce((sum,item)=>sum+item.mergedSkus.length,0),mergedSizes});
  }catch(error){res.status(500).json({success:false,error:error.message});}
});

// Keep the ordered lines for audit, but remove an entire product group from
// received stock and Shopify posting when it did not physically arrive.
router.post('/api/procurement/pos/:id/discard-received-group', (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success:false, error:'Purchases access required.' });
    const body=req.body||{},s=loadStore(),po=s.pos[req.params.id],key=String(body.groupKey||'');
    if(!po||isLockedPo(po))return res.status(409).json({success:false,error:'An editable purchase is required.'});
    const requestedSkus=new Set((Array.isArray(body.skus)?body.skus:[]).map(sku=>String(sku||'').trim().toUpperCase()).filter(Boolean));
    let matched=(po.lines||[]).filter(line=>key&&groupKey(line)===key);
    if(!matched.length&&requestedSkus.size)matched=(po.lines||[]).filter(line=>requestedSkus.has(String(line.sku||'').trim().toUpperCase()));
    if(!matched.length)return res.status(404).json({success:false,error:'Received product group not found. Refresh the PO and try again.'});
    const lines=matched.filter(line=>num(line.qty)>0);
    if(!lines.length)return res.json({success:true,removedLines:0,removedPieces:0,alreadyRemoved:true});
    const at=new Date().toISOString(),by=(req.user&&req.user.username)||'system';
    let pieces=0;
    for(const line of lines){
      pieces+=num(line.qty);
      if(!line.ordered||typeof line.ordered!=='object')line.ordered=orderedSnapshot(line);
      line.qty=0;line.didNotArrive=true;line.didNotArriveAt=at;line.didNotArriveBy=by;line.editedAt=at;line.editedBy=by;
    }
    po.receiptExceptions=Array.isArray(po.receiptExceptions)?po.receiptExceptions:[];
    po.receiptExceptions.push({type:'did-not-arrive',groupKey:key,skus:lines.map(line=>line.sku).filter(Boolean),pieces,at,by});
    saveStore(s);
    res.json({success:true,removedLines:lines.length,removedPieces:pieces});
  }catch(error){res.status(500).json({success:false,error:error.message});}
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
    if (b.billNo != null) {
      const billNo = String(b.billNo).trim();
      if (!billNo) return res.status(400).json({ success: false, error: 'Enter the vendor bill number before saving.' });
      const existingBill = duplicateBillPo(s, billNo, po.id);
      if (existingBill) return res.status(409).json({ success: false, error: 'Bill number "' + billNo + '" already exists in ' + existingBill.id + '. Duplicate bills are not allowed.' });
      po.billNo = billNo;
    }
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
      const beforeLines = (po.lines || []).map(line => ({ ...line }));
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
        incoming.rawPhotoUrl = incoming.rawPhotoUrl || old.rawPhotoUrl || old.photoUrl || incoming.photoUrl || '';
        incoming.photoUrl = incoming.photoUrl || incoming.rawPhotoUrl;
        incoming.designId = incoming.designId || old.designId;
        incoming.designOverrides = incoming.designOverrides || old.designOverrides || {};
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
        vendor: l.vendor || po.vendor, designCode: l.designCode,
        photoUrl: l.photoUrl || l.rawPhotoUrl, rawPhotoUrl: l.rawPhotoUrl || l.photoUrl,
        designId: l.designId, designOverrides: l.designOverrides || {},
        qty: l.qty, perPcsYuan: l.perPcsYuan,
        weightGrams: num(l.weightGrams),
        sku: l.sku, serialUsed: l.serialUsed || null, skuError: l.skuError || null,
        classification: l.classification,
        // Preserve the frozen ordered baseline: carried on the line, else matched
        // by SKU from before the edit, else seeded fresh so tracking still starts.
        ordered: (l.ordered && typeof l.ordered === 'object') ? l.ordered : (prevOrdered[l.sku] || orderedSnapshot(l)),
        receiptAdded: prevReceiptAdded[l.sku] || null
      }));
      reconcileStudioKeysAfterLineEdit(po, beforeLines, po.lines);
      po.seoDraft = Array.isArray(po.seoDraft) ? po.seoDraft : [];
      for (const np of (preview.newProducts || [])) {
        if (!po.seoDraft.some(d => d.key === np.key)) po.seoDraft.push({ key: np.key, designCode: np.designCode, colour: np.colour, productType: np.productType, seo: np.seo, seoApproved: false, source: 'product-details' });
      }
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
  return (preview.newProducts || []).map(np => studioSourceGroup(po, np));
}
// Keep the exact studio field order for compatibility with saved image hashes.
// Normal posting and continuation must inspect the same approved reference.
function studioSourceGroup(po, np) {
  const lines = (po.lines || []).filter(line => groupKey(line) === np.key);
  const line = lines.find(line => (line.photoUrl || '').trim());
  const details = lines[0] || {};
  const audiences = [...new Set(lines.map(line => String(line.audience || '').trim()))];
  const audience = audiences.length === 1 && ['Men','Women','Unisex'].includes(audiences[0]) ? audiences[0] : '';
  return {key:np.key, colour:np.colour, productType:np.productType, designName:np.designName,
    designCode:np.designCode, audience, line:po.line || '', season:details.season || po.season || '',
    fit:details.fit || '', sizeLabels:np.variants.map(variant => variant.sizeLabel), photoUrl:line ? line.photoUrl : ''};
}

// A design-code correction can split one product group into several groups
// (for example `87375` -> `87375 A`, `87375 B`, `87375 C`). Paid drafts are
// stored by group key, so keep the old group's drafts attached to the first
// resulting group: that group contains the same representative/source line
// the original generation used. The orphan is retained as an audit backup.
function restoreDraftsAfterGroupSplit(po, groups) {
  po.seoDraft = Array.isArray(po.seoDraft) ? po.seoDraft : [];
  po.imageStyling = po.imageStyling || {};
  po.backRefs = po.backRefs || {};
  const current = new Set(groups.map(g => g.key));
  let changed = false;
  for (const oldKey of Object.keys(po.aiImages || {})) {
    if (current.has(oldKey) || !Array.isArray(po.aiImages[oldKey]) || !po.aiImages[oldKey].length) continue;
    const cut = oldKey.lastIndexOf('|');
    if (cut < 0) continue;
    const oldDesign = oldKey.slice(0, cut).trim().toLowerCase();
    const oldColour = oldKey.slice(cut + 1).trim().toLowerCase();
    const oldSeo = (po.seoDraft || []).find(d => d.key === oldKey);
    const candidates = groups.filter(g => {
      const at = g.key.lastIndexOf('|');
      if (at < 0) return false;
      const design = g.key.slice(0, at).trim().toLowerCase();
      const colour = g.key.slice(at + 1).trim().toLowerCase();
      return colour === oldColour && design.startsWith(oldDesign + ' ');
    });
    const target = candidates.find(g => !((po.aiImages || {})[g.key] || []).length);
    if (!target) continue;
    const fingerprint = codexBatch.fingerprint(target, (po.backRefs || {})[target.key] || (po.backRefs || {})[oldKey]);
    po.aiImages[target.key] = po.aiImages[oldKey].map(image => ({ ...image, sourceFingerprint: fingerprint }));
    if (oldSeo && !(po.seoDraft || []).some(d => d.key === target.key)) {
      po.seoDraft.push({ ...oldSeo, key: target.key, designCode: target.designCode, colour: target.colour, productType: target.productType });
    }
    if ((po.imageStyling || {})[oldKey] && !(po.imageStyling || {})[target.key]) {
      po.imageStyling[target.key] = { ...po.imageStyling[oldKey] };
    }
    if ((po.backRefs || {})[oldKey] && !(po.backRefs || {})[target.key]) {
      po.backRefs[target.key] = po.backRefs[oldKey];
    }
    changed = true;
  }
  return changed;
}

// One-time repair for the PO-0006 87375 split. These are the exact saved URLs
// shown in the studio before the group was split; re-linking them is free and
// avoids a second paid generation. They belong to the first/source design only.
function restorePo0006SavedSet(po, groups) {
  if (po.id !== 'PO-0006') return false;
  const target = groups.find(g => /^87375\s+a$/i.test(String(g.designCode || '').trim()) && /^white$/i.test(g.colour || ''));
  if (!target || ((po.aiImages || {})[target.key] || []).length) return false;
  const saved = [
    ['front', 'Product front', '/api/procurement/photo/1788523008561-e875fe95028b.png'],
    ['female', 'Female model', '/api/procurement/photo/1789728503256-8df8b78a4abe.png'],
    ['model-side-female', 'Styled three-quarter view', '/api/procurement/photo/1789728530399-67289e013f9c.png']
  ];
  po.aiImages = po.aiImages || {};
  const restoredUrls = new Set(saved.map(([, , url]) => url));
  if((po.imageRejectionHistory||[]).some(image=>restoredUrls.has(image.url)))return false;
  let cleaned = false;
  // A previous over-broad recovery copied A's set onto B/C. Remove only those
  // exact duplicate links; never remove a distinct historical file.
  for (const group of groups.filter(g => /^87375\s+[bc]$/i.test(String(g.designCode || '').trim()))) {
    const before = ((po.aiImages || {})[group.key] || []);
    const after = before.filter(image => !restoredUrls.has(image.url));
    if (after.length !== before.length) { po.aiImages[group.key] = after; cleaned = true; }
  }
  if (!saved.every(([, , url]) => readStoredPhoto(url))) return false;
  if (((po.aiImages || {})[target.key] || []).length) return cleaned;
  const fingerprint = codexBatch.fingerprint(target, (po.backRefs || {})[target.key]);
  po.aiImages[target.key] = saved.map(([type, label, url]) => ({
    type, label, url, approved: true, source: 'openai-pilot', sourceFingerprint: fingerprint,
    qa: { status: 'manual-reviewed', issues: [] }
  }));
  return true;
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
    // The studio represents received stock that can be posted. Zero-quantity
    // ordered lines remain in publicPo() for audit but must never become cards.
    const snapshot = JSON.stringify(po);
    const receivedLines=(po.lines||[]).filter(line=>num(line.qty)>0);
    const preview = await computePreview(s, { lines: receivedLines, vendor: po.vendor, exRate: po.exRate, freightPerGram: po.freightPerGram, origin: po.origin, transportTotal: po.transportTotal });
    const groups = await newGroupsOf(s, po);
    const restoredKnownSet = restorePo0006SavedSet(po, groups);
    const interruptedAttempts = expireStalePaidAttempts(po);
    const promotedAdvisoryImages = promoteAdvisoryHeldImages(po,groups);
    const heldUncheckedImages = holdUncheckedImages(po,groups);
    if (interruptedAttempts||restoredKnownSet||promotedAdvisoryImages||heldUncheckedImages) {
      const latest = loadStore();
      // A generation may finish during the asynchronous catalogue read. Keep
      // its new result instead of persisting this older studio snapshot.
      if (JSON.stringify(latest.pos[req.params.id]) === snapshot) {
        latest.pos[req.params.id] = po;
        saveStore(latest);
      }
    }
    const byKey = new Map(groups.map(g => [g.key, g]));
    // Return the complete saved calculation as well as the studio groups. The
    // Purchases page uses this read-only response to restore the Shopify post
    // panel whenever a received PO is reopened; users should not have to save
    // the same weights again just to make the posting action appear.
    const savedPreview = stripPreviewForRole(preview, req);
    res.set('Cache-Control','no-store');
    res.json({ success: true, ...savedPreview, newProducts: (preview.newProducts || []).map(np => ({...np,
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
  po.imageStyling[key]=styling;
  for(const image of ((po.aiImages||{})[key]||[]))refreshImageStylingCheck(image,styling,group);
  saveStore(s);
  res.json({success:true,groupKey:key,styling,images:(po.aiImages||{})[key]||[]});
  } catch(e) { res.status(500).json({success:false,error:e.message}); }
});
const paidPilotInFlight = new Set();
const paidPilotWorkerId = crypto.randomUUID();
const paidPilotWorkerStartedAt = Date.now();
const activePaidAttemptIds = new Set();
const paidJobs = new Map();
const imageWorkflow = require('./procurement-image-workflow');
function assertNoImageGeneration(po, action) {
  if (po && (paidPilotInFlight.has(po.id) || (po.openaiPilot?.attempts || []).some(attempt => attempt.status === 'running'))) {
    const error = new Error('Wait for this PO’s image generation to finish or stop it before ' + action + '.');
    error.status = 409;
    throw error;
  }
}
// Freeze product edits while their paid job is active; no billed result should
// be discarded because a dropdown or reference was changed mid-request.
function attemptRecord(po,id){return (po?.openaiPilot?.attempts||[]).find(attempt=>attempt.id===id);}
function jobStopped(po,id,signal){return signal.aborted || !!attemptRecord(po,id)?.cancelRequestedAt;}
router.post('/api/procurement/stop-image-generation', (req,res)=>{
  if(!canStartPaidPilot(req))return res.status(403).json({success:false,error:'Paid image-generation access required.'});
  const store=loadStore(),at=new Date().toISOString(),by=String(req.user?.username||req.user?.role||'user');
  store.settings=store.settings||{};
  store.settings.imageGenerationEpoch=imageWorkflow.epochOf(store)+1;
  let stopped=0;
  for(const po of Object.values(store.pos||{}))for(const attempt of po.openaiPilot?.attempts||[])if(attempt.status==='running'){
    attempt.cancelRequestedAt=at;attempt.cancelledBy=by;stopped++;
    if(!activePaidAttemptIds.has(attempt.id)){attempt.status='cancelled';attempt.completedAt=at;}
  }
  saveStore(store);
  for(const controller of paidJobs.values())controller.abort(new DOMException('Image generation stopped by user.','AbortError'));
  res.json({success:true,stopped,generationEpoch:store.settings.imageGenerationEpoch,
    message:'Stopped app jobs and invalidated queued requests. Already submitted provider calls may still be billed. Saved photos were kept.'});
});
router.get('/api/procurement/pos/:id/image-prompts', async(req,res)=>{
  try{
    if(!canManagePurchases(req))return res.status(403).json({success:false,error:'Purchases access required.'});
    const store=loadStore(),po=store.pos[req.params.id],key=String(req.query.groupKey||'');
    if(!po)return res.status(404).json({success:false,error:'PO not found.'});
    const group=(await newGroupsOf(store,po)).find(group=>group.key===key);
    if(!group)return res.status(404).json({success:false,error:'Product group not found.'});
    const reference=await require('./procurement-image-source').normalizeSource(readStoredPhoto(group.photoUrl));
    const style=openaiPilot.normalizeStyling(po.imageStyling?.[key],group),back=!!readStoredPhoto(po.backRefs?.[key]);
    res.set('Cache-Control','no-store');
    res.json({success:true,model:process.env.PROCUREMENT_OPENAI_IMAGE_MODEL||'gpt-image-1.5',styling:style,
      referenceFormat:{original:reference.originalFormat,prepared:reference.format,converted:reference.converted},
      views:openaiPilot.pilotTypes(group,back).map(type=>({type,prompt:openaiPilot.imagePrompt(group,type,style,type.startsWith('model-side'))})),
      note:(reference.converted?'Reference format '+reference.originalFormat.toUpperCase()+' → '+reference.format.toUpperCase()+' (automatic conversion; original kept). ':'')+'Current base prompts. A source-fit check may fall back to Auto; a corrective attempt adds only the failed visual findings. Opening this preview does not generate images.'});
  }catch(error){res.status(error.api?.code==='invalid_source_image'?400:500).json({success:false,error:error.message});}
});
function refreshImageStylingCheck(image,styling,group) {
  if(image.source!=='openai-pilot'||!image.styling||!openaiPilot.MODEL_VIEWS.includes(image.type))return;
  const changes=openaiPilot.stylingChanges(image.requestedStyling||image.styling,styling,group,image.type);
  if(changes.length){
    if(!image.stylingReview){image.stylingReview={qa:structuredClone(image.qa||{}),approved:!!image.approved};}
    image.approved=false;
    image.qa={...(image.qa||{}),status:'needs-review',stylingChanged:true,stylingChanges:changes,
      issues:['Model styling changed: '+changes.map(x=>x.field+' ('+x.before+' → '+x.after+')').join(', ')+'. Restore the image’s styling or regenerate this view.']};
  }else if(image.stylingReview){
    image.qa=image.stylingReview.qa;image.approved=false;delete image.stylingReview;
    refreshImageStylingCheck(image,styling,group);
  }else if(image.qa?.issues?.length===1&&/^Model styling changed after this image was generated\./.test(image.qa.issues[0])&&Array.isArray(image.qa.failed)&&!image.qa.failed.length&&Array.isArray(image.qa.uncertain)&&!image.qa.uncertain.length){
    // Older styling invalidation overwrote the verdict but retained its findings.
    // Restore only a known clean check, and still require explicit approval.
    image.qa={...image.qa,status:image.qa.manualReview?'manual-reviewed':'pass',issues:[]};image.approved=false;
  }
}
function expireStalePaidAttempts(po) {
  let changed=false;
  for(const attempt of ((po.openaiPilot||{}).attempts||[])) {
    if(attempt.status!=='running' || activePaidAttemptIds.has(attempt.id))continue;
    const startedAt=Date.parse(attempt.startedAt||'');
    // Generation runs in this app's one server process. A restarted worker
    // cannot resume a saved job, but its images and paid-call audit survive.
    const lostWorker=!!attempt.workerId || Number.isFinite(startedAt)&&startedAt<paidPilotWorkerStartedAt;
    const stale=!Number.isFinite(startedAt)||Date.now()-startedAt>90*60*1000;
    if(lostWorker||stale) {
      attempt.status='interrupted';attempt.completedAt=new Date().toISOString();
      attempt.interruptionReason=lostWorker?'worker-restarted':'stale-job';
      attempt.errors=Array.isArray(attempt.errors)?attempt.errors:[];
      attempt.errors.push({type:'job',error:'Generation stopped or lost contact after the server restarted or lost the job. Saved drafts and earlier paid calls remain in the audit. Review them before explicitly generating again; no automatic paid retry was made.'});
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
  return canStartPaidPilot(req);
}
function imageCheckAccepted(image) {
  if(!image.qa||!['pass','manual-reviewed'].includes(image.qa.status))return false;
  return image.qa.status==='manual-reviewed'||!['front','back','detail'].includes(image.type)||image.qa.productOnlyVerified===true;
}
function holdUncheckedImages(po,groups) {
  if(isLockedPo(po))return false;
  let changed=false;
  for(const group of groups){
    const images=(po.aiImages||{})[group.key]||[];
    for(const image of images){
      const before=JSON.stringify(image),fingerprint=codexBatch.fingerprint(group,(po.backRefs||{})[group.key]);
      refreshImageStylingCheck(image,(po.imageStyling||{})[group.key],group);
      if(image.sourceFingerprint&&image.sourceFingerprint!==fingerprint){
        image.approved=false;image.qa={...(image.qa||{}),status:'needs-review',sourceChanged:true,issues:['The original product reference changed. Generate this view again from the current reference.']};
      }
      if(before!==JSON.stringify(image))changed=true;
    }
    const unchecked=images.filter(image=>image.url&&(!image.qa||image.qa.status==='pass'&&!imageCheckAccepted(image)));
    if(!unchecked.length)continue;
    po.qaRejected=po.qaRejected||{};po.qaRejected[group.key]=po.qaRejected[group.key]||[];
    for(const image of unchecked)po.qaRejected[group.key].push({...image,approved:false,
      qa:{...(image.qa||{}),status:'needs-review',failed:[],uncertain:['verification'],issues:['This legacy image has no product-only verification. Inspect it against the original before accepting or reject it.']},
      sourceFingerprint:codexBatch.fingerprint(group,(po.backRefs||{})[group.key]),
      styling:image.styling||openaiPilot.normalizeStyling((po.imageStyling||{})[group.key],group),at:new Date().toISOString()});
    po.aiImages[group.key]=images.filter(image=>!unchecked.includes(image));
    for(const seo of po.seoDraft||[])if(seo.key===group.key)seo.seoApproved=false;
    changed=true;
  }
  return changed;
}
function promoteAdvisoryHeldImages(po,groups) {
  let changed=false;
  po.aiImages=po.aiImages||{};po.qaRejected=po.qaRejected||{};
  for(const group of groups||[]){
    const key=group.key,rejected=Array.isArray(po.qaRejected[key])?po.qaRejected[key]:[];
    const latestByType={};
    for(const candidate of rejected)if(candidate&&candidate.url&&!candidate.supersededBy&&(!['front','back','detail'].includes(candidate.type)||candidate.qa?.productOnlyVerified===true)&&(openaiPilot.canAutoAcceptAdvisoryCheck(candidate.qa)||openaiPilot.canAutoAcceptConfirmedColourCheck(candidate.qa,group.colour)))latestByType[candidate.type]=candidate;
    for(const candidate of Object.values(latestByType)){
      const fingerprint=codexBatch.fingerprint(group,(po.backRefs||{})[key]);
      if(candidate.sourceFingerprint!==fingerprint||!readStoredPhoto(candidate.url))continue;
      const savedStyling=openaiPilot.normalizeStyling((po.imageStyling||{})[key],group);
      const images=po.aiImages[key]||[];
      if(images.some(image=>image.type===candidate.type&&image.url&&imageCheckAccepted(image)))continue;
      const warning=(candidate.qa.issues||[]).join('; ')||'Automated check could not determine apparent adult gender.';
      const rec={type:candidate.type,label:(AI_IMAGE_SPECS.find(x=>x.type===candidate.type)||{}).label||candidate.type,url:candidate.url,approved:false,source:'openai-pilot',
        qa:{...candidate.qa,status:'pass',failed:[],uncertain:[],issues:[],warnings:[...(candidate.qa.warnings||[]),warning],autoAcceptedAdvisory:true},
        sourceFingerprint:fingerprint,styling:candidate.styling||savedStyling};
      const idx=images.findIndex(image=>image.type===candidate.type);if(idx>=0)images[idx]=rec;else images.push(rec);
      po.aiImages[key]=images;candidate.supersededBy='active:'+candidate.url;candidate.supersededAt=new Date().toISOString();
      invalidateDependentSides(images,candidate.type);changed=true;
    }
  }
  return changed;
}
function invalidateDependentSides(images,frontType) {
  const sideType={'model-front':'model-side',female:'model-side-female',male:'model-side-male'}[frontType];
  if(!sideType)return;
  const side=images.find(image=>image.type===sideType);
  if(side){side.approved=false;delete side.stylingReview;side.qa={...(side.qa||{}),status:'needs-review',stylingChanged:false,issues:['Matching front image changed. Regenerate this three-quarter view.']};}
}
// Rejecting a draft is free and never deletes the permanent source or audit file.
function rejectGeneratedImage(po,{groupKey,type,url,reason,by},at=new Date().toISOString()) {
  if(!openaiPilot.IMAGE_TYPES.includes(type))throw new Error('Only generated listing views can be rejected.');
  if((po.lines||[]).some(line=>line.photoUrl===url||line.rawPhotoUrl===url)||Object.values(po.backRefs||{}).includes(url))throw new Error('Original references cannot be rejected.');
  const images=(po.aiImages||{})[groupKey]||[],held=(po.qaRejected||{})[groupKey]||[];
  const active=images.find(image=>image.type===type&&image.url===url);
  const candidate=active||held.find(image=>image.type===type&&image.url===url&&!image.supersededBy);
  if(!candidate)throw new Error('This image changed. Reopen the PO before rejecting it.');
  const retired=held.filter(image=>image.type===type&&!image.supersededBy);
  if(active)retired.push(active);
  po.imageRejectionHistory=po.imageRejectionHistory||[];
  for(const image of retired)po.imageRejectionHistory.push({...image,approved:false,groupKey,reason,by,rejectedAt:at});
  po.qaRejected=po.qaRejected||{};
  po.qaRejected[groupKey]=held.filter(image=>!retired.includes(image));
  if(active){
    po.aiImages[groupKey]=images.filter(image=>image!==active);
    invalidateDependentSides(po.aiImages[groupKey],type);
    for(const seo of po.seoDraft||[])if(seo.key===groupKey)seo.seoApproved=false;
  }
  return {images:(po.aiImages||{})[groupKey]||[],rejectedImages:po.qaRejected[groupKey]};
}
router.post('/api/procurement/pos/:id/reject-image',(req,res)=>{
  if(!canManagePurchases(req))return res.status(403).json({success:false,error:'Purchases access required.'});
  const s=loadStore(),po=s.pos[req.params.id],b=req.body||{};
  if(!po||isLockedPo(po))return res.status(409).json({success:false,error:'Editable PO required.'});
  if(expireStalePaidAttempts(po))saveStore(s);
  if(paidPilotInFlight.has(req.params.id)||((po.openaiPilot||{}).attempts||[]).some(item=>item.status==='running'))return res.status(409).json({success:false,error:'Wait for this PO’s generation to finish before rejecting an image.'});
  const reason=String(b.reason||'').trim();
  if(!reason||reason.length>500)return res.status(400).json({success:false,error:'Give a rejection reason (1–500 characters).'});
  try{
    const result=rejectGeneratedImage(po,{groupKey:String(b.groupKey||''),type:String(b.type||''),url:String(b.url||''),reason,by:String(req.user?.username||req.user?.role||'user')});
    saveStore(s);res.json({success:true,...result});
  }catch(e){res.status(409).json({success:false,error:e.message});}
});
router.get('/api/procurement/openai-pilot-status', (req,res) => {
  if (!canManagePurchases(req)) return res.status(403).json({success:false,error:'Purchases access required.'});
  res.json({success:true,configured:!!process.env.OPENAI_API_KEY,
    imageModel:process.env.PROCUREMENT_OPENAI_IMAGE_MODEL||'gpt-image-1.5',
    generationVersion:imageWorkflow.VERSION,generationEpoch:imageWorkflow.epochOf(loadStore()),
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
    if(!canReviewPaidImage(req))return res.status(403).json({success:false,error:'Paid image-generation access is required to use a saved exception image.'});
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
    const requestedStyling=candidate.requestedStyling||(attempt?.photoStyling&&JSON.stringify(openaiPilot.normalizeStyling(styling,group))===JSON.stringify(openaiPilot.normalizeStyling(attempt.photoStyling,group))?attempt.styling:styling);
    const modelView=openaiPilot.MODEL_VIEWS.includes(candidate.type);
    const changes=openaiPilot.stylingChanges(requestedStyling,(po.imageStyling||{})[key],group,candidate.type);
    if(modelView&&(!requestedStyling||changes.length))return res.status(409).json({success:false,error:'Model styling changed'+(changes.length?': '+changes.map(x=>x.field+' ('+x.before+' → '+x.after+')').join(', '):' or its saved settings are missing')+'. Restore the image’s styling or regenerate this view before review.',stylingChanges:changes});
    po.aiImages=po.aiImages||{};
    const images=po.aiImages[key]||[];
    const rec={type:candidate.type,label:(AI_IMAGE_SPECS.find(x=>x.type===candidate.type)||{}).label||candidate.type,url:candidate.url,approved:false,source:'openai-pilot',
      qa:{...candidate.qa,status:'manual-reviewed',issues:[],manualReview:{reason,by:String(req.user?.username||req.user?.role||'owner'),at:new Date().toISOString(),automatedCheck:structuredClone(candidate.qa)}},sourceFingerprint:fingerprint,
      styling:modelView?styling:null,requestedStyling:modelView?requestedStyling:null};
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
  let activeAttemptId;
  if (!canStartPaidPilot(req)) return res.status(403).json({success:false,error:'Paid image-generation access required.'});
  if (!process.env.OPENAI_API_KEY) return res.status(503).json({success:false,error:'Set OPENAI_API_KEY in Railway before starting the paid pilot.'});
  if (activePurchasePostings.has(lockKey) || activeDraftRecoveries.has(lockKey)) return res.status(409).json({success:false,error:'Shopify posting or recovery is running for this PO. Wait for it to finish before generating images.'});
  if (paidPilotInFlight.has(lockKey)) return res.status(409).json({success:false,error:'Another paid generation is running for this PO.'});
  paidPilotInFlight.add(lockKey);
  try {
    const s=loadStore(),po=s.pos[req.params.id],key=String((req.body||{}).groupKey||'');
    if (!po || isLockedPo(po)) return res.status(400).json({success:false,error:'Editable PO required.'});
    if(expireStalePaidAttempts(po))saveStore(s);
    if((req.body||{}).generationVersion!==imageWorkflow.VERSION || (req.body||{}).generationEpoch!==imageWorkflow.epochOf(s))
      return res.status(409).json({success:false,error:'Generation was stopped or this page is out of date. Refresh Purchases and confirm a new generation. No paid call was made.'});
    const startSnapshot=JSON.stringify(po),startEpoch=imageWorkflow.epochOf(s);
    const g=(await newGroupsOf(s,po)).find(x=>x.key===key);
    const latest=loadStore();
    if(JSON.stringify(latest.pos[req.params.id])!==startSnapshot || imageWorkflow.epochOf(latest)!==startEpoch)
      return res.status(409).json({success:false,error:'The PO or generation settings changed while preparing. Reopen the PO; no paid call was made.'});
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
    for(const image of savedImages)refreshImageStylingCheck(image,(po.imageStyling||{})[key],g);
    const usable=image=>image.type && image.url && readStoredPhoto(image.url) && imageCheckAccepted(image) && (!image.sourceFingerprint||image.sourceFingerprint===fingerprint);
    const heldImages=((po.qaRejected||{})[key]||[]).filter(image=>image&&image.url&&!image.supersededBy);
    const requestedRegeneration=(req.body||{}).regenerateTypes;
    if(requestedRegeneration!==undefined && (!Array.isArray(requestedRegeneration)||!requestedRegeneration.length||requestedRegeneration.length>allowedTypes.length||new Set(requestedRegeneration).size!==requestedRegeneration.length||requestedRegeneration.some(type=>!allowedTypes.includes(type)))) {
      return res.status(400).json({success:false,error:'Select supported image views to generate or regenerate.'});
    }
    const regenerateTypes=requestedRegeneration||[];
    const sideToFront={ 'model-side':'model-front', 'model-side-female':'female', 'model-side-male':'male' };
    const invalidTypes=allowedTypes.filter(type=>savedImages.some(x=>x.type===type&&x.url&&!usable(x)));
    const neededTypes=allowedTypes.filter(type=>!savedImages.some(x=>x.type===type&&x.url&&usable(x)));
    const existingSeo=(po.seoDraft||[]).find(x=>x.key===key);
    // A deterministic draft made when corrections were saved is NOT AI-written.
    // Preserve approved manual copy; replace unapproved placeholders with AI copy.
    const needsSeo=(req.body||{}).skipSeo!==true&&!regenerateTypes.length&&!(existingSeo&&existingSeo.seo&&(existingSeo.seoApproved||existingSeo.source==='openai-pilot'));
    const types=regenerateTypes.length?regenerateTypes:neededTypes;
    const confirmedTypes=(req.body||{}).confirmedTypes;
    if(confirmedTypes!==undefined&&(!Array.isArray(confirmedTypes)||new Set(confirmedTypes).size!==confirmedTypes.length||confirmedTypes.some(type=>!allowedTypes.includes(type))||types.some(type=>!confirmedTypes.includes(type))))
      return res.status(409).json({success:false,error:'Required views changed since the paid confirmation. Refresh the PO and confirm the current views. No paid call was made.'});
    const replaceTypes=new Set(regenerateTypes.concat(invalidTypes));
    if(types.some(type=>sideToFront[type]&&!types.includes(sideToFront[type])&&!savedImages.some(image=>image.type===sideToFront[type]&&image.url&&usable(image)))) {
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
    activeAttemptId=crypto.randomUUID();
    const attempt={id:activeAttemptId,workerId:paidPilotWorkerId,groupKey:key,sourceFingerprint:fingerprint,styling,regenerateTypes,maxImageAttempts,startedAt:new Date().toISOString(),status:'running',retry,views:[],errors:[]};
    activePaidAttemptIds.add(activeAttemptId);
    const controller=new AbortController(),signal=controller.signal;
    paidJobs.set(activeAttemptId,controller);
    po.openaiPilot.attempts.push(attempt);saveStore(s);
    res.status(202).json({success:true,groupKey:key,pilot:attempt});
    const imageModel=process.env.PROCUREMENT_OPENAI_IMAGE_MODEL||'gpt-image-1.5';
    const textModel=process.env.PROCUREMENT_OPENAI_TEXT_MODEL||'gpt-4.1-mini';
    const checkModel=process.env.PROCUREMENT_OPENAI_CHECK_MODEL||'gpt-4.1-mini';
    let preflightBlocked=false,safetyBlocked=false,jobFailure=null,photoStyling=styling;
    try {
      const fitPreflight=types.length?await openaiPilot.preflightFit({key:process.env.OPENAI_API_KEY,group:g,source,styling,signal,model:checkModel}):{status:'not-required',reason:'SEO-only job'};
      const fresh=loadStore(),current=fresh.pos[req.params.id],item=attemptRecord(current,activeAttemptId);
      if(jobStopped(current,activeAttemptId,signal))throw new DOMException('Image generation stopped by user.','AbortError');
      item.fitPreflight=fitPreflight;
      const freshGroup=(await newGroupsOf(fresh,current)).find(x=>x.key===key);
      if(!freshGroup||codexBatch.fingerprint(freshGroup,(current.backRefs||{})[key])!==fingerprint||JSON.stringify(openaiPilot.normalizeStyling((current.imageStyling||{})[key],freshGroup))!==JSON.stringify(styling)) {
        preflightBlocked=true;item.errors.push({type:'preflight',error:'Product photo or styling changed during the source check. No image call was made.'});
      } else if(fitPreflight.status==='conflict') {
        photoStyling=openaiPilot.stylingForPhoto(styling,fitPreflight);
        item.photoStyling=photoStyling;
        item.warnings=[`The selected ${styling.fit} fit/length conflicts with the original photo (${fitPreflight.reason}). Images will follow the photographed garment instead. Correct the saved fit/length before posting.`];
      }
      saveStore(fresh);
    } catch(e) {
      preflightBlocked=true;
      safetyBlocked=openaiPilot.isSafetyBlock(e);jobFailure=imageWorkflow.failureOf(e);
      const fresh=loadStore(),item=((fresh.pos[req.params.id]||{}).openaiPilot||{}).attempts?.find(x=>x.id===activeAttemptId);
      if(item){item.errors.push({type:'preflight',error:'Could not check the original fit: '+e.message+'. No image call was made.',...(e.api?{api:e.api}:{})});if(safetyBlocked)item.skippedViews=types;saveStore(fresh);}
    }
    if(!preflightBlocked)for(const type of types){
      if(jobStopped(loadStore().pos[req.params.id],activeAttemptId,signal))break;
      try {
        if (!replaceTypes.has(type)&&((po.aiImages||{})[key]||[]).some(x=>x.type===type && usable(x))) continue;
        const matchingFrontType=type==='model-side'?'model-front':type==='model-side-female'?'female':type==='model-side-male'?'male':'';
        const currentForReference=matchingFrontType?loadStore().pos[req.params.id]:null;
        if(matchingFrontType&&types.includes(matchingFrontType)&&!attemptRecord(currentForReference,activeAttemptId).views.some(view=>view.type===matchingFrontType)) {
          throw new Error('The new front model view did not pass its visual check. Three-quarter generation was skipped so it cannot reuse an older outfit.');
        }
        const matchingFront=matchingFrontType&&((currentForReference?.aiImages||{})[key]||[]).find(image=>image.type===matchingFrontType&&image.url&&imageCheckAccepted(image));
        const continuitySource=matchingFront?readStoredPhoto(matchingFront.url):null;
        if(matchingFrontType&&!continuitySource) throw new Error('Generate the matching front model view first so the three-quarter view can keep the same outfit.');
        const priorHeld=heldImages.slice().reverse().find(image=>image.type===type&&image.url&&!image.supersededBy);
        let repairFields=regenerateTypes.includes(type)&&priorHeld&&Array.isArray(priorHeld.qa&&priorHeld.qa.failed)?priorHeld.qa.failed:[];
        for(let imageAttempt=1;imageAttempt<=maxImageAttempts;imageAttempt++){
        const before=loadStore(),started=attemptRecord(before.pos[req.params.id],activeAttemptId);
        if(jobStopped(before.pos[req.params.id],activeAttemptId,signal))break;
        started.imageCalls=started.imageCalls||[];
        started.imageCalls.push({type,attempt:imageAttempt,startedAt:new Date().toISOString()});saveStore(before);
        const generated=await openaiPilot.generateImage({key:process.env.OPENAI_API_KEY,group:g,source:type==='back'?backSource:source,continuitySource,type,styling:photoStyling,repairFields,signal,model:imageModel});
        // Save a returned paid result before any checking call or catalogue
        // read. Cancellation, check outages and changed references keep it held.
        const saved=savePhotoBuffer(generated.buffer,'.png');
        const captured=loadStore(),capturedPo=captured.pos[req.params.id],capturedAttempt=attemptRecord(capturedPo,activeAttemptId);
        capturedPo.qaRejected=capturedPo.qaRejected||{};
        capturedPo.qaRejected[key]=capturedPo.qaRejected[key]||[];
        capturedPo.qaRejected[key].push({type,url:saved.url,qa:{status:'unavailable',failed:[],uncertain:['verification'],issues:['Visual check did not complete. Inspect this saved image against the original.']},sourceFingerprint:fingerprint,styling:photoStyling,requestedStyling:styling,attemptId:activeAttemptId,at:new Date().toISOString()});
        Object.assign(capturedAttempt.imageCalls[capturedAttempt.imageCalls.length-1],{completedAt:new Date().toISOString(),url:saved.url,usage:generated.usage||null});
        saveStore(captured);
        if(jobStopped(capturedPo,activeAttemptId,signal))break;
        let check;
        try {check=await openaiPilot.verifyImage({key:process.env.OPENAI_API_KEY,group:g,source:type==='back'?backSource:source,generated:generated.buffer,continuitySource,type,styling:photoStyling,signal,model:checkModel});}
        catch(e){
          check={status:'unavailable',failed:[],uncertain:['verification'],issues:['Visual check unavailable: '+e.message.slice(0,140)]};
          safetyBlocked=openaiPilot.isSafetyBlock(e);jobFailure=imageWorkflow.failureOf(e);
          const failed=loadStore(),failedAttempt=attemptRecord(failed.pos[req.params.id],activeAttemptId);
          failedAttempt.errors.push({type:'verification',error:e.message,...(e.api?{api:e.api}:{})});saveStore(failed);
        }
        let fresh=loadStore(),current=fresh.pos[req.params.id];
        const held=current.qaRejected[key].find(image=>image.url===saved.url);held.qa=check;saveStore(fresh);
        if(jobStopped(current,activeAttemptId,signal))break;
        const validationSnapshot=JSON.stringify(current);
        const freshGroup=current&&(await newGroupsOf(fresh,current)).find(x=>x.key===key);
        fresh=loadStore();current=fresh.pos[req.params.id];
        if(jobStopped(current,activeAttemptId,signal))break;
        if(JSON.stringify(current)!==validationSnapshot)throw new Error('Product changed while checking the generated image. The paid image was saved for review.');
        if(!freshGroup || codexBatch.fingerprint(freshGroup,(current.backRefs||{})[key])!==fingerprint) throw new Error('Product details changed during generation; result was kept in saved review drafts.');
        if(JSON.stringify(openaiPilot.normalizeStyling((current.imageStyling||{})[key],freshGroup))!==JSON.stringify(styling)) throw new Error('Styling changed during generation; result was kept in saved review drafts. Regenerate with the saved settings.');
        current.aiImages=current.aiImages||{};const images=current.aiImages[key]||[];
        const prior=images.find(x=>x.type===type);
        if(replaceTypes.has(type) && prior?.url!==savedImages.find(image=>image.type===type)?.url) throw new Error('This view changed during regeneration; result was kept in saved review drafts.');
        if(prior && prior.url && !replaceTypes.has(type)) throw new Error('An image already exists for this view; generated draft was not attached.');
        if(check.status!=='pass') {
          current.qaRejected=current.qaRejected||{};
          current.qaRejected[key]=Array.isArray(current.qaRejected[key])?current.qaRejected[key]:[];
          current.qaRejected[key]=current.qaRejected[key].slice(-12);
          const item=attemptRecord(current,activeAttemptId);
          const canRepair=!jobFailure&&openaiPilot.shouldRetryImageCheck(check,imageAttempt,maxImageAttempts);
          if(!canRepair)item.errors.push({type,error:'Visual check after '+imageAttempt+' image attempt(s): '+(check.issues.join('; ')||check.failed.join(', '))+'. Earlier image kept; inspect the held draft.'});
          saveStore(fresh);
          if(canRepair){repairFields=check.failed;continue;}
          break;
        }
        current.qaRejected[key]=current.qaRejected[key].filter(image=>image.url!==saved.url);
        const rec={type,label:(AI_IMAGE_SPECS.find(x=>x.type===type)||{}).label||type,url:saved.url,approved:false,source:'openai-pilot',qa:check,
          sourceFingerprint:fingerprint,styling:['female','male','model-front','model-side','model-side-female','model-side-male'].includes(type)?photoStyling:null,requestedStyling:styling};
        const idx=images.findIndex(x=>x.type===type);if(idx>=0)images[idx]=rec;else images.push(rec);
        for(const held of ((current.qaRejected||{})[key]||[]))if(held.type===type&&!held.supersededBy){held.supersededBy=rec.url;held.supersededAt=new Date().toISOString();}
        invalidateDependentSides(images,type);
        current.aiImages[key]=images;
        const item=attemptRecord(current,activeAttemptId);item.views.push({type,model:imageModel,attempts:imageAttempt,usage:generated.usage||null});
        saveStore(fresh);
        break;
        }
      }catch(e){
        safetyBlocked=openaiPilot.isSafetyBlock(e);jobFailure=imageWorkflow.failureOf(e);
        const fresh=loadStore(),item=((fresh.pos[req.params.id]||{}).openaiPilot||{}).attempts?.find(x=>x.id===activeAttemptId);
        if(item){item.errors.push({type,error:e.message,...(e.api?{api:e.api}:{})});if(safetyBlocked)item.skippedViews=types.slice(types.indexOf(type)+1);saveStore(fresh);}
        break;
      }
      if(jobFailure)break;
    }
    if (!preflightBlocked&&!jobFailure&&!jobStopped(loadStore().pos[req.params.id],activeAttemptId,signal)&&needsSeo) try {
      const generated=await openaiPilot.generateSeo({key:process.env.OPENAI_API_KEY,group:g,source,signal,model:textModel});
      const fresh=loadStore(),current=fresh.pos[req.params.id];
      const freshGroup=current&&(await newGroupsOf(fresh,current)).find(x=>x.key===key);
      if(!freshGroup || codexBatch.fingerprint(freshGroup,(current.backRefs||{})[key])!==fingerprint) throw new Error('Product details changed during generation; SEO was discarded.');
      const base=genSeo({...g,sizeCodeOf:label=>fresh.sizes[label]||label});
      const clean=(value,fallback)=>stripInternalCodes(String(value||'').trim(),g.designCode)||fallback;
      const draft=generated.seo;
      const retail=openaiPilot.retailFacts(g);
      let seo={displayName:clean(draft.displayName,base.displayName),title:clean(draft.title,base.title),
        handle:slugify([draft.displayName,g.colour,retail.productType,g.designCode].filter(Boolean).join(' ')) || base.handle,
        metaTitle:clean(draft.metaTitle,base.metaTitle).slice(0,70),metaDescription:clean(draft.metaDescription,base.metaDescription).slice(0,320),
        imageAlt:clean(draft.imageAlt,base.imageAlt),tags:draft.tags.map(x=>clean(x,'')).filter(Boolean),
        bodyHtml:clean(draft.bodyHtml,base.bodyHtml).replace(/<([^>]+)>/g,(tag,inside)=>/^\/?p$/i.test(inside.trim())?tag:'')};
      seo=canonicalSeoNaming(seo,g,siblingSeoStyle(current,g));
      if(seoNeedsReview(seo)) throw new Error('Generated SEO was incomplete or repetitive.');
      if(openaiPilot.seoCopyNeedsReview(seo,g)) throw new Error('Generated SEO did not meet the women’s top/fit and distinctive-name rules. The earlier draft was kept for review.');
      current.seoDraft=current.seoDraft||[];
      const rec={key,designCode:g.designCode,colour:g.colour,productType:g.productType,styleDescriptor:seo.styleDescriptor,seo,seoApproved:false,source:'openai-pilot'};
      const idx=current.seoDraft.findIndex(x=>x.key===key);if(idx>=0)current.seoDraft[idx]=rec;else current.seoDraft.push(rec);
      const item=attemptRecord(current,activeAttemptId);item.seo={model:textModel,usage:generated.usage||null};
      saveStore(fresh);
    }catch(e){
      safetyBlocked=openaiPilot.isSafetyBlock(e);jobFailure=imageWorkflow.failureOf(e);
      const fresh=loadStore(),item=((fresh.pos[req.params.id]||{}).openaiPilot||{}).attempts?.find(x=>x.id===activeAttemptId);
      if(item){item.errors.push({type:'seo',error:e.message,...(e.api?{api:e.api}:{})});saveStore(fresh);}
    }
    const done=loadStore(),donePo=done.pos[req.params.id],record=attemptRecord(donePo,activeAttemptId);
    record.failure=jobFailure;
    if(jobFailure)record.skippedViews=types.filter(type=>!record.imageCalls?.some(call=>call.type===type));
    record.status=jobStopped(donePo,activeAttemptId,signal)?'cancelled':safetyBlocked?'blocked':jobFailure?'stopped':record.errors.length?(record.views.length||record.seo?'partial':'failed'):'drafts-ready';
    record.completedAt=new Date().toISOString();saveStore(done);
  }catch(e){
    if (!res.headersSent) res.status(500).json({success:false,error:e.message});
    else {
      const s=loadStore(),record=(((s.pos[req.params.id]||{}).openaiPilot||{}).attempts||[]).find(x=>x.id===activeAttemptId);
      if(record){record.errors.push({type:'job',error:e.message});record.status=record.cancelRequestedAt?'cancelled':'failed';record.completedAt=new Date().toISOString();saveStore(s);}
    }
  }
  finally{paidJobs.delete(activeAttemptId);activePaidAttemptIds.delete(activeAttemptId);paidPilotInFlight.delete(lockKey);}
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
// Retire the unchecked legacy path; stale clients must reload before any charge.
router.post('/api/procurement/pos/:id/generate-images', (req,res) => {
  if(!canManagePurchases(req))return res.status(403).json({success:false,error:'Purchases access required.'});
  res.status(409).json({success:false,error:'This image-generation route is retired. Refresh Purchases and use the visually checked Generate image (paid) action. No paid call was made.'});
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
    if (!process.env.OPENAI_API_KEY) return res.status(503).json({ success: false, error: 'OpenAI SEO is not enabled. Set OPENAI_API_KEY in Railway.' });
    const s = loadStore();
    const po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found' });
    if (isLockedPo(po)) return res.status(400).json({ success: false, error: 'Posted or interrupted purchases cannot be edited.' });
    const b = req.body || {};
    if (!b.groupKey) return res.status(400).json({ success: false, error: 'groupKey required.' });
    const groups = await newGroupsOf(s, po);
    const g = groups.find(x => x.key === b.groupKey);
    if (!g) return res.status(404).json({ success: false, error: 'Product group not found.' });
    // Product approval now happens once, after images and SEO are reviewed.
    // Prefer a visually checked generated image even before that final approval;
    // fall back to the private raw reference only when no checked draft exists.
    const imgs = (po.aiImages && po.aiImages[g.key]) || [];
    const chosen = imgs.find(x => x.approved && (x.type === 'female' || x.type === 'male'))
                || imgs.find(x => imageCheckAccepted(x) && ['model-front','female','male','front'].includes(x.type))
                || imgs.find(x => x.approved) || null;
    const src = readStoredPhoto(chosen ? chosen.url : g.photoUrl);
    if (!src) return res.status(400).json({ success: false, error: 'No photo available to judge — generate/approve an image first.' });
    const generated = await openaiPilot.generateSeo({
      key: process.env.OPENAI_API_KEY,
      group: g,
      source: src,
      model: process.env.PROCUREMENT_OPENAI_TEXT_MODEL || 'gpt-4.1-mini'
    });
    const parsed = generated.seo;
    const sizeCodeOf = (label) => s.sizes[label] || label;
    // Deterministic fields still come from our own generator (handle uniqueness etc).
    // Clean the model's suggested name of any codes BEFORE using it as a fallback,
    // so genSeo's deterministic title/meta are built on clean copy too.
    const cleanDisplay = stripInternalCodes(String(parsed.displayName || '').trim(), g.designCode);
    const base = genSeo({ designName: cleanDisplay || g.designName, designCode: g.designCode, productType: g.productType, colour: g.colour, fit: g.fit, audience: g.audience, sizeLabels: g.sizeLabels, sizeCodeOf });
    // Belt-and-braces: strip codes from every customer-facing field the model returned.
    const clean = (v, fb) => stripInternalCodes(String(v || '').trim(), g.designCode) || fb;
    let seo = {
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
    seo = canonicalSeoNaming(seo, g, siblingSeoStyle(po, g));
    // Persist onto the PO's seoDraft (keyed by group) so it survives reloads/posts.
    po.seoDraft = Array.isArray(po.seoDraft) ? po.seoDraft : [];
    const di = po.seoDraft.findIndex(d => d.key === g.key);
    const rec = { key: g.key, designCode: g.designCode, colour: g.colour, productType: g.productType, styleDescriptor: seo.styleDescriptor, seo, seoApproved: false, source: 'openai' };
    if (di >= 0) po.seoDraft[di] = rec; else po.seoDraft.push(rec);
    saveStore(s);
    res.json({ success: true, groupKey: g.key, seo, source: 'openai' });
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// Persist edited/approved SEO from the studio (before posting).
router.post('/api/procurement/pos/:id/seo', async (req, res) => {
 try {
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
  const group = (await newGroupsOf(s, po)).find(g => g.key === b.groupKey);
  if (!group) return res.status(404).json({ success: false, error: 'Product group not found.' });
  const seo = canonicalSeoNaming(Object.assign({}, prev.seo, b.seo), group, prev.styleDescriptor || siblingSeoStyle(po, group));
  const rec = Object.assign({}, prev, { key: b.groupKey, designCode: group.designCode, colour: group.colour, productType: group.productType, styleDescriptor: seo.styleDescriptor, seo, seoApproved: !!b.seoApproved });
  if (di >= 0) po.seoDraft[di] = rec; else po.seoDraft.push(rec);
  saveStore(s);
  res.json({ success: true, seoDraft: po.seoDraft });
 } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

async function productApprovalContext(id) {
  const initial = loadStore(), before = initial.pos[id];
  if (!before || isLockedPo(before)) throw new Error('An editable purchase is required.');
  if (expireStalePaidAttempts(before)) saveStore(initial);
  const snapshot = JSON.stringify(before);
  const preview = await computePreview(initial, { lines: (before.lines || []).filter(line => num(line.qty) > 0), vendor: before.vendor, exRate: before.exRate, freightPerGram: before.freightPerGram, origin: before.origin, transportTotal: before.transportTotal });
  // Catalogue reads can yield to styling, generation or receipt saves. Never
  // overwrite a newer purchase with the snapshot that began approval.
  const s = loadStore(), po = s.pos[id];
  if (!po || JSON.stringify(po) !== snapshot) throw new Error('The purchase changed while checking approval. Reopen it and try again.');
  assertNoImageGeneration(po, 'approving');
  return { s, po, snapshot, products: preview.newProducts || [] };
}
function assertApprovalUnchanged(id, snapshot) {
  const current = loadStore().pos[id];
  if (!current || JSON.stringify(current) !== snapshot) throw new Error('The purchase changed while checking approval. Reopen it and try again.');
  assertNoImageGeneration(current, 'approving');
}
async function productApprovalDrafts(po, product) {
  const key = product.key, group = studioSourceGroup(po, product);
  if ((product.variantConflicts || []).length) throw new Error('Resolve the duplicate colour and size rows before approving.');
  const required = openaiPilot.pilotTypes(group, !!(po.backRefs || {})[key]);
  if (!required.length) throw new Error('Select Women, Men or Unisex before approving this product.');
  const images = ((po.aiImages || {})[key] || []);
  const fingerprint = codexBatch.fingerprint(group, (po.backRefs || {})[key]);
  const checked = [];
  for (const image of images) {
    refreshImageStylingCheck(image, (po.imageStyling || {})[key], group);
    if (required.includes(image.type) && image.url && image.url !== group.photoUrl && imageCheckAccepted(image) && (!image.sourceFingerprint || image.sourceFingerprint === fingerprint) && await readListingPhoto(image.url)) checked.push(image);
  }
  const missing = required.filter(type => !checked.some(image => image.type === type));
  if (missing.length) {
    const stylingChanged = images.filter(image => missing.includes(image.type) && image.qa?.stylingChanged).map(image => image.type);
    if (stylingChanged.length) throw new Error('Saved image styling differs for ' + stylingChanged.join(', ') + '. Restore image styling (free) or regenerate these views before approving.');
    const sourceChanged = images.filter(image => missing.includes(image.type) && image.sourceFingerprint && image.sourceFingerprint !== fingerprint).map(image => image.type);
    if (sourceChanged.length) throw new Error('Saved images no longer match the original product for ' + sourceChanged.join(', ') + '. Review the changed product reference before approving.');
    throw new Error('Review or generate the missing product views first: ' + missing.join(', ') + '.');
  }
  const draft = (po.seoDraft || []).find(item => item.key === key);
  if (!draft || !draft.seo || seoNeedsReview(draft.seo) || openaiPilot.seoCopyNeedsReview(draft.seo, group)) throw new Error('Generate and review the SEO names first.');
  return { images, seo: draft, checked, required };
}
function applyProductApproval(drafts) {
  drafts.images.forEach(image => { if (drafts.required.includes(image.type)) image.approved = drafts.checked.includes(image); });
  drafts.seo.seoApproved = true;
  return { images: drafts.images, seo: drafts.seo };
}

router.post('/api/procurement/pos/:id/approve-product', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    const key = String((req.body || {}).groupKey || '');
    if (!key) return res.status(400).json({ success: false, error: 'Choose a product first.' });
    const { s, po, snapshot, products } = await productApprovalContext(req.params.id);
    const product = products.find(item => item.key === key);
    if (!product) throw new Error('Product group not found.');
    const drafts = await productApprovalDrafts(po, product);
    assertApprovalUnchanged(req.params.id, snapshot);
    const approved = applyProductApproval(drafts);
    saveStore(s);
    res.json({ success: true, ...approved });
  } catch (e) { res.status(409).json({ success: false, error: e.message }); }
});

router.post('/api/procurement/pos/:id/approve-po', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    const { s, po, snapshot, products } = await productApprovalContext(req.params.id);
    const edits = (req.body || {}).seoDrafts;
    if (edits != null) {
      if (!Array.isArray(edits)) throw new Error('SEO edits must be a list.');
      const seen = new Set();
      for (const edit of edits) {
        const product = products.find(item => item.key === edit?.groupKey);
        if (!product || seen.has(edit.groupKey) || !edit.seo || typeof edit.seo !== 'object' || Array.isArray(edit.seo)) throw new Error('SEO edits contain an unknown or duplicate product. Reopen the PO and try again.');
        seen.add(edit.groupKey);
        const draft = (po.seoDraft || []).find(item => item.key === edit.groupKey);
        if (!draft || !draft.seo) throw new Error('Generate and review the SEO names first.');
        const group = studioSourceGroup(po, product);
        draft.seo = canonicalSeoNaming({ ...draft.seo, ...edit.seo }, group, draft.styleDescriptor || siblingSeoStyle(po, group));
        draft.styleDescriptor = draft.seo.styleDescriptor;
      }
    }
    const blockers = [], drafts = [];
    for (const product of products) {
      try { drafts.push(await productApprovalDrafts(po, product)); }
      catch (e) { blockers.push((product.designName || product.designCode || product.key) + ' · ' + product.colour + ': ' + e.message); }
    }
    if (blockers.length) return res.status(409).json({ success: false, error: 'Complete these products first — ' + blockers.join(' | ') });
    assertApprovalUnchanged(req.params.id, snapshot);
    drafts.forEach(applyProductApproval);
    saveStore(s);
    res.json({ success: true, approvedProducts: products.length, po: publicPo(po, req) });
  } catch (e) { res.status(409).json({ success: false, error: e.message }); }
});

// The gated write. Body carries the user-approved plan (edited SEO allowed).
router.post('/api/procurement/commit', async (req, res) => {
  const poId = String((req.body || {}).poId || '');
  let ownsLock = false;
  try {
    if (!SHOPIFY_STORE || !SHOPIFY_TOKEN) return res.status(400).json({ success: false, error: 'Shopify env not configured' });
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    if (activePurchasePostings.has(poId)) return res.status(409).json({ success:false, error:'Shopify posting is already running for this purchase. Check its progress before retrying.' });
    activePurchasePostings.add(poId); ownsLock = true;
    const s = loadStore(), b = req.body || {}, po = s.pos[b.poId];
    if (!b.approve || !po) return res.status(400).json({ success: false, error: 'A saved, approved purchase is required.' });
    if (po.status !== 'received') return res.status(409).json({ success: false, error: 'Purchase must be received and not already posted or partially posted.' });
    assertNoImageGeneration(po, 'posting to Shopify');
    const warehouseLocationId = String(s.settings.warehouseLocationId || '');
    if (!warehouseLocationId) return res.status(400).json({ success: false, error: 'Warehouse location not set — save it in Settings first.' });
    // Zero-quantity bill lines stay in the PO for audit, but never create a
    // Shopify product/variant or adjust existing stock.
    const receivedLines = (po.lines || []).filter(line => num(line.qty) > 0);
    if (!receivedLines.length) return res.status(400).json({ success: false, error: 'No received pieces to post.' });
    const preflightSnapshot = JSON.stringify(po);
    const preview = await computePreview(s, { lines: receivedLines, vendor: po.vendor, exRate: po.exRate,
      freightPerGram: po.freightPerGram, origin: po.origin, transportTotal: po.transportTotal, refresh:true });
    if (preview.counts.errors || preview.counts.ambiguous) return res.status(400).json({ success: false, error: 'Fix SKU or product-group errors before posting.' });
    const conflicts = preview.newProducts.flatMap(p => (p.variantConflicts || []).map(c => `${p.designCode || p.designName} / ${p.colour} / ${c.size}: ${c.skus.join(', ')}`));
    if (conflicts.length) return res.status(400).json({ success: false, error: 'Different SKUs have the same product, colour and size. Decide whether they are one article or separate products before posting: ' + conflicts.join('; ') });
    const allSkus = preview.lines.map(l => l.sku).filter(Boolean);
    if (allSkus.length !== preview.lines.length || new Set(allSkus).size !== allSkus.length) return res.status(400).json({ success: false, error: 'Every purchase line needs a unique SKU.' });
    for (const np of preview.newProducts) {
      const draft = (po.seoDraft || []).find(x => x.key === np.key);
      const seo = draft && draft.seo;
      const group = studioSourceGroup(po, np);
      if (!draft || !draft.seoApproved || seoNeedsReview(seo) || (group&&openaiPilot.seoCopyNeedsReview(seo,group))) {
        return res.status(400).json({ success: false, error: 'Approve complete, non-repetitive listing copy for every new product.' });
      }
      const required = openaiPilot.pilotTypes(group || {}, !!(po.backRefs || {})[np.key]);
      const currentFingerprint=group?codexBatch.fingerprint(group,(po.backRefs||{})[np.key]):'';
      // Older duplicate-row merges predate fingerprint carry-forward. Those
      // merges only combine the same size under the same original photograph,
      // so their reviewed images remain valid and can be repaired in place.
      const mergedSameArticle=(po.variantMergeHistory||[]).some(item=>item&&item.groupKey===np.key&&item.type==='duplicate-variant-merge');
      if(mergedSameArticle)for(const image of ((po.aiImages||{})[np.key]||[]))if(image&&image.url)image.sourceFingerprint=currentFingerprint;
      const approved = ((po.aiImages || {})[np.key] || []).filter(x => (group?.audience!=='Unisex'||required.includes(x.type)) && x.approved && imageCheckAccepted(x) && (!x.sourceFingerprint || x.sourceFingerprint===currentFingerprint) && x.type !== 'original' && x.url !== group?.photoUrl);
      const missingTypes=required.filter(type=>!approved.some(x=>x.type===type&&readStoredPhoto(x.url)));
      // Historical ENOSPC incidents could remove an older flat-front file while
      // leaving its approval record behind. If both approved model views still
      // exist, post those readable listing photos instead of charging for a
      // regeneration. The private vendor reference is never substituted.
      const readableApproved=approved.filter(image=>readStoredPhoto(image.url));
      const modelTypes=new Set(readableApproved.map(image=>image.type));
      const recoverableLostFlatFront=missingTypes.length===1&&missingTypes[0]==='front'&&
        (modelTypes.has('female')||modelTypes.has('male')||modelTypes.has('model-front'))&&
        (modelTypes.has('model-side-female')||modelTypes.has('model-side-male')||modelTypes.has('model-side'));
      if (!required.length || (missingTypes.length&&!recoverableLostFlatFront)) return res.status(400).json({ success: false, error: 'Cannot post '+(np.designName||np.designCode||np.colour||'product')+' ('+np.colour+'): missing readable approved view(s): '+(missingTypes.join(', ')||'required listing views')+'. The original reference photo cannot be posted.' });
      if(recoverableLostFlatFront){
        po.imageRecoveryHistory=Array.isArray(po.imageRecoveryHistory)?po.imageRecoveryHistory:[];
        if(!po.imageRecoveryHistory.some(item=>item&&item.groupKey===np.key&&item.type==='lost-flat-front'))po.imageRecoveryHistory.push({type:'lost-flat-front',groupKey:np.key,missingUrl:(approved.find(image=>image.type==='front')||{}).url||'',postedWith:readableApproved.map(image=>image.type),at:new Date().toISOString(),by:(req.user&&req.user.username)||'system'});
      }
      np.seo = seo;
      np.images = readableApproved.map(x => ({ url: x.url, alt: seo.imageAlt }));
      await validateListingPhotos(np.images);
    }
    // Reserve the PO before the first external write. Any uncertain/partial
    // result needs manual reconciliation, never a blind retry that duplicates stock.
    const currentStore = loadStore();
    assertNoImageGeneration(currentStore.pos[poId], 'posting to Shopify');
    if (JSON.stringify(currentStore.pos[poId]) !== preflightSnapshot || String(currentStore.settings.warehouseLocationId || '') !== warehouseLocationId)
      return res.status(409).json({ success:false, error:'This purchase changed during the posting check. Reopen it and review the latest quantities, images and approvals before posting.' });
    const results = { created: [], adjusted: [], errors: [] };
    po.postingAttemptId = require('crypto').randomUUID();
    po.status = 'posting_partial';
    po.postingStartedAt = new Date().toISOString();
    po.results = results;
    po.newProducts = preview.newProducts;
    po.existingAdds = preview.existingAdds;
    po.warehouseLocationId = warehouseLocationId;
    persistPostingPo(po);
    for (const np of preview.newProducts) {
      try {
        results.pendingOperation = { kind:'create', groupKey:np.key, attemptId:po.postingAttemptId, at:new Date().toISOString() };
        persistPostingPo(po);
        const result = await createDraftProduct(np, warehouseLocationId, { noCreateRetries:true, poId,
          onCreated: created => { results.created.push({ ...created, groupKey:np.key }); persistPostingPo(po); } });
        Object.assign(results.created[results.created.length - 1], result);
        delete results.pendingOperation;
        persistPostingPo(po);
        const stockError = (result.variants || []).find(v => v.stockError);
        if (stockError) throw new Error('Product created, but stock failed for ' + stockError.sku + ': ' + stockError.stockError);
      } catch (e) { results.errors.push({ kind: 'create', product: np.seo.title, error: e.message }); break; }
    }
    if (!results.errors.length) for (const ea of preview.existingAdds) {
      try {
        results.pendingOperation = { kind:'adjust', sku:ea.sku, qty:ea.qty, attemptId:po.postingAttemptId, at:new Date().toISOString() };
        persistPostingPo(po);
        const result = await addExistingInventory(ea, warehouseLocationId);
        if (result.error) throw new Error(result.error);
        results.adjusted.push(result);
        delete results.pendingOperation;
        persistPostingPo(po);
      } catch (e) { results.errors.push({ kind: 'adjust', sku: ea.sku, error: e.message }); break; }
    }
    po.newProducts = preview.newProducts;
    po.existingAdds = preview.existingAdds;
    po.warehouseLocationId = warehouseLocationId;
    if (!results.errors.length) {
      po.status = 'posted';
      po.postedAt = new Date().toISOString();
    }
    persistPostingPo(po);
    _catalogue = null;
    if (results.errors.length) return res.status(409).json({ success: false, poId: po.id, results,
      error: 'Shopify posting stopped after an error. This PO is locked as partially posted; inspect Shopify and the saved results before any retry. ' + results.errors[0].error });
    res.json({ success: true, poId: po.id, results });
  } catch (e) { res.status(e.status || 500).json({ success: false, error: e.message }); }
  finally { if (ownsLock) activePurchasePostings.delete(poId); }
});

// Continue an interrupted product-creation phase without duplicating anything
// already visible in Shopify. Shopify is re-read first; a design is created
// only when none of its received SKUs exists. A partly present design is held
// for manual reconciliation because creating only its missing sizes would
// split one article across two Shopify products.
const activePostingContinuations = activePurchasePostings;
router.post('/api/procurement/pos/:id/resume-posting', async (req, res) => {
  let ownsLock = false;
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    if (activePostingContinuations.has(req.params.id)) return res.status(409).json({success:false,error:'Posting continuation is already running for this purchase.'});
    activePostingContinuations.add(req.params.id); ownsLock = true;
    const s=loadStore(), po=s.pos[req.params.id];
    if(!po) return res.status(404).json({success:false,error:'PO not found'});
    if(po.status!=='posting_partial') return res.status(409).json({success:false,error:'Only an interrupted posting can be continued.'});
    assertNoImageGeneration(po, 'continuing Shopify posting');
    const continuationSnapshot = JSON.stringify(po);
    if(((po.results&&po.results.errors)||[]).length) return res.status(409).json({success:false,error:'This posting has a saved Shopify error. Reconcile that error before continuing.'});
    if (po.results && po.results.pendingOperation) return res.status(409).json({success:false,error:'A Shopify write was interrupted with an uncertain result. Reconcile the saved operation in Shopify before continuing.', pendingOperation:po.results.pendingOperation});
    if ((po.existingAdds || []).some(add => !((po.results || {}).adjusted || []).some(done => done.sku === add.sku)))
      return res.status(409).json({success:false,error:'This interrupted posting has unconfirmed restock quantities. Reconcile the stock adjustments before marking this PO posted.'});
    const received=(po.lines||[]).filter(line=>num(line.qty)>0), missingImageGroups=[];
    const cat=await loadCatalogue(true), byGroup=new Map();
    received.filter(line=>line.classification!=='EXISTING').forEach(line=>{
      const key=groupKey(line); if(!byGroup.has(key))byGroup.set(key,[]); byGroup.get(key).push(line);
    });
    const alreadyPresent=[], missing=[], partial=[];
    byGroup.forEach((lines,key)=>{
      const present=lines.filter(line=>cat.skuMap[String(line.sku||'').toUpperCase()]);
      if(!present.length)missing.push(key);
      else if(present.length===lines.length)alreadyPresent.push({key,skus:lines.map(line=>line.sku)});
      else partial.push({key,present:present.map(line=>line.sku),missing:lines.filter(line=>!cat.skuMap[String(line.sku||'').toUpperCase()]).map(line=>line.sku)});
    });
    if(partial.length)return res.status(409).json({success:false,error:'A Shopify product is only partly present. No write was made; reconcile these variants first.',partial});
    const preview=await computePreview(s,{lines:received,vendor:po.vendor,exRate:po.exRate,freightPerGram:po.freightPerGram,origin:po.origin,transportTotal:po.transportTotal,refresh:true});
    const pending=(preview.newProducts||[]).filter(np=>missing.includes(np.key));
    if(pending.length!==missing.length)return res.status(409).json({success:false,error:'The saved PO and current Shopify catalogue do not produce the same missing product groups. No write was made.',missing,pending:pending.map(np=>np.key)});
    for(const np of pending){
      const draft=(po.seoDraft||[]).find(x=>x.key===np.key), seo=draft&&draft.seo;
      const group=studioSourceGroup(po,np);
      if(!draft||!draft.seoApproved||seoNeedsReview(seo)||openaiPilot.seoCopyNeedsReview(seo,group))return res.status(409).json({success:false,error:'Saved SEO is no longer approved for '+(np.designName||np.key)+'. No Shopify write was made.'});
      const required=openaiPilot.pilotTypes(group,!!(po.backRefs||{})[np.key]);
      const fingerprint=codexBatch.fingerprint(group,(po.backRefs||{})[np.key]);
      // Match the normal posting path: a duplicate-size merge keeps the same
      // article and source photo, so its paid, reviewed images may safely carry
      // the current group fingerprint instead of being treated as missing.
      const mergedSameArticle=(po.variantMergeHistory||[]).some(item=>item&&item.groupKey===np.key&&item.type==='duplicate-variant-merge');
      if(mergedSameArticle)for(const image of ((po.aiImages||{})[np.key]||[]))if(image&&image.url)image.sourceFingerprint=fingerprint;
      const approved=((po.aiImages||{})[np.key]||[]).filter(x=>(group.audience!=='Unisex'||required.includes(x.type))&&x.approved&&imageCheckAccepted(x)&&(!x.sourceFingerprint||x.sourceFingerprint===fingerprint)&&x.type!=='original'&&x.url!==group.photoUrl&&readStoredPhoto(x.url));
      const missingTypes=required.filter(type=>!approved.some(x=>x.type===type));
      const modelTypes=new Set(approved.map(x=>x.type));
      const recoverable=missingTypes.length===1&&missingTypes[0]==='front'&&(modelTypes.has('female')||modelTypes.has('male')||modelTypes.has('model-front'))&&(modelTypes.has('model-side-female')||modelTypes.has('model-side-male')||modelTypes.has('model-side'));
      const changedReference=((po.aiImages||{})[np.key]||[]).some(image=>image.approved&&imageCheckAccepted(image)&&image.sourceFingerprint&&image.sourceFingerprint!==fingerprint&&readStoredPhoto(image.url));
      if(!required.length||(missingTypes.length&&!recoverable))return res.status(409).json({success:false,error:(changedReference?'Previously approved images no longer match the current product reference for ':'Missing readable approved image(s) for ')+(np.designName||np.designCode||'Trouser')+' · '+(np.colour||'')+': '+(missingTypes.join(', ')||'required listing views')+'. No Shopify write was made. '+(changedReference?'Review the changed product details before generating replacements.':'Restore or regenerate the missing views, then retry.'),groupKey:np.key,missingTypes});
      if(!approved.length)return res.status(409).json({success:false,error:'No readable approved listing photos remain for '+(np.designName||np.designCode||'Trouser')+' · '+(np.colour||'')+'. No Shopify write was made.',groupKey:np.key});
      np.seo=seo; np.images=approved.map(x=>({url:x.url,alt:seo.imageAlt}));
      await validateListingPhotos(np.images);
    }
    assertNoImageGeneration(loadStore().pos[po.id], 'continuing Shopify posting');
    if (JSON.stringify(loadStore().pos[po.id]) !== continuationSnapshot)
      return res.status(409).json({success:false,error:'This purchase changed during the continuation check. Reopen it and review the latest data before retrying.'});
    const results=po.results||(po.results={created:[],adjusted:[],errors:[]});
    po.newProducts=(po.newProducts||[]).filter(np=>!pending.some(created=>created.key===np.key)).concat(pending);
    po.postingResumedAt=new Date().toISOString(); persistPostingPo(po);
    for(const np of pending){
      try {
        results.pendingOperation = {kind:'create',groupKey:np.key,attemptId:crypto.randomUUID(),at:new Date().toISOString()};
        persistPostingPo(po);
        const result=await createDraftProduct(np,String(po.warehouseLocationId||s.settings.warehouseLocationId||''),{
          noCreateRetries:true,poId:po.id,onCreated:created=>{results.created.push({...created,groupKey:np.key});persistPostingPo(po);}
        });
        Object.assign(results.created[results.created.length-1],result);
        delete results.pendingOperation;persistPostingPo(po);
        const stockError=(result.variants||[]).find(v=>v.stockError);
        if(stockError)throw new Error('Product created, but stock failed for '+stockError.sku+': '+stockError.stockError);
      }
      catch(e){results.errors.push({kind:'create',product:np.seo.title,error:e.message});persistPostingPo(po);return res.status(409).json({success:false,error:'Continuation stopped after a Shopify error. '+e.message,results});}
    }
    _catalogue=null;
    const verified=await loadCatalogue(true), stillMissing=received.filter(line=>line.classification!=='EXISTING'&&!verified.skuMap[String(line.sku||'').toUpperCase()]).map(line=>line.sku);
    if(stillMissing.length)return res.status(409).json({success:false,error:'Shopify verification still found missing received SKUs. The PO remains locked.',stillMissing,results});
    po.newProducts=(po.newProducts||[]).filter(np=>!pending.some(created=>created.key===np.key)).concat(pending);
    po.status='posted'; po.postedAt=new Date().toISOString(); po.postingReconciliation={at:po.postedAt,by:(req.user&&req.user.username)||'system',alreadyPresent,createdGroups:pending.map(np=>np.key),missingImageGroups}; persistPostingPo(po);
    res.json({success:true,poId:po.id,alreadyPresent,created:pending.map(np=>np.key),missingImageGroups,results});
  }catch(e){res.status(e.status || 500).json({success:false,error:e.message});}
  finally{if(ownsLock)activePostingContinuations.delete(req.params.id);}
});

// Repair products from the historical resume bug that allowed Shopify drafts
// to be created after their local approved image files had been skipped.
// Existing Shopify images are never replaced; only an empty image list is
// filled from readable, approved listing views already saved on the PO.
router.post('/api/procurement/pos/:id/repair-shopify-images', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({ success: false, error: 'Purchases access required.' });
    const s = loadStore(), po = s.pos[req.params.id];
    if (!po) return res.status(404).json({ success: false, error: 'PO not found.' });
    if (po.status !== 'posted') return res.status(409).json({ success: false, error: 'This repair is only for a posted PO.' });
    assertNoImageGeneration(po, 'repairing Shopify photos');
    const groups = await newGroupsOf(s, po), cat = await loadCatalogue(true);
    const repaired = [], alreadyHadPhotos = [], unavailable = [];
    for (const group of groups) {
      const lineSkus = (po.lines || []).filter(line => num(line.qty) > 0 && groupKey(line) === group.key).map(line => String(line.sku || '').toUpperCase()).filter(Boolean);
      const productIds = Array.from(new Set(lineSkus.map(sku => cat.skuMap[sku] && cat.skuMap[sku].productId).filter(Boolean)));
      if (productIds.length !== 1) { unavailable.push({ groupKey: group.key, reason: 'Could not identify one Shopify product from the PO SKUs.' }); continue; }
      const productId = productIds[0];
      const detailResponse = await shopifyClient.request(`https://${SHOPIFY_STORE}/admin/api/${API}/products/${productId}.json?fields=id,title,images`);
      if (!detailResponse.ok) throw new Error('Shopify could not read product ' + productId + '.');
      const detail = (await detailResponse.json()).product || {};
      if ((detail.images || []).length) { alreadyHadPhotos.push({ groupKey: group.key, productId, count: detail.images.length }); continue; }
      const seoDraft = (po.seoDraft || []).find(item => item.key === group.key), alt = seoDraft?.seo?.imageAlt || '';
      const approved = ((po.aiImages || {})[group.key] || []).filter(image => image && (group.audience!=='Unisex'||openaiPilot.pilotTypes(group,!!(po.backRefs||{})[group.key]).includes(image.type)) && image.approved && imageCheckAccepted(image) && image.type !== 'original' && image.url !== group.photoUrl && readStoredPhoto(image.url));
      if (!approved.length) { unavailable.push({ groupKey: group.key, productId, reason: 'No readable approved saved photos remain. Restore or regenerate this product’s listing views.' }); continue; }
      await validateListingPhotos(approved);
      for (const image of approved) {
        const src = await readListingPhoto(image.url);
        assertNoImageGeneration(loadStore().pos[po.id], 'repairing Shopify photos');
        await shopifyPost(`products/${productId}/images.json`, { image: { attachment: src.buf.toString('base64'), alt: alt.slice(0, 512) } });
      }
      const verifyResponse = await shopifyClient.request(`https://${SHOPIFY_STORE}/admin/api/${API}/products/${productId}.json?fields=id,images`);
      const verified = verifyResponse.ok ? ((await verifyResponse.json()).product || {}) : {};
      const count = (verified.images || []).length;
      if (!count) throw new Error('Shopify did not confirm the repaired photos for product ' + productId + '.');
      repaired.push({ groupKey: group.key, productId, count });
    }
    po.shopifyImageRepairHistory = Array.isArray(po.shopifyImageRepairHistory) ? po.shopifyImageRepairHistory : [];
    po.shopifyImageRepairHistory.push({ at: new Date().toISOString(), by: (req.user && req.user.username) || 'system', repaired, alreadyHadPhotos, unavailable });
    saveStore(s); _catalogue = null;
    res.json({ success: true, repaired, alreadyHadPhotos, unavailable });
  } catch (e) { res.status(e.status || 500).json({ success: false, error: e.message }); }
});

// This catalogue is always fresh and retains duplicate SKU matches instead of
// silently choosing one product. It is used only by deleted-draft recovery.
async function recoveryCatalogue() {
  const catalogue = {};
  let url = `https://${SHOPIFY_STORE}/admin/api/${API}/products.json?limit=250&fields=id,status,variants`;
  while (url) {
    const response = await shopifyClient.request(url);
    if (!response.ok) throw new Error('Shopify could not check recovery SKUs: HTTP ' + response.status);
    const body = await response.json();
    for (const product of body.products || []) for (const variant of product.variants || []) {
      const sku = String(variant.sku || '').trim().toUpperCase();
      if (!sku) continue;
      (catalogue[sku] || (catalogue[sku] = [])).push({productId: String(product.id), status: product.status,
        variantId: String(variant.id), inventoryItemId: String(variant.inventory_item_id || '')});
    }
    const next = (response.headers.get('Link') || '').match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }
  return catalogue;
}
function recoveryPriceCalculator(s, po) {
  const lines = pricedLinesForPo(s, po);
  return line => {
    const index = (po.lines || []).indexOf(line);
    const priced = lines[index] || lines.find(candidate => candidate.sku === line.sku);
    return priced ? priced.suggestedMrp : 0;
  };
}
async function recoveryPlan(s, po, inventoryMode, includeRestocks = false, priceMode = 'saved') {
  return buildRecoveryPlan(po, await recoveryCatalogue(), {groupKey, sizes: s.sizes || {}, readPhoto: readStoredPhoto, inventoryMode, includeRestocks, priceMode, calculatePrice:recoveryPriceCalculator(s,po)});
}
const activeDraftRecoveries = new Set();
function recoveryHistory(id) {
  const s = loadStore(), po = s.pos[id];
  if (!po) return null;
  let changed = false;
  const history = po.shopifyDraftRecoveryHistory || [];
  if (!activeDraftRecoveries.has(id)) for (const run of history) {
    if (run.status !== 'running') continue;
    run.status = 'needs-reconciliation';
    run.finishedAt = new Date().toISOString();
    (run.errors || (run.errors = [])).push('Recovery was interrupted by a service restart. Check saved drafts before recovering the remaining products.');
    changed = true;
  }
  if (changed) saveStore(s);
  return {active:activeDraftRecoveries.has(id), history};
}
function persistDraftRecovery(poId, run, link) {
  const latest = loadStore(), po = latest.pos[poId];
  if (!po || po.status !== 'posted') throw new Error('The posted purchase changed during recovery.');
  const history = po.shopifyDraftRecoveryHistory || (po.shopifyDraftRecoveryHistory = []);
  const index = history.findIndex(item => item.id === run.id);
  if (index < 0) history.push(JSON.parse(JSON.stringify(run))); else history[index] = JSON.parse(JSON.stringify(run));
  if (link) {
    po.shopifyRecoveryLinks = po.shopifyRecoveryLinks || {};
    po.shopifyRecoveryLinks[link.groupKey] = {productId:link.productId, at:run.at, recoveryId:run.id};
  }
  saveStore(latest);
}
router.get('/api/procurement/pos/:id/shopify-recovery', async (req, res) => {
  try {
    if (!canManagePurchases(req)) return res.status(403).json({success:false,error:'Purchases access required.'});
    const progress = recoveryHistory(req.params.id);
    const s = loadStore(), po = s.pos[req.params.id];
    if (!po) return res.status(404).json({success:false,error:'PO not found.'});
    const plan = await recoveryPlan(s, po, req.query.inventoryMode || 'zero', req.query.includeRestocks === 'true', req.query.priceMode || 'saved');
    res.json({success:true, plan:publicRecoveryPlan(plan), ...progress});
  } catch (error) { res.status(409).json({success:false,error:error.message}); }
});
// Progress reads never wait for the Shopify catalogue or its request queue.
router.get('/api/procurement/pos/:id/shopify-recovery/status', (req, res) => {
  if (!canManagePurchases(req)) return res.status(403).json({success:false,error:'Purchases access required.'});
  const progress = recoveryHistory(req.params.id);
  if (!progress) return res.status(404).json({success:false,error:'PO not found.'});
  res.set('Cache-Control', 'no-store').json({success:true,...progress});
});
async function executeDraftRecovery(id, plan, warehouse, run) {
  try {
    for (const product of plan.products.filter(product => product.status === 'ready')) {
      run.phase = 'checking-skus'; persistDraftRecovery(id, run);
      const catalogue = await recoveryCatalogue();
      if (product.skus.some(sku => (catalogue[sku] || []).length)) throw new Error('Shopify now contains SKU(s) for ' + product.label + '. Refresh the preview.');
      const latest = loadStore(), current = buildRecoveryPlan(latest.pos[id], catalogue,
        {groupKey, sizes:latest.sizes || {}, readPhoto:readStoredPhoto, inventoryMode:plan.inventoryMode, includeRestocks:plan.includeRestocks, priceMode:plan.priceMode, calculatePrice:recoveryPriceCalculator(latest,latest.pos[id])}).products.find(candidate => candidate.key === product.key);
      if (JSON.stringify(current) !== JSON.stringify(product)) throw new Error('The saved listing or photos changed for ' + product.label + '. Refresh the preview.');
      assertNoImageGeneration(latest.pos[id], 'recovering Shopify drafts');
      await validateListingPhotos(product.images);
      assertNoImageGeneration(loadStore().pos[id], 'recovering Shopify drafts');
      const result = await createDraftProduct({...product, images:product.images, seo:{...product.seo,
        tags:Array.isArray(product.seo.tags) ? product.seo.tags : []}}, warehouse, {noCreateRetries:true,poId:id,
        beforeCreate:()=>{
          const latest=loadStore(), checked=buildRecoveryPlan(latest.pos[id],catalogue,
            {groupKey,sizes:latest.sizes || {},readPhoto:readStoredPhoto,inventoryMode:plan.inventoryMode,includeRestocks:plan.includeRestocks,priceMode:plan.priceMode,calculatePrice:recoveryPriceCalculator(latest,latest.pos[id])}).products.find(candidate=>candidate.key===product.key);
          if(JSON.stringify(checked)!==JSON.stringify(product))throw new Error('The saved listing or photos changed for '+product.label+'. Refresh the preview.');
          // Reserve only after local decoding and the final data check; an
          // uncertain external creation remains durable until verification.
          run.currentGroup=product.key;run.phase='creating';persistDraftRecovery(id,run);
        },
        onCreated:created => {run.phase = 'setting-stock'; run.created.push({...created, groupKey:product.key}); persistDraftRecovery(id,run,{...created,groupKey:product.key});}});
      Object.assign(run.created[run.created.length - 1], result);
      run.phase = 'verifying'; persistDraftRecovery(id, run); _catalogue = null;
      const stockError = result.variants.find(variant => variant.stockError);
      if (stockError) throw new Error('Draft created, but stock setup needs reconciliation for ' + stockError.sku + ': ' + stockError.stockError);
      const verified = await recoveryCatalogue();
      if (!product.skus.every(sku => (verified[sku] || []).length === 1 && verified[sku][0].productId === result.productId && verified[sku][0].status === 'draft'))
        throw new Error('Shopify did not confirm the complete recreated draft for ' + product.label + '. Inspect Shopify before retrying.');
      run.completed.push(product.key); delete run.currentGroup; persistDraftRecovery(id, run);
    }
    run.status = 'complete'; run.phase = 'complete'; run.finishedAt = new Date().toISOString(); persistDraftRecovery(id, run);
    return run.created;
  } catch (error) {
    run.status = 'needs-reconciliation'; run.finishedAt = new Date().toISOString(); run.errors.push(error.message);
    persistDraftRecovery(id, run);
    throw error;
  } finally { activeDraftRecoveries.delete(id); }
}
router.post('/api/procurement/pos/:id/shopify-recovery', async (req, res) => {
  const id = req.params.id;
  let ownsLock = false, run;
  try {
    if (!canManagePurchases(req)) return res.status(403).json({success:false,error:'Purchases access required.'});
    if (activeDraftRecoveries.has(id)) return res.status(409).json({success:false,error:'Recovery is already running for this purchase.'});
    recoveryHistory(id);
    activeDraftRecoveries.add(id); ownsLock = true;
    const s = loadStore(), po = s.pos[id], b = req.body || {};
    if (!po) return res.status(404).json({success:false,error:'PO not found.'});
    assertNoImageGeneration(po, 'recovering Shopify drafts');
    const recoverySnapshot = JSON.stringify(po);
    if (b.approve !== true || !b.fingerprint) return res.status(400).json({success:false,error:'Review the recovery preview before recreating drafts.'});
    const plan = await recoveryPlan(s, po, b.inventoryMode, b.includeRestocks === true, b.priceMode || 'saved');
    if (plan.fingerprint !== b.fingerprint) return res.status(409).json({success:false,error:'The saved receipt, photos or Shopify products changed. Refresh the recovery preview; no products were created.'});
    const pending = plan.products.filter(product => product.status === 'ready');
    for (const product of pending) await validateListingPhotos(product.images);
    const latest = loadStore();
    assertNoImageGeneration(latest.pos[id], 'recovering Shopify drafts');
    if (JSON.stringify(latest.pos[id]) !== recoverySnapshot) return res.status(409).json({success:false,error:'The purchase changed during the recovery check. Refresh the recovery preview; no products were created.'});
    const warehouse = String(po.warehouseLocationId || s.settings.warehouseLocationId || '');
    if (!warehouse) return res.status(409).json({success:false,error:'The original warehouse location is missing. No products were created.'});
    if (!pending.length) return res.json({success:true,created:[],plan:publicRecoveryPlan(plan)});
    run = {id:crypto.randomUUID(), at:new Date().toISOString(), by:(req.user || {}).username || 'system',
      inventoryMode:plan.inventoryMode, includeRestocks:plan.includeRestocks, priceMode:plan.priceMode, status:'running', created:[], completed:[], total:pending.length,
      pieces:pending.reduce((sum,p)=>sum+p.variants.reduce((qty,v)=>qty+v.qty,0),0), errors:[], skippedRestocks:plan.excludedRestocks};
    persistDraftRecovery(id, run);
    const task = executeDraftRecovery(id, plan, warehouse, run);
    ownsLock = false; // The worker holds the lock until its durable result is saved.
    if (b.background === true) {
      void task.catch(error => console.error('[purchase-recovery]', id, run.id, error.message));
      return res.status(202).json({success:true,runId:run.id,status:'running'});
    }
    res.json({success:true,created:await task,plan:publicRecoveryPlan(plan)});
  } catch (error) {
    res.status(409).json({success:false,error:error.message,created:run && run.created || []});
  } finally { if (ownsLock) activeDraftRecoveries.delete(id); }
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
  const s = loadStore(), body = req.body || {}, ids = body.poIds, historicalBills = Array.isArray(body.historicalBills) ? body.historicalBills : [];
  if (!Array.isArray(ids) || ids.length < 1 || ids.length !== new Set(ids).size || ids.some(id => typeof id !== 'string'))
    return res.status(400).json({ success: false, error: 'Select one or more distinct purchase bills.' });
  const historicalById = new Map(historicalBills.map(item => [String(item.id || ''), item]));
  const manualHistorical = ids.every(id => historicalById.has(id));
  if (manualHistorical && ids.some(id => !/^HIST-202608\d{2}-/.test(id) || historicalById.get(id).manualVendorBill !== true))
    return res.status(400).json({ success: false, error: 'Manual vendor calculations are available only for the nine recovered August bills.' });
  const pos = manualHistorical ? ids.map(id => historicalById.get(id)) : ids.map(id => s.pos[id]);
  if (pos.some(po => !po || (!manualHistorical && po.historical)) || (!manualHistorical && historicalBills.length))
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
    origin: manualHistorical ? 'historical' : (pos.every(po => po.origin === pos[0].origin) ? pos[0].origin : 'mixed'),
    manualHistorical, historicalBills: manualHistorical ? Object.fromEntries(ids.map(id => [id, {
      id, vendor: String(historicalById.get(id).vendor || 'Not recorded'), datePurchase: String(historicalById.get(id).datePurchase || ''),
      productCount: Math.max(0, Number(historicalById.get(id).productCount) || 0)
    }])) : undefined,
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
  for (const key of (invoice.manualHistorical ? ['localTransportationYuan', 'fixedTransportationYuan', 'extraChargesYuan', 'combinedFreightInr', 'exchangeRate'] : ['totalWeightGrams', 'localTransportationYuan', 'fixedTransportationYuan', 'extraChargesYuan', 'combinedFreightYuan', 'combinedFreightInr', 'exchangeRate'])) {
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
router.delete('/api/procurement/combined-invoices/:id', (req, res) => {
  if (!canReconcileVendorBill(req)) return res.status(403).json({ success: false, error: 'Purchases or accounting access required.' });
  const s = loadStore(), invoice = s.combinedVendorInvoices[req.params.id];
  if (!invoice) return res.status(404).json({ success: false, error: 'Invoice calculation not found.' });
  if (invoice.finalized) return res.status(409).json({ success: false, error: 'Reopen the finalized LG bill before cancelling its calculation.' });
  delete s.combinedVendorInvoices[req.params.id];
  saveStore(s);
  res.json({ success: true, cancelledId: req.params.id, poIds: invoice.poIds || [] });
});
router.post('/api/procurement/combined-invoices/:id/finalize', (req, res) => {
  if (!canReconcileVendorBill(req)) return res.status(403).json({ success: false, error: 'Purchases or accounting access required.' });
  const s = loadStore(), invoice = s.combinedVendorInvoices[req.params.id], basis = String((req.body || {}).basis || '');
  if (!invoice) return res.status(404).json({ success: false, error: 'Invoice calculation not found.' });
  if (invoice.finalized) return res.status(409).json({ success: false, error: 'This LG bill is already finalized.' });
  if (!['purchase', 'vendor'].includes(basis) || (invoice.manualHistorical && basis !== 'vendor')) return res.status(400).json({ success: false, error: invoice.manualHistorical ? 'Historical bills must use the manually entered vendor invoice calculation.' : 'Choose Purchase Summary or vendor invoice as the payable amount.' });
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
router.get('/api/procurement/history', async (req, res) => {
  const s = loadStore();
  const finalized = finalizedByPo(s);
  let accounting = null;
  try { accounting = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'expenses.json'), 'utf8')); } catch { /* Unavailable history must not imply unpaid. */ }
  // Stored POs remain authoritative. Add only Shopify products that are not
  // already linked to a stored PO on the same purchase/posting date.
  const pos = Object.values(s.pos)
    .filter(p => !p.historical)
    .map(p => ({ ...publicPo(p, req), paymentSummary: purchasePaymentStatus(p, accounting, canReconcileVendorBill(req), s.settings, finalized[p.id] && finalized[p.id].amount) }));
  try {
    const recovered = await loadShopifyPurchaseHistory(req.query.refresh === '1');
    const linkedProducts = new Map();
    Object.values(s.pos).forEach(po => {
      const poDate = String(po.postedAt || po.datePurchase || po.createdAt || '').slice(0, 10);
      ((po.results && po.results.created) || []).forEach(product => {
        if (product && product.productId) linkedProducts.set(String(product.productId), poDate);
      });
    });
    const historical = recovered.map(batch => {
      const products = (batch.products || []).filter(product => linkedProducts.get(String(product.productId)) !== batch.datePurchase);
      const vendorNames = Array.from(new Set(products.map(product => String(product.vendor || '').trim()).filter(Boolean)));
      return { ...batch, products, vendorNames,
        productCount: products.length,
        skuCount: products.reduce((n, product) => n + (product.skus || []).length, 0) };
    }).filter(batch => batch.productCount > 0);
    const history = pos.concat(historical).sort((a, b) =>
      String(b.datePurchase || b.createdAt || '').localeCompare(String(a.datePurchase || a.createdAt || '')) || String(b.id).localeCompare(String(a.id))
    );
    res.json({ success: true, history, combinedInvoices: Object.values(s.combinedVendorInvoices),
      completePurchases: pos.length, recoveredBatches: historical.length,
      recoveredProducts: historical.reduce((n, batch) => n + batch.productCount, 0) });
  } catch (error) {
    const history = pos.sort((a, b) => String(b.datePurchase || b.createdAt || '').localeCompare(String(a.datePurchase || a.createdAt || '')) || String(b.id).localeCompare(String(a.id)));
    res.json({ success: true, history, combinedInvoices: Object.values(s.combinedVendorInvoices),
      completePurchases: history.length, recoveredBatches: 0, recoveredProducts: 0,
      historyWarning: 'Shopify recovery is temporarily unavailable: ' + error.message });
  }
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
const SUMMARY_PENDING_STATUSES = ['advance', 'received', 'awaiting_approval', 'posting_partial'];
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
      const lineCost = purchaseCosts(po, s.settings).lines[po.lines.indexOf(line)];
      const perPc = lineCost.perPiece;
      const cost = lineCost.amount;
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

module.exports = { router, rejectGeneratedImage, imageCheckAccepted, genSeo, normalizeSeoStyle, canonicalSeoNaming, buildSku, rebuildLineSku, landedCost, parseSerial, nextSerial, canManagePurchases, canStartPaidPilot, canReviewPaidImage, parseLocalInvoiceText, duplicateBillPo, expandArticleWeights, retireAudienceModelImages, reconcileStudioKeysAfterLineEdit };
