// ═══════════════════════════════════════════════════════════════
// modules/casuals.js — SANKI CASUALS sourcing brain
//
// A SEPARATE line from SANKI FUNKY (fresh-procurement.js). Funky sorts
// vendor photos into loud/statement impression tiers. Casuals is the
// opposite discipline: everyday wearable staples bought to a DISTRIBUTION.
//
// The founder uploads a mixed pile of vendor photos (trousers, shirts,
// tees all together). We:
//   ① segregate by CATEGORY   → Trousers · Shirts · T-Shirts & Polos
//   ② inside a category, by FIT (wide-leg, relaxed, boxy, …)
//   ③ then buy to editable %s across FIT, SIZE and COLOUR.
//
// Defaults for every % come from the founder's "SANKI Casuals" workbook
// and are fully editable in the UI. Budget is HIERARCHY-WISE:
//   Category budget → Fit % → (Size % · Colour %) unit distributions.
// Budget is entered per category — a separate rupee ceiling for Trouser /
// Shirt / T-shirt (for all, any one, or any two enabled this cycle).
//
// Own store (casuals.json) + own photo pool (casuals-candidates) on the
// /data volume, completely independent of the Funky pool. Nothing writes
// to Shopify.
//
// Endpoints (behind the auth gate):
//   GET  /api/casuals/spec        → immutable category/fit/size/colour scaffold
//   GET  /api/casuals/settings    → merged (spec defaults ⊕ saved overrides)
//   POST /api/casuals/settings    → save budget model + edited %s
//   GET  /api/casuals/candidates  → pool + per-category counts
//   POST /api/casuals/candidates  → upload photos
//   GET  /api/casuals/candidate/:id            → serve image
//   DELETE /api/casuals/candidate/:id          → drop one
//   POST /api/casuals/candidates/clear         → empty the pool
//   POST /api/casuals/analyze     → vision: category + fit + colour + pattern
//   GET  /api/casuals/plan        → hierarchy-wise buy allocation
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const crypto  = require('crypto');
const multer  = require('multer');
let Jimp = null; try { Jimp = require('jimp'); } catch { /* dedup degrades to off */ }
let XLSX = null; try { XLSX = require('xlsx'); } catch { /* Excel invoice parsing degrades to off */ }

const router = express.Router();

// ── The Casuals scaffold (defaults from the SANKI Casuals workbook) ──
// Percentages are WEIGHTS — they need not sum to exactly 100; the planner
// normalises them. Every number here is an editable default; saved settings
// override it per category.
const CASUALS_SPEC = [
  {
    key: 'Trouser', label: 'Trousers',
    designNote: 'Baggy vs Normal; colour + fit are HARD caps',
    fitHard: true,   // Trousers run a colour×fit HARD grid (see selectIncluded)
    // Trousers run a 2-fit HARD grid (Baggy / Normal). Both fit AND colour are
    // hard caps: the colour×fit grid targets are largest-remainder rounded and a
    // cell that can't be filled becomes a "source more" instruction (no backfill).
    fits: [
      { key: 'baggy',  label: 'Baggy',  pct: 50, kw: ['baggy', 'wide', 'wide leg', 'wide-leg', 'loose', 'oversized', 'controlled baggy', 'balloon', 'parachute'] },
      { key: 'normal', label: 'Normal', pct: 50, kw: ['normal', 'regular', 'straight', 'relaxed straight', 'relaxed', 'slim', 'tapered', 'chino', 'fitted', 'classic', 'pleated', 'flared'] }
    ],
    sizes: [
      { key: 'XS', pct: 1 }, { key: 'S', pct: 2 }, { key: 'M', pct: 2 }, { key: 'L', pct: 2 },
      { key: 'XL', pct: 2 }, { key: 'XXL', pct: 1 }, { key: 'XXXL', pct: 1 }, { key: 'XXXXL', pct: 0 }
    ],
    colours: [
      { key: 'Black', pct: 25 }, { key: 'Beige', pct: 20 }, { key: 'Charcoal Grey', pct: 15 },
      { key: 'Olive', pct: 20 }, { key: 'Brown', pct: 10 }, { key: 'Navy Blue', pct: 5 }, { key: 'Off White', pct: 8 }
    ],
    avgCost: 700
  },
  {
    key: 'Shirt', label: 'Shirts',
    designNote: 'Solid ≈ 50%, rest prints / textures',
    fits: [
      { key: 'oversized',      label: 'Oversized / Drop Shoulder', pct: 40, kw: ['oversize', 'oversized', 'drop shoulder', 'drop-shoulder', 'boxy', 'box fit', 'box'] },
      { key: 'relaxedregular', label: 'Relaxed Regular',           pct: 60, kw: ['relaxed regular', 'relaxed', 'regular', 'classic', 'cuban', 'resort', 'camp collar', 'revere', 'bowling'] }
    ],
    sizes: [
      { key: 'XS', pct: 6 }, { key: 'S', pct: 28 }, { key: 'M', pct: 28 },
      { key: 'L', pct: 18 }, { key: 'XL', pct: 14 }, { key: 'XXL', pct: 6 }
    ],
    colours: [
      { key: 'Black', pct: 20 }, { key: 'Off White', pct: 20 }, { key: 'White', pct: 5 },
      { key: 'Charcoal Grey', pct: 10 }, { key: 'Olive Green', pct: 15 }, { key: 'Beige', pct: 15 }, { key: 'Sky Blue', pct: 10 }
    ],
    // Print-type split (Shirts & T-shirts only) — an EXTRA manual dimension on
    // top of fit. Solid ≈ half; the rest divided between prints/embroidery and
    // checks/stripes. Fully editable in the UI.
    printTypes: [
      { key: 'solid',  label: 'Solid (plain / logo)', pct: 50 },
      { key: 'others', label: 'Others',               pct: 50 }
    ],
    avgCost: 650
  },
  {
    key: 'T-shirt', label: 'T-Shirts & Polos',
    designNote: 'Solid 60–70%, texture / stripes / checks 25–30%',
    fits: [
      { key: 'oversizedround', label: 'Oversized Round Neck', pct: 50, kw: ['oversize', 'oversized', 'drop shoulder', 'oversized round', 'boxy', 'box fit', 'box'] },
      { key: 'relaxedround',   label: 'Relaxed Round Neck',   pct: 50, kw: ['relaxed round', 'relaxed', 'regular round', 'classic', 'regular tee', 'regular', 'polo', 'slim'] }
    ],
    sizes: [
      { key: 'XS', pct: 10 }, { key: 'S', pct: 20 }, { key: 'M', pct: 20 },
      { key: 'L', pct: 20 }, { key: 'XL', pct: 20 }, { key: 'XXL', pct: 10 }
    ],
    colours: [
      { key: 'Black', pct: 25 }, { key: 'Off White', pct: 20 }, { key: 'White', pct: 15 },
      { key: 'Charcoal Grey', pct: 15 }, { key: 'Olive Green', pct: 15 }, { key: 'Beige', pct: 5 }, { key: 'Navy Blue', pct: 5 }
    ],
    // Print-type split (Shirts & T-shirts only) — solid-heavy for tees.
    printTypes: [
      { key: 'solid',  label: 'Solid (plain / logo)', pct: 65 },
      { key: 'others', label: 'Others',               pct: 35 }
    ],
    avgCost: 400
  }
];
const CAT_KEYS  = CASUALS_SPEC.map(c => c.key);
const CAT_BY_KEY = CASUALS_SPEC.reduce((m, c) => (m[c.key] = c, m), {});
// Pieces in ONE default size-set per category (mirrors the frontend
// CZ_SETRATIO curve: 1+2+2+2+2+1 = 10). Used only for the soft "pieces coming"
// hint on a batch row — the real count is decided in the buy sheet.
const SET_PIECES = { Trouser: 10, Shirt: 10, 'T-shirt': 10 };

// Map a free-text garment word from the vision pass onto a Casuals category.
// Anything that isn't one of our three casual categories returns null (it will
// be shown under "Unsorted" so the founder can see stray uploads).
function normCasualCategory(raw) {
  const t = String(raw || '').toLowerCase().replace(/[^a-z]/g, '');
  if (/tshirt|tee|polo|halfsleeve/.test(t)) return 'T-shirt';
  if (/shirt|overshirt/.test(t)) return 'Shirt';
  if (/trouser|pant|chino|cargo|jean|denim|bottom/.test(t)) return 'Trouser';
  return null;
}
// Print-type buckets (Shirts & T-shirts). The vision pass returns a raw
// `pattern` (solid/texture/stripes/checks/print) which we fold into these three
// manual-%-split buckets. Trousers have no print-type dimension.
const PRINT_TYPES = [
  { key: 'solid',  label: 'Solid (plain / logo)' },
  { key: 'others', label: 'Others' }
];
const PRINT_BY_KEY = PRINT_TYPES.reduce((m, p) => (m[p.key] = p, m), {});
// Fits / print-types we removed from the spec but that OLD saved settings may
// still carry (mergePct keeps unknown saved keys). Prune these on load so the
// model stays at the current 2-fit / Solid+Others shape.
const RETIRED_FITS = { Shirt: ['boxy', 'cuban'], 'T-shirt': ['boxyround', 'classictee'], Trouser: ['wideleg', 'relaxedstraight', 'koreanpleated', 'flared', 'tapered'] };
const RETIRED_PRINTS = ['printed', 'checks'];
function catHasPrintTypes(catKey) { const c = CAT_BY_KEY[catKey]; return !!(c && Array.isArray(c.printTypes) && c.printTypes.length); }
// Fold a raw vision pattern into one of the two print-type buckets.
//   solid → solid · everything else (print, embroidery, checks, stripes,
//   texture, unknown) → the "others" bucket.
function printBucket(pattern) {
  const t = String(pattern || '').toLowerCase();
  if (!t) return null;
  if (t === 'solid' || t === 'plain') return 'solid';
  return 'others';
}
// Effective print bucket for a candidate: a manual segregation fix (printOverride)
// wins over the AI's pattern read, mirroring how fitOverride / colourOverride work.
function printKeyOf(c) { return (c && c.printOverride && c.printType) ? c.printType : printBucket(c && c.pattern); }

// Match a detected fit phrase to one of a category's canonical fits by keyword.
function normFit(catKey, raw) {
  const cat = CAT_BY_KEY[catKey];
  if (!cat) return null;
  const t = String(raw || '').toLowerCase();
  if (!t) return null;
  for (const f of cat.fits) if (f.kw.some(k => t.includes(k))) return f.key;
  return null;
}

// ── Duplicate detection — EXACT matches only ─────────────────────
// The founder wants a photo flagged as a duplicate only when it is the SAME
// picture, not merely a similar-looking garment. So the primary test is an
// exact SHA-256 of the file bytes; the perceptual hash is kept only as a
// pixel-identical safety net (hamming 0 + no colour drift) for the case where
// the identical image was re-encoded on the way in. Similar-but-different
// styles / colourways are NEVER merged.
function fileSha(filePath) {
  try { return crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex'); }
  catch { return null; }
}
async function computeSignature(filePath) {
  if (!Jimp) return null;
  try {
    const img = await Jimp.read(filePath);
    const g = img.clone().resize(9, 8).greyscale();
    let bits = '';
    for (let y = 0; y < 8; y++) for (let x = 0; x < 8; x++) {
      const l = Jimp.intToRGBA(g.getPixelColor(x, y)).r;
      const r = Jimp.intToRGBA(g.getPixelColor(x + 1, y)).r;
      bits += l > r ? '1' : '0';
    }
    const small = img.clone().resize(8, 8);
    let R = 0, G = 0, B = 0;
    for (let y = 0; y < 8; y++) for (let x = 0; x < 8; x++) {
      const p = Jimp.intToRGBA(small.getPixelColor(x, y)); R += p.r; G += p.g; B += p.b;
    }
    return { phash: bits, avg: [Math.round(R / 64), Math.round(G / 64), Math.round(B / 64)] };
  } catch { return null; }
}
function hamming(a, b) { if (!a || !b || a.length !== b.length) return 999; let d = 0; for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) d++; return d; }
function colourDist(a, b) { if (!a || !b) return 999; return Math.sqrt((a[0]-b[0])**2 + (a[1]-b[1])**2 + (a[2]-b[2])**2); }
// Vision returns a FREE-FORM colour word ("Cream", "Charcoal", "Army Green"),
// but the buy plan's colour % is a fixed preset list per category. This maps an
// observed word to the nearest preset key so colour % can shape the buy the same
// way fit % does. Returns 'Other' when nothing sensible matches (weight 0 — such
// designs only fill leftover slots). `keys` = this category's preset colour keys.
const COLOUR_SYN = [
  { canon: 'black',        toks: ['black', 'jet', 'onyx'] },
  { canon: 'off white',    toks: ['off white', 'offwhite', 'cream', 'ecru', 'ivory', 'bone', 'oatmeal', 'chalk'] },
  { canon: 'white',        toks: ['white', 'optic'] },
  { canon: 'charcoal grey',toks: ['charcoal', 'grey', 'gray', 'graphite', 'slate', 'ash', 'gunmetal'] },
  { canon: 'olive green',  toks: ['olive', 'army', 'military', 'fatigue', 'moss', 'forest', 'sage', 'green'] },
  { canon: 'beige',        toks: ['beige', 'khaki', 'sand', 'tan', 'camel', 'biscuit', 'stone', 'nude'] },
  { canon: 'brown',        toks: ['brown', 'coffee', 'chocolate', 'mocha', 'walnut', 'tobacco', 'rust', 'maroon'] },
  { canon: 'navy blue',    toks: ['navy', 'midnight', 'indigo'] },
  { canon: 'sky blue',     toks: ['sky', 'powder blue', 'baby blue', 'light blue'] },
  { canon: 'blue',         toks: ['blue', 'denim', 'cobalt', 'royal', 'teal'] }
];
function canonColour(observed, keys) {
  if (!Array.isArray(keys) || !keys.length) return 'Other';
  const o = String(observed || '').toLowerCase().trim();
  if (!o) return 'Other';
  const lk = keys.map(k => ({ key: k, low: k.toLowerCase() }));
  const findKey = concept => {                        // concept is a lowercase colour word
    let m = lk.find(k => k.low === concept); if (m) return m.key;
    m = lk.find(k => k.low.includes(concept) || concept.includes(k.low)); if (m) return m.key;
    return null;
  };
  const direct = lk.find(k => o === k.low || o.includes(k.low)); if (direct) return direct.key;
  let concept = null;
  for (const s of COLOUR_SYN) { if (s.toks.some(t => o.includes(t))) { concept = s.canon; break; } }
  if (concept) {
    let hit = findKey(concept); if (hit) return hit;
    // concept known but this category lacks that exact key — bridge close cousins.
    const alt = { 'olive green': 'olive', 'olive': 'olive green', 'white': 'off white', 'off white': 'white', 'blue': 'navy blue', 'navy blue': 'sky blue' }[concept];
    if (alt) { hit = findKey(alt); if (hit) return hit; }
  }
  const tokHit = lk.find(k => k.low.split(/\s+/).some(t => o.includes(t))); if (tokHit) return tokHit.key;
  return 'Other';
}
// Two candidates are duplicates only if they are the SAME picture: identical
// file bytes (sha) OR a pixel-identical re-encode (phash matches exactly with
// no colour drift). Anything less — a similar style, another colourway, a
// different angle — is a distinct product and stays.
function isExactDuplicate(a, b) {
  if (a.sha && b.sha) return a.sha === b.sha;
  if (a.phash && b.phash) return hamming(a.phash, b.phash) === 0 && colourDist(a.avg, b.avg) <= 6;
  return false;
}
function markDuplicates(cands) {
  const uniques = [];
  cands.forEach(c => {
    c.dupeOf = null;
    const hit = uniques.find(u => isExactDuplicate(c, u));
    if (hit) c.dupeOf = hit.id; else uniques.push(c);
  });
  return cands.filter(c => c.dupeOf).length;
}

// ── Storage ──────────────────────────────────────────────────────
const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const STORE_PATH = process.env.CASUALS_PATH || path.join(DATA_DIR, 'casuals.json');
const CAND_DIR = path.join(DATA_DIR, 'casuals-candidates');
try { fs.mkdirSync(CAND_DIR, { recursive: true }); } catch { /* exists */ }

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const AI_MODEL = process.env.CASUALS_AI_MODEL || process.env.FRESH_PROC_AI_MODEL || process.env.PROCUREMENT_AI_MODEL || 'claude-sonnet-4-6';
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const SPLITTER_IMAGE_MODEL = process.env.CASUALS_IMAGE_MODEL || process.env.OPENAI_IMAGE_MODEL || 'gpt-image-1.5';

function atomicWrite(fp, data) {
  const tmp = fp + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, fp);
}
function loadStore() {
  try {
    const s = JSON.parse(fs.readFileSync(STORE_PATH, 'utf8'));
    if (!s.settings) s.settings = {};
    if (!Array.isArray(s.candidates)) s.candidates = [];
    ensureBatches(s);
    return s;
  } catch { return ensureBatches({ settings: {}, candidates: [] }); }
}
function saveStore(s) { atomicWrite(STORE_PATH, JSON.stringify(s)); }

// Reclaim leaked photos: multer writes each upload to disk BEFORE the handler
// records it, so an aborted/failed upload leaves a file on the volume that no
// candidate references and that batch-delete never cleans. Sweep any file in
// CAND_DIR that isn't referenced by a live candidate. Returns bytes freed.
function sweepOrphanCandidates() {
  let freed = 0, removed = 0;
  try {
    const s = loadStore();
    const keep = new Set((s.candidates || []).map(c => c.file).filter(Boolean));
    for (const fn of fs.readdirSync(CAND_DIR)) {
      if (keep.has(fn)) continue;
      const fp = path.join(CAND_DIR, fn);
      try { const st = fs.statSync(fp); if (st.isFile()) { fs.unlinkSync(fp); freed += st.size; removed++; } } catch {}
    }
  } catch {}
  if (removed) console.log('[casuals] swept ' + removed + ' orphan photo(s), freed ' + Math.round(freed / 1024) + ' KB');
  return { removed, freed };
}
// Run once at boot so leaked space is reclaimed on the next deploy.
try { sweepOrphanCandidates(); } catch {}

// ── Batches ──────────────────────────────────────────────────────
// Photos live in named batches (e.g. "Batch 1 · 24 Jul 2026, 4:25 PM"). The
// plan, segregation and dupe check all run on the ACTIVE batch only, so a new
// upload run is never mixed into an earlier one's calculation.
function batchDateName(num, iso) {
  const d = iso ? new Date(iso) : new Date();
  let stamp;
  try {
    stamp = d.toLocaleString('en-IN', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true });
  } catch { stamp = d.toISOString(); }
  return 'Batch ' + num + ' · ' + stamp;
}
// The categories a batch spans. New batches store a `categories[]` set (any combo
// of Trouser / Shirt / T-shirt); legacy batches carry a single `.category` string.
// This reads either shape into a clean, de-duped array of valid category keys.
function batchCats(b) {
  if (!b) return [];
  const raw = Array.isArray(b.categories) ? b.categories : (b.category ? [b.category] : []);
  const out = [];
  raw.forEach(k => { if (CAT_BY_KEY[k] && out.indexOf(k) < 0) out.push(k); });
  return out;
}
function newBatch(s, name, category) {
  const num = (s.batches.reduce((m, b) => Math.max(m, b.num || 0), 0)) + 1;
  const createdAt = new Date().toISOString();
  // A batch spans one OR MORE categories, chosen up-front on New batch. `category`
  // may arrive as a single key (legacy) or an array of keys (multi-select).
  const rawCats = Array.isArray(category) ? category : (category ? [category] : []);
  const cats = [];
  rawCats.forEach(k => { if (CAT_BY_KEY[k] && cats.indexOf(k) < 0) cats.push(k); });
  const b = { id: crypto.randomBytes(6).toString('hex'), num, name: (name && String(name).trim()) || batchDateName(num, createdAt), createdAt,
    categories: cats, category: cats.length === 1 ? cats[0] : null };   // keep .category for single-cat back-compat
  s.batches.push(b);
  return b;
}
// Migrate legacy stores (candidates with no batch) into "Batch 1", stamped from
// the earliest photo's upload time — so the existing 84 photos become one batch.
function ensureBatches(s) {
  if (!Array.isArray(s.batches)) s.batches = [];
  const orphans = s.candidates.filter(c => !c.batch);
  if (orphans.length || (s.candidates.length && !s.batches.length)) {
    let first = s.batches.find(b => b.num === 1);
    if (!first) {
      const earliest = orphans.map(c => c.uploadedAt).filter(Boolean).sort()[0] || new Date().toISOString();
      first = { id: crypto.randomBytes(6).toString('hex'), num: 1, name: batchDateName(1, earliest), createdAt: earliest };
      s.batches.push(first);
    }
    orphans.forEach(c => { c.batch = first.id; });
  }
  if (!s.activeBatch || !s.batches.some(b => b.id === s.activeBatch)) {
    s.activeBatch = s.batches.length ? s.batches[s.batches.length - 1].id : null;
  }
  return s;
}
function activeCands(s) { return s.candidates.filter(c => c.batch === s.activeBatch); }
function batchList(s) {
  return s.batches.map(b => {
    const cs = s.candidates.filter(c => c.batch === b.id);
    const by = {}; let analysed = 0;
    cs.forEach(c => { if (c.category && CAT_BY_KEY[c.category]) { by[c.category] = (by[c.category] || 0) + 1; analysed++; } });
    const categories = CAT_KEYS.filter(k => by[k]).map(k => ({ category: k, label: CAT_BY_KEY[k].label, count: by[k] }));
    // Soft pieces-coming estimate = one default set per categorised photo.
    const pieces = Object.keys(by).reduce((sum, k) => sum + by[k] * (SET_PIECES[k] || 10), 0);
    return { id: b.id, num: b.num, name: b.name, createdAt: b.createdAt, category: b.category || null,
      batchCategories: batchCats(b),   // the category SET this batch spans (multi-category); [] = legacy/unconstrained
      count: cs.length, analysed, categories, pieces };
  });
}

function mediaTypeForFile(fn) {
  const e = path.extname(fn || '').toLowerCase();
  return e === '.png' ? 'image/png' : e === '.webp' ? 'image/webp' : e === '.gif' ? 'image/gif' : 'image/jpeg';
}
function extractJson(text) {
  if (!text) return null;
  let t = String(text).replace(/```json/gi, '').replace(/```/g, '').trim();
  const first = Math.min(...['{', '['].map(c => { const i = t.indexOf(c); return i < 0 ? Infinity : i; }));
  if (!isFinite(first)) return null;
  const open = t[first], close = open === '{' ? '}' : ']';
  let depth = 0, end = -1;
  for (let i = first; i < t.length; i++) { if (t[i] === open) depth++; else if (t[i] === close) { depth--; if (depth === 0) { end = i; break; } } }
  if (end < 0) return null;
  try { return JSON.parse(t.slice(first, end + 1)); } catch { return null; }
}

// ── Settings: spec defaults ⊕ saved overrides ────────────────────
// A saved settings blob only stores what the founder CHANGED. We fold it over
// the immutable spec so the UI always receives a complete, sane object.
function pctMapFromList(list) { const m = {}; list.forEach(x => { m[x.key] = x.pct; }); return m; }
function cleanPct(v, fallback) { const n = Number(v); return isFinite(n) && n >= 0 ? n : fallback; }
// Sanitize a { fitKey: units } map into positive integers — the per-fit
// "already on order / on the way" quantities used for open-to-buy netting.
function cleanIntMap(m) {
  const out = {};
  if (m && typeof m === 'object') Object.keys(m).forEach(k => { const n = parseInt(m[k], 10); if (isFinite(n) && n > 0) out[k] = n; });
  return out;
}
// Sanitize a { fitKey: unitRupees } map into positive numbers — the real
// vendor unit ₹ (from the uploaded invoice) for stock already on order. Used to
// value the freed budget accurately and to pre-fill matching new-design prices.
function cleanMoneyMap(m) {
  const out = {};
  if (m && typeof m === 'object') Object.keys(m).forEach(k => { const n = Math.round(Number(m[k])); if (isFinite(n) && n > 0) out[k] = n; });
  return out;
}

function settingsWithDefaults(s) {
  const saved = (s && s.settings) || {};
  const savedCats = saved.categories || {};
  const categories = {};
  CASUALS_SPEC.forEach(spec => {
    const sc = savedCats[spec.key] || {};
    const fitDef = pctMapFromList(spec.fits), sizeDef = pctMapFromList(spec.sizes), colDef = pctMapFromList(spec.colours);
    const printDef = pctMapFromList(spec.printTypes || []);   // {} for Trousers
    // Merge %s: start from defaults, apply any saved edits, keep unknown custom keys the founder added.
    const mergePct = (defMap, savedMap) => {
      const out = {};
      Object.keys(defMap).forEach(k => { out[k] = cleanPct(savedMap && savedMap[k], defMap[k]); });
      if (savedMap) Object.keys(savedMap).forEach(k => { if (!(k in out)) out[k] = cleanPct(savedMap[k], 0); });
      return out;
    };
    categories[spec.key] = {
      enabled: sc.enabled != null ? !!sc.enabled : true,
      budget: sc.budget != null ? Math.max(0, parseInt(sc.budget) || 0) : 0,
      avgCost: sc.avgCost != null ? Math.max(1, parseInt(sc.avgCost) || spec.avgCost) : spec.avgCost,
      // SIZING MODE — how the category's total piece target is set:
      //   'cost'    → pieces = budget ÷ avgCost (money-anchored estimate).
      //   'units'   → founder types the piece target directly (no price guess).
      //   'designs' → founder types the NUMBER OF DESIGNS + a per-design size run;
      //               pieces = designs × SET(size run). Budget/avgCost are optional
      //               here (they firm up at the PO stage). This is the spec-first
      //               Trouser intake: vendor → designs → size run → colours% → fit%.
      sizeMode: (sc.sizeMode === 'units' || sc.sizeMode === 'designs') ? sc.sizeMode : 'cost',
      targetUnits: sc.targetUnits != null ? Math.max(0, parseInt(sc.targetUnits) || 0) : 0,
      // Number of designs (used when sizeMode === 'designs').
      designs: sc.designs != null ? Math.max(0, parseInt(sc.designs) || 0) : 0,
      // Size-label system for this category's size run: 'alpha' (S/M/L/XL/XXL/XXXL)
      // or 'numeric' (26/28/30…). Only a UI hint — the engine reads whatever keys
      // are in `sizes`. Defaults to alpha for Trousers, else keeps prior behaviour.
      sizeSystem: (sc.sizeSystem === 'numeric' || sc.sizeSystem === 'alpha') ? sc.sizeSystem : (spec.key === 'Trouser' ? 'alpha' : 'alpha'),
      fits:    mergePct(fitDef, sc.fits),
      sizes:   mergePct(sizeDef, sc.sizes),
      colours: mergePct(colDef, sc.colours),
      // Print-type % split — only populated for categories that support it
      // (Shirts, T-shirts); an empty {} for Trousers.
      printTypes: mergePct(printDef, sc.printTypes),
      // Per-fit units already ordered / in transit (open-to-buy netting).
      onOrder: cleanIntMap(sc.onOrder),
      // Per-fit real vendor unit ₹ for that on-order stock (from the invoice).
      onOrderCost: cleanMoneyMap(sc.onOrderCost),
      // Extra fit / size / colour ROWS the founder added (label + key), beyond the spec.
      extraFits:    Array.isArray(sc.extraFits) ? sc.extraFits.filter(x => x && x.key) : [],
      extraSizes:   Array.isArray(sc.extraSizes) ? sc.extraSizes.filter(x => x && x.key) : [],
      extraColours: Array.isArray(sc.extraColours) ? sc.extraColours.filter(x => x && x.key) : []
    };
    // RETIRED-KEY PRUNE — mergePct keeps any saved key not in the spec defaults,
    // which would resurrect fits/prints we deliberately removed (older saved
    // settings still carry them). Strip those retired keys so the model stays at
    // the current 2-fit / Solid+Others shape.
    const retFits = RETIRED_FITS[spec.key];
    if (retFits) retFits.forEach(k => { delete categories[spec.key].fits[k]; });
    if (categories[spec.key].printTypes) RETIRED_PRINTS.forEach(k => { delete categories[spec.key].printTypes[k]; });
  });
  return {
    // Budget is ALWAYS entered per category (Trouser / Shirt / T-shirt) — the
    // old "one total budget split equally" mode was removed.
    budgetMode: 'perCategory',
    categories
  };
}

// Curation: the AI rating is NO LONGER an inclusion factor — every design is
// sourced by default. The founder can still manually include/exclude any design,
// stored per-candidate as `includeOverride` (true/false).
function isIncluded(c) {
  if (c.includeOverride === false) return false;
  return true; // rating gate DISABLED — include everything unless manually removed
}

// Public shape of a candidate (never leak the disk path).
function publicCandidate(c) {
  return { id: c.id, vendor: c.vendor || '', url: '/api/casuals/candidate/' + c.id,
    category: c.category || null, fit: c.fit || null, colour: c.colour || null,
    pattern: c.pattern || null, aiFit: c.aiFit || null, dupeOf: c.dupeOf || null,
    rating: (c.rating != null ? c.rating : null), ratingReason: c.ratingReason || null,
    includeOverride: (c.includeOverride === true || c.includeOverride === false) ? c.includeOverride : null,
    fitOverride: c.fitOverride === true, colourOverride: c.colourOverride === true,
    printKey: printKeyOf(c), printOverride: c.printOverride === true,
    included: isIncluded(c),
    designId: c.designId || null, designName: c.designName || null,
    uploadedAt: c.uploadedAt, batch: c.batch || null };
}
// Collapse already-ordered PO line items into product cards. A supplier bill
// lists ONE product across several size rows (Black L, Black XL, …), so each row
// arrives as its own candidate. Merge rows that share the same product identity
// (colour · vendor · unit ₹) into a single card whose size run is the SUM of its
// rows — matching how a batch product is shown (one photo, one size ladder). The
// card keeps every underlying candidate id so a single "Remove" deletes them all.
function mergeOrderedItems(items) {
  const groups = {}; const order = [];
  (items || []).forEach(c => {
    const colour = (c.colour && String(c.colour).trim()) || 'Unspecified';
    const cost = Math.max(0, c.orderedCost || 0);
    const gk = colour.toLowerCase() + '|' + String(c.vendor || '').trim().toLowerCase() + '|' + cost;
    let g = groups[gk];
    if (!g) { g = groups[gk] = { ids: [], repr: c, colour, cost, vendor: c.vendor || '', qty: 0, sizeMap: {} }; order.push(gk); }
    const reprHasImg = !!(g.repr.file || g.repr.photoRef);
    if (!reprHasImg && (c.file || c.photoRef)) g.repr = c;   // prefer a member that actually has a photo (local file OR PO photoRef)
    g.ids.push(c.id);
    g.qty += Math.max(0, c.orderedQty || 0);
    Object.keys(c.orderedSizes || {}).forEach(k => { const u = Math.max(0, c.orderedSizes[k] || 0); if (u > 0) g.sizeMap[k] = (g.sizeMap[k] || 0) + u; });
  });
  return order.map(gk => {
    const g = groups[gk];
    return {
      id: g.repr.id, ids: g.ids, url: '/api/casuals/candidate/' + g.repr.id, hasImg: !!(g.repr.file || g.repr.photoRef),
      vendor: g.vendor, colour: g.colour, qty: g.qty, cost: g.cost,
      sizes: Object.keys(g.sizeMap).map(k => ({ key: k, units: g.sizeMap[k] })).filter(x => x.units > 0)
    };
  });
}
function categoryCounts(cands) {
  const active = cands.filter(c => !c.dupeOf && !c.ordered);
  const by = {};
  active.forEach(c => { const k = c.category || 'Unsorted'; by[k] = (by[k] || 0) + 1; });
  return CAT_KEYS.concat(['Unsorted']).map(k => ({ category: k, label: (CAT_BY_KEY[k] && CAT_BY_KEY[k].label) || 'Unsorted', count: by[k] || 0 }))
    .filter(x => x.count > 0 || CAT_KEYS.includes(x.category));
}

// ── Upload middleware ────────────────────────────────────────────
const candidateUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => { try { fs.mkdirSync(CAND_DIR, { recursive: true }); } catch {} cb(null, CAND_DIR); },
    filename: (req, file, cb) => {
      const ext = (path.extname(file.originalname || '') || '.jpg').toLowerCase().replace(/[^.a-z0-9]/g, '');
      cb(null, Date.now() + '-' + crypto.randomBytes(6).toString('hex') + (ext || '.jpg'));
    }
  }),
  limits: { fileSize: 25 * 1024 * 1024, files: 200 },
  fileFilter: (req, file, cb) => cb(null, /^image\//.test(file.mimetype))
});

// ── Photo Splitter ──────────────────────────────────────────────
// Supplier screenshots often contain several colourway/product photos in one
// collage. Detect the individual photo rectangles, crop them server-side and
// return reviewable JPEGs. Nothing is written to a procurement batch here; the
// browser explicitly uploads only the crops the buyer keeps.
const splitterUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024, files: 20 },
  fileFilter: (req, file, cb) => cb(null, /^image\//.test(file.mimetype))
});

function validSplitBoxes(raw) {
  const boxes = [];
  (Array.isArray(raw) ? raw : []).forEach(b => {
    const a = Array.isArray(b) ? b : (b && b.box);
    if (!Array.isArray(a) || a.length !== 4) return;
    const n = a.map(Number);
    if (!n.every(Number.isFinite)) return;
    const x0 = Math.max(0, Math.min(1, n[0])), y0 = Math.max(0, Math.min(1, n[1]));
    const x1 = Math.max(0, Math.min(1, n[2])), y1 = Math.max(0, Math.min(1, n[3]));
    if (x1 - x0 < 0.08 || y1 - y0 < 0.08) return;
    // Avoid duplicate boxes returned with tiny coordinate differences.
    if (boxes.some(q => {
      if (Math.abs(q[0]-x0)<0.025 && Math.abs(q[1]-y0)<0.025 && Math.abs(q[2]-x1)<0.025 && Math.abs(q[3]-y1)<0.025) return true;
      const ix=Math.max(0,Math.min(q[2],x1)-Math.max(q[0],x0)), iy=Math.max(0,Math.min(q[3],y1)-Math.max(q[1],y0));
      const inter=ix*iy, union=(q[2]-q[0])*(q[3]-q[1])+(x1-x0)*(y1-y0)-inter;
      return inter/Math.max(0.001,union) >= 0.86;
    })) return;
    boxes.push([x0,y0,x1,y1]);
  });
  return boxes.slice(0, 30);
}

function splitDetectionNeedsDetail(boxes) {
  if (!Array.isArray(boxes) || boxes.length !== 1) return false;
  const b = boxes[0];
  return (b[2] - b[0]) * (b[3] - b[1]) >= 0.42;
}

function splitBoxesNeedFocusCards(boxes) {
  for (let i = 0; i < (boxes || []).length; i++) {
    for (let j = i + 1; j < boxes.length; j++) {
      const a = boxes[i], b = boxes[j];
      const ix = Math.max(0, Math.min(a[2], b[2]) - Math.max(a[0], b[0]));
      const iy = Math.max(0, Math.min(a[3], b[3]) - Math.max(a[1], b[1]));
      const smaller = Math.min((a[2]-a[0])*(a[3]-a[1]), (b[2]-b[0])*(b[3]-b[1]));
      if ((ix * iy) / Math.max(0.001, smaller) >= 0.08) return true;
    }
  }
  return false;
}

const splitSleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function reconstructSplitGarment(referenceBuffer) {
  if (!OPENAI_API_KEY) throw new Error('OpenAI reconstruction is not enabled. Set OPENAI_API_KEY in Railway.');
  const prompt = 'The green rectangle identifies ONE garment colourway in a supplier photograph. Create a clean isolated product-reference image of ONLY that selected garment. Reconstruct only portions hidden by neighbouring garments, conservatively continuing the visible fabric, colour, cut, length, waistband, pockets, seams and hem. Preserve the selected garment\'s exact visible colour, silhouette, proportions and design; do not copy a neighbour\'s colour or details and do not redesign it. Show the complete garment from top to hem, front-facing, centered on a plain light-grey background with even margins. No person, hanger, rack, other garments, text, labels, watermark, collage, border or green rectangle. Output exactly one garment. This is an AI reconstruction for procurement review, not a final product listing.';
  let last;
  for (let attempt=1; attempt<=3; attempt++) {
    const ctrl = new AbortController(), timer = setTimeout(() => ctrl.abort(), 120000);
    try {
      const form=new FormData();
      form.append('model',SPLITTER_IMAGE_MODEL);
      form.append('prompt',prompt);
      form.append('size','1024x1536');
      form.append('quality','medium');
      form.append('output_format','png');
      form.append('image',new Blob([referenceBuffer],{type:'image/jpeg'}),'selected-garment.jpg');
      const r = await fetch('https://api.openai.com/v1/images/edits', {
        method:'POST', signal:ctrl.signal,
        headers:{authorization:'Bearer '+OPENAI_API_KEY}, body:form
      });
      clearTimeout(timer);
      const j = await r.json().catch(() => ({}));
      if (!r.ok) { const e=new Error('OpenAI '+r.status+': '+(((j.error&&j.error.message)||'image error').slice(0,180))); e.status=r.status; throw e; }
      const data=j.data&&j.data[0]&&j.data[0].b64_json;
      if (!data) throw new Error('OpenAI returned no reconstructed image.');
      return {buffer:Buffer.from(data,'base64'),mimeType:'image/png'};
    } catch (e) {
      clearTimeout(timer); last=e;
      if (![429,500,502,503].includes(e.status) || attempt===3) throw e;
      await splitSleep(1000*Math.pow(2,attempt));
    }
  }
  throw last;
}

async function splitMapLimit(items, limit, worker) {
  const out = new Array(items.length); let next=0;
  async function run(){ while(next<items.length){ const i=next++; out[i]=await worker(items[i],i); } }
  await Promise.all(Array.from({length:Math.min(limit,items.length)},run));
  return out;
}

async function makeSplitFocusReference(img, box) {
  const W=img.bitmap.width,H=img.bitmap.height;
  const x=Math.max(0,Math.floor(box[0]*W)),y=Math.max(0,Math.floor(box[1]*H));
  const w=Math.max(1,Math.min(W-x,Math.ceil((box[2]-box[0])*W))),h=Math.max(1,Math.min(H-y,Math.ceil((box[3]-box[1])*H)));
  const focus=img.clone().brightness(-0.42);
  focus.composite(img.clone().crop(x,y,w,h),x,y);
  const line=Math.max(3,Math.round(Math.min(W,H)*0.006)),green=Jimp.rgbaToInt(16,185,129,255);
  for(let n=0;n<line;n++){
    for(let px=x;px<Math.min(W,x+w);px++){ if(y+n<H)focus.setPixelColor(green,px,y+n); if(y+h-1-n>=0)focus.setPixelColor(green,px,y+h-1-n); }
    for(let py=y;py<Math.min(H,y+h);py++){ if(x+n<W)focus.setPixelColor(green,x+n,py); if(x+w-1-n>=0)focus.setPixelColor(green,x+w-1-n,py); }
  }
  if(focus.bitmap.width>1400||focus.bitmap.height>1400)focus.scaleToFit(1400,1400);
  focus.quality(84); return focus.getBufferAsync(Jimp.MIME_JPEG);
}

async function requestPhotoBoxes(enc, prompt) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 90000);
  let r;
  try {
    r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST', signal: ctrl.signal,
      headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: AI_MODEL, max_tokens: 1800, temperature: 0, messages: [{ role:'user', content:[
        { type:'image', source:{ type:'base64', media_type:enc.mediaType, data:enc.data } },
        { type:'text', text:prompt }
      ]}] })
    });
  } finally { clearTimeout(timer); }
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error((j.error && j.error.message) || ('AI HTTP ' + r.status));
  const parsed = extractJson((j.content || []).map(c => c.text || '').join('')) || {};
  return validSplitBoxes(parsed.boxes);
}

async function detectPhotoBoxes(buffer, mediaType) {
  const enc = await shrinkForVision(buffer, mediaType);
  const primary = await requestPhotoBoxes(enc,
    'Split this supplier image into INDIVIDUAL garment colourways/products. A single continuous rack, row, collage panel, or shared background may contain several colours: make ONE separate crop for EACH visible garment/colourway, even when garments touch or overlap slightly. Do not return one box around an entire row when multiple garments are visible. For model photos, keep the complete model and garment. Exclude captions, app controls, blank margins, logos and tiny unrelated thumbnails. Coordinates are normalized against the whole image. If exactly one garment is visible, return one useful crop. Before answering, count the visible garments and ensure the box count matches. STRICT JSON ONLY: {"boxes":[[x0,y0,x1,y1]]}.');
  if (!splitDetectionNeedsDetail(primary)) return primary;

  // A large single box is commonly the whole rack/collage. Ask a second,
  // deliberately garment-level pass so same-background colour lineups are not
  // silently accepted as a successful one-photo split.
  const detailed = await requestPhotoBoxes(enc,
    'The earlier detector found only one large region. Audit the image specifically for MULTIPLE garment colourways inside that region. Count every separately visible trouser, shirt, T-shirt or other garment, including a row hanging on one rail or models shown side by side on the same background. Return ONE full-garment bounding box PER colourway. Boxes MAY overlap when the garments overlap; never cut off a garment merely to avoid overlap. Never group a multi-colour lineup into one box. If there truly is only one garment, return one box. Exclude UI, text-only areas and blank bars. Normalized coordinates. STRICT JSON ONLY: {"boxes":[[x0,y0,x1,y1]]}.');
  return detailed.length > primary.length ? detailed : primary;
}

router.post('/api/casuals/photo-split', (req, res) => {
  splitterUpload.array('photos', 20)(req, res, async err => {
    if (err) return res.status(400).json({ success:false, error:err.code === 'LIMIT_FILE_SIZE' ? 'A photo is larger than 25 MB.' : err.message });
    if (!Jimp) return res.status(503).json({ success:false, error:'Photo cropping is temporarily unavailable.' });
    const files = req.files || [];
    if (!files.length) return res.status(400).json({ success:false, error:'Choose at least one collage photo.' });
    try {
      const results = [], sources = [], warnings = [], splitRun = crypto.randomBytes(6).toString('hex');
      for (let fi=0; fi<files.length; fi++) {
        const f = files[fi];
        const boxes = await detectPhotoBoxes(f.buffer, f.mimetype);
        if (!boxes.length) continue;
        const img = await Jimp.read(f.buffer), W = img.bitmap.width, H = img.bitmap.height;
        const useFocusCards = splitBoxesNeedFocusCards(boxes);
        const preview=img.clone(); if(preview.bitmap.width>1200||preview.bitmap.height>1200)preview.scaleToFit(1200,1200); preview.quality(75);
        const previewBuf=await preview.getBufferAsync(Jimp.MIME_JPEG);
        sources.push({sourceIndex:fi,sourceName:f.originalname||('Photo '+(fi+1)),mimeType:'image/jpeg',data:'data:image/jpeg;base64,'+previewBuf.toString('base64')});
        if(useFocusCards){
          const made=await splitMapLimit(boxes,3,async(b,bi)=>{
            try{
              const ref=await makeSplitFocusReference(img,b),gen=await reconstructSplitGarment(ref);
              return {b,bi,gen};
            }catch(e){ warnings.push((f.originalname||('Photo '+(fi+1)))+' colour '+(bi+1)+': '+e.message); return null; }
          });
          for(const rec of made.filter(Boolean)){
            const ext=rec.gen.mimeType==='image/png'?'png':rec.gen.mimeType==='image/webp'?'webp':'jpg';
            results.push({id:'split-'+splitRun+'-'+fi+'-'+rec.bi,sourceIndex:fi,sourceName:f.originalname||('Photo '+(fi+1)),designGroup:'split-design-'+splitRun+'-'+fi,index:rec.bi+1,box:rec.b,aiReconstructed:true,mimeType:rec.gen.mimeType,data:'data:'+rec.gen.mimeType+';base64,'+rec.gen.buffer.toString('base64'),extension:ext});
          }
          continue;
        }
        for (let bi=0; bi<boxes.length; bi++) {
          const b = boxes[bi];
          const x = Math.max(0, Math.floor(b[0]*W)), y = Math.max(0, Math.floor(b[1]*H));
          const w = Math.max(1, Math.min(W-x, Math.ceil((b[2]-b[0])*W)));
          const h = Math.max(1, Math.min(H-y, Math.ceil((b[3]-b[1])*H)));
          let resultImage=img.clone().crop(x,y,w,h);
          if (resultImage.bitmap.width > 1800 || resultImage.bitmap.height > 1800) resultImage.scaleToFit(1800,1800);
          resultImage.quality(90);
          const out = await resultImage.getBufferAsync(Jimp.MIME_JPEG);
          results.push({ id:'split-'+splitRun+'-'+fi+'-'+bi, sourceIndex:fi, sourceName:f.originalname || ('Photo '+(fi+1)), designGroup:'split-design-'+splitRun+'-'+fi, index:bi+1, box:b, mimeType:'image/jpeg', data:'data:image/jpeg;base64,'+out.toString('base64') });
        }
      }
      if (!results.length) return res.status(422).json({ success:false, error:'No separate product photos could be detected. Try a clearer collage.' });
      res.json({ success:true, results, sources, warnings });
    } catch (e) {
      res.status(e && e.name === 'AbortError' ? 504 : 502).json({ success:false, error:e && e.name === 'AbortError' ? 'Photo splitting timed out. Try fewer photos at once.' : ('Could not split the photos: '+e.message) });
    }
  });
});
function runCandidateUpload(req, res) {
  return new Promise((resolve) => {
    candidateUpload.array('photos', 200)(req, res, (err) => {
      if (err) {
        const map = { LIMIT_FILE_SIZE: 'A photo is larger than 25 MB — compress it and retry.',
          LIMIT_FILE_COUNT: 'Too many photos at once (max 200 per batch).',
          LIMIT_UNEXPECTED_FILE: 'Unexpected upload field.' };
        resolve({ ok: false, error: map[err.code] || ('Upload error: ' + (err.message || err.code || 'unknown')) });
      } else resolve({ ok: true });
    });
  });
}

// ── Vision: category + fit + colour + pattern per photo ──────────
const VISION_BATCH = 10;
// Anthropic rejects any single request over ~32 MB. Full-resolution phone photos
// (often 3–8 MB each) blow past that in a batch of 10 — the "Request exceeds the
// maximum size" error. Anthropic already downscales anything over ~1568 px on its
// side, so we resize to that long edge and re-encode as JPEG(80) before sending:
// the same visual detail the model would see, at ~10–20× smaller payload.
const VISION_MAX_EDGE = 1568;
async function shrinkForVision(buffer, mediaType) {
  if (!Jimp) return { data: buffer.toString('base64'), mediaType };
  try {
    const img = await Jimp.read(buffer);
    const long = Math.max(img.getWidth(), img.getHeight());
    if (long > VISION_MAX_EDGE) img.scale(VISION_MAX_EDGE / long);
    img.quality(80);
    const out = await img.getBufferAsync(Jimp.MIME_JPEG);
    return { data: out.toString('base64'), mediaType: 'image/jpeg' };
  } catch { return { data: buffer.toString('base64'), mediaType }; }
}
async function scoreBatch(items) {
  // items: [{id, buffer, mediaType}] → map id→{category, fit, colour, pattern}
  const content = [];
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    content.push({ type: 'text', text: 'IMAGE ' + (i + 1) + ':' });
    const enc = await shrinkForVision(it.buffer, it.mediaType);
    content.push({ type: 'image', source: { type: 'base64', media_type: enc.mediaType, data: enc.data } });
  }
  const fitGuide = CASUALS_SPEC.map(c => '   • ' + c.label + ' fits: ' + c.fits.map(f => f.label).join(' | ')).join('\n');
  content.push({ type: 'text', text:
`You are a buyer for an Indian premium-casual clothing brand. For EACH image, identify a plain everyday garment and report six fields. Judge each image on its own.\n\n` +
`1) "category" — EXACTLY one of: Trousers, Shirts, T-Shirts & Polos. (A collared button-up = Shirts; a pullover round-neck knit/jersey top or polo = T-Shirts & Polos; any lower-body pant/chino/trouser = Trousers.) If it is truly none of these, use "Other".\n` +
`2) "fit" — the cut, chosen from that category's list:\n${fitGuide}\n   Pick the closest single fit label.\n` +
`3) "colour" — the dominant colour word (e.g. Black, Off White, Beige, Olive, Charcoal Grey, Navy Blue, Sky Blue, Brown).\n` +
`4) "pattern" — one of: "solid", "texture", "stripes", "checks", "print". Use "solid" for a plain single-colour garment.\n` +
`5) "rating" — an INTEGER 1-10 for how worth-buying this exact design is for a premium-casual Indian streetwear brand RIGHT NOW. Do NOT give a gut number. Score these four dimensions from the photo, then ADD them:\n` +
`   • TREND (0-3): how current the silhouette, cut and detailing are for 2026 premium streetwear (3 = clearly on-trend now, 0 = dated/generic).\n` +
`   • VERSATILITY (0-3): everyday wearability and broad appeal — easy to style across outfits, not a one-occasion or novelty piece (3 = wardrobe staple, 0 = niche/hard to wear).\n` +
`   • COLOUR & PATTERN (0-2): commercial desirability of the palette/print for Indian buyers (2 = clean in-demand colour, 1 = acceptable, 0 = off/muddy/dated).\n` +
`   • FIT & FINISH (0-2): proportion, drape and construction quality visible in the shot (2 = crisp premium look, 0 = cheap/ill-fitting).\n` +
`   Sum the four to get 1-10. 8+ means source it, so be discerning — a piece must be strong on TREND and VERSATILITY to reach 8. Do NOT give everything an 8+.\n` +
`6) "reason" — max 8 words naming the deciding dimension(s) (e.g. "on-trend cut, versatile" or "dated silhouette, weak colour").\n\n` +
`Return STRICT JSON ONLY: {"items":[{"category":"..","fit":"..","colour":"..","pattern":"..","rating":8,"reason":".."}, ...]} with exactly ${items.length} objects, one per image in order.` });
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 90000);
  let r;
  try {
    r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: AI_MODEL, max_tokens: 2500, temperature: 0, messages: [{ role: 'user', content }] }),
      signal: ctrl.signal
    });
  } finally { clearTimeout(timer); }
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error((j.error && j.error.message) || ('HTTP ' + r.status));
  const parsed = extractJson((j.content || []).map(c => c.text || '').join(''));
  const arr = parsed && Array.isArray(parsed.items) ? parsed.items : [];
  const out = {};
  items.forEach((it, i) => {
    const o = arr[i] || {};
    const cat = normCasualCategory(o.category);
    let rating = parseInt(o.rating, 10);
    if (!isFinite(rating)) rating = null; else rating = Math.max(1, Math.min(10, rating));
    out[it.id] = {
      category: cat,
      aiFit: String(o.fit || '').trim() || null,
      fit: cat ? normFit(cat, o.fit) : null,
      colour: String(o.colour || '').trim() || null,
      pattern: String(o.pattern || '').trim().toLowerCase() || null,
      rating,
      reason: String(o.reason || '').trim().slice(0, 80) || null
    };
  });
  return out;
}

// ── Simple Procurement: auditable assortment matching ───────────
// The simplified UI first defines named assortment slots, then asks this pass
// to compare every viable catalogue image against those slots. This is kept
// separate from the legacy fit/colour planner so the old workflow is unchanged.
async function scoreSimpleBatch(items, plan) {
  const content = [];
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    content.push({ type: 'text', text: 'CANDIDATE ' + (i + 1) + ':' });
    const enc = await shrinkForVision(it.buffer, it.mediaType);
    content.push({ type: 'image', source: { type: 'base64', media_type: enc.mediaType, data: enc.data } });
  }
  const slots = (plan.mix || []).map((s, i) => `${i + 1}. ${String(s.name || '').trim()} (${Math.max(1, parseInt(s.styles) || 1)} style slot${Math.max(1, parseInt(s.styles) || 1) === 1 ? '' : 's'})`).join('\n');
  content.push({ type: 'text', text:
`You are the senior buyer for SANKI, an Indian premium-casual fashion brand. Compare EACH candidate image against this APPROVED buy plan.

Gender: ${String(plan.gender || '')}
Category: ${String(plan.category || '')}
Approved assortment slots:
${slots}

For each candidate return:
1) "slot" — EXACT approved slot name it best matches, or "No match".
2) "silhouette" — specific visible garment silhouette, max 7 words.
3) "colour" — dominant visible colour.
4) "score" — integer 0-100, using exactly: slot match 0-40 + SANKI brand fit 0-20 + current Indian-market relevance 0-15 + versatility 0-15 + visible finish 0-10.
5) "reason" — max 18 words explaining the score using visible evidence.
6) "excludeReason" — if score below 65 or slot is No match, max 12 words; otherwise empty.

Be strict. A generic or wrong-gender product cannot score above 64. Do not invent fabric, price, measurements, stock or demand data from a photograph.
Return STRICT JSON ONLY: {"items":[{"slot":"..","silhouette":"..","colour":"..","score":78,"reason":"..","excludeReason":""}, ...]} with exactly ${items.length} objects in image order.` });
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 90000);
  let r;
  try {
    r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: AI_MODEL, max_tokens: 3500, temperature: 0, messages: [{ role: 'user', content }] }),
      signal: ctrl.signal
    });
  } finally { clearTimeout(timer); }
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error((j.error && j.error.message) || ('HTTP ' + r.status));
  const parsed = extractJson((j.content || []).map(c => c.text || '').join(''));
  const arr = parsed && Array.isArray(parsed.items) ? parsed.items : [];
  return items.map((it, i) => {
    const o = arr[i] || {};
    return { id: it.id, slot: String(o.slot || 'No match').trim(), silhouette: String(o.silhouette || '').trim().slice(0, 80),
      colour: String(o.colour || '').trim().slice(0, 40), score: Math.max(0, Math.min(100, parseInt(o.score) || 0)),
      reason: String(o.reason || '').trim().slice(0, 180), excludeReason: String(o.excludeReason || '').trim().slice(0, 120) };
  });
}

// ── Invoice ingest: read a supplier bill → already-ordered line items ────
// The founder uploads the vendor's invoice (photo, PDF or Excel) for stock
// that is ALREADY ordered / on the way. We read it and return line items so
// the buyer can review + confirm, then apply them as open-to-buy `onOrder`
// (units) and `onOrderCost` (real unit ₹) per category/fit. Nothing here
// touches Shopify or the design photo pool.
const invoiceUpload = multer({
  storage: multer.memoryStorage(),   // parsed in-memory then discarded — bills aren't kept
  limits: { fileSize: 25 * 1024 * 1024, files: 20 },
  fileFilter: (req, file, cb) => {
    const ok = /^image\//.test(file.mimetype) ||
      file.mimetype === 'application/pdf' ||
      /sheet|excel|csv|officedocument|ms-excel/i.test(file.mimetype) ||
      /\.(xlsx|xls|csv|pdf|png|jpe?g|webp|gif)$/i.test(file.originalname || '');
    cb(null, ok);
  }
});
function runInvoiceUpload(req, res) {
  return new Promise((resolve) => {
    invoiceUpload.array('files', 20)(req, res, (err) => {
      if (err) {
        const map = { LIMIT_FILE_SIZE: 'A file is larger than 25 MB — compress it and retry.',
          LIMIT_FILE_COUNT: 'Too many files at once (max 20).', LIMIT_UNEXPECTED_FILE: 'Unexpected upload field.' };
        resolve({ ok: false, error: map[err.code] || ('Upload error: ' + (err.message || err.code || 'unknown')) });
      } else resolve({ ok: true });
    });
  });
}
// Turn an Excel/CSV buffer into a compact text table Claude can read as an
// invoice. Every sheet is dumped as CSV so column headers stay intact.
function excelToText(buffer) {
  if (!XLSX) return '';
  try {
    const wb = XLSX.read(buffer, { type: 'buffer' });
    return wb.SheetNames.map(name => 'SHEET: ' + name + '\n' + XLSX.utils.sheet_to_csv(wb.Sheets[name])).join('\n\n').slice(0, 60000);
  } catch { return ''; }
}
// One Claude call: read whatever invoice content we gathered (images, PDFs,
// text tables) and return normalised garment line items.
async function scoreInvoice(content) {
  const catList = 'Trousers, Shirts, T-Shirts & Polos';
  content.push({ type: 'text', text:
`The blocks above are one or more supplier INVOICES / bills for casual clothing already ordered by an Indian premium-casual brand. Extract EVERY garment line item you can see.\n\n` +
`For each line item report:\n` +
`1) "description" — the item text as printed (max 12 words).\n` +
`2) "category" — EXACTLY one of: ${catList}. A collared button-up = Shirts; a round-neck/polo knit top = T-Shirts & Polos; any lower-body pant/chino/trouser/jean = Trousers. If it is clearly none of these apparel types, use "Other".\n` +
`3) "size" — the size as printed (e.g. "M", "32", or "" if none).\n` +
`4) "colour" — the colour word if shown, else "".\n` +
`5) "qty" — INTEGER quantity ordered for that line (default 1 if a line clearly means one unit).\n` +
`6) "unitPrice" — the per-unit price in the invoice's currency as a NUMBER (no symbols). If only a line total + qty are shown, divide to get the unit price. If unknown, use 0.\n` +
`7) "print" — for Shirts / T-Shirts only, the surface type if the text makes it clear: "solid" (plain colour) or "others" (any print / graphic / embroidery / checks / stripes / pattern). If unclear or a Trouser, use "".\n\n` +
`Ignore non-garment lines (freight, GST/tax rows, subtotals, totals, discounts). Do NOT invent items.\n` +
`Return STRICT JSON ONLY: {"items":[{"description":"..","category":"..","size":"..","colour":"..","qty":1,"unitPrice":0,"print":".."}, ...]}.` });
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 90000);
  let r;
  try {
    r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: AI_MODEL, max_tokens: 4000, temperature: 0, messages: [{ role: 'user', content }] }),
      signal: ctrl.signal
    });
  } finally { clearTimeout(timer); }
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error((j.error && j.error.message) || ('HTTP ' + r.status));
  const parsed = extractJson((j.content || []).map(c => c.text || '').join(''));
  const arr = parsed && Array.isArray(parsed.items) ? parsed.items : [];
  return arr.map(o => {
    let qty = parseInt(o.qty, 10); if (!isFinite(qty) || qty < 0) qty = 0;
    let price = Number(o.unitPrice); if (!isFinite(price) || price < 0) price = 0;
    return {
      description: String(o.description || '').trim().slice(0, 120),
      category: normCasualCategory(o.category),          // → 'Trouser' | 'Shirt' | 'T-shirt' | null
      size: String(o.size || '').trim().slice(0, 12),
      colour: String(o.colour || '').trim().slice(0, 40),
      qty, unitPrice: Math.round(price),
      fit: null,                                         // the buyer picks the fit in the review table
      printType: printBucket(o.print) || null            // pre-fill print bucket for uppers when the invoice says so
    };
  }).filter(x => x.qty > 0 || x.description);
}

// ── Hierarchy-wise allocation ────────────────────────────────────
// Largest-remainder integer split of `n` units across weighted buckets so the
// parts always sum back to n.
function splitInts(n, weights) {
  const keys = Object.keys(weights);
  const tot = keys.reduce((s, k) => s + Math.max(0, +weights[k] || 0), 0);
  if (n <= 0 || tot <= 0) { const z = {}; keys.forEach(k => z[k] = 0); return z; }
  const raw = {}, base = {}; let used = 0;
  keys.forEach(k => { raw[k] = n * Math.max(0, +weights[k] || 0) / tot; base[k] = Math.floor(raw[k]); used += base[k]; });
  let rem = n - used;
  keys.map(k => ({ k, f: raw[k] - Math.floor(raw[k]) })).sort((a, b) => b.f - a.f)
    .forEach((o, i) => { if (i < rem) base[o.k]++; });
  return base;
}
// 2-D LARGEST-REMAINDER grid. Splits `total` across a rows×cols grid where each
// cell's share = (rowW[r]/ΣrowW) × (colW[c]/ΣcolW). Returns the raw fractional
// target per cell AND an integer rounding whose cells ALWAYS re-sum to `total`
// (leftover pieces go to the cells with the biggest fractions). This is what lets
// the Trouser colour×fit grid show "0.41 → 0" while the whole plan stays exact.
function lrGrid(total, rowW, colW) {
  const rKeys = Object.keys(rowW), cKeys = Object.keys(colW);
  const rTot = rKeys.reduce((s, k) => s + Math.max(0, +rowW[k] || 0), 0) || 1;
  const cTot = cKeys.reduce((s, k) => s + Math.max(0, +colW[k] || 0), 0) || 1;
  const raw = {}, rounded = {}, cells = [];
  rKeys.forEach(r => {
    raw[r] = {}; rounded[r] = {};
    cKeys.forEach(c => {
      const frac = Math.max(0, total) * (Math.max(0, +rowW[r] || 0) / rTot) * (Math.max(0, +colW[c] || 0) / cTot);
      raw[r][c] = frac; rounded[r][c] = Math.floor(frac);
      cells.push({ r, c, rem: frac - Math.floor(frac) });
    });
  });
  let used = 0; rKeys.forEach(r => cKeys.forEach(c => { used += rounded[r][c]; }));
  let rem = Math.max(0, Math.round(total) - used);
  cells.sort((a, b) => b.rem - a.rem);
  for (let i = 0; i < cells.length && rem > 0; i++, rem--) rounded[cells[i].r][cells[i].c]++;
  return { raw, rounded };
}
// Turn a merged pct map into a normalised {key,label,pct,share} list for display.
function pctRows(map, labelFor) {
  const tot = Object.values(map).reduce((s, v) => s + Math.max(0, +v || 0), 0) || 1;
  return Object.keys(map).map(k => ({ key: k, label: labelFor(k), pct: Math.max(0, +map[k] || 0),
    share: Math.round(Math.max(0, +map[k] || 0) / tot * 100) }));
}

function buildPlan(cands, settings) {
  const active = cands.filter(c => !c.dupeOf);
  // Which categories are in play + each one's rupee budget.
  const enabledKeys = CAT_KEYS.filter(k => settings.categories[k] && settings.categories[k].enabled);
  // Budget is set manually per category (no total-split mode any more).
  const catBudget = {};
  enabledKeys.forEach(k => { catBudget[k] = Math.max(0, settings.categories[k].budget || 0); });

  const byCat = {}; active.forEach(c => { const k = c.category || 'Unsorted'; (byCat[k] = byCat[k] || []).push(c); });

  const categories = CAT_KEYS.map(catKey => {
    const spec = CAT_BY_KEY[catKey];
    const cfg = settings.categories[catKey];
    const enabled = enabledKeys.includes(catKey);
    let budget = catBudget[catKey] || 0;
    // MONEY is the anchor. We never need a real article price to plan — the
    // whole tree is a % split of rupees. `expPrice` is only an EDITABLE
    // ESTIMATE used to turn a rupee budget into a soft piece-count; it is
    // replaced by the vendor's real quote at PO time.
    const expPrice = Math.max(1, cfg.avgCost || spec.avgCost);
    // SET = pieces in ONE design's size run. The founder types ABSOLUTE per-size
    // quantities (alpha S/M/L/XL/XXL/XXXL or numeric 26/28/…); their SUM is the set
    // size. A design therefore = SET pieces. The saved `sizes` map can hold BOTH an
    // alpha and a numeric key set at once (seed defaults + system swaps), so we count
    // ONLY the keys belonging to the chosen sizeSystem — a numeric key matches /^\d+$/.
    const sizeIsNumeric = k => /^\d+$/.test(k);
    const activeSizeKeys = Object.keys(cfg.sizes || {}).filter(k =>
      cfg.sizeSystem === 'numeric' ? sizeIsNumeric(k) : !sizeIsNumeric(k));
    const SET = Math.max(1, activeSizeKeys.reduce((a, k) => a + Math.max(0, +cfg.sizes[k] || 0), 0));
    // The saved `sizes` map can hold BOTH the alpha and numeric key sets at once
    // (seed defaults + a later system swap leaves the retired keys behind). Every
    // place that enumerates sizes — the % breakdown, the size aggregate, the
    // per-design size fan-out — must use ONLY the active system's keys, or the
    // pieces get counted twice (once as S/M/L, once as 26/28/…). `activeSizes`
    // is `cfg.sizes` pruned to just the active-system keys.
    const activeSizes = {};
    activeSizeKeys.forEach(k => { activeSizes[k] = cfg.sizes[k]; });
    // Category PIECE target — THREE sizing modes the founder can pick per category:
    //   'cost'    → pieces = budget ÷ est price (money-anchored).
    //   'units'   → founder typed the piece target directly.
    //   'designs' → founder typed the NUMBER OF DESIGNS; pieces = designs × SET.
    //               This is the spec-first Trouser intake — no price needed (real
    //               prices only exist AFTER photos are sent to the vendor). We
    //               back-solve a notional budget so the rupee cascade below still
    //               lands on the right piece counts per fit cell.
    const catUnits = !enabled ? 0
      : (cfg.sizeMode === 'designs' ? Math.max(0, cfg.designs || 0) * SET
        : cfg.sizeMode === 'units' ? Math.max(0, cfg.targetUnits || 0)
        : Math.round(budget / expPrice));
    if (enabled && (cfg.sizeMode === 'units' || cfg.sizeMode === 'designs')) budget = catUnits * expPrice;
    const estUnits = catUnits;
    // Total DESIGNS = the number that cascades down print → fit → colour, so colour
    // % lands on the count of designs (impactful), not a piece count that collapses
    // to 1. In 'designs' mode it's exactly what the founder typed.
    const catDesigns = (cfg.sizeMode === 'designs') ? Math.max(0, cfg.designs || 0) : Math.round(catUnits / SET);

    const fitLabel = k => (spec.fits.find(f => f.key === k) || (cfg.extraFits.find(f => f.key === k)) || { label: k }).label;
    const fitRows = pctRows(cfg.fits, fitLabel);
    // HIERARCHY step 1 — category BUDGET flows to the fits by fit %.
    const fitBudget = splitInts(budget, cfg.fits);
    const sizePctRows = pctRows(activeSizes, k => k);

    // The batch pool splits in two: FRESH photos to curate into new buys, and
    // ALREADY-ORDERED members imported from a supplier PO. Ordered items are not
    // curated — they're already bought; they just NET this category's targets and
    // render as read-only "already ordered" cards inside their own fit box.
    const allPool = byCat[catKey] || [];
    const pool = allPool.filter(c => !c.ordered);
    const orderedPool = allPool.filter(c => c.ordered);
    const catColourAgg = {};        // observed colour → {budget, estUnits, photos}
    const catSizeAgg = {}; Object.keys(activeSizes).forEach(k => catSizeAgg[k] = 0);
    // Uppers (Shirts/T-shirts) carry a PRINT-TYPE level ABOVE fit: the category
    // budget first splits by print-type %, then each print-type's slice splits by
    // fit %. Trousers keep the flat category→fit split.
    const hasPrint = catHasPrintTypes(catKey);
    // Trousers run a colour×fit HARD grid: fit is a hard cap too (not the soft
    // category-wide guide the tops use). Flag comes from the spec.
    const fitHard = !!spec.fitHard;

    // Already-ordered PO members, bucketed by the SAME cell key the buy plan uses
    // ("print::fit" for uppers, bare fit for trousers). Each cell's ordered qty
    // nets its piece target; the items list renders as read-only cards below.
    const orderedByCell = {};
    orderedPool.forEach(c => {
      const key = hasPrint ? (printKeyOf(c) + '::' + c.fit) : c.fit;
      if (!key) return;
      const o = orderedByCell[key] = orderedByCell[key] || { qty: 0, priceQty: 0, items: [] };
      const q = Math.max(0, c.orderedQty || 0);
      o.qty += q; o.priceQty += Math.max(0, c.orderedCost || 0) * q; o.items.push(c);
    });
    const orderedQtyOf = k => (orderedByCell[k] ? orderedByCell[k].qty : 0);

    // BALANCED-GRID selection. The uploads are the vendor's FULL option set, not a
    // shortlist — so the app CURATES a balanced buy toward the %s, within the piece
    // target. The category piece target cascades down the SAME %s the money uses
    // → each fit-cell (print::fit for uppers, fit for trousers) gets a NET piece
    // target (after existing POs), which then splits by colour % into per-colour
    // cells. In each colour cell we keep the BEST designs (manual pins first, then
    // AI rating) up to that cell's design count; extras are held ('overfilled'),
    // colours not in the mix are held ('offlist'), and a colour we're SHORT of is
    // flagged ("needs sourcing") with its unfilled budget RESERVED — never poured
    // into another colour/fit/print. Manual excludes hold out; manual pins force in.
    const keyTargetUnits = {};   // fit-cell key → gross piece target (before onOrder)
    if (hasPrint) {
      const pbU = splitInts(budget, cfg.printTypes);
      Object.keys(pbU).forEach(pk => {
        const fbU = splitInts(pbU[pk], cfg.fits);
        Object.keys(fbU).forEach(fk => { keyTargetUnits[pk + '::' + fk] = expPrice > 0 ? Math.round(fbU[fk] / expPrice) : 0; });
      });
    } else {
      const fbU = splitInts(budget, cfg.fits);
      Object.keys(fbU).forEach(fk => { keyTargetUnits[fk] = expPrice > 0 ? Math.round(fbU[fk] / expPrice) : 0; });
    }

    let includedIds = new Set();
    let stage1Short = {};   // fit key → [] (per-fit colour shorts — retired, kept for shape)
    let excludeInfo = {};   // design id → { colour, offList, have, target } for held designs
    let catColourShort = [];// CATEGORY-level colour gaps: [{ key,label,have,want,need }]
    let catColourOver = []; // CATEGORY-level colour OVERSHOOTS: [{ key,label,target,selected,over }]
    let catPrintShort = []; // CATEGORY-level print gaps: [{ key,label,have,want,need }]
    let catFitShort = [];   // CATEGORY-level fit gaps: [{ key,label,have,want,need }]
    let catGrid = null;     // TROUSER colour×fit HARD grid: { colours, fits, cells:[{colour,fit,raw,rounded,have,selected,sourceMore,held}] }
    let catSourceMore = []; // TROUSER detailed shopping list: [{ colour,fit,fitLabel,need,needPieces,have }]
    let catCoverage = null; // TROUSER zero-coverage nudge: colours with a % that the buy is too small to include → source 1 design each. { colours:[{colour,pct}], addDesigns }
    let catOrderSheet = null; // TROUSER common-design order sheet (staircase): { set, fits:[{key,label,slots:[{slot,colours[],pieces}], bought:[{colour,designs,pieces}], toSourceDesigns, toSourceSkus, toSourcePieces, boughtDesigns}] }
    let catCalc = null;     // TRANSPARENT math: how each %/level turns into a target qty
    // NESTED BALANCE: PRINT (hard) → COLOUR (hard) → FIT (adaptive) → size.
    //   1. PRINT is the first cut: the category's design count splits by print %
    //      (solid/printed/checks). A print with fewer photos than its share buys
    //      what it has and flags "source more"; a print with more holds the extras.
    //      Budget never leaks between prints — this is what stops a 12-photo checks
    //      pile from swamping a 50%-target solid line.
    //   2. COLOUR is balanced INSIDE each print: that print's design count splits by
    //      colour %. Same over/under discipline, per colour, per print.
    //   3. FIT is ADAPTIVE: a print×colour cell only spreads across fits when it is
    //      big enough that every fit could get ≥1 design (cell target ≥ #fits).
    //      Below that threshold the grid would fragment to zero, so fit becomes a
    //      SOFT guide and we just keep the best-rated designs in the cell regardless
    //      of fit. As category volume grows, cells cross the threshold and fit
    //      balancing switches on by itself — no manual toggle.
    // Pins (includeOverride === true) are always kept; manual excludes always held.

    const selectIncluded = () => {
      const inc = new Set();
      stage1Short = {};
      excludeInfo = {};
      catColourShort = [];
      catColourOver = [];
      catPrintShort = [];
      catFitShort = [];
      catGrid = null;
      catSourceMore = [];
      catCoverage = null;
      catOrderSheet = null;
      catCalc = null;
      const colKeys = Object.keys(cfg.colours || {});
      const haveMix = colKeys.length > 0;
      const colW = {}; let cwTot = 0;
      colKeys.forEach(ck => { colW[ck] = Math.max(0, +cfg.colours[ck] || 0); cwTot += colW[ck]; });
      if (haveMix && cwTot <= 0) colKeys.forEach(ck => colW[ck] = 1);

      const fitKeys = Object.keys(cfg.fits || {});
      const fitW = {}; let fwTot = 0;
      fitKeys.forEach(fk => { fitW[fk] = Math.max(0, +cfg.fits[fk] || 0); fwTot += fitW[fk]; });
      if (fitKeys.length && fwTot <= 0) fitKeys.forEach(fk => fitW[fk] = 1);

      const assigned = pool.filter(c => c.fit && isIncluded(c));   // not manually excluded

      // No colour mix configured → can't balance, keep every design.
      if (!haveMix) {
        assigned.forEach(c => inc.add(c.id));
        pool.forEach(c => { if (c.includeOverride === true) inc.add(c.id); });
        return inc;
      }

      const byRatingSort = (a, b) =>
        ((b.includeOverride === true ? 1 : 0) - (a.includeOverride === true ? 1 : 0)) ||
        ((b.rating || 0) - (a.rating || 0));

      // ── TROUSER MODE — COLOUR × FIT HARD GRID ──────────────────────
      // Both colour AND fit are hard caps. The design count is split across a
      // colour×fit grid with largest-remainder rounding (cells re-sum to the exact
      // total). Each cell keeps its best-rated designs (pins first) up to its target;
      // extras are held. A cell we can't fill becomes a "source more" instruction —
      // never backfilled from another colour/fit (that gap is the shopping list).
      if (fitHard) {
        const g = lrGrid(catDesigns, colW, fitW);   // g.raw[ck][fk], g.rounded[ck][fk]
        const cellCand = {}, cellHave = {}, cellSel = {};
        const colUploaded = {}, fitUploaded = {};
        colKeys.forEach(ck => { cellCand[ck] = {}; cellHave[ck] = {}; cellSel[ck] = {};
          fitKeys.forEach(fk => { cellCand[ck][fk] = []; cellHave[ck][fk] = 0; cellSel[ck][fk] = 0; }); });
        // Bucket assigned designs into cells; off-list colour or fit → held.
        assigned.forEach(c => {
          const ck = canonColour(c.colour, colKeys);
          if (colKeys.indexOf(ck) < 0) {
            if (c.includeOverride === true) { inc.add(c.id); return; }
            excludeInfo[c.id] = { colour: (c.colour && String(c.colour).trim()) || 'Unspecified', offList: true, have: 1, target: 0 };
            return;
          }
          const fk = c.fit;
          if (fitKeys.indexOf(fk) < 0) {
            if (c.includeOverride === true) { inc.add(c.id); return; }
            excludeInfo[c.id] = { colour: ck, fit: fk || 'Unspecified', offListFit: true, offList: false, have: 1, target: 0 };
            return;
          }
          colUploaded[ck] = (colUploaded[ck] || 0) + 1;
          fitUploaded[fk] = (fitUploaded[fk] || 0) + 1;
          cellHave[ck][fk]++; cellCand[ck][fk].push(c);
        });
        // PO-import — already-purchased/paid Trouser lines net the grid at the
        // colour×fit CELL level (not just fit). Pieces convert to designs via ÷ SET
        // (Auto). A line whose colour OR fit isn't in this batch's mix is NEVER
        // backfilled into a planned cell — it becomes its own read-only "already
        // bought (not in this plan)" row.
        const ordCell = {}, ordOff = {};
        colKeys.forEach(ck => { ordCell[ck] = {}; fitKeys.forEach(fk => ordCell[ck][fk] = { pieces: 0, cost: 0, items: [] }); });
        orderedPool.forEach(c => {
          const pieces = Math.max(0, c.orderedQty || 0);
          if (pieces <= 0) return;
          const cost = Math.max(0, c.orderedCost || 0) * pieces;   // ₹ actual (unit × qty)
          const rawCol = (c.colour && String(c.colour).trim()) || 'Unspecified';
          const ck = canonColour(c.colour, colKeys);
          const fk = c.fit || '';
          const onPlan = colKeys.indexOf(ck) >= 0 && fitKeys.indexOf(fk) >= 0;
          if (onPlan) {
            const o = ordCell[ck][fk]; o.pieces += pieces; o.cost += cost; o.items.push(c);
          } else {
            const okey = rawCol + '::' + (fk || 'Unspecified');
            const o = ordOff[okey] = ordOff[okey] || { colour: rawCol, fit: fk || 'Unspecified', fitLabel: (fk ? fitLabel(fk) : 'Unspecified'), pieces: 0, cost: 0, items: [] };
            o.pieces += pieces; o.cost += cost; o.items.push(c);
          }
        });
        const pcToDesigns = pc => (SET > 0 ? Math.max(pc > 0 ? 1 : 0, Math.round(pc / SET)) : 0);
        // Fill each cell: pins first, then best-rated, up to the cell's hard target.
        colKeys.forEach(ck => fitKeys.forEach(fk => {
          const tgt = Math.max(0, g.rounded[ck][fk] || 0);
          const list = cellCand[ck][fk].slice().sort(byRatingSort);
          let taken = 0;
          list.forEach(c => { if (c.includeOverride === true && !inc.has(c.id)) { inc.add(c.id); cellSel[ck][fk]++; taken++; } });
          list.forEach(c => {
            if (inc.has(c.id)) return;
            if (taken >= tgt) { excludeInfo[c.id] = { colour: ck, fit: fk, offList: false, cellCapped: true, cellTarget: tgt, have: cellHave[ck][fk], target: tgt }; return; }
            inc.add(c.id); cellSel[ck][fk]++; taken++;
          });
        }));
        // Grid output + detailed source-more list.
        catGrid = { base: catDesigns, set: SET, catUnits,
          colours: colKeys.map(ck => ({ key: ck, label: ck, pct: cwTot ? Math.round(colW[ck] / cwTot * 100) : 0 })),
          fits: fitKeys.map(fk => ({ key: fk, label: fitLabel(fk), pct: fwTot ? Math.round(fitW[fk] / fwTot * 100) : 0 })),
          cells: [] };
        colKeys.forEach(ck => fitKeys.forEach(fk => {
          const tgt = Math.max(0, g.rounded[ck][fk] || 0), have = cellHave[ck][fk] || 0, sel = cellSel[ck][fk] || 0;
          const o = ordCell[ck][fk] || { pieces: 0, cost: 0 };
          const ordPieces = o.pieces || 0, ordDesigns = pcToDesigns(ordPieces), ordCost = Math.round(o.cost || 0);
          // Paid designs count toward the cell target — remaining need drops by them.
          const need = Math.max(0, tgt - sel - ordDesigns);
          catGrid.cells.push({ colour: ck, fit: fk, fitLabel: fitLabel(fk),
            raw: Math.round((g.raw[ck][fk] || 0) * 100) / 100, rounded: tgt,
            have, selected: sel, sourceMore: need, held: Math.max(0, have - sel),
            orderedDesigns: ordDesigns, orderedPieces: ordPieces, orderedCost: ordCost });
          if (need > 0) catSourceMore.push({ colour: ck, fit: fk, fitLabel: fitLabel(fk), need, needPieces: need * SET, have, paidDesigns: ordDesigns });
        }));
        catSourceMore.sort((a, b) => b.need - a.need);
        // Off-plan already-bought rows — colour/fit NOT in this batch's desired mix.
        // Shown as paid, need 0, so the founder sees the extra buy without it polluting
        // the planned grid or the source-more shopping list.
        catGrid.offPlan = Object.keys(ordOff).map(k => {
          const o = ordOff[k];
          return { colour: o.colour, fit: o.fit, fitLabel: o.fitLabel,
            orderedPieces: o.pieces, orderedDesigns: pcToDesigns(o.pieces), orderedCost: Math.round(o.cost) };
        }).sort((a, b) => b.orderedDesigns - a.orderedDesigns);
        // Totals split — money already SPENT (actual ₹, paid) vs still-to-source (estimate ₹).
        let boughtDesigns = 0, boughtPieces = 0, boughtCost = 0;
        catGrid.cells.forEach(c => { boughtDesigns += c.orderedDesigns; boughtPieces += c.orderedPieces; boughtCost += c.orderedCost; });
        catGrid.offPlan.forEach(o => { boughtDesigns += o.orderedDesigns; boughtPieces += o.orderedPieces; boughtCost += o.orderedCost; });
        const toSourceDesigns = catSourceMore.reduce((a, s) => a + s.need, 0);
        catGrid.split = {
          bought: { designs: boughtDesigns, pieces: boughtPieces, cost: boughtCost },
          toSource: { designs: toSourceDesigns, pieces: toSourceDesigns * SET, cost: Math.round(toSourceDesigns * SET * expPrice) }
        };
        // Zero-coverage nudge — colours that carry a % but the buy is too small to
        // give them even one design (both fit cells rounded to 0). The founder wants
        // an explicit "add N more designs" instruction: source ONE design in each such
        // colour specifically (no backfill, no re-rounding the whole grid).
        const zeroCov = [];
        colKeys.forEach(ck => {
          if (ck === 'Other') return;
          if ((+colW[ck] || 0) <= 0) return;
          const tot = fitKeys.reduce((a, fk) => a + Math.max(0, g.rounded[ck][fk] || 0), 0);
          // Already bought some of this colour via a PO → it's covered, no nudge.
          const paid = fitKeys.reduce((a, fk) => a + ((ordCell[ck] && ordCell[ck][fk] ? ordCell[ck][fk].pieces : 0)), 0);
          if (tot === 0 && paid <= 0) zeroCov.push({ colour: ck, pct: cwTot ? Math.round(colW[ck] / cwTot * 100) : 0, addDesigns: 1, addPieces: SET });
        });
        catCoverage = zeroCov.length ? { colours: zeroCov, addDesigns: zeroCov.length, addPieces: zeroCov.length * SET } : null;
        // Common-design ORDER SHEET (staircase) — collapse the colour×fit plan into
        // the MINIMUM number of base designs per fit, each repeated across colours.
        // Per fit: still-to-order per colour = rounded target − already-bought(PO).
        // #base designs = the largest single-colour still-to-order count; design
        // slot i runs in every colour whose remaining count > i (sorted desc). The
        // already-bought PO designs are subtracted here AND surfaced separately so
        // the founder sees both "make these" and "already have these".
        const SHEET_LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
        catOrderSheet = { set: SET, fits: fitKeys.map(fk => {
          const cols = colKeys.filter(ck => ck !== 'Other').map(ck => {
            const cell = catGrid.cells.find(c => c.colour === ck && c.fit === fk) || {};
            const target = Math.max(0, cell.rounded || 0);
            const bought = Math.max(0, cell.orderedDesigns || 0);
            return { colour: ck, order: Math.max(0, target - bought), bought };
          });
          const src = cols.filter(c => c.order > 0).sort((a, b) => b.order - a.order);
          const maxN = src.reduce((m, c) => Math.max(m, c.order), 0);
          const slots = [];
          for (let i = 0; i < maxN; i++) {
            const slotCols = src.filter(c => c.order > i).map(c => c.colour);
            slots.push({ slot: SHEET_LETTERS[i] || ('#' + (i + 1)), colours: slotCols, pieces: slotCols.length * SET });
          }
          const bought = cols.filter(c => c.bought > 0).map(c => ({ colour: c.colour, designs: c.bought, pieces: c.bought * SET }));
          const toSourceSkus = src.reduce((a, c) => a + c.order, 0);
          return { key: fk, label: fitLabel(fk), slots, bought,
            toSourceDesigns: slots.length, toSourceSkus, toSourcePieces: toSourceSkus * SET,
            boughtDesigns: bought.reduce((a, b) => a + b.designs, 0) };
        }).filter(f => f.slots.length || f.bought.length) };
        // Category-wide rollups (feed the existing colour/fit chips + math panel).
        colKeys.forEach(ck => {
          if (ck === 'Other') return;
          const tgt = fitKeys.reduce((a, fk) => a + Math.max(0, g.rounded[ck][fk] || 0), 0);
          const sel = fitKeys.reduce((a, fk) => a + (cellSel[ck][fk] || 0), 0);
          const upl = colUploaded[ck] || 0;
          if (tgt - sel > 0) catColourShort.push({ key: ck, label: ck, have: upl, want: tgt, need: tgt - sel });
          if (upl > tgt) catColourOver.push({ key: ck, label: ck, target: tgt, selected: sel, over: Math.max(0, sel - tgt), held: Math.max(0, upl - sel) });
        });
        fitKeys.forEach(fk => {
          const tgt = colKeys.reduce((a, ck) => a + Math.max(0, g.rounded[ck][fk] || 0), 0);
          const sel = colKeys.reduce((a, ck) => a + (cellSel[ck][fk] || 0), 0);
          if (tgt - sel > 0) catFitShort.push({ key: fk, label: fitLabel(fk), have: fitUploaded[fk] || 0, want: tgt, need: tgt - sel });
        });
        catColourShort.sort((a, b) => b.need - a.need);
        catColourOver.sort((a, b) => (b.over + b.held) - (a.over + a.held));
        catFitShort.sort((a, b) => b.need - a.need);
        let szT0 = 0; Object.keys(activeSizes).forEach(sk => szT0 += Math.max(0, +activeSizes[sk] || 0));
        catCalc = { base: catDesigns, catUnits, set: SET, totalUploaded: assigned.length,
          prints: [],
          colours: colKeys.map(ck => ({ key: ck, label: ck, pct: cwTot ? Math.round(colW[ck] / cwTot * 100) : 0,
            target: fitKeys.reduce((a, fk) => a + Math.max(0, g.rounded[ck][fk] || 0), 0), have: colUploaded[ck] || 0 })),
          fits: fitKeys.map(fk => ({ key: fk, label: fitLabel(fk), pct: fwTot ? Math.round(fitW[fk] / fwTot * 100) : 0,
            target: colKeys.reduce((a, ck) => a + Math.max(0, g.rounded[ck][fk] || 0), 0), have: fitUploaded[fk] || 0 })),
          sizes: sizePctRows.map(s => ({ key: s.key, label: s.label,
            pct: szT0 ? Math.round(Math.max(0, +cfg.sizes[s.key] || 0) / szT0 * 100) : 0 })) };
        pool.forEach(c => { if (c.includeOverride === true) inc.add(c.id); });
        return inc;
      }

      // PRINT weights → design count per print (trousers: one bucket = whole cat).
      const printKeys = hasPrint ? Object.keys(cfg.printTypes || {}) : ['_all'];
      const printW = {}; let pwTot = 0;
      printKeys.forEach(pk => { printW[pk] = hasPrint ? Math.max(0, +cfg.printTypes[pk] || 0) : 1; pwTot += printW[pk]; });
      if (pwTot <= 0) printKeys.forEach(pk => printW[pk] = 1);
      const printTarget = splitInts(catDesigns, printW);

      // WEIGHTED-PARALLEL selection.
      //   • PRINT is the ANCHOR: each print keeps its exact design target (a hard
      //     per-bucket capacity). Print only misses when supply runs out.
      //   • COLOUR and FIT are CATEGORY-WIDE targets (colour% × 17, fit% × 17),
      //     filled from ANY print — this is what removes the old print-box
      //     stranding (a black need can be met by a black shirt of any print).
      //   • Inside each print bucket we choose WHICH designs fill it by whichever
      //     best close the biggest remaining colour+fit gaps (rating as tiebreak).
      const colTarget = splitInts(catDesigns, colW);
      const fitCatTarget = fitKeys.length ? splitInts(catDesigns, fitW) : {};

      // Bucket assigned designs by print; off-list print or off-list colour → held.
      // colUploaded / fitUploaded = category-wide supply for the math panel.
      const byPrint = {};
      const colUploaded = {}; const fitUploaded = {};
      assigned.forEach(c => {
        const pk = hasPrint ? printKeyOf(c) : '_all';
        if (printKeys.indexOf(pk) < 0) {
          if (c.includeOverride === true) { inc.add(c.id); return; }
          excludeInfo[c.id] = { colour: (c.colour && String(c.colour).trim()) || 'Unspecified', print: pk, offListPrint: true, offList: false, have: 1, target: 0 };
          return;
        }
        const ck = canonColour(c.colour, colKeys);
        if (colKeys.indexOf(ck) < 0) {
          if (c.includeOverride === true) { inc.add(c.id); return; }
          excludeInfo[c.id] = { colour: (c.colour && String(c.colour).trim()) || 'Unspecified', print: pk, offList: true, have: 1, target: 0 };
          return;
        }
        colUploaded[ck] = (colUploaded[ck] || 0) + 1;
        if (c.fit) fitUploaded[c.fit] = (fitUploaded[c.fit] || 0) + 1;
        (byPrint[pk] = byPrint[pk] || []).push(c);
      });

      // Running CATEGORY-WIDE needs (shared across every print bucket).
      const needCol = {}; colKeys.forEach(ck => needCol[ck] = Math.max(0, colTarget[ck] || 0));
      const needFit = {}; fitKeys.forEach(fk => needFit[fk] = Math.max(0, fitCatTarget[fk] || 0));
      // Per-print remaining capacity (the anchor).
      const printCap = {}; printKeys.forEach(pk => printCap[pk] = Math.max(0, printTarget[pk] || 0));

      const byRating = (a, b) =>
        ((b.includeOverride === true ? 1 : 0) - (a.includeOverride === true ? 1 : 0)) ||
        ((b.rating || 0) - (a.rating || 0));

      // Manual pins go in FIRST and consume from every tally.
      printKeys.forEach(pk => {
        (byPrint[pk] || []).forEach(c => {
          if (c.includeOverride === true) {
            inc.add(c.id);
            printCap[pk] = Math.max(0, printCap[pk] - 1);
            const ck = canonColour(c.colour, colKeys);
            if (needCol[ck] != null) needCol[ck]--;
            if (needFit[c.fit] != null) needFit[c.fit]--;
          }
        });
      });

      // Candidate lists = non-pinned designs, best-rated first.
      const cand = {};
      printKeys.forEach(pk => { cand[pk] = (byPrint[pk] || []).filter(c => c.includeOverride !== true).sort(byRating); });

      // A design's value = how much its colour + fit close the biggest open needs.
      const scoreOf = c => {
        const ck = canonColour(c.colour, colKeys);
        return Math.max(0, needCol[ck] || 0) + Math.max(0, needFit[c.fit] || 0);
      };
      // GREEDY: repeatedly place the single best-scoring design across all print
      // buckets that still have capacity. PRINT and COLOUR are BOTH HARD CAPS — a
      // design is only eligible if its print bucket AND its colour still have room;
      // once a colour hits its % target the rest of that colour's photos are HELD
      // (never backfilled into another colour). FIT stays soft/parallel: it only
      // scores WHICH eligible design wins each slot (rating breaks ties). This can
      // leave a print bucket short when its only remaining photos are of colours
      // that are already full — that shortfall is flagged, not papered over.
      let guard = assigned.length + 5;
      while (guard-- > 0) {
        let best = null, bestPk = null, bestScore = -1, bestRating = -1;
        printKeys.forEach(pk => {
          if (printCap[pk] <= 0) return;
          const list = cand[pk];
          for (let i = 0; i < list.length; i++) {
            const c = list[i];
            if (inc.has(c.id)) continue;
            const ck = canonColour(c.colour, colKeys);
            if ((needCol[ck] || 0) <= 0) continue;   // COLOUR HARD CAP — no room, hold it
            const s = scoreOf(c), r = c.rating || 0;
            if (s > bestScore || (s === bestScore && r > bestRating)) { best = c; bestPk = pk; bestScore = s; bestRating = r; }
          }
        });
        if (!best) break;   // no capacity left anywhere, or nothing left to place
        inc.add(best.id);
        printCap[bestPk] = Math.max(0, printCap[bestPk] - 1);
        const bck = canonColour(best.colour, colKeys);
        if (needCol[bck] != null) needCol[bck]--;
        if (needFit[best.fit] != null) needFit[best.fit]--;
      }

      // Anything left in a print bucket wasn't bought → held. Two reasons now:
      //   • COLOUR full  — its colour hit the % cap (over-supplied that colour).
      //   • PRINT overfilled — more photos of this print than its design target.
      printKeys.forEach(pk => {
        (cand[pk] || []).forEach(c => {
          if (!inc.has(c.id)) {
            const ck = canonColour(c.colour, colKeys);
            const colTgt = Math.max(0, colTarget[ck] || 0);
            const colourCapped = (colUploaded[ck] || 0) > colTgt;
            excludeInfo[c.id] = { colour: ck, print: pk, offList: false, colourCapped, colTarget: colTgt, have: (byPrint[pk] || []).length, target: Math.max(0, printTarget[pk] || 0) };
          }
        });
      });

      // Category-wide SELECTED count per colour — needed for OVERSHOOT reporting
      // (a colour whose surplus uploads backfilled another colour's empty slots
      // because PRINT is the hard anchor, so it ends up above its own % target).
      const colSel = {};
      assigned.forEach(c => {
        if (!inc.has(c.id)) return;
        const ck = canonColour(c.colour, colKeys);
        if (colKeys.indexOf(ck) >= 0) colSel[ck] = (colSel[ck] || 0) + 1;
      });

      // SHORTFALLS at every level — a positive remaining need = truly no photo to
      // fill it (supply gap), because colour/fit could draw from ANY print.
      printKeys.forEach(pk => {
        if (pk === '_all') return;
        if (printCap[pk] > 0) catPrintShort.push({ key: pk, label: (PRINT_BY_KEY[pk] || { label: pk }).label,
          have: (byPrint[pk] || []).length, want: Math.max(0, printTarget[pk] || 0), need: printCap[pk] });
      });
      colKeys.forEach(ck => {
        if (ck === 'Other') return;
        const tgt = Math.max(0, colTarget[ck] || 0), sel = colSel[ck] || 0, upl = colUploaded[ck] || 0;
        if (needCol[ck] > 0) catColourShort.push({ key: ck, label: ck,
          have: upl, want: tgt, need: needCol[ck] });
        // OVER — colour is HARD-CAPPED at target, so any surplus uploads are HELD
        // (you over-supplied this colour vs its %). `over` = pin-driven excess past
        // the cap (rare); `held` = capped-out surplus photos not bought.
        if (upl > tgt) catColourOver.push({ key: ck, label: ck, target: tgt, selected: sel,
          over: Math.max(0, sel - tgt), held: Math.max(0, upl - sel) });
      });
      fitKeys.forEach(fk => {
        if (needFit[fk] > 0) catFitShort.push({ key: fk, label: fitLabel(fk),
          have: fitUploaded[fk] || 0, want: Math.max(0, fitCatTarget[fk] || 0), need: needFit[fk] });
      });
      catPrintShort.sort((a, b) => b.need - a.need);
      catColourShort.sort((a, b) => b.need - a.need);
      catColourOver.sort((a, b) => (b.over + b.held) - (a.over + a.held));
      catFitShort.sort((a, b) => b.need - a.need);

      // TRANSPARENT MATH — how each level's % turns into a category-wide target qty.
      // base = catDesigns (designs the %s split); each design = SET pieces.
      // `have` = uploaded photos of that print/colour/fit (supply on hand).
      let szTot = 0; Object.keys(activeSizes).forEach(sk => szTot += Math.max(0, +activeSizes[sk] || 0));
      catCalc = {
        base: catDesigns, catUnits, set: SET,
        totalUploaded: assigned.length,
        prints: printKeys.filter(pk => pk !== '_all').map(pk => ({
          key: pk, label: (PRINT_BY_KEY[pk] || { label: pk }).label,
          pct: pwTot ? Math.round(printW[pk] / pwTot * 100) : 0,
          target: Math.max(0, printTarget[pk] || 0),
          have: (byPrint[pk] || []).length
        })),
        colours: colKeys.map(ck => ({
          key: ck, label: ck,
          pct: cwTot ? Math.round(colW[ck] / cwTot * 100) : 0,
          target: Math.max(0, colTarget[ck] || 0),
          have: colUploaded[ck] || 0
        })),
        fits: fitKeys.map(fk => ({
          key: fk, label: fitLabel(fk),
          pct: fwTot ? Math.round(fitW[fk] / fwTot * 100) : 0,
          target: Math.max(0, fitCatTarget[fk] || 0),
          have: fitUploaded[fk] || 0
        })),
        sizes: sizePctRows.map(s => ({
          key: s.key, label: s.label,
          pct: szTot ? Math.round(Math.max(0, +cfg.sizes[s.key] || 0) / szTot * 100) : 0
        }))
      };

      pool.forEach(c => { if (c.includeOverride === true) inc.add(c.id); });
      return inc;
    };

    // Build ONE fit block. `fb` is this fit's rupee budget, `photos` its designs,
    // `otbKey` the open-to-buy key (composite "print::fit" for uppers, bare fit
    // key for trousers — so legacy trouser onOrder data still resolves).
    const buildFit = (fr, fb, photos, otbKey) => {
      const fitEst = enabled && expPrice > 0 ? Math.round(fb / expPrice) : 0;
      // OPEN-TO-BUY: existing POs for this exact fit — units already on the way
      // and the money they've already consumed. Keyed by otbKey so uppers net
      // per print-type × fit.
      // On-the-way = a hand-typed OTB number (cfg.onOrder) PLUS every already-
      // ordered PO member imported into this exact cell.
      const manualOnOrder = enabled ? Math.max(0, (cfg.onOrder && cfg.onOrder[otbKey]) || 0) : 0;
      const orderedCell = orderedByCell[otbKey] || null;
      const onOrder = enabled ? (manualOnOrder + (orderedCell ? orderedCell.qty : 0)) : 0;
      const orderedCost = (orderedCell && orderedCell.qty > 0)
        ? Math.round(orderedCell.priceQty / orderedCell.qty)
        : Math.max(0, (cfg.onOrderCost && cfg.onOrderCost[otbKey]) || 0);
      const freedBudget = enabled ? Math.round(Math.min(onOrder, fitEst) * (orderedCost > 0 ? orderedCost : expPrice)) : 0;
      // Budget left for NEW designs = fit budget MINUS what existing POs cost.
      const availBudget = Math.max(0, fb - freedBudget);

      // VARIETY inclusion is decided by selectIncluded: fit % sets how many
      // designs this fit sources, and colour % splits that within the fit. Here
      // we just read that decision. Manual pins (includeOverride === true) are in.
      const gatePassed = photos.filter(isIncluded);
      const ranked = gatePassed.slice().sort((a, b) => (b.rating || 0) - (a.rating || 0));
      const budgetIn = {};
      ranked.forEach(p => {
        if (p.includeOverride === true) { budgetIn[p.id] = true; return; }   // founder pinned it
        if (enabled && includedIds && includedIds.has(p.id)) budgetIn[p.id] = true;
      });

      // The KEPT designs share availBudget evenly; excluded designs get ₹0.
      const incList = ranked.filter(p => budgetIn[p.id]);
      const idWeights = {}; incList.forEach(p => idWeights[p.id] = 1);
      const designBud = incList.length ? splitInts(availBudget, idWeights) : {};

      const designs = photos.map(p => {
        const passedGate = isIncluded(p);
        const inc = passedGate && !!budgetIn[p.id];
        const db = inc ? (designBud[p.id] || 0) : 0;
        const du = inc && enabled && expPrice > 0 ? Math.round(db / expPrice) : 0;
        // A design's est units fan out over the SIZE ladder (editable %).
        const sizeUnits = splitInts(du, activeSizes);
        const sizes = sizePctRows.map(s => ({ key: s.key, label: s.label, share: s.share, units: sizeUnits[s.key] || 0 })).filter(s => s.units > 0);
        const colour = (p.colour && String(p.colour).trim()) || 'Unspecified';
        if (inc) {
          Object.keys(sizeUnits).forEach(sk => { catSizeAgg[sk] += sizeUnits[sk] || 0; });
          const ca = catColourAgg[colour] = catColourAgg[colour] || { budget: 0, estUnits: 0, photos: 0 };
          ca.budget += db; ca.estUnits += du; ca.photos += 1;
        }
        // A design is out because: the founder pulled it ('manual'), it fell
        // outside this fit's variety share ('variety'), or the category is off.
        // Drives the frontend "not included" label.
        const excludedReason = inc ? null
          : (p.includeOverride === false ? 'manual' : (enabled ? 'variety' : 'budget'));
        const excludedDetail = (excludedReason === 'variety') ? (excludeInfo[p.id] || null) : null;
        return Object.assign(publicCandidate(p), { budget: db, estUnits: du, colour, sizes, included: inc, excludedReason, excludedDetail });
      });
      designs.sort((a, b) => (b.included ? 1 : 0) - (a.included ? 1 : 0) || (b.rating || 0) - (a.rating || 0));

      const fitColMap = {};
      designs.forEach(dz => { if (!dz.included) return; const m = fitColMap[dz.colour] = fitColMap[dz.colour] || { budget: 0, estUnits: 0, photos: 0 }; m.budget += dz.budget; m.estUnits += dz.estUnits; m.photos += 1; });
      const colours = Object.keys(fitColMap).map(col => ({ key: col, label: col, budget: fitColMap[col].budget, estUnits: fitColMap[col].estUnits, photos: fitColMap[col].photos }))
        .sort((a, b) => b.budget - a.budget);

      const netUnits = Math.max(0, fitEst - onOrder);

      return {
        key: fr.key, label: fr.label, pct: fr.pct, share: fr.share, otbKey,
        budget: fb, availBudget, estUnits: photos.length ? designs.reduce((s, d) => s + d.estUnits, 0) : fitEst,
        target: fitEst, designsTarget: Math.round(fitEst / SET), set: SET, onOrder, orderedCost, netUnits, freedBudget,
        budgetHeld: gatePassed.length - incList.length,
        photoCount: photos.length, includedCount: incList.length, designs, colours, unassignedFit: false,
        // Already-ordered PO members that live in THIS cell — read-only cards.
        // A bill lists the SAME product across several size rows, so merge lines
        // that share (colour · vendor · unit ₹) into ONE product card whose size
        // run is the sum of its rows — exactly how a batch product is shown. Each
        // card carries every underlying candidate id so "Remove" can drop them all.
        ordered: mergeOrderedItems(orderedCell ? orderedCell.items : []),
        // Stage-1 sourcing gaps: colours this fit is short of, per the colour mix.
        colourShort: (enabled && stage1Short[otbKey]) ? stage1Short[otbKey] : []
      };
    };

    let fits = [], printGroups = null;
    if (hasPrint) {
      // Step 1 — category budget → print-type %.
      const printRows = pctRows(cfg.printTypes, k => (PRINT_BY_KEY[k] || { label: k }).label);
      const printBudget = splitInts(budget, cfg.printTypes);
      // VARIETY: each print×fit cell sources exactly the designs uploaded to it;
      // colour % then enforces the mix within that cell (drop overshoot, flag
      // shortfall). Print % / fit % drive the money split, not the design count.
      includedIds = selectIncluded(c => printKeyOf(c) + '::' + c.fit);
      printGroups = printRows.map(pr => {
        const pb = printBudget[pr.key] || 0;
        const groupPool = pool.filter(c => c.fit && printKeyOf(c) === pr.key);
        // Step 2 — this print-type's slice → fit %.
        const fitBudgetG = splitInts(pb, cfg.fits);
        const gfits = fitRows.map(fr => buildFit(fr, fitBudgetG[fr.key] || 0, groupPool.filter(c => c.fit === fr.key), pr.key + '::' + fr.key));
        fits = fits.concat(gfits);   // flat list too, for export / roll-ups
        const grpTarget = enabled && expPrice > 0 ? Math.round(pb / expPrice) : 0;
        const grpOrderedTowardTarget = gfits.reduce((s, f) => s + Math.min(Math.max(0, f.onOrder || 0), Math.max(0, f.target || 0)), 0);
        return {
          key: pr.key, label: pr.label, pct: pr.pct, share: pr.share, budget: pb,
          target: grpTarget,
          estUnits: gfits.reduce((s, f) => s + f.estUnits, 0),
          onOrder: gfits.reduce((s, f) => s + (f.onOrder || 0), 0),
          freedBudget: gfits.reduce((s, f) => s + (f.freedBudget || 0), 0),
          netUnits: Math.max(0, grpTarget - grpOrderedTowardTarget),
          photoCount: groupPool.length, fits: gfits
        };
      });
    } else {
      const fitBudget = splitInts(budget, cfg.fits);
      // VARIETY: each fit sources exactly the designs uploaded to it; colour % then
      // enforces the mix within that fit. Fit % drives the money split, not the count.
      includedIds = selectIncluded(c => c.fit);
      fits = fitRows.map(fr => buildFit(fr, fitBudget[fr.key] || 0, pool.filter(c => c.fit === fr.key), fr.key));
    }

    // Designs the AI couldn't assign a fit to — shown at the end so the founder
    // can still tag them. Kept in flat `fits` (export sees it) and rendered
    // outside the print groups for uppers.
    const noFit = pool.filter(c => !c.fit);
    let unassigned = null;
    if (noFit.length) {
      unassigned = { key: '_unassigned', label: 'Fit not detected', pct: 0, share: 0, budget: 0, estUnits: 0, photoCount: noFit.length,
        designs: noFit.map(p => Object.assign(publicCandidate(p), { budget: 0, estUnits: 0, colour: (p.colour && String(p.colour).trim()) || 'Unspecified', sizes: [] })), colours: [], unassignedFit: true };
      fits.push(unassigned);
    }

    // Category colour roll-up — observed across all fits (nothing preset).
    const colours = Object.keys(catColourAgg).map(col => ({ key: col, label: col, budget: catColourAgg[col].budget, estUnits: catColourAgg[col].estUnits, photos: catColourAgg[col].photos }))
      .sort((a, b) => b.budget - a.budget);

    const onOrderTotal = fits.reduce((s, f) => s + (f.onOrder || 0), 0);
    const freedBudgetTotal = fits.reduce((s, f) => s + (f.freedBudget || 0), 0);
    // "To source" anchors to the AUTHORITATIVE category target (estUnits), not to
    // the sum of per-fit rounded targets — otherwise independent per-fit rounding
    // makes Target and To source drift apart (e.g. 154 vs 156) even with nothing
    // ordered. Ordered units only count toward the target up to each fit's OWN
    // target: a fit ordered past its target can't shrink another fit (the "never
    // move budget between boxes" rule).
    const orderedTowardTarget = fits.reduce((s, f) => s + Math.min(Math.max(0, f.onOrder || 0), Math.max(0, f.target || 0)), 0);
    const netUnitsTotal = Math.max(0, estUnits - orderedTowardTarget);

    return {
      category: catKey, label: spec.label, designNote: spec.designNote,
      enabled, budget, expPrice, estUnits, poolCount: pool.length, hasPrint,
      sizeMode: cfg.sizeMode, targetUnits: cfg.targetUnits, set: SET, designsTarget: catDesigns,
      onOrder: onOrderTotal, freedBudget: freedBudgetTotal, netUnits: netUnitsTotal,
      // Colour gaps are now a CATEGORY-level list (colour balance is category-wide,
      // not per fit cell) — "source more <colour>".
      colourShort: (enabled ? catColourShort : []),
      colourOver: (enabled ? catColourOver : []),
      printShort: (enabled ? catPrintShort : []),
      fitShort: (enabled ? catFitShort : []),
      // Trouser colour×fit HARD grid + its detailed "source more" shopping list.
      grid: (enabled ? catGrid : null),
      sourceMore: (enabled ? catSourceMore : []),
      coverage: (enabled ? catCoverage : null),
      orderSheet: (enabled ? catOrderSheet : null),
      fitHard,
      calc: (enabled ? catCalc : null),
      fits, printGroups, unassigned,
      // Size ladder is still a decided % (rolled up from the design runs).
      sizes: sizePctRows.map(r => ({ ...r, units: catSizeAgg[r.key] || 0 })),
      colours   // observed colours only
    };
  });

  const unsorted = (byCat['Unsorted'] || []).map(publicCandidate);
  const totalBudget = enabledKeys.reduce((s, k) => s + (catBudget[k] || 0), 0);
  const totalEstUnits = categories.reduce((s, c) => s + (c.enabled ? c.estUnits : 0), 0);
  return { budgetMode: settings.budgetMode, totalBudget, totalEstUnits, categories, unsorted };
}

// ── Routes ───────────────────────────────────────────────────────
router.get('/api/casuals/spec', (req, res) => {
  res.json({ success: true, categories: CASUALS_SPEC });
});

// Generate the assortment itself for the simplified workflow. This is a
// strategic planning pass, separate from the later visual catalogue matching.
router.post('/api/casuals/simple-plan', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI planning is not enabled. Set ANTHROPIC_API_KEY in Railway.' });
    const b = req.body || {};
    const gender = String(b.gender || '').trim();
    const category = String(b.category || '').trim();
    const styleTarget = Math.max(1, Math.min(30, parseInt(b.styleTarget) || 0));
    const budget = Math.max(0, parseInt(b.budget) || 0);
    const launchDate = String(b.launchDate || '').trim() || 'next available launch';
    if (!gender || !category || !styleTarget || !budget) return res.status(400).json({ success: false, error: 'Gender, category, style count and budget are required.' });
    const prompt = `You are the senior merchandise planner for SANKI, an Indian premium-casual fashion brand that buys ready-stock, white-label garments from specialist Chinese vendors. Create a disciplined MICRO-BUY assortment plan.

Gender: ${gender}
Category: ${category}
Total distinct styles required: ${styleTarget}
Maximum total buying budget: INR ${budget} (validation ceiling only; do not change the assortment or style counts because of it)
Target launch date: ${launchDate}

Rules:
- Recommend gender-specific silhouettes, never resized opposite-gender garments.
- Keep the assortment independent of budget. Prices are not known yet; budget is checked later after landed costs are entered.
- Focus the assortment: commercially strong core pieces first, controlled fashion second, at most one experimental direction.
- Use specific product/silhouette names a visual model can match from vendor photographs.
- The sum of every "styles" value MUST equal exactly ${styleTarget}.
- Do not invent sales figures, vendor prices, market shares or claim live web research.
- "role" must be one of Core, Fashion, Experiment.
- "reason" must be max 18 words and explain the commercial job of that slot.
- "colourDirection" must be 1-3 practical colour families, max 8 words.

Return STRICT JSON ONLY:
{"summary":"max 24 words","mix":[{"name":"specific silhouette","styles":2,"role":"Core","reason":"...","colourDirection":"..."}]}`;
    const ctrl = new AbortController(); const timer = setTimeout(() => ctrl.abort(), 90000);
    let r;
    try {
      r = await fetch('https://api.anthropic.com/v1/messages', { method: 'POST',
        headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
        body: JSON.stringify({ model: AI_MODEL, max_tokens: 1800, temperature: 0, messages: [{ role: 'user', content: prompt }] }), signal: ctrl.signal });
    } finally { clearTimeout(timer); }
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error((j.error && j.error.message) || ('HTTP ' + r.status));
    const parsed = extractJson((j.content || []).map(c => c.text || '').join('')) || {};
    let mix = Array.isArray(parsed.mix) ? parsed.mix.map(x => ({ name: String(x.name || '').trim().slice(0, 80),
      styles: Math.max(1, parseInt(x.styles) || 1), role: ['Core','Fashion','Experiment'].includes(x.role) ? x.role : 'Fashion',
      reason: String(x.reason || '').trim().slice(0, 180), colourDirection: String(x.colourDirection || '').trim().slice(0, 100) })).filter(x => x.name) : [];
    if (!mix.length) throw new Error('AI returned no assortment slots');
    let total = mix.reduce((n, x) => n + x.styles, 0);
    while (total > styleTarget) { const x = mix.slice().reverse().find(y => y.styles > 1); if (x) x.styles--; else mix.pop(); total = mix.reduce((n, y) => n + y.styles, 0); }
    if (total < styleTarget) mix[0].styles += styleTarget - total;
    res.json({ success: true, summary: String(parsed.summary || '').trim().slice(0, 240), mix,
      basis: { gender, category, styleTarget, budget, launchDate, liveWebData: false } });
  } catch (err) {
    if (err && err.name === 'AbortError') return res.status(504).json({ success: false, error: 'AI planning timed out. Try again.' });
    res.status(502).json({ success: false, error: 'AI planning failed: ' + (err.message || 'unknown') });
  }
});

router.get('/api/casuals/settings', (req, res) => {
  res.json({ success: true, settings: settingsWithDefaults(loadStore()) });
});

router.post('/api/casuals/settings', (req, res) => {
  const s = loadStore();
  const b = req.body || {};
  const cur = settingsWithDefaults(s);
  // Store a normalised, complete settings blob (simplest + safest).
  const next = { budgetMode: 'perCategory', categories: {} };
  CAT_KEYS.forEach(k => {
    const inc = (b.categories && b.categories[k]) || {};
    const c = cur.categories[k];
    const pickPct = (incMap, curMap) => {
      const out = {};
      const keys = new Set([...Object.keys(curMap), ...(incMap ? Object.keys(incMap) : [])]);
      keys.forEach(kk => { out[kk] = cleanPct(incMap && incMap[kk] != null ? incMap[kk] : curMap[kk], curMap[kk] || 0); });
      return out;
    };
    next.categories[k] = {
      enabled: inc.enabled != null ? !!inc.enabled : c.enabled,
      budget: inc.budget != null ? Math.max(0, parseInt(String(inc.budget).replace(/[^\d]/g, '')) || 0) : c.budget,
      avgCost: inc.avgCost != null ? Math.max(1, parseInt(inc.avgCost) || c.avgCost) : c.avgCost,
      sizeMode: inc.sizeMode != null ? ((inc.sizeMode === 'units' || inc.sizeMode === 'designs') ? inc.sizeMode : 'cost') : c.sizeMode,
      targetUnits: inc.targetUnits != null ? Math.max(0, parseInt(String(inc.targetUnits).replace(/[^\d]/g, '')) || 0) : c.targetUnits,
      designs: inc.designs != null ? Math.max(0, parseInt(String(inc.designs).replace(/[^\d]/g, '')) || 0) : c.designs,
      sizeSystem: inc.sizeSystem != null ? (inc.sizeSystem === 'numeric' ? 'numeric' : 'alpha') : c.sizeSystem,
      fits: pickPct(inc.fits, c.fits),
      sizes: pickPct(inc.sizes, c.sizes),
      colours: pickPct(inc.colours, c.colours),
      printTypes: pickPct(inc.printTypes, c.printTypes),
      onOrder: cleanIntMap(inc.onOrder != null ? inc.onOrder : c.onOrder),
      onOrderCost: cleanMoneyMap(inc.onOrderCost != null ? inc.onOrderCost : c.onOrderCost),
      extraFits: Array.isArray(inc.extraFits) ? inc.extraFits.filter(x => x && x.key) : c.extraFits,
      extraSizes: Array.isArray(inc.extraSizes) ? inc.extraSizes.filter(x => x && x.key) : c.extraSizes,
      extraColours: Array.isArray(inc.extraColours) ? inc.extraColours.filter(x => x && x.key) : c.extraColours
    };
  });
  s.settings = next;
  saveStore(s);
  res.json({ success: true, settings: settingsWithDefaults(s) });
});

router.get('/api/casuals/candidates', (req, res) => {
  const s = loadStore();
  const active = activeCands(s);
  markDuplicates(active);
  const fresh = active.filter(c => !c.ordered);   // ordered PO members aren't segregation photos
  res.json({ success: true, candidates: fresh.map(publicCandidate),
    categories: categoryCounts(active), total: fresh.length,
    batches: batchList(s), activeBatch: s.activeBatch });
});

// List batches (id, name, count) + which one is active.
router.get('/api/casuals/batches', (req, res) => {
  const s = loadStore();
  res.json({ success: true, batches: batchList(s), activeBatch: s.activeBatch });
});

// Create a new (empty) batch and make it active.
router.post('/api/casuals/batches', (req, res) => {
  const s = loadStore();
  // Accept either `categories:[...]` (multi-select) or a single `category` (legacy).
  const cat = (req.body && Array.isArray(req.body.categories)) ? req.body.categories : (req.body && req.body.category);
  const b = newBatch(s, req.body && req.body.name, cat);
  s.activeBatch = b.id;
  saveStore(s);
  res.json({ success: true, batch: b, batches: batchList(s), activeBatch: s.activeBatch });
});

// Switch the active batch (the plan/segregate/upload target).
router.post('/api/casuals/batches/active', (req, res) => {
  const s = loadStore();
  const id = String((req.body && req.body.id) || '');
  if (!s.batches.some(b => b.id === id)) return res.status(400).json({ success: false, error: 'Unknown batch' });
  s.activeBatch = id;
  saveStore(s);
  res.json({ success: true, activeBatch: s.activeBatch, batches: batchList(s) });
});

// Delete a batch and every photo in it; fall back to another active batch.
router.delete('/api/casuals/batches/:id', (req, res) => {
  const s = loadStore();
  const id = req.params.id;
  if (!s.batches.some(b => b.id === id)) return res.status(404).json({ success: false, error: 'Not found' });
  s.candidates.filter(c => c.batch === id).forEach(c => { try { fs.unlinkSync(path.join(CAND_DIR, c.file)); } catch {} });
  s.candidates = s.candidates.filter(c => c.batch !== id);
  s.batches = s.batches.filter(b => b.id !== id);
  if (s.activeBatch === id) s.activeBatch = s.batches.length ? s.batches[s.batches.length - 1].id : null;
  saveStore(s);
  res.json({ success: true, batches: batchList(s), activeBatch: s.activeBatch });
});

router.post('/api/casuals/candidates', async (req, res) => {
  const r = await runCandidateUpload(req, res);
  // Any file multer already wrote to disk must be removed if we don't go on to
  // record it — otherwise a failed/aborted upload leaks an untracked photo.
  const discardFiles = () => (req.files || []).forEach(f => { try { fs.unlinkSync(path.join(CAND_DIR, f.filename)); } catch {} });
  if (res.headersSent) { discardFiles(); return; }
  if (!r.ok) { discardFiles(); return res.status(400).json({ success: false, error: r.error }); }
  const files = req.files || [];
  if (!files.length) return res.status(400).json({ success: false, error: 'No photos received' });
  const vendor = String((req.body && req.body.vendor) || '').trim();
  const s = loadStore();
  // Decide the target batch: 'new' → create one; a known id → use it; else the
  // active batch (creating a first one if the store somehow has none).
  const reqBatch = String((req.body && req.body.batch) || '').trim();
  let target;
  if (reqBatch === 'new') target = newBatch(s, req.body && req.body.batchName);
  else if (reqBatch && s.batches.some(b => b.id === reqBatch)) target = s.batches.find(b => b.id === reqBatch);
  else target = s.batches.find(b => b.id === s.activeBatch) || newBatch(s);
  s.activeBatch = target.id;   // new photos' batch becomes the one being planned
  // DESIGN-WISE upload (optional second mode): the founder uploads one photo per
  // colourway grouped under a named design, so `meta` is a JSON array parallel to
  // `files` — meta[i] = { designId, designName, fit, colour } for files[i]. When
  // present we pre-fill the candidate's category (= the batch's category), fit and
  // colour as HUMAN overrides (fitOverride/colourOverride), so a later re-analyse
  // leaves them alone and the plan math treats them exactly like a tagged pile
  // photo. rating stays null (rating only ranks variety, never gates inclusion).
  let meta = null;
  const rawMeta = req.body && req.body.meta;
  if (rawMeta) { try { const m = JSON.parse(rawMeta); if (Array.isArray(m)) meta = m; } catch {} }
  // A batch can span several categories, so each design carries its OWN category
  // (meta[i].category). Fall back to the batch's sole category when it has just one.
  const batchCatKeys = batchCats(target);
  const soleCat = batchCatKeys.length === 1 ? batchCatKeys[0] : null;
  const added = [];
  for (let i = 0; i < files.length; i++) {
    const f = files[i];
    const fp = path.join(CAND_DIR, f.filename);
    const sig = await computeSignature(fp);
    const c = { id: crypto.randomBytes(8).toString('hex'), file: f.filename, vendor, batch: target.id,
      category: null, fit: null, colour: null, pattern: null,
      uploadedAt: new Date().toISOString(), sha: fileSha(fp), phash: sig ? sig.phash : null, avg: sig ? sig.avg : null, dupeOf: null };
    const md = meta && meta[i];
    // Category for THIS design: prefer the design's own meta.category (must be one
    // the batch spans), else the batch's single category. Skip prefill if neither.
    let mdCat = md && String(md.category || '').trim();
    if (mdCat && !(CAT_BY_KEY[mdCat] && (!batchCatKeys.length || batchCatKeys.indexOf(mdCat) >= 0))) mdCat = null;
    const useCat = mdCat || soleCat;
    const spec = useCat ? CAT_BY_KEY[useCat] : null;
    if (md) {
      // Design identity does not depend on category being known. Photo Splitter
      // crops enter as an unclassified pile, but colourways from one source
      // collage must remain grouped as the same design after segregation.
      if (md.designId) c.designId = String(md.designId);
      if (md.designName) c.designName = String(md.designName).trim();
    }
    if (md && spec) {
      c.category = useCat;
      const fk = String(md.fit || '').trim();
      if (fk && spec.fits.some(x => x.key === fk)) { c.fit = fk; c.fitOverride = true; }
      const col = String(md.colour || '').trim();
      if (col) { c.colour = col; c.colourOverride = true; }
    }
    s.candidates.push(c);
    added.push(publicCandidate(c));
  }
  const active = activeCands(s);
  const dupes = markDuplicates(active);
  saveStore(s);
  res.json({ success: true, added, total: active.length, dupes,
    categories: categoryCounts(active), batches: batchList(s), activeBatch: s.activeBatch, batch: target });
});

router.get('/api/casuals/candidate/:id', (req, res) => {
  const s = loadStore();
  const c = s.candidates.find(x => x.id === req.params.id);
  if (!c) return res.status(404).end();
  // PO-imported lines carry no local file — just the supplier photo's URL
  // (photoRef). Redirect to it so the card shows the image with zero disk copy.
  if (!c.file && c.photoRef) return res.redirect(302, c.photoRef);
  if (!c.file) return res.status(404).end();
  const fp = path.join(CAND_DIR, c.file);
  if (!fs.existsSync(fp)) return res.status(404).end();
  res.sendFile(fp);
});

router.delete('/api/casuals/candidate/:id', (req, res) => {
  const s = loadStore();
  const i = s.candidates.findIndex(x => x.id === req.params.id);
  if (i < 0) return res.status(404).json({ success: false, error: 'Not found' });
  const [c] = s.candidates.splice(i, 1);
  try { fs.unlinkSync(path.join(CAND_DIR, c.file)); } catch {}
  const active = activeCands(s);
  markDuplicates(active);
  saveStore(s);
  res.json({ success: true, total: active.length, categories: categoryCounts(active),
    batches: batchList(s), activeBatch: s.activeBatch });
});

// Manual include / exclude override for one design. body.include:
//   true  → force into the PO, false → force out, null → back to AI's rating gate.
router.post('/api/casuals/candidate/:id/include', (req, res) => {
  const s = loadStore();
  const c = s.candidates.find(x => x.id === req.params.id);
  if (!c) return res.status(404).json({ success: false, error: 'Not found' });
  const v = req.body ? req.body.include : undefined;
  if (v === true) c.includeOverride = true;
  else if (v === false) c.includeOverride = false;
  else delete c.includeOverride; // null/undefined → clear, fall back to rating
  saveStore(s);
  const settings = settingsWithDefaults(s);
  const active = activeCands(s);
  markDuplicates(active);
  res.json({ success: true, analysed: active.some(x => x.category), settings,
    categories: categoryCounts(active), batches: batchList(s), activeBatch: s.activeBatch,
    ...buildPlan(active, settings) });
});

// Manual FIT / COLOUR correction for one design — the AI vision sometimes tags a
// wide-leg as relaxed, or reads a grey as black, which skews the fit split and the
// colour mix. body.fit (a valid fit key for the design's category, or '' to revert
// to the AI's read) and/or body.colour (a preset colour key, or '' to revert). A
// corrected value is flagged (fitOverride / colourOverride) so a future re-analyse
// won't clobber it.
router.post('/api/casuals/candidate/:id/tag', (req, res) => {
  const s = loadStore();
  const c = s.candidates.find(x => x.id === req.params.id);
  if (!c) return res.status(404).json({ success: false, error: 'Not found' });
  const b = req.body || {};
  const spec = CAT_BY_KEY[c.category];
  if (typeof b.fit === 'string') {
    if (!b.fit) { c.fit = spec ? normFit(c.category, c.aiFit) : null; delete c.fitOverride; }   // back to AI
    else if (spec && spec.fits.some(f => f.key === b.fit)) { c.fit = b.fit; c.fitOverride = true; }
  }
  if (typeof b.colour === 'string') {
    if (!b.colour) { delete c.colourOverride; }   // revert flag; AI colour re-applies on next analyse
    else { c.colour = b.colour.trim(); c.colourOverride = true; }
  }
  if (typeof b.print === 'string' && catHasPrintTypes(c.category)) {
    if (!b.print) { delete c.printType; delete c.printOverride; }   // back to the AI's pattern read
    else if (spec.printTypes.some(p => p.key === b.print)) { c.printType = b.print; c.printOverride = true; }
  }
  saveStore(s);
  const settings = settingsWithDefaults(s);
  const active = activeCands(s);
  markDuplicates(active);
  res.json({ success: true, settings, categories: categoryCounts(active),
    batches: batchList(s), activeBatch: s.activeBatch, ...buildPlan(active, settings) });
});

// Clear only the ACTIVE batch's photos (the batch itself stays, now empty).
router.post('/api/casuals/candidates/clear', (req, res) => {
  const s = loadStore();
  s.candidates.filter(c => c.batch === s.activeBatch).forEach(c => { try { fs.unlinkSync(path.join(CAND_DIR, c.file)); } catch {} });
  s.candidates = s.candidates.filter(c => c.batch !== s.activeBatch);
  saveStore(s);
  res.json({ success: true, total: 0, categories: categoryCounts([]),
    batches: batchList(s), activeBatch: s.activeBatch });
});

// Reclaim disk: delete any photo file on the volume not referenced by a live
// candidate (orphans from aborted uploads). Safe to run any time.
router.post('/api/casuals/candidates/sweep', (req, res) => {
  const { removed, freed } = sweepOrphanCandidates();
  res.json({ success: true, removed, freedKB: Math.round(freed / 1024) });
});

// Disk diagnostic: report the size of every top-level entry on the /data volume
// so we can see exactly what's consuming space before resizing. Read-only.
router.get('/api/casuals/disk', (req, res) => {
  const dirSize = p => { let t = 0; try { for (const e of fs.readdirSync(p, { withFileTypes: true })) { const fp = path.join(p, e.name); try { t += e.isDirectory() ? dirSize(fp) : fs.statSync(fp).size; } catch {} } } catch {} return t; };
  const out = [];
  try {
    for (const e of fs.readdirSync(DATA_DIR, { withFileTypes: true })) {
      const fp = path.join(DATA_DIR, e.name);
      let bytes = 0; try { bytes = e.isDirectory() ? dirSize(fp) : fs.statSync(fp).size; } catch {}
      out.push({ name: e.name + (e.isDirectory() ? '/' : ''), MB: +(bytes / 1048576).toFixed(2) });
    }
  } catch (err) { return res.status(500).json({ success: false, error: String(err) }); }
  out.sort((a, b) => b.MB - a.MB);
  res.json({ success: true, dataDir: DATA_DIR, totalMB: +(out.reduce((s, x) => s + x.MB, 0)).toFixed(2), entries: out });
});

router.post('/api/casuals/analyze', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI segregation is not enabled. Set ANTHROPIC_API_KEY in Railway to turn it on.' });
    const s = loadStore();
    const cands = activeCands(s);
    if (!cands.length) return res.status(400).json({ success: false, error: 'No photos in this batch yet — upload some casual-wear photos first.' });
    const force = req.body && (req.body.force === true || req.body.force === 'true');
    const dupes = markDuplicates(cands);
    // Re-score anything not yet categorised OR still missing an AI rating — so
    // batches segregated before the rating feature existed get rated on next run.
    const todo = cands.filter(c => !c.dupeOf && !c.ordered && (force || !c.category || c.rating == null));
    let classified = 0;
    for (let i = 0; i < todo.length; i += VISION_BATCH) {
      const slice = todo.slice(i, i + VISION_BATCH);
      const items = [];
      slice.forEach(c => { try { items.push({ id: c.id, buffer: fs.readFileSync(path.join(CAND_DIR, c.file)), mediaType: mediaTypeForFile(c.file) }); } catch {} });
      if (!items.length) continue;
      const map = await scoreBatch(items);
      slice.forEach(c => {
        const r = map[c.id];
        if (r) {
          c.category = r.category;
          if (!c.fitOverride) { c.fit = r.fit; c.aiFit = r.aiFit; }   // keep a manual fit correction
          if (!c.colourOverride) c.colour = r.colour;                 // keep a manual colour correction
          c.pattern = r.pattern; c.rating = r.rating; c.ratingReason = r.reason; classified++;
        }
      });
      saveStore(s);
    }
    const settings = settingsWithDefaults(s);
    const active = activeCands(s);
    res.json({ success: true, classified, dupes, settings, categories: categoryCounts(active),
      batches: batchList(s), activeBatch: s.activeBatch, ...buildPlan(active, settings) });
  } catch (err) {
    if (err && err.name === 'AbortError') return res.status(504).json({ success: false, error: 'Segregation timed out. Try again — sorted photos are saved.' });
    res.status(502).json({ success: false, error: 'Segregation failed: ' + (err.message || 'unknown') });
  }
});

// Match the current batch to the named assortment approved in the simplified
// workflow. Returns both winners and rejected alternatives with reasons, so the
// recommendation is inspectable rather than a silent first-N shortlist.
router.post('/api/casuals/simple-recommend', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI selection is not enabled. Set ANTHROPIC_API_KEY in Railway.' });
    const plan = req.body && req.body.plan;
    if (!plan || !Array.isArray(plan.mix) || !plan.mix.length) return res.status(400).json({ success: false, error: 'The approved assortment plan is missing.' });
    const s = loadStore();
    const wantedCat = String(plan.category || '');
    const all = activeCands(s).filter(c => !c.dupeOf && !c.ordered && (!wantedCat || c.category === wantedCat));
    if (!all.length) return res.status(400).json({ success: false, error: 'No non-duplicate products match this category.' });

    // Rank the strongest broad candidates first, but keep a generous ceiling so
    // specific approved silhouettes are not crowded out by generic high ratings.
    const ceiling = 80;
    const pool = all.slice().sort((a, b) => (b.rating || 0) - (a.rating || 0)).slice(0, ceiling);
    const scored = [];
    for (let i = 0; i < pool.length; i += VISION_BATCH) {
      const slice = pool.slice(i, i + VISION_BATCH);
      const items = [];
      slice.forEach(c => { try { items.push({ id: c.id, buffer: fs.readFileSync(path.join(CAND_DIR, c.file)), mediaType: mediaTypeForFile(c.file) }); } catch {} });
      if (items.length) scored.push(...await scoreSimpleBatch(items, plan));
    }
    const slotMap = {};
    plan.mix.forEach(x => { slotMap[String(x.name || '').trim().toLowerCase()] = { name: String(x.name || '').trim(), need: Math.max(1, parseInt(x.styles) || 1) }; });
    const byId = new Map(pool.map(c => [c.id, c]));
    const used = new Set(); const selected = [];
    Object.values(slotMap).forEach(slot => {
      scored.filter(x => String(x.slot || '').trim().toLowerCase() === slot.name.toLowerCase() && x.score >= 65 && !used.has(x.id))
        .sort((a, b) => b.score - a.score).slice(0, slot.need).forEach(x => { used.add(x.id); selected.push({ ...publicCandidate(byId.get(x.id)), ...x, slot: slot.name }); });
    });
    const excluded = scored.filter(x => !used.has(x.id)).sort((a, b) => b.score - a.score).map(x => {
      const c = byId.get(x.id); const recognised = slotMap[String(x.slot || '').trim().toLowerCase()];
      let why = x.excludeReason;
      if (!why && recognised) why = 'Lower score than selected alternative';
      if (!why) why = x.slot === 'No match' ? 'Outside approved assortment' : 'Does not fill an open slot';
      return { ...publicCandidate(c), ...x, excludeReason: why };
    });
    const missing = Object.values(slotMap).map(slot => ({ slot: slot.name, required: slot.need,
      selected: selected.filter(x => x.slot === slot.name).length })).filter(x => x.selected < x.required);
    res.json({ success: true, evaluated: scored.length, eligibleBeforeLimit: all.length, ceiling,
      selected, excluded, missing, basis: { threshold: 65, dimensions: { slotMatch: 40, brandFit: 20, marketRelevance: 15, versatility: 15, visibleFinish: 10 } } });
  } catch (err) {
    if (err && err.name === 'AbortError') return res.status(504).json({ success: false, error: 'AI selection timed out. Try again.' });
    res.status(502).json({ success: false, error: 'AI selection failed: ' + (err.message || 'unknown') });
  }
});

// Read an uploaded supplier invoice (photo / PDF / Excel) and return garment
// line items for the founder to review. Nothing is saved here — apply is a
// separate, confirmed step.
router.post('/api/casuals/invoice/parse', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI reading is not enabled. Set ANTHROPIC_API_KEY in Railway to turn it on.' });
    const r = await runInvoiceUpload(req, res);
    if (res.headersSent) return;
    if (!r.ok) return res.status(400).json({ success: false, error: r.error });
    const files = req.files || [];
    if (!files.length) return res.status(400).json({ success: false, error: 'No invoice files received' });
    const content = [];
    files.forEach((f, i) => {
      const name = f.originalname || ('file' + (i + 1));
      const isPdf = f.mimetype === 'application/pdf' || /\.pdf$/i.test(name);
      const isImg = /^image\//.test(f.mimetype) || /\.(png|jpe?g|webp|gif)$/i.test(name);
      const isXls = /sheet|excel|csv|officedocument|ms-excel/i.test(f.mimetype) || /\.(xlsx|xls|csv)$/i.test(name);
      content.push({ type: 'text', text: 'FILE ' + (i + 1) + ' (' + name + '):' });
      if (isImg) {
        content.push({ type: 'image', source: { type: 'base64', media_type: /^image\//.test(f.mimetype) ? f.mimetype : 'image/jpeg', data: f.buffer.toString('base64') } });
      } else if (isPdf) {
        content.push({ type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: f.buffer.toString('base64') } });
      } else if (isXls) {
        const txt = excelToText(f.buffer);
        content.push({ type: 'text', text: txt || '(could not read spreadsheet)' });
      } else {
        content.push({ type: 'text', text: f.buffer.toString('utf8').slice(0, 40000) });
      }
    });
    const items = await scoreInvoice(content);
    // Fit options per category (spec + any custom fits) for the review dropdowns.
    const merged = settingsWithDefaults(loadStore());
    const fits = {};
    CAT_KEYS.forEach(k => {
      const cfg = merged.categories[k];
      const labelOf = key => { const sp = CAT_BY_KEY[k].fits.find(f => f.key === key); if (sp) return sp.label; const ex = (cfg.extraFits || []).find(f => f.key === key); return ex ? ex.label : key; };
      fits[k] = Object.keys(cfg.fits).map(fk => ({ key: fk, label: labelOf(fk) }));
    });
    // Print-type options per category (only uppers have them) so the review
    // table can show a print dropdown alongside the fit dropdown.
    const prints = {};
    CAT_KEYS.forEach(k => { prints[k] = catHasPrintTypes(k) ? PRINT_TYPES.map(p => ({ key: p.key, label: p.label })) : []; });
    res.json({ success: true, items, fits, prints, cats: CAT_KEYS.map(k => ({ key: k, label: CAT_BY_KEY[k].label })) });
  } catch (err) {
    if (err && err.name === 'AbortError') return res.status(504).json({ success: false, error: 'Reading the invoice timed out — try fewer/smaller files.' });
    res.status(502).json({ success: false, error: 'Could not read the invoice: ' + (err.message || 'unknown') });
  }
});

// Dropdown metadata (category / fit / print options) WITHOUT reading a bill.
// Used when the already-ordered lines come from an existing procurement PO rather
// than an uploaded invoice — the front-end still needs the same fits/prints/cats
// to render the review table, but there's no AI parse step. Mirrors exactly the
// {fits, prints, cats} the parse endpoint returns.
router.get('/api/casuals/invoice/meta', (req, res) => {
  const merged = settingsWithDefaults(loadStore());
  const fits = {};
  CAT_KEYS.forEach(k => {
    const cfg = merged.categories[k];
    const labelOf = key => { const sp = CAT_BY_KEY[k].fits.find(f => f.key === key); if (sp) return sp.label; const ex = (cfg.extraFits || []).find(f => f.key === key); return ex ? ex.label : key; };
    fits[k] = Object.keys(cfg.fits).map(fk => ({ key: fk, label: labelOf(fk) }));
  });
  const prints = {};
  CAT_KEYS.forEach(k => { prints[k] = catHasPrintTypes(k) ? PRINT_TYPES.map(p => ({ key: p.key, label: p.label })) : []; });
  res.json({ success: true, fits, prints, cats: CAT_KEYS.map(k => ({ key: k, label: CAT_BY_KEY[k].label })) });
});

// Classify one product photo PER invoice line (buyer-uploaded, since bills rarely
// carry images). Reuses the Fresh-Procurement vision brain so a line gets its
// category / fit / print / colour auto-filled just like a design photo. Field
// name 'images', parallel `ids[]` line the results back up to the review rows.
function runInvoiceClassifyUpload(req, res) {
  return new Promise((resolve) => {
    invoiceUpload.array('images', 20)(req, res, (err) => {
      if (err) {
        const map = { LIMIT_FILE_SIZE: 'An image is larger than 25 MB — compress it and retry.',
          LIMIT_FILE_COUNT: 'Too many images at once (max 20).', LIMIT_UNEXPECTED_FILE: 'Unexpected upload field.' };
        resolve({ ok: false, error: map[err.code] || ('Upload error: ' + (err.message || err.code || 'unknown')) });
      } else resolve({ ok: true });
    });
  });
}
router.post('/api/casuals/invoice/classify', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI reading is not enabled. Set ANTHROPIC_API_KEY in Railway to turn it on.' });
    const r = await runInvoiceClassifyUpload(req, res);
    if (res.headersSent) return;
    if (!r.ok) return res.status(400).json({ success: false, error: r.error });
    const files = (req.files || []).filter(f => /^image\//.test(f.mimetype) || /\.(png|jpe?g|webp|gif)$/i.test(f.originalname || ''));
    if (!files.length) return res.status(400).json({ success: false, error: 'No product images received' });
    // Line ids run parallel to the uploaded images (multer keeps field order).
    let ids = req.body && req.body.ids;
    if (typeof ids === 'string') ids = [ids];
    ids = Array.isArray(ids) ? ids : [];
    const results = {};
    for (let i = 0; i < files.length; i += VISION_BATCH) {
      const slice = files.slice(i, i + VISION_BATCH);
      const items = slice.map((f, k) => ({ id: ids[i + k] || ('img' + (i + k)), buffer: f.buffer, mediaType: mediaTypeForFile(f.originalname || '') }));
      const map = await scoreBatch(items);
      items.forEach(it => {
        const o = map[it.id]; if (!o) return;
        const cat = o.category;
        results[it.id] = {
          category: cat,
          fit: cat ? o.fit : null,
          colour: o.colour || '',
          printType: (cat && catHasPrintTypes(cat)) ? (printBucket(o.pattern) || null) : null
        };
      });
    }
    res.json({ success: true, results });
  } catch (err) {
    if (err && err.name === 'AbortError') return res.status(504).json({ success: false, error: 'Reading the images timed out — try fewer at once.' });
    res.status(502).json({ success: false, error: 'Could not analyze the images: ' + (err.message || 'unknown') });
  }
});

// Apply the reviewed invoice lines. Each line becomes a REAL "already-ordered"
// member of the active batch: its product photo + colour + size run + qty + unit ₹
// are saved as a candidate flagged `ordered`. buildPlan then slots it into its
// own category → (print) → fit box, nets that cell's target, and renders it as a
// read-only card — so the founder SEES what's on the way, and re-uploading a bill
// can't silently double a running total (the members are visible & deletable).
// Multipart: `meta` = JSON array of lines; `images` files + parallel `ids[]` map
// a product photo to a line by its review-row id.
function runOrderedUpload(req, res) {
  return new Promise((resolve) => {
    invoiceUpload.array('images', 20)(req, res, (err) => {
      if (err) {
        const map = { LIMIT_FILE_SIZE: 'A product photo is larger than 25 MB — compress it and retry.',
          LIMIT_FILE_COUNT: 'Too many photos at once (max 20).', LIMIT_UNEXPECTED_FILE: 'Unexpected upload field.' };
        resolve({ ok: false, error: map[err.code] || ('Upload error: ' + (err.message || err.code || 'unknown')) });
      } else resolve({ ok: true });
    });
  });
}
router.post('/api/casuals/invoice/apply', async (req, res) => {
  const r = await runOrderedUpload(req, res);
  if (res.headersSent) return;
  if (!r.ok) return res.status(400).json({ success: false, error: r.error });
  const s = loadStore();
  const merged = settingsWithDefaults(s);
  // The PO lands in whatever batch is being planned right now (creating one if the
  // store somehow has none) — "upload a PO into THIS batch".
  let target = s.batches.find(b => b.id === s.activeBatch) || newBatch(s);
  s.activeBatch = target.id;
  let lines = [];
  try { lines = JSON.parse((req.body && req.body.meta) || '[]'); } catch { lines = []; }
  if (!Array.isArray(lines)) lines = [];
  // When the bill came from an existing procurement PO (not a fresh upload), stamp
  // every candidate with that PO id so the whole import can be undone in one go.
  const sourcePo = String((req.body && req.body.sourcePo) || '').trim() || null;
  // Photos run parallel to ids[] (multer preserves field order).
  let ids = req.body && req.body.ids; if (typeof ids === 'string') ids = [ids];
  ids = Array.isArray(ids) ? ids : [];
  const files = (req.files || []);
  const photoByLine = {}; files.forEach((f, i) => { const id = ids[i]; if (id) photoByLine[id] = f; });

  const SIZE_KEYS = ['XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL', '3XL', '4XL', '5XL', '28', '30', '32', '34', '36', '38', '40', '42'];
  const normSize = tok => { const t = String(tok || '').toUpperCase().replace(/\s+/g, ''); const hit = SIZE_KEYS.find(k => k === t); return hit || (t || '—'); };
  let applied = 0, skipped = 0;
  lines.forEach(it => {
    const cat = CAT_KEYS.includes(it && it.category) ? it.category : null;
    const cfg = cat ? merged.categories[cat] : null;
    const fit = (cfg && it.fit && (it.fit in cfg.fits)) ? it.fit : null;
    const hasP = cat ? catHasPrintTypes(cat) : false;
    const pt = (hasP && cfg && it.printType && (it.printType in cfg.printTypes)) ? it.printType : null;
    let qty = parseInt(it && it.qty, 10); if (!isFinite(qty) || qty <= 0) qty = 0;
    let price = Number(it && it.unitPrice); if (!isFinite(price) || price < 0) price = 0;
    if (!cat || !fit || !qty || (hasP && !pt)) { skipped++; return; }
    // Size run: prefer an explicit per-size object; else fold the whole qty under the
    // line's single size token (best-effort — bills often list one size per row).
    const orderedSizes = {};
    if (it && it.sizes && typeof it.sizes === 'object') {
      Object.keys(it.sizes).forEach(k => { const v = parseInt(it.sizes[k], 10); if (isFinite(v) && v > 0) orderedSizes[normSize(k)] = (orderedSizes[normSize(k)] || 0) + v; });
    }
    if (!Object.keys(orderedSizes).length) orderedSizes[normSize(it && it.size)] = qty;
    // Product photo (optional) → persist to the candidate pool so the card shows it.
    const pf = photoByLine[it && it._id];
    let file = null;
    if (pf && pf.buffer && (/^image\//.test(pf.mimetype) || /\.(png|jpe?g|webp|gif)$/i.test(pf.originalname || ''))) {
      const ext = ((path.extname(pf.originalname || '') || '.jpg').toLowerCase().replace(/[^.a-z0-9]/g, '')) || '.jpg';
      const fn = Date.now() + '-' + crypto.randomBytes(6).toString('hex') + ext;
      try { fs.writeFileSync(path.join(CAND_DIR, fn), pf.buffer); file = fn; } catch { file = null; }
    }
    // When importing from a procurement PO with no freshly-uploaded photo, reuse
    // the PO's own supplier image by URL (photoRef) — no re-download/copy, so it
    // costs no disk. The card falls back to photoRef when there's no local file.
    const photoRef = (!file && it && it.photoRef) ? String(it.photoRef).trim() : null;
    s.candidates.push({
      id: crypto.randomBytes(8).toString('hex'), file, photoRef: photoRef || null, vendor: String((it && it.vendor) || '').trim(),
      batch: target.id, category: cat, fit, colour: (it && it.colour) ? String(it.colour).trim() : null,
      pattern: null, printType: pt || null, printOverride: !!pt,
      ordered: true, orderedQty: qty, orderedSizes, orderedCost: Math.round(price),
      sourcePo: sourcePo || null,          // which procurement PO this line was imported from (null = uploaded bill)
      uploadedAt: new Date().toISOString(), sha: null, phash: null, avg: null, dupeOf: null
    });
    applied++;
  });
  saveStore(s);
  const settings = settingsWithDefaults(s);
  const active = activeCands(s);
  markDuplicates(active);
  res.json({ success: true, applied, skipped, analysed: active.some(c => c.category),
    settings, categories: categoryCounts(active), batches: batchList(s), activeBatch: s.activeBatch,
    ...buildPlan(active, settings) });
});

// Which procurement POs have been imported into the ACTIVE batch (so the picker can
// show "already imported" and offer an undo). Rolls each PO's imported lines up to
// a count + total pieces.
router.get('/api/casuals/imported-pos', (req, res) => {
  const s = loadStore();
  const roll = {};
  activeCands(s).forEach(c => {
    if (!c.sourcePo) return;
    const r = roll[c.sourcePo] || (roll[c.sourcePo] = { poId: c.sourcePo, lines: 0, pieces: 0 });
    r.lines += 1; r.pieces += (parseInt(c.orderedQty, 10) || 0);
  });
  res.json({ success: true, pos: Object.values(roll) });
});

// Undo a whole imported PO — delete every candidate in the active batch that came
// from that PO (removing its photo files), then hand back a fresh plan so the boxes
// re-net. Reverses one "Import an existing PO" in a single action.
router.delete('/api/casuals/imported-po/:poId', (req, res) => {
  const s = loadStore();
  const poId = String(req.params.poId || '').trim();
  if (!poId) return res.status(400).json({ success: false, error: 'No PO id' });
  const doomed = s.candidates.filter(c => c.batch === s.activeBatch && c.sourcePo === poId);
  if (!doomed.length) return res.status(404).json({ success: false, error: 'No imported lines found for ' + poId });
  doomed.forEach(c => { if (c.file) { try { fs.unlinkSync(path.join(CAND_DIR, c.file)); } catch {} } });
  const doomedIds = new Set(doomed.map(c => c.id));
  s.candidates = s.candidates.filter(c => !doomedIds.has(c.id));
  saveStore(s);
  const settings = settingsWithDefaults(s);
  const active = activeCands(s);
  markDuplicates(active);
  res.json({ success: true, removed: doomed.length, poId, analysed: active.some(c => c.category),
    settings, categories: categoryCounts(active), batches: batchList(s), activeBatch: s.activeBatch,
    ...buildPlan(active, settings) });
});

router.get('/api/casuals/plan', (req, res) => {
  const s = loadStore();
  const settings = settingsWithDefaults(s);
  const active = activeCands(s);
  markDuplicates(active);
  const analysed = active.some(c => c.category);
  const batchObj = s.batches.find(b => b.id === s.activeBatch);
  res.json({ success: true, analysed, settings, categories: categoryCounts(active),
    batches: batchList(s), activeBatch: s.activeBatch,
    designTags: (batchObj && batchObj.designTags) || {},
    ...buildPlan(active, settings) });
});

// Founder tags WHICH uploaded photo represents each base design slot in the
// common-design order sheet. Saved per batch (survives reload). Key format =
// "<fitKey>|<slotLetter>"; value = candidate id (or '' to clear).
router.post('/api/casuals/design-tags', (req, res) => {
  const s = loadStore();
  const b = req.body || {};
  const batchObj = s.batches.find(x => x.id === s.activeBatch);
  if (!batchObj) return res.status(400).json({ success: false, error: 'No active batch.' });
  const key = String(b.key || '').trim();
  if (!key) return res.status(400).json({ success: false, error: 'Missing slot key.' });
  batchObj.designTags = batchObj.designTags || {};
  const cid = String(b.candidateId || '').trim();
  if (cid) batchObj.designTags[key] = cid;
  else delete batchObj.designTags[key];
  saveStore(s);
  res.json({ success: true, designTags: batchObj.designTags });
});

module.exports = { router, CASUALS_SPEC, buildPlan, settingsWithDefaults, splitInts, validSplitBoxes, splitDetectionNeedsDetail, splitBoxesNeedFocusCards };
