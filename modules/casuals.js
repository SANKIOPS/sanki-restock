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
// Budget entry supports every option — one total split across the enabled
// categories, or a per-category rupee ceiling for all / any one / any two.
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

const router = express.Router();

// ── The Casuals scaffold (defaults from the SANKI Casuals workbook) ──
// Percentages are WEIGHTS — they need not sum to exactly 100; the planner
// normalises them. Every number here is an editable default; saved settings
// override it per category.
const CASUALS_SPEC = [
  {
    key: 'Trouser', label: 'Trousers',
    designNote: 'Mostly solids; texture / checks as accents',
    fits: [
      { key: 'wideleg',        label: 'Wide-leg / Controlled Baggy', pct: 30, kw: ['wide', 'baggy', 'wide leg', 'wide-leg', 'controlled baggy', 'loose'] },
      { key: 'relaxedstraight', label: 'Relaxed Straight',           pct: 30, kw: ['relaxed straight', 'straight', 'regular fit', 'relaxed'] },
      { key: 'koreanpleated',  label: 'Korean Pleated',              pct: 20, kw: ['pleat', 'pleated', 'korean', 'korean pleated'] },
      { key: 'flared',         label: 'Flared / Bootcut',            pct: 10, kw: ['flare', 'flared', 'bootcut', 'boot cut', 'boot-cut'] },
      { key: 'tapered',        label: 'Tapered / Chino',             pct: 10, kw: ['taper', 'tapered', 'chino', 'carrot', 'slim'] }
    ],
    sizes: [
      { key: '26', pct: 10 }, { key: '28', pct: 20 }, { key: '30', pct: 20 },
      { key: '32', pct: 20 }, { key: '34', pct: 20 }, { key: '36', pct: 10 }
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
      { key: 'oversized',      label: 'Oversized / Drop Shoulder', pct: 15, kw: ['oversize', 'oversized', 'drop shoulder', 'drop-shoulder'] },
      { key: 'relaxedregular', label: 'Relaxed Regular',           pct: 50, kw: ['relaxed regular', 'relaxed', 'regular', 'classic'] },
      { key: 'boxy',           label: 'Boxy',                      pct: 25, kw: ['boxy', 'box fit', 'box'] },
      { key: 'cuban',          label: 'Cuban / Resort Collar',     pct: 10, kw: ['cuban', 'resort', 'camp collar', 'revere', 'bowling'] }
    ],
    sizes: [
      { key: 'XS', pct: 6 }, { key: 'S', pct: 28 }, { key: 'M', pct: 28 },
      { key: 'L', pct: 18 }, { key: 'XL', pct: 14 }, { key: 'XXL', pct: 6 }
    ],
    colours: [
      { key: 'Black', pct: 20 }, { key: 'Off White', pct: 20 }, { key: 'White', pct: 5 },
      { key: 'Charcoal Grey', pct: 10 }, { key: 'Olive Green', pct: 15 }, { key: 'Beige', pct: 15 }, { key: 'Sky Blue', pct: 10 }
    ],
    avgCost: 650
  },
  {
    key: 'T-shirt', label: 'T-Shirts & Polos',
    designNote: 'Solid 60–70%, texture / stripes / checks 25–30%',
    fits: [
      { key: 'oversizedround', label: 'Oversized Round Neck', pct: 35, kw: ['oversize', 'oversized', 'drop shoulder', 'oversized round'] },
      { key: 'relaxedround',   label: 'Relaxed Round Neck',   pct: 35, kw: ['relaxed round', 'relaxed', 'regular round'] },
      { key: 'boxyround',      label: 'Boxy Round Neck',      pct: 15, kw: ['boxy', 'box fit', 'box'] },
      { key: 'classictee',     label: 'Classic Regular Tee',  pct: 15, kw: ['classic', 'regular tee', 'regular', 'polo', 'slim'] }
    ],
    sizes: [
      { key: 'XS', pct: 10 }, { key: 'S', pct: 20 }, { key: 'M', pct: 20 },
      { key: 'L', pct: 20 }, { key: 'XL', pct: 20 }, { key: 'XXL', pct: 10 }
    ],
    colours: [
      { key: 'Black', pct: 25 }, { key: 'Off White', pct: 20 }, { key: 'White', pct: 15 },
      { key: 'Charcoal Grey', pct: 15 }, { key: 'Olive Green', pct: 15 }, { key: 'Beige', pct: 5 }, { key: 'Navy Blue', pct: 5 }
    ],
    avgCost: 400
  }
];
const CAT_KEYS  = CASUALS_SPEC.map(c => c.key);
const CAT_BY_KEY = CASUALS_SPEC.reduce((m, c) => (m[c.key] = c, m), {});

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
    return s;
  } catch { return { settings: {}, candidates: [] }; }
}
function saveStore(s) { atomicWrite(STORE_PATH, JSON.stringify(s)); }

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
const DEFAULT_TOTAL_BUDGET = 1000000;
function pctMapFromList(list) { const m = {}; list.forEach(x => { m[x.key] = x.pct; }); return m; }
function cleanPct(v, fallback) { const n = Number(v); return isFinite(n) && n >= 0 ? n : fallback; }

function settingsWithDefaults(s) {
  const saved = (s && s.settings) || {};
  const savedCats = saved.categories || {};
  const categories = {};
  CASUALS_SPEC.forEach(spec => {
    const sc = savedCats[spec.key] || {};
    const fitDef = pctMapFromList(spec.fits), sizeDef = pctMapFromList(spec.sizes), colDef = pctMapFromList(spec.colours);
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
      fits:    mergePct(fitDef, sc.fits),
      sizes:   mergePct(sizeDef, sc.sizes),
      colours: mergePct(colDef, sc.colours),
      // Extra fit / size / colour ROWS the founder added (label + key), beyond the spec.
      extraFits:    Array.isArray(sc.extraFits) ? sc.extraFits.filter(x => x && x.key) : [],
      extraSizes:   Array.isArray(sc.extraSizes) ? sc.extraSizes.filter(x => x && x.key) : [],
      extraColours: Array.isArray(sc.extraColours) ? sc.extraColours.filter(x => x && x.key) : []
    };
  });
  return {
    budgetMode: saved.budgetMode === 'perCategory' ? 'perCategory' : 'total',
    budget: saved.budget != null ? Math.max(0, parseInt(saved.budget) || 0) : DEFAULT_TOTAL_BUDGET,
    categories
  };
}

// Public shape of a candidate (never leak the disk path).
function publicCandidate(c) {
  return { id: c.id, vendor: c.vendor || '', url: '/api/casuals/candidate/' + c.id,
    category: c.category || null, fit: c.fit || null, colour: c.colour || null,
    pattern: c.pattern || null, aiFit: c.aiFit || null, dupeOf: c.dupeOf || null,
    uploadedAt: c.uploadedAt };
}
function categoryCounts(cands) {
  const active = cands.filter(c => !c.dupeOf);
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
async function scoreBatch(items) {
  // items: [{id, buffer, mediaType}] → map id→{category, fit, colour, pattern}
  const content = [];
  items.forEach((it, i) => {
    content.push({ type: 'text', text: 'IMAGE ' + (i + 1) + ':' });
    content.push({ type: 'image', source: { type: 'base64', media_type: it.mediaType, data: it.buffer.toString('base64') } });
  });
  const fitGuide = CASUALS_SPEC.map(c => '   • ' + c.label + ' fits: ' + c.fits.map(f => f.label).join(' | ')).join('\n');
  content.push({ type: 'text', text:
`You are a buyer for an Indian premium-casual clothing brand. For EACH image, identify a plain everyday garment and report four fields. Judge each image on its own.\n\n` +
`1) "category" — EXACTLY one of: Trousers, Shirts, T-Shirts & Polos. (A collared button-up = Shirts; a pullover round-neck knit/jersey top or polo = T-Shirts & Polos; any lower-body pant/chino/trouser = Trousers.) If it is truly none of these, use "Other".\n` +
`2) "fit" — the cut, chosen from that category's list:\n${fitGuide}\n   Pick the closest single fit label.\n` +
`3) "colour" — the dominant colour word (e.g. Black, Off White, Beige, Olive, Charcoal Grey, Navy Blue, Sky Blue, Brown).\n` +
`4) "pattern" — one of: "solid", "texture", "stripes", "checks", "print". Use "solid" for a plain single-colour garment.\n\n` +
`Return STRICT JSON ONLY: {"items":[{"category":"..","fit":"..","colour":"..","pattern":".."}, ...]} with exactly ${items.length} objects, one per image in order.` });
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 90000);
  let r;
  try {
    r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: AI_MODEL, max_tokens: 2000, temperature: 0, messages: [{ role: 'user', content }] }),
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
    out[it.id] = {
      category: cat,
      aiFit: String(o.fit || '').trim() || null,
      fit: cat ? normFit(cat, o.fit) : null,
      colour: String(o.colour || '').trim() || null,
      pattern: String(o.pattern || '').trim().toLowerCase() || null
    };
  });
  return out;
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
  const catBudget = {};
  if (settings.budgetMode === 'perCategory') {
    enabledKeys.forEach(k => { catBudget[k] = Math.max(0, settings.categories[k].budget || 0); });
  } else {
    // TOTAL mode: split the one budget equally across the enabled categories.
    const each = enabledKeys.length ? Math.floor((settings.budget || 0) / enabledKeys.length) : 0;
    let leftover = (settings.budget || 0) - each * enabledKeys.length;
    enabledKeys.forEach((k, i) => { catBudget[k] = each + (i < leftover ? 1 : 0); });
  }

  const byCat = {}; active.forEach(c => { const k = c.category || 'Unsorted'; (byCat[k] = byCat[k] || []).push(c); });

  const categories = CAT_KEYS.map(catKey => {
    const spec = CAT_BY_KEY[catKey];
    const cfg = settings.categories[catKey];
    const enabled = enabledKeys.includes(catKey);
    const budget = catBudget[catKey] || 0;
    const avgCost = Math.max(1, cfg.avgCost || spec.avgCost);
    const units = enabled ? Math.floor(budget / avgCost) : 0;

    const fitLabel = k => (spec.fits.find(f => f.key === k) || (cfg.extraFits.find(f => f.key === k)) || { label: k }).label;
    const fitRows = pctRows(cfg.fits, fitLabel);
    // HIERARCHY, step 1: the category's units flow down to the fits by fit %.
    const fitUnits = splitInts(units, cfg.fits);
    const fitBudget = splitInts(budget, cfg.fits);

    const sizeKeys = Object.keys(cfg.sizes);
    const colKeys  = Object.keys(cfg.colours);
    // Aggregates rebuilt from the nested numbers so the category-level size /
    // colour totals always equal the sum of the fit-level splits (no drift).
    const sizeAgg = {}; sizeKeys.forEach(k => sizeAgg[k] = 0);
    const colAgg  = {}; colKeys.forEach(k => colAgg[k] = 0);

    const pool = byCat[catKey] || [];
    const fits = fitRows.map(fr => {
      const fu = fitUnits[fr.key] || 0;
      // step 2: EACH FIT's units are split into SIZES (a sub-portion of the fit,
      // not of the whole category).
      const fitSizeUnits = splitInts(fu, cfg.sizes);
      const sizes = sizeKeys.map(sk => {
        const su = fitSizeUnits[sk] || 0;
        sizeAgg[sk] += su;
        // step 3: EACH (fit, size)'s units are split into COLOURS.
        const scColUnits = splitInts(su, cfg.colours);
        const colours = colKeys.map(ck => { colAgg[ck] += (scColUnits[ck] || 0); return { key: ck, label: ck, units: scColUnits[ck] || 0 }; })
          .filter(c => c.units > 0);
        return { key: sk, label: sk, units: su, colours };
      }).filter(s => s.units > 0);
      return {
        key: fr.key, label: fr.label, pct: fr.pct, share: fr.share,
        units: fu, budget: fitBudget[fr.key] || 0,
        photos: pool.filter(c => c.fit === fr.key).map(publicCandidate),
        sizes,
        unassignedFit: false
      };
    });
    const noFit = pool.filter(c => !c.fit);
    if (noFit.length) fits.push({ key: '_unassigned', label: 'Fit not detected', pct: 0, share: 0, units: 0, budget: 0, photos: noFit.map(publicCandidate), sizes: [], unassignedFit: true });

    return {
      category: catKey, label: spec.label, designNote: spec.designNote,
      enabled, budget, avgCost, units, poolCount: pool.length,
      fits,
      // Category-level roll-ups (summed from the hierarchy above, for a quick read).
      sizes: pctRows(cfg.sizes, k => k).map(r => ({ ...r, units: sizeAgg[r.key] || 0 })),
      colours: pctRows(cfg.colours, k => k).map(r => ({ ...r, units: colAgg[r.key] || 0 }))
    };
  });

  const unsorted = (byCat['Unsorted'] || []).map(publicCandidate);
  const totalBudget = enabledKeys.reduce((s, k) => s + (catBudget[k] || 0), 0);
  const totalUnits = categories.reduce((s, c) => s + c.units, 0);
  return { budgetMode: settings.budgetMode, totalBudget, totalUnits, categories, unsorted };
}

// ── Routes ───────────────────────────────────────────────────────
router.get('/api/casuals/spec', (req, res) => {
  res.json({ success: true, categories: CASUALS_SPEC });
});

router.get('/api/casuals/settings', (req, res) => {
  res.json({ success: true, settings: settingsWithDefaults(loadStore()) });
});

router.post('/api/casuals/settings', (req, res) => {
  const s = loadStore();
  const b = req.body || {};
  const cur = settingsWithDefaults(s);
  // Store a normalised, complete settings blob (simplest + safest).
  const next = { budgetMode: b.budgetMode === 'perCategory' ? 'perCategory' : 'total',
    budget: b.budget != null ? Math.max(0, parseInt(String(b.budget).replace(/[^\d]/g, '')) || 0) : cur.budget,
    categories: {} };
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
      fits: pickPct(inc.fits, c.fits),
      sizes: pickPct(inc.sizes, c.sizes),
      colours: pickPct(inc.colours, c.colours),
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
  markDuplicates(s.candidates);
  res.json({ success: true, candidates: s.candidates.map(publicCandidate),
    categories: categoryCounts(s.candidates), total: s.candidates.length });
});

router.post('/api/casuals/candidates', async (req, res) => {
  const r = await runCandidateUpload(req, res);
  if (res.headersSent) return;
  if (!r.ok) return res.status(400).json({ success: false, error: r.error });
  const files = req.files || [];
  if (!files.length) return res.status(400).json({ success: false, error: 'No photos received' });
  const vendor = String((req.body && req.body.vendor) || '').trim();
  const s = loadStore();
  const added = [];
  for (const f of files) {
    const fp = path.join(CAND_DIR, f.filename);
    const sig = await computeSignature(fp);
    const c = { id: crypto.randomBytes(8).toString('hex'), file: f.filename, vendor,
      category: null, fit: null, colour: null, pattern: null,
      uploadedAt: new Date().toISOString(), sha: fileSha(fp), phash: sig ? sig.phash : null, avg: sig ? sig.avg : null, dupeOf: null };
    s.candidates.push(c);
    added.push(publicCandidate(c));
  }
  const dupes = markDuplicates(s.candidates);
  saveStore(s);
  res.json({ success: true, added, total: s.candidates.length, dupes, categories: categoryCounts(s.candidates) });
});

router.get('/api/casuals/candidate/:id', (req, res) => {
  const s = loadStore();
  const c = s.candidates.find(x => x.id === req.params.id);
  if (!c) return res.status(404).end();
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
  markDuplicates(s.candidates);
  saveStore(s);
  res.json({ success: true, total: s.candidates.length, categories: categoryCounts(s.candidates) });
});

router.post('/api/casuals/candidates/clear', (req, res) => {
  const s = loadStore();
  s.candidates.forEach(c => { try { fs.unlinkSync(path.join(CAND_DIR, c.file)); } catch {} });
  s.candidates = [];
  saveStore(s);
  res.json({ success: true, total: 0, categories: categoryCounts([]) });
});

router.post('/api/casuals/analyze', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI segregation is not enabled. Set ANTHROPIC_API_KEY in Railway to turn it on.' });
    const s = loadStore();
    const cands = s.candidates || [];
    if (!cands.length) return res.status(400).json({ success: false, error: 'No photos yet — upload some casual-wear photos first.' });
    const force = req.body && (req.body.force === true || req.body.force === 'true');
    const dupes = markDuplicates(s.candidates);
    const todo = cands.filter(c => !c.dupeOf && (force || !c.category));
    let classified = 0;
    for (let i = 0; i < todo.length; i += VISION_BATCH) {
      const slice = todo.slice(i, i + VISION_BATCH);
      const items = [];
      slice.forEach(c => { try { items.push({ id: c.id, buffer: fs.readFileSync(path.join(CAND_DIR, c.file)), mediaType: mediaTypeForFile(c.file) }); } catch {} });
      if (!items.length) continue;
      const map = await scoreBatch(items);
      slice.forEach(c => {
        const r = map[c.id];
        if (r) { c.category = r.category; c.fit = r.fit; c.aiFit = r.aiFit; c.colour = r.colour; c.pattern = r.pattern; classified++; }
      });
      saveStore(s);
    }
    const settings = settingsWithDefaults(s);
    res.json({ success: true, classified, dupes, settings, categories: categoryCounts(s.candidates), ...buildPlan(s.candidates, settings) });
  } catch (err) {
    if (err && err.name === 'AbortError') return res.status(504).json({ success: false, error: 'Segregation timed out. Try again — sorted photos are saved.' });
    res.status(502).json({ success: false, error: 'Segregation failed: ' + (err.message || 'unknown') });
  }
});

router.get('/api/casuals/plan', (req, res) => {
  const s = loadStore();
  const settings = settingsWithDefaults(s);
  markDuplicates(s.candidates);
  const analysed = s.candidates.some(c => c.category);
  res.json({ success: true, analysed, settings, categories: categoryCounts(s.candidates), ...buildPlan(s.candidates, settings) });
});

module.exports = { router, CASUALS_SPEC, buildPlan, settingsWithDefaults, splitInts };
