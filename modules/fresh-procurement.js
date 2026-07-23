// ═══════════════════════════════════════════════════════════════
// modules/fresh-procurement.js — THE PROCUREMENT BRAIN
//
// Decides WHAT NEW FASHION to source each cycle. This is NOT restocking
// (that reorders proven SKUs from sales history). Fresh procurement bets
// on new designs/categories the store has never sold, driven by:
//
//   ① The Feed        — vendor candidate photos sorted into funkiness
//                        tiers (Statement / Medium / Basic) by a % split.   [PRIMARY]
//   ② Market Trends    — live competitor + Google/niche signal, refreshed
//                        every cycle.                                        [PRIMARY]
//   ③ Buyer-taste lens — past-sales SIZES & COLOURS only; nudges the size
//                        curve & colour mix. Never picks products.          [LEAST]
//
// Budget-led: the founder sets a cycle length + a budget ceiling; the tier
// % splits that budget; the app reports how many pieces it bought.
//
// Data on the /data volume: fresh-procurement.json (settings + sourcing
// sessions). Signal ③ is computed LIVE from the orders ledger each call —
// never stored, always fresh. Nothing here writes to Shopify.
//
// Endpoints (all behind the auth gate):
//   GET  /api/fresh/settings          → cycle, budget, tier split, competitors
//   POST /api/fresh/settings          → save the above
//   GET  /api/fresh/taste             → Signal ③: sizes & colours from sales
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const crypto  = require('crypto');
const multer  = require('multer');
let Jimp = null; try { Jimp = require('jimp'); } catch { /* dedup degrades to off */ }

const router = express.Router();

// ── Category vocabulary ──────────────────────────────────────────
// Auto-detected per photo by the vision pass. We map free-text to this
// canonical list so "Tee"/"T-Shirt"/"tshirt" don't fragment into buckets.
// Anything unrecognised falls into 'Other'.
const CATEGORIES = ['T-shirt', 'Shirt', 'Trouser', 'Jeans', 'Cordset', 'Shorts', 'Jacket', 'Sweatshirt', 'Hoodie', 'Dress', 'Other'];
function normCategory(raw) {
  const t = String(raw || '').toLowerCase().replace(/[^a-z]/g, '');
  const map = {
    tshirt: 'T-shirt', tee: 'T-shirt', tees: 'T-shirt', halfsleeve: 'T-shirt',
    shirt: 'Shirt', shirts: 'Shirt', overshirt: 'Shirt',
    trouser: 'Trouser', trousers: 'Trouser', pant: 'Trouser', pants: 'Trouser', cargo: 'Trouser', cargos: 'Trouser', chino: 'Trouser', chinos: 'Trouser',
    jean: 'Jeans', jeans: 'Jeans', denim: 'Jeans',
    cordset: 'Cordset', coord: 'Cordset', coordset: 'Cordset', coords: 'Cordset', twopiece: 'Cordset',
    short: 'Shorts', shorts: 'Shorts',
    jacket: 'Jacket', jackets: 'Jacket', bomber: 'Jacket', coat: 'Jacket',
    sweatshirt: 'Sweatshirt', crewneck: 'Sweatshirt', sweater: 'Sweatshirt',
    hoodie: 'Hoodie', hooded: 'Hoodie',
    dress: 'Dress', gown: 'Dress'
  };
  if (map[t]) return map[t];
  // substring fallback
  for (const k of Object.keys(map)) if (t.includes(k)) return map[k];
  return 'Other';
}

// ── Perceptual hash + near-dup detection ─────────────────────────
// dHash: resize to 9×8 greyscale, compare each pixel to its right neighbour →
// 64 bits. Near-identical shots (same garment, slightly different angle) land
// within a few bits. We AND it with an average-colour check so two different
// COLOURWAYS of the same style are NOT merged (they're different SKUs to buy).
const DUP_HAM_MAX = 8;      // ≤8 of 64 bits differ → structurally the same shot
const DUP_COL_MAX = 40;     // avg-colour euclidean distance ceiling
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
// Scan candidates in upload order; mark each as dupeOf the FIRST earlier unique
// candidate it near-matches. Dupes are excluded from all segregation counts.
function markDuplicates(cands) {
  const uniques = [];
  cands.forEach(c => {
    c.dupeOf = null;
    if (!c.phash) { uniques.push(c); return; } // no signature → treat as unique
    const hit = uniques.find(u => u.phash && hamming(c.phash, u.phash) <= DUP_HAM_MAX && colourDist(c.avg, u.avg) <= DUP_COL_MAX);
    if (hit) c.dupeOf = hit.id; else uniques.push(c);
  });
  return cands.filter(c => c.dupeOf).length;
}

const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');

// Vision model that sorts candidate photos into funkiness tiers (same key the
// Purchases module already uses for invoice reading).
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const AI_MODEL = process.env.FRESH_PROC_AI_MODEL || process.env.PROCUREMENT_AI_MODEL || 'claude-sonnet-4-6';

// Pull the first {...} JSON object/array out of a model reply (it may wrap the
// JSON in prose or ```json fences).
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

function mediaTypeForFile(fn) {
  const e = path.extname(fn || '').toLowerCase();
  return e === '.png' ? 'image/png' : e === '.webp' ? 'image/webp' : e === '.gif' ? 'image/gif' : 'image/jpeg';
}

// ── JSON store (atomic write, same pattern as every module) ──────
const STORE_PATH = process.env.FRESH_PROC_PATH || path.join(DATA_DIR, 'fresh-procurement.json');

// The orders ledger lives beside us on the volume — Signal ③ reads it.
const ORDERS_PATH = process.env.ORDERS_PATH || path.join(DATA_DIR, 'orders.json');

// ── Candidate photos (Signal ①) ─────────────────────────────────
// Vendor candidate photos uploaded for a sourcing cycle. Stored on the
// persistent volume so they survive redeploys. These are the pool the tier
// sorter and (later) AI trend-ranker judge.
const CAND_DIR = path.join(DATA_DIR, 'fresh-candidates');
try { fs.mkdirSync(CAND_DIR, { recursive: true }); } catch { /* exists */ }
const candidateUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      try { fs.mkdirSync(CAND_DIR, { recursive: true }); } catch { /* exists */ }
      cb(null, CAND_DIR);
    },
    filename: (req, file, cb) => {
      const ext = (path.extname(file.originalname || '') || '.jpg').toLowerCase().replace(/[^.a-z0-9]/g, '');
      cb(null, Date.now() + '-' + crypto.randomBytes(6).toString('hex') + (ext || '.jpg'));
    }
  }),
  limits: { fileSize: 25 * 1024 * 1024, files: 200 },
  fileFilter: (req, file, cb) => cb(null, /^image\//.test(file.mimetype))
});
// Run the multer middleware but always resolve to JSON — an uncaught multer
// error (file too large, too many files, disk issue) would otherwise fall to
// Express's default HTML error page, which the client can't parse.
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

const DEFAULTS = {
  cycleDays: 20,
  budget: 400000,
  tiers: { statement: 20, medium: 50, basic: 30 },
  competitors: ['Zara', 'Snitch', 'The Souled Store', 'Bluorng', 'Jaywalking', 'Dripping Monkey', 'Farak']
};

function atomicWrite(fp, data) {
  const tmp = fp + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, fp);
}
function loadStore() {
  try {
    const s = JSON.parse(fs.readFileSync(STORE_PATH, 'utf8'));
    if (!s.settings) s.settings = {};
    if (!s.sessions) s.sessions = {};
    if (!Array.isArray(s.candidates)) s.candidates = [];
    return s;
  } catch { return { settings: {}, sessions: {}, candidates: [] }; }
}
function saveStore(s) { atomicWrite(STORE_PATH, JSON.stringify(s)); }

// Public shape of a candidate (never leak the disk path).
function publicCandidate(c) {
  return { id: c.id, vendor: c.vendor || '', colour: c.colour || '', cost: c.cost || null,
    url: '/api/fresh/candidate/' + c.id, tier: c.tier || null, funk: (typeof c.funk === 'number' ? c.funk : null),
    category: c.category || null, aiColour: c.aiColour || null, articles: c.articles || 1,
    dupeOf: c.dupeOf || null,
    locked: !!c.tierLocked, selected: !!c.selected, uploadedAt: c.uploadedAt };
}
// Group candidates by vendor for the chip counts the UI shows.
function vendorCounts(cands) {
  const by = {};
  cands.forEach(c => { const v = c.vendor || 'Unassigned'; by[v] = (by[v] || 0) + 1; });
  return Object.entries(by).map(([vendor, count]) => ({ vendor, count })).sort((a, b) => b.count - a.count);
}

// Sanitise a {statement,medium,basic} split into whole non-negative numbers.
function cleanSplit(o) {
  return { statement: Math.max(0, parseInt(o && o.statement) || 0),
    medium: Math.max(0, parseInt(o && o.medium) || 0),
    basic: Math.max(0, parseInt(o && o.basic) || 0) };
}
function settingsWithDefaults(s) {
  const st = s.settings || {};
  // Per-category tier %: an optional override map { 'T-shirt': {statement,medium,basic}, … }.
  // Any category without an entry falls back to the global `tiers` split.
  const ct = {};
  if (st.categoryTiers && typeof st.categoryTiers === 'object') {
    for (const [k, v] of Object.entries(st.categoryTiers)) if (v && typeof v === 'object') ct[k] = cleanSplit(v);
  }
  return {
    cycleDays: st.cycleDays || DEFAULTS.cycleDays,
    budget: st.budget != null ? st.budget : DEFAULTS.budget,
    tiers: {
      statement: st.tiers && st.tiers.statement != null ? st.tiers.statement : DEFAULTS.tiers.statement,
      medium:    st.tiers && st.tiers.medium    != null ? st.tiers.medium    : DEFAULTS.tiers.medium,
      basic:     st.tiers && st.tiers.basic      != null ? st.tiers.basic      : DEFAULTS.tiers.basic
    },
    categoryTiers: ct,
    competitors: Array.isArray(st.competitors) && st.competitors.length ? st.competitors : DEFAULTS.competitors.slice()
  };
}
// The tier % split that governs a given category — its own override if set,
// else the global split. This is what makes each category cut independently.
function tiersForCategory(settings, cat) {
  const ct = settings.categoryTiers && settings.categoryTiers[cat];
  if (ct && (ct.statement || ct.medium || ct.basic)) return ct;
  return settings.tiers;
}

// ── Signal ③ — buyer-taste lens (sizes & colours from the ledger) ─
// Known colour vocabulary (from the decoded SKU colour table). Titles bake
// the colour in, e.g. "A611-Glowing Sweatshirt-BLACK", so we match against
// this list. Multi-word colours first so "SKY BLUE" wins over "BLUE".
const COLOURS = [
  'SKY BLUE', 'BLACK', 'BLUE', 'BROWN', 'CREAM', 'GREEN', 'GREY', 'GRAY',
  'MAROON', 'ORANGE', 'PINK', 'PURPLE', 'RED', 'WHITE', 'YELLOW', 'BEIGE',
  'OLIVE', 'KHAKI', 'GOLDEN', 'SILVER'
];
// Size vocabulary in display order (waist sizes handled separately as numbers).
const SIZE_ORDER = ['FS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', '3XL', '4XL', '5XL'];

function detectColour(title) {
  const t = String(title || '').toUpperCase();
  for (const c of COLOURS) { if (t.includes(c)) return c === 'GRAY' ? 'GREY' : c; }
  return null;
}
function detectSize(variantTitle, sku) {
  let v = String(variantTitle || '').trim().toUpperCase();
  // Shopify variant option is usually just the size ("M", "Large", "XL").
  const map = { 'MEDIUM': 'M', 'LARGE': 'L', 'EXTRA LARGE': 'XL', 'SMALL': 'S',
    'EXTRA SMALL': 'XS', 'FREE SIZE': 'FS', 'FREESIZE': 'FS',
    'DOUBLE EXTRA LARGE': 'XXL', 'TRIPLE EXTRA LARGE': '3XL' };
  if (map[v]) return map[v];
  if (SIZE_ORDER.includes(v)) return v;
  const waist = v.match(/\b(2[6-9]|3[0-9]|4[0-6])\b/); // waist 26–46
  if (waist) return waist[1];
  // Fall back to the trailing size code on the SKU (…J934FS → FS).
  const m = String(sku || '').toUpperCase().match(/([A-Z]\d+)((?:FS|XS|XXL|3XL|4XL|5XL|XL|S|M|L))$/);
  if (m) return m[2];
  const w = String(sku || '').match(/(2[6-9]|3[0-9]|4[0-6])$/);
  if (w) return w[1];
  return null;
}

function computeTaste(windowDays) {
  let ledger;
  try { ledger = JSON.parse(fs.readFileSync(ORDERS_PATH, 'utf8')); }
  catch { return { ok: false, reason: 'orders ledger not found', colours: [], sizes: [], orderCount: 0 }; }

  const orders = Object.values(ledger.orders || {});
  const cutoff = windowDays ? new Date(Date.now() - windowDays * 864e5).toISOString() : null;
  const colourQty = {}, sizeQty = {};
  let considered = 0, unitsCounted = 0;

  for (const o of orders) {
    if (cutoff && o.createdAt && o.createdAt < cutoff) continue;
    if (o.cancelledAt) continue;
    considered++;
    for (const li of (o.lineItems || [])) {
      const q = li.qty || 0;
      const col = detectColour(li.title);
      if (col) colourQty[col] = (colourQty[col] || 0) + q;
      const sz = detectSize(li.variantTitle, li.sku);
      if (sz) sizeQty[sz] = (sizeQty[sz] || 0) + q;
      unitsCounted += q;
    }
  }

  const colTotal = Object.values(colourQty).reduce((a, b) => a + b, 0) || 1;
  const colours = Object.entries(colourQty)
    .map(([k, v]) => ({ name: k, qty: v, pct: Math.round(v / colTotal * 100) }))
    .sort((a, b) => b.qty - a.qty);

  const szTotal = Object.values(sizeQty).reduce((a, b) => a + b, 0) || 1;
  const sizes = Object.entries(sizeQty)
    .map(([k, v]) => ({ name: k, qty: v, pct: Math.round(v / szTotal * 100) }))
    .sort((a, b) => {
      const ia = SIZE_ORDER.indexOf(a.name), ib = SIZE_ORDER.indexOf(b.name);
      if (ia >= 0 && ib >= 0) return ia - ib;
      if (ia >= 0) return -1; if (ib >= 0) return 1;
      return (parseInt(a.name) || 99) - (parseInt(b.name) || 99);
    });

  return { ok: true, windowDays: windowDays || null, orderCount: considered, unitsCounted, colours, sizes };
}

// ── Routes ───────────────────────────────────────────────────────
router.get('/api/fresh/settings', (req, res) => {
  res.json({ success: true, settings: settingsWithDefaults(loadStore()) });
});

router.post('/api/fresh/settings', (req, res) => {
  const s = loadStore();
  const b = req.body || {};
  const cur = settingsWithDefaults(s);
  const next = {
    cycleDays: b.cycleDays != null ? parseInt(b.cycleDays) || cur.cycleDays : cur.cycleDays,
    budget: b.budget != null ? Math.max(0, parseInt(String(b.budget).replace(/[^\d]/g, '')) || 0) : cur.budget,
    tiers: b.tiers && typeof b.tiers === 'object' ? cleanSplit(b.tiers) : cur.tiers,
    categoryTiers: (() => {
      if (!b.categoryTiers || typeof b.categoryTiers !== 'object') return cur.categoryTiers;
      const out = {};
      for (const [k, v] of Object.entries(b.categoryTiers)) if (v && typeof v === 'object') out[k] = cleanSplit(v);
      return out;
    })(),
    competitors: Array.isArray(b.competitors) ? b.competitors.map(x => String(x).trim()).filter(Boolean) : cur.competitors
  };
  s.settings = next;
  saveStore(s);
  res.json({ success: true, settings: settingsWithDefaults(s) });
});

router.get('/api/fresh/taste', (req, res) => {
  const w = req.query.window ? parseInt(req.query.window) : null;
  res.json({ success: true, taste: computeTaste(w) });
});

// ── Signal ① — candidate photo feed ─────────────────────────────
// List all candidates + per-vendor counts.
router.get('/api/fresh/candidates', (req, res) => {
  const s = loadStore();
  res.json({ success: true, candidates: s.candidates.map(publicCandidate),
    vendors: vendorCounts(s.candidates), total: s.candidates.length });
});

// Upload one or more photos, tagged with a vendor (+ optional colour / ¥ cost).
router.post('/api/fresh/candidates', async (req, res) => {
  const r = await runCandidateUpload(req, res);
  if (res.headersSent) return;
  if (!r.ok) return res.status(400).json({ success: false, error: r.error });
  const files = req.files || [];
  if (!files.length) return res.status(400).json({ success: false, error: 'No photos received' });
  const vendor = String((req.body && req.body.vendor) || '').trim();
  const colour = String((req.body && req.body.colour) || '').trim();
  const cost = req.body && req.body.cost ? parseFloat(req.body.cost) || null : null;
  const s = loadStore();
  const added = [];
  for (const f of files) {
    const sig = await computeSignature(path.join(CAND_DIR, f.filename));
    const c = { id: crypto.randomBytes(8).toString('hex'), file: f.filename,
      vendor, colour, cost, tier: null, uploadedAt: new Date().toISOString(),
      phash: sig ? sig.phash : null, avg: sig ? sig.avg : null, dupeOf: null };
    s.candidates.push(c);
    added.push(publicCandidate(c));
  }
  const dupes = markDuplicates(s.candidates);  // flag near-dupes across the whole pool
  saveStore(s);
  res.json({ success: true, added, total: s.candidates.length, dupes, vendors: vendorCounts(s.candidates) });
});

// Serve a stored candidate image by id.
router.get('/api/fresh/candidate/:id', (req, res) => {
  const s = loadStore();
  const c = s.candidates.find(x => x.id === req.params.id);
  if (!c) return res.status(404).end();
  const fp = path.join(CAND_DIR, c.file);
  if (!fs.existsSync(fp)) return res.status(404).end();
  res.sendFile(fp);
});

// Delete one candidate (removes the file too).
router.delete('/api/fresh/candidate/:id', (req, res) => {
  const s = loadStore();
  const i = s.candidates.findIndex(x => x.id === req.params.id);
  if (i < 0) return res.status(404).json({ success: false, error: 'Not found' });
  const [c] = s.candidates.splice(i, 1);
  try { fs.unlinkSync(path.join(CAND_DIR, c.file)); } catch { /* already gone */ }
  saveStore(s);
  res.json({ success: true, total: s.candidates.length, vendors: vendorCounts(s.candidates) });
});

// Clear the whole candidate pool (start a fresh cycle).
router.post('/api/fresh/candidates/clear', (req, res) => {
  const s = loadStore();
  s.candidates.forEach(c => { try { fs.unlinkSync(path.join(CAND_DIR, c.file)); } catch { /* ignore */ } });
  s.candidates = [];
  saveStore(s);
  res.json({ success: true, total: 0, vendors: [] });
});

// ── Signal ①b — AI tier sort + buy plan ─────────────────────────
// Classify each candidate photo into a funkiness tier via Claude vision, then
// turn the budget + tier % split into a concrete allocation.
const TIER_KEYS = ['statement', 'medium', 'basic'];
const VISION_BATCH = 10; // images per vision call

// Score each photo's "funkiness" on an ABSOLUTE 0–100 scale at temperature 0.
// Absolute + deterministic means the same photo gets (near) the same score every
// run and regardless of which batch it lands in — the two things that made the
// old per-batch tier call jump around and mis-file statements as medium.
async function scoreBatch(items) {
  // items: [{id, buffer, mediaType}]. Returns map id→{funk, category, colour, articles}.
  const content = [];
  items.forEach((it, i) => {
    content.push({ type: 'text', text: 'IMAGE ' + (i + 1) + ':' });
    content.push({ type: 'image', source: { type: 'base64', media_type: it.mediaType, data: it.buffer.toString('base64') } });
  });
  content.push({ type: 'text', text:
`You are a buyer for an Indian premium streetwear brand. For EACH image above, report four things. Judge each image on its own — NOT relative to the others.\n\n` +
`1) "funk" — how LOUD / FUNKY / statement-y the garment is, ABSOLUTE 0–100:\n` +
`   - 85–100: extremely loud hero piece — heavy all-over graphics, wild clashing colour, bold unusual cut.\n` +
`   - 60–84: clearly funky/eye-catching — prominent print or graphic, strong colour, distinctive styling.\n` +
`   - 35–59: has some character but everyday-wearable — a modest logo/print, one accent colour, mild detailing.\n` +
`   - 15–34: mostly clean — subtle branding or texture on an otherwise plain garment.\n` +
`   - 0–14: pure basic — plain solid colour, no graphics, essential staple.\n` +
`2) "category" — the garment type, EXACTLY one of: ${CATEGORIES.join(', ')}. A matching top+bottom worn together is "Cordset". Use "Other" only if none fit.\n` +
`3) "colour" — the single dominant colour word (e.g. Black, White, Blue, Olive, Maroon, Cream).\n` +
`4) "articles" — how many SEPARATE distinct garments are laid out in the photo (a cordset counts as 1 outfit; count 2+ only if clearly different products shown together).\n\n` +
`Return STRICT JSON ONLY, no prose: {"items":[{"funk":n,"category":"..","colour":"..","articles":n}, ...]} with exactly ${items.length} objects, one per image in order.` });
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
  const arr = parsed && Array.isArray(parsed.items) ? parsed.items : (parsed && Array.isArray(parsed.scores) ? parsed.scores.map(n => ({ funk: n })) : []);
  const out = {};
  items.forEach((it, i) => {
    const o = arr[i] || {};
    const n = Math.round(Number(o.funk));
    out[it.id] = {
      funk: isFinite(n) ? Math.max(0, Math.min(100, n)) : 50,
      category: normCategory(o.category),
      colour: String(o.colour || '').trim() || null,
      articles: Math.max(1, Math.round(Number(o.articles)) || 1)
    };
  });
  return out;
}

// Turn the tier % split into exact whole-photo counts for N candidates, using
// largest-remainder rounding so the counts always sum to N (e.g. 50 @ 20/50/30
// → 10 / 25 / 15).
function splitCounts(n, settings) {
  const p = settings.tiers || {};
  let s = Math.max(0, +p.statement || 0), m = Math.max(0, +p.medium || 0), b = Math.max(0, +p.basic || 0);
  let tot = s + m + b; if (tot <= 0) { s = 20; m = 50; b = 30; tot = 100; }
  const raw = { statement: n * s / tot, medium: n * m / tot, basic: n * b / tot };
  const base = { statement: Math.floor(raw.statement), medium: Math.floor(raw.medium), basic: Math.floor(raw.basic) };
  let rem = n - (base.statement + base.medium + base.basic);
  const order = TIER_KEYS.map(k => ({ k, f: raw[k] - Math.floor(raw[k]) })).sort((a, b) => b.f - a.f);
  for (let i = 0; i < rem; i++) base[order[i % 3].k]++;
  return base;
}

// A piece's tier is INTRINSIC to how funky it is — it is decided purely by its
// own funk score against fixed bands, and is NEVER shifted to a different tier
// to hit a quota. Matches the scoring rubric: 60+ = statement, 35–59 = medium,
// below 35 = basic.
function tierFromScore(score) {
  const n = Number(score) || 0;
  if (n >= 60) return 'statement';
  if (n >= 35) return 'medium';
  return 'basic';
}

// Taste nudge. This is FRESH sourcing, not restocking, so past sales must never
// drive picks — but at the MARGIN (which pieces make the cap) a colour that
// actually sells should edge out a dead one. We give each candidate a small,
// bounded bonus from its colour's share of past unit sales. Capped at
// TASTE_MAX_BONUS so it can never override the impression tier or pull a clearly
// weaker piece over a clearly funkier one — it only tips near-boundary picks.
const TASTE_MAX_BONUS = 10;
function colourDemandMap() {
  const t = computeTaste(null);
  const map = {};
  if (t && t.ok) (t.colours || []).forEach(c => { map[String(c.name).toUpperCase()] = c.pct || 0; });
  return map;
}
function tasteBonus(colour, map) {
  if (!colour || !map) return 0;
  const pct = map[String(colour).toUpperCase().trim()] || 0; // colour's % of past unit sales
  return Math.min(TASTE_MAX_BONUS, pct * 0.5);               // 20%+ of sales → full +10
}

// Classify + select. The pool is cut CATEGORY-FIRST (Shirts, T-shirts,
// Trousers…), and only then by impression inside each category. Three steps:
//   0) EXCLUDE near-duplicates entirely — a dupe is never tiered or selected.
//   1) TIER  = intrinsic band from the funk score (never cross-shifted).
//              Manually locked pieces keep the tier the user pinned.
//   2) SELECT = within each (category, tier), that category's own % split is a
//              CAP ("buy the best N"). Rank by funk + a bounded taste bonus and
//              mark the top `quota` selected; the rest stay in the same tier but
//              held back. Under-fill is fine — we never borrow across tiers or
//              across categories. Locked pieces are always selected.
function assignTiersBySplit(cands, settings) {
  const demand = colourDemandMap();
  // Step 0 — dupes sit out of every count.
  const active = cands.filter(c => !c.dupeOf);
  cands.forEach(c => { if (c.dupeOf) c.selected = false; });
  // Step 1 — intrinsic tier (impression only — taste never moves a tier).
  active.forEach(c => {
    if (c.tierLocked && TIER_KEYS.includes(c.tier)) return; // user-pinned
    c.tier = tierFromScore(c.funk);
  });
  // Step 2 — bucket by category, then cap each tier by THAT category's split.
  const byCat = {};
  active.forEach(c => { const cat = c.category || 'Other'; (byCat[cat] = byCat[cat] || []).push(c); });
  Object.keys(byCat).forEach(cat => {
    const group = byCat[cat];
    const quota = splitCounts(group.length, { tiers: tiersForCategory(settings, cat) });
    TIER_KEYS.forEach(t => {
      const inTier = group.filter(c => c.tier === t).sort((a, b) => {
        const la = a.tierLocked ? 1 : 0, lb = b.tierLocked ? 1 : 0;
        if (la !== lb) return lb - la;                 // locked first
        const sa = (a.funk || 0) + tasteBonus(a.aiColour || a.colour, demand);
        const sb = (b.funk || 0) + tasteBonus(b.aiColour || b.colour, demand);
        return (sb - sa) || (a.id < b.id ? -1 : 1);
      });
      const cap = Math.max(0, quota[t] || 0);
      inTier.forEach((c, idx) => { c.selected = idx < cap || !!c.tierLocked; });
    });
  });
}

// Budget-led allocation, CATEGORY-FIRST. Dupes are excluded up front. Within
// each category the pieces are grouped by impression tier and the best-N (that
// category's cap) are marked as the designs to buy. Stage 1 spreads the budget
// evenly across every selected design as a placeholder — the true per-piece
// rupee→unit maths lands in Stage 2, once vendor costs are entered.
function buildPlan(cands, settings) {
  const budget = Math.max(0, Number(settings.budget) || 0);
  const active = cands.filter(c => !c.dupeOf);
  const totalSelected = active.filter(c => c.selected).length;
  const perDesign = totalSelected ? Math.round(budget / totalSelected) : 0;

  const byCat = {};
  active.forEach(c => { const cat = c.category || 'Other'; (byCat[cat] = byCat[cat] || []).push(c); });

  const categories = Object.keys(byCat).sort((a, b) => {
    const ia = CATEGORIES.indexOf(a), ib = CATEGORIES.indexOf(b);
    return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib);
  }).map(cat => {
    const group = byCat[cat];
    const catPct = tiersForCategory(settings, cat);
    const quota = splitCounts(group.length, { tiers: catPct });
    const plan = {};
    TIER_KEYS.forEach(t => {
      const all = group.filter(c => c.tier === t)
        .sort((a, b) => ((b.funk || 0) - (a.funk || 0)) || (a.id < b.id ? -1 : 1));
      const designs = all.filter(c => c.selected);
      const overflow = all.filter(c => !c.selected);
      plan[t] = {
        tier: t,
        targetPct: Math.max(0, Number(catPct[t]) || 0),
        quota: quota[t] || 0,
        tierCount: all.length,
        designCount: designs.length,
        overflowCount: overflow.length,
        perDesign,
        designs: designs.map(publicCandidate),
        overflow: overflow.map(publicCandidate)
      };
    });
    const selectedCount = group.filter(c => c.selected).length;
    return { category: cat, total: group.length, selectedCount, subtotal: selectedCount * perDesign, plan };
  });

  return { budget, total: active.length, totalSelected, perDesign, categories };
}

// Buy sheet: take every SELECTED design (across all categories) and regroup by
// vendor — the actual "go order this" Desired PO. Each line carries the per-
// design budget so a vendor's subtotal = what to spend with that vendor.
function buildBuySheet(cands, settings) {
  const { budget, perDesign, categories } = buildPlan(cands, settings);
  const byVendor = {};
  categories.forEach(cat => {
    TIER_KEYS.forEach(t => {
      cat.plan[t].designs.forEach(c => {
        const v = c.vendor || 'Unassigned';
        (byVendor[v] = byVendor[v] || []).push(Object.assign({}, c, { budget: perDesign, category: cat.category }));
      });
    });
  });
  const vendors = Object.entries(byVendor).map(([vendor, lines]) => {
    lines.sort((a, b) => (CATEGORIES.indexOf(a.category) - CATEGORIES.indexOf(b.category))
      || (TIER_KEYS.indexOf(a.tier) - TIER_KEYS.indexOf(b.tier)) || ((b.funk || 0) - (a.funk || 0)));
    const subtotal = lines.reduce((s, l) => s + (l.budget || 0), 0);
    return { vendor, count: lines.length, subtotal, lines };
  }).sort((a, b) => b.subtotal - a.subtotal);
  const totalDesigns = vendors.reduce((s, v) => s + v.count, 0);
  const allocated = vendors.reduce((s, v) => s + v.subtotal, 0);
  return { budget, totalDesigns, allocated, vendors };
}

// The vendor-grouped buy sheet — derived live from the stored scores + split.
router.get('/api/fresh/buysheet', (req, res) => {
  const s = loadStore();
  const settings = settingsWithDefaults(s);
  const cands = s.candidates || [];
  const scored = cands.some(c => typeof c.funk === 'number');
  if (scored) { markDuplicates(cands); assignTiersBySplit(cands, settings); saveStore(s); }
  res.json({ success: true, scored, ...buildBuySheet(cands, settings) });
});

// Analyze: (re)classify every candidate, persist tiers, return the buy plan.
router.post('/api/fresh/analyze', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI tier-sorting is not enabled. Set ANTHROPIC_API_KEY in Railway to turn it on.' });
    const s = loadStore();
    const cands = s.candidates || [];
    if (!cands.length) return res.status(400).json({ success: false, error: 'No candidate photos yet — upload some in The Feed first.' });
    const force = req.body && (req.body.force === true || req.body.force === 'true');
    // De-dupe FIRST so near-duplicate shots never skew the tier counts.
    const dupes = markDuplicates(s.candidates);
    // Score anything without a funk score yet (or everything, if forced) — but
    // never spend vision calls on flagged duplicates.
    const todo = cands.filter(c => !c.dupeOf && (force || typeof c.funk !== 'number'));
    let classified = 0;
    for (let i = 0; i < todo.length; i += VISION_BATCH) {
      const slice = todo.slice(i, i + VISION_BATCH);
      const items = [];
      slice.forEach(c => {
        try { items.push({ id: c.id, buffer: fs.readFileSync(path.join(CAND_DIR, c.file)), mediaType: mediaTypeForFile(c.file) }); }
        catch { /* file missing — skip */ }
      });
      if (!items.length) continue;
      const map = await scoreBatch(items);
      slice.forEach(c => {
        const r = map[c.id];
        if (r && typeof r.funk === 'number') { c.funk = r.funk; c.category = r.category; c.aiColour = r.colour; c.articles = r.articles; classified++; }
      });
      saveStore(s); // persist progressively so a mid-run failure keeps prior work
    }
    const settings = settingsWithDefaults(s);
    // Segregate by category → impression tier, cut each category by its split.
    assignTiersBySplit(s.candidates, settings);
    saveStore(s);
    const result = buildPlan(s.candidates, settings);
    res.json({ success: true, classified, dupes, settings, candidates: s.candidates.map(publicCandidate), vendors: vendorCounts(s.candidates), ...result });
  } catch (err) {
    if (err && err.name === 'AbortError') return res.status(504).json({ success: false, error: 'The tier sorter timed out. Try again — already-sorted photos are saved.' });
    res.status(502).json({ success: false, error: 'Tier sort failed: ' + (err.message || 'unknown') });
  }
});

// Return the current plan without re-running the AI. Re-cuts tiers from the
// stored funk scores against the current split, so changing the % split re-
// divides the pool instantly (and for free) — no re-scoring needed.
router.get('/api/fresh/plan', (req, res) => {
  const s = loadStore();
  const settings = settingsWithDefaults(s);
  const cands = s.candidates || [];
  const scored = cands.some(c => typeof c.funk === 'number');
  if (scored) { markDuplicates(cands); assignTiersBySplit(cands, settings); saveStore(s); }
  res.json({ success: true, scored, settings, candidates: cands.map(publicCandidate), vendors: vendorCounts(cands), ...buildPlan(cands, settings) });
});

// Manual override: pin one candidate to a tier (locks it so re-analyze/re-cut
// won't move it). Pass tier:null to unlock and let it flow back to auto-ranking.
router.post('/api/fresh/candidate/:id/tier', (req, res) => {
  const s = loadStore();
  const c = (s.candidates || []).find(x => x.id === req.params.id);
  if (!c) return res.status(404).json({ success: false, error: 'Not found' });
  const t = req.body && req.body.tier;
  if (t === null || t === '' || t === 'auto') { c.tierLocked = false; }
  else if (TIER_KEYS.includes(t)) { c.tier = t; c.tierLocked = true; }
  else return res.status(400).json({ success: false, error: 'Bad tier' });
  const settings = settingsWithDefaults(s);
  markDuplicates(s.candidates);
  assignTiersBySplit(s.candidates, settings); // re-cut the rest around the pin
  saveStore(s);
  res.json({ success: true, settings, candidates: s.candidates.map(publicCandidate), vendors: vendorCounts(s.candidates), ...buildPlan(s.candidates, settings) });
});

module.exports = { router, computeTaste, detectColour, detectSize, buildPlan, buildBuySheet, splitCounts, assignTiersBySplit };
