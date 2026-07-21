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

const router = express.Router();

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
    url: '/api/fresh/candidate/' + c.id, tier: c.tier || null, uploadedAt: c.uploadedAt };
}
// Group candidates by vendor for the chip counts the UI shows.
function vendorCounts(cands) {
  const by = {};
  cands.forEach(c => { const v = c.vendor || 'Unassigned'; by[v] = (by[v] || 0) + 1; });
  return Object.entries(by).map(([vendor, count]) => ({ vendor, count })).sort((a, b) => b.count - a.count);
}

function settingsWithDefaults(s) {
  const st = s.settings || {};
  return {
    cycleDays: st.cycleDays || DEFAULTS.cycleDays,
    budget: st.budget != null ? st.budget : DEFAULTS.budget,
    tiers: {
      statement: st.tiers && st.tiers.statement != null ? st.tiers.statement : DEFAULTS.tiers.statement,
      medium:    st.tiers && st.tiers.medium    != null ? st.tiers.medium    : DEFAULTS.tiers.medium,
      basic:     st.tiers && st.tiers.basic      != null ? st.tiers.basic      : DEFAULTS.tiers.basic
    },
    competitors: Array.isArray(st.competitors) && st.competitors.length ? st.competitors : DEFAULTS.competitors.slice()
  };
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
    tiers: b.tiers && typeof b.tiers === 'object' ? {
      statement: Math.max(0, parseInt(b.tiers.statement) || 0),
      medium:    Math.max(0, parseInt(b.tiers.medium) || 0),
      basic:     Math.max(0, parseInt(b.tiers.basic) || 0)
    } : cur.tiers,
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
  const added = files.map(f => {
    const c = { id: crypto.randomBytes(8).toString('hex'), file: f.filename,
      vendor, colour, cost, tier: null, uploadedAt: new Date().toISOString() };
    s.candidates.push(c);
    return publicCandidate(c);
  });
  saveStore(s);
  res.json({ success: true, added, total: s.candidates.length, vendors: vendorCounts(s.candidates) });
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

async function classifyBatch(items) {
  // items: [{id, buffer, mediaType}]. Returns map id→tier.
  const content = [];
  items.forEach((it, i) => {
    content.push({ type: 'text', text: 'IMAGE ' + (i + 1) + ':' });
    content.push({ type: 'image', source: { type: 'base64', media_type: it.mediaType, data: it.buffer.toString('base64') } });
  });
  content.push({ type: 'text', text:
`You are a buyer for an Indian premium streetwear brand deciding how "funky" each garment photo is. ` +
`Classify EACH of the ${items.length} images above into exactly one tier:\n` +
`- "statement" = loud, bold, funky hero pieces — heavy graphics, wild colour, unusual cuts; the showstoppers.\n` +
`- "medium" = funky-but-wearable daily pieces — some character/print but everyday-friendly; the volume core.\n` +
`- "basic" = clean essentials — plain or minimal, anchor pieces that pair with statements.\n\n` +
`Return STRICT JSON ONLY, no prose: {"tiers":["statement"|"medium"|"basic", ... one per image in order]}. ` +
`The array MUST have exactly ${items.length} entries.` });
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 90000);
  let r;
  try {
    r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: AI_MODEL, max_tokens: 1000, messages: [{ role: 'user', content }] }),
      signal: ctrl.signal
    });
  } finally { clearTimeout(timer); }
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error((j.error && j.error.message) || ('HTTP ' + r.status));
  const parsed = extractJson((j.content || []).map(c => c.text || '').join(''));
  const arr = parsed && Array.isArray(parsed.tiers) ? parsed.tiers : [];
  const out = {};
  items.forEach((it, i) => { const t = String(arr[i] || '').toLowerCase(); out[it.id] = TIER_KEYS.includes(t) ? t : 'medium'; });
  return out;
}

// Budget-led allocation: each tier gets budget×pct; spread evenly across the
// designs the AI put in that tier. Returns a per-tier plan the UI renders.
function buildPlan(cands, settings) {
  const budget = Math.max(0, Number(settings.budget) || 0);
  const pct = settings.tiers || {};
  const plan = {};
  TIER_KEYS.forEach(t => {
    const designs = cands.filter(c => c.tier === t);
    const share = Math.max(0, Number(pct[t]) || 0);
    const tierBudget = Math.round(budget * share / 100);
    plan[t] = {
      tier: t,
      targetPct: share,
      designCount: designs.length,
      tierBudget,
      perDesign: designs.length ? Math.round(tierBudget / designs.length) : 0,
      designs: designs.map(publicCandidate)
    };
  });
  const total = cands.length;
  return { budget, total, plan };
}

// Analyze: (re)classify every candidate, persist tiers, return the buy plan.
router.post('/api/fresh/analyze', async (req, res) => {
  try {
    if (!ANTHROPIC_API_KEY) return res.status(400).json({ success: false, error: 'AI tier-sorting is not enabled. Set ANTHROPIC_API_KEY in Railway to turn it on.' });
    const s = loadStore();
    const cands = s.candidates || [];
    if (!cands.length) return res.status(400).json({ success: false, error: 'No candidate photos yet — upload some in The Feed first.' });
    const force = req.body && (req.body.force === true || req.body.force === 'true');
    // Only classify what needs it (unless forced) to save time/cost.
    const todo = cands.filter(c => force || !c.tier);
    let classified = 0;
    for (let i = 0; i < todo.length; i += VISION_BATCH) {
      const slice = todo.slice(i, i + VISION_BATCH);
      const items = [];
      slice.forEach(c => {
        try { items.push({ id: c.id, buffer: fs.readFileSync(path.join(CAND_DIR, c.file)), mediaType: mediaTypeForFile(c.file) }); }
        catch { /* file missing — skip */ }
      });
      if (!items.length) continue;
      const map = await classifyBatch(items);
      slice.forEach(c => { if (map[c.id]) { c.tier = map[c.id]; classified++; } });
      saveStore(s); // persist progressively so a mid-run failure keeps prior work
    }
    const settings = settingsWithDefaults(s);
    const result = buildPlan(s.candidates, settings);
    res.json({ success: true, classified, settings, candidates: s.candidates.map(publicCandidate), vendors: vendorCounts(s.candidates), ...result });
  } catch (err) {
    if (err && err.name === 'AbortError') return res.status(504).json({ success: false, error: 'The tier sorter timed out. Try again — already-sorted photos are saved.' });
    res.status(502).json({ success: false, error: 'Tier sort failed: ' + (err.message || 'unknown') });
  }
});

// Return the current plan without re-running the AI (uses stored tiers).
router.get('/api/fresh/plan', (req, res) => {
  const s = loadStore();
  const settings = settingsWithDefaults(s);
  res.json({ success: true, settings, ...buildPlan(s.candidates || [], settings) });
});

module.exports = { router, computeTaste, detectColour, detectSize, buildPlan };
