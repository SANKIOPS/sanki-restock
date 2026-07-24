// ═══════════════════════════════════════════════════════════════
// modules/size-tracker.js — SANKI Size Tracker
//
// Two jobs:
//   ① Store SANKI's OWN target size charts — the measurements a garment
//      SHOULD have, fed once per (category, fit, size): e.g. a Relaxed
//      T-shirt in M = chest 54cm, shoulder 46cm, length 71cm, sleeve 22cm.
//   ② At order time, take the VENDOR's (China) size chart, compare it to
//      the stored target, and say — per desired size — which China size to
//      actually source (China's "M" may really be SANKI's "S"), or flag
//      that no China size fits within tolerance.
//
// This removes the manual "what's China's M chest vs India's M chest for
// this fit?" cross-check that's done by hand on every PO.
//
// Categories / fits / sizes are borrowed from the Casuals spec so the two
// tools stay in lock-step. Measurement FIELDS differ by garment type
// (tops measure chest/shoulder; bottoms measure waist/hip).
//
// Store: size-tracker.json on the /data volume. Nothing writes to Shopify.
//
// Endpoints (behind the auth gate):
//   GET  /api/sizetracker/config           → categories, fits, sizes, fields, tolerance
//   GET  /api/sizetracker/targets          → all saved target charts
//   POST /api/sizetracker/targets          → save one (category, fit) target chart
//   POST /api/sizetracker/tolerance        → save the match tolerance (cm)
//   POST /api/sizetracker/compare          → compare a vendor chart to the target
// ═══════════════════════════════════════════════════════════════
const express = require('express');
const path    = require('path');
const fs      = require('fs');
const multer  = require('multer');

const router = express.Router();

// Vision (read a size-chart photo into numbers). Shares the Casuals key/model.
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const AI_MODEL = process.env.SIZETRACKER_AI_MODEL || process.env.CASUALS_AI_MODEL || 'claude-sonnet-4-6';

// Borrow the category / fit / size scaffold from Casuals so the two never drift.
let CASUALS_SPEC = [];
try { CASUALS_SPEC = require('./casuals').CASUALS_SPEC || []; } catch { CASUALS_SPEC = []; }

// Measurement fields per category (all centimetres). Tops vs bottoms differ.
const FIELDS_BY_CAT = {
  'T-shirt': ['Chest', 'Shoulder', 'Length', 'Sleeve'],
  'Shirt':   ['Chest', 'Shoulder', 'Length', 'Sleeve'],
  'Trouser': ['Waist', 'Hip', 'Inseam', 'Thigh', 'Bottom hem']
};
function fieldsFor(catKey) { return FIELDS_BY_CAT[catKey] || ['Chest', 'Shoulder', 'Length']; }

// Build the config the UI needs: each category with its fits, sizes, and the
// measurement fields that apply to it.
function buildConfig() {
  return CASUALS_SPEC.map(spec => ({
    key: spec.key,
    label: spec.label,
    fits: spec.fits.map(f => ({ key: f.key, label: f.label })),
    sizes: spec.sizes.map(s => s.key),
    fields: fieldsFor(spec.key)
  }));
}
const CAT_KEYS = () => CASUALS_SPEC.map(c => c.key);
const catExists = (k) => CAT_KEYS().includes(k);
const fitExists = (catKey, fitKey) => {
  const c = CASUALS_SPEC.find(x => x.key === catKey);
  return !!(c && c.fits.some(f => f.key === fitKey));
};

// ── Store ─────────────────────────────────────────────────────────
const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const STORE_PATH = process.env.SIZE_TRACKER_PATH || path.join(DATA_DIR, 'size-tracker.json');
const DEFAULT_TOLERANCE = 1.5; // cm — a China size within ±this on every field is a match

function atomicWrite(fp, data) {
  const tmp = fp + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, data);
  fs.renameSync(tmp, fp);
}
function loadStore() {
  try {
    const s = JSON.parse(fs.readFileSync(STORE_PATH, 'utf8'));
    if (!s.targets) s.targets = {};
    if (typeof s.tolerance !== 'number') s.tolerance = DEFAULT_TOLERANCE;
    return s;
  } catch { return { targets: {}, tolerance: DEFAULT_TOLERANCE }; }
}
function saveStore(s) { atomicWrite(STORE_PATH, JSON.stringify(s)); }

// Coerce a measurement value to a positive number or null (blank = not set).
function num(v) { const n = parseFloat(v); return isFinite(n) && n >= 0 ? Math.round(n * 10) / 10 : null; }
// Clean an incoming chart { sizeLabel: { field: value } } down to known fields.
function cleanChart(raw, fields) {
  const out = {};
  if (!raw || typeof raw !== 'object') return out;
  for (const [size, meas] of Object.entries(raw)) {
    if (!meas || typeof meas !== 'object') continue;
    const row = {};
    fields.forEach(f => { const v = num(meas[f]); if (v != null) row[f] = v; });
    if (Object.keys(row).length) out[String(size).trim().toUpperCase()] = row;
  }
  return out;
}

// ── Comparison engine ─────────────────────────────────────────────
// For a target size T and a vendor size V, over the fields the TARGET defines:
//   diff(field) = V - T   (positive = China runs bigger)
//   ok(field)   = |diff| <= tol
//   dist        = Σ |diff| over shared fields (unshared target fields penalised)
// A vendor size "fits" a target when every target field is present AND within tol.
function compareChartToTarget(targetChart, vendorChart, fields, tol, unit) {
  const results = [];
  const targetSizes = Object.keys(targetChart);
  const vendorSizes = Object.keys(vendorChart);
  // Storage & math are always in CENTIMETRES. `unit` only controls how numbers
  // are FORMATTED for display back to the buyer (cm ↔ inches). tol is in cm.
  const U = unit === 'in' ? 'in' : 'cm';
  const F = U === 'in' ? (1 / 2.54) : 1;
  const disp = (cm) => cm == null ? null : Math.round(cm * F * 10) / 10;

  targetSizes.forEach(tSize => {
    const T = targetChart[tSize];
    const tFields = fields.filter(f => T[f] != null);

    // Score every vendor size against this target.
    const scored = vendorSizes.map(vSize => {
      const V = vendorChart[vSize];
      let dist = 0, missing = 0, allOk = true;
      const diffs = {};
      tFields.forEach(f => {
        if (V[f] == null) { missing++; allOk = false; diffs[f] = { v: null, t: disp(T[f]), d: null, ok: false }; return; }
        const rawD = Math.round((V[f] - T[f]) * 10) / 10;   // cm — used for the fit test
        const ok = Math.abs(rawD) <= tol;
        if (!ok) allOk = false;
        dist += Math.abs(rawD);
        diffs[f] = { v: disp(V[f]), t: disp(T[f]), d: disp(rawD), ok };  // displayed in chosen unit
      });
      // Penalise vendor sizes that don't cover all target fields so they rank last.
      dist += missing * 999;
      return { label: vSize, dist: Math.round(dist * 10) / 10, fits: allOk && missing === 0, missing, diffs };
    }).sort((a, b) => a.dist - b.dist);

    const best = scored[0] || null;
    const sameLabel = scored.find(s => s.label === tSize) || null;
    const dtol = disp(tol);

    // Decide the recommendation.
    let verdict, source = null, status;
    if (!best || !vendorSizes.length) {
      verdict = 'No vendor chart entered for a comparison yet.'; status = 'none';
    } else if (sameLabel && sameLabel.fits) {
      verdict = '✓ Source China ' + tSize + ' — it matches your ' + tSize + ' within ±' + dtol + U + '.';
      source = tSize; status = 'match';
    } else if (best.fits) {
      verdict = '⚠ Re-map: China ' + tSize + ' does not match — source China ' + best.label + ' for your ' + tSize + ' (it fits within ±' + dtol + U + ').';
      source = best.label; status = 'remap';
    } else {
      // Nothing fits. Report the closest and the worst offending field (in display unit).
      let worst = null;
      Object.entries(best.diffs).forEach(([f, o]) => { if (o.d != null && (!worst || Math.abs(o.d) > Math.abs(worst.d))) worst = { field: f, d: o.d }; });
      const off = worst ? (Math.abs(worst.d) + U + ' on ' + worst.field + (worst.d > 0 ? ' (China bigger)' : ' (China smaller)')) : 'measurements incomplete';
      verdict = '✗ No China size fits your ' + tSize + ' within ±' + dtol + U + '. Closest is China ' + best.label + ', off by ' + off + '. Ask the vendor to adjust, or drop this size.';
      status = 'nofit';
    }

    results.push({ size: tSize, target: T, fields: tFields, best, sameLabel, source, status, verdict });
  });

  // Summary: which China sizes to actually order, and which desired sizes have no match.
  const toSource = results.filter(r => r.source).map(r => ({ desired: r.size, china: r.source, remap: r.status === 'remap' }));
  const unmatched = results.filter(r => r.status === 'nofit').map(r => r.size);
  return { tolerance: disp(tol), unit: U, results, toSource, unmatched, hasTarget: targetSizes.length > 0, hasVendor: vendorSizes.length > 0 };
}

// ── Vision: read a size-chart photo into a { size: { field: cm } } chart ──
const chartUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024, files: 1 },
  fileFilter: (req, file, cb) => cb(null, /^image\//.test(file.mimetype))
});
function runChartUpload(req, res) {
  return new Promise((resolve) => {
    chartUpload.single('image')(req, res, (err) => {
      if (err) resolve({ ok: false, error: err.code === 'LIMIT_FILE_SIZE' ? 'Image is larger than 25 MB — compress it and retry.' : ('Upload error: ' + (err.message || err.code)) });
      else resolve({ ok: true });
    });
  });
}
// Minimal JSON extractor (first balanced {...} / [...] block in the reply).
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
async function extractChart(buffer, mediaType, fields) {
  const content = [
    { type: 'image', source: { type: 'base64', media_type: mediaType, data: buffer.toString('base64') } },
    { type: 'text', text:
`You are reading a clothing SIZE CHART from this image. Extract garment/body measurements for every size, in CENTIMETRES.\n` +
`The chart may be in ANY language — it is often a CHINESE vendor chart. Read Chinese (or other) text and TRANSLATE each column header to the matching English field below. Do not skip a column just because its header is not in English.\n` +
`The measurement fields we want, using EXACTLY these English keys in the output: ${fields.join(', ')}.\n` +
`Common Chinese → English column headers (match by meaning, ignore extra characters like 净/半/平铺/cm):\n` +
`  胸围/胸宽 → Chest · 肩宽 → Shoulder · 衣长/身长 → Length · 袖长 → Sleeve ·\n` +
`  腰围 → Waist · 臀围 → Hip · 内长/裤内长 → Inseam · 大腿围/腿围/髀围 → Thigh · 脚口/裤口/下摆 → Bottom hem\n` +
`Size labels (the row headers, often under 尺码/码/规格) may be XS S M L XL XXL, numeric waist 28 30 32 34, or Chinese like 均码/加大 — keep the label AS PRINTED (romanise S/M/L; leave numeric as-is).\n` +
`Values are usually already in cm. If a value is clearly in inches, convert (1 inch = 2.54 cm). If a Chinese chart gives a HALF/flat 半胸/平铺 chest, still report the number as printed. Ignore only columns with no measurement meaning (price, weight tolerance, model height suggestion).\n` +
`Return STRICT JSON ONLY: {"chart":{"<SIZE>":{"<Field>": <number in cm>, ...}, ...}} with one object per size found. No commentary.` }
  ];
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 90000);
  let r;
  try {
    r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: AI_MODEL, max_tokens: 1500, temperature: 0, messages: [{ role: 'user', content }] }),
      signal: ctrl.signal
    });
  } finally { clearTimeout(timer); }
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error((j.error && j.error.message) || ('HTTP ' + r.status));
  const parsed = extractJson((j.content || []).map(c => c.text || '').join(''));
  return (parsed && parsed.chart && typeof parsed.chart === 'object') ? parsed.chart : {};
}

// ── Routes ─────────────────────────────────────────────────────────
router.get('/api/sizetracker/config', (req, res) => {
  const s = loadStore();
  res.json({ success: true, categories: buildConfig(), tolerance: s.tolerance });
});

router.get('/api/sizetracker/targets', (req, res) => {
  const s = loadStore();
  res.json({ success: true, targets: s.targets, tolerance: s.tolerance });
});

// Save the SANKI target chart for one (category, fit).
router.post('/api/sizetracker/targets', (req, res) => {
  const b = req.body || {};
  const category = String(b.category || '').trim();
  const fit = String(b.fit || '').trim();
  if (!catExists(category)) return res.status(400).json({ success: false, error: 'Unknown category' });
  if (!fitExists(category, fit)) return res.status(400).json({ success: false, error: 'Unknown fit for this category' });
  const fields = fieldsFor(category);
  const chart = cleanChart(b.chart, fields);
  const s = loadStore();
  if (!s.targets[category]) s.targets[category] = {};
  s.targets[category][fit] = chart;
  saveStore(s);
  res.json({ success: true, targets: s.targets });
});

// Read a size-chart PHOTO into a { size: { field: cm } } chart for a category.
// Used for BOTH the SANKI target chart (Section 1) and the vendor/China chart
// (Section 2) — the caller decides what to do with the returned numbers.
router.post('/api/sizetracker/extract', async (req, res) => {
  const up = await runChartUpload(req, res);
  if (!up.ok) return res.status(400).json({ success: false, error: up.error });
  if (!req.file) return res.status(400).json({ success: false, error: 'No image uploaded' });
  const category = String((req.body && req.body.category) || '').trim();
  if (!catExists(category)) return res.status(400).json({ success: false, error: 'Unknown category' });
  if (!ANTHROPIC_API_KEY) return res.status(500).json({ success: false, error: 'Vision key not configured on the server.' });
  const fields = fieldsFor(category);
  try {
    const raw = await extractChart(req.file.buffer, req.file.mimetype || 'image/jpeg', fields);
    const chart = cleanChart(raw, fields);
    if (!Object.keys(chart).length) return res.status(422).json({ success: false, error: 'Could not read any sizes from that photo. Try a clearer, straight-on shot of the chart.' });
    res.json({ success: true, category, fields, chart });
  } catch (e) {
    res.status(502).json({ success: false, error: 'Vision read failed: ' + (e.message || e) });
  }
});

router.post('/api/sizetracker/tolerance', (req, res) => {
  const s = loadStore();
  const t = parseFloat(req.body && req.body.tolerance);
  s.tolerance = isFinite(t) && t >= 0 ? Math.round(t * 10) / 10 : DEFAULT_TOLERANCE;
  saveStore(s);
  res.json({ success: true, tolerance: s.tolerance });
});

// Compare a vendor's (China) chart to the stored target for (category, fit).
router.post('/api/sizetracker/compare', (req, res) => {
  const b = req.body || {};
  const category = String(b.category || '').trim();
  const fit = String(b.fit || '').trim();
  if (!catExists(category)) return res.status(400).json({ success: false, error: 'Unknown category' });
  if (!fitExists(category, fit)) return res.status(400).json({ success: false, error: 'Unknown fit for this category' });
  const s = loadStore();
  const fields = fieldsFor(category);
  let targetChart = (s.targets[category] && s.targets[category][fit]) || {};
  // Optional: only compare the India sizes the buyer actually wants this PO.
  if (Array.isArray(b.wanted) && b.wanted.length) {
    const want = new Set(b.wanted.map(w => String(w).trim().toUpperCase()));
    const filtered = {};
    Object.keys(targetChart).forEach(k => { if (want.has(k)) filtered[k] = targetChart[k]; });
    targetChart = filtered;
  }
  const vendorChart = cleanChart(b.vendor, fields);
  const unit = b.unit === 'in' ? 'in' : 'cm';
  // Tolerance arrives in the chosen display unit; normalise it to cm for the math.
  const tol = (() => {
    let t = parseFloat(b.tolerance);
    if (!(isFinite(t) && t >= 0)) return s.tolerance;
    if (unit === 'in') t = t * 2.54;
    return Math.round(t * 10) / 10;
  })();
  const cmp = compareChartToTarget(targetChart, vendorChart, fields, tol, unit);
  res.json({ success: true, category, fit, fields, ...cmp });
});

module.exports = { router, compareChartToTarget, fieldsFor, buildConfig };
