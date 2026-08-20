'use strict';
// Salary + attendance — the payroll side of in-app accounting.
// Model (replicates the founder's "JUL SALARY" sheet exactly):
//   Salary Amt  = monthly salary ÷ 30 × Paid Days   (30 is fixed, not calendar days)
//   Net Payable = Salary Amt − Advance
//   Balance     = Net Payable − Paid                 (over/under-payment carries)
// Paid Days can EXCEED 30 (overtime / extra days) and is editable per month.
// Attendance (manager-marked) auto-suggests Paid Days but the payroll row can
// always override it. Salary is CONFIDENTIAL → every route is admin/accounting only.
// Salary Amt (earned) feeds the P&L Fixed Cost row, channel-tagged (POS/Website/
// Shared), Shared split by revenue share like other shared costs.
const express = require('express');
const fs = require('fs');
const path = require('path');
const router = express.Router();

const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const SAL_PATH = path.join(DATA_DIR, 'salary.json');

function num(v) { const n = parseFloat(v); return isNaN(n) ? 0 : n; }
function round0(n) { return Math.round(n); }
function round2(n) { return Math.round(n * 100) / 100; }

const CHANNELS = ['POS', 'Website', 'Shared'];
// Paid-day value per attendance mark: Present 1, Half 0.5, Paid-leave 1,
// Week-off 1 (paid), Absent 0.
const MARKS = { P: 1, H: 0.5, PL: 1, WO: 1, A: 0 };

function blank() { return { employees: {}, months: {}, divisor: 30, seq: 0 }; }
function load() { try { return Object.assign(blank(), JSON.parse(fs.readFileSync(SAL_PATH, 'utf8'))); } catch { return blank(); } }
function save(s) { const tmp = SAL_PATH + '.tmp-' + process.pid + '-' + Date.now(); fs.writeFileSync(tmp, JSON.stringify(s)); fs.renameSync(tmp, SAL_PATH); }

// ── Access: salary is sensitive → admin or accounting only ──
function rolesOf(req) { return (req.user && (req.user.roles || (req.user.role ? [req.user.role] : []))) || []; }
function guard(req, res, next) {
  const r = rolesOf(req);
  if (r.includes('admin') || r.includes('accounting') || r.includes('owner')) return next();
  return res.status(403).json({ success: false, error: 'Salary is admin/accounting only.' });
}

function daysInMonth(ym) { const p = String(ym).split('-').map(Number); return new Date(p[0], p[1], 0).getDate(); }
function attPaidDays(att) {
  if (!att) return null;
  let sum = 0, any = false;
  Object.keys(att).forEach(d => { const v = MARKS[att[d]]; if (v != null) { sum += v; any = true; } });
  return any ? sum : null;
}
function ensureMonth(s, ym) { if (!s.months[ym]) s.months[ym] = { finalized: false, rows: {}, attendance: {} }; return s.months[ym]; }

// Compute a month's payroll rows for every employee.
function computeMonth(s, ym) {
  const mo = s.months[ym] || { rows: {}, attendance: {} };
  const div = num(s.divisor) || 30;
  return Object.values(s.employees).map(e => {
    const row = (mo.rows || {})[e.id] || {};
    const computed = attPaidDays((mo.attendance || {})[e.id]);
    const paidDays = row.paidDays != null ? num(row.paidDays) : computed;   // override → attendance → null
    const salaryAmt = paidDays != null ? (num(e.salary) / div * paidDays) : 0;
    const advance = num(row.advance);
    const netPayable = salaryAmt - advance;
    const paid = num(row.paid);
    return {
      id: e.id, name: e.name, post: e.post, channel: e.channel, active: e.active !== false,
      salary: num(e.salary), paidDays, computedPaidDays: computed,
      salaryAmt: round2(salaryAmt), advance, netPayable: round2(netPayable),
      paid, balance: round2(netPayable - paid), remarks: row.remarks || ''
    };
  });
}

// ── P&L feed: earned salary (Salary Amt) by channel for a date range ──
// A month contributes its salary pro-rated by how many of its calendar days fall
// inside [from,to] (full month = 100%). Shared is returned separately so pl.js can
// split it by revenue share, exactly like other shared fixed costs.
function summaryForPL(from, to) {
  const s = load();
  const buckets = { POS: 0, Website: 0, Shared: 0 };
  Object.keys(s.months || {}).forEach(ym => {
    const dim = daysInMonth(ym);
    const mStart = ym + '-01', mEnd = ym + '-' + String(dim).padStart(2, '0');
    const lo = (from && from > mStart) ? from : mStart;
    const hi = (to && to < mEnd) ? to : mEnd;
    if (lo > hi) return;
    const overlapDays = (Date.parse(hi) - Date.parse(lo)) / 86400000 + 1;
    const frac = Math.max(0, Math.min(1, overlapDays / dim));
    if (frac <= 0) return;
    computeMonth(s, ym).forEach(r => {
      const ch = CHANNELS.includes(r.channel) ? r.channel : 'Shared';
      buckets[ch] += r.salaryAmt * frac;
    });
  });
  return { POS: round0(buckets.POS), Website: round0(buckets.Website), Shared: round0(buckets.Shared) };
}

// ── Employee master ──
router.get('/api/salary/employees', guard, (req, res) => {
  const s = load();
  res.json({ success: true, employees: Object.values(s.employees), divisor: num(s.divisor) || 30, channels: CHANNELS });
});
router.post('/api/salary/employees', guard, (req, res) => {
  const s = load(); const b = req.body || {};
  let id = b.id;
  if (!id) { s.seq = (s.seq || 0) + 1; id = 'E' + String(s.seq).padStart(3, '0'); }
  const cur = s.employees[id] || {};
  s.employees[id] = {
    id,
    name: String(b.name != null ? b.name : cur.name || '').trim(),
    post: String(b.post != null ? b.post : cur.post || '').trim(),
    salary: b.salary != null ? num(b.salary) : num(cur.salary),
    channel: CHANNELS.includes(b.channel) ? b.channel : (cur.channel || 'Shared'),
    note: b.note != null ? String(b.note) : (cur.note || ''),
    active: b.active != null ? !!b.active : (cur.active !== false),
    createdAt: cur.createdAt || new Date().toISOString()
  };
  save(s);
  res.json({ success: true, employee: s.employees[id] });
});
router.delete('/api/salary/employees/:id', guard, (req, res) => {
  const s = load();
  if (s.employees[req.params.id]) { s.employees[req.params.id].active = false; save(s); }
  res.json({ success: true });
});

// ── A month: computed rows + attendance + totals ──
router.get('/api/salary/month/:ym', guard, (req, res) => {
  const ym = req.params.ym; const s = load();
  const rows = computeMonth(s, ym);
  const mo = s.months[ym] || { finalized: false, attendance: {} };
  const totals = rows.reduce((t, r) => {
    t.salary += r.salary; t.salaryAmt += r.salaryAmt; t.advance += r.advance;
    t.netPayable += r.netPayable; t.paid += r.paid; t.balance += r.balance; return t;
  }, { salary: 0, salaryAmt: 0, advance: 0, netPayable: 0, paid: 0, balance: 0 });
  Object.keys(totals).forEach(k => totals[k] = round2(totals[k]));
  res.json({ success: true, ym, divisor: num(s.divisor) || 30, daysInMonth: daysInMonth(ym), finalized: !!mo.finalized, rows, attendance: mo.attendance || {}, totals });
});

// Mark one attendance cell. mark='' clears it.
router.post('/api/salary/attendance/:ym', guard, (req, res) => {
  const s = load(); const mo = ensureMonth(s, req.params.ym); const b = req.body || {};
  if (b.empId && b.day) {
    mo.attendance[b.empId] = mo.attendance[b.empId] || {};
    if (b.mark && MARKS[b.mark] != null) mo.attendance[b.empId][b.day] = b.mark;
    else delete mo.attendance[b.empId][b.day];
  }
  save(s);
  res.json({ success: true });
});

// Edit a payroll row (paidDays override / advance / paid / remarks).
router.post('/api/salary/row/:ym', guard, (req, res) => {
  const s = load(); const mo = ensureMonth(s, req.params.ym); const b = req.body || {};
  if (!b.empId) return res.status(400).json({ success: false, error: 'empId required' });
  const row = mo.rows[b.empId] = mo.rows[b.empId] || {};
  if (b.paidDays !== undefined) row.paidDays = (b.paidDays === '' || b.paidDays === null) ? null : num(b.paidDays);
  if (b.advance !== undefined) row.advance = num(b.advance);
  if (b.paid !== undefined) row.paid = num(b.paid);
  if (b.remarks !== undefined) row.remarks = String(b.remarks);
  save(s);
  res.json({ success: true });
});

router.post('/api/salary/divisor', guard, (req, res) => {
  const s = load(); const d = num((req.body || {}).divisor);
  if (d > 0) s.divisor = d;
  save(s);
  res.json({ success: true, divisor: s.divisor });
});

// ── One-time seed from the founder's JUL SALARY sheet (21 staff + July run) ──
// Runs only when no salary.json exists yet, so it never clobbers real edits.
function seedIfEmpty() {
  if (fs.existsSync(SAL_PATH)) return;
  // [name, post, salary, channel, paidDays, advance, paid, remarks]  (July 2026)
  const SEED = [
    ['ARSHPREET SINGH', 'MANAGER', 30000, 'Shared', 30, 52000, 0, '2000 Last Month + 50000 This Month'],
    ['Guard', 'Security', 15000, 'Shared', 30, 3000, 12000, ''],
    ['SUNNY SHARMA', 'SALES EXECUTIVE', 24000, 'POS', 22, 20000, 0, '7000 Last Month'],
    ['PARDEEP', 'EXECUTIVE', 19000, 'Shared', 21.5, 0, 13500, ''],
    ['NANDANI', 'SALES EXECUTIVE', 17000, 'POS', 29, 0, 16500, ''],
    ['SHIVAM', 'SALES EXECUTIVE', 17000, 'POS', 30, 1500, 15500, ''],
    ['ISHA', 'SALES EXECUTIVE', 18000, 'POS', 30, 100, 18000, ''],
    ['HITESH', 'PHOTOGRAPHER', 20000, 'Website', 27, 0, 18000, ''],
    ['AJAY', 'VIDEO EDITOR', 18000, 'Website', 29, 0, 17500, 'Salary Delay'],
    ['PIYUSH', 'TAILOR', 18500, 'Shared', 28, 0, 0, ''],
    ['UMAIR', 'DRIVER', 28000, 'Shared', 27.5, 17000, 9000, ''],
    ['TUSHAR(INV)', 'PACKING HELPER', 15000, 'Shared', 31, 2000, 13500, ''],
    ['NIDA', 'LOGISTIC', 25000, 'Shared', 29, 0, 24000, ''],
    ['GAURAV', 'EDITOR', 18500, 'Website', 28.5, 0, 17500, ''],
    ['PRASHANT', 'ACCOUNTS', 26000, 'Shared', 29.5, 17467, 13000, '7467 Last Month + 10000 This Month'],
    ['Suraj(OB)', 'Office Boy', 18000, 'Shared', 25, 1500, 13500, ''],
    ['TUSHAR(MOD)', 'Model', 28000, 'Website', 30, 0, 28000, ''],
    ['Indervir', 'ACCOUNTS', 17000, 'Shared', 28.5, 4000, 12000, ''],
    ['CSH', 'MARKETING', 15000, 'Website', null, 0, 0, 'Confirmation'],
    ['Ravi', 'DRIVER', 26000, 'Shared', 9, 6500, 4000, ''],
    ['Pooja', 'MODEL', 20000, 'Website', 10, 0, 6500, '']
  ];
  const s = blank();
  const now = new Date().toISOString();
  const rows = {};
  SEED.forEach((r, i) => {
    s.seq = i + 1;
    const id = 'E' + String(s.seq).padStart(3, '0');
    s.employees[id] = { id, name: r[0], post: r[1], salary: r[2], channel: r[3], note: r[7] || '', active: true, createdAt: now };
    rows[id] = { paidDays: r[4], advance: r[5], paid: r[6], remarks: r[7] || '' };
  });
  s.months['2026-07'] = { finalized: false, rows, attendance: {} };
  try { save(s); } catch (_) { /* volume not ready → skip; endpoints will seed lazily via load() default */ }
}
seedIfEmpty();

module.exports = { router, summaryForPL };
