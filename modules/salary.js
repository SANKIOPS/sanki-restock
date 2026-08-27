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
function byEmployeeName(a, b) { return String(a.name || a.employeeName || '').localeCompare(String(b.name || b.employeeName || ''), 'en', { sensitivity:'base', numeric:true }); }

const CHANNELS = ['POS', 'Website', 'Shared'];
const WEEK_DAYS = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
// Paid-day value per attendance mark: Present 1, Half 0.5, Paid-leave 1,
// Week-off 1 (paid), Absent 0.
const MARKS = { P: 1, H: 0.5, PL: 1, WO: 1, A: 0 };

function blank() { return { employees: {}, months: {}, divisor: 30, seq: 0, advances: {}, advanceSeq: 0, advanceAudit: [], payrollPostings:{}, salaryPayments:[], salaryPaymentBatchSeq:0 }; }
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
function employmentAttendance(att, e, ym) {
  if (!att) return att;
  const filtered={}; Object.keys(att).forEach(day=>{const date=ym+'-'+String(day).padStart(2,'0');if(e.joiningDate&&date<e.joiningDate)return;if(e.lastWorkingDate&&date>e.lastWorkingDate)return;filtered[day]=att[day];}); return filtered;
}
function ensureMonth(s, ym) { if (!s.months[ym]) s.months[ym] = { finalized: false, rows: {}, attendance: {} }; return s.months[ym]; }
function advanceRecovered(a) { return round2((a.recoveries || []).reduce((n, x) => n + num(x.amount), 0)); }
function advanceOutstanding(a) { return round2(Math.max(0, num(a.amount) - advanceRecovered(a))); }
function advanceStatus(a) { const r = advanceRecovered(a); return r <= 0 ? 'Outstanding' : (r + .001 >= num(a.amount) ? 'Recovered' : 'Partially recovered'); }
function advanceView(a) { return Object.assign({}, a, { recovered: advanceRecovered(a), outstanding: advanceOutstanding(a), status: a.active === false ? 'Cancelled' : advanceStatus(a) }); }
function monthRecovery(s, empId, ym) { return round2(Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === empId).reduce((n, a) => n + (a.recoveries || []).filter(r => r.ym === ym).reduce((m, r) => m + num(r.amount), 0), 0)); }
function auditAdvance(s, req, action, advanceId, details) { s.advanceAudit = s.advanceAudit || []; s.advanceAudit.push({ at: new Date().toISOString(), by: req.user && req.user.username || 'admin', action, advanceId, details: details || {} }); }

// Compute a month's payroll rows for every employee.
function computeMonth(s, ym) {
  const mo = s.months[ym] || { rows: {}, attendance: {} };
  const div = num(s.divisor) || 30;
  return Object.values(s.employees).sort(byEmployeeName).map(e => {
    const row = (mo.rows || {})[e.id] || {};
    const computed = attPaidDays(employmentAttendance((mo.attendance || {})[e.id],e,ym));
    const paidDays = row.paidDays != null ? num(row.paidDays) : computed;   // override → attendance → null
    const salaryAmt = paidDays != null ? (num(e.salary) / div * paidDays) : 0;
    const legacyAdvance = num(row.advance);
    const loggedAdvanceRecovery = monthRecovery(s, e.id, ym);
    const advance = round2(legacyAdvance + loggedAdvanceRecovery);
    const netPayable = salaryAmt - advance;
    const legacyPaid=num(row.paid),transactionPaid=round2((s.salaryPayments||[]).filter(p=>p.empId===e.id&&p.ym===ym&&p.active!==false).reduce((n,p)=>n+num(p.amount),0)),paid=round2(legacyPaid+transactionPaid);
    return {
      id: e.id, name: e.name, post: e.post, channel: e.channel, weekOffDay: e.weekOffDay || '', joiningDate:e.joiningDate||'', lastWorkingDate:e.lastWorkingDate||'', active: e.active !== false,
      salary: num(e.salary), paidDays, computedPaidDays: computed,
      salaryAmt: round2(salaryAmt), advance, legacyAdvance, loggedAdvanceRecovery, netPayable: round2(netPayable),
      outstandingAdvance: round2(Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === e.id).reduce((n, a) => n + advanceOutstanding(a), 0)),
      paid, legacyPaid, transactionPaid, balance: round2(netPayable - paid), remarks: row.remarks || ''
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
  res.json({ success: true, employees: Object.values(s.employees).sort(byEmployeeName), divisor: num(s.divisor) || 30, channels: CHANNELS, weekDays: WEEK_DAYS });
});
router.post('/api/salary/employees', guard, (req, res) => {
  const s = load(); const b = req.body || {};
  let id = b.id;
  if (!id) { s.seq = (s.seq || 0) + 1; id = 'E' + String(s.seq).padStart(3, '0'); }
  const cur = s.employees[id] || {};
  const joiningDate=b.joiningDate!==undefined?String(b.joiningDate||'').slice(0,10):(cur.joiningDate||''),lastWorkingDate=b.lastWorkingDate!==undefined?String(b.lastWorkingDate||'').slice(0,10):(cur.lastWorkingDate||'');
  if((joiningDate&&!/^\d{4}-\d{2}-\d{2}$/.test(joiningDate))||(lastWorkingDate&&!/^\d{4}-\d{2}-\d{2}$/.test(lastWorkingDate)))return res.status(400).json({success:false,error:'Use valid joining and last-working dates.'});
  if(joiningDate&&lastWorkingDate&&lastWorkingDate<joiningDate)return res.status(400).json({success:false,error:'Last working date cannot be before the joining date.'});
  s.employees[id] = {
    id,
    name: String(b.name != null ? b.name : cur.name || '').trim(),
    post: String(b.post != null ? b.post : cur.post || '').trim(),
    salary: b.salary != null ? num(b.salary) : num(cur.salary),
    channel: CHANNELS.includes(b.channel) ? b.channel : (cur.channel || 'Shared'),
    weekOffDay: b.weekOffDay !== undefined ? (WEEK_DAYS.includes(b.weekOffDay) ? b.weekOffDay : '') : (cur.weekOffDay || ''),
    joiningDate, lastWorkingDate,
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

router.post('/api/salary/post/:ym', guard, (req,res)=>{
  const s=load(),ym=req.params.ym;if(!/^\d{4}-\d{2}$/.test(ym))return res.status(400).json({success:false,error:'Invalid payroll month.'});
  s.payrollPostings=s.payrollPostings||{};if(s.payrollPostings[ym])return res.status(409).json({success:false,error:'This month is already posted to salary ledgers.'});
  const rows=computeMonth(s,ym).filter(r=>r.salaryAmt||r.advance||r.paid).map(r=>({empId:r.id,employeeName:r.name,salaryAmt:r.salaryAmt,advanceRecovery:r.loggedAdvanceRecovery,legacyAdvance:r.legacyAdvance,legacyPaid:r.legacyPaid,paid:r.paid,netPayable:r.netPayable}));
  s.payrollPostings[ym]={ym,rows,postedAt:new Date().toISOString(),postedBy:req.user&&req.user.username||'admin'};save(s);res.json({success:true,posting:s.payrollPostings[ym]});
});
router.post('/api/salary/payments/batch',guard,(req,res)=>{
  const s=load(),b=req.body||{},ym=String(b.ym||''),date=String(b.date||''),account=String(b.account||'').trim(),proof=String(b.proof||'').trim(),items=Array.isArray(b.items)?b.items:[];
  if(!/^\d{4}-\d{2}$/.test(ym)||!/^\d{4}-\d{2}-\d{2}$/.test(date)||!account||!proof||!items.length)return res.status(400).json({success:false,error:'Choose employees, date, paying account and payment proof.'});
  const rows=computeMonth(s,ym),seen=new Set(),prepared=[];for(const x of items){const row=rows.find(r=>r.id===x.empId),amount=round2(num(x.amount));if(!row||seen.has(x.empId)||!(amount>0)||amount>Math.max(0,row.balance)+.001)return res.status(400).json({success:false,error:'A payment is invalid or exceeds the employee’s remaining payable balance.'});seen.add(x.empId);prepared.push({row,amount});}
  s.salaryPaymentBatchSeq=(s.salaryPaymentBatchSeq||0)+1;const batchId='SALB-'+String(s.salaryPaymentBatchSeq).padStart(5,'0'),now=new Date().toISOString();s.salaryPayments=s.salaryPayments||[];prepared.forEach((x,i)=>s.salaryPayments.push({id:batchId+'-'+String(i+1).padStart(3,'0'),batchId,ym,empId:x.row.id,employeeName:x.row.name,amount:x.amount,date,account,proof,reference:String(b.reference||'').trim(),note:String(b.note||'').trim(),active:true,createdBy:req.user&&req.user.username||'admin',createdAt:now}));save(s);res.json({success:true,batchId,count:prepared.length,total:round2(prepared.reduce((n,x)=>n+x.amount,0))});
});
router.get('/api/salary/ledgers',guard,(req,res)=>{
  const s=load(),by={};const ensure=(id,name)=>by[id]||(by[id]={empId:id,name,ledgerName:(name||id)+' — Salary',entries:[]});
  Object.values(s.advances||{}).filter(a=>a.active!==false).forEach(a=>ensure(a.empId,a.employeeName).entries.push({id:a.id,date:a.date,kind:'advance',description:'Salary advance paid'+(a.note?' · '+a.note:''),debit:num(a.amount),credit:0,proof:a.proof||'',reference:a.reference||a.id}));
  (s.salaryPayments||[]).filter(p=>p.active!==false).forEach(p=>ensure(p.empId,p.employeeName).entries.push({id:p.id,date:p.date,kind:'salary_paid',description:p.ym+' salary payment'+(p.note?' · '+p.note:''),debit:num(p.amount),credit:0,proof:p.proof||'',reference:p.reference||p.batchId}));
  Object.values(s.payrollPostings||{}).forEach(p=>(p.rows||[]).forEach(r=>{const l=ensure(r.empId,r.employeeName),date=p.ym+'-'+String(daysInMonth(p.ym)).padStart(2,'0');if(num(r.salaryAmt))l.entries.push({id:p.ym+'/'+r.empId+'/EARNED',date,kind:'salary_earned',description:p.ym+' salary earned',debit:0,credit:num(r.salaryAmt),reference:p.ym});if(num(r.legacyPaid))l.entries.push({id:p.ym+'/'+r.empId+'/LEGACY-PAID',date,kind:'legacy_salary_paid',description:p.ym+' legacy paid amount',debit:num(r.legacyPaid),credit:0,reference:p.ym});}));
  const ledgers=Object.values(by).map(l=>{l.entries.sort((a,b)=>String(a.date+a.id).localeCompare(String(b.date+b.id)));let balance=0;l.entries.forEach(e=>{balance+=num(e.credit)-num(e.debit);e.balance=round2(balance);});l.balance=round2(balance);l.status=balance>0?'Company owes':balance<0?'Employee owes':'Settled';l.lastPostingDate=l.entries.at(-1)&&l.entries.at(-1).date||'';l.outstandingAdvance=round2(Object.values(s.advances||{}).filter(a=>a.active!==false&&a.empId===l.empId).reduce((n,a)=>n+advanceOutstanding(a),0));l.entries=l.entries.slice().reverse();return l;}).sort(byEmployeeName);
  res.json({success:true,ledgers});
});

// Salary advances are recoverable employee balances, not salary/P&L expenses.
router.get('/api/salary/advances', guard, (req, res) => {
  const s = load(), q = req.query || {};
  let rows = Object.values(s.advances || {}).map(advanceView);
  if (q.employee) rows = rows.filter(a => a.empId === q.employee);
  if (q.month) rows = rows.filter(a => String(a.date || '').slice(0, 7) === q.month);
  if (q.status) rows = rows.filter(a => a.status === q.status);
  if (q.account) rows = rows.filter(a => a.account === q.account);
  rows.sort((a, b) => String(b.date + b.id).localeCompare(String(a.date + a.id)));
  const summary = Object.values(s.employees).sort(byEmployeeName).map(e => {
    const all = Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === e.id);
    const total = all.reduce((n, a) => n + num(a.amount), 0), recovered = all.reduce((n, a) => n + advanceRecovered(a), 0);
    return { empId: e.id, name: e.name, thisMonth: all.filter(a => String(a.date).slice(0, 7) === (q.summaryMonth || new Date().toISOString().slice(0, 7))).reduce((n, a) => n + num(a.amount), 0), total: round2(total), recovered: round2(recovered), outstanding: round2(total - recovered), transactions: all.map(advanceView).sort((a,b)=>String(b.date+b.id).localeCompare(String(a.date+a.id))) };
  }).filter(x => x.total || x.recovered);
  const totals = summary.reduce((t, x) => ({ total: t.total + x.total, recovered: t.recovered + x.recovered, outstanding: t.outstanding + x.outstanding }), { total: 0, recovered: 0, outstanding: 0 });
  res.json({ success: true, advances: rows, summary, totals, audit: (s.advanceAudit || []).slice().reverse().slice(0, 500) });
});

router.post('/api/salary/advances', guard, (req, res) => {
  const s = load(), b = req.body || {}, emp = s.employees[b.empId], amount = num(b.amount);
  if (!emp) return res.status(400).json({ success: false, error: 'Select an employee.' });
  if (!(amount > 0)) return res.status(400).json({ success: false, error: 'Enter a valid advance amount.' });
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(b.date || ''))) return res.status(400).json({ success: false, error: 'Select the payment date.' });
  if (!String(b.account || '').trim()) return res.status(400).json({ success: false, error: 'Select the paying account.' });
  if (!String(b.proof || '').trim()) return res.status(400).json({ success: false, error: 'Payment proof is required.' });
  const recoveryStartMonth = String(b.recoveryStartMonth || b.date.slice(0, 7));
  if (!/^\d{4}-\d{2}$/.test(recoveryStartMonth)) return res.status(400).json({ success: false, error: 'Select a recovery start month.' });
  s.advanceSeq = (s.advanceSeq || 0) + 1; const id = 'ADV-' + String(s.advanceSeq).padStart(5, '0'), now = new Date().toISOString();
  s.advances[id] = { id, empId: emp.id, employeeName: emp.name, amount: round2(amount), date: b.date, account: String(b.account).trim(), proof: String(b.proof).trim(), note: String(b.note || '').trim(), reference: String(b.reference || '').trim(), recoveryStartMonth, recoveries: [], active: true, createdBy: req.user && req.user.username || 'admin', createdAt: now };
  auditAdvance(s, req, 'CREATED', id, { amount, account: b.account }); save(s);
  res.json({ success: true, advance: advanceView(s.advances[id]) });
});

router.post('/api/salary/recoveries/:ym', guard, (req, res) => {
  const s = load(), b = req.body || {}, ym = req.params.ym, amount = num(b.amount);
  if (!s.employees[b.empId] || !/^\d{4}-\d{2}$/.test(ym) || amount < 0) return res.status(400).json({ success: false, error: 'Invalid employee, month or amount.' });
  const eligible = Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === b.empId && a.recoveryStartMonth <= ym && String(a.date).slice(0, 7) <= ym).sort((a,b)=>String(a.date+a.id).localeCompare(String(b.date+b.id)));
  eligible.forEach(a => { a.recoveries = (a.recoveries || []).filter(r => r.ym !== ym); });
  const available = eligible.reduce((n, a) => n + advanceOutstanding(a), 0);
  if (amount > available + .001) return res.status(400).json({ success: false, error: 'Recovery cannot exceed the eligible outstanding advance of ₹' + round2(available) + '.' });
  let left = amount; eligible.forEach(a => { if (left <= 0) return; const take = Math.min(left, advanceOutstanding(a)); if (take > 0) { a.recoveries.push({ ym, amount: round2(take), by: req.user && req.user.username || 'admin', at: new Date().toISOString() }); left = round2(left - take); } });
  auditAdvance(s, req, 'RECOVERY_SET', '', { empId: b.empId, ym, amount }); save(s); res.json({ success: true, amount: round2(amount) });
});

router.post('/api/salary/advances/:id/cancel', guard, (req, res) => {
  const s = load(), a = (s.advances || {})[req.params.id], reason = String((req.body || {}).reason || '').trim();
  if (!a || a.active === false) return res.status(404).json({ success: false, error: 'Advance not found.' });
  if (!reason) return res.status(400).json({ success: false, error: 'A cancellation reason is required.' });
  if (advanceRecovered(a) > 0) return res.status(400).json({ success: false, error: 'Reverse its payroll recoveries before cancelling this advance.' });
  a.active = false; a.cancelledAt = new Date().toISOString(); a.cancelledBy = req.user && req.user.username || 'admin'; a.cancelReason = reason; auditAdvance(s, req, 'CANCELLED', a.id, { reason }); save(s); res.json({ success: true });
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
    let mark = b.mark;
    const emp=s.employees[b.empId],dateText=req.params.ym+'-'+String(b.day).padStart(2,'0'),date=new Date(dateText+'T00:00:00Z'),weekday=!isNaN(date)?WEEK_DAYS[date.getUTCDay()]:'';
    if(emp&&((emp.joiningDate&&dateText<emp.joiningDate)||(emp.lastWorkingDate&&dateText>emp.lastWorkingDate)))return res.status(400).json({success:false,error:'This date is outside the employee’s employment period.'});
    if(mark==='A'&&emp&&emp.weekOffDay&&emp.weekOffDay===weekday)mark='WO';
    if (mark && MARKS[mark] != null) mo.attendance[b.empId][b.day] = mark;
    else delete mo.attendance[b.empId][b.day];
    b.savedMark=mark;
  }
  save(s);
  res.json({ success: true, mark:b.savedMark || '' });
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
