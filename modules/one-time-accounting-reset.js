'use strict';

const fs = require('fs');
const path = require('path');

const CUTOFF_DAY = '2026-08-22';
const CUTOFF_AT = '2026-08-22T00:00:00+05:30';

function read(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; }
}
function write(file, value) {
  const tmp = file + '.tmp-' + process.pid + '-' + Date.now();
  fs.writeFileSync(tmp, JSON.stringify(value));
  fs.renameSync(tmp, file);
}
function dayOf(value) { return String(value || '').slice(0, 10); }
function keep(value) { return dayOf(value) >= CUTOFF_DAY; }
function referencedProofNames(value, out) {
  if (typeof value === 'string' && value.includes('/api/expenses/photo/')) out.add(path.basename(value));
  else if (Array.isArray(value)) value.forEach(x => referencedProofNames(x, out));
  else if (value && typeof value === 'object') Object.values(value).forEach(x => referencedProofNames(x, out));
}

function runOneTimeAccountingReset() {
  const dataDir = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
  const marker = path.join(dataDir, 'reset-complete-2026-08-22.json');
  if (fs.existsSync(marker)) return { ran: false };

  const expensesFile = path.join(dataDir, 'expenses.json');
  const expenses = read(expensesFile, null);
  if (expenses) {
    expenses.expenses = Object.fromEntries(Object.entries(expenses.expenses || {}).filter(([, x]) => keep(x.date || x.createdAt)));
    expenses.receivables = Object.fromEntries(Object.entries(expenses.receivables || {}).filter(([, x]) => keep(x.date || x.createdAt)));
    expenses.adjustments = (expenses.adjustments || []).filter(x => keep(x.date || x.createdAt));
    expenses.transfers = (expenses.transfers || []).filter(x => keep(x.date || x.createdAt));
    expenses.requests = (expenses.requests || []).filter(x => keep(x.at));
    expenses.openingBalances = {};
    expenses.openingBalancesByNature = { SAMAST: {}, PERSONAL: {} };
    if (expenses.procurementAccounting) {
      expenses.procurementAccounting.trackPostedFrom = CUTOFF_AT;
      expenses.procurementAccounting.paymentsByPo = {};
    }
    write(expensesFile, expenses);

    const retained = new Set(); referencedProofNames(expenses, retained);
    const proofDir = path.join(dataDir, 'expense-proofs');
    try { fs.readdirSync(proofDir).forEach(name => { if (!retained.has(name)) fs.unlinkSync(path.join(proofDir, name)); }); } catch { /* no proof folder */ }
  }

  const salesFile = process.env.SALES_PATH || path.join(dataDir, 'sales.json');
  const sales = read(salesFile, null);
  if (sales) { sales.sales = (sales.sales || []).filter(x => keep(x.day || x.date || x.ts)); write(salesFile, sales); }

  const salaryFile = path.join(dataDir, 'salary.json');
  const salary = read(salaryFile, null);
  if (salary) {
    salary.months = Object.fromEntries(Object.entries(salary.months || {}).filter(([ym]) => ym >= CUTOFF_DAY.slice(0, 7)));
    const month = salary.months[CUTOFF_DAY.slice(0, 7)];
    if (month) {
      month.rows = {};
      Object.values(month.attendance || {}).forEach(att => Object.keys(att || {}).forEach(d => { if (Number(d) < 22) delete att[d]; }));
      month.finalized = false;
    }
    write(salaryFile, salary);
  }

  write(path.join(dataDir, 'accounting-boundary.json'), { startAt: CUTOFF_AT, resetAt: new Date().toISOString(), reason: 'Owner-confirmed fresh accounting start' });
  write(marker, { cutoffDay: CUTOFF_DAY, completedAt: new Date().toISOString() });
  return { ran: true, cutoffDay: CUTOFF_DAY };
}

module.exports = { runOneTimeAccountingReset };
