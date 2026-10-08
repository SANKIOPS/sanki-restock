'use strict';
const express = require('express');
const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');
const { buildReport, START, day, validDate } = require('./pnl-report');

function loadSources(directory, env = process.env) {
  const feeds = [];
  const record = value => value && typeof value === 'object' && !Array.isArray(value);
  const collection = value => Array.isArray(value) || record(value);
  function read(name, file, empty, required = true, validate = record) {
    try {
      const value = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (!record(value) || !validate(value)) throw Error('Invalid store');
      feeds.push({ name, status: 'ready', required, updatedAt: value.sync?.lastSyncedAt || null });
      return value;
    } catch (error) {
      feeds.push({ name, status: error.code === 'ENOENT' ? 'missing' : 'unreadable', required });
      return empty;
    }
  }
  return {
    orders: read('Shopify orders', env.ORDERS_PATH || path.join(directory, 'orders.json'), {}, true, v => record(v.orders)),
    expenses: read('Expenses and income', path.join(directory, 'expenses.json'), {}, true, v => record(v.expenses)),
    purchases: read('Received purchases', env.PROCUREMENT_PATH || path.join(directory, 'procurement.json'), {}, true, v => record(v.pos)),
    salary: read('Salary payments', path.join(directory, 'salary.json'), {}, true, v => record(v.employees) && collection(v.salaryPayments || [])),
    incentives: read('Sales incentive payments', env.INCENTIVES_PATH || path.join(directory, 'incentives.json'), {}, false, v => Array.isArray(v.payments)),
    manualSales: read('Manual sales', env.SALES_PATH || path.join(directory, 'sales.json'), {}, false, v => Array.isArray(v.sales)),
    opening: read('Verified opening SKU costs', path.join(directory, 'pnl-opening-inventory.json'), {}, false, v => Array.isArray(v.lots)),
    feeds
  };
}
function exportWorkbook(report) {
  const workbook = XLSX.utils.book_new();
  const summary = [
    ['SANKI P&L'], ['From', report.range.from, 'To', report.range.to], ['Basis', report.basis],
    ['Channel', report.channel], ['Result status', report.totals.complete ? 'Complete' : 'Incomplete: review warnings'], [],
    ['Particulars', 'Selected period', 'Previous period'],
    ...[['Sales including GST before discounts', 'grossSales'], ['Discounts', 'discounts'], ['Returns / refunds', 'returns'], ['GST net of returns', 'gst'], ['Net sales excluding GST', 'netSales'], ['Other business income', 'otherIncome'], ['SKU COGS', 'cogs'], ['Gross profit', 'grossProfit'], ['Paid operating expenses', 'expenses'], ['Management profit / loss', 'netProfit']].map(([label, field]) => [label, report.totals[field] ?? 'Unavailable', report.previous?.totals[field] ?? 'Unavailable']),
    [], ['Expense group', 'Category', 'Paid amount', 'Payment count'], ...report.categories.map(r => [r.group, r.category, r.amount, r.count])
  ];
  const sheets = {
    Summary: summary,
    Transactions: [['Date', 'Expense date', 'Type', 'Channel', 'Category', 'Particulars', 'Reference', 'Account', 'Method', 'Gross sales', 'Discounts', 'Returns', 'GST', 'Net sales', 'Other income', 'Known COGS', 'Paid expense', 'Tax complete', 'Cost complete', 'Proof'], ...report.transactions.map(r => [r.date, r.expenseDate || '', r.kind, r.channel, r.category, r.particulars, r.reference, r.account, r.method, r.grossSales, r.discounts, r.returns, r.taxComplete ? r.gst : 'Unavailable', r.netSales ?? 'Unavailable', r.otherIncome, r.cogs, r.expenses, r.taxComplete, r.costComplete, r.proof])],
    'SKU allocations': [['Date', 'Reference', 'SKU', 'Quantity', 'Purchase batch', 'Allocated quantity', 'Unit cost', 'Allocated cost'], ...report.transactions.flatMap(r => (r.details || []).flatMap(d => (d.allocations || []).map(a => [r.date, r.reference, d.sku, d.qty, a.purchaseId, a.qty, a.unitCost, a.amount])))],
    Tax: [['Date', 'Reference', 'SKU', 'Quantity', 'Channel', 'Inclusive amount', 'Recorded GST', 'Calculated GST', 'Included GST', 'Rate', 'Status'], ...report.taxes.map(r => [r.date, r.reference, r.sku, r.qty, r.channel, r.gross, r.recorded ?? 'Not provided', r.calculated ?? 'Unavailable', r.tax ?? 'Unavailable', r.rate ?? '', r.status])],
    Collections: [['Order', 'Date', 'Channel', 'Order value', 'Collected from customer', 'Cash refunded', 'Uncollected balance', 'Customer advance held', 'Status'], ...report.collections.map(r => [r.reference, r.date, r.channel, r.orderValue, r.collected ?? 'Unavailable', r.cashRefunded, r.balanceToCollect ?? 'Unavailable', r.customerAdvance ?? 'Unavailable', r.status])],
    Warnings: [['Reference', 'Date', 'Area', 'Warning'], ...report.warnings.map(r => [r.reference, r.date || '', r.scope, r.message])],
    Policy: Object.entries(report.policy).map(([name, description]) => [name, description])
  };
  for (const [name, rows] of Object.entries(sheets)) {
    const sheet = XLSX.utils.aoa_to_sheet(rows);
    sheet['!cols'] = Array.from({ length: Math.max(...rows.map(r => r.length)) }, (_, i) => ({ wch: i === 0 ? 36 : 22 }));
    if (sheet['!ref']) for (const [address, cell] of Object.entries(sheet)) {
      if (!address.startsWith('!') && cell.t === 'n') cell.z = '#,##0.00';
      // aoa_to_sheet stores strings as strings, never interpreted formulas.
    }
    XLSX.utils.book_append_sheet(workbook, sheet, name);
  }
  return XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
}
function createRouter({ read = () => loadSources(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..')), clock = () => new Date() } = {}) {
  const router = express.Router();
  function report(req) {
    const today = day(clock().toISOString()), month = today.slice(0, 7) + '-01';
    const from = String(req.query.from === '' ? START : req.query.from || month), to = String(req.query.to || today);
    if (!validDate(from) || !validDate(to) || to > today) throw Object.assign(new Error('Choose valid dates ending today or earlier.'), { status: 400 });
    const sources = read();
    return buildReport(sources, { from, to, start: START, channel: String(req.query.channel || 'All') });
  }
  router.get('/api/pl/report', (req, res) => {
    try { res.set('Cache-Control', 'no-store').json(report(req)); }
    catch (error) { res.status(error.status || 500).json({ success: false, error: error.status ? error.message : 'P&L could not be read safely. No accounting records were changed.' }); }
  });
  router.get('/api/pl/report/export', (req, res) => {
    try {
      const data = report(req);
      res.set('Cache-Control', 'no-store');
      res.set('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.set('Content-Disposition', 'attachment; filename="SANKI_PnL_' + data.range.from + '_' + data.range.to + '.xlsx"');
      res.send(exportWorkbook(data));
    } catch (error) { res.status(error.status || 500).json({ success: false, error: error.status ? error.message : 'P&L export could not be prepared. No accounting records were changed.' }); }
  });
  return router;
}
module.exports = { loadSources, exportWorkbook, createRouter };
