'use strict';
const express = require('express');
const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');
const { buildReport, START, day, validDate } = require('./pnl-report');
const lineCorrections = require('./pnl-line-corrections');

function loadSources(directory, env = process.env) {
  const feeds = [];
  const record = value => value && typeof value === 'object' && !Array.isArray(value);
  const collection = value => Array.isArray(value) || record(value);
  const orderDirectory = path.dirname(env.ORDERS_PATH || path.join(directory, 'orders.json'));
  function read(name, file, empty, required = true, validate = record) {
    try {
      const value = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (!record(value) || !validate(value)) throw Error('Invalid store');
      feeds.push({ name, status: 'ready', required, updatedAt: value.sync?.lastSyncedAt || value.lastSync || value.at || null, error: value.error || null });
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
    shopifyCosts: read('Shopify Cost per item', env.INVENTORY_SNAPSHOT_PATH || path.join(directory, 'inventory-snapshot.json'), { items: [] }, false, v => Number.isFinite(Date.parse(v.at)) && Array.isArray(v.items)),
    lineCorrections: read('P&L-only item corrections', path.join(directory, 'pnl-line-corrections.json'), {}, false, v => v.schemaVersion === 1 && record(v.entries) && Array.isArray(v.history)),
    velocity: {
      index: read('Velocity automatic shipment discovery', path.join(orderDirectory, 'velocity_order_index.json'), {}, false, v => record(v.byOrder) && record(v.shipments)),
      tracking: read('Velocity carrier tracking', path.join(orderDirectory, 'velocity_tracking_evidence.json'), {}, false, v => record(v.shipments)),
      cache: read('Velocity tracking cache', path.join(orderDirectory, 'velocity_cache.json'), {}, false, v => record(v.shipments))
    },
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
    Transactions: [['Date', 'Expense date', 'Type', 'Channel', 'Category', 'Particulars', 'Reference', 'Account', 'Method', 'P&L gross sales', 'Discounts', 'P&L returns', 'P&L-calculated GST', 'Net sales', 'Other income', 'Known COGS', 'Paid expense', 'Tax complete', 'Cost complete', 'Proof', 'Source invoice total (unchanged)', 'Source refund amount (unchanged)'], ...report.transactions.map(r => [r.date, r.expenseDate || '', r.kind, r.channel, r.category, r.particulars, r.reference, r.account, r.method, r.grossSales, r.discounts, r.returns, r.taxComplete ? r.gst : 'Unavailable', r.netSales ?? 'Unavailable', r.otherIncome, r.cogs, r.expenses, r.taxComplete, r.costComplete, r.proof, r.sourceInvoiceTotal ?? '', r.sourceRefundAmount ?? ''])],
    'SKU allocations': [['Date', 'Reference', 'SKU', 'Quantity', 'Purchase batch', 'Allocated quantity', 'Unit cost', 'Allocated cost'], ...report.transactions.flatMap(r => (r.details || []).flatMap(d => (d.allocations || []).map(a => [r.date, r.reference, d.sku, d.qty, a.purchaseId, a.qty, a.unitCost, a.amount])))],
    Tax: [['Date', 'Reference', 'SKU', 'Quantity', 'Channel', 'P&L inclusive amount', 'Invoice GST (reference only)', 'P&L-calculated GST', 'P&L included GST', 'Rate', 'Status'], ...report.taxes.map(r => [r.date, r.reference, r.sku, r.qty, r.channel, r.gross, r.recorded ?? 'Not provided', r.calculated ?? 'Unavailable', r.tax ?? 'Unavailable', r.rate ?? '', r.status])],
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
function createRouter({ read, clock = () => new Date(), correctionsFile } = {}) {
  const router = express.Router();
  let refreshAfter = 0;
  const defaultRead = () => {
    const sources = loadSources(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'));
    const feed = sources.feeds.find(f => f.name === 'Shopify Cost per item');
    feed.updatedAt = sources.shopifyCosts.at || null;
    // Reuse the existing persisted Shopify inventory snapshot. Report reads
    // return immediately; a coalesced background refresh never changes stock,
    // invoices, expenses, payments or reconciliations.
    if (Date.now() >= refreshAfter) {
      refreshAfter = Date.now() + (sources.shopifyCosts.at ? 15 * 60 * 1000 : 30000);
      try {
        const live = require('./inventory-state').readSnapshot();
        if (live.snapshot) { sources.shopifyCosts = live.snapshot; feed.updatedAt = live.snapshot.at; feed.status = 'ready'; }
        feed.refreshing = live.refreshing; feed.stale = live.stale;
      } catch (_) { /* Retain the persisted source; missing costs remain explicit. */ }
    }
    return sources;
  };
  function report(req) {
    const today = day(clock().toISOString()), month = today.slice(0, 7) + '-01';
    const from = String(req.query.from === '' ? START : req.query.from || month), to = String(req.query.to || today);
    if (!validDate(from) || !validDate(to) || to > today) throw Object.assign(new Error('Choose valid dates ending today or earlier.'), { status: 400 });
    const sources = (read || defaultRead)();
    return buildReport(sources, { from, to, start: START, channel: String(req.query.channel || 'All') });
  }
  const canEdit = req => ['admin','owner'].includes(String(req.user?.role || '').toLowerCase());
  const correctionError = (res,error) => res.status(error.status || 500).json({success:false,error:error.status ? error.message : 'P&L item corrections could not be saved safely. No source financial records were changed.'});
  router.get('/api/pl/report/line-corrections', (req,res)=>{
    try {
      const today=day(clock().toISOString()),from=String(req.query.from||START),to=String(req.query.to||today);
      if(!validDate(from)||!validDate(to)||from<START||to>today||from>to)throw Object.assign(Error('Choose a valid accounting date range.'),{status:400});
      const sources=(read||defaultRead)();
      res.set('Cache-Control','no-store').json({success:true,canEdit:canEdit(req),rows:lineCorrections.reviewRows(sources,from,to),historyCount:sources.lineCorrections?.history?.length||0});
    }catch(error){correctionError(res,error);}
  });
  router.post('/api/pl/report/line-corrections/preview',(req,res)=>{
    if(!canEdit(req))return res.status(403).json({success:false,error:'Only Admin or Owner can correct P&L item mappings.'});
    try {res.set('Cache-Control','no-store').json({success:true,items:lineCorrections.prepare((read||defaultRead)(),req.body?.items)});}catch(error){correctionError(res,error);}
  });
  router.post('/api/pl/report/line-corrections',(req,res)=>{
    if(!canEdit(req))return res.status(403).json({success:false,error:'Only Admin or Owner can correct P&L item mappings.'});
    try {
      const file=correctionsFile||path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname,'..'),'pnl-line-corrections.json');
      const result=lineCorrections.save(file,(read||defaultRead)(),req.body?.items,req.user.username,req.body?.note,clock().toISOString());
      res.set('Cache-Control','no-store').json({success:true,...result});
    }catch(error){correctionError(res,error);}
  });
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
