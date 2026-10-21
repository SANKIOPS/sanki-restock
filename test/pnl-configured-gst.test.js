'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const XLSX = require('xlsx');
const { buildReport, lineTax } = require('../modules/pnl-report');
const { exportWorkbook } = require('../modules/pnl-routes');

function fixture(overrides = {}) {
  return {
    orders: { orders: { O: { id: 'O', name: '#TEST', channel: 'POS', financialStatus: 'paid', processedAt: '2026-10-01', taxesIncluded: true, total: 1999, discount: 0, lineItems: [{ id: 'L', sku: 'SKU', qty: 1, price: 1999, taxable: false, taxLines: [{ rate: .18, price: 304.94 }] }], ...overrides } } },
    opening: { lots: [{ sku: 'SKU', qty: 10, unitCost: 500, verified: true }] },
    expenses: { payments: [{ amount: 321, reference: 'UNCHANGED' }], bankStatements: { B: { closing: 9876 } }, bankReconciliations: [{ status: 'finalized', links: ['UNCHANGED'] }] }
  };
}
const run = (sources, options = {}) => buildReport(sources, { from: '2026-10-01', to: '2026-10-18', ...options });
const freeze = object => { if (object && typeof object === 'object') { Object.values(object).forEach(freeze); Object.freeze(object); } return object; };

test('recorded GST, invoice rate and exempt flag never choose or override P&L garment GST', () => {
  for (const recorded of [0, 95.18, 304.94, 9000]) {
    const tax = lineTax({ qty: 1, price: 1999, taxable: false, taxLines: [{ rate: .99, price: recorded }] });
    assert.equal(tax.valid, true); assert.equal(tax.tax, 95.19); assert.equal(tax.calculated, 95.19);
    assert.equal(tax.rate, .05); assert.equal(tax.gross, 1999); assert.equal(tax.recorded, recorded);
    assert.equal(tax.status, 'P&L configured GST');
  }
});

test('explicit perfume titles use the confirmed report-only 28 percent rate', () => {
  for (const title of ['Example perfume', 'PERFUMES']) {
    const line = { title, qty: 2, price: 1500, discount: 200, taxable: false, taxLines: [{rate: .05, price: 100}] };
    const before = JSON.stringify(line), tax = lineTax(freeze(line));
    assert.equal(tax.rate, .28); assert.equal(tax.tax, 612.5); assert.equal(tax.gross, 2800);
    assert.equal(JSON.stringify(line), before);
    assert.equal(lineTax(line, false).tax, 784);
  }
  assert.equal(lineTax({title:'Custom sale',qty:1,price:1500}).rate,.05);
});

test('perfume refunds reverse original report GST without changing source orders', () => {
  const sources=fixture({total:1280,lineItems:[{id:'L',sku:'SKU',title:'Example perfume',qty:1,price:1280}],refunds:[{id:'RF',date:'2026-10-03',amount:1280,lineItems:[{lineItemId:'L',qty:1,restockType:'return'}]}]});
  const before=JSON.stringify(sources), report=run(freeze(sources));
  assert.equal(report.taxes.find(t=>t.qty===1).rate,.28);
  assert.equal(report.totals.gst,0); assert.equal(report.totals.netProfit,0);
  assert.equal(JSON.stringify(sources),before);
});

test('inclusive final selling-price boundary is tested per piece after discount, not order total', () => {
  assert.equal(lineTax({ qty: 2, price: 2500 }).tax, 238.1);
  assert.equal(lineTax({ qty: 1, price: 3999 }).tax, 610.02);
  assert.equal(lineTax({ qty: 2, price: 3000, discount: 1002 }).tax, 238);
  assert.equal(lineTax({ qty: 1, price: 2500 }).rate, .05);
  assert.equal(lineTax({ qty: 1, price: 2500.01 }).rate, .18);
  assert.equal(lineTax({ qty: 1, price: 2625 }).rate, .18);
  assert.equal(lineTax({ qty: 1, price: 2950.01 }).rate, .18);
  assert.equal(lineTax({ qty: 1, price: 2625.01 }).valid, true);
  assert.equal(lineTax({ qty: 1, price: 2800, taxable: false, taxLines: [{ rate: .05, price: 133.33 }] }).valid, true);
  assert.equal(lineTax({ qty: 1, price: 2800,unitTaxableValue:2300 }).rate,.18);
  assert.equal(lineTax({ qty: 0, price: 1999, taxable: false }).valid, false);
});

test('P&L becomes tax complete despite mismatched invoice GST without editing source data', () => {
  const sources = fixture(), before = JSON.stringify(sources), report = run(freeze(sources));
  assert.equal(report.totals.gst, 95.19); assert.equal(report.totals.netSales, 1903.81);
  assert.equal(report.totals.netProfit, 1403.81); assert.equal(report.totals.complete, true);
  assert.equal(report.taxes[0].recorded, 304.94); assert.equal(report.warnings.length, 0);
  assert.equal(JSON.stringify(sources), before);
});

test('tax-exclusive report net sales use taxable value and retain invoice total as reference', () => {
  const sources = fixture({ taxesIncluded: false, total: 2358.82, lineItems: [{ id: 'L', sku: 'SKU', qty: 1, price: 1999, taxLines: [{ rate: .18, price: 359.82 }] }] });
  const before = JSON.stringify(sources), report = run(sources);
  assert.equal(report.totals.gst, 99.95); assert.equal(report.totals.netSales, 1999);
  assert.equal(report.totals.grossSales, 2098.95); assert.equal(report.totals.complete, true);
  assert.equal(report.transactions[0].sourceInvoiceTotal, 2358.82);
  assert.equal(report.collections[0].orderValue, 2358.82);
  assert.equal(JSON.stringify(sources), before);
});

test('missing inventory costs still block profit, and unrelated missing-date warnings remain', () => {
  const sources = fixture(); sources.opening = {};
  let report = run(sources);
  assert.equal(report.totals.gst, 95.19); assert.equal(report.totals.netSales, 1903.81);
  assert.equal(report.totals.cogs, null); assert.equal(report.totals.netProfit, null);
  assert.ok(report.warnings.some(w => w.scope === 'cogs'));
  sources.orders.orders.D = { ...sources.orders.orders.O, id: 'D', name: '#DATE', channel: 'Website', deliveryComplete: true };
  report = run(sources); assert.ok(report.warnings.some(w => /actual date is missing/.test(w.message)));
});

test('full item refund reverses original P&L GST, not its higher invoice tax', () => {
  const sources = fixture({ refunds: [{ id: 'RF', date: '2026-10-03', amount: 1999, lineItems: [{ lineItemId: 'L', qty: 1, subtotal: 1694.06, tax: 304.94, restockType: 'return' }] }] });
  const before = JSON.stringify(sources), report = run(freeze(sources));
  assert.equal(report.totals.gst, 0); assert.equal(report.totals.netSales, 0);
  assert.equal(report.totals.cogs, 0); assert.equal(report.totals.netProfit, 0);
  const refund = report.taxes.find(t => t.status === 'Original P&L GST reversed');
  assert.equal(refund.recorded, -304.94); assert.equal(refund.calculated, -95.19);
  assert.equal(JSON.stringify(sources), before);
});

test('tax-exclusive full refund reverses report values while retaining actual source refund', () => {
  const sources = fixture({ taxesIncluded: false, total: 2358.82, lineItems: [{ id: 'L', sku: 'SKU', qty: 1, price: 1999, taxLines: [{ rate: .18, price: 359.82 }] }], refunds: [{ id: 'RF', date: '2026-10-03', amount: 2358.82, lineItems: [{ lineItemId: 'L', qty: 1, subtotal: 1999, tax: 359.82, restockType: 'return' }] }] });
  const report = run(sources), refund = report.transactions.find(r => r.kind === 'return');
  assert.equal(report.totals.gst, 0); assert.equal(report.totals.netSales, 0); assert.equal(report.totals.netProfit, 0);
  assert.equal(refund.returns, 2098.95); assert.equal(refund.sourceRefundAmount, 2358.82);
});

test('partial refund retains original slab even when refunded value falls into lower slab', () => {
  const sources = fixture({ total: 3999, lineItems: [{ id: 'L', sku: 'SKU', qty: 1, price: 3999 }], refunds: [{ id: 'RF', date: '2026-10-03', amount: 1000, lineItems: [{ lineItemId: 'L', qty: 1, subtotal: 847.46, tax: 152.54, restockType: 'no_restock' }] }] });
  const report = run(sources), refund = report.transactions.find(r => r.kind === 'return');
  assert.equal(refund.gst, -152.54); assert.equal(report.totals.gst, 457.48);
  assert.equal(report.totals.cogs, 500); assert.equal(report.totals.complete, true);
});

test('linked full refund does not require invoice GST to calculate report GST', () => {
  const sources = fixture({ refunds: [{ id: 'RF', date: '2026-10-03', amount: 1999, lineItems: [{ lineItemId: 'L', qty: 1, restockType: 'return' }] }] });
  const report = run(sources), refund = report.taxes.find(t => t.qty === 0);
  assert.equal(report.totals.gst, 0); assert.equal(report.totals.netProfit, 0);
  assert.equal(refund.recorded, null); assert.equal(refund.calculated, -95.19);
});

test('refund allocations use cumulative paise rounding and cannot reverse the same quantity twice', () => {
  const sources = fixture({ total: .3, lineItems: [{ id: 'L', sku: 'SKU', qty: 3, price: .1 }], refunds: [1, 2, 3].map(n => ({ id: 'RF'+n, date: '2026-10-0'+(n+1), amount: .1, lineItems: [{ lineItemId: 'L', qty: 1, subtotal: .1, tax: 0, restockType: 'return' }] })) });
  let report = run(sources); assert.equal(report.totals.gst, 0); assert.equal(report.totals.netSales, 0); assert.equal(report.totals.cogs, 0);
  sources.orders.orders.O.refunds.push({ id: 'DUP', date: '2026-10-06', amount: .1, lineItems: [{ lineItemId: 'L', qty: 1, subtotal: .1, tax: 0, restockType: 'return' }] });
  report = run(sources); assert.equal(report.totals.netProfit, null); assert.ok(report.warnings.some(w => /Refund GST\/item linkage/.test(w.message)));
});

test('unknown refund item and unresolved price basis remain explicit blockers', () => {
  const sources = fixture({ refunds: [{ id: 'RF', date: '2026-10-03', amount: 1999, lineItems: [{ lineItemId: 'UNKNOWN', qty: 1, tax: 0 }] }] });
  assert.equal(run(sources).totals.netProfit, null);
  sources.orders.orders.O.refunds = []; delete sources.orders.orders.O.taxesIncluded;
  assert.ok(run(sources).warnings.some(w => /price basis is missing/.test(w.message)));
});

test('GST-only recalculation cannot write accounting files or make any remote request', () => {
  const sources = freeze(fixture()), before = JSON.stringify(sources);
  const methods = ['writeFileSync', 'appendFileSync', 'renameSync', 'unlinkSync'];
  const saved = methods.map(method => fs[method]);
  const originalFetch = global.fetch;
  try {
    methods.forEach(method => { fs[method] = () => { throw Error('Unexpected accounting write'); }; });
    global.fetch = () => { throw Error('Unexpected remote request'); };
    const report = run(sources); exportWorkbook(report);
    assert.equal(report.totals.gst, 95.19); assert.equal(JSON.stringify(sources), before);
  } finally { methods.forEach((method, index) => { fs[method] = saved[index]; }); global.fetch = originalFetch; }
});

test('Excel separates reference invoice GST from report GST and exposes unchanged source amounts', () => {
  const workbook = XLSX.read(exportWorkbook(run(fixture())), { type: 'buffer' });
  const tax = XLSX.utils.sheet_to_json(workbook.Sheets.Tax, { header: 1 });
  assert.equal(tax[0][6], 'Invoice GST (reference only)'); assert.equal(tax[1][6], 304.94); assert.equal(tax[1][7], 95.19); assert.equal(tax[1][8], 95.19);
  const rows = XLSX.utils.sheet_to_json(workbook.Sheets.Transactions, { header: 1 });
  assert.equal(rows[0][20], 'Source invoice total (unchanged)'); assert.equal(rows[1][20], 1999);
});
