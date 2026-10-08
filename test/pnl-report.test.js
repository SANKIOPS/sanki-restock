'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { buildReport, lineTax, gstRate, day, costLots } = require('../modules/pnl-report');
const { exportWorkbook, loadSources, createRouter } = require('../modules/pnl-routes');
const XLSX = require('xlsx');
const express = require('express');

function fixtures(orderChanges = {}) {
  const order = { id: '1', name: '#1', channel: 'POS', financialStatus: 'paid', processedAt: '2026-10-01T05:00:00Z', total: 2999, discount: 0, shipping: 0, taxesIncluded: true, lineItems: [{ id: 'L1', sku: 'SKU1', qty: 1, price: 2999, discount: 0 }], ...orderChanges };
  return { orders: { orders: { 1: order } }, purchases: {}, opening: { lots: [{ id: 'OPEN1', sku: 'SKU1', qty: 10, unitCost: 999, verified: true }] }, expenses: {}, salary: {}, manualSales: {}, feeds: [] };
}
const run = (sources, options = {}) => buildReport(sources, { from: '2026-10-01', to: '2026-10-08', ...options });
test('GST boundary is inclusive at 2500 and exclusive above, tested per piece', () => {
  assert.equal(gstRate(2499), .05); assert.equal(gstRate(2500), .05); assert.equal(gstRate(2500.01), .18); assert.equal(gstRate(2501), .18);
  assert.equal(lineTax({ qty: 2, price: 2500 }, false).tax, 250);
  assert.equal(lineTax({ qty: 1, price: 2501 }, false).tax, 450.18);
});
test('inclusive GST is extracted, never multiplied straight onto the gross amount', () => {
  const report = run(fixtures());
  assert.equal(report.totals.gst, 457.47); assert.equal(report.totals.netSales, 2541.53);
  assert.equal(report.totals.cogs, 999); assert.equal(report.totals.netProfit, 1542.53);
});
test('invoice tax mismatch and ambiguous tax-inclusive value do not produce a final profit', () => {
  assert.equal(lineTax({ qty: 1, price: 2999, taxLines: [{ rate: .05, price: 142.81 }] }).valid, false);
  assert.equal(lineTax({ qty: 1, price: 2800 }).valid, false);
  const sources = fixtures(); sources.orders.orders[1].lineItems[0].taxLines = [{ rate: .05, price: 142.81 }];
  assert.equal(run(sources).totals.netProfit, null);
});
test('CGST and SGST invoice tax components combine correctly', () => {
  const tax = lineTax({ qty: 1, price: 1180, taxLines: [{ rate: .025, price: 28.1 }, { rate: .025, price: 28.09 }] });
  assert.equal(tax.valid, true); assert.equal(tax.rate, .05); assert.equal(tax.tax, 56.19);
});
test('paid portions only, excess advances and reimbursements never inflate expenses', () => {
  const sources = fixtures();
  sources.expenses.expenses = {
    EX1: { id: 'EX1', nature: 'SANKI', amount: 200, status: 'paid', type: 'variable', ledger: 'Flowers', payments: [{ id: 'PAY1', date: '2026-10-01', amount: 418 }], reimbursementPayments: [{ id: 'REIM1', date: '2026-10-02', amount: 200 }] },
    EX2: { id: 'EX2', nature: 'SANKI', amount: 1000, status: 'partially_paid', type: 'marketing', payments: [{ id: 'PAY1', date: '2026-10-02', amount: 400 }] },
    EX3: { id: 'EX3', nature: 'SANKI', amount: 900, status: 'approved', payments: [] },
    EX4: { id: 'EX4', nature: 'SANKI', amount: 500, status: 'pending', payments: [{ id: 'PAY1', date: '2026-10-02', amount: 500 }] },
    EX5: { id: 'EX5', nature: 'PERSONAL', amount: 500, status: 'paid', payments: [{ id: 'PAY1', date: '2026-10-02', amount: 500 }] }
  };
  assert.equal(run(sources).totals.expenses, 600);
  assert.equal(run(sources, { from: '2026-10-02' }).totals.expenses, 400);
});
test('actual payroll payments only, not earned salary or salary advances', () => {
  const sources = fixtures();
  sources.salary = { months: { '2026-10': { salaryAmt: 50000 } }, advances: [{ amount: 8000 }], salaryPayments: [{ id: 'SAL1', amount: 1500, date: '2026-10-01', employeeName: 'Employee', active: true }, { id: 'SAL2', amount: 2000, date: '2026-10-01', active: false }] };
  assert.equal(run(sources).totals.expenses, 1500);
});

test('only active paid incentives count, not incentive approvals or earned balances', () => {
  const sources = fixtures();
  sources.incentives = { approvals: { earned: { amount: 9000 } }, payments: [
    { id: 'INCP1', salesperson: 'Example', amount: 240, date: '2026-10-03', active: true, proofs: ['/proof.jpg'] },
    { id: 'INCP2', amount: 200, date: '2026-10-03', active: false },
    { id: 'INCP3', amount: 300, date: '2026-10-09', active: true }
  ] };
  assert.equal(run(sources).totals.expenses, 240);
  assert.equal(run(sources, { to: '2026-10-02' }).totals.expenses, 0);
  assert.equal(run(sources, { channel: 'Website' }).totals.expenses, 0);
  assert.equal(run(sources).categories.find(c => c.category === 'Sales incentives').amount, 240);
});

function refundedExpense() {
  const sources = fixtures();
  sources.expenses = { expenses: { EX1: { id: 'EX1', nature: 'SANKI', date: '2026-10-01', amount: 200, status: 'paid', ledger: 'Flowers', type: 'variable', payments: [{ id: 'PAY1', amount: 200, date: '2026-10-01' }] } },
    expenseRefunds: [{ id: 'ER1', nature: 'SANKI', date: '2026-10-04', status: 'received', sources: [{ kind: 'payment', expenseId: 'EX1', amount: 100, billCreditAmount: 100, paymentAllocations: [{ paymentId: 'PAY1', amount: 100 }] }], components: [{ mode: 'bank', receiptId: 'R1' }] }],
    receipts: [{ id: 'R1', nature: 'SANKI', date: '2026-10-04', receiptType: 'expense_refund', amount: 100 }] };
  return sources;
}
test('received supplier refunds reverse original paid costs on receipt date, never add income', () => {
  const sources = refundedExpense();
  assert.equal(run(sources, { to: '2026-10-03' }).totals.expenses, 200);
  assert.equal(run(sources).totals.expenses, 100);
  assert.equal(run(sources, { from: '2026-10-04' }).totals.expenses, -100);
  assert.equal(run(sources).totals.otherIncome, 0);
  assert.equal(run(sources).categories[0].amount, 100);
  sources.expenses.expenseRefunds[0].components[0].mode = 'voucher';
  assert.equal(run(sources).totals.expenses, 100);
  const before = JSON.stringify(sources); run(sources); assert.equal(JSON.stringify(sources), before);
});
test('pending, void, unpaid credit notes and returned overpayments do not reverse paid costs', () => {
  const sources = refundedExpense(), refund = sources.expenses.expenseRefunds[0];
  for (const status of ['pending', 'voided']) { refund.status = status; assert.equal(run(sources).totals.expenses, 200); }
  refund.status = 'received'; refund.creditNote = true; refund.sources[0].paymentAllocations = [];
  assert.equal(run(sources).totals.expenses, 200); assert.equal(run(sources).totals.complete, true);
  refund.creditNote = false; refund.sources[0].kind = 'advance'; delete refund.sources[0].expenseId;
  assert.equal(run(sources).totals.expenses, 200); assert.equal(run(sources).totals.complete, true);
});
test('matched card refund evidence and its source allocation reverse costs only once', () => {
  const sources = refundedExpense(); delete sources.expenses.expenses.EX1;
  sources.expenses.reconciliationExpenses = [
    { id: 'CCE1', nature: 'SANKI', amount: 200, date: '2026-10-01', creditCardId: 'CARD1', category: 'Flowers' },
    { id: 'CCE2', nature: 'SANKI', amount: -100, date: '2026-10-04', creditCardId: 'CARD1', expenseRefundReceiptId: 'R1', category: 'Flowers' }
  ];
  const source = sources.expenses.expenseRefunds[0].sources[0]; source.expenseId = 'CCE1'; source.paymentAllocations[0].paymentId = 'STATEMENT';
  assert.equal(run(sources).totals.expenses, 100); assert.equal(run(sources).totals.complete, true);
  delete sources.expenses.reconciliationExpenses[1].expenseRefundReceiptId;
  sources.expenses.expenseRefunds[0].components[0].externalMovementId = 'CCE2';
  assert.equal(run(sources).totals.expenses, 100);
});
test('refunds cannot exceed recognized paid portions or silently lose their payment link', () => {
  const sources = refundedExpense(), refund = sources.expenses.expenseRefunds[0];
  refund.sources[0].amount = 250; refund.sources[0].paymentAllocations[0].amount = 250;
  assert.equal(run(sources).totals.expenses, 0); assert.equal(run(sources).totals.netProfit, null);
  refund.sources[0].paymentAllocations[0].paymentId = 'UNKNOWN';
  assert.equal(run(sources).totals.expenses, 200); assert.equal(run(sources).totals.netProfit, null);
});
test('refund allocations identify applied vendor advances using the existing refund schema', () => {
  const sources = refundedExpense(), expense = sources.expenses.expenses.EX1;
  expense.payments = []; expense.vendorAdvanceApplications = [{ vendorAdvanceId: 'VA1', amount: 200, date: '2026-10-01' }];
  sources.expenses.expenseRefunds[0].sources[0].paymentAllocations[0].paymentId = 'ADV-VA1-0';
  assert.equal(run(sources).totals.expenses, 100); assert.equal(run(sources).totals.complete, true);
});
test('draft bank costs excluded, posted costs count once', () => {
  const sources = fixtures(); sources.expenses = { bankReconciliationDrafts: { D1: {} }, reconciliationExpenses: [{ id: 'BRE1', nature: 'SANKI', date: '2026-10-01', amount: 5.9, reconciliationDraft: 'D1' }, { id: 'BRE2', nature: 'SANKI', date: '2026-10-01', amount: 5.9, reconciliationDraft: 'D2' }] };
  assert.equal(run(sources).totals.expenses, 5.9);
});
test('SKU FIFO uses received purchases and is stable after a later purchase', () => {
  const sources = fixtures(); sources.opening.lots[0].qty = 1;
  sources.purchases = { pos: { PO2: { id: 'PO2', status: 'received', dateReceive: '2026-10-02', origin: 'india', lines: [{ sku: 'SKU1', qty: 10, perPcsYuan: 1199 }] }, PO3: { id: 'PO3', status: 'advance', dateReceive: '2026-09-20', origin: 'india', lines: [{ sku: 'SKU1', qty: 10, perPcsYuan: 500 }] } } };
  assert.equal(run(sources).totals.cogs, 999);
  sources.orders.orders[2] = { ...sources.orders.orders[1], id: '2', name: '#2', processedAt: '2026-10-03' };
  const result = run(sources); assert.equal(result.totals.cogs, 2198);
  assert.equal(result.transactions.find(r => r.id === 'Shopify/1').details[0].allocations[0].purchaseId, 'Opening stock');
  assert.equal(result.transactions.find(r => r.id === 'Shopify/2').details[0].allocations[0].purchaseId, 'PO2');
});
test('missing costs stay unavailable, never a percentage fallback or zero-profit fiction', () => {
  const sources = fixtures(); sources.opening = {};
  assert.equal(run(sources).totals.cogs, null); assert.equal(run(sources).totals.netProfit, null);
  assert.ok(run(sources).warnings.some(w => w.scope === 'cogs'));
});
test('COD advance is not revenue; delivery recognises one sale and payments remain separate', () => {
  const sources = fixtures({ channel: 'Website', financialStatus: 'partially_paid', total: 50000, lineItems: [{ id: 'L1', sku: 'SKU1', qty: 1, price: 50000 }], paymentTransactions: [{ id: 'T1', kind: 'sale', status: 'success', amount: 5000, gateway: 'UPI', processedAt: '2026-10-01' }] });
  let report = run(sources); assert.equal(report.totals.grossSales, 0); assert.equal(report.collections[0].customerAdvance, 5000); assert.equal(report.collections[0].balanceToCollect, 45000);
  sources.orders.dispatch = { 1: { packingStatus: 'delivered', deliveredAt: '2026-10-03' } };
  sources.orders.orders[1].paymentTransactions.push({ id: 'T2', kind: 'sale', status: 'success', amount: 45000, gateway: 'COD', processedAt: '2026-10-03' });
  sources.expenses.transfers = [{ amount: 45000, date: '2026-10-05' }];
  report = run(sources); assert.equal(report.totals.grossSales, 50000); assert.equal(report.totals.salesCount, 1); assert.equal(report.collections[0].collected, 50000);
});
test('cancelled and RTO before delivery do not create sales', () => {
  const sources = fixtures({ channel: 'Website', cancelledAt: '2026-10-02' });
  assert.equal(run(sources).totals.grossSales, 0);
  sources.orders.orders[1].cancelledAt = null; sources.orders.dispatch = { 1: { packingStatus: 'rto' } };
  assert.equal(run(sources).totals.grossSales, 0);
});
test('refund event uses refund date; only physically restocked quantities reverse COGS', () => {
  const sources = fixtures({ refunds: [{ id: 'RF1', date: '2026-10-05', amount: 2999, lineItems: [{ lineItemId: 'L1', qty: 1, subtotal: 2541.53, tax: 457.47, restockType: 'return' }] }] });
  let report = run(sources, { to: '2026-10-04' }); assert.equal(report.totals.cogs, 999); assert.equal(report.totals.returns, 0);
  report = run(sources); assert.equal(report.totals.cogs, 0); assert.equal(report.totals.netProfit, 0); assert.equal(report.totals.returns, 2999);
  sources.orders.orders[1].refunds[0].lineItems[0].restockType = 'no_restock';
  assert.equal(run(sources).totals.cogs, 999); assert.equal(run(sources).totals.netProfit, -999);
});
test('refund amount without tax or date never silently produces a complete profit', () => {
  const sources = fixtures({ refundAmount: 200 }); assert.equal(run(sources).totals.netProfit, null);
  sources.orders.orders[1].refundTransactions = [{ id: 'RF', amount: 200, processedAt: '2026-10-03' }];
  assert.equal(run(sources).totals.returns, 200); assert.equal(run(sources).totals.netProfit, null);
});
test('opening verified zero cost is distinct from a missing cost', () => {
  assert.equal(costLots({}, { lots: [{ sku: 'X', qty: 1, unitCost: 0, verified: true }] })[0].cost, 0);
});
test('start boundary and IST date cutoffs are enforced', () => {
  assert.equal(day('2026-08-21T20:00:00Z'), '2026-08-22');
  const report = run(fixtures(), { from: '2026-08-01' }); assert.equal(report.range.from, '2026-08-22'); assert.equal(report.range.partial, true);
  assert.throws(() => run(fixtures(), { from: '2026-02-30' }), /valid date range/);
});
test('receipts count genuine business income, not capital or duplicate split sale receipts', () => {
  const sources = fixtures(); sources.expenses.receipts = [{ id: 'R1', nature: 'SANKI', receiptType: 'bank_interest', amount: 100, date: '2026-10-02' }, { id: 'R2', nature: 'SANKI', receiptType: 'owner_contribution', amount: 10000, date: '2026-10-02' }, { id: 'R3', nature: 'PERSONAL', receiptType: 'other_income', amount: 500, date: '2026-10-02' }];
  assert.equal(run(sources).totals.otherIncome, 100);
});
test('read-only report does not mutate any input stores', () => {
  const sources = fixtures(); const before = JSON.stringify(sources); run(sources); assert.equal(JSON.stringify(sources), before);
});

test('historical COD collection excludes payments and completion that occurred later', () => {
  const sources = fixtures({ channel: 'Website', financialStatus: 'partially_paid', createdAt: '2026-10-01', total: 50000,
    completedAt: '2026-10-05', lineItems: [{ id: 'L1', sku: 'SKU1', qty: 1, price: 50000 }],
    paymentTransactions: [{ id: 'A', kind: 'sale', status: 'success', amount: 5000, processedAt: '2026-10-01' }, { id: 'B', kind: 'sale', status: 'success', amount: 45000, processedAt: '2026-10-05' }] });
  const report = run(sources, { to: '2026-10-03' });
  assert.equal(report.totals.grossSales, 0); assert.equal(report.collections[0].collected, 5000);
  assert.equal(report.collections[0].customerAdvance, 5000); assert.equal(report.collections[0].balanceToCollect, 45000);
  assert.equal(report.collections[0].payments.length, 1);
});
test('delivery timestamps must be actual evidence, not an unrelated Shopify edit timestamp', () => {
  const { normalizeOrder } = require('../modules/orders');
  const normalized = normalizeOrder({ id: 'D', source_name: 'web', taxes_included: true,
    line_items: [{ id: 'L', sku: 'SKU1', quantity: 2, price: 999 }],
    fulfillments: [{ status: 'success', shipment_status: 'delivered', updated_at: '2026-10-08', line_items: [{ id: 'L', quantity: 1 }] }] });
  assert.equal(normalized.deliveryComplete, false); assert.equal(normalized.completedAt, null);
  const raw = { id: 'D', source_name: 'web', created_at: '2026-09-01', line_items: [{ id: 'L', sku: 'SKU1', quantity: 1, price: 999 }], fulfillments: [{ status: 'success', shipment_status: 'delivered', updated_at: '2026-10-08', line_items: [{ id: 'L', quantity: 1 }] }] };
  const missingDate = normalizeOrder(raw); assert.equal(missingDate.deliveryComplete, true); assert.equal(missingDate.completedAt, null);
  const sources = fixtures(); sources.orders.orders = { D: missingDate };
  assert.ok(run(sources).warnings.some(w => /actual date is missing/.test(w.message)));
  assert.equal(run(sources).totals.netProfit, null);
});
test('pre-boundary purchases cannot impersonate verified opening stock', () => {
  const sources = fixtures(); sources.opening = {};
  sources.purchases = { pos: { OLD: { id: 'OLD', dateReceive: '2026-04-01', status: 'received', origin: 'india', lines: [{ sku: 'SKU1', qty: 100, perPcsYuan: 100 }] } } };
  assert.equal(run(sources).totals.cogs, null);
});
test('manual sales and ledger customer refunds are included without hidden double counting', () => {
  const sources = fixtures(); sources.orders = {};
  sources.manualSales = { sales: [{ id: 'M1', day: '2026-10-01', channel: 'walk-in', total: 2999, items: [{ sku: 'SKU1', qty: 1, unitPrice: 2999, lineTotal: 2999 }] }] };
  assert.equal(run(sources).totals.grossSales, 2999); assert.equal(run(sources).totals.cogs, 999);
  sources.expenses.salesRefunds = [{ id: 'R', nature: 'SANKI', amount: 100, date: '2026-10-03', saleReference: 'M1' }];
  assert.equal(run(sources).totals.returns, 100); assert.equal(run(sources).totals.netProfit, null);
  const shopify = fixtures({ refunds: [{ id: 'RF', date: '2026-10-03', amount: 2999, lineItems: [{ lineItemId: 'L1', qty: 1, tax: 457.47, subtotal: 2541.53, restockType: 'return' }] }] });
  shopify.expenses.salesRefunds = [{ id: 'COPY', nature: 'SANKI', amount: 2999, date: '2026-10-03', saleReference: '#1' }];
  assert.equal(run(shopify).totals.returns, 2999);
});
test('multiple legacy vendor advance applications do not collapse into one undefined id', () => {
  const sources = fixtures(); sources.expenses.expenses = { E: { id: 'E', nature: 'SANKI', status: 'paid', amount: 200, vendorAdvanceApplications: [{ vendorAdvanceId: 'A', amount: 50, date: '2026-10-02' }, { vendorAdvanceId: 'B', amount: 150, date: '2026-10-02' }] } };
  assert.equal(run(sources).totals.expenses, 200); assert.equal(run(sources).transactions.filter(r => r.kind === 'expense').length, 2);
});
test('channel-specific cost warnings do not poison another direct channel report', () => {
  const sources = fixtures(); sources.orders.orders[2] = { ...sources.orders.orders[1], id: '2', channel: 'Website', completedAt: '2026-10-02', lineItems: [{ id: 'L2', sku: 'MISSING', qty: 1, price: 2999 }] };
  assert.equal(run(sources).totals.netProfit, null); assert.equal(run(sources, { channel: 'POS' }).totals.netProfit, 1542.53);
});
test('Excel export retains typed numbers, explicit unavailable results and source allocations', () => {
  const report = run(fixtures()); const workbook = XLSX.read(exportWorkbook(report), { type: 'buffer' });
  assert.ok(workbook.SheetNames.includes('SKU allocations')); assert.ok(workbook.SheetNames.includes('Tax'));
  const rows = XLSX.utils.sheet_to_json(workbook.Sheets.Summary, { header: 1 });
  assert.equal(rows.find(r => r[0] === 'Management profit / loss')[1], 1542.53);
  report.totals.netProfit = null; const missing = XLSX.read(exportWorkbook(report), { type: 'buffer' });
  assert.equal(XLSX.utils.sheet_to_json(missing.Sheets.Summary, { header: 1 }).find(r => r[0] === 'Management profit / loss')[1], 'Unavailable');
});
test('missing/malformed sources are reported without saving or replacing stores', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'pnl-read-test-'));
  try {
    // Only a disposable test fixture, never a production financial file.
    fs.writeFileSync(path.join(directory, 'expenses.json'), 'invalid');
    const before = fs.readdirSync(directory); const sources = loadSources(directory, {});
    assert.equal(sources.feeds.find(f => f.name === 'Expenses and income').status, 'unreadable');
    assert.deepEqual(fs.readdirSync(directory), before); assert.equal(fs.readFileSync(path.join(directory, 'expenses.json'), 'utf8'), 'invalid');
    assert.equal(run(sources).totals.netProfit, null);
    fs.writeFileSync(path.join(directory, 'orders.json'), '{}');
    assert.equal(loadSources(directory, {}).feeds.find(f => f.name === 'Shopify orders').status, 'unreadable');
  } finally {
    assert.equal(path.dirname(path.resolve(directory)), path.resolve(os.tmpdir()));
    assert.ok(path.basename(directory).startsWith('pnl-read-test-'));
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
test('report API validates dates and exports without writes', async () => {
  const app = express(); app.use(createRouter({ read: () => fixtures(), clock: () => new Date('2026-10-08T06:00:00Z') }));
  const server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
  try {
    const base = 'http://127.0.0.1:' + server.address().port;
    assert.equal((await fetch(base + '/api/pl/report?from=2026-10-01&to=2026-10-09')).status, 400);
    const response = await fetch(base + '/api/pl/report?from=2026-10-01&to=2026-10-08'); assert.equal(response.headers.get('cache-control'), 'no-store'); assert.equal((await response.json()).totals.netProfit, 1542.53);
    const excel = await fetch(base + '/api/pl/report/export?from=2026-10-01&to=2026-10-08'); assert.match(excel.headers.get('content-type'), /spreadsheetml/);
  } finally { await new Promise(resolve => server.close(resolve)); }
});
