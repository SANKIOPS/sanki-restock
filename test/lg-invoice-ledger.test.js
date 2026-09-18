const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { finalizedByPo } = require('../modules/lg-invoices');
const { purchaseBillingAmount, purchasePaymentStatus } = require('../modules/purchase-payment-status');

test('finalized LG bill appears once in sourcing ledger projection with its LG date and chosen amount', () => {
  const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
  const start = source.indexOf('function procurementPayables('), end = source.indexOf('function ledgerMeta(', start);
  const proc = { settings: {}, pos: {
    'PO-A': { id: 'PO-A', vendor: 'A', billNo: '101', datePurchase: '2026-09-01', origin: 'india', lines: [{ qty: 1, perPcsYuan: 100 }] },
    'PO-B': { id: 'PO-B', vendor: 'B', billNo: '102', datePurchase: '2026-09-02', origin: 'india', lines: [{ qty: 1, perPcsYuan: 200 }] },
    'PO-C': { id: 'PO-C', vendor: 'C', billNo: '103', datePurchase: '2026-09-03', origin: 'india', lines: [{ qty: 1, perPcsYuan: 50 }] }
  }, combinedVendorInvoices: {
    'CVI-1': { id: 'CVI-1', poIds: ['PO-A', 'PO-B'], vendors: ['A', 'B'], lgBillNumber: 'LG-501', lgDate: '2026-09-18',
      finalized: { basis: 'vendor', amountInr: 330, allocations: { 'PO-A': 110, 'PO-B': 220 } } }
  } };
  const accounting = { procurementAccounting: { mediator: 'Logistics Mediator', paymentsByPo: {
    'PO-A': { payments: [{ id: 'PAY-1', batchPaymentId: 'PPB-1', amount: 40, date: '2026-09-20' }] },
    'PO-B': { payments: [{ id: 'PAY-1', batchPaymentId: 'PPB-1', amount: 60, date: '2026-09-20' }] }
  } } };
  const context = { procurementAccounting: s => s.procurementAccounting, loadProcurementStore: () => proc,
    finalizedByPo, purchaseBillingAmount, round0: Math.round, num: x => Number(x) || 0, poCostBreakdown: () => ({ lines: [] }) };
  const project = vm.runInNewContext(source.slice(start, end) + '\nprocurementLedgerPayables;', context);
  const rows = project(accounting, true);
  assert.equal(rows.length, 2);
  const lg = rows.find(row => row.id === 'CVI-1');
  assert.equal(lg.date, '2026-09-18');
  assert.equal(lg.billNo, 'LG-501');
  assert.equal(lg.amount, 330);
  assert.equal(lg.paidAmount, 100);
  assert.equal(lg.balanceDue, 230);
  assert.equal(lg.payments.length, 1);
  assert.equal(lg.payments[0].amount, 100);
  assert.deepEqual(Array.from(lg.poIds), ['PO-A', 'PO-B']);
  assert.equal(lg.purchaseBills.length, 2);
  assert.equal(rows.find(row => row.id === 'PO-C').amount, 50);
  assert.equal(purchasePaymentStatus(proc.pos['PO-A'], accounting, true, {}, 110).amount, 110);
});

test('pending payments contains only finalized LG bill rows, filtered by LG date', () => {
  const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
  const start = source.indexOf("router.get('/api/expenses/pending-payments'");
  const end = source.indexOf('\n});', start) + 4;
  let handler;
  const context = { router: { get: (_route, fn) => { handler = fn; } }, isAdmin: () => true,
    loadStore: () => ({ expenses: {}, procurementAccounting: { mediator: 'Logistics Mediator' } }),
    normalizedNature: x => x, approvalNatures: () => ['SANKI'], canApproveExpenseNature: () => true,
    procurementLedgerPayables: () => [
      { id: 'CVI-1', source: 'procurement_lg', vendor: 'Logistics Mediator', billNo: 'LG-501', date: '2026-09-18', balanceDue: 2200, paidAmount: 0 },
      { id: 'PO-9', source: 'procurement', vendor: 'Logistics Mediator', date: '2026-09-01', balanceDue: 100, paidAmount: 0 }
    ], procurementAccounting: s => s.procurementAccounting, round0: Math.round, num: x => Number(x) || 0,
    Date, Object, String, Math };
  vm.runInNewContext(source.slice(start, end), context);
  let response;
  handler({ query: { nature: 'SANKI', from: '2026-09-18', to: '2026-09-18' } }, { json: value => { response = value; } });
  assert.deepEqual(Array.from(response.purchases, item => item.id), ['CVI-1']);
  assert.equal(response.totalOutstanding, 2200);
});
