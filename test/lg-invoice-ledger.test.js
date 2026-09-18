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
  assert.equal(rows.length, 1);
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
  assert.equal(rows.find(row => row.id === 'PO-C'), undefined);
  assert.equal(lg.purchaseBills[0].comparison.quantity, 1);
  assert.equal(lg.purchaseBills[0].comparison.billValueYuan, 100);
  assert.deepEqual(Object.keys(lg.childBills), []);
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

test('sourcing ledger LG bill expands to show purchase and vendor calculation differences', () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
  const start = html.indexOf('    function procurementCostHtml(raw)');
  const end = html.indexOf('    function vendorLedgerTable(v)', start);
  const render = vm.runInNewContext(html.slice(start, end) + '\nprocurementCostHtml;', {
    esc: String, fmt: n => '₹' + n
  });
  const output = render({ source: 'procurement_lg', id: 'CVI-1', billNo: 'LG-001', date: '2026-09-18', amount: 90000,
    poIds: ['PO-A', 'PO-B'], finalized: { basis: 'vendor' }, combined: {
      combinedFreightInr: 100, localTransportationYuan: 5, fixedTransportationYuan: 0,
      extraChargesYuan: 0, exchangeRate: 10, totalWeightGrams: 30
    }, childBills: { 'PO-A': { totalQuantity: 1, billValueYuan: 2015 },
      'PO-B': { totalQuantity: 1, billValueYuan: 3825 } }, purchaseBills: [
      { id: 'PO-A', billNo: 'SBGB174', amount: 50000, comparison: { quantity: 1, billValueYuan: 2000 }, costBreakdown: { lines: [] } },
      { id: 'PO-B', billNo: 'SBGB171', amount: 40000, comparison: { quantity: 1, billValueYuan: 3800 }, costBreakdown: { lines: [] } }
    ] });
  assert.match(output, /View LG bill · LG-001/);
  assert.match(output, /Vendor bill subtotal <b>¥5,840<\/b>/);
  assert.match(output, /Our data vs vendor invoice/);
  assert.match(output, /Final total \(INR\)/);
  assert.match(output, /SBGB174 bill value \(Yuan\)/);
});

test('LG payment records partial and final allocations under one reference per payment', () => {
  const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
  const start = source.indexOf("router.post('/api/expenses/procurement-lg/:id/pay'");
  const end = source.indexOf("router.post('/api/expenses/procurement-payables/:id/pay'", start);
  let handler, saved = 0, nonce = 0;
  const store = { procurementAccounting: { mediator: 'LG', paymentsByPo: {} } };
  const project = () => {
    const paid = id => ((store.procurementAccounting.paymentsByPo[id] || {}).payments || []).reduce((n, p) => n + p.amount, 0);
    const children = [{ id: 'PO-A', billNo: '101', supplier: 'A', balanceDue: 600 - paid('PO-A') },
      { id: 'PO-B', billNo: '102', supplier: 'B', balanceDue: 400 - paid('PO-B') }];
    return [{ id: 'CVI-1', source: 'procurement_lg', finalized: { basis: 'vendor' }, poIds: ['PO-A', 'PO-B'],
      billNo: 'LG-001', purchaseBills: children, balanceDue: children.reduce((n, p) => n + p.balanceDue, 0),
      paidAmount: paid('PO-A') + paid('PO-B') }];
  };
  const context = { router: { post: (_, fn) => { handler = fn; } }, canApprove: () => true,
    loadStore: () => store, procurementLedgerPayables: project, round0: Math.round,
    allowedPayingAccount: (_req, _nature, account) => account === 'Bank 1234' ? account : undefined,
    proofList: (many, one) => many || (one ? [one] : []), PAYMENT_TYPES: ['UPI','Bank Transfer'],
    crypto: { randomBytes: () => Buffer.from(String(++nonce).padStart(3, '0')) }, procurementAccounting: s => s.procurementAccounting,
    audit: () => {}, saveStore: () => { saved++; }, Date, Math, Number, String, Array, Set, Buffer };
  vm.runInNewContext(source.slice(start, end), context);
  const pay = (amount, method = 'UPI') => { let status = 200, body;
    handler({ params: { id: 'CVI-1' }, body: { amount, account: 'Bank 1234', date: '2026-09-18',
      reference: 'UTR-123', paymentProofs: ['/proof.jpg'], paymentType: method }, user: { username: 'owner' } },
    { status(n) { status = n; return this; }, json(x) { body = x; } }); return { status, body }; };
  assert.equal(pay(1001).status, 400);
  const first = pay(250);assert.equal(first.body.success, true);assert.equal(first.body.payable.balanceDue, 750);
  assert.equal(first.body.allocations.reduce((n, a) => n + a.amount, 0), 250);
  assert.equal(new Set(first.body.allocations.map(a => a.poId)).size, 2);
  const second = pay(750, 'NEFT');assert.equal(second.body.success, true);assert.equal(second.body.payable.balanceDue, 0);
  assert.equal(pay(1).status, 400);
  assert.equal(saved, 2);
  const payments = Object.values(store.procurementAccounting.paymentsByPo).flatMap(x => x.payments);
  assert.equal(new Set(payments.map(x => x.batchPaymentId)).size, 2);
  assert.ok(payments.every(x => x.combinedInvoiceId === 'CVI-1' && x.bankReference === 'UTR-123'));
  assert.ok(payments.some(x => x.paymentType === 'NEFT'));
});

test('legacy Logistics Mediator label appears as LG without changing payment history', () => {
  const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
  const start = source.indexOf('function procurementAccounting(s)');
  const end = source.indexOf('function loadProcurementStore()', start);
  const migrate = vm.runInNewContext(source.slice(start, end) + '\nprocurementAccounting;');
  const history = { 'PO-A': { payments: [{ amount: 250 }] } };
  const store = { procurementAccounting: { mediator: 'Logistics Mediator', paymentsByPo: history } };
  assert.equal(migrate(store).mediator, 'LG');
  assert.equal(store.procurementAccounting.paymentsByPo, history);
  assert.equal(migrate({}).mediator, 'LG');
});
