const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../modules/procurement.js'), 'utf8');
const html = fs.readFileSync(path.join(__dirname, '../public/procurement.html'), 'utf8');
const { invoiceAmounts, allocateAmount, finalizedByPo } = require('../modules/lg-invoices');

function handlers(store) {
  const routes = {}, context = {
    router: { post: (route, fn) => routes[route] = fn, patch: (route, fn) => routes[route] = fn },
    canReconcileVendorBill: () => true, loadStore: () => store, saveStore: () => {},
    invoiceAmounts, allocateAmount, finalizedByPo, DATA_DIR: '/tmp', path,
    fs: { readFileSync: () => JSON.stringify({ procurementAccounting: { paymentsByPo: {} } }) },
    Date, Math, Number, String, Object, Array, Set
  };
  const start = source.indexOf("router.post('/api/procurement/combined-invoices'");
  const end = source.indexOf("router.get('/api/procurement/history'", start);
  vm.runInNewContext(source.slice(start, end), context);
  return routes;
}
function call(handler, body, id) {
  let status = 200, result;
  const res = { status(n) { status = n; return this; }, json(value) { result = value; } };
  handler({ body, params: { id }, user: { username: 'tester' } }, res);
  return { status, result };
}

test('combined invoice links bills from any vendors without modifying their POs', () => {
  const p1 = { id: 'PO-A', vendor: 'Vendor A', origin: 'china', lines: [{ qty: 2 }] };
  const p2 = { id: 'PO-B', vendor: 'vendor a', origin: 'china', lines: [{ qty: 3 }] };
  const p3 = { id: 'PO-C', vendor: 'Other vendor', origin: 'china' };
  const store = { pos: { 'PO-A': p1, 'PO-B': p2, 'PO-C': p3 }, combinedVendorInvoices: {} };
  const routes = handlers(store), create = routes['/api/procurement/combined-invoices'];
  const made = call(create, { poIds: ['PO-A', 'PO-C'] });
  assert.equal(made.status, 201);
  assert.deepEqual(Array.from(made.result.invoice.poIds), ['PO-A', 'PO-C']);
  assert.deepEqual(Array.from(made.result.invoice.vendors), ['Vendor A', 'Other vendor']);
  assert.equal(made.result.invoice.vendor, 'Multiple vendors');
  assert.equal(p1.lines[0].qty, 2);
  assert.equal(p2.lines[0].qty, 3);
  assert.equal(call(create, { poIds: ['PO-A', 'PO-C'] }).status, 409);
  const update = routes['/api/procurement/combined-invoices/:id'];
  const vendor = call(update, { childBills: {
    'PO-A': { billNumber: 'VA-1', totalQuantity: 2, billValueYuan: 100 },
    'PO-C': { billNumber: 'OC-2', totalQuantity: 3, billValueYuan: 150 }
  }, combined: { totalWeightGrams: 500, combinedFreightYuan: 20, localTransportationYuan: 5,
    fixedTransportationYuan: 2, extraChargesYuan: 1, exchangeRate: 15 } }, made.result.invoice.id);
  assert.equal(vendor.result.success, true);
  assert.equal(vendor.result.invoice.childBills['PO-C'].billValueYuan, 150);
  assert.equal(vendor.result.invoice.combined.exchangeRate, 15);
  assert.equal(p1.lines[0].qty, 2);
});

test('purchase UI offers invoice calculation for selected bills, vendor figures and component comparison', () => {
  for (const text of ['data-combine-po', 'Calculate invoices', 'data-payment-group', 'Enter Vendor Data',
    'data-combined-child', 'data-combined-field', 'data-compare-combined', 'Final total (INR)']) {
    assert.ok(html.includes(text), `Missing ${text}`);
  }
  assert.match(html, /!ids\.length\?' disabled'/);
});

test('one selected bill creates an invoice calculation and can be reconciled', () => {
  const po = { id: 'PO-SINGLE', vendor: 'Logistics vendor', origin: 'india', billNo: 'B-1', lines: [{ qty: 2, perPcsYuan: 50 }] };
  const store = { pos: { 'PO-SINGLE': po }, combinedVendorInvoices: {} };
  const routes = handlers(store), create = routes['/api/procurement/combined-invoices'];
  assert.equal(call(create, { poIds: [] }).status, 400);
  const made = call(create, { poIds: ['PO-SINGLE'] });
  assert.equal(made.status, 201);
  assert.deepEqual(Array.from(made.result.invoice.poIds), ['PO-SINGLE']);
  assert.equal(call(create, { poIds: ['PO-SINGLE'] }).status, 409);
  const saved = call(routes['/api/procurement/combined-invoices/:id'], { childBills: {
    'PO-SINGLE': { billNumber: 'LV-1', totalQuantity: 2, billValueYuan: 10 }
  }, combined: { totalWeightGrams: 0, localTransportationYuan: 0, fixedTransportationYuan: 0,
    extraChargesYuan: 0, combinedFreightYuan: 0, exchangeRate: 10 } }, made.result.invoice.id);
  assert.equal(saved.result.success, true);
  assert.equal(saved.result.invoice.childBills['PO-SINGLE'].billNumber, 'LV-1');
  assert.equal(po.billNo, 'B-1');
  const start = html.indexOf('    function combinedInvoiceComparison(inv)');
  const end = html.indexOf('    var selectedPaymentBills=', start);
  const detail = vm.runInNewContext(html.slice(start, end) + '\ncombinedInvoiceDetail;', {
    purchaseHistory: [po], settings: {}, esc: String, yuan: n => '¥' + n, money: n => '₹' + n
  });
  const rendered = detail(saved.result.invoice);
  assert.match(rendered, /Invoice calculation: B-1/);
  assert.match(rendered, /data-combined-child="PO-SINGLE"/);
  assert.match(rendered, /LG date.*LG bill number/);
  assert.match(rendered, /Total LG bill value/);
  assert.match(rendered, /Combined freight ₹ \(enter manually\)/);
  assert.match(rendered, /Finalize LG bill/);
  assert.doesNotMatch(rendered, /Combined invoice calculation:/);
});

test('combined invoice accepts bills with different purchase currencies', () => {
  const store = { pos: {
    'PO-CNY': { id: 'PO-CNY', vendor: 'China vendor', origin: 'china' },
    'PO-INR': { id: 'PO-INR', vendor: 'India vendor', origin: 'india' }
  }, combinedVendorInvoices: {} };
  const made = call(handlers(store)['/api/procurement/combined-invoices'], { poIds: ['PO-CNY', 'PO-INR'] });
  assert.equal(made.status, 201);
  assert.equal(made.result.invoice.origin, 'mixed');
});

test('combined comparison identifies child bill and charge differences', () => {
  const start = html.indexOf('    function combinedInvoiceComparison(inv)');
  const end = html.indexOf('    function combinedInvoiceDetail(inv)', start);
  const comparison = vm.runInNewContext(html.slice(start, end) + '\ncombinedInvoiceComparison;', {
    purchaseHistory: [
      { id: 'PO-A', billNo: '101', origin: 'china', exRate: 10, freightPerGram: 1,
        localTransportYuan: 3, lines: [{ qty: 2, perPcsYuan: 50, weightGrams: 10 }] },
      { id: 'PO-B', billNo: '102', origin: 'china', exRate: 10, freightPerGram: 1,
        localTransportYuan: 2, lines: [{ qty: 1, perPcsYuan: 40, weightGrams: 10 }] }
    ], settings: {}, esc: String, yuan: n => '¥' + n, money: n => '₹' + n
  });
  const out = comparison({ poIds: ['PO-A', 'PO-B'], childBills: {
    'PO-A': { totalQuantity: 2, billValueYuan: 105 },
    'PO-B': { totalQuantity: 1, billValueYuan: 40 }
  }, combined: { totalWeightGrams: 30, combinedFreightYuan: 4, localTransportationYuan: 5,
    fixedTransportationYuan: 2, extraChargesYuan: 1, exchangeRate: 10 } });
  assert.match(out, /101 value \(Yuan\).*¥100.*¥105.*¥5/);
  assert.match(out, /Freight \(Yuan\).*¥3.*¥4.*¥1/);
  assert.match(out, /Final total \(Yuan\).*¥148.*¥157.*¥9/);
  assert.match(out, /Final total \(INR\).*₹1480.*₹1570.*₹90/);
});

test('finalized LG bill snapshots the chosen amount and allocates it to original POs', () => {
  const store = { settings: { exRate: 10 }, pos: {
    'PO-A': { id: 'PO-A', vendor: 'A', origin: 'china', exRate: 10, lines: [{ qty: 1, perPcsYuan: 100 }] },
    'PO-B': { id: 'PO-B', vendor: 'B', origin: 'china', exRate: 10, lines: [{ qty: 1, perPcsYuan: 100 }] }
  }, combinedVendorInvoices: {} };
  const routes = handlers(store), made = call(routes['/api/procurement/combined-invoices'], { poIds: ['PO-A', 'PO-B'] });
  const id = made.result.invoice.id;
  const saved = call(routes['/api/procurement/combined-invoices/:id'], {
    lgDate: '2026-09-18', lgBillNumber: 'LG-501', childBills: {
      'PO-A': { totalQuantity: 1, billValueYuan: 110 }, 'PO-B': { totalQuantity: 1, billValueYuan: 100 }
    }, combined: { totalWeightGrams: 0, combinedFreightYuan: 10, localTransportationYuan: 0,
      fixedTransportationYuan: 0, extraChargesYuan: 0, exchangeRate: 10 }
  }, id);
  assert.equal(saved.result.success, true);
  const finalized = call(routes['/api/procurement/combined-invoices/:id/finalize'], { basis: 'vendor' }, id);
  assert.equal(finalized.result.success, true);
  assert.equal(finalized.result.invoice.finalized.amountInr, 2200);
  assert.equal(Object.values(finalized.result.invoice.finalized.allocations).reduce((a, b) => a + b, 0), 2200);
  assert.equal(finalizedByPo(store)['PO-A'].invoice.id, id);
  assert.equal(finalized.result.invoice.lgDate, '2026-09-18');
  assert.equal(finalized.result.invoice.lgBillNumber, 'LG-501');
  assert.equal(call(routes['/api/procurement/combined-invoices/:id/finalize'], { basis: 'purchase' }, id).status, 409);
  assert.equal(call(routes['/api/procurement/combined-invoices/:id'], { childBills: {}, combined: {} }, id).status, 409);
  assert.equal(store.pos['PO-A'].lines[0].qty, 1);
});

test('manual INR freight is included once in vendor total and converted for comparison', () => {
  const invoice = { poIds: ['PO-A', 'PO-B'], childBills: {
    'PO-A': { totalQuantity: 10, billValueYuan: 2015 },
    'PO-B': { totalQuantity: 20, billValueYuan: 3825 }
  }, combined: { combinedFreightInr: 28350, localTransportationYuan: 75,
    fixedTransportationYuan: 50, extraChargesYuan: 0, exchangeRate: 15.33 } };
  const pos = { 'PO-A': { id: 'PO-A', lines: [] }, 'PO-B': { id: 'PO-B', lines: [] } };
  const amounts = invoiceAmounts(invoice, pos);
  assert.equal(amounts.vendorBillYuan, 5840);
  assert.equal(amounts.vendorTotalYuan, 5965 + 28350 / 15.33);
  assert.equal(amounts.vendorAmountInr, Math.round(5965 * 15.33 + 28350));
});
