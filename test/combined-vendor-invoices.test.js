const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../modules/procurement.js'), 'utf8');
const html = fs.readFileSync(path.join(__dirname, '../public/procurement.html'), 'utf8');

function handlers(store) {
  const routes = {}, context = {
    router: { post: (route, fn) => routes[route] = fn, patch: (route, fn) => routes[route] = fn },
    canReconcileVendorBill: () => true, loadStore: () => store, saveStore: () => {},
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

test('purchase UI offers combined invoice selection, vendor figures and component comparison', () => {
  for (const text of ['data-combine-po', 'Combine Invoices', 'data-payment-group', 'Enter Vendor Data',
    'data-combined-child', 'data-combined-field', 'data-compare-combined', 'Final total (INR)']) {
    assert.ok(html.includes(text), `Missing ${text}`);
  }
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
