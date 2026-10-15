'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// The expense module initializes and repairs its store at import time. Every
// possible financial source is isolated before importing it: no live files,
// Shopify requests, or production accounts are used by these regressions.
const testRoot = path.join(__dirname, '..', 'tmp', 'paytm-rounding-integration');
fs.mkdirSync(testRoot, { recursive: true });
const tempDir = fs.mkdtempSync(path.join(testRoot, 'synthetic-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
process.env.SHOPIFY_STORE = 'synthetic-rounding-test.myshopify.com';
process.env.SHOPIFY_ACCESS_TOKEN = '';
for (const [name, file] of Object.entries({
  ORDERS_PATH: 'orders.json', SALES_PATH: 'sales.json',
  PROCUREMENT_PATH: 'procurement.json', INCENTIVES_PATH: 'incentives.json',
  VELOCITY_REMITTANCE_PATH: 'velocity_remittance.json'
})) process.env[name] = path.join(tempDir, file);
const expenseFile = path.join(tempDir, 'expenses.json');
fs.writeFileSync(expenseFile, '{}');
for (const file of ['orders.json', 'sales.json', 'procurement.json', 'incentives.json', 'velocity_remittance.json', 'salary.json', 'credit-cards.json']) {
  fs.writeFileSync(path.join(tempDir, file), '{}');
}
const { router } = require('../modules/expenses');
const { buildReport } = require('../modules/pnl-report');
const { shopifyClient } = require('../modules/shopify-client');
let shopifyRequests = 0;
shopifyClient.request = async () => {
  shopifyRequests++;
  throw new Error('Network access is disabled in the rounding integration fixture');
};
const baseline = JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
const copy = value => JSON.parse(JSON.stringify(value));
const money = value => Math.round(value * 100) / 100;
const fixtures = [
  { number: 2824, base: 3198.99, actual: 3199, date: '2026-09-20', settledDate: '2026-09-21', utr: 'SYNTHETIC-21' },
  { number: 2825, base: 3199.98, actual: 3200, date: '2026-09-20', settledDate: '2026-09-21', utr: 'SYNTHETIC-21' },
  { number: 2833, base: 4743.99, actual: 4745, date: '2026-09-25', settledDate: '2026-09-26', utr: 'SYNTHETIC-26' },
  { number: 2834, base: 2299.99, actual: 2300, date: '2026-09-25', settledDate: '2026-09-26', utr: 'SYNTHETIC-26' },
  { number: 2840, base: 198, actual: 200, date: '2026-09-26', settledDate: '2026-09-27', utr: 'SYNTHETIC-27' }
];

function setup(rows = fixtures) {
  const store = copy(baseline), orders = {};
  Object.assign(store, {
    expenses: {}, adjustments: [], receipts: [], transfers: [], vendorAdvances: [],
    bankTruthMovements: [], receivables: {}, salesRefunds: [],
    paytmReportTransactions: {}, paytmOrderLinks: {}, paytmRoundingReviews: {},
    paytmShopifyPayments: {}, paytmManualResolutions: {},
    paytmSettlements: [], paytmPayoutPostings: [], paytmVerifiedSettlements: [],
    bankDateOverrides: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {},
    bankStatements: { 'Axis Bank 3448': { transactions: {}, imports: [] } },
    openingBalances: { 'Axis Bank 3448': 100, 'Paytm Settlement Clearing': 0 },
    saleAllocationOverrides: {}, saleComponentAmountOverrides: {}
  });
  rows.forEach((x, index) => {
    const orderId = String(9000000 + x.number), transactionId = 'SYNTHETIC-TX-' + x.number;
    x = { ...x, orderId, transactionId };
    orders[orderId] = {
      id: orderId, number: x.number, orderNumber: x.number, name: '#' + x.number,
      processedAt: x.date + 'T12:00:00Z', createdAt: x.date + 'T12:00:00Z',
      financialStatus: 'paid', channel: 'POS', total: x.total ?? x.base,
      paymentGateways: x.total ? ['Cash', 'Paytm'] : ['Paytm'],
      taxesIncluded: true, shipping: 0, discount: 0,
      note: 'Synthetic Paytm reference ' + (100000 + index),
      lineItems: [{ id: 'LINE-' + x.number, sku: 'SYNTHETIC-SKU-' + x.number, qty: 1, price: x.total ?? x.base, discount: 0 }],
      paymentTransactions: [
        { id: 'PAYTM-' + x.number, kind: 'sale', status: 'success', gateway: 'paytm', amount: x.base },
        ...(x.total ? [{ id: 'CASH-' + x.number, kind: 'sale', status: 'success', gateway: 'Cash', amount: money(x.total - x.base) }] : [])
      ]
    };
    store.paytmReportTransactions[transactionId] = {
      transactionId, date: x.date, amount: x.actual, commission: 0, platformFee: 0,
      gst: 0, settledAmount: x.actual, payoutId: x.utr, utr: x.utr,
      settledDate: x.settledDate, isCustomerPayment: true
    };
    store.paytmOrderLinks[transactionId] = {
      transactionId, orderId, orderNumber: String(x.number), amount: x.actual,
      matchBasis: 'owner_reviewed', reason: 'Synthetic receipt linked for rounding regression'
    };
  });
  // These two unrelated live-style blockers must not be repaired or finalized
  // as a side-effect of reviewing the requested rounding transactions.
  orders['9002776'] = {
    id: '9002776', number: 2776, orderNumber: 2776, name: '#2776',
    financialStatus: 'paid', channel: 'POS', total: 4998, cashAmount: 4998,
    paymentGateways: ['Cash', 'Paytm'], note: 'Paytm synthetic 164074',
    createdAt: '2026-09-04T12:00:00Z'
  };
  store.paytmReportTransactions['OUTSIDE-2776'] = {
    transactionId: 'OUTSIDE-2776', date: '2026-09-04', amount: 4998,
    commission: 0, platformFee: 0, gst: 0, settledAmount: 4998,
    payoutId: 'SYNTHETIC-UNVERIFIED', utr: 'SYNTHETIC-UNVERIFIED',
    settledDate: '2026-09-05', isCustomerPayment: true
  };
  store.paytmOrderLinks['OUTSIDE-2776'] = {
    transactionId: 'OUTSIDE-2776', orderId: '9002776', orderNumber: '2776', amount: 4998
  };
  store.paytmReportTransactions['OUTSIDE-RENTAL'] = {
    transactionId: 'OUTSIDE-RENTAL', date: '2026-08-25', amount: 352,
    commission: 0, platformFee: 0, gst: 0, settledAmount: 352,
    payoutId: 'SYNTHETIC-RENTAL', utr: 'SYNTHETIC-RENTAL', settledDate: '2026-08-26',
    isCustomerPayment: true, comments: 'Monthly rental requires a separate review'
  };
  store.paytmExcludedTransactions = { 'OUTSIDE-RENTAL': { reason: 'Synthetic monthly rental excluded from Shopify' } };
  fs.writeFileSync(expenseFile, JSON.stringify(store));
  fs.writeFileSync(process.env.ORDERS_PATH, JSON.stringify({ orders }));
  return { store, orders };
}

function invoke(method, url, { body = {}, query = {}, role = 'owner' } = {}) {
  const layer = router.stack.find(item => item.route && item.route.path === url && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'Registered route: ' + url);
  const response = { status: 200, body: undefined };
  const req = { method, path: url, body, query, headers: {},
    user: { username: role === 'admin' ? 'prashant' : 'synthetic-' + role, role, roles: [role] } };
  const pending = layer.route.stack.at(-1).handle(req, {
    status(value) { response.status = value; return this; },
    json(value) { response.body = value; return this; }
  });
  if (pending && typeof pending.then === 'function') response.finished = pending;
  return response;
}
const current = () => JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
const ledger = () => invoke('GET', '/api/expenses/account-ledger', {
  query: { nature: 'SANKI', account: 'Paytm Settlement Clearing', from: '2026-09-20', to: '2026-09-27' }
}).body;
const review = (row, extra = {}) => invoke('POST', '/api/expenses/paytm-reports/rounding-review', {
  body: { transactionId: 'SYNTHETIC-TX-' + row.number, difference: money(row.actual - row.base), reason: 'Owner confirmed the small Paytm receipt rounding difference', ...extra }
});
const totalCredits = result => money(result.entries.filter(x => x.kind !== 'opening').reduce((sum, x) => sum + Number(x.credit || 0), 0));

test('five explicit rounding reviews persist separate income without changing Shopify sales or double-counting Paytm credits', () => {
  const { orders } = setup();
  const originalOrders = fs.readFileSync(process.env.ORDERS_PATH, 'utf8');
  const before = ledger();
  assert.equal(before.success, true);
  assert.equal(totalCredits(before), 13640.95);
  const initialReport = invoke('GET', '/api/expenses/paytm-reports').body;
  for (const row of fixtures) {
    const preview = initialReport.orderMatches.find(x => x.transactionId === 'SYNTHETIC-TX-' + row.number).roundingReview;
    assert.deepEqual([preview.eligible, preview.approved, preview.receiptAmount, preview.sourceAmount, preview.difference],
      [true, false, row.actual, row.base, money(row.actual - row.base)]);
  }
  for (const row of fixtures) {
    const response = review(row);
    assert.equal(response.status, 200, JSON.stringify(response.body));
    assert.equal(response.body.success, true);
    assert.equal(response.body.created, true);
    assert.equal(response.body.receipt.receiptType, 'other_income');
    assert.equal(response.body.receipt.category, 'Rounding Off');
    assert.equal(response.body.receipt.paytmRoundingTransactionId, 'SYNTHETIC-TX-' + row.number);
    assert.equal(response.body.receipt.amount, money(row.actual - row.base));
    assert.equal(response.body.receipt.date, row.date);
  }
  const saved = current(), after = ledger();
  assert.equal(saved.receipts.length, 5);
  assert.equal(money(saved.receipts.reduce((sum, receipt) => sum + receipt.amount, 0)), 3.05);
  assert.equal(totalCredits(after), 13644);
  const customerSales = after.entries.filter(x => x.kind === 'paytm_customer_receipt');
  assert.equal(money(customerSales.reduce((sum, x) => sum + x.credit, 0)), 13640.95);
  assert.equal(after.entries.filter(x => x.kind === 'receipt').length, 5);
  assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), originalOrders, 'Shopify source orders remain byte-for-byte unchanged');
  const report = buildReport({
    orders: { orders }, expenses: saved, purchases: {}, salary: {}, manualSales: {}, feeds: [],
    opening: { lots: fixtures.map(x => ({ id: 'LOT-' + x.number, sku: 'SYNTHETIC-SKU-' + x.number, qty: 1, unitCost: 1, verified: true })) }
  }, { from: '2026-09-20', to: '2026-09-27' });
  assert.equal(report.totals.otherIncome, 3.05, 'Rounding is income, not altered gross sales or an unclassified balance adjustment');
  assert.equal(report.totals.grossSales, 13640.95);
});

test('approval is idempotent and never finalizes payouts, posts bank credits, or touches unrelated blockers', () => {
  const { store } = setup();
  const selected = fixtures[0];
  const first = review(selected);
  assert.equal(first.status, 200, JSON.stringify(first.body));
  const one = current();
  const again = review(selected);
  assert.equal(again.status, 200, JSON.stringify(again.body));
  assert.equal(again.body.success, true);
  assert.equal(again.body.created, false);
  const repeated = current();
  assert.deepEqual(repeated.receipts, one.receipts);
  assert.equal(Object.keys(repeated.paytmRoundingReviews).length, 1);
  assert.deepEqual(repeated.paytmReportTransactions, store.paytmReportTransactions);
  assert.deepEqual(repeated.paytmOrderLinks, store.paytmOrderLinks);
  assert.deepEqual(repeated.paytmExcludedTransactions, store.paytmExcludedTransactions);
  assert.deepEqual(repeated.paytmVerifiedSettlements, []);
  assert.deepEqual(repeated.paytmPayoutPostings, []);
  assert.deepEqual(repeated.paytmSettlements, []);
  assert.deepEqual(repeated.bankStatements, store.bankStatements);
  assert.deepEqual(repeated.bankDateOverrides, store.bankDateOverrides);
});

test('unauthorized or stale-difference requests cannot create rounding records', () => {
  setup();
  // Warm existing store migrations before testing non-mutating denied requests.
  ledger();
  const before = fs.readFileSync(expenseFile, 'utf8');
  const denied = invoke('POST', '/api/expenses/paytm-reports/rounding-review', {
    role: 'claimant', body: { transactionId: 'SYNTHETIC-TX-2824', difference: .01, reason: 'Not authorized to review financial receipts' }
  });
  assert.equal(denied.status, 403);
  assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
  const stale = review(fixtures[0], { difference: .02 });
  assert.equal(stale.status >= 400, true);
  assert.equal(stale.body.success, false);
  assert.equal(fs.readFileSync(expenseFile, 'utf8'), before, 'A changed review difference cannot silently write a receipt');
});

test('mixed-tender fallback uses approved base rather than counting the rounded link and the income twice', () => {
  const row = { number: 2900, base: 198, actual: 200, total: 1000,
    date: '2026-09-26', settledDate: '2026-09-27', utr: 'SYNTHETIC-MIXED' };
  const { orders } = setup([row]);
  assert.equal(totalCredits(ledger()), 198);
  const approved = review(row);
  assert.equal(approved.status, 200, JSON.stringify(approved.body));
  assert.equal(totalCredits(ledger()), 200);
  const order = orders[String(9000000 + row.number)];
  delete order.paymentTransactions;
  order.cashAmount = 802;
  order.paytmAmount = 198;
  fs.writeFileSync(process.env.ORDERS_PATH, JSON.stringify({ orders }));
  const fallback = ledger();
  assert.equal(totalCredits(fallback), 200, '₹198 original receipt plus ₹2 income, not ₹200 linked receipt plus ₹2 income');
  assert.equal(fallback.entries.find(x => x.kind === 'paytm_customer_receipt').credit, 198);
  assert.equal(fallback.entries.find(x => x.kind === 'receipt').credit, 2);
});

test('saved report and ledger refreshes do not reapply or silently approve rounding', () => {
  setup();
  ledger();
  const before = fs.readFileSync(expenseFile, 'utf8');
  const preview = invoke('GET', '/api/expenses/paytm-reports');
  assert.equal(preview.status, 200);
  assert.equal(preview.body.success, true);
  ledger();
  assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
  assert.deepEqual(current().receipts, []);
  assert.deepEqual(current().paytmRoundingReviews, {});
});

test('synthetic settlement finalization accepts only explicitly treated rounding and retains unrelated blockers', () => {
  setup();
  const blocked = invoke('POST', '/api/expenses/paytm-reports/finalize-settlement', {
    body: { payoutId: 'SYNTHETIC-21' }
  });
  assert.equal(blocked.status, 400, 'Small mismatches cannot finalize before their explicit treatment');
  for (const row of fixtures) assert.equal(review(row).status, 200);
  const preview = invoke('GET', '/api/expenses/paytm-reports').body;
  for (const row of fixtures) {
    const matched = preview.orderMatches.find(x => x.transactionId === 'SYNTHETIC-TX-' + row.number);
    assert.equal(matched.roundingReview.approved, true);
  }
  // This is a synthetic-only workflow check; it never finalizes live records.
  const finalized = invoke('POST', '/api/expenses/paytm-reports/finalize-all');
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  assert.equal(finalized.body.finalized, 3);
  assert.equal(finalized.body.skipped, 2, '#2776 and the excluded rental remain blocked');
  const saved = current();
  assert.deepEqual(saved.paytmVerifiedSettlements.map(x => x.utr).sort(), ['SYNTHETIC-21', 'SYNTHETIC-26', 'SYNTHETIC-27']);
  assert.equal(saved.receipts.length, 5);
  assert.equal(money(saved.receipts.reduce((sum, x) => sum + x.amount, 0)), 3.05);
  assert.deepEqual(saved.paytmPayoutPostings, [], 'Report finalization still cannot create an Axis bank posting');
  const again = invoke('POST', '/api/expenses/paytm-reports/finalize-all');
  assert.equal(again.body.finalized, 0);
  assert.equal(current().paytmVerifiedSettlements.length, 3);
  assert.equal(current().receipts.length, 5);
});

test('audited rounding income is not editable through the ordinary ledger-entry workflow', () => {
  setup();
  const row = fixtures[0], approved = review(row);
  assert.equal(approved.status, 200);
  const entry = ledger().entries.find(x => x.id === approved.body.receipt.id);
  assert.equal(entry.kind, 'receipt');
  assert.equal(entry.credit, .01);
  assert.equal(entry.editable, false, 'The ordinary ledger edit control must not be offered for audited rounding');
  const before = fs.readFileSync(expenseFile, 'utf8');
  const edited = invoke('PATCH', '/api/expenses/ledger-entry', {
    body: { id: approved.body.receipt.id, kind: 'receipt', nature: 'SANKI',
      account: 'Paytm Settlement Clearing', amount: .02, date: row.date,
      source: 'Ordinary ledger correction', note: 'Attempt to edit the audited rounding receipt',
      reason: 'Synthetic lifecycle protection test' }
  });
  assert.equal(edited.status, 409, JSON.stringify(edited.body));
  assert.equal(edited.body.success, false);
  assert.match(edited.body.error, /Paytm transaction/);
  assert.equal(fs.readFileSync(expenseFile, 'utf8'), before, 'Denied editing preserves receipt, evidence, approval and audit');
});

test('rounded receipt lifecycle locks prevent exclusion, relinking, payment sync and split changes without loss', async () => {
  setup();
  const row = fixtures[0];
  assert.equal(review(row).status, 200);
  ledger();
  const before = fs.readFileSync(expenseFile, 'utf8');
  const ordersBefore = fs.readFileSync(process.env.ORDERS_PATH, 'utf8');
  const transactionId = 'SYNTHETIC-TX-' + row.number, orderId = String(9000000 + row.number);
  for (const [url, body, status] of [
    ['/api/expenses/paytm-reports/exclude', { transactionId, reason: 'Attempt to exclude an audited rounding receipt' }, 409],
    ['/api/expenses/paytm-reports/link-order', { transactionId, orderId: String(9000000 + fixtures[1].number), reason: 'Attempt to relink an audited rounding receipt' }, 400],
    ['/api/expenses/paytm-reports/sync-shopify-payments', { orderId }, 409],
    ['/api/expenses/sale-allocation', { saleId: 'SHOPIFY/' + orderId, cashAmount: 100, reason: 'Attempt to change an audited rounding payment split' }, 409]
  ]) {
    const denied = invoke('POST', url, { body });
    if (denied.finished) await denied.finished;
    assert.equal(denied.status, status, url + ': ' + JSON.stringify(denied.body));
    assert.equal(denied.body.success, false);
    assert.match(denied.body.error, /audited rounding receipt/);
    assert.equal(fs.readFileSync(expenseFile, 'utf8'), before, url + ' must preserve all audited financial evidence');
    assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), ordersBefore);
  }
  assert.equal(shopifyRequests, 0, 'The audit lock must reject payment sync before any Shopify request');
});
