const test = require('node:test');
const assert = require('node:assert/strict');
const { parsePaytmReport, summarizePayouts } = require('../modules/paytm-report');
const { registerPaytmReports } = require('../modules/paytm-reports-routes');
const { validateOrderLink, validatePayoutPosting } = require('../modules/paytm-accounting');

const header = 'Transaction_ID,Transaction_Date,Status,Amount,Commission,GST,Settled_Amount,Payout_ID,Payout_Date,UTR_No.,Payment_Mode\n';
const row = (id, date, amount, fee, gst, net, payout = 'P1') => `'${id}','${date}',SUCCESS,${amount},${fee},${gst},${net},'${payout}','18-09-2026',U123,UPI\n`;

test('imports successful Paytm payments, preserves paise and groups actual charges by payout', () => {
  const csv = header + row('T1', '2026-09-17 12:00:00', 10000, 50.25, 9.05, 9940.70) + row('T2', '17-09-2026 12:30:00', 10000, 0, 0, 10000) + row('T2', '17-09-2026 12:30:00', 10000, 0, 0, 10000) + "'T3','17-09-2026',FAILURE,100,0,0,100,'P1','18-09-2026',U123,UPI\n";
  const result = parsePaytmReport(Buffer.from(csv), 'test.csv', '2026-09-18');
  assert.equal(result.transactions.length, 2);
  const [payout] = summarizePayouts(result.transactions);
  assert.deepEqual([payout.count, payout.gross, payout.commission, payout.gst, payout.net], [2, 20000, 50.25, 9.05, 19940.70]);
});

test('rejects an unbalanced successful transaction instead of estimating fees', () => {
  assert.throws(() => parsePaytmReport(Buffer.from(header + row('T1', '2026-09-17', 100, 1, 0, 100)), 'test.csv', '2026-09-18'), /does not balance/);
});

test('shows old summary-only rows as not importable without transaction IDs', () => {
  const csv = 'Updated_Date,Status,Amount,Commission,GST,Settled_Amount\n2026-08-25,SUCCESS,5000,0,0,5000\n';
  const result = parsePaytmReport(Buffer.from(csv), 'old.csv', '2026-09-18');
  assert.equal(result.missingId, 1);
  assert.equal(result.transactions.length, 0);
  assert.match(result.warnings[0], /no transaction ID/);
});

test('Paytm report preview and confirmation import evidence without touching accounting balances', () => {
  const routes = {}, store = { paytmReportTransactions: {}, paytmReportDrafts: {}, paytmReportImports: [], paytmSettlements: [], bankStatements: { 'Axis Bank 3448': { transactions: {} } }, openingBalances: { 'Axis Bank 3448': 500 } };
  const router = { get: (url, handler) => { routes[url] = handler; }, post: (url, ...handlers) => { routes[url] = handlers.at(-1); } };
  registerPaytmReports(router, { loadStore: () => store, saveStore: () => {}, audit: () => {}, canAccess: () => true, upload: { array: () => () => {} }, view: s => ({ count: Object.keys(s.paytmReportTransactions).length }), today: () => '2026-09-18' });
  const req = { user: { username: 'owner' }, files: [{ originalname: 'paytm.csv', buffer: Buffer.from(header + row('T1', '2026-09-17', 100, 0, 0, 100)) }] };
  const response = () => ({ status() { return this; }, json(body) { this.body = body; return this; } });
  const preview = response(); routes['/api/expenses/paytm-reports/preview'](req, preview);
  assert.equal(preview.body.newTransactions, 1);
  assert.equal(Object.keys(store.paytmReportTransactions).length, 0);
  const confirmed = response(); routes['/api/expenses/paytm-reports/confirm']({ user: req.user, body: { draftId: preview.body.draftId } }, confirmed);
  assert.equal(confirmed.body.added, 1);
  assert.equal(store.openingBalances['Axis Bank 3448'], 500);
  assert.deepEqual(store.paytmSettlements, []);
  const repeated = response(); routes['/api/expenses/paytm-reports/preview'](req, repeated);
  assert.equal(repeated.body.newTransactions, 0);
  assert.equal(repeated.body.duplicates, 1);
});

test('reviewed Shopify links and exact UTR bank credit are required before payout posting', () => {
  const tx = { transactionId: 'T1', date: '2026-09-17', amount: 100, commission: 1, gst: 0.18, settledAmount: 98.82, payoutId: 'P1', payoutDate: '2026-09-18', utr: 'UTR123', posId: 'POS1' };
  const store = { paytmReportTransactions: { T1: tx }, paytmOrderLinks: {}, bankStatements: { 'Axis Bank 3448': { transactions: { b: { id: 'BTX-1', date: '2026-09-18', credit: 98.82, debit: 0, reference: 'PAYTM UTR123' } } } } };
  const orders = [{ id: 'O1', orderNumber: 2801, financialStatus: 'paid', note: 'Paytm T1' }];
  const sales = [{ id: 'SHOPIFY/O1', orderId: 'O1', orderNumber: '2801', date: '2026-09-17', account: 'Axis Bank 3448', amount: 100 }];
  assert.throws(() => validatePayoutPosting(store, 'P1', 'BTX-1', sales), /Every Paytm payment/);
  const link = validateOrderLink(store, tx, '#2801', orders, sales, '');
  store.paytmOrderLinks.T1 = link;
  assert.deepEqual([link.orderId, link.matchBasis], ['O1', 'transaction_id_in_shopify_note']);
  const result = validatePayoutPosting(store, 'P1', 'BTX-1', sales);
  assert.deepEqual([result.payout.gross, result.payout.net, result.bank.id], [100, 98.82, 'BTX-1']);
  store.bankDateOverrides = { 'SHOPIFY/O1': { bankTransactionId: 'BTX-2' } };
  assert.throws(() => validatePayoutPosting(store, 'P1', 'BTX-1', sales), /already linked/);
  delete store.bankDateOverrides;
  store.paytmPayoutPostings = [{ payoutId: 'P1', bankTransactionId: 'BTX-1' }];
  assert.throws(() => validatePayoutPosting(store, 'P1', 'BTX-1', sales), /already posted/);
});

test('manual amount-only link requires a reason and one-to-one order', () => {
  const tx = { transactionId: 'T1', date: '2026-09-17', amount: 100, posId: 'POS1' };
  const orders = [{ id: 'O1', orderNumber: 2801, financialStatus: 'paid', note: '' }];
  const sales = [{ id: 'SHOPIFY/O1', orderId: 'O1', orderNumber: '2801', date: '2026-09-17', account: 'Axis Bank 3448', amount: 100 }];
  assert.throws(() => validateOrderLink({}, tx, '2801', orders, sales, ''), /review reason/);
  const link = validateOrderLink({}, tx, '2801', orders, sales, 'Reviewed Paytm receipt against Shopify sale');
  assert.equal(link.matchBasis, 'owner_reviewed');
  assert.throws(() => validateOrderLink({ paytmOrderLinks: { T2: link } }, tx, '2801', orders, sales, 'Reviewed Paytm receipt against Shopify sale'), /already linked/);
});

test('posting a reviewed payout records exact fee and bank evidence once', () => {
  const tx = { transactionId: 'T1', date: '2026-09-17', amount: 100, commission: 1, gst: 0.18, settledAmount: 98.82, payoutId: 'P1', payoutDate: '2026-09-18', utr: 'UTR123', posId: 'POS1' };
  const store = { paytmReportTransactions: { T1: tx }, paytmOrderLinks: { T1: { transactionId: 'T1', orderId: 'O1', orderNumber: '2801', amount: 100 } }, bankStatements: { 'Axis Bank 3448': { transactions: { b: { id: 'BTX-1', date: '2026-09-18', credit: 98.82, debit: 0, reference: 'PAYTM UTR123' } } } } };
  const routes = {}, audits = [];
  const router = { get: (url, handler) => { routes[url] = handler; }, post: (url, ...handlers) => { routes[url] = handlers.at(-1); } };
  registerPaytmReports(router, { loadStore: () => store, saveStore: () => {}, audit: (_store, _req, action) => audits.push(action), canAccess: () => true, upload: { array: () => () => {} }, view: () => ({}), today: () => '2026-09-18', orders: () => [], saleRows: () => [{ id: 'SHOPIFY/O1', orderId: 'O1', orderNumber: '2801', date: '2026-09-17', account: 'Axis Bank 3448', amount: 100 }] });
  const reply = () => ({ code: 200, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } });
  const req = { user: { username: 'owner' }, body: { payoutId: 'P1', bankTransactionId: 'BTX-1' } };
  const first = reply(); routes['/api/expenses/paytm-reports/post-payout'](req, first);
  assert.equal(first.body.success, true);
  assert.deepEqual([store.paytmPayoutPostings[0].gross, store.paytmPayoutPostings[0].commission, store.paytmPayoutPostings[0].gst, store.paytmPayoutPostings[0].net], [100, 1, 0.18, 98.82]);
  assert.equal(store.reconciliationExpenses[0].amount, 1.18);
  assert.equal(store.bankDateOverrides[store.paytmPayoutPostings[0].id].bankTransactionId, 'BTX-1');
  assert.deepEqual(audits, ['PAYTM_PAYOUT_POSTED']);
  const again = reply(); routes['/api/expenses/paytm-reports/post-payout'](req, again);
  assert.equal(again.code, 400);
  assert.equal(store.paytmPayoutPostings.length, 1);
});
