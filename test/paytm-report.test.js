const test = require('node:test');
const assert = require('node:assert/strict');
const { parsePaytmReport, summarizePayouts } = require('../modules/paytm-report');
const { registerPaytmReports } = require('../modules/paytm-reports-routes');

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
