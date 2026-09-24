const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { parsePaytmReport, summarizePayouts } = require('../modules/paytm-report');
const { registerPaytmReports } = require('../modules/paytm-reports-routes');
const { summarizeShopifyPayments, autoMatchShopifyNotes, validateOrderLink, validatePayoutPosting } = require('../modules/paytm-accounting');

test('automatically matches a unique six-digit Shopify note suffix but never a collision or wrong amount', () => {
  const orders=[
    {id:'O1',number:2845,createdAt:'2026-09-19T10:00:00Z',financialStatus:'paid',total:15000,note:'Paytm 123456'},
    {id:'O2',number:2846,createdAt:'2026-09-19T10:00:00Z',financialStatus:'paid',total:1000,note:'Paytm 654321'}
  ];
  const tx={transactionId:'202609190000123456',date:'2026-09-19',amount:5000};
  const store={};
  assert.equal(autoMatchShopifyNotes(store,{[tx.transactionId]:tx},orders,[]).length,1);
  assert.equal(store.paytmOrderLinks[tx.transactionId].orderId,'O1');
  assert.equal(store.paytmOrderLinks[tx.transactionId].transactionSuffix,'123456');
  assert.equal(autoMatchShopifyNotes({}, {[tx.transactionId]:tx},[...orders,{...orders[1],id:'O3',note:'another 123456'}],[]).length,0);
  assert.equal(autoMatchShopifyNotes({}, {[tx.transactionId]:{...tx,amount:16000}},orders,[]).length,0);
  assert.equal(autoMatchShopifyNotes({}, {[tx.transactionId]:{...tx,date:'2026-09-15'}},orders,[]).length,0);
});

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

test('uses actual Paytm platform fee, settled date and UTR as the bank batch', () => {
  const csv = 'transaction_id,transaction_type,transaction_date,status,amount,commission,gst,utr_no,settled_date,settled_amount,platform_fee,comments\n' +
    '202609240000123456,ACQUIRING,24-09-2026 11:00:00,SUCCESS,1000,10,1.8,UTR-24,25-09-2026,983.2,5,Customer payment\n';
  const parsed = parsePaytmReport(Buffer.from(csv), 'actual.csv', '2026-09-24');
  const [batch] = summarizePayouts(parsed.transactions);
  assert.deepEqual([batch.utr, batch.settledDate, batch.gross, batch.platformFee, batch.net, batch.customerPaymentCount], ['UTR-24', '2026-09-25', 1000, 5, 983.2, 1]);
});

test('VAS deduction reduces the UTR bank net and never inflates customer sales', () => {
  const payments = [
    { transactionId: 'sale-1', utr: 'PB0309232845', settledDate: '2026-08-26', payoutId: 'P1', amount: 15094, commission: 451.31, platformFee: 0, gst: 81.24, settledAmount: 14561.45, isCustomerPayment: true },
    { transactionId: 'sale-2', utr: 'PB0309232845', settledDate: '2026-08-26', payoutId: 'P1', amount: 1999, commission: 59.77, platformFee: 0, gst: 0, settledAmount: 1939.23, isCustomerPayment: true },
    { transactionId: 'vas', utr: 'PB0309232845', settledDate: '2026-08-26', payoutId: 'P1', amount: 352, commission: 0, platformFee: 0, gst: 0, settledAmount: 352, isCustomerPayment: false, transactionType: 'VAS Deductions' }
  ];
  const [batch] = summarizePayouts(payments);
  assert.deepEqual([batch.gross, batch.customerGross, batch.nonCustomerAmount, batch.commission, batch.gst, batch.net], [17093, 17093, 352, 511.08, 81.24, 16148.68]);
});

test('Paytm clearing shows its dedicated report uploader instead of the debit-credit bank reader', () => {
  const html = fs.readFileSync(require.resolve('../public/expenses.html'), 'utf8');
  assert.match(html, /Default Paytm transaction report CSV/);
  assert.match(html, /reconcileEligible&&!isPaytmClearing/);
  assert.match(html, /Finalize Paytm reconciliation/);
  assert.match(html, /finalize-settlement/);
  assert.match(html, /Finalize all matched Paytm reconciliations/);
  assert.match(html, /finalize-all/);
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

test('re-upload enriches legacy transactions instead of reporting false conflicts', () => {
  const id = '202609240000123456';
  const store = { paytmReportTransactions: { [id]: { transactionId: id, date: '2026-09-24', time: '24-09-2026 11:00:00', amount: 1000, commission: 10, gst: 1.8, settledAmount: 988.2, source: 'older.csv' } }, paytmReportDrafts: {} };
  const routes = {}, router = { get: (url, handler) => { routes[url] = handler; }, post: (url, ...handlers) => { routes[url] = handlers.at(-1); } };
  registerPaytmReports(router, { loadStore: () => store, saveStore: () => {}, audit: () => {}, canAccess: () => true, upload: { array: () => () => {} }, view: () => ({}), today: () => '2026-09-24', orders: () => [], saleRows: () => [] });
  const csv = 'transaction_id,transaction_type,transaction_date,status,amount,commission,gst,utr_no,settled_date,settled_amount,platform_fee,comments\n' +
    `${id},ACQUIRING,24-09-2026 11:00:00,SUCCESS,1000,10,1.8,UTR-24,25-09-2026,988.2,0,Customer payment\n`;
  const reply = () => ({ status() { return this; }, json(body) { this.body = body; return this; } });
  const preview = reply(); routes['/api/expenses/paytm-reports/preview']({ user: { username: 'owner' }, files: [{ originalname: 'paytm.csv', buffer: Buffer.from(csv) }] }, preview);
  assert.equal(preview.body.warnings.length, 0);
  assert.equal(preview.body.enrichedTransactions, 1);
  const confirmed = reply(); routes['/api/expenses/paytm-reports/confirm']({ user: { username: 'owner' }, body: { draftId: preview.body.draftId } }, confirmed);
  assert.equal(confirmed.body.enriched, 1);
  assert.deepEqual([store.paytmReportTransactions[id].utr, store.paytmReportTransactions[id].settledDate, store.paytmReportTransactions[id].transactionType, store.paytmReportTransactions[id].platformFee], ['UTR-24', '2026-09-25', 'ACQUIRING', 0]);
});

test('saving a detailed Paytm report automatically links a unique Shopify note suffix', () => {
  const routes={},store={paytmReportTransactions:{},paytmReportDrafts:{}};
  const router={get:(url,handler)=>{routes[url]=handler;},post:(url,...handlers)=>{routes[url]=handlers.at(-1);}};
  const txId='202609170001123456';
  registerPaytmReports(router,{loadStore:()=>store,saveStore:()=>{},audit:()=>{},canAccess:()=>true,upload:{array:()=>()=>{}},view:()=>({}),today:()=> '2026-09-19',orders:()=>[{id:'O1',number:2801,financialStatus:'paid',createdAt:'2026-09-17T12:00:00Z',total:999,note:'Paytm 123456'}],saleRows:()=>[]});
  const reply=()=>({status(){return this;},json(body){this.body=body;return this;}});
  const preview=reply();routes['/api/expenses/paytm-reports/preview']({user:{username:'owner'},files:[{originalname:'paytm.csv',buffer:Buffer.from(header+row(txId,'2026-09-17',999,0,0,999))}]},preview);
  const saved=reply();routes['/api/expenses/paytm-reports/confirm']({user:{username:'owner'},body:{draftId:preview.body.draftId}},saved);
  assert.equal(saved.body.autoMatched,1);
  assert.equal(store.paytmOrderLinks[txId].orderId,'O1');
  assert.equal(store.paytmPayoutPostings,undefined,'matching a receipt does not post a payout');
});

test('reviewed Shopify links and exact UTR bank credit are required before payout posting', () => {
  const tx = { transactionId: 'T1', rrn: '123456', date: '2026-09-17', amount: 100, commission: 1, gst: 0.18, settledAmount: 98.82, payoutId: 'P1', payoutDate: '2026-09-18', utr: 'UTR123', posId: 'POS1' };
  const store = { paytmReportTransactions: { T1: tx }, paytmOrderLinks: {}, bankStatements: { 'Axis Bank 3448': { transactions: { b: { id: 'BTX-1', date: '2026-09-18', credit: 98.82, debit: 0, reference: 'PAYTM UTR123' } } } } };
  const orders = [{ id: 'O1', orderNumber: 2801, financialStatus: 'paid', note: 'Paytm 123456' }];
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

test('links the Shopify number field used by imported orders', () => {
  const tx = { transactionId: 'T2728', date: '2026-08-25', amount: 15094, posId: 'POS1' };
  const orders = [{ id: 'shopify-order-2728', number: 2728, financialStatus: 'paid', note: '' }];
  const sales = [{ id: 'SHOPIFY/shopify-order-2728', orderId: 'shopify-order-2728', orderNumber: '2728', date: '2026-08-25', account: 'Paytm Settlement Clearing', amount: 15094 }];
  const link = validateOrderLink({}, tx, '2728', orders, sales, 'Matched the Paytm receipt to order 2728');
  assert.equal(link.orderId, 'shopify-order-2728');
});

test('split order links only the Paytm component and excludes store credit used as tender', () => {
  const order = { id: 'split', number: 2845, financialStatus: 'paid', createdAt: '2026-09-19T12:00:00Z', total: 2498, refundAmount: 500, storeCreditAmount: 1499, note: 'Paytm 123456' };
  const tx = { transactionId: '123456', date: '2026-09-19', amount: 999, posId: 'POS1' };
  const link = validateOrderLink({}, tx, '2845', [order], [], '');
  assert.equal(link.amount, 999);
  assert.equal(link.orderTotal, 2498, 'later refunds do not rewrite the original collected amount');
  assert.equal(link.partial, true);
  assert.equal(link.matchBasis, 'transaction_id_in_shopify_note');
  assert.throws(() => validateOrderLink({}, { ...tx, amount: 1000 }, '2845', [order], [], ''), /store credit/);
});

test('original Paytm receipt remains linkable after partial or full Shopify refund', () => {
  const tx = { transactionId: '202609040000169675', date: '2026-09-04', amount: 4998, posId: 'POS1' };
  const base = { id: 'refunded-order', number: 2776, createdAt: '2026-09-04T12:00:00Z', total: 4998, note: 'Paytm 169675' };
  for (const order of [
    { ...base, financialStatus: 'partially_refunded', refundAmount: 2000 },
    { ...base, financialStatus: 'refunded', refundAmount: 4998 }
  ]) {
    const store = {};
    assert.equal(autoMatchShopifyNotes(store, { [tx.transactionId]: tx }, [order], []).length, 1);
    assert.equal(store.paytmOrderLinks[tx.transactionId].amount, 4998);
    assert.equal(store.paytmOrderLinks[tx.transactionId].subsequentlyRefunded, order.refundAmount);
  }
  assert.throws(() => validateOrderLink({}, tx, '2776', [{ ...base, financialStatus: 'refunded', cancelledAt: '2026-09-05' }], [], ''), /non-cancelled/);
});

test('Paytm pos_id DEFAULT does not wrongly classify a QR/UPI customer receipt as non-POS', () => {
  const tx = { transactionId: '20260902110870000301991517820518327', date: '2026-09-02', amount: 5100, transactionType: 'ACQUIRING', paymentMode: 'UPI', posId: 'DEFAULT' };
  const order = { id: 'order-2760', number: 2760, createdAt: '2026-09-02T19:29:00Z', financialStatus: 'paid', total: 5100, note: 'Paytm 518327' };
  const store = {};
  assert.equal(autoMatchShopifyNotes(store, { [tx.transactionId]: tx }, [order], []).length, 1);
  assert.equal(store.paytmOrderLinks[tx.transactionId].orderNumber, '2760');
});

test('Shopify sale/capture components never count authorization or store credit as Paytm', () => {
  const summary = summarizeShopifyPayments([
    { id: 1, kind: 'authorization', status: 'success', gateway: 'paytm', amount: '999.00' },
    { id: 2, kind: 'capture', status: 'success', gateway: 'paytm', amount: '999.00' },
    { id: 3, kind: 'sale', status: 'success', gateway: 'shopify_store_credit', amount: '1499.00' },
    { id: 4, kind: 'sale', status: 'failure', gateway: 'paytm', amount: '100.00' }
  ]);
  assert.deepEqual([summary.paytmAmount, summary.storeCreditAmount, summary.cashAmount, summary.otherAmount], [999, 1499, 0, 0]);
  assert.equal(summary.transactions.length, 2);
});

test('Shopify payment split sync caches exact successful components for review', async () => {
  const store = {}, routes = {}, router = { get: (url, handler) => { routes[url] = handler; }, post: (url, ...handlers) => { routes[url] = handlers.at(-1); } };
  registerPaytmReports(router, { loadStore: () => store, saveStore: () => {}, audit: () => {}, canAccess: () => true, upload: { array: () => () => {} }, view: () => ({}), today: () => '2026-09-19', orders: () => [{ id: '12345', number: 2845 }], shopifyStore: 'example.myshopify.com', shopifyClient: { request: async () => ({ ok: true, json: async () => ({ transactions: [{ id: 1, kind: 'sale', status: 'success', gateway: 'paytm', amount: '999.00' }, { id: 2, kind: 'sale', status: 'success', gateway: 'shopify_store_credit', amount: '1499.00' }] }) }) } });
  const res = { status() { return this; }, json(body) { this.body = body; return this; } };
  await routes['/api/expenses/paytm-reports/sync-shopify-payments']({ user: { username: 'owner' }, body: { orderId: '2845' } }, res);
  assert.equal(res.body.success, true);
  assert.equal(store.paytmShopifyPayments['12345'].paytmAmount, 999);
  assert.equal(store.paytmShopifyPayments['12345'].storeCreditAmount, 1499);
});

test('exclusion is audited and reversible but cannot silently post a mixed payout', () => {
  const tx = { transactionId: 'T1', date: '2026-09-17', amount: 100, commission: 0, gst: 0, settledAmount: 100, payoutId: 'P1', payoutDate: '2026-09-18', utr: 'UTR123' };
  const store = { paytmReportTransactions: { T1: tx }, paytmOrderLinks: {}, bankStatements: { 'Axis Bank 3448': { transactions: {} } } };
  const routes = {}, audits = [];
  const router = { get: (url, handler) => { routes[url] = handler; }, post: (url, ...handlers) => { routes[url] = handlers.at(-1); } };
  registerPaytmReports(router, { loadStore: () => store, saveStore: () => {}, audit: (_store, _req, action) => audits.push(action), canAccess: () => true, upload: { array: () => () => {} }, view: () => ({}), today: () => '2026-09-18', orders: () => [], saleRows: () => [] });
  const reply = () => ({ code: 200, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } });
  const first = reply();
  routes['/api/expenses/paytm-reports/exclude']({ user: { username: 'owner' }, body: { transactionId: 'T1', reason: 'Not a Shopify customer sale' } }, first);
  assert.equal(first.body.success, true);
  assert.match(store.paytmExcludedTransactions.T1.reason, /Not a Shopify/);
  assert.throws(() => validatePayoutPosting(store, 'P1', 'bank-id', []), /excluded payment/);
  const restored = reply();
  routes['/api/expenses/paytm-reports/restore']({ user: { username: 'owner' }, body: { transactionId: 'T1', reason: 'Confirmed against Shopify order' } }, restored);
  assert.equal(restored.body.success, true);
  assert.equal(store.paytmExcludedTransactions.T1, undefined);
  assert.deepEqual(audits, ['PAYTM_TRANSACTION_EXCLUDED', 'PAYTM_TRANSACTION_RESTORED']);
});

test('manual Paytm sale and third-party cash transfer remain distinct from Shopify revenue', () => {
  const transactions = {
    SALE: { transactionId: 'SALE', date: '2026-09-17', amount: 999, commission: 0, gst: 0, settledAmount: 999, payoutId: 'P1', payoutDate: '2026-09-18', utr: 'UTR123' },
    CASH: { transactionId: 'CASH', date: '2026-09-17', amount: 50000, commission: 0, gst: 0, settledAmount: 50000, payoutId: 'P1', payoutDate: '2026-09-18', utr: 'UTR123' }
  };
  const store = { paytmReportTransactions: transactions, receipts: [], transfers: [], bankStatements: { 'Axis Bank 3448': { transactions: { b: { id: 'BTX-1', date: '2026-09-18', credit: 50999, debit: 0, reference: 'UTR123' } } } } };
  const routes = {}, router = { get: (url, handler) => { routes[url] = handler; }, post: (url, ...handlers) => { routes[url] = handlers.at(-1); } };
  registerPaytmReports(router, { loadStore: () => store, saveStore: () => {}, audit: () => {}, canAccess: () => true, canClassify: () => true, upload: { array: () => () => {} }, view: () => ({}), today: () => '2026-09-18', orders: () => [], saleRows: () => [] });
  const reply = () => ({ status() { return this; }, json(body) { this.body = body; return this; } });
  const proof = '/api/expenses/photo/evidence.jpg';
  const sale = reply(); routes['/api/expenses/paytm-reports/classify']({ user: { username: 'owner' }, body: { transactionId: 'SALE', type: 'manual_sale', party: 'Customer', details: 'Shirt sold at counter', reason: 'Shopify unavailable at counter', proof } }, sale);
  const cash = reply(); routes['/api/expenses/paytm-reports/classify']({ user: { username: 'owner' }, body: { transactionId: 'CASH', type: 'cash_transfer', party: 'Agent', details: 'Cash handed to agent', reason: 'Agent transferred cash via Paytm', proof } }, cash);
  assert.equal(sale.body.success, true); assert.equal(cash.body.success, true);
  assert.equal(store.receipts.length, 1); assert.equal(store.receipts[0].amount, 999);
  assert.equal(store.transfers.length, 1); assert.deepEqual([store.transfers[0].fromAccount, store.transfers[0].toAccount, store.transfers[0].amount], ['Counter Cash', 'Paytm Settlement Clearing', 50000]);
  assert.equal(validatePayoutPosting(store, 'P1', 'BTX-1', []).payout.net, 50999);
  const corrected = reply(); routes['/api/expenses/paytm-reports/unclassify']({ user: { username: 'owner' }, body: { transactionId: 'CASH', reason: 'Correcting wrong third-party name' } }, corrected);
  assert.equal(corrected.body.success, true); assert.equal(store.transfers.length, 0); assert.equal(store.receipts.length, 1);
  assert.throws(() => validatePayoutPosting(store, 'P1', 'BTX-1', []), /classified/);
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
