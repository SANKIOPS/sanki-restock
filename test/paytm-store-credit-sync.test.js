'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { registerPaytmReports } = require('../modules/paytm-reports-routes');
const clone = value => JSON.parse(JSON.stringify(value));
const route = '/api/expenses/paytm-reports/sync-shopify-payments';
const orderId = '800000009002', unrelatedId = '800000009099';

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
function correction(id) {
  return { saleId:'SHOPIFY/' + id, orderId:id, orderNumber:id === orderId ? '9002' : '9099', originalOrderId:'800000009001', originalOrderNumber:'9001', treatment:'exchange_store_credit', cashAmount:0, nonCashAmount:0, storeCreditAmount:2499, reason:'Synthetic replacement uses the original return credit; no new cash was received.', walletStatus:'pending_external_user_action', approvedBy:'synthetic-owner', approvedAt:'2098-06-02T12:00:00.000Z', revision:1 };
}
const mixedTransactions = [
  { id:'TX-SYNTHETIC-CASH', kind:'sale', status:'success', gateway:'Cash', amount:'1000.00', processed_at:'2098-06-02T10:00:00.000Z' },
  { id:'TX-SYNTHETIC-CREDIT', kind:'sale', status:'success', gateway:'shopify_store_credit', amount:'1499.00', processed_at:'2098-06-02T10:00:00.000Z' },
  { id:'TX-SYNTHETIC-REFUND', kind:'refund', status:'success', gateway:'Cash', amount:'20.00' },
  { id:'TX-SYNTHETIC-FAILED', kind:'sale', status:'failure', gateway:'paytm', amount:'99.00' }
];

// This harness has no filesystem, HTTP client, or production configuration.
// Every load is detached to reproduce the real route's stale-snapshot risk.
function fixture(options = {}) {
  const routes = {}, requestGate = deferred(), requestEntered = deferred(), bodyGate = deferred(), bodyEntered = deferred();
  const sourceOrders = [{ id:orderId, orderNumber:9002, name:'#9002', total:2499, channel:'POS', createdAt:'2098-06-02T10:00:00.000Z', paymentGateways:['Cash','shopify_store_credit'] }];
  let persisted = { saleStoreCreditCorrections:{}, requests:[], auditLog:[{ id:'AUD-PRIOR', action:'SYNTHETIC_PRIOR_EVENT', subjectId:'prior' }], auditSeq:1,
    paytmShopifyPayments:{ PRIOR:{ total:11, cashAmount:11, transactions:[] } }, paytmRoundingReviews:{}, paytmPayoutPostings:[], receipts:[], transfers:[], adjustments:[], expenses:{} };
  let saves = 0, loads = 0, requestCount = 0, status = 200, result;
  const response = { status(code) { status = code; return this; }, json(body) { result = clone(body); return this; } };
  const router = { get(name, ...handlers) { routes['GET ' + name] = handlers.at(-1); }, post(name, ...handlers) { routes['POST ' + name] = handlers.at(-1); } };
  registerPaytmReports(router, {
    loadStore() { loads++; return clone(persisted); },
    saveStore(next) { saves++; persisted = clone(next); },
    audit(store, req, action, subjectType, subjectId, details) { store.auditSeq = Number(store.auditSeq || 0) + 1; store.auditLog.push({ id:'AUD-' + store.auditSeq, action, subjectType, subjectId, by:req.user.username, ...clone(details) }); },
    canAccess:() => true, canClassify:() => true, upload:{ array:() => (_req, _res, next) => next() },
    view:store => ({ paymentIds:Object.keys(store.paytmShopifyPayments || {}), correctionIds:Object.keys(store.saleStoreCreditCorrections || {}), requestIds:(store.requests || []).map(item => item.id) }),
    today:() => '2098-06-02', orders:() => clone(sourceOrders), saleRows:() => [], shopifyStore:'synthetic-store-credit.myshopify.com',
    shopifyClient:{ request(url) { requestCount++; assert.equal(url, 'https://synthetic-store-credit.myshopify.com/admin/api/2024-01/orders/' + orderId + '/transactions.json'); requestEntered.resolve(); return requestGate.promise; } }
  });
  const start = () => routes['POST ' + route]({ user:{ username:'synthetic-owner' }, body:{ orderId:'9002' } }, response);
  const successfulResponse = () => ({ ok:true, status:200, async json() { bodyEntered.resolve(); return options.deferBody ? bodyGate.promise : { transactions:clone(mixedTransactions) }; } });
  return {
    read:() => clone(persisted), source:() => clone(sourceOrders), counts:() => ({ saves, loads, requestCount }),
    mutate(change) { const current = clone(persisted); change(current); persisted = current; },
    async duringRequest(change, returned = successfulResponse()) {
      const pending = start(); await requestEntered.promise; change && this.mutate(change); requestGate.resolve(returned); await pending;
      return { status, body:result };
    },
    async duringBody(change) {
      const pending = start(); await requestEntered.promise; requestGate.resolve(successfulResponse()); await bodyEntered.promise; this.mutate(change); bodyGate.resolve({ transactions:clone(mixedTransactions) }); await pending;
      return { status, body:result };
    }
  };
}
function addConcurrentExchange(store, id = unrelatedId) {
  store.saleStoreCreditCorrections['SHOPIFY/' + id] = correction(id);
  store.requests.push({ id:'RQ-CONCURRENT', kind:'sale_store_credit', status:'pending', by:'synthetic-admin', meta:{ saleId:'SHOPIFY/800000009098', storeCreditAmount:500, reason:'Another synthetic exchange is pending.' } });
  store.auditSeq++;
  store.auditLog.push({ id:'AUD-CONCURRENT', action:'SALE_STORE_CREDIT_CORRECTED', subjectId:'SHOPIFY/' + id, after:clone(store.saleStoreCreditCorrections['SHOPIFY/' + id]) });
  store.receipts.push({ id:'REC-CONCURRENT', amount:13, account:'Counter Cash', note:'Unrelated synthetic receipt retained.' });
  store.paytmShopifyPayments.CONCURRENT = { total:17, cashAmount:17, transactions:[] };
}

test('Shopify sync preserves an unrelated correction, pending request, audit and receipt created during request await', async () => {
  const f = fixture(), source = f.source(); let concurrent;
  const result = await f.duringRequest(store => { addConcurrentExchange(store); concurrent = clone(store); });
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.success, true); assert.equal(f.counts().saves, 1); assert.ok(f.counts().loads >= 2);
  const saved = f.read();
  for (const key of ['saleStoreCreditCorrections','requests','receipts','transfers','adjustments','expenses']) assert.deepEqual(saved[key], concurrent[key], key + ' survives the async sync');
  assert.deepEqual(saved.auditLog.slice(0, concurrent.auditLog.length), concurrent.auditLog); assert.equal(saved.auditLog.at(-1).action, 'PAYTM_SHOPIFY_PAYMENTS_SYNCED');
  assert.deepEqual(saved.paytmShopifyPayments.PRIOR, concurrent.paytmShopifyPayments.PRIOR); assert.deepEqual(saved.paytmShopifyPayments.CONCURRENT, concurrent.paytmShopifyPayments.CONCURRENT);
  assert.equal(saved.saleStoreCreditCorrections['SHOPIFY/' + unrelatedId].walletStatus, 'pending_external_user_action'); assert.deepEqual(f.source(), source);
});

test('latest state is reloaded after the response body await as well', async () => {
  const f = fixture({ deferBody:true }); let concurrent;
  const result = await f.duringBody(store => { addConcurrentExchange(store); concurrent = clone(store); });
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(f.counts().saves, 1); assert.deepEqual(f.read().saleStoreCreditCorrections, concurrent.saleStoreCreditCorrections); assert.deepEqual(f.read().requests, concurrent.requests); assert.deepEqual(f.read().auditLog.slice(0, concurrent.auditLog.length), concurrent.auditLog);
});

for (const [name, protect] of Object.entries({
  'approved store-credit correction': store => { store.saleStoreCreditCorrections['SHOPIFY/' + orderId] = correction(orderId); },
  'pending store-credit request': store => { store.requests.push({ id:'RQ-SELECTED', kind:'sale_store_credit', status:'pending', meta:{ saleId:'SHOPIFY/' + orderId, orderId } }); },
  'legacy sale-ID-only pending request': store => { store.requests.push({ id:'RQ-LEGACY-SELECTED', kind:'sale_store_credit', status:'pending', meta:{ saleId:'SHOPIFY/' + orderId } }); },
  'audited rounding review': store => { store.paytmRoundingReviews['ROUNDING-SELECTED'] = { orderId, amount:1 }; },
  'posted payout': store => { store.paytmPayoutPostings.push({ id:'POSTING-SELECTED', orderIds:[orderId], transactionIds:['TX-SYNTHETIC-SELECTED'] }); }
})) test('new ' + name + ' during request await blocks sync with no save', async () => {
  const f = fixture(), source = f.source(); let concurrent;
  const result = await f.duringRequest(store => { protect(store); store.auditLog.push({ id:'AUD-PROTECTED', action:'SYNTHETIC_CONCURRENT_PROTECTION', subjectId:orderId }); concurrent = clone(store); });
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(result.body.success, false); assert.equal(f.counts().saves, 0); assert.deepEqual(f.read(), concurrent); assert.deepEqual(f.source(), source);
  assert.match(result.body.error, name.includes('rounding') ? /rounding/i : name.includes('payout') ? /posted payout/i : /store.credit/i);
});

test('selected-order correction created during body await also blocks without dropping its audit', async () => {
  const f = fixture({ deferBody:true }); let concurrent;
  const result = await f.duringBody(store => { addConcurrentExchange(store, orderId); concurrent = clone(store); });
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.match(result.body.error, /store.credit/i); assert.equal(f.counts().saves, 0); assert.deepEqual(f.read(), concurrent);
});

test('an ordinary mixed cash/store-credit receipt remains classified correctly and does not post accounting movements', async () => {
  const f = fixture(), before = f.read(), source = f.source(), result = await f.duringRequest();
  assert.equal(result.status, 200, JSON.stringify(result.body)); const saved = f.read(), summary = saved.paytmShopifyPayments[orderId];
  assert.equal(summary.cashAmount, 1000); assert.equal(summary.storeCreditAmount, 1499); assert.equal(summary.paytmAmount, 0); assert.equal(summary.transactions.reduce((sum, tx) => sum + tx.amount, 0), 2499); assert.equal(summary.transactions.length, 2); assert.equal(summary.by, 'synthetic-owner');
  for (const key of ['saleStoreCreditCorrections','requests','receipts','transfers','adjustments','expenses','paytmPayoutPostings','paytmRoundingReviews']) assert.deepEqual(saved[key], before[key]);
  assert.deepEqual(f.source(), source); assert.equal(f.counts().saves, 1);
});

for (const [name, response] of Object.entries({
  'HTTP failure': () => ({ ok:false, status:503, json:async () => { throw Error('Error body should not be parsed.'); } }),
  'malformed response body': () => ({ ok:true, status:200, json:async () => { throw Error('Synthetic invalid response body'); } }),
  'no successful receipt components': () => ({ ok:true, status:200, json:async () => ({ transactions:[{ id:'FAILED-ONLY', kind:'sale', status:'failure', gateway:'paytm', amount:2499 }] }) })
})) test(name + ' preserves concurrent evidence and makes no accounting save', async () => {
  const f = fixture(), source = f.source(); let concurrent;
  const result = await f.duringRequest(store => { addConcurrentExchange(store); concurrent = clone(store); }, response());
  assert.equal(result.status, 502, JSON.stringify(result.body)); assert.equal(result.body.success, false); assert.equal(f.counts().saves, 0); assert.deepEqual(f.read(), concurrent); assert.deepEqual(f.source(), source);
});
