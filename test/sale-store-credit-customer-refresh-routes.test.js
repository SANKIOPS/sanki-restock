'use strict';

const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id:rentalSource, filename:rentalSource, loaded:true, exports:{ id:'SYNTHETIC-CUSTOMER-REFRESH', asOf:'2098-01-01', file:'Synthetic fixture', sheet:'None', range:'', rows:[] } };
const tempRoot = path.resolve(os.tmpdir()), temp = fs.mkdtempSync(path.join(tempRoot, 'store-credit-customer-route-'));
process.env.DATA_PATH = path.join(temp, 'data.json'); process.env.ORDERS_PATH = path.join(temp, 'orders.json'); process.env.SALES_PATH = path.join(temp, 'sales.json'); process.env.INCENTIVES_PATH = path.join(temp, 'incentives.json');
const file = path.join(temp, 'expenses.json'), date = '2098-06-02', amount = 2499, saleId = 'SHOPIFY/1002', endpoint = '/api/expenses/sale-store-credit-corrections/refresh-customers';
fs.writeFileSync(file, JSON.stringify({ expenses:{}, receipts:[], receivables:{}, oneTimeMigrations:{} })); fs.writeFileSync(process.env.ORDERS_PATH, JSON.stringify({ orders:{} })); fs.writeFileSync(process.env.SALES_PATH, JSON.stringify({ sales:[] }));
const { router } = require('../modules/expenses'), orderService = require('../modules/orders'), originalRefresh = orderService.refreshAccountingCustomerEvidence;
const clone = value => JSON.parse(JSON.stringify(value)), read = () => JSON.parse(fs.readFileSync(file, 'utf8')), write = state => fs.writeFileSync(file, JSON.stringify(state)), orders = () => JSON.parse(fs.readFileSync(process.env.ORDERS_PATH, 'utf8')), writeOrders = state => fs.writeFileSync(process.env.ORDERS_PATH, JSON.stringify(state));
const intent = { saleId, originalOrderNumber:'#9001' };
let serviceCalls = [], cacheWrites = 0, incomingIds = { '1001':'800000001234', '1002':'800000001234' };

function invoke(route, { method='GET', role='owner', body={}, query={} } = {}) {
  const layer = router.stack.find(item => item.route?.path === route && item.route.methods[method.toLowerCase()]); assert.ok(layer, route); let status = 200, result;
  const req = { method, url:route, path:route, body, query, params:{}, headers:{ 'user-agent':'Synthetic customer evidence refresh route test' }, get(name) { return this.headers[String(name).toLowerCase()] || ''; }, user:{ role, roles:[role], username:role === 'admin' ? 'prashant' : 'synthetic-' + role, permissions:{} } };
  const res = { status(code) { status = code; return this; }, json(value) { result = value; return this; } };
  const pending = layer.route.stack[0].handle(req, res, () => { throw Error('Unexpected next'); });
  return pending && typeof pending.then === 'function' ? pending.then(() => ({ status, body:result })) : { status, body:result };
}
assert.equal(invoke('/api/expenses/config').status, 200); const baseline = read();
test.after(() => { orderService.refreshAccountingCustomerEvidence = originalRefresh; assert.equal(path.dirname(path.resolve(temp)), tempRoot); fs.rmSync(temp, { recursive:true, force:true }); });

function order(id, number, day = date) {
  return { id, name:'#' + number, orderNumber:number, channel:'POS', currency:'INR', financialStatus:'paid', total:amount, createdAt:day + 'T10:00:00.000Z', processedAt:day + 'T10:00:00.000Z', note:'Shivam', customer:{ id:'', name:'Synthetic customer' }, paymentGateways:['Cash'], paymentTransactions:[{ id:'TX-' + id, kind:'sale', status:'success', gateway:'Cash', amount, currency:'INR', processedAt:day + 'T10:00:00.000Z' }], refundTransactions:[], refundAmount:0, moneyRefunded:0, storeCreditIssued:0 };
}
function reset(changeStore = () => {}, changeOrders = () => {}) {
  serviceCalls = []; cacheWrites = 0; incomingIds = { '1001':'800000001234', '1002':'800000001234' };
  const original = order('1001', 9001, '2098-06-01'); original.financialStatus = 'refunded'; original.refundAmount = amount; original.storeCreditIssued = amount;
  original.refundTransactions = [{ id:'RF-SYNTHETIC-CREDIT', refundId:'RF-SYNTHETIC', kind:'refund', status:'success', gateway:'store_credit', amount, parentId:'TX-1001', processedAt:date + 'T09:00:00.000Z' }];
  const source = { orders:{ '1001':original, '1002':order('1002', 9002) }, dispatch:{}, sync:{ count:2 } }; changeOrders(source); writeOrders(source);
  fs.writeFileSync(process.env.SALES_PATH, JSON.stringify({ sales:[] })); fs.writeFileSync(process.env.INCENTIVES_PATH, JSON.stringify({ revision:0, reviews:{}, approvals:{}, payments:[], audit:[] }));
  const state = { ...clone(baseline), expenses:{}, receipts:[], receivables:{}, transfers:[], adjustments:[], vendorAdvances:[], vendorOpeningPayables:[], salesRefunds:[], reconciliationExpenses:[], bankTruthMovements:[], requests:[], reqSeq:0, auditLog:[], auditSeq:0, openingBalances:{ 'Counter Cash':0 }, openingBalancesByNature:{}, openingBalanceDates:{}, cashReconciliations:[], bankStatements:{}, bankDateOverrides:{}, bankReconciliationLinks:{}, bankReconciliationApprovals:{}, bankReconciliationDrafts:{}, saleAllocationOverrides:{}, saleComponentAmountOverrides:{}, saleStoreCreditCorrections:{}, paytmOrderLinks:{}, paytmShopifyPayments:{}, paytmRoundingReviews:{}, paytmReportTransactions:{}, paytmPayoutPostings:[], paytmVerifiedSettlements:[], paytmSettlements:[], gokwikPostings:{}, gokwikOrderLinks:{} }; changeStore(state); write(state); assert.equal(invoke('/api/expenses/config').status, 200);
  // Stub only the service boundary. Its route-provided validation runs against
  // full detached before/after order maps before this synthetic cache is saved.
  orderService.refreshAccountingCustomerEvidence = async options => {
    assert.equal(typeof options.validate, 'function'); assert.deepEqual(options.orderIds, ['1002','1001']); serviceCalls.push({ orderIds:clone(options.orderIds), actor:options.actor });
    const cached = orders(), before = clone(cached.orders), after = clone(before);
    for (const id of options.orderIds) { assert.ok(after[id]); assert.equal(after[id].customer.id, '', 'this fixture hydrates missing identities only'); after[id].customer.id = incomingIds[id]; }
    await options.validate(before, after);
    cached.orders = after; writeOrders(cached); cacheWrites++;
    return { success:true, refreshed:options.orderIds.length, orderIds:options.orderIds };
  };
  return read();
}
const refresh = options => invoke(endpoint, { method:'POST', body:intent, ...options });
function withoutCustomerIds(value) { const out = clone(value); for (const row of Object.values(out.orders)) row.customer.id = ''; return out; }
function bytes() { return { accounting:fs.readFileSync(file, 'utf8'), incentives:fs.readFileSync(process.env.INCENTIVES_PATH, 'utf8'), sales:fs.readFileSync(process.env.SALES_PATH, 'utf8') }; }

for (const role of ['claimant','accounting','samast_accounting']) test(role + ' cannot refresh source customer identities', async () => {
  reset(); const before = bytes(), source = fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), result = await refresh({ role }); assert.equal(result.status, 403); assert.equal(serviceCalls.length, 0); assert.equal(cacheWrites, 0); assert.deepEqual(bytes(), before); assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), source);
});

for (const role of ['admin','owner']) test(role + ' refresh hydrates only missing customer IDs without creating a correction, request or financial posting', async () => {
  reset(); const before = bytes(), source = orders(), blocked = await invoke('/api/expenses/sale-store-credit-corrections/preview', { method:'POST', role, body:{ ...intent, storeCreditAmount:amount, reason:'Synthetic replacement uses original store credit.' } }); assert.ok([400,409].includes(blocked.status)); assert.match(blocked.body.error, /customer/i);
  const result = await refresh({ role }); assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.success, true); assert.equal(serviceCalls.length, 1); assert.equal(cacheWrites, 1); assert.equal(serviceCalls[0].actor, role === 'admin' ? 'prashant' : 'synthetic-owner');
  assert.deepEqual(bytes(), before); assert.equal(orders().orders['1001'].customer.id, '800000001234'); assert.equal(orders().orders['1002'].customer.id, '800000001234'); assert.deepEqual(withoutCustomerIds(orders()), source); assert.deepEqual(read().requests, []); assert.deepEqual(read().saleStoreCreditCorrections, {});
  const preview = await invoke('/api/expenses/sale-store-credit-corrections/preview', { method:'POST', role, body:{ ...intent, storeCreditAmount:amount, reason:'Synthetic replacement uses original store credit.' } }); assert.equal(preview.status, 200, JSON.stringify(preview.body)); assert.deepEqual(bytes(), before);
});

for (const [name, body, changeOrders] of [
  ['component ID instead of immutable order ID', { ...intent, saleId:'SHOPIFY/1002/NONCASH' }, () => {}],
  ['missing target order', { ...intent, saleId:'SHOPIFY/9999' }, () => {}],
  ['same original and replacement', { ...intent, originalOrderNumber:'#9002' }, () => {}],
  ['non-exact original order number', { ...intent, originalOrderNumber:'#09001' }, () => {}],
  ['ambiguous original order number', intent, source => { source.orders['1003'] = { ...clone(source.orders['1001']), id:'1003' }; }],
  ['target cache key disagrees with order ID', intent, source => { source.orders['1002'].id = '1004'; }],
  ['target no longer supplies a Counter Cash receipt', intent, source => { source.orders['1002'].paymentGateways = ['Paytm']; source.orders['1002'].paymentTransactions[0].gateway = 'paytm'; }]
]) test('refresh rejects ' + name + ' before reaching Shopify service', async () => {
  reset(() => {}, changeOrders); const before = bytes(), source = fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), result = await refresh({ body }); assert.ok([400,409].includes(result.status), JSON.stringify(result.body)); assert.equal(serviceCalls.length, 0); assert.equal(cacheWrites, 0); assert.deepEqual(bytes(), before); assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), source);
});

for (const [name, protect] of Object.entries({
  'approved correction referencing the original order': state => { state.saleStoreCreditCorrections['SHOPIFY/1999'] = { saleId:'SHOPIFY/1999', orderId:'1999', originalOrderId:'1001', originalOrderNumber:'9001', storeCreditAmount:amount, cashAmount:0, date, reason:'Existing synthetic source correction', walletStatus:'pending_external_user_action' }; },
  'pending correction referencing the replacement': state => { state.requests.push({ id:'RQ-PROTECTED', kind:'sale_store_credit', status:'pending', meta:{ saleId, orderId:'1002', originalOrderId:'1001', reason:'Existing synthetic request' } }); }
})) test(name + ' blocks the validation callback before any cache write', async () => {
  reset(protect); const before = bytes(), source = fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), result = await refresh(); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.match(result.body.error, /saved correction/i); assert.equal(serviceCalls.length, 1); assert.equal(cacheWrites, 0); assert.deepEqual(bytes(), before); assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), source);
});

test('refreshing IDs that would change approved incentive evidence is blocked before cache persistence', async () => {
  reset(); const source = orders(), companion = order('1003', 9003); companion.total = 8000; companion.paymentTransactions[0].amount = 8000; source.orders['1003'] = companion; writeOrders(source);
  const incentives = { revision:1, reviews:{}, approvals:{}, payments:[], audit:[] }, { calculate } = require('../modules/incentives'), day = calculate(Object.values(source.orders), incentives).daily.find(row => row.date === date && row.salesperson === 'Shivam'); assert.equal(day.qualifies, true);
  incentives.approvals[day.key] = { id:'IA-SYNTHETIC', date, salesperson:'Shivam', eligibleAmount:day.eligibleAmount, incentiveBaseAmount:day.incentiveBaseAmount, incentive:day.incentive, qualificationSources:day.qualificationSources }; fs.writeFileSync(process.env.INCENTIVES_PATH, JSON.stringify(incentives));
  // A genuine identity hydration can activate the existing excluded-customer
  // rule. The approved incentive must therefore remain protected.
  incomingIds = { '1001':'8898466611452', '1002':'8898466611452' }; const before = bytes(), sourceBytes = fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), result = await refresh();
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.match(result.body.error, /incentive/i); assert.equal(serviceCalls.length, 1); assert.equal(cacheWrites, 0); assert.deepEqual(bytes(), before); assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), sourceBytes);
});

test('different verified customer IDs remain different and the same-customer correction check still fails', async () => {
  reset(); incomingIds = { '1001':'800000001111', '1002':'800000002222' }; const before = bytes(), result = await refresh(); assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(cacheWrites, 1);
  const preview = await invoke('/api/expenses/sale-store-credit-corrections/preview', { method:'POST', body:{ ...intent, storeCreditAmount:amount, reason:'Synthetic request must retain strict identity matching.' } }); assert.ok([400,409].includes(preview.status)); assert.match(preview.body.error, /customer/i); assert.deepEqual(bytes(), before); assert.deepEqual(read().requests, []); assert.deepEqual(read().saleStoreCreditCorrections, {});
});
