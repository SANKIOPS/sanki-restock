'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Fictional source evidence and a private temporary directory only. No live
// Shopify wallet, order, payment or reconciliation is read or changed.
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id:rentalSource, filename:rentalSource, loaded:true, exports:{ id:'SYNTHETIC-STORE-CREDIT', asOf:'2098-01-01', file:'Synthetic fixture', sheet:'None', range:'', rows:[] } };
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sale-store-credit-routes-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
process.env.ORDERS_PATH = path.join(temp, 'orders.json');
process.env.SALES_PATH = path.join(temp, 'sales.json');
process.env.INCENTIVES_PATH = path.join(temp, 'incentives.json');
const file = path.join(temp, 'expenses.json');
fs.writeFileSync(file, JSON.stringify({ expenses:{}, receipts:[], receivables:{}, oneTimeMigrations:{} }));
fs.writeFileSync(process.env.ORDERS_PATH, JSON.stringify({ orders:{} }));
fs.writeFileSync(process.env.SALES_PATH, JSON.stringify({ sales:[] }));
const { router } = require('../modules/expenses');
const clone = value => JSON.parse(JSON.stringify(value));
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));
const orders = () => JSON.parse(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'));
const writeOrders = value => fs.writeFileSync(process.env.ORDERS_PATH, JSON.stringify(value));
const previewRoute = '/api/expenses/sale-store-credit-corrections/preview';
const applyRoute = '/api/expenses/sale-store-credit-corrections';
const date = '2098-06-02', amount = 2499, saleId = 'SHOPIFY/synthetic-replacement';
const intent = { saleId, originalOrderNumber:'#9001', storeCreditAmount:amount, reason:'Replacement #9002 uses the credit issued for returned #9001; cash was selected in error and no additional money was collected.' };

function request(method, route, options = {}) {
  const role = options.role || 'owner';
  return { method, get path() { return this.url.split('?')[0]; }, url:route, originalUrl:route, baseUrl:'', body:options.body || {}, query:options.query || {}, params:options.params || {}, headers:{ 'user-agent':'Synthetic store-credit correction test' }, get(name) { return this.headers[String(name).toLowerCase()] || ''; }, user:{ role, roles:[role], username:options.username || (role === 'admin' ? 'prashant' : 'synthetic-' + role), permissions:options.permissions || {} } };
}
function invoke(route, options = {}) {
  const method = options.method || 'GET', layer = router.stack.find(item => item.route?.path === route && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, method + ' ' + route); let status = 200, body;
  layer.route.stack[0].handle(request(method, route, options), { status(code) { status = code; return this; }, json(value) { body = value; return this; } }, () => { throw Error('Unexpected next: ' + route); });
  return { status, body };
}
function dispatch(route, options = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, finished = false; const headers = {};
    const finish = body => { if (!finished) { finished = true; resolve({ status, body }); } };
    const response = { statusCode:200, headersSent:false, status(code) { this.statusCode = status = code; return this; }, setHeader(name, value) { headers[String(name).toLowerCase()] = value; }, getHeader(name) { return headers[String(name).toLowerCase()]; }, removeHeader(name) { delete headers[String(name).toLowerCase()]; }, json(body) { this.headersSent = true; finish(body); return this; }, send(body) { this.headersSent = true; finish(body); return this; }, end(body) { this.headersSent = true; finish(body); return this; } };
    router.handle(request(options.method || 'POST', route, options), response, error => error ? reject(error) : finish(undefined));
  });
}
assert.equal(invoke('/api/expenses/config').status, 200);
const baseline = read();
test.after(() => fs.rmSync(temp, { recursive:true, force:true }));

function receipt(id, day, value = amount) {
  return { id, kind:'sale', status:'success', gateway:'Cash', amount:value, currency:'INR', processedAt:day + 'T10:00:00.000Z' };
}
function order(id, number, day = date) {
  return { id, name:'#' + number, orderNumber:number, channel:'POS', currency:'INR', financialStatus:'paid', total:amount, createdAt:day + 'T10:00:00.000Z', processedAt:day + 'T10:00:00.000Z', note:'Shivam', customer:{ id:'synthetic-customer', name:'Synthetic customer' }, paymentGateways:['Cash'], paymentTransactions:[receipt('TX-' + id, day)], refundTransactions:[], refundAmount:0, moneyRefunded:0, storeCreditIssued:0 };
}
function reset(mutate = () => {}, mutateOrders = () => {}) {
  const original = order('synthetic-original', 9001, '2098-06-01');
  original.financialStatus = 'refunded'; original.refundAmount = amount; original.storeCreditIssued = amount;
  original.refundTransactions = [{ id:'RF-SYNTHETIC-CREDIT', refundId:'REFUND-SYNTHETIC', kind:'refund', status:'success', gateway:'store_credit', amount, parentId:original.paymentTransactions[0].id, processedAt:date + 'T09:00:00.000Z' }];
  const source = { orders:{ [original.id]:original, 'synthetic-replacement':order('synthetic-replacement', 9002) } };
  mutateOrders(source); writeOrders(source);
  fs.writeFileSync(process.env.SALES_PATH, JSON.stringify({ sales:[] }));
  fs.writeFileSync(process.env.INCENTIVES_PATH, JSON.stringify({ revision:0, reviews:{}, approvals:{}, payments:[], audit:[] }));
  const state = { ...clone(baseline), expenses:{}, receipts:[], receivables:{}, transfers:[], adjustments:[], vendorAdvances:[], vendorOpeningPayables:[], salesRefunds:[], reconciliationExpenses:[], bankTruthMovements:[], requests:[], reqSeq:0, auditLog:[], auditSeq:0, openingBalances:{ 'Counter Cash':0 }, openingBalancesByNature:{}, openingBalanceDates:{}, cashReconciliations:[], bankStatements:{}, bankDateOverrides:{}, bankReconciliationLinks:{}, bankReconciliationApprovals:{}, bankReconciliationDrafts:{}, saleAllocationOverrides:{}, saleComponentAmountOverrides:{}, saleStoreCreditCorrections:{}, paytmOrderLinks:{}, paytmShopifyPayments:{}, paytmRoundingReviews:{}, paytmReportTransactions:{}, paytmPayoutPostings:[], paytmVerifiedSettlements:[], paytmSettlements:[], gokwikPostings:{}, gokwikOrderLinks:{} };
  mutate(state); write(state);
  // Normalize pre-existing application settings before asserting no-write
  // previews, using only this isolated synthetic directory.
  assert.equal(invoke('/api/expenses/config').status, 200);
  return read();
}
const preview = (options = {}) => invoke(previewRoute, { method:'POST', body:intent, ...options });
const apply = (fingerprint, options = {}) => invoke(applyRoute, { method:'POST', body:{ ...intent, previewFingerprint:fingerprint }, ...options });
const decide = (id, options = {}) => invoke('/api/expenses/requests/:id/decide', { method:'POST', params:{ id }, body:{ approve:true }, ...options });
function ledger(account = 'Counter Cash', role = 'owner') {
  const result = invoke('/api/expenses/account-ledger', { role, query:{ nature:'SANKI', account, from:'2098-06-01', to:date } });
  assert.equal(result.status, 200, JSON.stringify(result.body)); return result.body;
}
function financialRecordsUnchanged(before, after) {
  for (const key of ['expenses','receipts','receivables','transfers','adjustments','vendorAdvances','vendorOpeningPayables','salesRefunds','reconciliationExpenses','bankTruthMovements','openingBalances','bankStatements','bankDateOverrides','bankReconciliationLinks','bankReconciliationDrafts','cashReconciliations','paytmOrderLinks','paytmPayoutPostings','paytmSettlements','saleAllocationOverrides','saleComponentAmountOverrides']) assert.deepEqual(after[key], before[key], key + ' remains unchanged');
}

test('store-credit preview is reachable through the router and preserves accounting and Shopify evidence', async () => {
  reset(); const before = fs.readFileSync(file, 'utf8'), orderBytes = fs.readFileSync(process.env.ORDERS_PATH, 'utf8');
  const result = await dispatch(previewRoute, { body:intent });
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.success, true); assert.equal(result.body.proposal.cashAmount, 0); assert.equal(result.body.proposal.storeCreditAmount, amount); assert.equal(result.body.proposal.originalOrderId, 'synthetic-original'); assert.match(result.body.previewFingerprint, /^[a-f0-9]{64}$/);
  assert.equal(fs.readFileSync(file, 'utf8'), before); assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), orderBytes);
});

test('store-credit apply reaches its specific route instead of the expense-id editor', async () => {
  reset(); const p = preview(), result = await dispatch(applyRoute, { body:{ ...intent, previewFingerprint:p.body.previewFingerprint } });
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.approved, true); assert.equal(read().saleStoreCreditCorrections[saleId].storeCreditAmount, amount);
});

for (const role of ['claimant','accounting','samast_accounting']) test(role + ' cannot preview or apply the Admin/Owner sale correction', () => {
  reset(); const before = fs.readFileSync(file, 'utf8');
  assert.equal(preview({ role }).status, 403); assert.equal(apply('a'.repeat(64), { role }).status, 403); assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('Admin can request without changing cash and only Owner can approve', () => {
  const before = reset(), p = preview({ role:'admin' }); assert.equal(p.status, 200, JSON.stringify(p.body));
  const pending = apply(p.body.previewFingerprint, { role:'admin' }); assert.equal(pending.status, 200, JSON.stringify(pending.body)); assert.equal(pending.body.approved, false); assert.equal(pending.body.request.kind, 'sale_store_credit'); assert.deepEqual(read().saleStoreCreditCorrections, {});
  financialRecordsUnchanged(before, read()); assert.equal(ledger().entries.find(row => row.id === saleId).credit, amount); assert.equal(decide(pending.body.request.id, { role:'admin' }).status, 403);
  const approved = decide(pending.body.request.id); assert.equal(approved.status, 200, JSON.stringify(approved.body));
  const correction = read().saleStoreCreditCorrections[saleId]; assert.equal(correction.storeCreditAmount, amount); assert.equal(correction.requestedBy, 'prashant'); assert.equal(correction.walletStatus, 'pending_external_user_action');
});

test('Owner correction removes only false cash and retains a zero-money audit row for both roles', () => {
  const before = reset(), orderBytes = fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), incentiveBytes = fs.readFileSync(process.env.INCENTIVES_PATH, 'utf8');
  const beforeCash = ledger().entries.reduce((sum,row) => sum + Number(row.credit || 0) - Number(row.debit || 0), 0);
  assert.equal(ledger().entries.find(row => row.id === 'SHOPIFY/synthetic-original').credit, amount, 'credit refund never withdraws genuine original cash');
  const p = preview(), result = apply(p.body.previewFingerprint); assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.approved, true);
  const stored = read(), corrected = stored.saleStoreCreditCorrections[saleId]; assert.equal(corrected.cashAmount, 0); assert.equal(corrected.nonCashAmount, 0); assert.equal(corrected.storeCreditAmount, amount); assert.equal(corrected.walletStatus, 'pending_external_user_action'); assert.equal(corrected.reason, intent.reason);
  financialRecordsUnchanged(before, stored); assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), orderBytes); assert.equal(fs.readFileSync(process.env.INCENTIVES_PATH, 'utf8'), incentiveBytes);
  for (const role of ['admin','owner']) {
    const cash = ledger('Counter Cash', role); assert.equal(cash.entries.find(row => row.id === 'SHOPIFY/synthetic-original').credit, amount); assert.equal(cash.entries.some(row => row.id === saleId && row.credit > 0), false);
    assert.equal(beforeCash - cash.entries.reduce((sum,row) => sum + Number(row.credit || 0) - Number(row.debit || 0), 0), amount);
    const audit = cash.entries.find(row => row.kind === 'sale_store_credit_correction'); assert.ok(audit); assert.equal(audit.credit, 0); assert.equal(audit.debit, 0); assert.equal(audit.reason, intent.reason); assert.equal(audit.storeCreditAmount, amount); assert.equal(audit.originalOrderId, 'synthetic-original');
  }
  for (const account of ['Axis Bank 3448','Paytm Settlement Clearing']) assert.equal(ledger(account).entries.some(row => row.orderId === 'synthetic-replacement' || JSON.stringify(row.connectedSales || []).includes('synthetic-replacement')), false, 'no bank/provider receipt is created');
});

test('identical retries are idempotent and different pending intent conflicts', () => {
  reset(); const p = preview({ role:'admin' }), first = apply(p.body.previewFingerprint, { role:'admin' }); assert.equal(first.status, 200, JSON.stringify(first.body));
  const again = apply(p.body.previewFingerprint, { role:'admin' }); assert.equal(again.status, 200, JSON.stringify(again.body)); assert.equal(again.body.request.id, first.body.request.id); assert.equal(read().requests.filter(row => row.kind === 'sale_store_credit').length, 1);
  assert.equal(apply(p.body.previewFingerprint, { role:'admin', body:{ ...intent, reason:'Different correction intent', previewFingerprint:p.body.previewFingerprint } }).status, 409);
  assert.equal(decide(first.body.request.id).status, 200); const bytes = fs.readFileSync(file, 'utf8'), repeated = apply(p.body.previewFingerprint); assert.equal(repeated.status, 200, JSON.stringify(repeated.body)); assert.equal(fs.readFileSync(file, 'utf8'), bytes);
});

test('one source credit cannot fund another replacement while the first request is pending', () => {
  reset(() => {}, source => { source.orders['synthetic-replacement-two'] = order('synthetic-replacement-two', 9004); });
  const p = preview({ role:'admin' }), pending = apply(p.body.previewFingerprint, { role:'admin' }); assert.equal(pending.status, 200, JSON.stringify(pending.body));
  const before = fs.readFileSync(file, 'utf8'), conflict = preview({ body:{ ...intent, saleId:'SHOPIFY/synthetic-replacement-two', reason:'Second replacement attempts to use the same credit.' } });
  assert.equal(conflict.status, 409, JSON.stringify(conflict.body)); assert.equal(fs.readFileSync(file, 'utf8'), before); assert.equal(read().requests[0].status, 'pending');
});

test('the cash-split workflow cannot overwrite a pending or approved store-credit correction', () => {
  reset(); const p = preview({ role:'admin' }), pending = apply(p.body.previewFingerprint, { role:'admin' }); assert.equal(pending.status, 200, JSON.stringify(pending.body));
  const split = () => invoke('/api/expenses/sale-allocation', { method:'POST', body:{ saleId, cashAmount:amount, reason:'Attempt to turn replacement credit back into cash.' } });
  const before = fs.readFileSync(file, 'utf8'); assert.equal(split().status, 409); assert.equal(fs.readFileSync(file, 'utf8'), before);
  assert.equal(decide(pending.body.request.id).status, 200); const approvedBytes = fs.readFileSync(file, 'utf8'); assert.equal(split().status, 409); assert.equal(fs.readFileSync(file, 'utf8'), approvedBytes);
});

test('changed source after preview or submission blocks application and retains the pending request', () => {
  reset(); const p = preview(), changed = orders(); changed.orders['synthetic-replacement'].paymentTransactions[0].processedAt = date + 'T11:00:00.000Z'; writeOrders(changed);
  const before = fs.readFileSync(file, 'utf8'); assert.equal(apply(p.body.previewFingerprint).status, 409); assert.equal(fs.readFileSync(file, 'utf8'), before);
  reset(); const pendingPreview = preview({ role:'admin' }), pending = apply(pendingPreview.body.previewFingerprint, { role:'admin' }); assert.equal(pending.status, 200, JSON.stringify(pending.body));
  const sourceChanged = orders(); sourceChanged.orders['synthetic-original'].refundTransactions[0].processedAt = date + 'T09:30:00.000Z'; writeOrders(sourceChanged);
  const beforeDecision = fs.readFileSync(file, 'utf8'); assert.equal(decide(pending.body.request.id).status, 409); assert.equal(fs.readFileSync(file, 'utf8'), beforeDecision); assert.equal(read().requests[0].status, 'pending');
});

for (const [name, mutateOrders] of Object.entries({
  'different customer': source => { source.orders['synthetic-original'].customer.id = 'another-synthetic-customer'; },
  'missing credit evidence': source => { source.orders['synthetic-original'].storeCreditIssued = 0; source.orders['synthetic-original'].refundTransactions = []; },
  'different replacement total': source => { source.orders['synthetic-replacement'].total = 3000; },
  'replacement refund': source => { source.orders['synthetic-replacement'].refundAmount = 1; },
  'non-POS replacement': source => { source.orders['synthetic-replacement'].channel = 'Website'; }
})) test('rejects ' + name + ' without recording a correction', () => {
  reset(() => {}, mutateOrders); const before = fs.readFileSync(file, 'utf8'), result = preview(); assert.ok([400,409].includes(result.status), JSON.stringify(result.body)); assert.equal(fs.readFileSync(file, 'utf8'), before); assert.deepEqual(read().saleStoreCreditCorrections, {});
});

for (const [name, mutate] of Object.entries({
  'approved cash count': state => { state.cashReconciliations = [{ id:'CCR-SYNTHETIC', status:'approved', account:'Counter Cash', nature:'SANKI', verifiedThrough:date, ledgerBalance:4998, countedBalance:4998 }]; },
  'finalized bank link': state => { state.bankDateOverrides[saleId] = { bankDate:date, reconciliationDraft:'BST-SYNTHETIC' }; },
  'pending bank component link': state => { state.bankReconciliationDrafts.DRAFT = { id:'DRAFT', nature:'SANKI', account:'Axis Bank 3448', transactions:[], summary:{ from:date, to:date }, resolutions:{ 'bank-0':{ action:'link_existing', appIds:[saleId + '/PAYMENT-1'] } }, expiresAt:'2099-01-01T00:00:00.000Z' }; },
  'audited rounding': state => { state.paytmRoundingReviews.R = { orderId:'synthetic-replacement', amount:1 }; },
  'existing cash split': state => { state.saleAllocationOverrides[saleId] = { cashAmount:2499, reason:'Other reviewed split' }; }
})) test('submission and approval both retain the ' + name + ' safety check', () => {
  reset(mutate); const before = fs.readFileSync(file, 'utf8'), blocked = preview(); assert.equal(blocked.status, 409, JSON.stringify(blocked.body)); assert.equal(fs.readFileSync(file, 'utf8'), before);
  reset(); const p = preview({ role:'admin' }), pending = apply(p.body.previewFingerprint, { role:'admin' }); assert.equal(pending.status, 200, JSON.stringify(pending.body));
  const changed = read(); mutate(changed); write(changed); invoke('/api/expenses/config'); const beforeDecision = fs.readFileSync(file, 'utf8'), result = decide(pending.body.request.id); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(fs.readFileSync(file, 'utf8'), beforeDecision);
});

test('Shopify refresh cannot resurrect false cash, and changed financial evidence is flagged', () => {
  reset(); const p = preview(), applied = apply(p.body.previewFingerprint); assert.equal(applied.status, 200, JSON.stringify(applied.body));
  const source = orders(); source.orders['synthetic-replacement'].updatedAt = '2098-07-01T00:00:00.000Z'; source.orders['synthetic-replacement'].note = 'Shivam\nRoutine source refresh'; writeOrders(source);
  let cash = ledger(); assert.equal(cash.entries.some(row => row.id === saleId && row.credit > 0), false); assert.equal(cash.entries.find(row => row.kind === 'sale_store_credit_correction').stale, false);
  source.orders['synthetic-replacement'].paymentTransactions[0].amount = 2500; writeOrders(source); cash = ledger(); assert.equal(cash.entries.some(row => row.id === saleId && row.credit > 0), false); assert.equal(cash.entries.find(row => row.kind === 'sale_store_credit_correction').stale, true); assert.equal(read().saleStoreCreditCorrections[saleId].walletStatus, 'pending_external_user_action');
});

for (const [name, replaceTransactions] of Object.entries({
  'Shopify temporarily returns no payment transactions': () => [],
  'Shopify introduces a different successful UPI transaction': () => [{ id:'TX-SYNTHETIC-CHANGED-UPI', kind:'sale', status:'success', gateway:'UPI', amount, currency:'INR', processedAt:date + 'T11:00:00.000Z' }]
})) test('approved full-credit correction stays non-cash when ' + name, () => {
  reset(); const p = preview(), applied = apply(p.body.previewFingerprint); assert.equal(applied.status, 200, JSON.stringify(applied.body));
  const correction = clone(read().saleStoreCreditCorrections[saleId]), source = orders(); source.orders['synthetic-replacement'].paymentTransactions = replaceTransactions(); writeOrders(source); const sourceBytes = fs.readFileSync(process.env.ORDERS_PATH, 'utf8');
  for (const role of ['admin','owner']) {
    const cash = ledger('Counter Cash', role); assert.equal(cash.entries.some(row => row.orderId === 'synthetic-replacement' && Number(row.credit || 0) > 0), false);
    assert.equal(cash.entries.find(row => row.id === 'SHOPIFY/synthetic-original').credit, amount);
    const audit = cash.entries.find(row => row.kind === 'sale_store_credit_correction'); assert.ok(audit); assert.equal(audit.stale, true); assert.equal(audit.credit, 0); assert.equal(audit.debit, 0); assert.equal(audit.storeCreditAmount, amount); assert.equal(audit.reason, intent.reason);
    for (const account of ['Paytm Settlement Clearing','Axis Bank 3448']) assert.equal(ledger(account, role).entries.some(row => row.orderId === 'synthetic-replacement' || JSON.stringify(row.connectedSales || []).includes('synthetic-replacement')), false, 'changed source evidence cannot create an unverified provider/bank receipt');
  }
  assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), sourceBytes); assert.deepEqual(read().saleStoreCreditCorrections[saleId], correction);
});

test('rejecting a pending credit correction retains its older ordinary sale audit when history is trimmed', () => {
  reset(); const p = preview({ role:'admin' }), pending = apply(p.body.previewFingerprint, { role:'admin' }); assert.equal(pending.status, 200, JSON.stringify(pending.body));
  const state = read(), oldAudit = { id:'AUD-SYNTHETIC-OLD-SPLIT', action:'SALE_ALLOCATION_CORRECTED', subjectType:'sale', subjectId:saleId, before:null, after:{ cashAmount:amount }, note:'Historical ordinary split evidence remains part of this correction source.' }, requestAudit = clone(state.auditLog.find(row => row.subjectId === pending.body.request.id));
  state.auditLog.unshift(oldAudit);
  state.auditLog.push(...Array.from({ length:10001 }, (_, index) => ({ id:'AUD-SYNTHETIC-UNRELATED-' + index, action:'SYNTHETIC_UNRELATED_EVENT', subjectId:'UNRELATED-' + index, before:null, after:null })));
  state.auditSeq += 10002; write(state); const before = read(), sourceBytes = fs.readFileSync(process.env.ORDERS_PATH, 'utf8');
  const result = decide(pending.body.request.id, { body:{ approve:false } }); assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.request.status, 'rejected');
  const after = read(); financialRecordsUnchanged(before, after); assert.deepEqual(after.saleStoreCreditCorrections, {}); assert.deepEqual(after.requests.find(row => row.id === pending.body.request.id).meta, before.requests.find(row => row.id === pending.body.request.id).meta);
  assert.deepEqual(after.auditLog.find(row => row.id === oldAudit.id), oldAudit); assert.deepEqual(after.auditLog.find(row => row.id === requestAudit.id), requestAudit); assert.equal(after.auditLog.some(row => row.id === 'AUD-SYNTHETIC-UNRELATED-0'), false, 'ordinary old history still follows the normal trim'); assert.ok(after.auditLog.length < before.auditLog.length); assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), sourceBytes);
});

function currentDayCorrection() {
  reset(); const { indiaBusinessDate } = require('../modules/expenses'), today = indiaBusinessDate(), yesterday = new Date(Date.parse(today + 'T00:00:00Z') - 86400000).toISOString().slice(0,10), source = orders();
  for (const row of Object.values(source.orders)) {
    const day = row.id === 'synthetic-original' ? yesterday : today;
    row.createdAt = row.processedAt = day + 'T10:00:00.000Z'; row.paymentTransactions[0].processedAt = day + 'T10:00:00.000Z';
  }
  source.orders['synthetic-original'].refundTransactions[0].processedAt = today + 'T09:00:00.000Z'; writeOrders(source);
  const p = preview(), result = apply(p.body.previewFingerprint); assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.approved, true);
  return { today, yesterday };
}
function driftReplacementReceipt(today) {
  const source = orders(); source.orders['synthetic-replacement'].paymentTransactions[0].processedAt = today + 'T11:00:00.000Z'; writeOrders(source);
}

test('real cash-count route blocks a stale correction instead of finalizing a current-day count', async () => {
  const { today } = currentDayCorrection(); driftReplacementReceipt(today); const before = fs.readFileSync(file, 'utf8'), sourceBytes = fs.readFileSync(process.env.ORDERS_PATH, 'utf8');
  const result = await dispatch('/api/expenses/cash-reconciliations', { body:{ verifiedThrough:today, countedBalance:amount, note:'Synthetic physical count for the genuine original receipt.' } });
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.match(result.body.error, /store.credit correction source evidence changed/i); assert.equal(fs.readFileSync(file, 'utf8'), before); assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), sourceBytes); assert.deepEqual(read().cashReconciliations, []);
});

test('stale correction blocks pending cash-count approval but still allows rejecting that verification', async () => {
  const { today } = currentDayCorrection(), pending = await dispatch('/api/expenses/cash-reconciliations', { role:'admin', username:'prashant', body:{ verifiedThrough:today, countedBalance:amount, note:'Synthetic counted cash after exchange correction.' } });
  assert.equal(pending.status, 200, JSON.stringify(pending.body)); assert.equal(pending.body.approvalPending, true); const id = pending.body.verification.id;
  driftReplacementReceipt(today); const before = fs.readFileSync(file, 'utf8'), correction = clone(read().saleStoreCreditCorrections[saleId]), route = '/api/expenses/cash-reconciliations/' + id + '/decision';
  const approve = await dispatch(route, { body:{ decision:'approve', reason:'Attempt approval after source receipt drift.' } });
  assert.equal(approve.status, 409, JSON.stringify(approve.body)); assert.match(approve.body.error, /store.credit correction source evidence changed/i); assert.equal(fs.readFileSync(file, 'utf8'), before); assert.equal(read().cashReconciliations.find(row => row.id === id).status, 'pending');
  const reject = await dispatch(route, { body:{ decision:'reject', reason:'Reject count because corrected receipt evidence needs review.' } });
  assert.equal(reject.status, 200, JSON.stringify(reject.body)); assert.equal(reject.body.verification.status, 'rejected'); assert.equal(read().cashReconciliations.find(row => row.id === id).status, 'rejected'); assert.deepEqual(read().saleStoreCreditCorrections[saleId], correction); assert.deepEqual(read().receipts, []); assert.deepEqual(read().transfers, []); assert.deepEqual(read().adjustments, []);
});

test('Counter Cash import planning blocks stale source evidence only within the affected period', () => {
  const { bankFinalizationImportPlan } = require('../modules/expenses'), { today, yesterday } = currentDayCorrection(); driftReplacementReceipt(today); const state = read(), before = fs.readFileSync(file, 'utf8');
  const draft = { id:'BRD-SYNTHETIC-CASH', nature:'SANKI', account:'Counter Cash', summary:{ from:yesterday, to:today }, transactions:[] }, plan = bankFinalizationImportPlan(state, draft);
  assert.match(plan.error, /store.credit correction source evidence changed/i); assert.equal(bankFinalizationImportPlan(state, { ...draft, summary:{ from:yesterday, to:yesterday } }).error, undefined); assert.equal(bankFinalizationImportPlan(state, { ...draft, account:'Axis Bank 3448' }).error, undefined); assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('an approved incentive impact blocks correction without editing approved or paid incentives', () => {
  const { calculate } = require('../modules/incentives'); reset(); const source = orders(), companion = order('synthetic-companion', 9003); companion.total = 8000; companion.paymentTransactions[0].amount = 8000; source.orders[companion.id] = companion; writeOrders(source);
  const state = { revision:1, reviews:{}, approvals:{}, payments:[{ id:'IP-SYNTHETIC', salesperson:'Shivam', amount:100, account:'Counter Cash', date, active:true }], audit:[] };
  const day = calculate(Object.values(source.orders), state).daily.find(row => row.date === date && row.salesperson === 'Shivam'); assert.equal(day.qualifies, true);
  state.approvals[day.key] = { id:'IA-SYNTHETIC', date, salesperson:'Shivam', eligibleAmount:day.eligibleAmount, incentiveBaseAmount:day.incentiveBaseAmount, incentive:day.incentive, qualificationSources:day.qualificationSources, approvedBy:'synthetic-owner', approvedAt:date + 'T20:00:00.000Z' }; fs.writeFileSync(process.env.INCENTIVES_PATH, JSON.stringify(state));
  const before = fs.readFileSync(file, 'utf8'), incentiveBytes = fs.readFileSync(process.env.INCENTIVES_PATH, 'utf8'), blocked = preview(); assert.equal(blocked.status, 409, JSON.stringify(blocked.body)); assert.match(blocked.body.error, /incentive/i); assert.equal(fs.readFileSync(file, 'utf8'), before); assert.equal(fs.readFileSync(process.env.INCENTIVES_PATH, 'utf8'), incentiveBytes);
});
