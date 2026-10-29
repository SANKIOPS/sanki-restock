'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { fingerprint, prepare, projectOrder, auditRows, assertPreserved, sourceRevision, protectedAuditSubjects } = require('../modules/sale-store-credit-corrections');
const copy = value => JSON.parse(JSON.stringify(value));
function fixture() {
  const original = {
    id:'old', number:1001, name:'#1001', channel:'POS', currency:'INR',
    createdAt:'2026-10-20T12:00:00Z', processedAt:'2026-10-20T12:00:00Z',
    customer:{id:'customer-a'}, total:2499, refundAmount:2499, moneyRefunded:0, storeCreditIssued:2499, financialStatus:'refunded',
    paymentGateways:['Cash'], paymentTransactions:[{id:'cash-old', kind:'sale', status:'success', gateway:'Cash', amount:2499}],
    refundTransactions:[{id:'refund-old', parentId:'cash-old', refundId:'return-old', gateway:'store_credit', amount:2499, processedAt:'2026-10-21T09:00:00Z'}]
  };
  const replacement = {
    id:'new', number:1002, name:'#1002', channel:'POS', currency:'INR',
    createdAt:'2026-10-21T12:00:00Z', processedAt:'2026-10-21T12:00:00Z',
    customer:{id:'customer-a'}, total:2499, refundAmount:0, moneyRefunded:0, storeCreditIssued:0, financialStatus:'paid',
    paymentGateways:['Cash'], paymentTransactions:[{id:'cash-new', kind:'sale', status:'success', gateway:'Cash', amount:2499}]
  };
  return { orders:{old:original,new:replacement}, store:{}, input:{saleId:'SHOPIFY/new', originalOrderNumber:'#1001', storeCreditAmount:2499, reason:'Exchange used credit; no new cash was received.'} };
}
function rejected(action, status, message) {
  assert.throws(action, error => error.status === status && (!message || message.test(error.message)));
}
function approve(f) {
  const proposal = prepare(f.orders, f.store, f.input);
  f.store.saleStoreCreditCorrections = { 'SHOPIFY/new':{...proposal, approvedBy:'owner', approvedAt:'2026-10-22T12:00:00Z'} };
  return proposal;
}

test('full cash-to-credit proposal is pure, traceable and explicitly pending external wallet action', () => {
  const f = fixture(), before = copy(f), plan = prepare(f.orders, f.store, f.input);
  assert.deepEqual(f, before);
  assert.equal(plan.saleId, 'SHOPIFY/new');
  assert.equal(plan.originalOrderId, 'old');
  assert.equal(plan.storeCreditAmount, 2499);
  assert.equal(plan.cashAmount, 0);
  assert.equal(plan.nonCashAmount, 0);
  assert.equal(plan.date, '2026-10-21');
  assert.equal(plan.walletStatus, 'pending_external_user_action');
  assert.deepEqual(plan.transactionIds, ['cash-new']);
  assert.equal(plan.sourceFingerprint, fingerprint(f.orders.new));
  assert.equal(plan.originalFingerprint, fingerprint(f.orders.old));
});

test('order number matching is exact and accepts the ordinary optional hash and leading zeros', () => {
  const f = fixture();
  for (const number of ['1001', '#1001', '001001', '# 001001']) assert.equal(prepare(f.orders, f.store, {...f.input, originalOrderNumber:number}).originalOrderId, 'old');
  rejected(() => prepare(f.orders, f.store, {...f.input, originalOrderNumber:'100'}), 400, /not found/);
  f.orders.other = {...copy(f.orders.old), id:'other'};
  rejected(() => prepare(f.orders, f.store, f.input), 400, /ambiguous/);
});

test('same order, mismatched or missing customer IDs and non-POS sources cannot be linked', () => {
  for (const mutate of [
    f => { f.input.originalOrderNumber = 1002; },
    f => { f.orders.old.customer.id = 'another-customer'; },
    f => { f.orders.new.customer.id = ''; },
    f => { f.orders.old.channel = 'Website'; },
    f => { f.orders.new.channel = 'Website'; },
    f => { f.orders.new.currency = 'USD'; }
  ]) {
    const f = fixture(); mutate(f); rejected(() => prepare(f.orders, f.store, f.input), 400);
  }
});

test('invalid or partial monetary inputs and blank reasons fail without coercion to zero', () => {
  for (const amount of [undefined, null, '', ' ', 'not-money', -2499, 0, 2498, 2499.001, Infinity, true]) {
    const f = fixture(); rejected(() => prepare(f.orders, f.store, {...f.input, storeCreditAmount:amount}), 400);
  }
  const f = fixture();
  rejected(() => prepare(f.orders, f.store, {...f.input, reason:' '}), 400, /reason/);
  assert.equal(prepare(f.orders, f.store, {...f.input, storeCreditAmount:'2499.00'}).storeCreditAmount, 2499);
  f.orders.new.refundAmount = -1;
  rejected(() => prepare(f.orders, f.store, f.input), 400, /Replacement refund/);
});

test('replacement cancellations and every supported positive refund source block the correction', () => {
  for (const change of [
    {cancelledAt:'2026-10-22T00:00:00Z'}, {financialStatus:'voided'},
    {refundAmount:1}, {moneyRefunded:1}, {storeCreditIssued:1},
    {refundTransactions:[{id:'ref-new', gateway:'Cash', amount:1}]},
    {paymentTransactions:[{id:'ref-new', kind:'refund', status:'success', gateway:'Cash', amount:1}]},
    {refunds:[{id:'return-new', amount:0, lineItems:[{qty:1}]}]}
  ]) {
    const f = fixture(); Object.assign(f.orders.new, change);
    rejected(() => prepare(f.orders, f.store, f.input), 409);
  }
});

test('receipt evidence must be successful, all cash, uniquely identified and total exactly to the bill', () => {
  for (const transactions of [
    [], [{id:'cash-new', kind:'sale', status:'pending', gateway:'Cash', amount:2499}],
    [{id:'cash-new', kind:'sale', status:'success', gateway:'Paytm', amount:2499}],
    [{id:'cash-new', kind:'sale', status:'success', gateway:'Cash on Delivery', amount:2499}],
    [{id:'', kind:'sale', status:'success', gateway:'Cash', amount:2499}],
    [{id:'same', kind:'sale', status:'success', gateway:'Cash', amount:1249},{id:'same', kind:'capture', status:'success', gateway:'Cash', amount:1250}],
    [{id:'cash-new', kind:'sale', status:'success', gateway:'Cash', amount:2498}],
    [{id:'cash-new', kind:'sale', status:'success', gateway:'Cash', amount:2499, currency:'USD'}]
  ]) {
    const f = fixture(); f.orders.new.paymentTransactions = transactions;
    assert.throws(() => prepare(f.orders, f.store, f.input), error => [400,409].includes(error.status));
  }
  const f = fixture();
  f.orders.new.paymentTransactions = [{id:'second', kind:'capture', status:'success', gateway:'Cash', amount:1250},{id:'first', kind:'sale', status:'success', gateway:'Cash', amount:1249}];
  assert.deepEqual(prepare(f.orders, f.store, f.input).transactionIds, ['first','second']);
});

test('issued original credit and available refund evidence must independently support the amount', () => {
  const f = fixture();
  f.orders.old.storeCreditIssued = 2498;
  rejected(() => prepare(f.orders, f.store, f.input), 409, /enough issued/);
  f.orders.old.storeCreditIssued = 2499;
  f.orders.old.refundTransactions[0].gateway = 'Cash';
  rejected(() => prepare(f.orders, f.store, f.input), 409, /refund transaction evidence/);
  f.orders.old.refundTransactions = [];
  assert.equal(prepare(f.orders, f.store, f.input).storeCreditAmount, 2499, 'the explicit normalized issued-credit field remains usable when transaction detail is unavailable');
});

test('duplicate embedded and normalized refunds cannot inflate original credit capacity', () => {
  const f = fixture();
  f.orders.old.storeCreditIssued = 4998;
  f.orders.old.paymentTransactions.push({...copy(f.orders.old.refundTransactions[0]), kind:'refund', status:'success'});
  f.store.saleStoreCreditCorrections = {'SHOPIFY/other':{originalOrderId:'old',storeCreditAmount:2499}};
  rejected(() => prepare(f.orders, f.store, f.input), 409, /allocated or reserved/);
});

test('existing split/component overrides and reviewed Paytm or GoKwik evidence block independent tender changes', () => {
  for (const store of [
    {saleAllocationOverrides:{'SHOPIFY/new':{cashAmount:2499}}},
    {saleComponentAmountOverrides:{'SHOPIFY/new/PAYMENT-1':{amount:2499}}},
    {paytmOrderLinks:{tx:{orderId:'new'}}},
    {paytmRoundingReviews:{tx:{orderId:'new'}}},
    {paytmPayoutPostings:[{id:'posting', orderIds:['new']}]},
    {paytmVerifiedSettlements:[{id:'verified', orderIds:['new']}]},
    {gokwikOrderLinks:{payment:{orderId:'new'}}},
    {gokwikPostings:{batch:{orderId:'new'}}},
    {gokwikPostings:{batch:{orderIds:['new']}}},
    {paytmShopifyPayments:{new:{paytmAmount:2499}}}
  ]) {
    const f = fixture(); rejected(() => prepare(f.orders, store, f.input), 409);
  }
});

test('unrelated GoKwik links and postings do not block another order correction', () => {
  const f = fixture();
  f.store.gokwikOrderLinks = {payment:{orderId:'unrelated'}};
  f.store.gokwikPostings = {batch:{orderIds:['unrelated']}};
  assert.equal(prepare(f.orders,f.store,f.input).storeCreditAmount,2499);
  f.store.gokwikPostings.batch.orderIds.push('new');
  rejected(() => prepare(f.orders,f.store,f.input),409,/Paytm\/GoKwik payment-provider evidence/);
});

test('approved allocations and pending requests reserve original credit without double-counting own approval', () => {
  const f = fixture(), proposal = prepare(f.orders, f.store, f.input);
  f.store.requests = [{id:'RQ-current',kind:'sale_store_credit',status:'pending',meta:proposal}];
  assert.deepEqual(prepare(f.orders, f.store, proposal, {excludeRequestId:'RQ-current'}), proposal);
  assert.deepEqual(prepare(f.orders, f.store, f.input), proposal, 'an identical pending retry is harmless');
  rejected(() => prepare(f.orders, f.store, {...f.input,reason:'A different change'}), 409, /different correction/);
  f.store.requests[0].meta = {...proposal, saleId:'SHOPIFY/another', orderId:'another'};
  rejected(() => prepare(f.orders, f.store, f.input), 409, /allocated or reserved/);
  f.store.requests[0].status = 'rejected';
  assert.equal(prepare(f.orders, f.store, f.input).storeCreditAmount, 2499);
  f.store.requests = [{kind:'sale_store_credit',status:'pending',meta:{...proposal,saleId:'SHOPIFY/another',orderId:'another'}}];
  rejected(() => prepare(f.orders, f.store, f.input), 409, /allocated or reserved/);
  f.store.requests = [];
  f.store.saleStoreCreditCorrections = {'SHOPIFY/another':{...proposal,saleId:'SHOPIFY/another',orderId:'another'}};
  rejected(() => prepare(f.orders, f.store, f.input), 409, /allocated or reserved/);
});

test('identical approved retries exclude themselves from capacity and conflicting amendments fail', () => {
  const f = fixture(), proposal = approve(f);
  assert.deepEqual(prepare(f.orders, f.store, f.input), proposal);
  rejected(() => prepare(f.orders, f.store, {...f.input,reason:'Changed accounting intent'}), 409, /different approved/);
});

test('source fingerprints ignore ordinary metadata and source-array ordering but include financial evidence', () => {
  const f = fixture(), before = fingerprint(f.orders.new);
  Object.assign(f.orders.new,{note:'A new note',updatedAt:'2026-10-25T12:00:00Z',city:'Delhi'});
  f.orders.new.customer.name = 'Updated spelling';
  assert.equal(fingerprint(f.orders.new), before);
  const split = copy(f.orders.new);
  split.paymentTransactions.push({id:'auth',kind:'authorization',status:'success',gateway:'Cash',amount:2499});
  const multiple = fingerprint(split); split.paymentTransactions.reverse();
  assert.equal(fingerprint(split), multiple);
  for (const change of [
    order => { order.total++; }, order => { order.customer.id = 'other'; },
    order => { order.paymentTransactions[0].amount++; },
    order => { order.paymentTransactions[0].gateway = 'Paytm'; },
    order => { order.refundAmount = 1; }
  ]) {
    const changed = copy(f.orders.new); change(changed);
    assert.notEqual(fingerprint(changed), before);
  }
});

test('approval detects stale replacement or original evidence even when the current totals remain valid', () => {
  for (const change of [
    f => { f.orders.new.paymentTransactions[0].id = 'different-cash'; },
    f => { f.orders.old.refundTransactions[0].id = 'different-refund'; }
  ]) {
    const f = fixture(), proposal = prepare(f.orders, f.store, f.input); change(f);
    rejected(() => prepare(f.orders, f.store, proposal, {excludeRequestId:'own'}), 409, /evidence changed/);
  }
});

test('projected replacement changes only tender classification and leaves raw orders and original receipts intact', () => {
  const f = fixture(); approve(f);
  const before = copy(f), projected = projectOrder(f.orders.new, f.store, f.orders);
  assert.deepEqual(f, before);
  assert.equal(projected.paymentTransactions[0].gateway,'store_credit');
  assert.equal(projected.paymentTransactions[0].amount,2499);
  assert.equal(projected.paymentTransactions[0].appStoreCreditCorrection.stale,false);
  assert.deepEqual(projected.appStoreCreditCorrection.transactionIds,['cash-new']);
  assert.equal(projected.appStoreCreditCorrection.walletStatus,'pending_external_user_action');
  assert.equal(projected.total,2499);
  assert.equal(projected.refundAmount,0);
  assert.deepEqual(projectOrder(f.orders.old,f.store,f.orders),f.orders.old);
  projected.paymentTransactions[0].amount = 0;
  assert.equal(f.orders.new.paymentTransactions[0].amount,2499);
});

test('stale sync never restores the disputed cash and marks both transaction and order evidence for review', () => {
  const f = fixture(); approve(f);
  f.orders.new.paymentTransactions[0].id = 'cash-after-resync';
  let projected = projectOrder(f.orders.new,f.store,f.orders);
  assert.equal(projected.paymentTransactions[0].gateway,'store_credit');
  assert.equal(projected.appStoreCreditCorrection.stale,true);
  assert.equal(projected.paymentTransactions[0].appStoreCreditCorrection.stale,true);
  f.orders.old.refundTransactions[0].amount = 2000;
  projected = projectOrder(f.orders.new,f.store,f.orders);
  assert.equal(projected.appStoreCreditCorrection.originalFingerprintStale,true);
  assert.equal(projected.appStoreCreditCorrection.walletStatus,'pending_external_user_action');
  assert.equal(f.orders.new.paymentTransactions[0].gateway,'Cash');
});

test('a pending request does not project an approved correction', () => {
  const f = fixture(), proposal = prepare(f.orders,f.store,f.input);
  f.store.requests = [{id:'RQ-1',kind:'sale_store_credit',status:'pending',meta:proposal}];
  const projected = projectOrder(f.orders.new,f.store,f.orders);
  assert.deepEqual(projected,f.orders.new);
  assert.equal(projected.appStoreCreditCorrection,undefined);
  assert.deepEqual(auditRows(f.store,f.orders),[]);
});

test('audit rows retain a visible zero-cash correction, reason, linked order and external pending status', () => {
  const f = fixture(); approve(f);
  const before = copy(f), rows = auditRows(f.store,f.orders);
  assert.deepEqual(f,before);
  assert.equal(rows.length,1);
  assert.equal(rows[0].kind,'sale_store_credit_correction');
  assert.equal(rows[0].credit,0);
  assert.equal(rows[0].debit,0);
  assert.equal(rows[0].cashAmount,0);
  assert.equal(rows[0].nonCashAmount,0);
  assert.equal(rows[0].storeCreditAmount,2499);
  assert.equal(rows[0].reason,f.input.reason);
  assert.equal(rows[0].originalOrderId,'old');
  assert.equal(rows[0].stale,false);
  assert.equal(rows[0].walletStatus,'pending_external_user_action');
  delete f.orders.new;
  const missing = auditRows(f.store,f.orders)[0];
  assert.equal(missing.stale,true);
  assert.equal(missing.date,'2026-10-21','retains the original receipt date after a source disappears');
});

test('a later Shopify source date cannot move the correction audit outside its original affected period', () => {
  const f = fixture(); approve(f);
  f.orders.new.processedAt = '2026-11-15T12:00:00Z';
  const row = auditRows(f.store,f.orders)[0];
  assert.equal(row.date,'2026-10-21');
  assert.equal(row.stale,true);
  assert.equal(row.credit,0);
  assert.equal(row.debit,0);
  assert.equal(auditRows(f.store,f.orders).filter(item => item.date <= '2026-10-31' && item.stale).length,1);
  delete f.store.saleStoreCreditCorrections['SHOPIFY/new'].date;
  assert.equal(auditRows(f.store,f.orders)[0].date,'2026-11-15','legacy corrections without a stored date use the available source date');
});

function protectedStore() {
  const f = fixture(), proposal = prepare(f.orders,f.store,f.input);
  const request = {id:'RQ-credit',kind:'sale_store_credit',name:'Exchange credit correction',nature:'SANKI',meta:proposal,status:'pending',by:'admin',at:'2026-10-21T14:00:00Z',decidedBy:null,decidedAt:null};
  return {
    requests:[request], saleStoreCreditCorrections:{},
    auditLog:[{id:'AUD-credit-request',action:'SALE_STORE_CREDIT_CORRECTION_REQUESTED',subjectId:request.id,after:copy(request),at:request.at}],
    receipts:[{id:'receipt-unrelated',amount:100}]
  };
}
function approvedStore() {
  const store = protectedStore(), request = store.requests[0];
  const correction = {...copy(request.meta),requestedBy:'admin',approvedBy:'owner',approvedAt:'2026-10-22T10:00:00Z',approvalRequestId:request.id,revision:1};
  store.saleStoreCreditCorrections[correction.saleId] = correction;
  Object.assign(request,{status:'approved',decidedBy:'owner',decidedAt:correction.approvedAt,correction:copy(correction)});
  store.auditLog.push(
    {id:'AUD-credit-applied',action:'SALE_STORE_CREDIT_CORRECTED',subjectId:correction.saleId,after:copy(correction)},
    {id:'AUD-credit-approved',action:'REQUEST_APPROVED',subjectId:request.id,after:copy(request)}
  );
  return store;
}

test('preservation rejects a stale whole-store save missing a newer approved correction without mutating either snapshot', () => {
  const persisted = approvedStore(), stale = protectedStore(), before = copy({persisted,stale});
  rejected(() => assertPreserved(stale,persisted),409,/remove or overwrite/);
  assert.deepEqual({persisted,stale},before);
  const changed = copy(persisted);
  changed.saleStoreCreditCorrections['SHOPIFY/new'].cashAmount = 2499;
  rejected(() => assertPreserved(changed,persisted),409,/correction/);
});

test('preservation retains pending requests and their exact financial identity and array evidence', () => {
  const persisted = protectedStore();
  for (const change of [
    next => { next.requests = []; },
    next => { next.requests[0].meta.storeCreditAmount = 2500; },
    next => { next.requests[0].by = 'another-admin'; },
    next => { next.requests[0].kind = 'vendor'; },
    next => { next.requests[0].at = '2026-10-22T00:00:00Z'; },
    next => { next.requests[0].meta.transactionIds.push('different-source'); }
  ]) {
    const next = copy(persisted); change(next);
    rejected(() => assertPreserved(next,persisted),409,/request/);
  }
});

test('fresh Owner approval and rejection may append decisions while keeping request metadata unchanged', () => {
  const persisted = protectedStore(), approved = approvedStore(), before = copy({persisted,approved});
  assert.equal(assertPreserved(approved,persisted),true);
  assert.deepEqual({persisted,approved},before);
  const rejectedRequest = copy(persisted);
  Object.assign(rejectedRequest.requests[0],{status:'rejected',decidedBy:'owner',decidedAt:'2026-10-22T10:00:00Z'});
  rejectedRequest.auditLog.push({id:'AUD-rejected',action:'REQUEST_REJECTED',subjectId:'RQ-credit'});
  assert.equal(assertPreserved(rejectedRequest,persisted),true);
  approved.requests[0].meta.reason = 'Changed the request during approval';
  rejected(() => assertPreserved(approved,persisted),409,/request/);
});

test('terminal request decisions and their original approval evidence cannot be rolled back or rewritten', () => {
  const persisted = approvedStore();
  for (const change of [
    next => { next.requests[0].status = 'pending'; },
    next => { next.requests[0].status = 'rejected'; },
    next => { next.requests[0].decidedBy = 'another-owner'; },
    next => { next.requests[0].correction.reason = 'Changed approval'; }
  ]) {
    const next = copy(persisted); change(next);
    rejected(() => assertPreserved(next,persisted),409,/request/);
  }
  const rejectedRequest = protectedStore();
  Object.assign(rejectedRequest.requests[0],{status:'rejected',decidedBy:'owner',decidedAt:'2026-10-22T10:00:00Z'});
  const rollback = copy(rejectedRequest); rollback.requests[0].status = 'pending';
  rejected(() => assertPreserved(rollback,rejectedRequest),409,/request/);
});

test('protected audit events cannot disappear, be overwritten, reordered or have their identity reused', () => {
  const persisted = approvedStore();
  persisted.auditLog.push({id:'AUD-sale-related',action:'SOURCE_REVIEWED',subjectId:'SHOPIFY/new',details:{references:['first','second']}});
  for (const change of [
    next => { next.auditLog.shift(); },
    next => { next.auditLog[0].after.meta.storeCreditAmount = 1; },
    next => { next.auditLog[2].action = 'OTHER_ACTION'; },
    next => { next.auditLog.reverse(); },
    next => { next.auditLog.at(-1).details.references.reverse(); },
    next => { next.auditLog.push({...next.auditLog[0],action:'DUPLICATE_ID'}); }
  ]) {
    const next = copy(persisted); change(next);
    rejected(() => assertPreserved(next,persisted),409,/audit history/);
  }
});

test('canonical object key ordering is irrelevant while unrelated changes and new records remain allowed', () => {
  const persisted = approvedStore(), next = copy(persisted);
  next.saleStoreCreditCorrections['SHOPIFY/new'] = Object.fromEntries(Object.entries(next.saleStoreCreditCorrections['SHOPIFY/new']).reverse());
  next.requests[0].meta = Object.fromEntries(Object.entries(next.requests[0].meta).reverse());
  next.auditLog[0] = Object.fromEntries(Object.entries(next.auditLog[0]).reverse());
  next.receipts = [{id:'a-new-unrelated-receipt',amount:500}];
  next.unrelatedSettings = {anything:true};
  next.saleStoreCreditCorrections['SHOPIFY/another'] = {...copy(next.saleStoreCreditCorrections['SHOPIFY/new']),saleId:'SHOPIFY/another',orderId:'another'};
  next.requests.push({id:'RQ-new',kind:'sale_store_credit',status:'pending',meta:{saleId:'SHOPIFY/another'}});
  next.auditLog.push({id:'AUD-new',action:'SALE_STORE_CREDIT_CORRECTION_REQUESTED',subjectId:'RQ-new'});
  assert.equal(assertPreserved(next,persisted),true);
});

test('unrelated legacy audit entries are outside the narrow preservation guard', () => {
  const persisted = protectedStore(), next = copy(persisted);
  persisted.auditLog.unshift({id:'AUD-unrelated',action:'VENDOR_CREATED',subjectId:'vendor-other'});
  assert.equal(assertPreserved(next,persisted),true);
  const plain = {receipts:[{id:'legacy',amount:10}],auditLog:[{id:'old',action:'OLD',subjectId:'other'}]};
  assert.equal(assertPreserved({},plain),true);
});

test('protected source revision is deterministic and ignores unrelated accounting changes', () => {
  const before = approvedStore(), after = copy(before), revision = sourceRevision(before);
  after.receipts = [];
  after.auditLog.push({id:'AUD-unrelated',action:'VENDOR_CREATED',subjectId:'vendor-other'});
  after.saleStoreCreditCorrections['SHOPIFY/new'] = Object.fromEntries(Object.entries(after.saleStoreCreditCorrections['SHOPIFY/new']).reverse());
  assert.equal(sourceRevision(after),revision);
  after.requests[0].meta.reason = 'Changed source';
  assert.notEqual(sourceRevision(after),revision);
});

test('audit trimming uses the same protected subjects as preservation for pending and approved corrections', () => {
  const store = protectedStore();
  store.saleStoreCreditCorrections = {'SHOPIFY/map-key':{saleId:'SHOPIFY/approved-sale',approvalRequestId:'RQ-approved'}};
  const before = copy(store), subjects = protectedAuditSubjects(store);
  for (const id of ['RQ-credit','SHOPIFY/new','SHOPIFY/map-key','SHOPIFY/approved-sale','RQ-approved']) assert.equal(subjects.has(id),true,id);
  assert.equal(subjects.has('receipt-unrelated'),false);
  assert.deepEqual(store,before);
});
