'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { CLEARING, BANK, validateOrderLink, autoMatchShopifyNotes, validateSettlementReview, validatePayoutPosting } = require('../modules/paytm-accounting');
const copy = value => JSON.parse(JSON.stringify(value));
function fixture() {
  const order = {
    id:'replacement', number:1002, name:'#1002', channel:'POS', financialStatus:'paid', total:2499,
    createdAt:'2026-10-21T12:00:00Z', note:'654321 Krishna',
    paymentGateways:['Cash'], paymentTransactions:[{id:'cash-replacement',kind:'sale',status:'success',gateway:'Cash',amount:2499}]
  };
  const tx = {transactionId:'PAYMENT654321',date:'2026-10-21',amount:2499,commission:0,gst:0,platformFee:0,settledAmount:2499,payoutId:'PAYOUT-EXCHANGE-TEST',utr:'UTR-EXCHANGE-TEST',settledDate:'2026-10-22',isCustomerPayment:true};
  const link = {transactionId:tx.transactionId,orderId:order.id,orderNumber:'1002',amount:2499};
  const rows = [{id:'SHOPIFY/replacement/NONCASH',orderId:order.id,orderNumber:'1002',account:CLEARING,amount:2499,date:tx.date}];
  const bank = {id:'bank-credit',date:tx.settledDate,credit:2499,debit:0,reference:tx.utr};
  const store = {
    paytmReportTransactions:{[tx.transactionId]:tx}, paytmOrderLinks:{}, paytmManualResolutions:{},
    paytmRoundingReviews:{}, paytmPayoutPostings:[], paytmVerifiedSettlements:[], paytmSettlements:[],
    bankStatements:{[BANK]:{transactions:{[bank.id]:bank},imports:[]}}, bankDateOverrides:{}, requests:[]
  };
  return {order,tx,link,rows,bank,store};
}
function approved(f) {
  f.store.saleStoreCreditCorrections = {'SHOPIFY/replacement':{
    treatment:'exchange_store_credit',saleId:'SHOPIFY/replacement',orderId:'replacement',originalOrderId:'original',originalOrderNumber:'1001',
    cashAmount:0,nonCashAmount:0,storeCreditAmount:2499,reason:'Exchange used existing customer credit.',walletStatus:'pending_external_user_action'
  }};
}
function pending(f) {
  f.store.requests.push({id:'RQ-exchange',kind:'sale_store_credit',status:'pending',meta:{saleId:'SHOPIFY/replacement',orderId:'replacement',storeCreditAmount:2499}});
}

test('approved full-credit correction blocks a new Paytm link with its own order and source reason', () => {
  const f = fixture(); approved(f);
  const before = copy(f);
  assert.throws(() => validateOrderLink(f.store,f.tx,f.order.id,[f.order],f.rows,'A reviewed source collection'), /#1002.*approved store-credit exchange correction/);
  assert.deepEqual(f,before);
});

test('pending Owner correction blocks both ID and order-number Paytm selection', () => {
  const f = fixture(); pending(f);
  for (const selected of ['replacement','1002','#1002']) {
    assert.throws(() => validateOrderLink(f.store,f.tx,selected,[f.order],f.rows,'A reviewed source collection'), /#1002.*awaiting Owner approval/);
  }
  assert.deepEqual(f.store.paytmOrderLinks,{});
});

test('automatic note matching leaves approved and pending credit orders unmatched', () => {
  for (const block of [approved,pending]) {
    const f = fixture(); block(f); const before = copy(f);
    assert.deepEqual(autoMatchShopifyNotes(f.store,f.store.paytmReportTransactions,[f.order],f.rows),[]);
    assert.deepEqual(f,before);
  }
});

test('settlement review rechecks the correction even if a link and source row already exist', () => {
  for (const block of [approved,pending]) {
    const f = fixture(); f.store.paytmOrderLinks[f.tx.transactionId] = f.link; block(f);
    const before = copy(f);
    assert.throws(() => validateSettlementReview(f.store,f.tx.payoutId,f.rows), /#1002.*store-credit exchange correction/);
    assert.deepEqual(f,before);
  }
});

test('posting cannot bypass the credit correction using an exact amount, UTR and bank date', () => {
  for (const block of [approved,pending]) {
    const f = fixture(); f.store.paytmOrderLinks[f.tx.transactionId] = f.link; block(f);
    const before = copy(f);
    assert.throws(() => validatePayoutPosting(f.store,f.tx.payoutId,f.bank.id,f.rows), /#1002.*store-credit exchange correction/);
    assert.deepEqual(f,before);
  }
});

test('a rounding marker does not bypass the full-credit check', () => {
  const f = fixture(); approved(f);
  f.store.paytmOrderLinks[f.tx.transactionId] = f.link;
  f.store.paytmRoundingReviews[f.tx.transactionId] = {orderId:f.order.id};
  assert.throws(() => validateSettlementReview(f.store,f.tx.payoutId,f.rows), /approved store-credit exchange correction/);
});

test('unrelated or rejected credit corrections preserve the existing exact Paytm validation', () => {
  const f = fixture();
  f.store.saleStoreCreditCorrections = {'SHOPIFY/another':{originalOrderId:f.order.id,storeCreditAmount:2499}};
  f.store.requests = [{id:'RQ-rejected',kind:'sale_store_credit',status:'rejected',meta:{saleId:'SHOPIFY/replacement'}}];
  const validated = validateOrderLink(f.store,f.tx,'1002',[f.order],f.rows,'');
  assert.equal(validated.orderId,f.order.id);
  f.store.paytmOrderLinks[f.tx.transactionId] = validated;
  assert.equal(validateSettlementReview(f.store,f.tx.payoutId,f.rows).linked[0].orderId,f.order.id);
  assert.equal(validatePayoutPosting(f.store,f.tx.payoutId,f.bank.id,f.rows).bank.id,f.bank.id);
  f.tx.amount = 3000;
  assert.throws(() => validateOrderLink(f.store,f.tx,'1002',[f.order],f.rows,''), /exceed/);
});
