'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const gokwik=require('../modules/gokwik-accounting');
const order={id:'42',orderNumber:'9002',total:2499,paymentTransactions:[{id:'late-evidence',gateway:'easebuzz',kind:'sale',status:'success',amount:2499}]};
const tx={paymentId:'SYNTH-CREDIT-1',orderId:'42',orderNumber:'9002',gross:2499};
function state(){return {gokwikTransactions:{[tx.paymentId]:tx},gokwikOrderLinks:{},paytmOrderLinks:{},bankDateOverrides:{},bankStatements:{},requests:[]};}
test('approved exchange cannot be assigned to GoKwik after Shopify gateway evidence changes',()=>{
  const store=state();store.saleStoreCreditCorrections={'SHOPIFY/42':{orderId:'42',treatment:'exchange_store_credit',storeCreditAmount:2499}};
  const before=JSON.stringify(store);assert.throws(()=>gokwik.validateLink(store,tx,[order]),/store-credit exchange.*no new GoKwik/i);assert.equal(JSON.stringify(store),before);
});
test('pending exchange reserves its payment classification against another provider',()=>{
  const store=state();store.requests=[{kind:'sale_store_credit',status:'pending',meta:{orderId:'42'}}];
  assert.throws(()=>gokwik.validateLink(store,tx,[order]),/pending store-credit exchange/i);
});
test('rejected or unrelated exchange does not block verified GoKwik collection',()=>{
  const store=state();store.requests=[{kind:'sale_store_credit',status:'rejected',meta:{orderId:'42'}},{kind:'sale_store_credit',status:'pending',meta:{orderId:'other'}}];store.saleStoreCreditCorrections={'SHOPIFY/other':{orderId:'other'}};
  assert.equal(gokwik.validateLink(store,tx,[order]).orderId,'42');
});
