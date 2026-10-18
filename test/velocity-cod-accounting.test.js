'use strict';

const test=require('node:test');
const assert=require('node:assert/strict');
const velocity=require('../modules/velocity-cod-accounting');

const pendingOrder=(changes={})=>Object.assign({id:'COD-1',channel:'Website',financialStatus:'pending',paymentGateways:['Cash on Delivery (COD)'],total:12000,shipments:[{awb:'VEL-1',status:'in_transit'}],paymentTransactions:[{id:'COD-TX',kind:'sale',status:'pending',gateway:'Cash on Delivery (COD)',amount:12000}]},changes);

test('shipped pending COD becomes a Velocity receivable',()=>{
  const shop={dispatch:{}},order=pendingOrder(),eligible=velocity.isPendingReceivableOrder(shop,order);
  assert.equal(eligible,true);
  assert.equal(velocity.pendingReceivable(order,eligible).amount,12000);
});

test('partial COD sends only its unpaid remainder to Velocity',()=>{
  const shop={dispatch:{}},order=pendingOrder({financialStatus:'partially_paid',total:10000,paymentGateways:['Paytm','Cash on Delivery (COD)'],paymentTransactions:[{id:'PTM',kind:'sale',status:'success',gateway:'Paytm',amount:2000},{id:'COD',kind:'sale',status:'pending',gateway:'Cash on Delivery (COD)',amount:8000}]}),eligible=velocity.isPendingReceivableOrder(shop,order),result=velocity.pendingReceivable(order,eligible);
  assert.equal(result.amount,8000);
  assert.equal(result.successfulTransactions.length,1);
});

test('unshipped, RTO and cancelled COD orders stay outside Velocity',()=>{
  const shop={dispatch:{}};
  assert.equal(velocity.isPendingReceivableOrder(shop,pendingOrder({shipments:[]})),false);
  assert.equal(velocity.isPendingReceivableOrder(shop,pendingOrder({shipments:[{awb:'VEL-RTO',status:'rto'}]})),false);
  assert.equal(velocity.isPendingReceivableOrder(shop,pendingOrder({cancelledAt:'2026-10-16T10:00:00Z'})),false);
});
