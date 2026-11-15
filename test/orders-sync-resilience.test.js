'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');

process.env.SHOPIFY_STORE = process.env.SHOPIFY_STORE || 'fixture.myshopify.com';
const orders = require('../modules/orders');

function raw(id) {
  return {
    id, name:'#'+id, order_number:id, created_at:'2026-11-11T10:00:00Z', updated_at:'2026-11-11T10:00:00Z',
    source_name:'pos', financial_status:'paid', fulfillment_status:'fulfilled', currency:'INR',
    subtotal_price:'3598', total_discounts:'0', total_tax:'0', total_price:'3598', line_items:[],
    refunds:[], fulfillments:[], payment_gateway_names:['Cash'], discount_codes:[]
  };
}

test('one failed Shopify transaction-detail request does not suppress unrelated POS cash orders', async () => {
  const input=[raw(2951),raw(2952)],previous={id:'2952-payment',kind:'sale',status:'success',gateway:'cash',amount:100};
  const store={orders:{'2952':{paymentTransactions:[previous],paymentTransactionSchemaVersion:2}}};
  const errors=await orders.enrichOrdersWithTransactions(input,store,async url=>{
    if(url.includes('/2952/'))throw Error('Shopify transaction timeout');
    return [{id:'tx-2951',kind:'sale',status:'success',gateway:'cash',amount:'3598'}];
  });
  assert.equal(errors.length,1);
  const imported=orders.normalizeOrder(input[0]);
  const preserved=orders.normalizeOrder(input[1],store.orders['2952']);
  assert.equal(imported.id,'2951');
  assert.equal(imported.paymentTransactions[0].amount,3598);
  assert.equal(imported.paymentTransactionSchemaVersion,2);
  assert.deepEqual(preserved.paymentTransactions,[previous]);
  assert.equal(preserved.paymentTransactionSchemaVersion,2);
});

test('Velocity payout evidence still refreshes when the separate finance feed fails', async () => {
  const result=await orders.syncVelocityEvidence({
    finance:async()=>{throw Error('finance provider unavailable');},
    payouts:async()=>({batches:4,allocationIssues:0,lastSync:'2026-11-15T12:00:00Z'}),
    payoutConfigured:true
  });
  assert.equal(result.success,true);
  assert.equal(result.payouts.batches,4);
  assert.equal(result.errors[0].source,'finance');
});

