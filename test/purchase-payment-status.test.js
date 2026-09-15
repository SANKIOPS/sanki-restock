const test = require('node:test');
const assert = require('node:assert/strict');
const { purchasePaymentStatus } = require('../modules/purchase-payment-status');
const po = { id:'PO-0002', status:'posted', postedAt:'2026-08-25', newProducts:[{variants:[{landed:125,qty:8}]}] };
test('PO status uses its own accounting payments and preserves partial payments', () => {
  const accounting = {procurementAccounting:{paymentsByPo:{'PO-0002':{payments:[{amount:400,date:'2026-08-26',proof:'/api/expenses/photo/proof.png'}]},'PO-other':{payments:[{amount:1000}]}}}};
  const before = JSON.stringify(accounting);
  assert.equal(purchasePaymentStatus(po,accounting,true).status,'partially_paid');
  assert.equal(purchasePaymentStatus(po,accounting,true).balanceDue,600);
  assert.equal(purchasePaymentStatus(po,accounting,true).payments[0].proof,'/api/expenses/photo/proof.png');
  assert.equal(purchasePaymentStatus(po,accounting,false).payments,undefined);
  assert.equal(JSON.stringify(accounting),before);
  accounting.procurementAccounting.paymentsByPo[po.id].payments.push({amount:600});
  assert.equal(purchasePaymentStatus(po,accounting,true).status,'paid');
});
test('untracked or unavailable history is never presented as unpaid', () => {
  assert.equal(purchasePaymentStatus(po,null,true).status,'not_recorded');
  assert.equal(purchasePaymentStatus({...po,status:'advance'},{},true).status,'not_recorded');
  assert.equal(purchasePaymentStatus({...po,historical:true},{},true).status,'not_recorded');
  assert.equal(purchasePaymentStatus(po,{},true).status,'unpaid');
});

test('every PO has a billing amount before receipt or Shopify posting', () => {
  const { purchaseBillingAmount } = require('../modules/purchase-payment-status');
  for (const status of ['advance', 'received', 'posted']) {
    const pending = {id:'PO-7',status,origin:'india',transportTotal:100,lines:[{qty:10,perPcsYuan:50}]};
    assert.equal(purchaseBillingAmount(pending),600);
    assert.equal(purchasePaymentStatus(pending,{},true).amount,600);
  }
  assert.equal(purchaseBillingAmount({origin:'china',exRate:15,freightPerGram:0.45,lines:[{qty:1,perPcsYuan:60,weightGrams:500}]}),1125);
});
test('sourcing ledger includes bills in every fulfillment status on the purchase date', () => {
  const fs = require('node:fs'), vm = require('node:vm');
  const source = fs.readFileSync(require('node:path').join(__dirname,'../modules/expenses.js'),'utf8');
  const start = source.indexOf('function procurementPayables('), end = source.indexOf('function ledgerMeta(',start);
  const pos = Object.fromEntries(['advance','received','posted'].map((status,i) => ['PO-'+i,{id:'PO-'+i,status,datePurchase:'2026-09-02',dateReceive:'2026-09-09',origin:'india',lines:[{qty:2,perPcsYuan:100}]}]));
  const context = {procurementAccounting:()=>({paymentsByPo:{},mediator:'Mediator'}),loadProcurementStore:()=>({pos,settings:{}}),purchaseBillingAmount:require('../modules/purchase-payment-status').purchaseBillingAmount,round0:Math.round,num:x=>Number(x)||0,poCostBreakdown:()=>({})};
  const read = vm.runInNewContext(source.slice(start,end)+'\nprocurementPayables;',context);
  const rows = read({},true);
  assert.equal(rows.length,3);
  assert.ok(rows.every(row=>row.amount===200 && row.date==='2026-09-02'));
});
