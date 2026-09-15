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
