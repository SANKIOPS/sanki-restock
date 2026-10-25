'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const authority=require('../modules/bank-amount-authority');
const clone=value=>structuredClone(value),account='IndusInd Bank 7883';
function fixture(kind='payment',marker='source'){
  const id=kind==='receipt'?'REC-FEE':'EX-FEE/PAY-001',movement={id,date:'2026-10-20',debit:kind==='payment'?100:0,credit:kind==='receipt'?100:0,entity:'SAMAST',description:'Synthetic fee'};
  const bank={date:'2026-10-21',debit:kind==='payment'?100.5:0,credit:kind==='receipt'?100.5:0,reference:'FEE-REF',description:'Synthetic fee'};
  const draft={id:'FEE-DRAFT',account,nature:'SAMAST',transactions:[bank],resolutions:{}},store={expenses:{},receipts:[],bankStatements:{},bankDateOverrides:{},bankReconciliationLinks:{},bankReconciliationDrafts:{[draft.id]:draft},rawMovements:[movement]};
  if(kind==='receipt')store.receipts.push({id,account,nature:'SAMAST',amount:100,date:movement.date,receiptType:'other_income',...(marker==='source'?{rentalFeeId:'FEE-ID'}:{})});
  else store.expenses['EX-FEE']={id:'EX-FEE',account,nature:'SAMAST',amount:100,requestedAmount:100,paidAmount:100,status:'paid',approvedAt:'2026-10-20',date:movement.date,...(marker==='source'?{rentalFeeId:'FEE-ID'}:{}),payments:[{id:'PAY-001',account,date:movement.date,amount:100,...(marker==='payment'?{rentalFeeId:'FEE-ID'}:{})}]};
  const options={universal:true,mode:'manual',bankRowId:'bank-0',resolveMovement:s=>s.rawMovements};
  return{store,draft,movement,bank,options};
}
for(const [kind,marker] of [['payment','source'],['payment','payment'],['receipt','source']])for(const mode of ['auto','manual'])test(kind+' fee '+marker+' marker blocks '+mode+' universal cash fallback without changes',()=>{
  const f=fixture(kind,marker),before=clone(f.store);f.options.mode=mode;
  assert.throws(()=>authority.plan(f.store,f.draft,f.bank,f.movement,f.options),/Tenant fee/);assert.deepEqual(f.store,before);assert.equal(f.store.bankMovementAuthority,undefined);
});
test('an aggregate containing a fee source cannot acquire a separate cash override',()=>{
  for(const sourceId of ['EX-FEE/PAY-001','EX-FEE']){
    const f=fixture(),aggregate={...f.movement,id:'BATCH-FEE',sourceIds:[sourceId]};f.store.rawMovements=[aggregate];const before=clone(f.store);
    assert.throws(()=>authority.plan(f.store,f.draft,f.bank,aggregate,f.options),/Tenant fee/);assert.deepEqual(f.store,before);
  }
});
test('a resolved movement fee marker blocks fallback even when its source adapter is unavailable',()=>{
  const f=fixture();f.store.expenses={};f.movement.rentalFeeId='FEE-ID';const before=clone(f.store);
  assert.throws(()=>authority.plan(f.store,f.draft,f.bank,f.movement,f.options),/Tenant fee/);assert.deepEqual(f.store,before);
});
for(const kind of ['payment','receipt'])test('validation rebuild rejects a pre-existing cash plan after '+kind+' is recognized as a tenant fee',()=>{
  const f=fixture(kind,'none');
  // Force the pre-fee source through the universal fallback; the candidate is
  // immutable and its normalized movement fingerprint intentionally excludes
  // source-only markers, so validation must consult the actual source again.
  if(kind==='payment')f.store.expenses['EX-FEE'].isInstallment=true;else f.store.receipts[0].creditCardRefund=true;
  f.options.isProtected=()=>true;
  const plan=authority.plan(f.store,f.draft,f.bank,f.movement,f.options);assert.equal(plan.version,2);
  if(kind==='payment')f.store.expenses['EX-FEE'].rentalFeeId='FEE-ID';else f.store.receipts[0].rentalFeeId='FEE-ID';
  const before=clone(f.store);assert.throws(()=>authority.validate(f.store,plan,f.options),/Tenant fee/);assert.throws(()=>authority.applyAll(f.store,[plan],f.options),/Tenant fee/);assert.deepEqual(f.store,before);
});
test('ordinary non-fee expense correction still uses its invoice/payment adapter',()=>{
  const f=fixture('payment','none'),plan=authority.plan(f.store,f.draft,f.bank,f.movement,f.options);assert.equal(plan.version,1);
  authority.applyAll(f.store,[plan],f.options);assert.equal(f.store.expenses['EX-FEE'].amount,100.5);assert.equal(f.store.bankMovementAuthority,undefined);
});
