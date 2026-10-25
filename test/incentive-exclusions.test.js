'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {calculate,buildView,createRouter}=require('../modules/incentives');
const copy=value=>JSON.parse(JSON.stringify(value));
const date='2026-10-01';
function sale(id,amount,note='Krishna',day=date){return{id:String(id),name:'#'+id,channel:'POS',note,total:amount,createdAt:day+'T10:00:00+05:30',paymentTransactions:[{id:'tx-'+id,amount,gateway:'Cash',kind:'sale',status:'success',processedAt:day+'T12:00:00+05:30'}]};}
function setup(orders){
  let state={revision:0,reviews:{},approvals:{},payments:[],audit:[]},saves=0;
  const router=createRouter({loadOrders:()=>orders,loadState:()=>state,saveState:value=>{state=value;saves++;}});
  function call(path,body={},params={}){
    const layer=router.stack.find(row=>row.route&&row.route.path===path&&row.route.methods.post);assert.ok(layer,path);
    let status=200,payload;
    layer.route.stack[0].handle({body,params,user:{username:'tester',roles:['owner']}},{status(value){status=value;return this;},json(value){payload=value;}},error=>{throw error;});
    return{status,payload};
  }
  const approve=(day=date,person='Krishnakant')=>call('/api/incentives/approve-day',{date:day,salesperson:person});
  const preview=(id,excluded=true)=>call('/api/incentives/exclusions/:orderId/preview',{excluded},{orderId:String(id)});
  function commit(id,excluded=true,patch={}){const plan=preview(id,excluded);assert.equal(plan.status,200);return call('/api/incentives/exclusions/:orderId',{excluded,reason:'Owner reviewed order',previewToken:plan.payload.previewToken,...patch},{orderId:String(id)});}
  return{orders,router,call,approve,preview,commit,get state(){return state;},get saves(){return saves;},view:filters=>buildView(orders,state,filters),ledger:person=>buildView(orders,state).ledgers.find(row=>row.salesperson===(person||'Krishnakant'))};
}

test('preview is read-only and predicts whole-day threshold loss after final approval',()=>{
  const app=setup([sale(1,6000),sale(2,5000)]);assert.equal(app.approve().status,200);
  const before=copy(app.state),saves=app.saves,orders=copy(app.orders),plan=app.preview(1).payload;
  assert.equal(plan.totalAdjustment,-220);assert.equal(plan.affectedDays[0].eligible,5000);assert.equal(plan.affectedDays[0].approvedAfter,0);
  assert.equal(plan.balances[0].balance,0);assert.deepEqual(app.state,before);assert.deepEqual(app.orders,orders);assert.equal(app.saves,saves);
  assert.equal(app.commit(1).status,200);assert.equal(app.ledger().earned,0);assert.equal(app.ledger().payableBalance,0);
  assert.deepEqual(app.state.approvals,before.approvals,'original approval is not overwritten');assert.equal(app.state.exclusionAdjustments[0].amount,-220);
  assert.equal(app.state.audit.at(-1).action,'INCENTIVE_ORDER_EXCLUDED');assert.deepEqual(app.orders,orders);
});

test('restoration after finalization recovers the approved amount and repeated requests are idempotent',()=>{
  const app=setup([sale(1,6000),sale(2,5000)]);app.approve();app.commit(1);
  const saves=app.saves;app.commit(1);assert.equal(app.saves,saves);assert.equal(app.state.exclusionAdjustments.length,1);
  assert.equal(app.preview(1,false).payload.totalAdjustment,220);app.commit(1,false);
  assert.equal(app.ledger().earned,220);assert.equal(app.state.exclusionAdjustments.length,2);
  assert.equal(app.view().days[0].approvalStatus,'approved');assert.equal(app.state.audit.at(-1).action,'INCENTIVE_ORDER_RESTORED');
  app.commit(1,false);assert.equal(app.state.exclusionAdjustments.length,2);
});

test('remaining qualified amount, period summary and salesperson filter all agree',()=>{
  const app=setup([sale(1,5000),sale(2,25000)]);app.approve();app.commit(1);
  const view=app.view({from:date,to:date,salesperson:'Krishnakant'});
  assert.equal(app.ledger().earned,500);assert.equal(view.summary.approvedIncentive,500);assert.equal(view.summary.incentiveEarned,500);
  assert.equal(view.days[0].approvalStatus,'approved');assert.equal(view.records.length,2);
  const excluded=view.records.find(row=>row.orderId==='1');assert.equal(excluded.eligibleAmount,0);assert.equal(excluded.reviewStatus,'excluded');assert.equal(excluded.manuallyExcluded,true);
  assert.match(excluded.exclusionReason,/Owner reviewed order/);assert.equal(excluded.manualExclusion.by,'tester');
});

test('unapproved earnings recalculate without silently creating an approval',()=>{
  const app=setup([sale(1,20000),sale(2,15000)]);app.commit(1);
  assert.equal(app.view().days[0].incentive,300);assert.equal(app.view().days[0].approvalStatus,'pending');assert.equal(app.ledger().earned,0);
  assert.deepEqual(app.state.approvals,{});assert.equal((app.state.exclusionAdjustments||[]).length,0);
});

test('shared order exclusions recalculate both active salespeople but never reassign former shares',()=>{
  const app=setup([sale(1,36000,'Shivam + Krishna + Isha')]);app.approve(date,'Shivam');app.approve();
  const plan=app.preview(1).payload;assert.equal(plan.totalAdjustment,-480);
  app.commit(1);assert.equal(app.ledger().earned,0);assert.equal(app.ledger('Shivam').earned,0);assert.equal(app.ledger('Isha').earned,0);
  app.commit(1,false);assert.equal(app.ledger().earned,240);assert.equal(app.ledger('Shivam').earned,240);assert.equal(app.ledger('Isha').earned,0);
});

test('loss of original qualification also reverses dependent later receipts',()=>{
  const partial=sale(2,2000);partial.paymentTransactions.push({...partial.paymentTransactions[0],id:'later',amount:1000,processedAt:'2026-10-02T12:00:00+05:30'});
  const app=setup([sale(1,9000),partial]);app.approve();app.approve('2026-10-02');
  assert.equal(app.ledger().earned,240);const plan=app.preview(1).payload;
  assert.equal(plan.affectedDays.length,2);assert.equal(plan.totalAdjustment,-240);
  app.commit(1);assert.equal(app.ledger().earned,0);app.commit(1,false);assert.equal(app.ledger().earned,240);
});

test('paid incentives become visible recoverable balances without altering payment records',()=>{
  const app=setup([sale(1,6000),sale(2,5000)]);app.approve();
  app.state.payments.push({id:'INCP-1',salesperson:'Krishnakant',amount:100,date,account:'Counter Cash',proofs:['/uploads/proof.png'],active:true});
  const payments=copy(app.state.payments),plan=app.preview(1).payload;
  assert.equal(plan.balances[0].overpaid,100);app.commit(1);
  assert.equal(app.ledger().balance,-100);assert.equal(app.ledger().payableBalance,0);assert.equal(app.ledger().overpaidAmount,100);assert.deepEqual(app.state.payments,payments);
  const result=app.call('/api/incentives/payments',{salesperson:'Krishnakant',amount:1,date,account:'Counter Cash',proofs:['/uploads/new.png']});
  assert.equal(result.status,409);assert.deepEqual(app.state.payments,payments);
});

test('stale previews reject changed orders, approvals and payments without mutating state',()=>{
  for(const change of [app=>app.orders[1].paymentTransactions[0].amount=7000,app=>app.state.revision++,app=>app.state.payments.push({id:'new',amount:1,salesperson:'Krishnakant',date})]){
    const app=setup([sale(1,6000),sale(2,5000)]);app.approve();const previewToken=app.preview(1).payload.previewToken;change(app);
    const before=copy(app.state),saves=app.saves,result=app.call('/api/incentives/exclusions/:orderId',{excluded:true,reason:'Reviewed',previewToken},{orderId:'1'});
    assert.equal(result.status,409);assert.match(result.payload.error,/Refresh/);assert.deepEqual(app.state,before);assert.equal(app.saves,saves);
  }
});

test('reason, explicit boolean, known POS order and access checks are required',()=>{
  const app=setup([sale(1,11000),{...sale(2,11000),channel:'Online'}]);
  assert.equal(app.preview(99).status,404);assert.equal(app.preview(2).status,400);assert.equal(app.preview(1,'true').status,400);
  assert.equal(app.commit(1,true,{reason:'   '}).status,400);assert.equal(app.commit(1,true,{previewToken:''}).status,409);assert.equal(app.saves,0);
  let status;app.router.stack[0].handle({user:{roles:['staff']}},{status(code){status=code;return this;},json(){}},()=>assert.fail('Unauthorized request passed'));
  assert.equal(status,403);
});

test('later reapproval does not double deduct previous exclusions or lose adjustment history',()=>{
  const app=setup([sale(1,5000),sale(2,25000)]);app.approve();app.commit(1);assert.equal(app.ledger().earned,500);
  app.orders.push(sale(3,10000));assert.equal(app.view().days[0].approvalStatus,'needs_reapproval');app.approve();
  assert.equal(app.ledger().earned,700);assert.equal(app.state.exclusionAdjustments.length,2);assert.equal(app.state.exclusionAdjustments[0].amount,-100);assert.equal(app.state.exclusionAdjustments[1].amount,100);
  app.commit(1,false);assert.equal(app.ledger().earned,700,'restoring cannot approve the additional earnings beyond the last explicit approval');
  assert.equal(app.view().days[0].approvalStatus,'needs_reapproval');app.approve();assert.equal(app.ledger().earned,800);
});

test('manual exclusion survives sync, re-review and serialization without modifying order notes',()=>{
  const app=setup([sale(1,11000)]);app.approve();app.commit(1);const original=copy(app.orders);
  app.call('/api/incentives/reviews/:orderId',{salespersons:['Shivam'],gatewayModes:{},noSalesperson:false,reason:'Different name'},{orderId:'1'});
  app.orders[0].note='123456\nShivam';const reloaded=buildView(app.orders,copy(app.state));
  assert.equal(reloaded.records[0].manuallyExcluded,true);assert.equal(reloaded.records[0].eligibleAmount,0);assert.equal(app.ledger().earned,0);
  assert.equal(original[0].note,'Krishna','exclusion itself does not edit Shopify notes');
});

test('restoring a manual flag does not bypass automatic customer or store-credit exclusions',()=>{
  const row=sale(1,11000);row.customer={name:'Gagan sir'};
  const app=setup([row]);app.commit(1);app.commit(1,false);
  assert.equal(app.view().records[0].reviewStatus,'excluded');assert.equal(app.ledger().earned,0);
});

test('explicit exclusion repairs stale approvals left by the old no-salesperson review workflow',()=>{
  const partial=sale(2,2000);partial.paymentTransactions.push({...partial.paymentTransactions[0],id:'later',amount:1000,processedAt:'2026-10-02T12:00:00+05:30'});
  const app=setup([sale(1,9000),partial]);app.approve();app.approve('2026-10-02');
  app.call('/api/incentives/reviews/:orderId',{salespersons:[],noSalesperson:true},{orderId:'1'});
  assert.equal(app.ledger().earned,240,'old review route did not reverse approvals');
  const plan=app.preview(1).payload;assert.equal(plan.totalAdjustment,-240);assert.equal(plan.affectedDays.length,2);
  app.commit(1);assert.equal(app.ledger().earned,0);
});

test('restoration does not reapprove earnings when the source day now has unresolved tenders',()=>{
  const app=setup([sale(1,15000)]);app.approve();app.commit(1);
  const unknown=sale(2,1000);unknown.paymentTransactions[0].gateway='Unknown';app.orders.push(unknown);
  assert.equal(app.preview(1,false).payload.totalAdjustment,0);app.commit(1,false);
  assert.equal(app.ledger().earned,0);assert.equal(app.view().days[0].approvalStatus,'needs_reapproval');
  assert.equal(app.approve().status,409);
});

test('several exclusions and restorations on the same approved day do not overwrite each other',()=>{
  const app=setup([sale(1,5000),sale(2,10000),sale(3,15000)]);app.approve();
  app.commit(1);assert.equal(app.ledger().earned,500);app.commit(2);assert.equal(app.ledger().earned,300);
  app.commit(1,false);assert.equal(app.ledger().earned,400);assert.equal(app.view().records.find(row=>row.orderId==='2').manuallyExcluded,true);
  app.commit(2,false);assert.equal(app.ledger().earned,600);assert.equal(app.view().days[0].approvalStatus,'approved');
});

test('recalculations round each day to cents and retain the inclusive 10000 threshold',()=>{
  const app=setup([sale(1,4999.99),sale(2,10000)]);app.approve();assert.equal(app.ledger().earned,300);
  app.commit(1);assert.equal(app.ledger().earned,200);assert.equal(app.view().days[0].approvalStatus,'approved');
  app.commit(1,false);assert.equal(app.ledger().earned,300);
});
