'use strict';

const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');

const isolated=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-incentive-store-credit-'));
process.env.DATA_PATH=path.join(isolated,'data.json');
process.env.ORDERS_PATH=path.join(isolated,'orders.json');
process.env.INCENTIVES_PATH=path.join(isolated,'incentives.json');
const {buildRecords,calculate,buildView,ledgerView,createRouter,assertStoreCreditCorrectionSafe}=require('../modules/incentives');
const corrections=require('../modules/sale-store-credit-corrections');
const copy=value=>JSON.parse(JSON.stringify(value));
const day='2026-10-22';
const metadata={reason:'Owner confirmed this is a replacement against the returned order.',originalOrderNumber:'2902',walletStatus:'pending',stale:false};
function state(patch={}){return{revision:0,reviews:{},approvals:{},payments:[],audit:[],...patch};}
function sale(id,amount,date=day,note='123456\nShivam'){
  return{id:String(id),name:'#'+id,orderNumber:String(id),channel:'POS',note,total:amount,customer:{name:'Synthetic customer'},createdAt:date+'T10:00:00+05:30',processedAt:date+'T10:00:00+05:30',paymentTransactions:[{id:'tx-'+id,kind:'sale',status:'success',gateway:'Cash',amount,processedAt:date+'T10:00:00+05:30'}]};
}
// A consumer fixture with the exact pure-projector contract. Integration with
// the shared projector and persisted source fingerprints is checked below.
function projected(order,patch={}){
  const result=copy(order),correction={...metadata,...patch};result.appStoreCreditCorrection=correction;
  result.paymentTransactions[0]={...result.paymentTransactions[0],gateway:'store_credit',appStoreCreditCorrection:copy(correction)};
  return result;
}
function approval(orders,current=state(),date=day,salesperson='Shivam'){
  const value=calculate(orders,current).daily.find(row=>row.date===date&&row.salesperson===salesperson);assert.ok(value,'day exists');
  return{id:value.key,date,salesperson,eligibleAmount:value.eligibleAmount,incentiveBaseAmount:value.incentiveBaseAmount,carriedQualifiedAmount:value.carriedQualifiedAmount,thresholdMet:value.thresholdMet,qualificationSources:copy(value.qualificationSources),incentive:value.incentive,approvedAt:'2026-10-25T10:00:00Z',approvedBy:'owner'};
}
function invoke(router,method,url,body={},params={},query={}){
  const layer=router.stack.find(row=>row.route&&row.route.path===url&&row.route.methods[method]);assert.ok(layer,url);
  let status=200,payload;layer.route.stack[0].handle({body,params,query,user:{username:'owner',roles:['owner']}},{status(value){status=value;return this;},json(value){payload=value;return this;}},error=>{throw error;});
  return{status,payload};
}
test.after(()=>fs.rmSync(isolated,{recursive:true,force:true}));

test('replacement is a retained store-credit receipt, not cash or a second incentive',()=>{
  const row=buildRecords([projected(sale(2903,2499))],state())[0];
  assert.equal(row.totalReceived,2499);assert.equal(row.cashAmount,0);assert.equal(row.storeCreditAmount,2499);assert.equal(row.eligibleAmount,0);assert.equal(row.reviewStatus,'excluded');
  assert.match(row.exclusionReason,/#2902/);assert.match(row.exclusionReason,/no new cash/);assert.match(row.exclusionReason,/wallet debit pending/);assert.equal(row.appStoreCreditCorrection.walletStatus,'pending');
  const result=calculate([projected(sale(2903,2499))],state());assert.deepEqual(result.daily,[]);assert.deepEqual(result.records[0].shares,[]);
});

test('canonical correction defeats previously saved cash and card tender classifications',()=>{
  for(const mode of ['cash','upi_card']){
    const current=state({reviews:{'2903':{salespersons:['Shivam'],gatewayModes:{'tx-2903':mode}}}}),row=buildRecords([projected(sale(2903,2499))],current)[0];
    assert.equal(row.transactions[0].effectiveMode,'store_credit');assert.equal(row.cashAmount,0);assert.equal(row.upiCardAmount,0);assert.equal(row.eligibleAmount,0);assert.equal(row.reviewStatus,'excluded');
  }
});

test('re-review cannot turn a corrected store-credit replacement into cash',()=>{
  let current=state();const orders=[projected(sale(2903,2499))],router=createRouter({loadOrders:()=>orders,loadState:()=>current,saveState:value=>{current=value;}});
  const result=invoke(router,'post','/api/incentives/reviews/:orderId',{salespersons:['Shivam'],gatewayModes:{'tx-2903':'cash'},reason:'Old tender review'},{orderId:'2903'});
  assert.equal(result.status,200);assert.equal(result.payload.records[0].cashAmount,0);assert.equal(result.payload.records[0].storeCreditAmount,2499);assert.equal(result.payload.summary.incentiveEarned,0);
});

test('only marked receipt is reclassified; genuine additional tenders retain their modes and dates',()=>{
  const row=projected(sale(2903,2499));row.total=14499;row.paymentTransactions.push({id:'genuine-card',kind:'sale',status:'success',gateway:'Card machine',amount:12000,processedAt:day+'T11:00:00+05:30'});
  const result=calculate([row],state()),record=result.records[0];
  assert.equal(record.storeCreditAmount,2499);assert.equal(record.upiCardAmount,12000);assert.equal(record.cashAmount,0);assert.equal(result.daily[0].eligibleAmount,12000);assert.equal(result.daily[0].incentive,240);
});

test('pending daily threshold excludes replacement value without creating an approval',()=>{
  const before=[sale(2903,2499),sale(3001,8000)],after=[projected(before[0]),before[1]],current=state(),saved=JSON.stringify(current);
  assert.equal(calculate(before,current).daily[0].incentive,209.98);assert.equal(assertStoreCreditCorrectionSafe(before,after,current),true);
  const result=buildView(after,current);assert.equal(result.days[0].eligibleAmount,8000);assert.equal(result.days[0].thresholdMet,false);assert.equal(result.summary.incentiveEarned,0);assert.equal(result.summary.storeCreditExcluded,2499);assert.equal(JSON.stringify(current),saved);
});

test('genuine original receipt, returns and later qualified collections are unchanged',()=>{
  const original=sale(2902,12000,'2026-10-20');original.refundAmount=12000;original.cancelledAt='2026-10-21';
  original.paymentTransactions.push({id:'later-original',kind:'capture',status:'success',gateway:'UPI',amount:500,processedAt:'2026-10-23T11:00:00+05:30'});
  const replacement=sale(2903,2499),before=[original,replacement],after=[original,projected(replacement)],current=state();
  assert.deepEqual(calculate(after,current).records.filter(row=>row.orderId==='2902'),calculate(before,current).records.filter(row=>row.orderId==='2902'));
  const originalDays=calculate(after,current).daily.filter(row=>row.date!==day);assert.equal(originalDays[0].incentive,240);assert.equal(originalDays[1].incentive,10);assert.equal(originalDays[1].carriedQualifiedAmount,500);
  assert.equal(original.paymentTransactions[0].gateway,'Cash');assert.equal(original.refundAmount,12000);
});

test('approved original incentive and actual payment are preserved when replacement is still pending',()=>{
  const original=sale(2902,12000,'2026-10-20'),replacement=sale(2903,2499),before=[original,replacement],after=[original,projected(replacement)];
  const earned=approval(before,state(),'2026-10-20'),current=state({approvals:{[earned.id]:earned},payments:[{id:'INCP-SYNTHETIC',salesperson:'Shivam',amount:240,date:'2026-10-21',account:'Counter Cash',active:true,proofs:['/synthetic-proof.jpg']}]}),saved=JSON.stringify(current),ledgerBefore=ledgerView(current);
  assert.equal(assertStoreCreditCorrectionSafe(before,after,current),true);assert.deepEqual(ledgerView(current),ledgerBefore);assert.equal(JSON.stringify(current),saved);
});

test('changed approved day blocks rather than reversing approved earnings',()=>{
  const before=[sale(2903,2499),sale(3001,10000)],after=[projected(before[0]),before[1]],earned=approval(before),current=state({approvals:{[earned.id]:earned}}),saved=JSON.stringify(current);
  assert.throws(()=>assertStoreCreditCorrectionSafe(before,after,current),error=>error.status===409&&/approved or paid/.test(error.message));assert.equal(JSON.stringify(current),saved);assert.equal(ledgerView(current)[0].earned,249.98);
});

test('paid day blocks and payment proof, amount and ledger are never changed',()=>{
  const before=[sale(2903,2499),sale(3001,10000)],after=[projected(before[0]),before[1]],earned=approval(before),current=state({approvals:{[earned.id]:earned},payments:[{id:'INCP-SYNTHETIC',salesperson:'Shivam',amount:249.98,date:'2026-10-23',account:'Counter Cash',active:true,proofs:['/synthetic-proof.jpg']}]}),saved=JSON.stringify(current),ledgerBefore=ledgerView(current);
  assert.throws(()=>assertStoreCreditCorrectionSafe(before,after,current),{status:409});assert.equal(JSON.stringify(current),saved);assert.deepEqual(ledgerView(current),ledgerBefore);
});

test('later approved dependent collection blocks even when source day was never approved',()=>{
  const replacement=sale(2903,2499),other=sale(3001,9000);other.paymentTransactions.push({id:'later-other',kind:'capture',status:'success',gateway:'Cash',amount:1000,processedAt:'2026-10-23T11:00:00+05:30'});
  const before=[replacement,other],after=[projected(replacement),other],earned=approval(before,state(),'2026-10-23'),current=state({approvals:{[earned.id]:earned}}),saved=JSON.stringify(current);
  assert.equal(earned.incentive,20);assert.equal(calculate(after,current).daily.find(row=>row.date==='2026-10-23').incentive,0);
  assert.throws(()=>assertStoreCreditCorrectionSafe(before,after,current),error=>error.status===409&&/2026-10-23/.test(error.message));assert.equal(JSON.stringify(current),saved);
});

test('approval revision and posted adjustment keys stay protected even without a base approval',()=>{
  const before=[sale(2903,2499),sale(3001,10000)],after=[projected(before[0]),before[1]],key=day+'|Shivam';
  for(const patch of [{approvalRevisions:{[key]:{eligibleAmount:12499}}},{exclusionAdjustments:[{id:'INCX-SYNTHETIC',approvalKey:key,amount:-49.98}]}]){
    const current=state(patch),saved=JSON.stringify(current);assert.throws(()=>assertStoreCreditCorrectionSafe(before,after,current),{status:409});assert.equal(JSON.stringify(current),saved);
  }
});

test('unchanged canonical projection is idempotent with approved incentives',()=>{
  const orders=[projected(sale(2903,2499)),sale(3001,12000)],earned=approval(orders),current=state({approvals:{[earned.id]:earned}}),saved=JSON.stringify(current);
  assert.equal(assertStoreCreditCorrectionSafe(orders,copy(orders),current),true);assert.equal(JSON.stringify(current),saved);
});

test('stale correction remains blocked instead of silently counting as cash',()=>{
  const orders=[projected(sale(2903,2499),{stale:true})],current=state({reviews:{'2903':{salespersons:['Shivam'],gatewayModes:{'tx-2903':'cash'}}}}),view=buildView(orders,current);
  assert.equal(view.records[0].reviewStatus,'needs_review');assert.equal(view.records[0].cashAmount,0);assert.equal(view.records[0].storeCreditAmount,2499);assert.equal(view.records[0].eligibleAmount,0);assert.match(view.records[0].reviewIssues.join(' '),/evidence changed/);assert.equal(view.summary.incentiveEarned,0);
});

test('stale evidence cannot be hidden with a no-salesperson or former-employee review',()=>{
  for(const current of [state({reviews:{'2903':{noSalesperson:true,salespersons:[]}}}),state({reviews:{'2903':{salespersons:['Isha']}}})]){
    assert.equal(buildRecords([projected(sale(2903,2499),{stale:true})],current)[0].reviewStatus,'needs_review');
  }
});

test('calculations and guard do not mutate order evidence, correction metadata or incentive state',()=>{
  const before=[sale(2903,2499)],after=[projected(before[0])],current=state(),rawBefore=JSON.stringify(before),rawAfter=JSON.stringify(after),rawState=JSON.stringify(current);
  buildView(after,current);assertStoreCreditCorrectionSafe(before,after,current);assert.equal(JSON.stringify(before),rawBefore);assert.equal(JSON.stringify(after),rawAfter);assert.equal(JSON.stringify(current),rawState);
});

test('shared pure projector and default loader consume the same retained source correction without writes',()=>{
  const original=sale(2902,2499,'2026-10-21'),replacement=sale(2903,2499);original.customer.id='customer-synthetic';replacement.customer.id='customer-synthetic';
  original.storeCreditIssued=2499;original.refundAmount=2499;original.refundTransactions=[{id:'credit-synthetic',kind:'refund',status:'success',gateway:'store_credit',amount:2499}];
  const orders=[original,replacement],record=corrections.prepare(orders,{}, {saleId:'SHOPIFY/2903',originalOrderNumber:'2902',storeCreditAmount:2499,reason:'Owner confirmed replacement credit.'}),accounting={saleStoreCreditCorrections:{'SHOPIFY/2903':record}};
  const effective=orders.map(order=>corrections.projectOrder(order,accounting,orders)),direct=buildView(effective,state()),row=direct.records.find(item=>item.orderId==='2903');
  assert.equal(row.cashAmount,0);assert.equal(row.storeCreditAmount,2499);assert.equal(row.reviewStatus,'excluded');assert.equal(row.appStoreCreditCorrection.walletStatus,'pending_external_user_action');assert.match(row.exclusionReason,/wallet debit pending/);
  assert.equal(orders[1].paymentTransactions[0].gateway,'Cash');assert.equal(buildRecords(effective,state()).find(item=>item.orderId==='2902').eligibleAmount,2499);
  const expenseFile=path.join(isolated,'expenses.json'),rawAccounting=JSON.stringify(accounting),rawOrders=JSON.stringify({orders:Object.fromEntries(orders.map(order=>[order.id,order]))});
  fs.writeFileSync(expenseFile,rawAccounting);fs.writeFileSync(process.env.ORDERS_PATH,rawOrders);
  const router=createRouter({loadState:()=>state()});
  for(let index=0;index<2;index++){
    const result=invoke(router,'get','/api/incentives');assert.equal(result.status,200);assert.deepEqual(result.payload.records,direct.records);assert.equal(fs.readFileSync(expenseFile,'utf8'),rawAccounting);assert.equal(fs.readFileSync(process.env.ORDERS_PATH,'utf8'),rawOrders);
  }
  original.refundAmount=2500;const changedOrders=JSON.stringify({orders:Object.fromEntries(orders.map(order=>[order.id,order]))});fs.writeFileSync(process.env.ORDERS_PATH,changedOrders);
  const changed=invoke(router,'get','/api/incentives').payload.records.find(item=>item.orderId==='2903');assert.equal(changed.reviewStatus,'needs_review');assert.equal(changed.cashAmount,0);assert.equal(changed.appStoreCreditCorrection.originalFingerprintStale,true);assert.equal(fs.readFileSync(expenseFile,'utf8'),rawAccounting);assert.equal(fs.readFileSync(process.env.ORDERS_PATH,'utf8'),changedOrders);
});

test('default loader refuses unreadable correction evidence without migrating or writing stores',()=>{
  fs.writeFileSync(process.env.ORDERS_PATH,JSON.stringify({orders:{2903:sale(2903,2499)}}));const expenseFile=path.join(isolated,'expenses.json');
  for(const contents of ['{ malformed JSON','[]','null']){
    fs.writeFileSync(expenseFile,contents);const result=invoke(createRouter(),'get','/api/incentives');assert.equal(result.status,409);assert.match(result.payload.error,/corrections cannot be read safely/);assert.equal(fs.readFileSync(expenseFile,'utf8'),contents);
  }
  fs.writeFileSync(expenseFile,'{}');fs.writeFileSync(path.join(isolated,'.bank-authority-transaction.json'),'retained pending source journal');
  const result=invoke(createRouter(),'get','/api/incentives');assert.equal(result.status,409);assert.equal(fs.readFileSync(expenseFile,'utf8'),'{}');assert.equal(fs.readFileSync(path.join(isolated,'.bank-authority-transaction.json'),'utf8'),'retained pending source journal');
});
