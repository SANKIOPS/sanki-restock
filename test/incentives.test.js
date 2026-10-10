const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {parseSalespeople,classifyGateway,calculate,buildView,ledgerView,createRouter}=require('../modules/incentives');

function order(id,{note='123456\nShivam',total=0,refundAmount=0,transactions=[],channel='POS',cancelledAt=null}={}){
  return {id:String(id),name:'#'+id,channel,note,total,refundAmount,cancelledAt,customer:{name:'Customer '+id},createdAt:'2026-10-01T10:00:00+05:30',processedAt:'2026-10-01T10:00:00+05:30',paymentTransactions:transactions};
}
function tx(id,amount,gateway,date='2026-10-01'){return{id:String(id),amount,gateway,kind:'sale',status:'success',processedAt:date+'T12:00:00+05:30'};}
function state(patch={}){return Object.assign({reviews:{},approvals:{},payments:[]},patch);}
function invoke(router,method,routePath,body,params={}){
  const layer=router.stack.find(item=>item.route&&item.route.path===routePath&&item.route.methods[method]);assert.ok(layer,'route '+method+' '+routePath);
  let payload,status=200;const req={body:body||{},params,query:{},user:{username:'owner',roles:['owner']}},res={status(code){status=code;return this;},json(value){payload=value;return this;}};
  layer.route.stack[0].handle(req,res,error=>{throw error;});return{status,payload};
}

test('order-note parser reads six-digit reference and normalized salesperson names',()=>{
  assert.deepEqual(parseSalespeople('654321\nSHIVAM').salespersons,['Shivam']);
  assert.deepEqual(parseSalespeople('654321 Krishna Kant').salespersons,['Krishnakant']);
  assert.deepEqual(parseSalespeople('654321\nShIvAm + KRISHNA').salespersons,['Shivam','Krishnakant']);
  assert.equal(parseSalespeople('654321\nShivam').reference,'654321');
});

test('spelling mistakes are suggested but never assigned automatically',()=>{
  const result=parseSalespeople('654321\nShivm');
  assert.deepEqual(result.salespersons,[]);
  assert.equal(result.confidence,'suggested');
  assert.equal(result.suggestions[0].name,'Shivam');
});

test('gateway classification includes only cash, UPI and card-machine receipts',()=>{
  assert.equal(classifyGateway('Cash'),'cash');
  assert.equal(classifyGateway('Shopify Payments'),'upi_card');
  assert.equal(classifyGateway('UPI - Pine Labs Card Machine'),'upi_card');
  assert.equal(classifyGateway('shopify_store_credit'),'store_credit');
  assert.equal(classifyGateway('Cash on Delivery (COD)'),'excluded');
  assert.equal(classifyGateway('Unmapped manual gateway'),'unknown');
});

test('crossing ₹10,000 applies two percent to the whole daily eligible amount',()=>{
  const rows=[order(1,{total:6000,transactions:[tx(1,6000,'Cash')]}),order(2,{total:5000,transactions:[tx(2,5000,'Card Machine')]})];
  const result=calculate(rows,state()),day=result.daily.find(x=>x.salesperson==='Shivam');
  assert.equal(day.eligibleAmount,11000);
  assert.equal(day.qualifies,true);
  assert.equal(day.incentive,220);
  assert.deepEqual(result.records.map(x=>x.shares[0].incentive),[120,100]);
});

test('a shared sale divides eligible value and incentive equally',()=>{
  const rows=[order(3,{note:'778899\nShivam + Krishna',total:20000,transactions:[tx(3,20000,'UPI')]})],result=calculate(rows,state());
  assert.deepEqual(result.daily.map(x=>[x.salesperson,x.eligibleAmount,x.incentive]),[['Krishnakant',10000,200],['Shivam',10000,200]]);
  assert.deepEqual(result.records[0].shares.map(x=>[x.salesperson,x.eligibleAmount,x.incentive]),[['Shivam',10000,200],['Krishnakant',10000,200]]);
});

test('store credit is excluded while refunds remain informational',()=>{
  const rows=[order(4,{total:12000,refundAmount:5000,cancelledAt:'2026-10-04T10:00:00+05:30',transactions:[tx(4,10000,'Cash'),tx(5,2000,'Store Credit')]})],view=buildView(rows,state(),{from:'2026-10-01',to:'2026-10-01'}),record=view.records[0];
  assert.equal(record.totalReceived,12000);
  assert.equal(record.storeCreditAmount,2000);
  assert.equal(record.eligibleAmount,10000);
  assert.equal(record.refundAmount,5000);
  assert.equal(view.summary.incentiveEarned,200);
});

test('receipt date, not order date, controls separate daily thresholds',()=>{
  const rows=[order(5,{total:12000,transactions:[tx(6,6000,'Cash','2026-10-02'),tx(7,6000,'UPI','2026-10-03')]})],result=calculate(rows,state());
  assert.deepEqual(result.records.map(x=>x.receiptDate),['2026-10-02','2026-10-03']);
  assert.deepEqual(result.daily.map(x=>[x.date,x.eligibleAmount,x.qualifies]),[['2026-10-02',6000,false],['2026-10-03',6000,false]]);
});

test('unknown gateways require review and a saved classification resolves them',()=>{
  const rows=[order(6,{total:11000,transactions:[tx(8,11000,'Manual POS Tender')]})];
  assert.equal(calculate(rows,state()).records[0].reviewStatus,'needs_review');
  const reviewed=state({reviews:{'6':{salespersons:['Shivam'],noSalesperson:false,gatewayModes:{'8':'upi_card'},reviewedAt:'2026-10-01',reviewedBy:'owner'}}});
  const result=calculate(rows,reviewed);
  assert.equal(result.records[0].reviewStatus,'ready');
  assert.equal(result.daily[0].incentive,220);
});

test('approved earnings and proof-backed payments stay in a separate ledger',()=>{
  const ledger=ledgerView(state({approvals:{'2026-10-01|Shivam':{id:'2026-10-01|Shivam',date:'2026-10-01',salesperson:'Shivam',eligibleAmount:12000,incentive:240,approvedAt:'2026-10-02T01:00:00Z',approvedBy:'owner'}},payments:[{id:'INCP-00001',date:'2026-10-03',salesperson:'Shivam',amount:100,account:'Counter Cash',proofs:['/uploads/proof.jpg'],active:true,createdAt:'2026-10-03T01:00:00Z',createdBy:'owner'}]})).find(x=>x.salesperson==='Shivam');
  assert.equal(ledger.earned,240);
  assert.equal(ledger.paid,100);
  assert.equal(ledger.balance,140);
  assert.deepEqual(ledger.entries.map(x=>x.balance),[240,140]);
});

test('day approval and partial payment routes preserve the approved liability',()=>{
  const orders=[order(7,{total:12000,transactions:[tx(9,12000,'Cash')]})];let current=state({revision:0,paymentSeq:0,audit:[]});
  const router=createRouter({loadOrders:()=>orders,loadState:()=>current,saveState:value=>{current=value;}});
  const approved=invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-01',salesperson:'Shivam'});
  assert.equal(approved.status,200);assert.equal(current.approvals['2026-10-01|Shivam'].incentive,240);
  const paid=invoke(router,'post','/api/incentives/payments',{salesperson:'Shivam',amount:100,date:'2026-10-02',account:'Counter Cash',reference:'UTR-1',proofs:['/api/expenses/photo/proof.jpg']});
  assert.equal(paid.status,200);assert.equal(current.payments[0].amount,100);assert.equal(ledgerView(current).find(x=>x.salesperson==='Shivam').balance,140);
});

test('incentive page includes agreed reporting and review fields',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','incentives.html'),'utf8');
  for(const label of ['Order / customer','Payment mode','Store credit','Refund / return','Exclusion reason','Shopify order note','Review status','Approved','Separate incentive ledgers'])assert.match(html,new RegExp(label));
  assert.doesNotMatch(html,/Payroll month/i);
});

test('former salesperson names are recognised case-insensitively without substring matches',()=>{
  for(const [note,names] of [['654321\nISHA',['Isha']],['654321 naNDini',['Nandini']],['654321\nIsha + NANDINI',['Isha','Nandini']],['654321\nShivam + Isha',['Shivam','Isha']],['654321\nSIMRAN',['Simran']],['654321 siMRan',['Simran']],['654321\nIsha + NANDINI + Simran',['Isha','Nandini','Simran']]]){
    const parsed=parseSalespeople(note);assert.deepEqual(parsed.salespersons,names);assert.equal(parsed.confidence,'confirmed');
  }
  assert.ok(!parseSalespeople('654321\nNisha').salespersons.includes('Isha'));
  assert.deepEqual(parseSalespeople('654321\nIshaa').salespersons,[]);
  assert.ok(!parseSalespeople('654321\nSimranjeet').salespersons.includes('Simran'));
  assert.deepEqual(parseSalespeople('654321\nSimrn').salespersons,[]);
  assert.equal(parseSalespeople('654321\nSimrn').suggestions[0].name,'Simran');
});

test('former employee receipts remain in the report but never qualify or need repeated review',()=>{
  const orders=[order(20,{note:'123456\nIsha',total:25000,transactions:[tx(20,25000,'Cash')]}),order(21,{note:'123457\nNandini',total:18000,transactions:[tx(21,18000,'UPI')]}),order(31,{note:'123458\nSimran',total:30000,transactions:[tx(31,30000,'Card Machine')]})];
  const current=state(),before=JSON.stringify(current),view=buildView(orders,current);
  assert.equal(view.summary.orders,3);assert.equal(view.summary.totalReceived,73000);assert.equal(view.summary.eligibleReceived,0);assert.equal(view.summary.recordOnlyReceipts,73000);assert.equal(view.summary.incentiveEarned,0);assert.equal(view.summary.outstanding,0);assert.equal(view.summary.needsReview,0);
  for(const row of view.records){assert.equal(row.reviewStatus,'record_only');assert.equal(row.matchConfidence,'confirmed');assert.deepEqual(row.reviewIssues,[]);assert.equal(row.eligibleAmount,0);assert.match(row.exclusionReason,/former employee/);assert.equal(row.shares[0].incentive,0);assert.equal(row.shares[0].approvalStatus,'record_only');}
  for(const day of view.days){assert.equal(day.qualifies,false);assert.equal(day.incentive,0);assert.equal(day.unresolved,false);assert.equal(day.approvalStatus,'record_only');}
  assert.deepEqual(buildView(orders,current),view);assert.equal(JSON.stringify(current),before);
});

test('record-only orders do not block active daily approval even with missing or unknown tenders',()=>{
  const orders=[order(22,{total:12000,transactions:[tx(22,12000,'Cash')]}),order(23,{note:'654321\nIsha',transactions:[tx(23,10000,'Unmapped tender')]}),order(24,{note:'Nandini'}),order(32,{note:'Simran',transactions:[tx(32,10000,'Unmapped tender')]}),order(33,{note:'Simran'})];
  let current=state({revision:0,audit:[]});const router=createRouter({loadOrders:()=>orders,loadState:()=>current,saveState:value=>{current=value;}});
  const view=buildView(orders,current);assert.equal(view.summary.needsReview,0);assert.equal(view.days.find(day=>day.salesperson==='Shivam').unresolved,false);
  assert.equal(invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-01',salesperson:'Shivam'}).status,200);assert.equal(current.approvals['2026-10-01|Shivam'].incentive,240);
});

test('shared active and former sales preserve equal shares without reallocating former incentives',()=>{
  const rows=[order(25,{note:'123456\nShivam + Isha',total:24000,transactions:[tx(25,24000,'Card Machine')]})],view=buildView(rows,state()),row=view.records[0];
  assert.equal(row.reviewStatus,'ready');assert.equal(row.eligibleTenderAmount,24000);assert.equal(row.eligibleAmount,12000);assert.equal(row.recordOnlyAmount,12000);assert.equal(view.summary.eligibleReceived,12000);assert.equal(view.summary.incentiveEarned,240);
  assert.deepEqual(row.shares.map(share=>[share.salesperson,share.receiptAmount,share.eligibleAmount,share.incentive,share.recordOnly]),[['Shivam',12000,12000,240,false],['Isha',12000,0,0,true]]);
  const below=calculate([order(26,{note:'Shivam + Nandini',transactions:[tx(26,18000,'Cash')]})],state()).daily.find(day=>day.salesperson==='Shivam');assert.equal(below.eligibleAmount,9000);assert.equal(below.qualifies,false);assert.equal(below.incentive,0);
});

test('mixed former and active orders still require genuine tender review',()=>{
  const view=buildView([order(27,{note:'Krishna + Nandini',transactions:[tx(27,24000,'Unmapped tender')]})],state());
  assert.equal(view.records[0].reviewStatus,'needs_review');assert.match(view.records[0].reviewIssues[0],/Classify/);assert.equal(view.summary.needsReview,1);assert.equal(view.summary.incentiveEarned,0);
});

test('former names are filterable and manually reviewable without becoming payable',()=>{
  let current=state({revision:0,audit:[]});const orders=[order(28,{note:'Nandni',transactions:[tx(28,22000,'Cash')]})],router=createRouter({loadOrders:()=>orders,loadState:()=>current,saveState:value=>{current=value;}});
  assert.equal(parseSalespeople('Nandni').suggestions[0].name,'Nandini');
  const reviewed=invoke(router,'post','/api/incentives/reviews/:orderId',{salespersons:['Nandini'],reason:'Confirmed former salesperson'}, {orderId:'28'});assert.equal(reviewed.status,200);assert.equal(reviewed.payload.summary.needsReview,0);assert.equal(reviewed.payload.records[0].reviewStatus,'record_only');
  const filtered=buildView(orders,current,{salesperson:'Nandini',status:'record_only'});assert.equal(filtered.records.length,1);assert.equal(filtered.summary.incentiveEarned,0);
  assert.deepEqual(filtered.configuration.salespersonDetails.filter(person=>person.recordOnly).map(person=>person.name),['Isha','Nandini','Simran']);assert.ok(filtered.configuration.salespersons.includes('Isha'));
});

test('server rejects incentive approval and payment for former staff without changing state',()=>{
  for(const salesperson of ['Isha','Nandini','Simran']){
    const id='2026-10-01|'+salesperson,current=state({revision:4,audit:[],paymentSeq:0,approvals:{[id]:{id,date:'2026-10-01',salesperson,eligibleAmount:25000,incentive:500}}}),before=JSON.stringify(current);
    let saves=0;const router=createRouter({loadOrders:()=>[order(29,{note:salesperson,transactions:[tx(29,25000,'Cash')]})],loadState:()=>current,saveState:()=>{saves++;}});
    const approved=invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-01',salesperson});assert.equal(approved.status,409);assert.match(approved.payload.error,/record only/);
    const paid=invoke(router,'post','/api/incentives/payments',{salesperson,amount:100,date:'2026-10-02',account:'Counter Cash',proofs:['/proof.jpg']});assert.equal(paid.status,409);assert.match(paid.payload.error,/not allowed/);assert.equal(saves,0);assert.equal(JSON.stringify(current),before);
    const ledger=ledgerView(current).find(item=>item.salesperson===salesperson);assert.equal(ledger.recordOnly,true);assert.equal(ledger.payableBalance,0);assert.equal(ledger.entries.length,1);assert.equal(ledger.balance,500);
    const view=buildView([],current);assert.equal(view.summary.approvedIncentive,0);assert.equal(view.summary.outstanding,0);
  }
});

test('former-only store credits and refunds remain visible without incentive liability',()=>{
  const view=buildView([order(30,{note:'Isha + Nandini + Simran',total:20000,refundAmount:1000,transactions:[tx(30,20000,'Store credit')]})],state());
  assert.equal(view.summary.storeCreditExcluded,20000);assert.equal(view.summary.refundsReturns,1000);assert.equal(view.records[0].reviewStatus,'record_only');assert.equal(view.summary.needsReview,0);assert.equal(view.summary.incentiveEarned,0);
});

test('Simran shares stay record-only without increasing the active salesperson share',()=>{
  const view=buildView([order(34,{note:'654321\nShivam + Nandini + Simran',total:30000,transactions:[tx(34,30000,'UPI')]})],state());
  assert.equal(view.summary.eligibleReceived,10000);assert.equal(view.summary.recordOnlyReceipts,20000);assert.equal(view.summary.incentiveEarned,200);
  const simran=view.records[0].shares.find(share=>share.salesperson==='Simran');assert.equal(simran.receiptAmount,10000);assert.equal(simran.eligibleAmount,0);assert.equal(simran.incentive,0);assert.equal(simran.recordOnly,true);
  const mixedUnknown=buildView([order(35,{note:'Shivam + Simran',transactions:[tx(35,24000,'Unmapped tender')]})],state());assert.equal(mixedUnknown.records[0].reviewStatus,'needs_review');assert.equal(mixedUnknown.summary.incentiveEarned,0);
});

test('a confirmed Simran spelling correction stays filterable and non-payable',()=>{
  let current=state({revision:0,audit:[]});const orders=[order(36,{note:'Simrn',transactions:[tx(36,22000,'Cash')]})],router=createRouter({loadOrders:()=>orders,loadState:()=>current,saveState:value=>{current=value;}});
  const reviewed=invoke(router,'post','/api/incentives/reviews/:orderId',{salespersons:['Simran'],reason:'Confirmed former salesperson'},{orderId:'36'});assert.equal(reviewed.status,200);
  const filtered=buildView(orders,current,{salesperson:'Simran',status:'record_only'});assert.equal(filtered.records.length,1);assert.equal(filtered.records[0].matchConfidence,'reviewed');assert.equal(filtered.summary.recordOnlyReceipts,22000);assert.equal(filtered.summary.needsReview,0);assert.equal(filtered.summary.incentiveEarned,0);assert.equal(filtered.summary.outstanding,0);
});

test('qualified order 2864 earns on its 7500 balance when received without a second threshold',()=>{
  const rows=[order(2864,{total:57500,transactions:[tx(102,7500,'UPI','2026-10-02'),tx(101,50000,'Card Machine')]})],current=state(),before=JSON.stringify(current),result=calculate(rows,current);
  assert.deepEqual(result.daily.map(day=>[day.date,day.eligibleAmount,day.thresholdMet,day.qualifies,day.incentive]),[['2026-10-01',50000,true,true,1000],['2026-10-02',7500,false,true,150]]);
  const later=result.records.find(row=>row.receiptDate==='2026-10-02').shares[0];assert.equal(later.inheritedQualification,true);assert.equal(later.qualificationDate,'2026-10-01');assert.equal(later.incentive,150);
  assert.deepEqual(result.daily[1].qualificationSources,[{orderId:'2864',orderNumber:'#2864',qualificationDate:'2026-10-01',qualifyingDailyAmount:50000,receivedAmount:7500}]);
  const laterOnly=buildView(rows,current,{from:'2026-10-02',to:'2026-10-02',salesperson:'Shivam'});assert.equal(laterOnly.summary.incentiveEarned,150);assert.equal(laterOnly.summary.totalReceived,7500);assert.equal(laterOnly.summary.totalBilling,57500);assert.equal(laterOnly.days.length,1);
  const unpaid=calculate([order(2864,{total:57500,transactions:[tx(101,50000,'Card Machine')]})],current);assert.equal(unpaid.daily.length,1);assert.equal(unpaid.daily[0].incentive,1000);
  assert.deepEqual(calculate(rows,current),result);assert.equal(JSON.stringify(current),before);
});

test('an order qualified by combined daily receipts retains qualification across several collections',()=>{
  const rows=[order(50,{total:10000,transactions:[tx(201,6000,'Cash'),tx(202,2000,'UPI','2026-10-02'),tx(203,1500,'Cash','2026-10-03'),tx(204,500,'Cash','2026-10-04')]}),order(51,{total:4000,transactions:[tx(205,4000,'Cash')]})],result=calculate(rows,state());
  assert.deepEqual(result.daily.map(day=>[day.date,day.incentive]),[['2026-10-01',200],['2026-10-02',40],['2026-10-03',30],['2026-10-04',10]]);
  for(const day of result.daily.slice(1)){assert.equal(day.thresholdMet,false);assert.equal(day.qualificationSources[0].qualificationDate,'2026-10-01');assert.equal(day.qualificationSources[0].qualifyingDailyAmount,10000);}
  assert.deepEqual(calculate(rows.slice().reverse(),state()),result);
});

test('qualification is per order and salesperson and does not exempt unrelated new sales',()=>{
  const rows=[order(52,{note:'Shivam + Krishna',total:20000,transactions:[tx(206,18000,'UPI'),tx(207,2000,'Cash','2026-10-02')]}),order(53,{total:1000,transactions:[tx(208,1000,'Cash')]}),order(54,{total:1000,transactions:[tx(209,1000,'Cash','2026-10-02')]})],result=calculate(rows,state());
  const later=result.records.find(row=>row.orderId==='52'&&row.receiptDate==='2026-10-02');assert.deepEqual(later.shares.map(share=>[share.salesperson,share.receiptAmount,share.inheritedQualification,share.incentive]),[['Shivam',1000,true,20],['Krishnakant',1000,false,0]]);
  const day=result.daily.find(day=>day.key==='2026-10-02|Shivam');assert.equal(day.eligibleAmount,2000);assert.equal(day.incentiveBaseAmount,1000);assert.equal(day.carriedQualifiedAmount,1000);assert.equal(day.incentive,20);
  const unrelated=result.records.find(row=>row.orderId==='54').shares[0];assert.equal(unrelated.qualifies,false);assert.equal(unrelated.incentive,0);assert.equal(unrelated.approvalStatus,'not_eligible');
});

test('later qualifying days never retroactively qualify earlier below-threshold receipts',()=>{
  const result=calculate([order(55,{total:62500,transactions:[tx(210,5000,'Cash'),tx(211,50000,'UPI','2026-10-02'),tx(212,7500,'Cash','2026-10-03')]})],state());
  assert.deepEqual(result.daily.map(day=>[day.date,day.incentive]),[['2026-10-01',0],['2026-10-02',1000],['2026-10-03',150]]);
  assert.equal(result.records[2].shares[0].qualificationDate,'2026-10-02');assert.equal(result.records[0].shares[0].inheritedQualification,false);
});

test('later store-credit, failed and authorization transactions cannot earn carried incentives',()=>{
  const rows=[order(56,{total:57500,refundAmount:5000,transactions:[tx(213,50000,'Cash'),tx(214,5000,'Store Credit','2026-10-02'),tx(215,2500,'UPI','2026-10-02'),{...tx(216,20000,'Cash','2026-10-02'),status:'failure'},{...tx(217,20000,'Cash','2026-10-02'),kind:'authorization'}]})],view=buildView(rows,state(),{from:'2026-10-02',to:'2026-10-02'});
  assert.equal(view.summary.totalReceived,7500);assert.equal(view.summary.storeCreditExcluded,5000);assert.equal(view.summary.eligibleReceived,2500);assert.equal(view.summary.incentiveEarned,50);assert.equal(view.summary.refundsReturns,5000);
  const creditOnly=calculate([order(57,{total:57500,transactions:[tx(218,50000,'Cash'),tx(219,7500,'Gift Card','2026-10-02')]})],state());assert.equal(creditOnly.records[1].reviewStatus,'excluded');assert.deepEqual(creditOnly.records[1].shares,[]);assert.equal(creditOnly.daily.length,1);
});

test('all former employees remain non-payable on later collections of large shared sales',()=>{
  for(const name of ['Isha','Nandini','Simran']){
    const view=buildView([order(58,{note:'Shivam + '+name,total:57500,transactions:[tx(220,50000,'Cash'),tx(221,7500,'UPI','2026-10-02')]})],state(),{from:'2026-10-02',to:'2026-10-02'});
    assert.equal(view.summary.incentiveEarned,75);assert.equal(view.summary.recordOnlyReceipts,3750);
    const former=view.records[0].shares.find(share=>share.salesperson===name);assert.equal(former.incentive,0);assert.equal(former.inheritedQualification,false);assert.equal(former.approvalStatus,'record_only');
  }
});

test('approval of a later collection retains qualification evidence and never adds the original receipt twice',()=>{
  const rows=[order(2864,{total:57500,transactions:[tx(222,50000,'Cash'),tx(223,7500,'UPI','2026-10-02')]})];
  let current=state({revision:0,audit:[],approvals:{'2026-10-01|Shivam':{id:'2026-10-01|Shivam',date:'2026-10-01',salesperson:'Shivam',eligibleAmount:50000,incentive:1000}},payments:[{id:'INCP-00001',salesperson:'Shivam',amount:1000,date:'2026-10-01',account:'Counter Cash',active:true,proofs:['/proof.jpg']}],paymentSeq:1});
  const originalApproval=JSON.stringify(current.approvals['2026-10-01|Shivam']),originalPayments=JSON.stringify(current.payments),router=createRouter({loadOrders:()=>rows,loadState:()=>current,saveState:value=>{current=value;}});
  for(let n=0;n<2;n++){
    const approved=invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-02',salesperson:'Shivam',incentive:9999,eligibleAmount:57500});assert.equal(approved.status,200);
    const later=current.approvals['2026-10-02|Shivam'];assert.equal(later.incentive,150);assert.equal(later.incentiveBaseAmount,7500);assert.equal(later.thresholdMet,false);assert.equal(later.carriedQualifiedAmount,7500);assert.equal(later.qualificationSources[0].orderId,'2864');assert.equal(later.qualificationSources[0].qualificationDate,'2026-10-01');
    const ledger=ledgerView(current).find(item=>item.salesperson==='Shivam');assert.equal(ledger.earned,1150);assert.equal(ledger.payableBalance,150);assert.match(ledger.entries.find(entry=>entry.date==='2026-10-02').description,/previously qualified orders/);
    assert.equal(approved.payload.records.find(row=>row.receiptDate==='2026-10-02').shares[0].approvalStatus,'approved');
  }
  assert.equal(JSON.stringify(current.approvals['2026-10-01|Shivam']),originalApproval);assert.equal(JSON.stringify(current.payments),originalPayments);assert.equal(current.paymentSeq,1);
  assert.equal(current.audit[0].details.after.qualificationSources[0].receivedAmount,7500);
});

test('below-threshold unrelated shares stay unapproved even when a carried share is approved',()=>{
  let current=state({revision:0,audit:[]});const rows=[order(59,{total:17500,transactions:[tx(224,10000,'Cash'),tx(225,7500,'UPI','2026-10-02')]}),order(60,{total:1000,transactions:[tx(226,1000,'Cash','2026-10-02')]})],router=createRouter({loadOrders:()=>rows,loadState:()=>current,saveState:value=>{current=value;}});
  const approved=invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-02',salesperson:'Shivam'});assert.equal(approved.status,200);assert.equal(current.approvals['2026-10-02|Shivam'].eligibleAmount,8500);assert.equal(current.approvals['2026-10-02|Shivam'].incentiveBaseAmount,7500);assert.equal(current.approvals['2026-10-02|Shivam'].incentive,150);
  const unrelated=approved.payload.records.find(row=>row.orderId==='60').shares[0];assert.equal(unrelated.incentive,0);assert.equal(unrelated.approvalStatus,'not_eligible');
});

test('unresolved source-day reviews still block later collection approval and survive period filtering',()=>{
  let current=state({revision:0,audit:[]}),saves=0;const rows=[order(61,{total:57500,transactions:[tx(227,50000,'Cash'),tx(228,7500,'UPI','2026-10-02')]}),order(62,{total:100,transactions:[tx(229,100,'Unknown tender')]})],router=createRouter({loadOrders:()=>rows,loadState:()=>current,saveState:value=>{current=value;saves++;}});
  const filtered=buildView(rows,current,{from:'2026-10-02',to:'2026-10-02'});assert.equal(filtered.days[0].unresolved,true);assert.deepEqual(filtered.days[0].qualificationReviewDates,['2026-10-01']);
  const before=JSON.stringify(current),blocked=invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-02',salesperson:'Shivam'});assert.equal(blocked.status,409);assert.match(blocked.payload.error,/qualifying date.*2026-10-01/);assert.equal(saves,0);assert.equal(JSON.stringify(current),before);
  assert.equal(invoke(router,'post','/api/incentives/reviews/:orderId',{salespersons:['Shivam'],gatewayModes:{'229':'excluded'},reason:'Checked non-eligible tender'},{orderId:'62'}).status,200);
  assert.equal(invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-02',salesperson:'Shivam'}).status,200);assert.equal(current.approvals['2026-10-02|Shivam'].incentive,150);
});

test('late synced receipts require reapproval without duplicating approved earnings',()=>{
  let current=state({revision:0,audit:[]});const rows=[order(63,{total:60000,transactions:[tx(230,50000,'Cash'),tx(231,7500,'UPI','2026-10-02')]})],router=createRouter({loadOrders:()=>rows,loadState:()=>current,saveState:value=>{current=value;}});
  assert.equal(invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-02',salesperson:'Shivam'}).status,200);
  rows[0].paymentTransactions.push(tx(232,2500,'Cash','2026-10-02'));
  const changed=buildView(rows,current,{from:'2026-10-02',to:'2026-10-02'});assert.equal(changed.days[0].approvalStatus,'needs_reapproval');assert.equal(changed.days[0].incentive,200);
  assert.equal(invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-02',salesperson:'Shivam'}).status,200);assert.equal(ledgerView(current).find(item=>item.salesperson==='Shivam').earned,200);assert.equal(Object.keys(current.approvals).length,1);
});

test('earlier qualification is derived from real receipts rather than client claims or unrelated approvals',()=>{
  const rows=[order(64,{total:57500,transactions:[{...tx(233,50000,'Cash'),status:'pending'},tx(234,7500,'Cash','2026-10-02')]})];let current=state({revision:0,audit:[],approvals:{'2026-10-01|Shivam':{id:'2026-10-01|Shivam',date:'2026-10-01',salesperson:'Shivam',eligibleAmount:50000,incentive:1000}}}),saves=0;
  const router=createRouter({loadOrders:()=>rows,loadState:()=>current,saveState:()=>{saves++;}}),before=JSON.stringify(current);
  const response=invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-02',salesperson:'Shivam',qualifies:true,qualificationDate:'2026-10-01'});assert.equal(response.status,409);assert.equal(saves,0);assert.equal(JSON.stringify(current),before);
});

test('normal daily totals still include eligible follow-up receipts without paying any receipt twice',()=>{
  const result=calculate([order(65,{total:17500,transactions:[tx(235,10000,'Cash'),tx(236,7500,'UPI','2026-10-02')]}),order(66,{total:2500,transactions:[tx(237,2500,'Cash','2026-10-02')]})],state());
  const day=result.daily[1];assert.equal(day.thresholdMet,true);assert.equal(day.eligibleAmount,10000);assert.equal(day.incentiveBaseAmount,10000);assert.equal(day.carriedQualifiedAmount,7500);assert.equal(day.incentive,200);
  assert.deepEqual(result.records.filter(row=>row.receiptDate==='2026-10-02').map(row=>row.shares[0].incentive),[150,50]);
});

test('an uncertain later tender needs classification, not a second threshold or a manual qualification override',()=>{
  let current=state({revision:0,audit:[]});const rows=[order(69,{total:57500,transactions:[tx(238,50000,'Cash'),tx(239,7500,'Unknown tender','2026-10-02')]})],router=createRouter({loadOrders:()=>rows,loadState:()=>current,saveState:value=>{current=value;}});
  const before=JSON.stringify(current),blocked=invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-02',salesperson:'Shivam'});assert.equal(blocked.status,409);assert.match(blocked.payload.error,/Review every uncertain POS order on 2026-10-02/);assert.equal(JSON.stringify(current),before);
  assert.equal(invoke(router,'post','/api/incentives/reviews/:orderId',{salespersons:['Shivam'],gatewayModes:{'239':'upi_card'},reason:'Confirmed payment tender'},{orderId:'69'}).status,200);
  const approved=invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-02',salesperson:'Shivam'});assert.equal(approved.status,200);assert.equal(current.approvals['2026-10-02|Shivam'].incentive,150);
});

test('a later independently qualifying reviewed day avoids an obsolete provisional-source dependency',()=>{
  let current=state({revision:0,audit:[]});const rows=[order(70,{total:67500,transactions:[tx(240,50000,'Cash'),tx(241,10000,'Cash','2026-10-02'),tx(242,7500,'UPI','2026-10-03')]}),order(71,{total:100,transactions:[tx(243,100,'Unknown tender')]})],router=createRouter({loadOrders:()=>rows,loadState:()=>current,saveState:value=>{current=value;}});
  const view=buildView(rows,current,{from:'2026-10-03',to:'2026-10-03'});assert.equal(view.days[0].unresolved,false);assert.equal(view.records[0].shares[0].qualificationDate,'2026-10-02');assert.deepEqual(view.days[0].qualificationReviewDates,[]);
  assert.equal(invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-03',salesperson:'Shivam'}).status,200);assert.equal(current.approvals['2026-10-03|Shivam'].incentive,150);
  assert.equal(invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-01',salesperson:'Shivam'}).status,409,'the original provisional day still requires its own review');
});
