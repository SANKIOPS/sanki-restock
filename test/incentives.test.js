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
  for(const [note,names] of [['654321\nISHA',['Isha']],['654321 naNDini',['Nandini']],['654321\nIsha + NANDINI',['Isha','Nandini']],['654321\nShivam + Isha',['Shivam','Isha']]]){
    const parsed=parseSalespeople(note);assert.deepEqual(parsed.salespersons,names);assert.equal(parsed.confidence,'confirmed');
  }
  assert.ok(!parseSalespeople('654321\nNisha').salespersons.includes('Isha'));
  assert.deepEqual(parseSalespeople('654321\nIshaa').salespersons,[]);
});

test('former employee receipts remain in the report but never qualify or need repeated review',()=>{
  const orders=[order(20,{note:'123456\nIsha',total:25000,transactions:[tx(20,25000,'Cash')]}),order(21,{note:'123457\nNandini',total:18000,transactions:[tx(21,18000,'UPI')]})];
  const current=state(),before=JSON.stringify(current),view=buildView(orders,current);
  assert.equal(view.summary.orders,2);assert.equal(view.summary.totalReceived,43000);assert.equal(view.summary.eligibleReceived,0);assert.equal(view.summary.recordOnlyReceipts,43000);assert.equal(view.summary.incentiveEarned,0);assert.equal(view.summary.outstanding,0);assert.equal(view.summary.needsReview,0);
  for(const row of view.records){assert.equal(row.reviewStatus,'record_only');assert.equal(row.matchConfidence,'confirmed');assert.deepEqual(row.reviewIssues,[]);assert.equal(row.eligibleAmount,0);assert.match(row.exclusionReason,/former employee/);assert.equal(row.shares[0].incentive,0);assert.equal(row.shares[0].approvalStatus,'record_only');}
  for(const day of view.days){assert.equal(day.qualifies,false);assert.equal(day.incentive,0);assert.equal(day.unresolved,false);assert.equal(day.approvalStatus,'record_only');}
  assert.deepEqual(buildView(orders,current),view);assert.equal(JSON.stringify(current),before);
});

test('record-only orders do not block active daily approval even with missing or unknown tenders',()=>{
  const orders=[order(22,{total:12000,transactions:[tx(22,12000,'Cash')]}),order(23,{note:'654321\nIsha',transactions:[tx(23,10000,'Unmapped tender')]}),order(24,{note:'Nandini'})];
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
  assert.deepEqual(filtered.configuration.salespersonDetails.filter(person=>person.recordOnly).map(person=>person.name),['Isha','Nandini']);assert.ok(filtered.configuration.salespersons.includes('Isha'));
});

test('server rejects incentive approval and payment for former staff without changing state',()=>{
  for(const salesperson of ['Isha','Nandini']){
    const id='2026-10-01|'+salesperson,current=state({revision:4,audit:[],paymentSeq:0,approvals:{[id]:{id,date:'2026-10-01',salesperson,eligibleAmount:25000,incentive:500}}}),before=JSON.stringify(current);
    let saves=0;const router=createRouter({loadOrders:()=>[order(29,{note:salesperson,transactions:[tx(29,25000,'Cash')]})],loadState:()=>current,saveState:()=>{saves++;}});
    const approved=invoke(router,'post','/api/incentives/approve-day',{date:'2026-10-01',salesperson});assert.equal(approved.status,409);assert.match(approved.payload.error,/record only/);
    const paid=invoke(router,'post','/api/incentives/payments',{salesperson,amount:100,date:'2026-10-02',account:'Counter Cash',proofs:['/proof.jpg']});assert.equal(paid.status,409);assert.match(paid.payload.error,/not allowed/);assert.equal(saves,0);assert.equal(JSON.stringify(current),before);
    const ledger=ledgerView(current).find(item=>item.salesperson===salesperson);assert.equal(ledger.recordOnly,true);assert.equal(ledger.payableBalance,0);assert.equal(ledger.entries.length,1);assert.equal(ledger.balance,500);
    const view=buildView([],current);assert.equal(view.summary.approvedIncentive,0);assert.equal(view.summary.outstanding,0);
  }
});

test('former-only store credits and refunds remain visible without incentive liability',()=>{
  const view=buildView([order(30,{note:'Isha + Nandini',total:20000,refundAmount:1000,transactions:[tx(30,20000,'Store credit')]})],state());
  assert.equal(view.summary.storeCreditExcluded,20000);assert.equal(view.summary.refundsReturns,1000);assert.equal(view.records[0].reviewStatus,'record_only');assert.equal(view.summary.needsReview,0);assert.equal(view.summary.incentiveEarned,0);
});
