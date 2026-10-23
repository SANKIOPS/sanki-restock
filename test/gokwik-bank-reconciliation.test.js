'use strict';
const {test,after}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const temp=fs.mkdtempSync(path.join(__dirname,'.tmp-gkw-bank-'));
process.env.DATA_PATH=path.join(temp,'data.json');process.env.ORDERS_PATH=path.join(temp,'orders.json');process.env.SALES_PATH=path.join(temp,'sales.json');process.env.PROCUREMENT_PATH=path.join(temp,'procurement.json');
const file=path.join(temp,'expenses.json');fs.writeFileSync(file,'{}');
const apiFile=path.join(temp,'gokwik_api_evidence.json');
const {router}=require('../modules/expenses'),g=require('../modules/gokwik-accounting'),bridge=require('../modules/gokwik-bank-reconciliation');
const state=()=>JSON.parse(fs.readFileSync(file,'utf8')),save=s=>fs.writeFileSync(file,JSON.stringify(s));
const order={id:'SYNTH-O1',orderNumber:'SYNTH7001',name:'#SYNTH7001',total:150,channel:'Website',financialStatus:'paid',processedAt:'2026-08-24T10:00:00Z',paymentGateways:['upi'],paymentTransactions:[{id:'T1',kind:'sale',status:'success',gateway:'upi',amount:150}]};
function request(method,route,{role='owner',body={},query={}}={}){return {method,path:route,url:route,originalUrl:route,baseUrl:'',body,query,params:{},headers:{},get(){return '';},user:{username:role==='admin'?'prashant':'Owner',role,roles:[role]}};}
function invoke(method,route,options={}){return new Promise((resolve,reject)=>{let status=200,done=false;const finish=body=>{if(!done){done=true;resolve({status,body});}},headers={};const res={statusCode:200,headersSent:false,status(code){status=code;this.statusCode=code;return this;},setHeader(k,v){headers[k]=v;},getHeader(k){return headers[k];},removeHeader(k){delete headers[k];},json(body){this.headersSent=true;finish(body);return this;},send(body){finish(body);return this;},end(body){finish(body);return this;}};router.handle(request(method,route,options),res,error=>error?reject(error):finish(undefined));});}
async function seed(count=1){if(fs.existsSync(apiFile))fs.unlinkSync(apiFile);await invoke('GET','/api/expenses/config');const baseline=state(),tx={paymentId:'SYNTH-P1',orderId:order.id,orderNumber:order.orderNumber,gokwikOrderId:'SYNTH-G1',date:'2026-08-24',settledDate:'2026-08-25',utr:'SYNTHUTR01',provider:'easebuzz',currency:'INR',gross:150,fee:3.75,tax:.68,additionalFee:0,additionalTax:0,deduction:75,credit:145.57,net:70.57,importId:'I'};
 const transactions={},links={},orders={},bank=[];for(let i=0;i<count;i++){const t={...tx,paymentId:'SYNTH-P'+i,orderId:order.id+i,orderNumber:order.orderNumber+i,utr:'SYNTHUTR0'+i};transactions[t.paymentId]=t;links[t.paymentId]={orderId:t.orderId,orderTotal:150};orders[t.orderId]={...order,id:t.orderId,orderNumber:t.orderNumber,name:'#'+t.orderNumber};bank.push({date:t.settledDate,reference:'BANKREF'+i,description:'N/'+t.utr+'/EASEBUZZ',credit:70.57,debit:0,balance:Number(((i+1)*70.57).toFixed(2))});}
 save({...baseline,expenses:{},receivables:{},receipts:[],transfers:[],adjustments:[],vendorAdvances:[],vendorOpeningPayables:[],salesRefunds:[],reconciliationExpenses:[],bankTruthMovements:[],bankStatements:{},bankDateOverrides:{},bankReconciliationLinks:{},bankReconciliationApprovals:{},openingBalances:{[g.BANK]:0,[g.CLEARING]:0},openingBalanceDates:{},paytmReportTransactions:{},paytmOrderLinks:{},paytmShopifyPayments:{},paytmSettlements:[],paytmVerifiedSettlements:[],paytmPayoutPostings:[],saleAllocationOverrides:{},saleComponentAmountOverrides:{},gokwikTransactions:transactions,gokwikOrderLinks:links,gokwikPostings:{},gokwikApprovals:{},gokwikPreviews:{},gokwikImports:{I:{id:'I',hash:'synthetic-hash',name:'synthetic.csv'}},bankReconciliationDrafts:{D:{id:'D',nature:'SANKI',account:g.BANK,transactions:bank,resolutions:{},summary:{from:'2026-08-22',to:'2026-08-25',openingBalance:0,closingBalance:Number((count*70.57).toFixed(2)),totalCredits:Number((count*70.57).toFixed(2)),totalDebits:0,validated:true},matchingPolicy:'balanced_date_amount_v5',createdAt:'2026-10-18T01:00:00Z',createdBy:'prashant',expiresAt:'2099-01-01'}}});
 fs.writeFileSync(process.env.ORDERS_PATH,JSON.stringify({orders}));fs.writeFileSync(process.env.SALES_PATH,JSON.stringify({sales:[]}));return Object.values(orders);
}
const reconcile=()=>invoke('POST','/api/expenses/bank-statements/reconcile',{body:{draftId:'D'}});
const finalize=(options={})=>invoke('POST','/api/expenses/bank-statements/finalize',{...options,body:{draftId:'D',...options.body}});
after(()=>fs.rmSync(temp,{recursive:true,force:true}));
test('preview uses report figures, matches net 0437 credits and changes no financial records',async()=>{await seed(4);const before=JSON.stringify(state()),view=await reconcile();assert.equal(view.status,200,JSON.stringify(view));assert.equal(view.body.unresolved,0);assert.equal(view.body.gokwik.pending,4);assert.equal(view.body.ledgerClosing,282.28);assert.equal(view.body.balanceDifference,0);assert.ok(view.body.rows.every(row=>row.status==='matched'&&row.app.pendingPosting));const statement=await invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account:g.CLEARING,from:'2026-08-22',to:'2026-08-31'}});assert.equal(statement.body.providerStatement.totalIn,600);assert.equal(statement.body.providerStatement.totalOut,600);assert.equal(statement.body.providerStatement.closingBalance,0);assert.match(statement.body.providerStatement.notice,/0437/);assert.equal(JSON.stringify(state()),before);});
test('Admin submits only bank approval; Owner finalizes four settlements and bank links exactly once',async()=>{await seed(4);const submitted=await finalize({role:'admin'});assert.equal(submitted.status,202,JSON.stringify(submitted));let s=state();assert.equal(s.transfers.length,0);assert.equal(Object.keys(s.expenses).length,0);assert.equal(Object.keys(s.gokwikPostings).length,0);assert.ok(submitted.body.approval.gokwikFingerprint);assert.equal((await finalize()).status,409);const approved=await finalize({body:{approveRequestId:submitted.body.approval.id}});assert.equal(approved.status,200,JSON.stringify(approved));s=state();assert.equal(s.transfers.length,4);assert.equal(Object.keys(s.expenses).length,8);assert.equal(Object.keys(s.gokwikPostings).length,4);assert.equal(s.bankReconciliationApprovals[submitted.body.approval.id].status,'approved');assert.equal(s.bankReconciliationDrafts.D,undefined);const book=s.bankStatements[g.BANK];assert.equal(Object.keys(book.transactions).length,4);assert.equal(book.imports[0].reconciliationRows.length,4);assert.ok(book.imports[0].reconciliationRows.every(row=>row.linkedRecordIds.length===1&&row.linkedRecordIds[0].startsWith('TR-')));assert.doesNotMatch(JSON.stringify(book),/GKW-PENDING/);assert.ok(Object.values(s.gokwikPostings).every(p=>p.bankImportId===book.imports[0].id));assert.equal(Object.values(s.expenses).reduce((n,e)=>n+g.money(e.amount),0),31772);assert.ok(Object.values(s.expenses).filter(e=>e.ledger===g.SUBSCRIPTION).every(e=>e.vendor==='GoKwik'&&e.amount===75&&/service charge/.test(e.particulars)));const before=JSON.stringify(s);assert.equal((await finalize({body:{approveRequestId:submitted.body.approval.id}})).status,404);assert.equal(JSON.stringify(state()),before);for(const [account,balance]of [[g.CLEARING,0],[g.BANK,282.28]]){const ledger=await invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account,from:'2026-08-22',to:'2026-08-31'}});assert.equal(ledger.body.balance,balance,JSON.stringify(ledger));}const spending=await invoke('GET','/api/expenses/spending-dashboard',{query:{nature:'SANKI',from:'2026-08-22',to:'2026-08-31'}});assert.equal(spending.body.totalPaid,317.72);});
test('Owner may finalize directly but generic Admin route cannot post early',async()=>{await seed();assert.equal((await finalize()).status,200);assert.equal(state().transfers.length,1);await seed();for(const action of ['submit','approve','withdraw']){const before=JSON.stringify(state()),response=await invoke('POST','/api/expenses/gokwik-reports/'+action,{body:{settlementId:g.batches(Object.values(state().gokwikTransactions))[0].id,reason:'Reviewed original evidence'}});assert.equal(response.status,409);assert.match(response.body.error,/0437/);assert.equal(JSON.stringify(state()),before);}});
test('stale report/order/bank evidence fails with no partial posting and can be resubmitted',async()=>{for(const field of ['order','bank','source','removed']){await seed(2);const submitted=await finalize({role:'admin'});assert.equal(submitted.status,202);const s=state();if(field==='order'){const data=JSON.parse(fs.readFileSync(process.env.ORDERS_PATH));Object.values(data.orders)[0].total=200;fs.writeFileSync(process.env.ORDERS_PATH,JSON.stringify(data));}if(field==='bank')s.bankReconciliationDrafts.D.transactions[0].balance=999;if(field==='source')s.gokwikImports.I.hash='changed-hash';if(field==='removed')s.gokwikTransactions={};save(s);const before=JSON.stringify(state()),result=await finalize({body:{approveRequestId:submitted.body.approval.id}});assert.equal(result.status,409,field+JSON.stringify(result));assert.equal(JSON.stringify(state()),before);assert.equal(state().transfers.length,0);if(field!=='removed'){const resubmit=await finalize({role:'admin'});assert.equal(resubmit.status,202);assert.equal((await finalize({body:{approveRequestId:resubmit.body.approval.id}})).status,200);}}});
test('bank date/net/UTR ambiguity, conflicting decisions and missing order links block all settlements',async()=>{for(const change of [s=>s.bankReconciliationDrafts.D.transactions[0].credit=70.58,s=>s.bankReconciliationDrafts.D.transactions.push({...s.bankReconciliationDrafts.D.transactions[0]}),s=>s.bankReconciliationDrafts.D.resolutions['bank-0']={action:'exclude',reason:'conflicting classification'},s=>s.gokwikOrderLinks={},s=>s.receipts.push({id:'RCPT-DUP',account:g.BANK,nature:'SANKI',date:'2026-08-25',amount:70.57,bankReference:'SYNTHUTR00'}),s=>s.bankReconciliationDrafts.D.matchingExclusions={'bank-0':{reason:'unmatched'}}]){await seed(2);const s=state();change(s);save(s);const result=await finalize();assert.equal(result.status,409,JSON.stringify(result));assert.equal(state().transfers.length,0);assert.equal(Object.keys(state().expenses).length,0);}});
test('other accounts never gain GoKwik report movements and finalized credits are never back-posted',async()=>{const orders=await seed();let s=state();assert.equal(bridge.plan(s,{...s.bankReconciliationDrafts.D,account:'IndusInd Bank 8181'},orders).items.length,0);s.bankStatements[g.BANK]={transactions:{X:s.bankReconciliationDrafts.D.transactions[0]},imports:[]};save(s);const result=await finalize();assert.equal(result.status,409);assert.equal(state().transfers.length,0);});
test('an unresolved opening is not inferred or silently changed',async()=>{await seed();const s=state();s.bankReconciliationDrafts.D.summary.openingBalance=100;s.bankReconciliationDrafts.D.summary.closingBalance=170.57;save(s);assert.equal((await finalize()).status,409);assert.equal(state().transfers.length,0);assert.equal(state().openingBalances[g.BANK],0);});
test('failure during a later settlement saves neither earlier settlements nor bank finalization',async()=>{await seed(2);const original=bridge.post;let calls=0;bridge.post=(...args)=>{if(++calls===2)throw Error('Synthetic late posting failure');return original(...args);};const before=JSON.stringify(state());try{const result=await finalize();assert.equal(result.status,409);assert.match(result.body.error,/Nothing was saved/);assert.equal(JSON.stringify(state()),before);}finally{bridge.post=original;}});

async function seedFreshApiEvidence() {
  const orders=await seed(),s=state(),tx=Object.values(s.gokwikTransactions)[0];
  // Unlike the older CSV fixtures, no provider import or order link has ever
  // been saved. loadStore must project this API evidence read-only first.
  s.gokwikTransactions={};s.gokwikImports={};s.gokwikOrderLinks={};delete s.gokwikApiAccounting;save(s);
  const row={type:'Payment',paymentId:tx.paymentId,orderId:tx.orderId,orderNumber:'#'+tx.orderNumber,
    providerOrderId:tx.gokwikOrderId,date:tx.settledDate,transactionDate:tx.date+'T10:00:00Z',
    utr:tx.utr,provider:tx.provider,gross:tx.gross,fees:4.43,fee:tx.fee,tax:tx.tax,credit:tx.credit,
    debit:0,deductions:{gokwik:tx.deduction},reportedGokwikDeduction:tx.deduction,net:tx.net};
  fs.writeFileSync(apiFile,JSON.stringify({days:{[row.date]:[row]}}));
  return {orders,paymentId:tx.paymentId};
}

test('fresh API evidence survives Admin first-save fingerprinting and Owner approval exactly once',async()=>{
  const {orders,paymentId}=await seedFreshApiEvidence(),before=JSON.stringify(state());
  const preview=await reconcile();
  assert.equal(preview.status,200,JSON.stringify(preview));assert.equal(preview.body.unresolved,0);
  assert.equal(preview.body.gokwik.pending,1);
  assert.equal(JSON.stringify(state()),before,'read-only staging does not persist provider audit timestamps or accounting');
  const submitted=await finalize({role:'admin'});
  assert.equal(submitted.status,202,JSON.stringify(submitted));
  const saved=state(),approval=submitted.body.approval,imports=Object.values(saved.gokwikImports),link=saved.gokwikOrderLinks[paymentId];
  assert.equal(imports.length,1);assert.match(imports[0].at,/^\d{4}-\d{2}-\d{2}T/);
  assert.equal(link.at,imports[0].at,'the approval save records one real audit time for new API evidence');
  assert.equal(Object.hasOwn(link,'_gokwikApiPendingAudit'),false);
  assert.equal(Object.hasOwn(imports[0],'_gokwikApiPendingAudit'),false);
  assert.equal(approval.gokwikFingerprint,bridge.plan(saved,saved.bankReconciliationDrafts.D,orders).fingerprint,
    'approval fingerprint must describe the actual saved evidence, including finalized audit metadata');
  assert.equal(saved.transfers.length,0);assert.equal(Object.keys(saved.expenses).length,0);
  assert.equal(Object.keys(saved.gokwikPostings).length,0,'Admin request never posts settlements');
  assert.equal(saved.bankReconciliationApprovals[approval.id].status,'pending');
  const approved=await finalize({body:{approveRequestId:approval.id}});
  assert.equal(approved.status,200,JSON.stringify(approved));
  const after=state();
  assert.equal(after.bankReconciliationApprovals[approval.id].status,'approved');
  assert.equal(after.transfers.length,1);assert.equal(Object.keys(after.expenses).length,2);
  assert.equal(Object.keys(after.gokwikPostings).length,1);
  assert.equal(Object.keys(after.bankStatements[g.BANK].transactions).length,1);
  assert.equal(after.gokwikOrderLinks[paymentId].at,link.at,'Owner approval must not restamp previously saved evidence');
  const posted=JSON.stringify(after);
  assert.equal((await finalize({body:{approveRequestId:approval.id}})).status,404);
  assert.equal(JSON.stringify(state()),posted,'repeating the approval cannot duplicate bank links or accounting');
});

test('Owner direct finalization fingerprints the saved API audit metadata and posts exactly once',async()=>{
  const {orders,paymentId}=await seedFreshApiEvidence(),before=state();
  const finalized=await finalize();
  assert.equal(finalized.status,200,JSON.stringify(finalized));
  const after=state(),postings=Object.values(after.gokwikPostings),imports=Object.values(after.gokwikImports);
  assert.equal(postings.length,1);assert.equal(after.transfers.length,1);assert.equal(Object.keys(after.expenses).length,2);
  assert.equal(Object.keys(after.bankStatements[g.BANK].transactions).length,1);
  assert.equal(imports.length,1);assert.match(imports[0].at,/^\d{4}-\d{2}-\d{2}T/);
  assert.equal(after.gokwikOrderLinks[paymentId].at,imports[0].at);
  assert.doesNotMatch(JSON.stringify(after),/_gokwikApiPendingAudit/,'no transient audit markers may enter saved accounting or audit history');
  // Reconstruct the pre-posting review using the exact provider records that
  // were actually saved. Finalized bank history changes the bank locator, so
  // keep the original synthetic draft and other pre-posting evidence here.
  const reviewed={...before,gokwikTransactions:after.gokwikTransactions,gokwikImports:after.gokwikImports,
    gokwikOrderLinks:after.gokwikOrderLinks};
  const plan=bridge.plan(reviewed,reviewed.bankReconciliationDrafts.D,orders);
  assert.equal(plan.errors.length,0,JSON.stringify(plan.errors));assert.equal(plan.items.length,1);
  assert.equal(postings[0].approval.fingerprint,plan.items[0].fingerprint,
    'immutable posting approval must hash the same audit-bearing provider records saved by finalization');
  const saved=JSON.stringify(after);
  assert.equal((await finalize()).status,404);
  assert.equal(JSON.stringify(state()),saved,'repeated Owner direct finalization cannot post twice');
});

test('real changes after fresh API approval still invalidate Owner approval without partial posting',async()=>{
  for(const change of ['order funding','retained source hash','bank row','saved order link']){
    const {paymentId}=await seedFreshApiEvidence(),submitted=await finalize({role:'admin'});
    assert.equal(submitted.status,202,JSON.stringify(submitted));
    const s=state();
    if(change==='order funding'){
      const data=JSON.parse(fs.readFileSync(process.env.ORDERS_PATH,'utf8'));
      Object.values(data.orders)[0].total=200;fs.writeFileSync(process.env.ORDERS_PATH,JSON.stringify(data));
    }
    if(change==='retained source hash')Object.values(s.gokwikImports)[0].hash='changed-retained-api-evidence';
    if(change==='bank row')s.bankReconciliationDrafts.D.transactions[0].balance=999;
    if(change==='saved order link')s.gokwikOrderLinks[paymentId].reason+='; changed after Admin review';
    save(s);
    const before=JSON.stringify(state()),result=await finalize({body:{approveRequestId:submitted.body.approval.id}});
    assert.equal(result.status,409,change+': '+JSON.stringify(result));
    assert.equal(JSON.stringify(state()),before,change+': rejected approval must save no partial changes');
    assert.equal(state().transfers.length,0);assert.equal(Object.keys(state().expenses).length,0);
    assert.equal(Object.keys(state().gokwikPostings).length,0);
    assert.equal(state().bankReconciliationApprovals[submitted.body.approval.id].status,'pending');
  }
});
