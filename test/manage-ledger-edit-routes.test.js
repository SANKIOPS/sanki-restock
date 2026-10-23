'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
// Fictional fixtures only. No production snapshots, accounts data or IDs.
const temp=fs.mkdtempSync(path.join(os.tmpdir(),'manage-ledger-edit-'));
process.env.DATA_PATH=path.join(temp,'data.json');process.env.SESSION_SECRET='synthetic-ledger-edit-secret';
for(const [key,name] of Object.entries({PROCUREMENT_PATH:'procurement.json',INCENTIVES_PATH:'incentives.json',USERS_PATH:'users.json'}))process.env[key]=path.join(temp,name);
const rental=require.resolve('../modules/rental-source.json');require.cache[rental]={id:rental,filename:rental,loaded:true,exports:{id:'synthetic',rows:[]}};
const file=path.join(temp,'expenses.json'),cardFile=path.join(temp,'credit-cards.json');
fs.writeFileSync(file,JSON.stringify({expenses:{},receivables:{},receipts:[],oneTimeMigrations:{}}));
const {router}=require('../modules/expenses');
const clone=x=>JSON.parse(JSON.stringify(x)),read=()=>JSON.parse(fs.readFileSync(file,'utf8')),raw=()=>fs.readFileSync(file,'utf8'),write=s=>fs.writeFileSync(file,JSON.stringify(s));
const bank='Axis Bank 3448',other='IndusInd Bank 8181',day='2098-04-17',previewPath='/api/expenses/ledger-entry/edit-preview',patchPath='/api/expenses/ledger-entry';
const actor={username:'synthetic-admin',role:'admin',roles:['admin'],permissions:{}},owner={username:'synthetic-owner',role:'owner',roles:['owner'],permissions:{}};
function invoke(method,route,{body={},query={},user=actor}={}){
  const layer=router.stack.find(x=>x.route?.path===route&&x.route.methods[method.toLowerCase()]);assert.ok(layer,route);
  let status=200,result;const req={method,path:route,url:route,originalUrl:route,body,query,params:{},user:clone(user),headers:{'user-agent':'Synthetic ledger tests'},get(k){return this.headers[k]||'';}};
  layer.route.stack[0].handle(req,{status(code){status=code;return this;},json(value){result=value;return this;}},()=>{throw Error('Unexpected next');});
  return {status,body:result};
}
// Finish existing initialization only in our isolated synthetic directory.
assert.equal(invoke('GET','/api/expenses/config',{user:owner}).status,200);
const baseline=read();
function seed(mutate=()=>{}){
  const s={...clone(baseline),expenses:{'EX-SYNTHETIC':{id:'EX-SYNTHETIC',nature:'SANKI',amount:500,date:day,requestedAmount:500,status:'partially_paid',paidAmount:250,approvedAt:day,account:bank,payments:[{id:'PAY-001',amount:250,date:day,account:bank,personalFunds:false,proof:'/synthetic-proof.jpg',note:'Exact original note'}],reimbursementPayments:[],reimbursementAmount:0,personalPaidAmount:0,reimbursementStatus:'not_applicable'}},
    transfers:[{id:'TR-SYNTHETIC',nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',fromAccount:bank,toAccount:other,classification:'internal_transfer',amount:100,date:day,note:'Original transfer note',proof:'/synthetic-proof.jpg'}],
    receipts:[{id:'REC-SYNTHETIC',nature:'SANKI',account:bank,date:day,amount:150,receiptType:'other_income',category:'Other Income',source:'Synthetic source',note:'Exact receipt note',proof:'/synthetic-proof.jpg'}],
    adjustments:[],reconciliationExpenses:[],loanRepayments:[],vendorAdvances:[],vendorOpeningPayables:[],expenseRefunds:[],refunds:[],bankStatements:{},bankDateOverrides:{},bankReconciliationLinks:{},bankReconciliationDrafts:{},bankReconciliationApprovals:{},bankMovementAuthority:{},cashReconciliations:[],gokwikPostings:{},openingBalances:{[bank]:1000},openingBalanceDates:{},openingBalanceDatesByNature:{},auditSeq:1,auditLog:[{id:'AUD-PRIOR',action:'SYNTHETIC_PRIOR',note:'Never truncate or rewrite'}],untouched:{proof:'Original preserved data'}};
  for(const name of ['salary.json','salary-samast.json','credit-cards.json','procurement.json','incentives.json'])fs.writeFileSync(path.join(temp,name),'{}');
  mutate(s);write(s);return s;
}
const intent=(kind='transfer')=>({kind,id:kind==='transfer'?'TR-SYNTHETIC':kind==='receipt'?'REC-SYNTHETIC':'EX-SYNTHETIC/PAY-001',nature:'SANKI',account:bank});
const preview=(kind='transfer',options={})=>invoke('GET',previewPath,{query:intent(kind),...options});
const apply=(fingerprint,changes={},options={})=>invoke('PATCH',patchPath,{body:{...intent(),fingerprint,reason:'Correct synthetic entry after checking source evidence',...changes},...options});
const noWrite=(before,result,status=409)=>{assert.equal(result.status,status,JSON.stringify(result));assert.equal(raw(),before,'Rejected action must not migrate, normalize, audit or save anything');};
test.after(()=>fs.rmSync(temp,{recursive:true,force:true}));

test('Admin/Owner capability is narrower than accounting, claimant, PERSONAL-only or page grants',()=>{
  seed();
  for(const user of [actor,owner,{...actor,username:'prashant'}])assert.equal(invoke('GET','/api/expenses/config',{user}).body.canEditLedgerTransactions,true);
  for(const role of ['accounting','samast_accounting','claimant','personal_claimant','inventory','stocksearch']){
    const user={username:'synthetic-'+role,role,roles:[role],permissions:{personalAccounting:true}};
    assert.equal(invoke('GET','/api/expenses/config',{user}).body.canEditLedgerTransactions,false,role);
    const before=raw();noWrite(before,preview('transfer',{user}),403);noWrite(before,apply('x',{}, {user}),403);
  }
});
test('preview is exact and read-only even with unmigrated historical data',()=>{
  seed(s=>{s.oneTimeMigrations={};s.categories=['Do not normalize'];s.expenses['EX-LEGACY']={id:'EX-LEGACY',amount:123,legacy:true};});
  const before=raw(),r=preview();assert.equal(r.status,200,JSON.stringify(r));assert.match(r.body.fingerprint,/^[a-f0-9]{64}$/);assert.equal(r.body.entry.amount,100);assert.equal(r.body.entry.note,'Original transfer note');assert.equal(raw(),before);
});
test('actual Express dispatch reaches the read-only preview and applies its audited partial edit',async()=>{
  seed();
  function dispatch(method,route,body={},query={}){return new Promise((resolve,reject)=>{let status=200;const req={method,path:route,url:route,originalUrl:route,baseUrl:'',body,query,params:{},headers:{},user:clone(actor),get(){return'';}},headers={},res={headersSent:false,statusCode:200,status(n){status=n;this.statusCode=n;return this;},setHeader(k,v){headers[k]=v;},getHeader(k){return headers[k];},removeHeader(k){delete headers[k];},json(value){this.headersSent=true;resolve({status,body:value});return this;},end(value){resolve({status,body:value});}};router.handle(req,res,error=>error?reject(error):resolve({status:404}));});}
  const before=raw(),p=await dispatch('GET',previewPath,{},intent());assert.equal(p.status,200,JSON.stringify(p));assert.equal(raw(),before);
  const r=await dispatch('PATCH',patchPath,{...intent(),fingerprint:p.body.fingerprint,amount:111.11,reason:'Verified partial edit through real routing'});assert.equal(r.status,200,JSON.stringify(r));assert.equal(read().transfers[0].amount,111.11);
});
test('per-snapshot evidence index preserves exact nested values and keys without mutation or reuse',()=>{
  const editor=require('../modules/ledger-transaction-edits').create({});
  const state={bankDateOverrides:{OVERRIDE:{bankReference:'NOT-A-LOCK'}},bankReconciliationLinks:{LINK:{}},bankStatements:{B:{imports:[{ledger:{id:'EX-SYNTHETIC/PAY-001'},linkedRecordIds:['TR-SYNTHETIC'],reason:'prefix REC-SYNTHETIC suffix'}]}},bankReconciliationDrafts:{D:{resolutions:{'bank-0':{appIds:['REC-SYNTHETIC']}}}},bankMovementAuthority:{'AUTHORITY-KEY':{id:'AUTHORITY-VALUE'}}};
  const external={salary:{records:[{expenseId:'EX-SALARY'}]},cards:{'TR-CARD':{reference:'unrelated'}}},before=JSON.stringify([state,external]);
  const index=editor.evidence(state,external);for(const id of ['OVERRIDE','LINK','EX-SYNTHETIC/PAY-001','TR-SYNTHETIC','REC-SYNTHETIC','AUTHORITY-KEY','AUTHORITY-VALUE'])assert.equal(index.bank.has(id),true,id);
  assert.equal(index.bank.has('NOT-A-LOCK'),false);assert.equal(index.bank.has('SYNTHETIC'),false);assert.equal(index.external.has('EX-SALARY'),true);assert.equal(index.external.has('TR-CARD'),true);assert.equal(JSON.stringify([state,external]),before);
  const next=clone(state);next.bankDateOverrides.NEW={};const nextIndex=editor.evidence(next,external);assert.equal(index.bank.has('NEW'),false);assert.equal(nextIndex.bank.has('NEW'),true);assert.notEqual(index.bank,nextIndex.bank);
});
for(const kind of ['transfer','receipt','expense'])test('Admin can audit one unlinked '+kind+' correction without touching unrelated records',()=>{
  const before=seed(),prepared=preview(kind);assert.equal(prepared.status,200,JSON.stringify(prepared));
  const result=apply(prepared.body.fingerprint,{...intent(kind),amount:'299.25'});assert.equal(result.status,200,JSON.stringify(result));const after=read();
  const old=kind==='transfer'?before.transfers[0]:kind==='receipt'?before.receipts[0]:before.expenses['EX-SYNTHETIC'].payments[0],current=kind==='transfer'?after.transfers[0]:kind==='receipt'?after.receipts[0]:after.expenses['EX-SYNTHETIC'].payments[0];
  assert.equal(current.amount,299.25);for(const field of ['date','note','source','proof','nature','account','fromAccount','toAccount','fromNature','toNature'])assert.deepEqual(current[field],old[field],field);
  for(const key of ['adjustments','bankStatements','bankDateOverrides','bankReconciliationLinks','bankReconciliationDrafts','bankReconciliationApprovals','openingBalances','openingBalanceDates','untouched'])assert.deepEqual(after[key],before[key],key);
  if(kind!=='transfer')assert.deepEqual(after.transfers,before.transfers);if(kind!=='receipt')assert.deepEqual(after.receipts,before.receipts);if(kind!=='expense')assert.deepEqual(after.expenses,before.expenses);
  assert.deepEqual(after.auditLog[0],before.auditLog[0]);const audit=after.auditLog.at(-1);assert.equal(audit.user,actor.username);assert.equal(audit.subjectId,intent(kind).id);assert.equal(audit.nature,'SANKI');assert.equal(audit.account,bank);assert.equal(audit.before.amount,old.amount);assert.equal(audit.after.amount,299.25);assert.match(audit.note,/source evidence/);
  if(kind==='expense'){assert.equal(after.expenses['EX-SYNTHETIC'].paidAmount,299.25);assert.equal(after.expenses['EX-SYNTHETIC'].amount,500);assert.equal(audit.expenseBefore.paidAmount,250);assert.equal(audit.expenseAfter.paidAmount,299.25);}
});
test('description and date corrections require reason but never require changing an empty legacy source',()=>{
  seed(s=>{s.receipts[0].source='';});let p=preview('receipt'),r=apply(p.body.fingerprint,{...intent('receipt'),amount:175.01});assert.equal(r.status,200,JSON.stringify(r));assert.equal(read().receipts[0].source,'');
  p=preview('receipt');r=apply(p.body.fingerprint,{...intent('receipt'),date:'2098-04-18',note:'Corrected note',source:'Verified payer'});assert.equal(r.status,200,JSON.stringify(r));assert.equal(read().receipts[0].date,'2098-04-18');assert.equal(read().receipts[0].source,'Verified payer');
});
test('same-value edit does not append audit or save',()=>{seed();const p=preview(),before=raw(),r=apply(p.body.fingerprint,{amount:100,note:'Original transfer note'});assert.equal(r.status,200);assert.equal(r.body.unchanged,true);assert.equal(raw(),before);});
test('full old audit history is retained beyond the legacy ten-thousand-event cap',()=>{
  const before=seed(s=>{s.auditLog=Array.from({length:10001},(_,i)=>({id:'OLD-'+i}));s.auditSeq=10001;});const p=preview();assert.equal(apply(p.body.fingerprint,{amount:101}).status,200);assert.deepEqual(read().auditLog.slice(0,10001),before.auditLog);assert.equal(read().auditLog.length,10002);
});
for(const change of [{amount:0},{amount:-1},{amount:1.001},{amount:'1.005'},{amount:'1e3'},{amount:'20junk'},{amount:Infinity},{date:'2098-02-30'},{source:'Forged payer'},{proof:'/changed.jpg'},{reason:''}])test('invalid or out-of-scope payload fails without writes '+JSON.stringify(change),()=>{seed();const p=preview(),before=raw();noWrite(before,apply(p.body.fingerprint,{amount:120,...change}),400);});
test('expense overpayment cannot silently become vendor credit',()=>{seed();const p=preview('expense'),before=raw();noWrite(before,apply(p.body.fingerprint,{...intent('expense'),amount:501}));});
test('missing preview, stale store, stale external evidence and changed actor reject',()=>{
  for(const mode of ['missing','store','external','actor']){
    seed();const p=preview();if(mode==='store'){const s=read();s.untouched.revision=2;write(s);}if(mode==='external')fs.writeFileSync(cardFile,JSON.stringify({version:2}));
    const before=raw();noWrite(before,apply(mode==='missing'?'':p.body.fingerprint,{amount:125},mode==='actor'?{user:owner}:{}));
  }
});
test('permission revocation after preview cannot reuse an old fingerprint',()=>{
  seed();const p=preview();const user={username:actor.username,role:'accounting',roles:['accounting'],permissions:{}};noWrite(raw(),apply(p.body.fingerprint,{amount:125},{user}),403);
});
test('spoofing a displayed account or nature never bypasses stored scopes',()=>{
  for(const forged of [{nature:'SAMAST'},{account:'Counter Cash'}]){seed();const p=preview();noWrite(raw(),apply(p.body.fingerprint,{amount:125,...forged}),403);}
  seed(s=>{s.transfers[0].toNature='PERSONAL';s.transfers[0].toAccount='Namita 5464';});noWrite(raw(),preview(),403);
  const p=preview('transfer',{user:owner});assert.equal(p.status,200,JSON.stringify(p));noWrite(raw(),apply(p.body.fingerprint,{amount:125}),403);
});
test('PERSONAL source scope requires personalAccounting independently of a visible business book',()=>{
  seed(s=>{s.expenses['EX-SYNTHETIC'].nature='PERSONAL';});noWrite(raw(),preview('expense'),403);
  const user={...actor,permissions:{personalAccounting:true}},p=preview('expense',{user});assert.equal(p.status,200,JSON.stringify(p));const r=apply(p.body.fingerprint,{...intent('expense'),amount:275},{user});assert.equal(r.status,200,JSON.stringify(r));assert.equal(read().auditLog.at(-1).nature,'PERSONAL');
});
test('PERSONAL receipt reporting entity also requires permission',()=>{seed(s=>{s.receipts[0].reportingNature='PERSONAL';});noWrite(raw(),preview('receipt'),403);});
test('stored SAMAST shared-account scope is not misclassified as PERSONAL editing authority',()=>{
  seed(s=>{Object.assign(s.transfers[0],{nature:'SAMAST',fromNature:'SAMAST',toNature:'SAMAST',fromAccount:'IndusInd Bank 7883',toAccount:'ICICI Bank 0993'});});
  const query={...intent(),nature:'SAMAST',account:'IndusInd Bank 7883'},p=preview('transfer',{query});assert.equal(p.status,200,JSON.stringify(p));
  const r=apply(p.body.fingerprint,{...query,amount:125});assert.equal(r.status,200,JSON.stringify(r));assert.equal(read().auditLog.at(-1).nature,'SAMAST');
  seed(s=>{Object.assign(s.transfers[0],{fromNature:'SANKI',fromAccount:'Namita 5464'});});noWrite(raw(),preview('transfer',{query:{...intent(),account:'Namita 5464'}}),403);
});
test('private account visibility and Owner-only expense restrictions remain',()=>{
  seed(s=>{s.transfers[0].toAccount='Services';});noWrite(raw(),preview(),403);
  seed(s=>{s.expenses['EX-SYNTHETIC'].ownerOnly=true;});noWrite(raw(),preview('expense'),403);
});
const locks=[
  ['direct override',s=>{s.bankDateOverrides['TR-SYNTHETIC']={bankDate:day};}],
  ['grouped finalized history',s=>{s.bankStatements[other]={imports:[{reconciliationRows:[{ledger:{id:'GROUP',sourceIds:['TR-SYNTHETIC']}}]}]};}],
  ['active draft linkage',s=>{s.bankReconciliationDrafts.D={resolutions:{'bank-0':{appIds:['TR-SYNTHETIC']}}};}],
  ['receiving leg finalized period',s=>{s.bankStatements[other]={reconciledThrough:day};}],
  ['sending leg finalized period',s=>{s.bankStatements[bank]={reconciledThrough:day};}],
  ['pending receiving statement',s=>{s.bankReconciliationApprovals.A={status:'pending',account:other,from:day,to:day};}],
  ['pending missing range',s=>{s.bankReconciliationApprovals.A={status:'pending',account:other};}],
  ['pending draft-derived period',s=>{s.bankReconciliationDrafts.D={account:other,summary:{from:day,to:day}};s.bankReconciliationApprovals.A={status:'pending',draftId:'D'};}],
  ['cash verification',s=>{s.cashReconciliations=[{account:other,status:'approved',verifiedThrough:day}];}],
  ['pending cash verification',s=>{s.cashReconciliations=[{account:other,status:'pending',verifiedThrough:day}];}],
  ['period before account opening',s=>{s.openingBalanceDates[other]='2098-04-18';}],
  ['excluded transfer',s=>{s.transfers[0].accountingExcluded=true;}],
  ['opening absorbed transfer',s=>{s.transfers[0].openingBalanceTreatment='absorbed';}],
  ['credit card payment',s=>{s.transfers[0].classification='credit_card_payment';}],
  ['salary advance transfer',s=>{s.transfers[0].salaryAdvanceId='ADV-SYNTHETIC';}],
  ['reimbursement transfer',s=>{s.transfers[0].reimbursementAllocations=[{expenseId:'EX-SYNTHETIC',paymentId:'REIM-001'}];}],
  ['receipt source',s=>{s.receipts[0].manualSaleId='SALE-SYNTHETIC';},'receipt'],
  ['receipt draft',s=>{s.receipts[0].bankReconciliationEvidence={draftId:'D'};},'receipt'],
  ['provisional payment',s=>{s.expenses['EX-SYNTHETIC'].payments[0].provisional=true;},'expense'],
  ['consolidated payment',s=>{s.expenses['EX-SYNTHETIC'].payments[0].batchPaymentId='B-SYNTHETIC';},'expense'],
  ['credit card statement payment',s=>{s.expenses['EX-SYNTHETIC'].payments[0].creditCardStatementId='CC-SYNTHETIC';},'expense'],
  ['expense reconciliation source',s=>{s.expenses['EX-SYNTHETIC'].reconciliationSource={draftId:'D'};},'expense'],
  ['expense reimbursement linkage',s=>{s.expenses['EX-SYNTHETIC'].reimbursementPayments=[{id:'REIM-001',amount:50}];},'expense']
];
for(const [label,mutate,kind='transfer'] of locks)test(label+' remains locked even for Owner and note-only forged requests',()=>{
  seed(mutate);const before=raw();for(const user of [actor,owner]){noWrite(before,preview(kind,{user}));noWrite(before,apply('forged',{...intent(kind),note:'Cannot detach existing evidence'},{user}));}
});
test('new dates cannot enter a closed or pending receiving-leg period',()=>{
  for(const pending of [false,true]){seed(s=>{if(pending)s.bankReconciliationApprovals.A={status:'pending',account:other,from:'2098-04-01',to:'2098-04-05'};else s.bankStatements[other]={reconciledThrough:'2098-04-05'};});const p=preview();assert.equal(p.status,200,JSON.stringify(p));noWrite(raw(),apply(p.body.fingerprint,{date:'2098-04-04'}));}
});
for(const name of ['salary.json','salary-samast.json','credit-cards.json','procurement.json','incentives.json'])test(name+' exact source link prevents isolated edits',()=>{
  seed();fs.writeFileSync(path.join(temp,name),JSON.stringify({records:[{linkedTransferId:'TR-SYNTHETIC'}]}));noWrite(raw(),preview());
});
test('missing/duplicate IDs, corrupt external store and pending atomic journal fail closed',()=>{
  seed(s=>s.transfers.push(clone(s.transfers[0])));noWrite(raw(),preview(),404);
  seed();noWrite(raw(),preview('transfer',{query:{...intent(),id:'TR-NOT-FOUND'}}),404);
  seed();fs.writeFileSync(cardFile,'not-json');noWrite(raw(),preview());
  seed();const journal=path.join(temp,'.bank-authority-transaction.json');fs.writeFileSync(journal,'{}');try{noWrite(raw(),preview());assert.equal(fs.readFileSync(journal,'utf8'),'{}');}finally{fs.unlinkSync(journal);}
});
test('personal payment kind cannot be forged, and an unlinked permitted personal payment updates its aggregate',()=>{
  seed(s=>{s.expenses['EX-SYNTHETIC'].payments[0].personalFunds=true;s.expenses['EX-SYNTHETIC'].personalPaidAmount=250;});noWrite(raw(),preview('expense'));
  const p=preview('personal_expense');assert.equal(p.status,200,JSON.stringify(p));assert.equal(apply(p.body.fingerprint,{...intent('personal_expense'),amount:275}).status,200);assert.equal(read().expenses['EX-SYNTHETIC'].personalPaidAmount,275);
});
test('unrelated Owner-only adjustment, reimbursement, opening and exchange authorities are not expanded',()=>{
  seed();for(const kind of ['adjustment','reimbursement','reimbursement_received','receivable'])noWrite(raw(),invoke('PATCH',patchPath,{body:{kind,id:'NOT-SUPPORTED',amount:25,date:day,reason:'Attempt broader authority'}}),403);
  const code=fs.readFileSync(path.join(__dirname,'../modules/expenses.js'),'utf8');assert.match(code,/router\.delete\('\/api\/expenses\/ledger-entry\/:id',[\s\S]*?if\(!isOwner\(req\)\)/);
});
