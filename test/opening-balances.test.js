'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'opening-balances-'));
for (const [key,file] of Object.entries({DATA_PATH:'data.json', ORDERS_PATH:'orders.json', SALES_PATH:'sales.json', PROCUREMENT_PATH:'procurement.json', INCENTIVES_PATH:'incentives.json', VELOCITY_REMITTANCE_PATH:'velocity.json'})) process.env[key] = path.join(dir,file);
for (const file of ['expenses.json','orders.json','sales.json','procurement.json','incentives.json','velocity.json','salary.json','credit-cards.json']) fs.writeFileSync(path.join(dir,file),'{}');
const file = path.join(dir,'expenses.json'), {router} = require('../modules/expenses');
test.after(() => fs.rmSync(dir,{recursive:true,force:true}));
const account = 'Tiana Traders IndusInd 0437', base = '/api/expenses/opening-balances';
const read = () => JSON.parse(fs.readFileSync(file,'utf8'));
function seed(extra = {}) {
  const store = {expenses:{}, openingBalances:{[account]:0}, openingBalanceDates:{}, bankStatements:{}, bankReconciliationDrafts:{}, receipts:[{id:'KEEP',amount:12.5}], transfers:[], adjustments:[], gokwikSettlements:{KEEP:{posted:false}}, unknownFinancialState:{keep:true}, auditLog:[], auditSeq:0, ...extra};
  fs.writeFileSync(file,JSON.stringify(store)); return store;
}
function invoke(method, suffix = '', body = {}, role = 'admin', query = {nature:'SANKI'}) {
  let status = 200, result, failure, done = false;
  const req = {method,path:base+suffix,url:base+suffix,originalUrl:base+suffix,baseUrl:'',query,body,params:{},headers:{},get(){return '';},user:{username:role==='admin'?'prashant':'test-'+role,role,roles:[role]}};
  const res = {headersSent:false,status(c){status=c;return this;},json(v){result=v;done=true;this.headersSent=true;return this;},setHeader(){},getHeader(){},end(){done=true;}};
  router.handle(req,res,error=>{failure=error||Error('No matching route');done=true;});
  if(failure)throw failure; assert.equal(done,true); return {status,body:result};
}
function getRow(role='admin',nature='SANKI',name=account) {
  const response=invoke('GET','',{},role,{nature}); assert.equal(response.status,200,JSON.stringify(response));
  return response.body.rows.find(r=>r.account===name);
}
function staged(extra={}) {
  const row=getRow(); return invoke('POST','/stage',{nature:'SANKI',account,amount:'2345.67',effectiveDate:'2026-08-22',reason:'Verified original statement opening',revision:row.revision,...extra});
}
function apply(row,extra={},role='owner') { return invoke('POST','/apply',{nature:'SANKI',account,proposalId:row.proposal.id,revision:row.revision,acknowledge:true,...extra},role); }
function withoutDraftMetadata(store) { const copy=structuredClone(store); for(const key of ['openingBalanceProposals','auditLog','auditSeq'])delete copy[key]; return copy; }

test('catalogue is exact read-only and exposes recorded opening, not closing or staged values',()=>{
  seed(); const bytes=fs.readFileSync(file,'utf8'),row=getRow();
  assert.deepEqual(row.current,{amount:0,effectiveDate:''}); assert.equal(row.proposal,null);
  assert.equal(fs.readFileSync(file,'utf8'),bytes);
});
test('Admin stages amount/date/reason without financial postings or historical migrations',()=>{
  const before=seed(),response=staged(); assert.equal(response.status,200,JSON.stringify(response));
  assert.equal(response.body.stagedOnly,true); assert.equal(response.body.row.proposal.amount,2345.67);
  assert.equal(response.body.row.proposal.effectiveDate,'2026-08-22');
  const after=read(); assert.deepEqual(withoutDraftMetadata(after),withoutDraftMetadata(before));
  assert.equal(after.auditLog.at(-1).action,'OPENING_BALANCE_STAGED');
  assert.equal(getRow().current.amount,0);
});
test('only explicit Owner apply changes opening; no settlements, payments or reconciliation records change',()=>{
  seed(); const row=staged().body.row,before=read();
  assert.equal(apply(row,{},'admin').status,403); assert.equal(apply(row,{acknowledge:false}).status,400);
  assert.deepEqual(read(),before);
  const result=apply(row); assert.equal(result.status,200,JSON.stringify(result));
  const after=read(); assert.equal(after.openingBalances[account],2345.67); assert.equal(after.openingBalanceDates[account],'2026-08-22');
  for(const key of ['receipts','transfers','adjustments','expenses','bankStatements','bankReconciliationDrafts','gokwikSettlements','unknownFinancialState'])assert.deepEqual(after[key],before[key]);
  assert.equal(after.auditLog.at(-1).action,'OPENING_BALANCE_CHANGED');
  const bytes=fs.readFileSync(file,'utf8'); assert.equal(apply(row).status,409); assert.equal(fs.readFileSync(file,'utf8'),bytes,'retries never double apply');
});
test('missing, malformed, excess-precision and non-calendar dates are rejected without writes',()=>{
  for(const amount of ['',null,{},[],true,'NaN','Infinity','1e3','0x10','2345.671','1,000']) { seed(); const bytes=fs.readFileSync(file,'utf8'); assert.equal(staged({amount}).status,400,String(amount)); assert.equal(fs.readFileSync(file,'utf8'),bytes); }
  for(const effectiveDate of ['',null,'2026-02-30','2026-08-22extra','9999-01-01']) { seed(); assert.equal(staged({effectiveDate}).status,400); }
  for(const reason of ['', ' ', 'x'.repeat(1001)]) { seed(); assert.equal(staged({reason}).status,400); }
});
test('zero and negative opening balances are valid, not mistaken for missing data',()=>{
  for(const amount of ['0','-1234.56']) { seed(); const response=staged({amount}); assert.equal(response.status,200); assert.equal(response.body.row.proposal.amount,Number(amount)); }
});
test('active 0437 statement exposes staged carry-forward and prevents competing direct opening proposals',()=>{
  const draft={id:'BRD-TEST',nature:'SANKI',account,summary:{from:'2026-08-22',to:'2026-10-14'},openingResolution:{amount:2345.67,reason:'Original statement verified',by:'prashant'}};
  seed({bankReconciliationDrafts:{'BRD-TEST':draft}});
  const bytes=fs.readFileSync(file,'utf8'),row=getRow(); assert.equal(row.drafts[0].openingResolution.amount,2345.67); assert.match(row.reason,/active statement/i);
  assert.equal(staged().status,409); assert.equal(fs.readFileSync(file,'utf8'),bytes);
});
test('finalized bank and approved cash reconciliations lock initial openings even for Owner',()=>{
  for(const extra of [{bankStatements:{[account]:{reconciledThrough:'2026-10-14'}}},{bankStatements:{[account]:{imports:[{id:'FINAL'}]}}},{cashReconciliations:[{account,nature:'SANKI',status:'approved'}]},{cashReconciliations:[{account,nature:'SANKI',status:'pending'}]}]) {
    seed(extra); const bytes=fs.readFileSync(file,'utf8'); assert.equal(getRow('owner').locked,true); assert.equal(staged().status,409); assert.equal(fs.readFileSync(file,'utf8'),bytes);
  }
});
test('stale revisions, changed opening and newer proposals cannot be overwritten or applied',()=>{
  seed(); const old=getRow(),first=staged().body.row;
  assert.equal(staged({revision:old.revision}).status,409);
  staged({amount:'8000'}); assert.equal(apply(first).status,409);
  const row=getRow(),store=read(); store.openingBalances[account]=1; fs.writeFileSync(file,JSON.stringify(store));
  const bytes=fs.readFileSync(file,'utf8'); assert.equal(apply(row).status,409); assert.equal(fs.readFileSync(file,'utf8'),bytes);
});
test('statement created or finalized after staging blocks Owner apply without destroying draft decisions',()=>{
  for(const mutate of [s=>{s.bankReconciliationDrafts.NEW={id:'NEW',account,nature:'SANKI'};},s=>{s.bankStatements[account]={lastReconciliation:{closingBalance:12}};}]) {
    seed(); const row=staged().body.row,store=read(); mutate(store); fs.writeFileSync(file,JSON.stringify(store));
    const bytes=fs.readFileSync(file,'utf8'); assert.equal(apply(row).status,409); assert.equal(fs.readFileSync(file,'utf8'),bytes);
  }
});
test('entity scope, private books, hidden accounts and role restrictions cannot be bypassed',()=>{
  seed();
  for(const role of ['claimant','personal_claimant','accounting','samast_accounting','viewer']) for(const [method,suffix] of [['GET',''],['POST','/stage'],['POST','/apply']]) assert.equal(invoke(method,suffix,{},role).status,403);
  assert.equal(invoke('GET','',{},'admin',{nature:'PERSONAL'}).status,403);
  assert.equal(staged({nature:'PERSONAL'}).status,403); assert.equal(staged({nature:'typo'}).status,403); assert.equal(staged({account:'Not an assigned account'}).status,403);
});
test('SAMAST and Owner PERSONAL proposals use separate entity opening maps',()=>{
  seed();
  for(const nature of ['SAMAST','PERSONAL']) {
    const ownerRows=invoke('GET','',{},'owner',{nature}); assert.equal(ownerRows.status,200);
    const row=ownerRows.body.rows[0]; assert.ok(row);
    const response=invoke('POST','/stage',{nature,account:row.account,amount:'99.50',effectiveDate:'2026-08-22',reason:'Verified entity opening',revision:row.revision},'owner'); assert.equal(response.status,200,JSON.stringify(response));
    const result=invoke('POST','/apply',{nature,account:row.account,proposalId:response.body.row.proposal.id,revision:response.body.row.revision,acknowledge:true},'owner'); assert.equal(result.status,200,JSON.stringify(result));
    assert.equal(read().openingBalancesByNature[nature][row.account],99.5); assert.equal(read().openingBalances[account],0);
  }
});
test('unreadable store fails closed and is not replaced with a blank financial file',()=>{
  fs.writeFileSync(file,'{broken'); const response=invoke('GET'); assert.equal(response.status,503); assert.equal(fs.readFileSync(file,'utf8'),'{broken');
});
test('failed atomic writes preserve the original accounting file and permit safe retry',()=>{
  seed(); const bytes=fs.readFileSync(file,'utf8'),rename=fs.renameSync; let result;
  fs.renameSync=()=>{throw Error('Synthetic write failure');}; try { result=staged(); } finally {fs.renameSync=rename;}
  assert.equal(result.status,503); assert.equal(fs.readFileSync(file,'utf8'),bytes); assert.equal(staged().status,200);
});
