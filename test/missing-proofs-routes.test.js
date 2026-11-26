'use strict';

const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'missing-proofs-route-'));process.env.DATA_PATH=path.join(dir,'data.json');
const fixtureSource=require.resolve('../modules/rental-source.json');require.cache[fixtureSource]={id:fixtureSource,filename:fixtureSource,loaded:true,exports:{id:'SYNTHETIC-MISSING-PROOFS',rows:[]}};
const file=path.join(dir,'expenses.json'),proofDir=path.join(dir,'expense-proofs');fs.mkdirSync(proofDir,{recursive:true});fs.writeFileSync(file,'{}');
const {router}=require('../modules/expenses');
test.after(()=>fs.rmSync(dir,{recursive:true,force:true}));

function expense(){return{id:'EX-PROOF',nature:'SANKI',date:'2098-11-26',vendor:'Demo Vendor',particulars:'Demo expense',ledger:'Props Expense',amount:1200,status:'paid',approvedAt:'2098-11-26T10:00:00.000Z',paidAmount:1200,account:'Axis Bank 3448',billPhoto:'',billPhotos:[],payments:[{id:'PAY-001',date:'2098-11-26',account:'Axis Bank 3448',amount:1200,bankReconciliationDraft:'BRD-FINAL',bankReconciliationRow:'bank-0'}]};}
function seed(){fs.writeFileSync(file,JSON.stringify({expenses:{'EX-PROOF':expense()},auditLog:[],auditSeq:0}));}
function invoke(method,route,{body={},query={},role='owner'}={}){return new Promise((resolve,reject)=>{let status=200,done=false;const finish=value=>{if(!done){done=true;resolve({status,body:value});}},req={method,path:route,url:route,originalUrl:route,body,query,params:{},headers:{},get(){return'';},user:{role,roles:[role],username:'synthetic-'+role,permissions:{personalAccounting:role==='owner'}}},headers={},res={statusCode:200,status(n){status=n;this.statusCode=n;return this;},setHeader(k,v){headers[k]=v;},getHeader(k){return headers[k];},removeHeader(k){delete headers[k];},json(value){finish(value);return this;},send(value){finish(value);return this;},end(value){finish(value);return this;},once(){return this;}};router.handle(req,res,error=>error?reject(error):finish(undefined));});}

test('queue lists the exact missing bill and hides bank-evidenced payment proof',async()=>{
  seed();const result=await invoke('GET','/api/expenses/missing-proofs',{query:{nature:'SANKI'}});
  assert.equal(result.status,200,JSON.stringify(result.body));assert.equal(result.body.count,1);assert.equal(result.body.gapCount,1);assert.equal(result.body.rows[0].gaps[0].kind,'bill');assert.equal(result.body.rows[0].gaps.some(gap=>gap.kind==='payment'),false);
  assert.equal((await invoke('GET','/api/expenses/missing-proofs',{role:'staff'})).status,403);
});

test('late proof attachment is append-only, audited and requires a stored upload',async()=>{
  seed();const missing='/api/expenses/photo/missing.jpg',denied=await invoke('POST','/api/expenses/EX-PROOF/proofs',{body:{kind:'bill',proofs:[missing]}});assert.equal(denied.status,400);
  const before=JSON.parse(fs.readFileSync(file)),name='proof-existing.jpg',url='/api/expenses/photo/'+name;fs.writeFileSync(path.join(proofDir,name),'synthetic proof');
  const attached=await invoke('POST','/api/expenses/EX-PROOF/proofs',{body:{kind:'bill',proofs:[url],note:'Uploaded after reconciliation'}});assert.equal(attached.status,200,JSON.stringify(attached.body));assert.equal(attached.body.remaining,0);
  const after=JSON.parse(fs.readFileSync(file)),old=before.expenses['EX-PROOF'],saved=after.expenses['EX-PROOF'];
  assert.deepEqual({amount:saved.amount,date:saved.date,account:saved.account,ledger:saved.ledger,status:saved.status,paidAmount:saved.paidAmount,payments:saved.payments},{amount:old.amount,date:old.date,account:old.account,ledger:old.ledger,status:old.status,paidAmount:old.paidAmount,payments:old.payments});
  assert.deepEqual(saved.billPhotos,[url]);assert.equal(saved.proofHistory.length,1);assert.ok(after.auditLog.some(entry=>entry.action==='PROOF_ATTACHED'&&entry.subjectId==='EX-PROOF'));
  assert.equal((await invoke('POST','/api/expenses/EX-PROOF/proofs',{body:{kind:'bill',proofs:[url]}})).status,409);
});

test('the Accounting group exposes the compact Missing Proofs UI',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
  assert.match(html,/data-t="proofs"[^>]*>Missing proofs/);assert.match(html,/\/api\/expenses\/missing-proofs/);assert.match(html,/does not change the amount, date, account, category or reconciliation/);assert.match(html,/data-missing-proof/);
});
