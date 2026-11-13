'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'shared-bank-report-'));process.env.DATA_PATH=path.join(dir,'data.json');
const file=path.join(dir,'expenses.json');fs.writeFileSync(file,'{}');
const {router}=require('../modules/expenses');
function invoke(route,query={},user={role:'owner',roles:['owner'],username:'owner',permissions:{}}){return new Promise((resolve,reject)=>{let status=200;const req={method:'GET',path:route,url:route,originalUrl:route,baseUrl:'',query,body:{},headers:{},get:()=>'',user};const res={statusCode:200,headersSent:false,status(n){status=n;return this;},set(){return this;},setHeader(){},getHeader(){},removeHeader(){},json(body){resolve({status,body});return this;},end(){resolve({status});}};router.handle(req,res,e=>e?reject(e):resolve({status:404}));});}
let baseline;
test.before(async()=>{await invoke('/api/expenses/config');baseline=JSON.parse(fs.readFileSync(file));for(const [name,value] of Object.entries({'orders.json':{orders:{}},'procurement.json':{pos:{}},'salary.json':{employees:{},salaryPayments:[]},'salary-samast.json':{employees:{},salaryPayments:[]}}))fs.writeFileSync(path.join(dir,name),JSON.stringify(value));});
test.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
test('both entity ledger routes and balance cards use one physical close and later movements',async()=>{
 const s=structuredClone(baseline),account='IndusInd Bank 7883';
 Object.assign(s,{expenses:{},receipts:[],transfers:[],adjustments:[],vendorAdvances:[],bankMovementAuthority:{},bankDateOverrides:{},bankReconciliationDrafts:{},bankStatements:{[account]:{reconciledThrough:'2026-10-19',lastReconciliation:{through:'2026-10-19',closingBalance:370197.82,reconciled:true},imports:[],transactions:{}}},physicalBankOpeningBalances:{[account]:111710.4}});
 s.receipts=[{id:'REC-TEST',nature:'SAMAST',account,date:'2026-10-15',amount:100000,receiptType:'rental_income'},{id:'REC-AFTER',nature:'PERSONAL',account,date:'2026-10-21',amount:20000,receiptType:'other_income'}];
 s.expenses.TEST={id:'TEST',nature:'SANKI',status:'paid',approvedAt:'2026-10-20',date:'2026-10-20',amount:5000,ledger:'Food',payments:[{id:'PAY-1',account,date:'2026-10-20',amount:5000}]};
 fs.writeFileSync(file,JSON.stringify(s));
 for(const nature of ['PERSONAL','SAMAST']){
  const result=await invoke('/api/expenses/account-ledger',{nature,account,to:'2026-11-13'});assert.equal(result.status,200,JSON.stringify(result.body));assert.equal(result.body.balance,385197.82);assert.equal(result.body.physicalBalance,true);assert.ok(result.body.entries.some(r=>r.id==='TEST/PAY-1'));
  const filtered=await invoke('/api/expenses/account-ledger',{nature,account,to:'2026-11-13',expenseNature:'PERSONAL'});assert.equal(filtered.body.balance,385197.82,'entity filter never changes physical balance');
  const balances=await invoke('/api/expenses/balances',{nature,to:'2026-11-13'});assert.equal(balances.body.accounts.find(a=>a.name===account).balance,385197.82);
 }
});
test('compact report reads do not migrate or write financial source and reject unauthorized roles',async()=>{
 const before=fs.readFileSync(file,'utf8');const result=await invoke('/api/expenses/compact-report',{entity:'SAMAST',from:'2026-08-22',to:'2026-11-13'});assert.equal(result.status,200,JSON.stringify(result.body));assert.equal(result.body.totals.income,100000);assert.equal(fs.readFileSync(file,'utf8'),before);
 const forbidden=await invoke('/api/expenses/compact-report',{entity:'PERSONAL',from:'2026-08-22',to:'2026-11-13'},{role:'accounting',roles:['accounting'],username:'staff',permissions:{}});assert.equal(forbidden.status,403);
 const personal=await invoke('/api/expenses/compact-report',{entity:'PERSONAL',from:'2026-08-22',to:'2026-11-13'},{role:'admin',roles:['admin'],username:'admin',permissions:{}});assert.equal(personal.status,403);
});
