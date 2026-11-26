'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'personal-report-routes-'));process.env.DATA_PATH=path.join(dir,'data.json');
const file=path.join(dir,'expenses.json');fs.writeFileSync(file,'{}');
const {router}=require('../modules/expenses');
const owner={role:'owner',roles:['owner'],username:'owner',permissions:{}},admin={role:'admin',roles:['admin'],username:'admin',permissions:{personalAccounting:true}};
function invoke(method,route,{query={},body={},user=owner}={}){return new Promise((resolve,reject)=>{let status=200;const req={method,path:route,url:route,originalUrl:route,baseUrl:'',query,body,headers:{'content-type':'application/json'},get:()=>'',user,params:{}};const res={statusCode:200,headersSent:false,status(n){status=n;return this;},set(){return this;},setHeader(){},getHeader(){},removeHeader(){},json(value){resolve({status,body:value});return this;},end(){resolve({status});}};router.handle(req,res,error=>error?reject(error):resolve({status:404}));});}
test.before(async()=>{await invoke('GET','/api/expenses/config');for(const [name,value] of Object.entries({'orders.json':{orders:{}},'procurement.json':{pos:{}},'salary.json':{employees:{},salaryPayments:[]},'salary-samast.json':{employees:{},salaryPayments:[]}}))fs.writeFileSync(path.join(dir,name),JSON.stringify(value));const s=JSON.parse(fs.readFileSync(file));s.expenses={'EX-PERSONAL':{id:'EX-PERSONAL',date:'2026-10-17',nature:'PERSONAL',status:'paid',approvedAt:'2026-10-17',amount:50000,ledger:'Food',particulars:'Personal meal',payments:[{id:'PAY-001',date:'2026-10-17',amount:50000,account:'ICICI Bank 0993'}]}};s.receipts=[{id:'REC-LOAN',date:'2026-10-15',nature:'PERSONAL',account:'ICICI Bank 0993',amount:100000,source:'Synthetic lender',receiptType:'loan_received'}];s.loanRepayments=[];s.transfers=[];fs.writeFileSync(file,JSON.stringify(s));});
test.after(()=>fs.rmSync(dir,{recursive:true,force:true}));

test('personal report settings are owner-only and never edit financial movements',async()=>{
 const original=JSON.parse(fs.readFileSync(file)),financial={expense:{amount:original.expenses['EX-PERSONAL'].amount,date:original.expenses['EX-PERSONAL'].date,paymentAmount:original.expenses['EX-PERSONAL'].payments[0].amount,paymentDate:original.expenses['EX-PERSONAL'].payments[0].date,account:original.expenses['EX-PERSONAL'].payments[0].account},loan:{amount:original.receipts[0].amount,date:original.receipts[0].date,account:original.receipts[0].account,nature:original.receipts[0].nature},transfers:original.transfers};
 assert.equal((await invoke('POST','/api/expenses/personal-report/budget',{body:{month:'2026-10',category:'Food',amount:25000},user:admin})).status,403);
 assert.equal((await invoke('POST','/api/expenses/personal-report/budget',{body:{month:'2026-10',category:'Food',amount:25000}})).status,200);
 assert.equal((await invoke('POST','/api/expenses/personal-report/source',{body:{movementId:'EX-PERSONAL/PAY-001',source:'SANKI',reason:'Owner confirmed'}})).status,200);
 assert.equal((await invoke('POST','/api/expenses/personal-report/loan-owner',{body:{loanId:'REC-LOAN',owner:'SANKI',reason:'Business loan'}})).status,200);
 const saved=JSON.parse(fs.readFileSync(file));assert.equal(saved.personalReporting.budgets['2026-10'].Food,25000);assert.equal(saved.personalReporting.movementSources['EX-PERSONAL/PAY-001'].source,'SANKI');assert.equal(saved.personalReporting.loanOwners['REC-LOAN'],'SANKI');
 assert.deepEqual({expense:{amount:saved.expenses['EX-PERSONAL'].amount,date:saved.expenses['EX-PERSONAL'].date,paymentAmount:saved.expenses['EX-PERSONAL'].payments[0].amount,paymentDate:saved.expenses['EX-PERSONAL'].payments[0].date,account:saved.expenses['EX-PERSONAL'].payments[0].account},loan:{amount:saved.receipts[0].amount,date:saved.receipts[0].date,account:saved.receipts[0].account,nature:saved.receipts[0].nature},transfers:saved.transfers},financial);
});

test('compact PERSONAL response exposes the simple personal workbook',async()=>{
 const response=await invoke('GET','/api/expenses/compact-report',{query:{entity:'PERSONAL',from:'2026-10-01',to:'2026-10-31'}});assert.equal(response.status,200,JSON.stringify(response.body));
 assert.equal(response.body.personal.spending.total,50000);assert.equal(response.body.personal.moneyTaken.bySource.SANKI,50000);assert.equal(response.body.personal.spending.budgets[0].budget,25000);assert.equal(response.body.personal.review.rows.length,0);
 assert.match(response.body.personal.basis,/counted once/i);
});
