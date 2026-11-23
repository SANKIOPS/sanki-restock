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
test('7883 cards and ledger use bank closing instead of one-entity arithmetic while balance review is pending',async t=>{
 const before=fs.readFileSync(file,'utf8');t.after(()=>fs.writeFileSync(file,before));
 const s=structuredClone(baseline),account='IndusInd Bank 7883',pending={id:'BST-PENDING',draftId:'DRAFT-PENDING',from:'2026-11-14',to:'2026-11-15',balanceReconciled:false,closingBalanceDeferred:true,balanceDifference:-328232,statementSummary:{from:'2026-11-14',to:'2026-11-15',openingBalance:119909.35,closingBalance:109402.35,totalDebits:101000,totalCredits:90493,validated:true}};
 Object.assign(s,{expenses:{},receipts:[],transfers:[],adjustments:[],vendorAdvances:[],bankMovementAuthority:{},bankDateOverrides:{},bankReconciliationDrafts:{},bankStatements:{[account]:{reconciledThrough:'2026-11-15',lastReconciliation:{through:'2026-11-15',closingBalance:109402.35,balanceReconciled:false,closingBalanceDeferred:true,balanceDifference:-328232,draftId:'DRAFT-PENDING'},imports:[pending],transactions:{}}},physicalBankOpeningBalances:{[account]:111710.4}});
 s.receipts=[{id:'REC-RENT',nature:'SAMAST',account,date:'2026-11-18',amount:136200,receiptType:'rental_income'}];
 s.expenses.PERSONAL={id:'PERSONAL',nature:'PERSONAL',status:'paid',approvedAt:'2026-11-18',date:'2026-11-18',amount:80892,ledger:'Personal',payments:[{id:'PAY-1',account,date:'2026-11-18',amount:80892}]};
 fs.writeFileSync(file,JSON.stringify(s));
 const ledger=await invoke('/api/expenses/account-ledger',{nature:'PERSONAL',account,to:'2026-11-18'});assert.equal(ledger.status,200,JSON.stringify(ledger.body));assert.equal(ledger.body.balance,164710.35);assert.equal(ledger.body.bankVerified,false);assert.match(ledger.body.balanceBasis,/ledger balance pending/i);
 const balances=await invoke('/api/expenses/balances',{nature:'PERSONAL',to:'2026-11-18'});assert.equal(balances.status,200,JSON.stringify(balances.body));const card=balances.body.accounts.find(row=>row.name===account);assert.equal(card.balance,164710.35);assert.equal(card.physicalBalance,true);
});
test('7883 cards use the latest validated saved preview when an older opening conflict blocks finalization',async t=>{
 const before=fs.readFileSync(file,'utf8');t.after(()=>fs.writeFileSync(file,before));
 const s=structuredClone(baseline),account='IndusInd Bank 7883',pending={id:'BST-PENDING',draftId:'DRAFT-PENDING',from:'2026-11-14',to:'2026-11-15',balanceReconciled:false,closingBalanceDeferred:true,balanceDifference:-328232,statementSummary:{from:'2026-11-14',to:'2026-11-15',openingBalance:119909.35,closingBalance:109402.35,totalDebits:101000,totalCredits:90493,validated:true}};
 Object.assign(s,{expenses:{},receipts:[],transfers:[],adjustments:[],vendorAdvances:[],bankMovementAuthority:{},bankDateOverrides:{},bankStatements:{[account]:{reconciledThrough:'2026-11-15',lastReconciliation:{through:'2026-11-15',closingBalance:109402.35,balanceReconciled:false,closingBalanceDeferred:true,balanceDifference:-328232,draftId:'DRAFT-PENDING'},imports:[pending],transactions:{}}},physicalBankOpeningBalances:{[account]:111710.4}});
 s.bankReconciliationDrafts={'BRD-CURRENT':{id:'BRD-CURRENT',nature:'SAMAST',account,transactions:[],resolutions:{},summary:{from:'2026-11-16',to:'2026-11-18',openingBalance:109402.35,totalDebits:80892,totalCredits:136200,closingBalance:164710.35,validated:true}}};
 s.receipts=[{id:'REC-RENT',nature:'SAMAST',account,date:'2026-11-18',amount:136200,receiptType:'rental_income'}];
 s.expenses.PERSONAL={id:'PERSONAL',nature:'PERSONAL',status:'paid',approvedAt:'2026-11-18',date:'2026-11-18',amount:4160,ledger:'Personal',payments:[{id:'PAY-1',account,date:'2026-11-18',amount:4160}]};
 fs.writeFileSync(file,JSON.stringify(s));
 const ledger=await invoke('/api/expenses/account-ledger',{nature:'PERSONAL',account,to:'2026-11-23'});assert.equal(ledger.status,200,JSON.stringify(ledger.body));assert.equal(ledger.body.balance,164710.35);assert.equal(ledger.body.reconciledThrough,'2026-11-18');assert.match(ledger.body.balanceBasis,/temporary bank statement/i);
 const balances=await invoke('/api/expenses/balances',{nature:'SAMAST',to:'2026-11-23'});assert.equal(balances.status,200,JSON.stringify(balances.body));const card=balances.body.accounts.find(row=>row.name===account);assert.equal(card.balance,164710.35);assert.equal(card.reconciledThrough,'2026-11-18');assert.match(card.reconciliationIssues.map(issue=>issue.message).join(' '),/final reconciliation is still pending/i);
});
test('compact report reads do not migrate or write financial source and reject unauthorized roles',async()=>{
 const before=fs.readFileSync(file,'utf8');const result=await invoke('/api/expenses/compact-report',{entity:'SAMAST',from:'2026-08-22',to:'2026-11-13'});assert.equal(result.status,200,JSON.stringify(result.body));assert.equal(result.body.totals.income,100000);assert.equal(fs.readFileSync(file,'utf8'),before);
 const forbidden=await invoke('/api/expenses/compact-report',{entity:'PERSONAL',from:'2026-08-22',to:'2026-11-13'},{role:'accounting',roles:['accounting'],username:'staff',permissions:{}});assert.equal(forbidden.status,403);
 const personal=await invoke('/api/expenses/compact-report',{entity:'PERSONAL',from:'2026-08-22',to:'2026-11-13'},{role:'admin',roles:['admin'],username:'admin',permissions:{}});assert.equal(personal.status,403);
});
test('Samast report refresh reads latest payroll file, counts linked salaries once and never writes source or balances',async()=>{
 const query={entity:'SAMAST',from:'2026-08-22',to:'2026-11-13'},salaryFile=path.join(dir,'salary-samast.json');
 const raw=fs.readFileSync(file,'utf8'),empty=await invoke('/api/expenses/compact-report',query);
 const payroll={employees:{},salaryPayments:[{id:'LIVE-SAL',date:'2026-10-15',amount:1200,employeeName:'Samast guard',account:'IndusInd Bank 7883',entity:'SAMAST'}]};
 fs.writeFileSync(salaryFile,JSON.stringify(payroll));const paid=await invoke('/api/expenses/compact-report',query);
 assert.equal(paid.body.samastProfit.totals.salary,1200);assert.equal(paid.body.samastProfit.totals.expense,empty.body.samastProfit.totals.expense);
 payroll.salaryPayments[0].amount=1300;fs.writeFileSync(salaryFile,JSON.stringify(payroll));const fresh=await invoke('/api/expenses/compact-report',query);
 assert.equal(fresh.body.samastProfit.totals.salary,1300);assert.equal(fresh.body.samastProfit.entries.filter(e=>e.profitKind==='salary').length,1);
 assert.equal(fs.readFileSync(file,'utf8'),raw);assert.equal(fs.readFileSync(salaryFile,'utf8'),JSON.stringify(payroll));
});
