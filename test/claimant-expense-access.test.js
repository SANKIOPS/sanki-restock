'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const policy=require('../modules/expense-claimant-policy');
const root=fs.mkdtempSync(path.join(__dirname,'.tmp-claimant-access-'));
process.env.DATA_PATH=path.join(root,'data.json');
for(const [env,file] of Object.entries({ORDERS_PATH:'orders.json',SALES_PATH:'sales.json',PROCUREMENT_PATH:'procurement.json',INCENTIVES_PATH:'incentives.json',VELOCITY_REMITTANCE_PATH:'velocity.json'}))process.env[env]=path.join(root,file);
for(const file of ['expenses.json','orders.json','sales.json','procurement.json','incentives.json','velocity.json','salary.json','credit-cards.json'])fs.writeFileSync(path.join(root,file),'{}');
const {router,telegramApi}=require('../modules/expenses'),express=require('express');
const expenseFile=path.join(root,'expenses.json');
let server,base;
const user=(roles=['claimant'],username='claimant-one')=>({username,roles,role:roles[0]});
async function request(method,route,{body,roles=['claimant'],username='claimant-one'}={}){
  const response=await fetch(base+route,{method,headers:{'Content-Type':'application/json','x-test-roles':roles.join(','),'x-test-user':username},body:body===undefined?undefined:JSON.stringify(body)});
  const raw=await response.text();let data;try{data=JSON.parse(raw);}catch{data=raw;}return{status:response.status,body:data};
}
test.before(async()=>{
  const app=express();app.use(express.json());app.use((req,res,next)=>{req.user=user(String(req.get('x-test-roles')||'claimant').split(','),req.get('x-test-user')||'claimant-one');next();});app.use(router);app.get('/other-module',(req,res)=>res.json({success:true}));
  server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));base='http://127.0.0.1:'+server.address().port;
  await request('GET','/api/expenses/config',{roles:['owner']});
});
test.after(async()=>{if(server)await new Promise(resolve=>server.close(resolve));fs.rmSync(root,{recursive:true,force:true});});
const expenseBody=extra=>Object.assign({nature:'SANKI',vendor:'Synthetic Vendor',particulars:'Synthetic expense',amount:100,billPhoto:'/api/expenses/photo/own-bill.jpg',paymentType:'Cash'},extra);

test('India business date is authoritative before/after midnight and accepts no timestamp/date-prefix tricks',()=>{
  const actor={user:user()},before='2026-10-15T18:29:59.999Z',after='2026-10-15T18:30:00.000Z';
  assert.equal(policy.submissionDate(actor,undefined,before).date,'2026-10-15');assert.equal(policy.submissionDate(actor,undefined,after).date,'2026-10-16');
  for(const value of ['2026-10-15','2026-10-17','2026-10-16T12:00:00Z',' 2026-10-16',0,['2026-10-16','2026-10-15']])assert.ok(policy.submissionDate(actor,value,after).error,JSON.stringify(value));
  assert.equal(policy.submissionDate(actor,'2026-10-16',after).date,'2026-10-16');
  for(const role of ['admin','owner','accounting','samast_accounting']){
    const req={user:user(['claimant',role])};assert.equal(policy.claimantOnly(req),false);assert.equal(policy.submissionDate(req,'2026-09-01',after).date,'2026-09-01');
  }
  assert.equal(policy.claimantOnly({user:user(['personal_claimant','inventory'])}),true);
});

test('claimants receive today-only config and can create pending bills only for the current India date',async()=>{
  const config=(await request('GET','/api/expenses/config')).body;
  assert.equal((await request('GET','/other-module')).status,200,'expense restrictions do not block unrelated modules');
  assert.equal(config.claimantOnly,true);assert.equal(config.canApprove,false);assert.equal(config.expenseDatePolicy,'today_only');assert.equal(config.businessTimeZone,'Asia/Kolkata');assert.equal(config.businessDate,policy.businessDate());
  const before=fs.readFileSync(expenseFile,'utf8');
  for(const date of ['2000-01-01','2099-01-01',config.businessDate+'T01:00:00Z']){
    const out=await request('POST','/api/expenses',{body:expenseBody({date})});assert.equal(out.status,400);assert.equal(out.body.businessDate,config.businessDate);assert.match(out.body.error,/only for today/);assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
  }
  for(const date of [undefined,config.businessDate]){
    const out=await request('POST','/api/expenses',{body:expenseBody({date,paidAlready:true,personalPaymentProof:'/api/expenses/photo/own-personal.jpg',status:'approved',approvedBy:'spoof',createdBy:'other'})});
    assert.equal(out.status,200,JSON.stringify(out.body));assert.equal(out.body.expense.date,config.businessDate);assert.equal(out.body.expense.payments[0].date,config.businessDate);assert.equal(out.body.expense.status,'pending');assert.equal(out.body.expense.approvedBy,null);assert.equal(out.body.expense.createdBy,'claimant-one');
  }
  const personal=await request('POST','/api/expenses',{roles:['personal_claimant'],body:expenseBody({nature:'PERSONAL',date:'2000-01-01'})});assert.equal(personal.status,400);
  const named=(await request('GET','/api/expenses/config',{username:'prashant'})).body;assert.equal(named.claimantOnly,true);assert.equal(named.canVerifyCounterCash,false);assert.equal(named.canManageCategories,false);assert.equal(named.canManageVendors,false);
});

test('Telegram dispatch uses the same claimant route and current-date restrictions',()=>{
  const before=fs.readFileSync(expenseFile,'utf8');
  const actor={username:'prashant',roles:['claimant']};
  const backdated=telegramApi('POST','/api/expenses',actor,{body:expenseBody({date:'2000-01-01'})});assert.equal(backdated.success,false);assert.equal(backdated.status,400);
  const category=telegramApi('POST','/api/expenses/custom-ledgers',actor,{body:{name:'Unauthorized category',group:'General',type:'variable'}});assert.equal(category.status,403);
  const edit=telegramApi('POST','/api/expenses/:id',actor,{params:{id:'EX-1'},body:{date:'2000-01-01'}});assert.equal(edit.status,403);
  assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
});

test('claimant direct mutation and accounting-workspace requests fail before any store writes',async()=>{
  const made=await request('POST','/api/expenses',{body:expenseBody()});assert.equal(made.status,200);const id=made.body.expense.id;
  const before=fs.readFileSync(expenseFile,'utf8');
  for(const [method,route] of [['POST','/'+id],['POST','/'+id+'/approve'],['POST','/'+id+'/reject'],['POST','/'+id+'/pay'],['POST','/'+id+'/reimburse'],['DELETE','/'+id],['DELETE','/'+id+'/payments/PAY-001'],['POST','/bank-statements/finalize'],['POST','/requests'],['GET','/vendors'],['GET','/account-ledger'],['GET','/credit-cards'],['GET','/audit-log']]){
    const out=await request(method,'/api/expenses'+route,{body:method==='GET'?undefined:{date:'2000-01-01',vendor:'spoof',reason:'Unauthorized mutation',amount:1}});assert.equal(out.status,403,method+' '+route);assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
  }
  const detail=(await request('GET','/api/expenses/'+id)).body.expense;assert.equal(detail.canEditMetadata,false);assert.equal(detail.canManageExpense,false);
});

test('old own expenses, lifecycle status and single/array payment and reimbursement proofs remain readable, not other claimants’ records',async()=>{
  const s=JSON.parse(fs.readFileSync(expenseFile,'utf8')),id='EX-99001',other='EX-99002';
  const proofs=['own-bill.jpg','own-payment.jpg','own-array.jpg','own-reimbursement.jpg'];for(const proof of proofs.concat('other.jpg'))fs.writeFileSync(path.join(root,'expense-proofs',proof),'synthetic proof');
  s.expenses[id]={id,date:'2026-09-01',nature:'SANKI',createdBy:'claimant-one',claimant:'claimant-one',status:'paid',approvedAt:'2026-09-02',approvedBy:'approver',amount:100,paidAmount:100,paidAlready:true,fundedBy:'claimant',personalPaidAmount:100,reimbursementAmount:100,reimbursementStatus:'reimbursed',billPhoto:'/api/expenses/photo/own-bill.jpg',paymentProofs:[],payments:[{id:'PAY-001',amount:100,proof:'/api/expenses/photo/own-payment.jpg',proofs:['/api/expenses/photo/own-array.jpg']}],reimbursementPayments:[{id:'REIM-001',amount:100,proof:'/api/expenses/photo/own-reimbursement.jpg'}]};
  s.expenses[other]={...s.expenses[id],id:other,createdBy:'claimant-two',claimant:'claimant-two',billPhoto:'/api/expenses/photo/other.jpg',payments:[],reimbursementPayments:[]};fs.writeFileSync(expenseFile,JSON.stringify(s));
  const detail=await request('GET','/api/expenses/'+id);assert.equal(detail.status,200);assert.equal(detail.body.expense.status,'paid');assert.equal(detail.body.expense.reimbursementStatus,'reimbursed');assert.equal(detail.body.expense.approvedBy,'approver');
  const list=await request('GET','/api/expenses/list?claimant=claimant-two');assert.equal(list.status,200);assert.equal(list.body.expenses.length,0);
  assert.equal((await request('GET','/api/expenses/'+other)).status,403);
  for(const proof of proofs)assert.equal((await request('GET','/api/expenses/photo/'+proof)).status,200,proof);
  assert.equal((await request('GET','/api/expenses/photo/other.jpg')).status,403);
});

test('separately authorized approvers retain backdated entries and approval, with proof gates intact',async()=>{
  const config=(await request('GET','/api/expenses/config',{roles:['admin']})).body;
  assert.equal(config.claimantOnly,false);assert.equal(config.expenseDatePolicy,'any');
  const ledger=config.ledgersByNature.SANKI[0].name;
  const made=await request('POST','/api/expenses',{roles:['admin'],body:expenseBody({date:'2026-09-01',ledger})});assert.equal(made.status,200,JSON.stringify(made.body));const id=made.body.expense.id;
  const detail=(await request('GET','/api/expenses/'+id,{roles:['admin']})).body.expense;assert.equal(detail.canManageExpense,true);assert.equal(detail.canEditMetadata,true);
  assert.equal((await request('POST','/api/expenses/'+id+'/approve',{roles:['accounting']})).status,200);
  const payables=(await request('GET','/api/expenses/pending-payments',{roles:['admin']})).body.expenses;assert.equal(payables.find(e=>e.id===id).canManageExpense,true,'payable cards retain authorized actions');
  const noProof=await request('POST','/api/expenses/'+id+'/pay',{roles:['admin'],body:{account:'Counter Cash'}});assert.equal(noProof.status,400);assert.match(noProof.body.error,/proof/i);
});

test('actual expense-card renderer is read-only for claimants and displays rejection/reimbursement status and every payment proof',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  const sandbox={cfg:{claimantOnly:true,canApprove:false},fmt:n=>'INR '+n,esc:s=>String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])),refundExpenseHistory:()=>''};
  vm.createContext(sandbox);
  for(const name of ['expenseOperatorAllowed','proofGallery','expenseTrackingDetails','expenseActions','paymentHistory','expenseCard']){
    const start=html.indexOf('    function '+name+'(');assert.ok(start>=0,name);const next=html.indexOf('\n    function ',start+5);vm.runInContext(html.slice(start,next<0?undefined:next),sandbox);
  }
  const expense={id:'EX-1',nature:'SANKI',status:'paid',date:'2026-09-01',amount:100,paidAmount:100,billPhoto:'/bill.jpg',paymentProofs:[],paidAlready:true,fundedBy:'claimant',personalPaidAmount:100,reimbursementStatus:'reimbursed',reimbursementAmount:100,payments:[{id:'PAY-1',amount:100,proof:'/payment.jpg'}],reimbursementPayments:[{id:'REIM-1',amount:100,proof:'/reimbursement.jpg'}],canManageExpense:true};
  // A stale overly-permissive row flag may not override the claimant session.
  const rendered=sandbox.expenseCard(expense,1);assert.match(rendered,/Approval & payment status/);assert.match(rendered,/reimbursed/);assert.match(rendered,/\/payment.jpg/);assert.match(rendered,/\/reimbursement.jpg/);assert.doesNotMatch(rendered,/>Approve<|>Reject<|>Delete<|>Unpay<|>Edit<|onclick="openPay/);
  assert.match(sandbox.expenseCard({...expense,status:'rejected',approvedAt:null,rejectReason:'Wrong <bill>'},1),/Rejected — Wrong &lt;bill&gt;/);
  assert.match(sandbox.expenseTrackingDetails({...expense,refundAdjustedPersonalPaid:60,reimbursementAmount:40}),/INR 20 remaining/);
  assert.equal(sandbox.expenseOperatorAllowed(expense),false);sandbox.cfg={canApprove:true,claimantOnly:false};assert.equal(sandbox.expenseOperatorAllowed(expense),true);assert.equal(sandbox.expenseOperatorAllowed({...expense,canManageExpense:false}),false);
  assert.match(html,/readonly min="/);assert.match(html,/cfg\.claimantOnly&&tab!=='log'&&tab!=='list'/);assert.match(html,/button\.dataset\.t!=='log'&&button\.dataset\.t!=='list'/);assert.match(html,/style="visibility:hidden"/);
  assert.match(html,/querySelector\('\.tabs-primary'\)\.appendChild\(ownExpensesTab\)/,'claimants keep All Expenses visible outside the collapsed accounting group');
  for(const script of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))new vm.Script(script[1]);
});
