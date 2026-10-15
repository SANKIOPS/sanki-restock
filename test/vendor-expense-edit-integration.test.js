'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const root=fs.mkdtempSync(path.join(__dirname,'.tmp-vendor-edit-'));
process.env.DATA_PATH=path.join(root,'data.json');
for(const [env,file] of Object.entries({ORDERS_PATH:'orders.json',SALES_PATH:'sales.json',PROCUREMENT_PATH:'procurement.json',INCENTIVES_PATH:'incentives.json',VELOCITY_REMITTANCE_PATH:'velocity.json'}))process.env[env]=path.join(root,file);
for(const file of ['expenses.json','orders.json','sales.json','procurement.json','incentives.json','velocity.json','salary.json','credit-cards.json'])fs.writeFileSync(path.join(root,file),'{}');
const {router,telegramApproveExpense}=require('../modules/expenses');
const expenseFile=path.join(root,'expenses.json');
invoke('GET','/api/expenses/config');
const baseline=JSON.parse(fs.readFileSync(expenseFile,'utf8'));
const clone=x=>JSON.parse(JSON.stringify(x)),read=()=>JSON.parse(fs.readFileSync(expenseFile,'utf8'));
function invoke(method,route,{body={},params={},query={},role='admin',username=role==='admin'?'prashant':'synthetic-'+role}={}){
  const layer=router.stack.find(x=>x.route&&x.route.path===route&&x.route.methods[method.toLowerCase()]);assert.ok(layer,route);
  const out={status:200},req={body,params,query,user:{username,role,roles:[role]},headers:{},get(){return'';}};
  layer.route.stack.at(-1).handle(req,{status(n){out.status=n;return this;},json(value){out.body=value;return this;}},()=>{out.status=404;});return out;
}
function seed({locked=true,personal=false}={}){
  const s=clone(baseline),id='EX-99001',date='2098-03-04',proof='/api/expenses/photo/synthetic.png';
  const e={id,nature:'SANKI',date,amount:200,requestedAmount:200,paidAmount:200,status:'paid',approvedAt:date+'T10:00:00Z',vendor:'Synthetic Vendor',particulars:'Original particulars',ledger:'Flowers',type:'variable',channel:'Shared',billPhoto:proof,billPhotos:[proof],qrPhoto:proof,paymentType:'UPI',createdBy:'synthetic-claimant',claimant:'synthetic-claimant',paidAlready:personal,account:'Axis Bank 3448',payments:[{id:'PAY-001',amount:200,date,account:personal?'Claimant Bank':'Axis Bank 3448',proof,proofs:[proof],personalFunds:personal,note:'Preserve this payment',paidAt:date+'T11:00:00Z'}],reimbursementPayments:personal?[{id:'REIM-001',date:'2098-03-06',amount:200,account:'Axis Bank 3448',proof,proofs:[proof]}]:[],reimbursementAmount:personal?200:0,reimbursementStatus:personal?'reimbursed':'not_applicable',purchasePaymentProof:personal?proof:'',purchasePaymentProofs:personal?[proof]:[],personalPaidAmount:personal?200:0};
  Object.assign(s,{expenses:{[id]:e},bankDateOverrides:locked?{[id+'/'+(personal?'REIM-001':'PAY-001')]:{bankDate:'2098-03-06',bankReference:'SYNTHETIC-LOCK'}}:{},bankReconciliationDrafts:{},bankStatements:{},cashReconciliations:[],vendorOpeningPayables:[],vendorAdvances:[],expenseRefunds:[],auditLog:[],auditSeq:0});
  fs.writeFileSync(expenseFile,JSON.stringify(s));return{id,s,e};
}
test.after(()=>fs.rmSync(root,{recursive:true,force:true}));

test('fresh detail exposes financial lock and enforces PERSONAL visibility without writes',()=>{
  const {id}=seed();const before=fs.readFileSync(expenseFile,'utf8');
  const got=invoke('GET','/api/expenses/:id',{params:{id}});assert.equal(got.status,200);assert.equal(got.body.expense.financialFieldsLocked,true);assert.equal(got.body.expense.canEditMetadata,true);assert.match(got.body.expense.financialLockReason,/reconcil/i);
  assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
  const s=read();s.expenses[id].nature='PERSONAL';fs.writeFileSync(expenseFile,JSON.stringify(s));
  assert.equal(invoke('GET','/api/expenses/:id',{params:{id},role:'admin'}).status,403);
  assert.equal(invoke('GET','/api/expenses/:id',{params:{id},role:'owner'}).status,200);
  assert.equal(invoke('GET','/api/expenses/:id',{params:{id:'EX-MISSING'}}).status,404);
});

test('detail endpoint does not shadow rental or credit-card workspaces in actual Express dispatch',async()=>{
  seed();const express=require('express'),http=require('node:http'),app=express();
  app.use((req,res,next)=>{req.user={username:'synthetic-owner',role:'owner',roles:['owner']};next();});
  app.use(router);app.use(require('../modules/credit-cards').router);
  const server=http.createServer(app);await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  try{
    for(const workspace of ['rentals','credit-cards']){
      const response=await fetch('http://127.0.0.1:'+server.address().port+'/api/expenses/'+workspace);
      assert.equal(response.status,200,workspace);assert.equal((await response.json()).success,true,workspace);
    }
  }finally{await new Promise(resolve=>server.close(resolve));}
});

test('amount and date cannot be changed on reconciled expenses even by Owner or direct API',()=>{
  for(const role of ['owner','admin'])for(const change of [{amount:201},{date:'2098-03-07'},{requestedAmount:201},{paymentAccount:'Counter Cash'}]){
    const {id}=seed(),before=fs.readFileSync(expenseFile,'utf8');
    const out=invoke('POST','/api/expenses/:id',{params:{id},role,body:{...change,editReason:'Attempted reconciled source change'}});
    assert.equal(out.status,409,role+JSON.stringify(change));assert.equal(out.body.financialFieldsLocked,true);assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
  }
});

test('metadata correction preserves all payment/reimbursement facts, status and bank evidence exactly',()=>{
  for(const personal of [false,true]){
    const {id}=seed({personal}),before=read(),out=invoke('POST','/api/expenses/:id',{params:{id},body:{vendor:'Corrected Vendor',particulars:'Corrected particulars',ledger:'Flowers',type:'running',editReason:'Correct descriptive entry without altering money'}});
    assert.equal(out.status,200,JSON.stringify(out.body));const after=read(),e=after.expenses[id],old=before.expenses[id];
    assert.equal(e.vendor,'Corrected Vendor');assert.equal(e.particulars,'Corrected particulars');assert.equal(e.type,'running');
    for(const key of ['amount','date','requestedAmount','paidAmount','payments','reimbursementPayments','reimbursementAmount','reimbursementStatus','personalPaidAmount','account','billPhotos','purchasePaymentProofs'])assert.deepEqual(e[key],old[key],key);
    for(const key of ['bankDateOverrides','bankStatements','bankReconciliationDrafts','vendorAdvances'])assert.deepEqual(after[key],before[key],key);
    assert.deepEqual(e.auditHistory.at(-1).changes.map(x=>x.field).sort(),['vendor','particulars','type'].sort());assert.equal(after.auditLog.at(-1).action,'EDITED');
    assert.equal(out.body.expense.financialFieldsLocked,true);
  }
});

test('a reconciled expense can move to another approved category without changing financial facts',()=>{
  const {id}=seed(),config=invoke('GET','/api/expenses/config').body;
  const category=config.ledgers.find(row=>row.name!=='Flowers');assert.ok(category,'an existing category is available');
  const before=read().expenses[id],out=invoke('POST','/api/expenses/:id',{params:{id},body:{ledger:category.name,type:category.type||'variable',editReason:'Correct approved category'}});
  assert.equal(out.status,200,JSON.stringify(out.body));const after=read().expenses[id];assert.equal(after.ledger,category.name);
  for(const key of ['amount','date','payments','reimbursementPayments','paidAmount'])assert.deepEqual(after[key],before[key],key);
});

test('finalized metadata edits require reason, preserve authorization and identical retries are no-ops',()=>{
  const {id}=seed(),before=fs.readFileSync(expenseFile,'utf8');
  assert.equal(invoke('POST','/api/expenses/:id',{params:{id},body:{particulars:'Changed'}}).status,400);
  assert.equal(invoke('POST','/api/expenses/:id',{params:{id},role:'claimant',body:{particulars:'Changed',editReason:'Not allowed'}}).status,403);
  assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
  const patch={particulars:'Corrected once',editReason:'Verified particulars correction'};
  assert.equal(invoke('POST','/api/expenses/:id',{params:{id},body:patch}).status,200);const once=fs.readFileSync(expenseFile,'utf8');
  assert.equal(invoke('POST','/api/expenses/:id',{params:{id},body:patch}).status,200);assert.equal(fs.readFileSync(expenseFile,'utf8'),once);
});

test('unreconciled Owner amount/date corrections retain the existing workflow',()=>{
  const {id}=seed({locked:false}),out=invoke('POST','/api/expenses/:id',{params:{id},role:'owner',body:{amount:225,date:'2098-03-05',editReason:'Unreconciled bill correction'}});
  assert.equal(out.status,200,JSON.stringify(out.body));assert.equal(read().expenses[id].amount,225);assert.equal(read().expenses[id].date,'2098-03-05');assert.equal(read().expenses[id].payments[0].amount,200);
});

test('unchanged legacy full financial payload preserves an unreconciled personal reimbursement',()=>{
  const {id,e}=seed({locked:false,personal:true}),before=read();
  const body={vendor:'Corrected Vendor',particulars:'Corrected without resetting reimbursement',ledger:e.ledger,type:e.type,editReason:'Descriptive correction',nature:e.nature,date:e.date,amount:String(e.amount),paidAlready:true,paymentType:e.paymentType,paymentAccount:e.account,personalAccount:e.payments[0].account,isInstallment:false,requestedAmount:e.amount,billPhoto:e.billPhoto,billPhotos:e.billPhotos,qrPhoto:e.qrPhoto,personalPaymentProof:e.purchasePaymentProof,personalPaymentProofs:e.purchasePaymentProofs,purchasePaymentProof:e.purchasePaymentProof,purchasePaymentProofs:e.purchasePaymentProofs};
  const out=invoke('POST','/api/expenses/:id',{params:{id},body,role:'owner'});assert.equal(out.status,200,JSON.stringify(out.body));
  const after=read().expenses[id];
  for(const key of ['payments','reimbursementPayments','reimbursementAmount','reimbursementStatus','paidAmount','personalPaidAmount','purchasePaymentProofs'])assert.deepEqual(after[key],before.expenses[id][key],key);
});

test('pending claimant metadata changes do not register an approved vendor and synthetic sources stay read-only',()=>{
  const {id}=seed({locked:false}),s=read();Object.assign(s.expenses[id],{status:'pending',paidAmount:0,approvedAt:null,payments:[]});fs.writeFileSync(expenseFile,JSON.stringify(s));
  const before=read();const out=invoke('POST','/api/expenses/:id',{params:{id},role:'claimant',username:'synthetic-claimant',body:{vendor:'Not an approved master vendor',particulars:'Pending correction'}});
  assert.equal(out.status,200,JSON.stringify(out.body));const after=read();assert.equal(after.expenses[id].vendor,'Not an approved master vendor');
  for(const key of ['vendors','vendorsByNature','vendorDirectory'])assert.deepEqual(after[key],before[key],key);
  for(const marker of ['salaryAdvanceId','nonExpenseReimbursement']){
    const s=read();s.expenses[id][marker]=marker==='salaryAdvanceId'?'ADV-SYNTHETIC':true;fs.writeFileSync(expenseFile,JSON.stringify(s));
    const before=fs.readFileSync(expenseFile,'utf8');assert.equal(invoke('GET','/api/expenses/:id',{params:{id},role:'owner'}).body.expense.canEditMetadata,false);
    assert.equal(invoke('POST','/api/expenses/:id',{params:{id},role:'owner',body:{particulars:'Invalid source correction'}}).status,409);assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
  }
});

test('Telegram approval cannot change a reconciled amount on a legacy pending expense',()=>{
  const {id}=seed(),s=read();s.expenses[id].status='pending';fs.writeFileSync(expenseFile,JSON.stringify(s));const before=fs.readFileSync(expenseFile,'utf8');
  const out=telegramApproveExpense(id,'synthetic-owner',{amount:201});assert.equal(out.success,false);assert.match(out.error,/reconcil|already paid/i);assert.equal(read().expenses[id].amount,200);
});

test('vendor-funded advance applications cannot silently become credit at another vendor',()=>{
  const {id}=seed(),s=read();s.expenses[id].vendorAdvanceApplications=[{id:'APP-1',vendorAdvanceId:'VA-SYNTHETIC',amount:200}];fs.writeFileSync(expenseFile,JSON.stringify(s));
  const before=fs.readFileSync(expenseFile,'utf8'),out=invoke('POST','/api/expenses/:id',{params:{id},role:'owner',body:{vendor:'Different real vendor',editReason:'Do not silently transfer vendor credit'}});
  assert.equal(out.status,409);assert.match(out.body.error,/advance.*Rename or merge/i);assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
  assert.equal(invoke('POST','/api/expenses/:id',{params:{id},role:'owner',body:{particulars:'Corrected details',editReason:'Metadata only'}}).status,200);
});

test('ledger payment editor cannot bypass grouped reconciliation or approved cash-period locks',()=>{
  for(const source of ['finalized_group','cash_period','reimbursement_group'])for(const changed of ['amount','date']){
    const personal=source==='reimbursement_group',{id}=seed({locked:false,personal}),s=read(),payment=personal?s.expenses[id].reimbursementPayments[0]:s.expenses[id].payments[0],reference=id+'/'+payment.id;
    if(source==='cash_period'){
      payment.account='Counter Cash';s.cashReconciliations=[{id:'CASH-SYNTHETIC',status:'approved',account:'Counter Cash',verifiedThrough:'2098-03-10'}];
    }else s.bankStatements={'SANKI|Axis Bank 3448':{imports:[{id:'BST-SYNTHETIC',reconciliationRows:[{linkedRecordIds:[reference],ledger:{id:'GROUP-SYNTHETIC',sourceIds:[reference]}}]}]}};
    fs.writeFileSync(expenseFile,JSON.stringify(s));const before=fs.readFileSync(expenseFile,'utf8');
    assert.equal(s.bankDateOverrides[reference],undefined,'no direct override is present');
    const out=invoke('PATCH','/api/expenses/ledger-entry',{role:'owner',body:{id:reference,kind:personal?'reimbursement':'expense',amount:changed==='amount'?201:payment.amount,date:changed==='date'?'2098-03-11':payment.date,reason:'Attempt to change linked payment'}});
    assert.equal(out.status,409,source+' '+changed);assert.equal(out.body.financialFieldsLocked,true);assert.match(out.body.error,/locked for every role/i);assert.equal(fs.readFileSync(expenseFile,'utf8'),before,'rejected correction does not write or mutate stored facts');
  }
});

test('ledger payment editor retains unreconciled corrections and same-value descriptive corrections',()=>{
  let seeded=seed({locked:false}),id=seeded.id,reference=id+'/PAY-001';
  let out=invoke('PATCH','/api/expenses/ledger-entry',{role:'owner',body:{id:reference,kind:'expense',amount:225,date:'2098-03-05',reason:'Correct unreconciled payment',note:'Unreconciled note'}});
  assert.equal(out.status,200,JSON.stringify(out.body));assert.equal(read().expenses[id].payments[0].amount,225);assert.equal(read().expenses[id].payments[0].date,'2098-03-05');
  seeded=seed({locked:false});id=seeded.id;reference=id+'/PAY-001';const s=read();
  s.bankStatements={'SANKI|Axis Bank 3448':{imports:[{reconciliationRows:[{linkedRecordIds:[reference]}]}]}};fs.writeFileSync(expenseFile,JSON.stringify(s));
  out=invoke('PATCH','/api/expenses/ledger-entry',{role:'owner',body:{id:reference,kind:'expense',amount:200,date:seeded.e.date,reason:'Correct note only',note:'Verified descriptive note'}});
  assert.equal(out.status,200,JSON.stringify(out.body));const payment=read().expenses[id].payments[0];assert.equal(payment.amount,200);assert.equal(payment.date,seeded.e.date);assert.equal(payment.note,'Verified descriptive note');
  assert.equal(invoke('POST','/api/expenses/:id',{params:{id},role:'owner',body:{particulars:'Expense metadata remains editable',editReason:'Descriptive correction'}}).status,200);
});

test('bank mismatch correction cannot bypass an expense payment lock but retains valid unlinked corrections',()=>{
  const code=fs.readFileSync(path.join(__dirname,'../modules/expenses.js'),'utf8'),start=code.indexOf('function correctLedgerMovementAmount('),end=code.indexOf('function setLedgerMovementExcluded(',start);
  assert.ok(start>=0&&end>start,'extract the actual amount-correction helper');
  const correct=require('node:vm').runInNewContext(code.slice(start,end)+'\ncorrectLedgerMovementAmount;',{
    num:value=>Number(value)||0,expenseLoans:{protectsMovement:()=>false},expenseRefunds:{protectsMovement:()=>false},
    expenseFinancialLock:require('../modules/expense-financial-lock').expenseFinancialLock,unpayBlockedReferences:()=>new Set()
  });
  assert.match(code,/if\(change\.error\)return res\.status\(409\)\.json\(Object\.assign\(\{success:false\},change\)\)/,'route returns the explicit lock error before any audit or save');
  for(const field of ['payments','reimbursementPayments'])for(const source of ['finalized_group','cash_period','unlinked']){
    const {id}=seed({locked:false,personal:field==='reimbursementPayments'}),s=read(),payment=s.expenses[id][field][0],reference=id+'/'+payment.id;
    if(source==='finalized_group')s.bankStatements={'SANKI|Axis Bank 3448':{imports:[{reconciliationRows:[{linkedRecordIds:[reference]}]}]}};
    if(source==='cash_period')s.cashReconciliations=[{status:'approved',account:payment.account,verifiedThrough:'2098-03-10'}];
    const before=JSON.stringify(s),out=correct(s,reference,225);
    if(source==='unlinked'){
      assert.equal(out.error,undefined);assert.equal(out.before,200);assert.equal(out.after,225);assert.equal(payment.amount,225);
    }else{
      assert.equal(out.financialFieldsLocked,true);assert.match(out.error,/locked for every role/i);assert.equal(JSON.stringify(s),before,'lock detection and rejection are read-only');
      const same=correct(s,reference,200);assert.equal(same.error,undefined);assert.equal(payment.amount,200,'a no-change amount does not manufacture a correction');
    }
  }
});
