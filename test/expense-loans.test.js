'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const loans=require('../modules/expense-loans'),ui=require('../public/expense-loans');
const temp=fs.mkdtempSync(path.join(__dirname,'.tmp-loans-'));
process.env.DATA_PATH=path.join(temp,'data.json');
fs.writeFileSync(path.join(temp,'expenses.json'),JSON.stringify({expenses:{},receivables:{},receipts:[],openingBalances:{},oneTimeMigrations:{}}));
const {router,summaryForPL}=require('../modules/expenses');
const {buildReport}=require('../modules/pnl-report');
test.after(()=>fs.rmSync(temp,{recursive:true,force:true}));
const file=path.join(temp,'expenses.json'),account='IndusInd Bank 8181',day='2098-10-10';
const baseline=JSON.parse(fs.readFileSync(file,'utf8'));
function invoke(method,route,{body={},query={},params={},role='admin',username='prashant'}={}){
  const layer=router.stack.find(l=>l.route&&l.route.path===route&&l.route.methods[method.toLowerCase()]);assert.ok(layer,route);
  let status=200,result;const req={body,query,params,headers:{},get(){return '';},user:{username,role,roles:[role]}};
  layer.route.stack[0].handle(req,{status(c){status=c;return this;},json(r){result=r;return this;}},()=>{throw Error('Unexpected next');});
  return{status,body:result};
}
function reset(extra={}){const s=JSON.parse(JSON.stringify(baseline));Object.assign(s,{expenses:{},receivables:{},receipts:[],receiptSeq:0,loanRepayments:[],loanRepaymentSeq:0,transfers:[],adjustments:[],vendorAdvances:[],bankStatements:{},bankDateOverrides:{},bankReconciliationDrafts:{},reconciliationExpenses:[],salesRefunds:[],openingBalances:{[account]:0},restrictedFunds:[],auditLog:[]},extra);fs.writeFileSync(file,JSON.stringify(s));return s;}
function store(){return JSON.parse(fs.readFileSync(file,'utf8'));}
function borrow(changes={},role='admin'){return invoke('POST','/api/expenses/receipts',{role,body:{nature:'SANKI',account,receiptType:loans.TYPE,amount:500000,source:'Example Company',date:day,reference:'RTGS-DEMO-500K',note:'Temporary loan to be returned',proof:'/api/expenses/proof/loan.jpg',requestId:'loan-test-request-1',loanTerms:{lenderKind:'company',dueDate:'2098-11-10',lenderReference:'DEMO-AGREEMENT'},...changes}});}
function repay(id,changes={},role='admin'){return invoke('POST','/api/expenses/loans/:id/repay',{params:{id},role,body:{amount:100000,account,date:'2098-10-11',reference:'PAY-DEMO-100K',note:'First principal instalment',proof:'/api/expenses/proof/payment.jpg',requestId:'repay-test-request-1',...changes}});}
function position(asOf='2098-12-31',nature='SANKI',role='owner'){return invoke('GET','/api/expenses/balance-sheet',{role,query:{nature,asOf}}).body;}
function throughRouter(method,url,body,role='owner'){
  return new Promise((resolve,reject)=>{const req={method,path:url,url,originalUrl:url,baseUrl:'',body,query:{},headers:{},get(){return '';},user:{username:role==='admin'?'prashant':'loan-test-owner',role,roles:[role]}};let status=200,done=false;const headers={},finish=value=>{if(!done){done=true;resolve({status,body:value});}},res={headersSent:false,statusCode:200,status(n){status=n;this.statusCode=n;return this;},setHeader(k,v){headers[k]=v;},getHeader(k){return headers[k];},removeHeader(k){delete headers[k];},json(v){this.headersSent=true;finish(v);return this;},send(v){this.headersSent=true;finish(v);return this;},end(v){this.headersSent=true;finish(v);return this;}};router.handle(req,res,e=>e?reject(e):finish(undefined));});
}
function draft(id,transactions,opening){const s=store(),totalDebits=transactions.reduce((n,t)=>n+Number(t.debit||0),0),totalCredits=transactions.reduce((n,t)=>n+Number(t.credit||0),0);s.bankReconciliationDrafts[id]={id,account,nature:'SANKI',transactions,summary:{from:transactions[0].date,to:transactions.at(-1).date,openingBalance:opening,closingBalance:opening+totalCredits-totalDebits,totalDebits,totalCredits,validated:true},resolutions:{},matchingPolicy:'balanced_date_amount_v5',createdBy:'prashant',createdAt:new Date().toISOString()};fs.writeFileSync(file,JSON.stringify(s));}

test('strict principal, lender and real calendar validation',()=>{
  for(const amount of [0,-1,'abc','Infinity',Infinity,1.001,null,'',900719925474099])assert.throws(()=>loans.amount(amount));
  assert.equal(loans.amount('500000.01'),500000.01);
  assert.equal(loans.validDate('2026-02-29'),false);assert.equal(loans.validDate('2028-02-29'),true);
  reset();for(const changes of [{source:''},{note:''},{loanTerms:{}},{loanTerms:{lenderKind:'person',dueDate:'2098-10-09'}},{date:'2098-02-30'},{amount:'200junk'},{reference:''}]){const r=borrow(changes);assert.equal(r.status,400,JSON.stringify(r));assert.equal(store().receipts.length,0);}
});
test('normal loan receipt credits bank once and creates an equal liability without profit',()=>{
  reset();const before=position(),r=borrow();assert.equal(r.status,200,JSON.stringify(r));const l=r.body.receipt;assert.equal(l.receiptType,'loan_received');assert.equal(l.category,loans.CATEGORY);assert.equal(l.source,'Example Company');assert.equal(l.loanTerms.lenderKind,'company');
  const after=position();assert.equal(after.liabilities.loans,500000);assert.equal(after.liabilities.loanDetails[0].lender,l.source);assert.equal(after.assets.bankAndCash-before.assets.bankAndCash,500000);assert.equal(after.equity.recordedNetPosition,before.equity.recordedNetPosition);
  const ledger=invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account,from:day,to:'2098-12-31'}}).body;assert.equal(ledger.entries.filter(e=>e.id===l.id).length,1);assert.equal(ledger.entries.find(e=>e.id===l.id).editable,false);assert.equal(ledger.balance,500000);
  const legacy=summaryForPL(day,'2098-12-31');assert.equal(legacy.Shared.variable,0);
  const report=buildReport({expenses:store(),orders:{orders:{}},purchases:{},opening:{lots:[]},salary:{},manualSales:{},feeds:[]},{from:day,to:'2098-12-31'});assert.equal(report.totals.otherIncome,0);assert.equal(report.totals.expenses,0);
});
test('repayments reduce bank and outstanding principal, not P&L, with as-of history',()=>{
  reset();const l=borrow({amount:500000.25}).body.receipt;const r=repay(l.id);assert.equal(r.status,200,JSON.stringify(r));assert.equal(r.body.position.outstanding,400000.25);assert.equal(position('2098-10-10').liabilities.loans,500000.25);assert.equal(position('2098-10-11').liabilities.loans,400000.25);
  const full=repay(l.id,{amount:400000.25,date:'2098-10-12',reference:'PAY-FULL',requestId:'repay-test-request-2'});assert.equal(full.status,200);assert.equal(full.body.position.status,'repaid');assert.equal(position().liabilities.loans,0);assert.equal(position().assets.accounts.find(a=>a.name===account),undefined);
  const ledger=invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account,from:day,to:'2098-12-31'}}).body;assert.equal(ledger.entries.filter(e=>e.kind==='loan_repayment').length,2);assert.equal(ledger.balance,0);
  const report=buildReport({expenses:store(),orders:{orders:{}},purchases:{},opening:{lots:[]},salary:{},manualSales:{},feeds:[]},{from:day,to:'2098-12-31'});assert.equal(report.totals.otherIncome,0);assert.equal(report.totals.expenses,0);
});
test('request retries are idempotent and reference reuse cannot double-post',()=>{
  reset();const l=borrow().body.receipt;assert.equal(borrow().body.duplicate,true);assert.equal(store().receipts.length,1);
  assert.equal(borrow({amount:500001}).status,409);assert.equal(borrow({requestId:'loan-other-request'}).status,409);
  const p=repay(l.id);assert.equal(p.status,200);assert.equal(repay(l.id).body.duplicate,true);assert.equal(store().loanRepayments.length,1);assert.equal(repay(l.id,{requestId:'repay-other-request'}).status,409);assert.equal(repay(l.id,{amount:50}).status,409);
});
test('cash loans preserve existing proof rules and never silently round fractional principal',()=>{
  reset();const before=position();assert.equal(borrow({account:'Counter Cash',amount:300.25},'owner').status,400);
  assert.equal(borrow({account:'Counter Cash',amount:300,proof:''},'admin').status,400);
  const r=borrow({account:'Counter Cash',amount:300,proof:''},'owner');assert.equal(r.status,200,JSON.stringify(r));assert.equal(position().liabilities.loans,300);assert.equal(position().assets.bankAndCash-before.assets.bankAndCash,300);
  assert.equal(repay(r.body.receipt.id,{account:'Counter Cash',amount:100.25}).status,400);
  assert.equal(repay(r.body.receipt.id,{account:'Counter Cash',amount:100}).status,200);assert.equal(position().liabilities.loans,200);
  reset();assert.equal(borrow({account:'Counter Cash',date:'2026-08-21',amount:300},'owner').status,409);assert.equal(store().receipts.length,0);
});
test('overpayment, backdating, missing proof and invalid currency never mutate loans',()=>{
  reset();const l=borrow().body.receipt;for(const changes of [{amount:500001},{date:'2098-10-09'},{date:'2098-02-30'},{proof:''},{amount:1.001},{note:''},{reference:''},{account:'Fake Bank'}]){const r=repay(l.id,changes);assert.equal(r.status,400,JSON.stringify(r));assert.equal(store().loanRepayments.length,0);}
  assert.equal(repay(l.id).status,200);assert.equal(repay(l.id,{amount:450000,date:day,requestId:'repay-backdated-request',reference:'PAY-BACKDATED'}).status,400,'later payments remain counted when validating a backdated repayment');
});
test('permissions keep private entities/accounts and unrelated receipt privileges unchanged',()=>{
  reset();assert.equal(borrow({},'claimant').status,403);assert.equal(borrow({receiptType:'other_income'},'admin').status,403);assert.equal(borrow({nature:'PERSONAL'},'admin').status,403);assert.equal(borrow({account:'Services'},'admin').status,403);const l=borrow().body.receipt;
  assert.equal(repay(l.id,{},'claimant').status,403);assert.equal(invoke('GET','/api/expenses/loans',{role:'claimant'}).status,403);assert.equal(invoke('GET','/api/expenses/loans',{query:{nature:'PERSONAL'}}).status,403);assert.equal(repay(l.id,{account:'Services'}).status,403);
  const privateLoan=borrow({nature:'PERSONAL',account:'ICICI Bank 0992',reference:'PRIVATE-LOAN',requestId:'private-loan-request'},'owner');assert.equal(privateLoan.status,200,JSON.stringify(privateLoan));const listed=invoke('GET','/api/expenses/loans',{query:{asOf:'2098-12-31'}}).body;assert.equal(listed.loans.length,1);assert.equal(position('2098-12-31','','admin').liabilities.loanDetails.length,1);
});
test('loan receipt from reconciliation uses authoritative evidence and consumes one ledger credit',()=>{
  reset();draft('BRD-LOAN',[{date:day,description:'RTGS Example Company repayable funds',reference:'RTGS-STATEMENT',debit:0,credit:500000,balance:500000}],0);
  const r=invoke('POST','/api/expenses/bank-statements/create-incoming',{body:{draftId:'BRD-LOAN',rowId:'bank-0',sourceKind:'external',receiptType:loans.TYPE,source:'Example Company',amount:1,account:'Axis Bank 3448',date:'2098-01-01',category:'Other Income',note:'Loan principal received; will repay',loanTerms:{lenderKind:'company'}}});
  assert.equal(r.status,200,JSON.stringify(r));const l=r.body.receipt;assert.equal(l.amount,500000);assert.equal(l.date,day);assert.equal(l.account,account);assert.equal(l.category,loans.CATEGORY);assert.equal(l.bankReconciliationEvidence.rowId,'bank-0');assert.equal(r.body.balanceDifference,0);assert.equal(r.body.unresolved,0);assert.equal(r.body.rows.length,1);assert.equal(r.body.rows[0].app.id,l.id);assert.equal(r.body.canFinalize,true);
  assert.equal(invoke('POST','/api/expenses/bank-statements/create-incoming',{body:{draftId:'BRD-LOAN',rowId:'bank-0',sourceKind:'external',receiptType:loans.TYPE,source:'Example',note:'Again',loanTerms:{lenderKind:'company'}}}).status,400);assert.equal(store().receipts.length,1);
  assert.equal(position().liabilities.loans,500000);
});
test('reconciliation repayment consumes the statement debit exactly once and keeps finalization controls',()=>{
  reset();const l=borrow().body.receipt;draft('BRD-LOAN-REPAY',[{date:day,description:'Example Company',reference:'RTGS-DEMO-500K',debit:0,credit:500000,balance:500000},{date:'2098-10-11',description:'Loan returned to Example Company',reference:'BANK-REPAYMENT',debit:100000,credit:0,balance:400000}],0);
  const r=invoke('POST','/api/expenses/bank-statements/create-loan-repayment',{body:{draftId:'BRD-LOAN-REPAY',rowId:'bank-1',loanId:l.id,amount:1,date:'2098-01-01',account:'Fake',note:'First repayment of loan principal'}});assert.equal(r.status,200,JSON.stringify(r));assert.equal(r.body.repayment.amount,100000);assert.equal(r.body.repayment.date,'2098-10-11');assert.equal(r.body.balanceDifference,0);assert.equal(r.body.unresolved,0);assert.equal(r.body.rows.length,2);assert.equal(r.body.rows.find(x=>x.id==='bank-1').app.id,r.body.repayment.id);assert.equal(r.body.rows.find(x=>x.id==='bank-1').resolution.action,'create_loan_repayment');assert.equal(position().liabilities.loans,400000);
  assert.equal(invoke('POST','/api/expenses/bank-statements/create-loan-repayment',{body:{draftId:'BRD-LOAN-REPAY',rowId:'bank-1',loanId:l.id,note:'Retry'}}).status,400);assert.equal(store().loanRepayments.length,1);
  assert.equal(invoke('POST','/api/expenses/loans/:id/void',{role:'owner',params:{id:r.body.repayment.id},body:{reason:'Try to void linked repayment'}}).status,409);
});
test('existing manual receipts and repayments are linked instead of re-created',()=>{
  reset();const l=borrow().body.receipt;draft('BRD-EXISTING',[{date:day,description:'Example Company',reference:'RTGS-DEMO-500K',debit:0,credit:500000,balance:500000}],0);
  const view=invoke('POST','/api/expenses/bank-statements/reconcile',{body:{draftId:'BRD-EXISTING',account}}).body;assert.equal(view.rows[0].app.id,l.id);assert.equal(view.rows[0].status,'matched');
  assert.equal(invoke('POST','/api/expenses/bank-statements/create-incoming',{body:{draftId:'BRD-EXISTING',rowId:'bank-0',sourceKind:'external',receiptType:loans.TYPE,source:'Example',note:'Must link',loanTerms:{lenderKind:'company'}}}).status,400);
  assert.equal(invoke('POST','/api/expenses/bank-statements/resolve',{body:{draftId:'BRD-EXISTING',rowId:'bank-0',action:'unmatch_suggestion',reason:'Review wrong bank match'}}).status,200);assert.equal(position().liabilities.loans,500000,'unmatching never deletes a real loan');
  const duplicate=invoke('POST','/api/expenses/bank-statements/create-incoming',{body:{draftId:'BRD-EXISTING',rowId:'bank-0',sourceKind:'external',receiptType:loans.TYPE,source:'Example',note:'Do not duplicate',loanTerms:{lenderKind:'company'}}});assert.equal(duplicate.status,409,JSON.stringify(duplicate));assert.equal(store().receipts.length,1);
});
test('generic ledger corrections cannot change principal independently; Owner void is audited',()=>{
  reset();const l=borrow().body.receipt;const r=invoke('PATCH','/api/expenses/ledger-entry',{role:'owner',body:{id:l.id,kind:'receipt',amount:1,date:day,source:'Wrong',reason:'Trying generic edit',nature:'SANKI',account}});assert.equal(r.status,409,JSON.stringify(r));assert.equal(store().receipts[0].amount,500000);
  const p=repay(l.id).body.repayment;assert.equal(invoke('POST','/api/expenses/loans/:id/void',{role:'owner',params:{id:l.id},body:{reason:'Incorrect loan'}}).status,409);assert.equal(invoke('POST','/api/expenses/loans/:id/void',{params:{id:p.id},body:{reason:'Wrong'}}).status,403);
  assert.equal(invoke('POST','/api/expenses/loans/:id/void',{role:'owner',params:{id:p.id},body:{reason:'Wrong repayment record'}}).status,200);assert.equal(position().liabilities.loans,500000);assert.equal(invoke('POST','/api/expenses/loans/:id/void',{role:'owner',params:{id:l.id},body:{reason:'Incorrect lender receipt'}}).status,200);assert.equal(position().liabilities.loans,0);assert.equal(store().auditLog.filter(a=>a.action==='LOAN_RECORD_VOIDED').length,2);
});
test('loan register groups neither unrelated receipts nor payments to the wrong lender',()=>{
  reset();const a=borrow().body.receipt,b=borrow({source:'Another Person',reference:'SECOND-LOAN',requestId:'loan-test-request-2',amount:500,loanTerms:{lenderKind:'person'}}).body.receipt;repay(b.id,{amount:200});assert.equal(loans.position(store(),a).outstanding,500000);assert.equal(loans.position(store(),b).outstanding,300);
  const r=invoke('GET','/api/expenses/loans',{query:{nature:'SANKI',asOf:'2098-12-31'}});assert.equal(r.body.totals.outstanding,500300);assert.equal(r.body.loans.length,2);
});

test('vendor groups and payable source filters keep loan principal separate from vendor bills',()=>{
  reset({expenses:{'EX-LENDER-BILL':{id:'EX-LENDER-BILL',nature:'SANKI',vendor:'Example Company',date:day,approvedAt:day,status:'approved',amount:200,paidAmount:0,paymentType:'UPI',ledger:'Flowers'}}});
  const loan=borrow().body.receipt;repay(loan.id);
  const query={nature:'SANKI',from:day,to:'2098-10-12'};
  const normal=invoke('GET','/api/expenses/vendors',{query:{...query,source:'expense'}}).body;
  assert.equal(normal.vendors.find(v=>v.name==='Example Company').ledgerClosingBalance,200);
  const lenders=invoke('GET','/api/expenses/vendors',{query:{...query,source:'loan'}}).body;
  assert.equal(lenders.vendors.length,0);assert.equal(lenders.lenders.length,1);assert.equal(lenders.totalDue,400000);assert.equal(lenders.totalAdvance,0);assert.equal(lenders.lenders[0].loans[0].id,loan.id);
  const vendorPayables=invoke('GET','/api/expenses/pending-payments',{query:{...query,source:'expense'}}).body;
  assert.equal(vendorPayables.expenses.length,1);assert.equal(vendorPayables.loans.length,0);assert.equal(vendorPayables.purchases.length,0);assert.equal(vendorPayables.totalOutstanding,200);
  const loanPayables=invoke('GET','/api/expenses/pending-payments',{query:{...query,source:'loan'}}).body;
  assert.equal(loanPayables.expenses.length,0);assert.equal(loanPayables.purchases.length,0);assert.equal(loanPayables.loanOutstanding,400000);assert.equal(loanPayables.totalOutstanding,400000);
  const all=invoke('GET','/api/expenses/pending-payments',{query}).body;assert.equal(all.totalOutstanding,400200);assert.equal(all.loans.length,1);
  assert.equal(position().liabilities.loans,400000,'classification changes no posted principal');
});

test('older unpaid loans carry forward while future, repaid and voided loans do not become payable',()=>{
  reset();const old=borrow({amount:500000.25}).body.receipt;
  const future=borrow({date:'2098-11-01',reference:'FUTURE',requestId:'future-loan-request',loanTerms:{lenderKind:'person'}});assert.equal(future.status,200);
  const paid=borrow({amount:500,source:'Paid Lender',reference:'SETTLED',requestId:'settled-loan-request'}).body.receipt;assert.equal(repay(paid.id,{amount:500,reference:'REPAID',requestId:'settled-pay-request'}).status,200);
  const voided=borrow({amount:100,source:'Voided Lender',reference:'VOIDED',requestId:'voided-loan-request'}).body.receipt;assert.equal(invoke('POST','/api/expenses/loans/:id/void',{role:'owner',params:{id:voided.id},body:{reason:'Synthetic incorrect lender'}}).status,200);
  const q={source:'loan',from:'2098-10-12',to:'2098-10-12'};
  const payable=invoke('GET','/api/expenses/pending-payments',{query:q}).body;assert.deepEqual(payable.loans.map(l=>l.id),[old.id]);assert.equal(payable.totalOutstanding,500000.25);
  const view=invoke('GET','/api/expenses/vendors',{query:q}).body,group=view.lenders.find(l=>l.name==='Example Company');
  assert.equal(group.openingPrincipal,500000.25);assert.equal(group.ledgerRows.length,0);assert.equal(group.outstanding,500000.25);assert.equal(view.loans.some(l=>l.id===future.body.receipt.id||l.id===voided.id),false);
});

test('lenders support partial repayment filters, search, empty results and real as-of positions',()=>{
  reset();const a=borrow().body.receipt,b=borrow({source:'Other Lender',amount:200.25,reference:'SECOND',requestId:'second-loan-request'}).body.receipt;repay(b.id,{amount:100.10});
  const q={source:'loan',to:'2098-10-12'};
  assert.deepEqual(invoke('GET','/api/expenses/pending-payments',{query:{...q,bucket:'partial'}}).body.loans.map(l=>l.id),[b.id]);
  assert.deepEqual(invoke('GET','/api/expenses/pending-payments',{query:{...q,bucket:'approved'}}).body.loans.map(l=>l.id),[a.id]);
  assert.equal(invoke('GET','/api/expenses/pending-payments',{query:{...q,bucket:'credit'}}).body.loans.length,0);
  assert.equal(invoke('GET','/api/expenses/pending-payments',{query:{...q,vendor:'other lender'}}).body.totalOutstanding,100.15);
  assert.equal(invoke('GET','/api/expenses/vendors',{query:{...q,search:'DEMO-AGREEMENT'}}).body.lenders.length,2);
  assert.equal(invoke('GET','/api/expenses/vendors',{query:{...q,search:'nothing matching'}}).body.lenders.length,0);
  assert.equal(invoke('GET','/api/expenses/pending-payments',{query:{...q,to:day,vendor:'Other Lender'}}).body.loanOutstanding,200.25);
  assert.equal(invoke('GET','/api/expenses/vendors',{query:{source:'loan',to:'2098-02-30'}}).status,400);
  assert.equal(invoke('GET','/api/expenses/pending-payments',{query:{source:'loan',to:'2098-02-30'}}).status,400);
  for(const route of ['/api/expenses/vendors','/api/expenses/pending-payments'])assert.equal(invoke('GET',route,{query:{source:'invalid'}}).status,400);
});

test('lender classification respects private entities and redacts restricted repayment evidence',()=>{
  reset();const loan=borrow().body.receipt;
  assert.equal(repay(loan.id,{account:'Services',reference:'PRIVATE-PAYMENT',note:'Private account note',proof:'/private-proof.jpg'},'owner').status,200);
  assert.equal(borrow({nature:'PERSONAL',account:'ICICI Bank 0992',source:'Private Lender',reference:'PRIVATE-LOAN',requestId:'private-request'},'owner').status,200);
  for(const route of ['/api/expenses/vendors','/api/expenses/pending-payments']){
    const q={source:'loan',to:'2098-12-31'},r=invoke('GET',route,{query:q});assert.equal(r.status,200);assert.equal(r.body.loans.length,1);
    const payment=r.body.loans[0].payments[0];assert.equal(payment.account,'Owner-restricted account');assert.equal(payment.amount,100000);
    const json=JSON.stringify(r.body);for(const secret of ['PRIVATE-PAYMENT','Private account note','private-proof','Private Lender'])assert.equal(json.includes(secret),false,secret);
    assert.equal(invoke('GET',route,{query:{...q,nature:'PERSONAL'}}).status,403);assert.equal(invoke('GET',route,{query:q,role:'claimant'}).status,403);
    const owner=invoke('GET',route,{query:q,role:'owner'}).body;assert.equal(owner.loans.length,2);
  }
});

test('sourcing view never falls back to ordinary expenses in another entity',()=>{
  reset({expenses:{SAM:{id:'SAM',nature:'SAMAST',vendor:'Samast vendor',date:day,status:'approved',amount:100,paidAmount:0}}});
  const q={source:'sourcing',nature:'SAMAST',to:'2098-12-31'};
  assert.equal(invoke('GET','/api/expenses/vendors',{query:q,role:'owner'}).body.vendors.length,0);
  const pending=invoke('GET','/api/expenses/pending-payments',{query:q,role:'owner'}).body;
  assert.equal(pending.expenses.length,0);assert.equal(pending.purchases.length,0);assert.equal(pending.loans.length,0);assert.equal(pending.totalOutstanding,0);
});

test('multiple lender agreements combine only within the same entity and preserve period history',()=>{
  const receipt=(id,source,nature,amount,date)=>({id,source,nature,date,receiptType:loans.TYPE,amount});
  const s={receipts:[receipt('A','Example Company','SANKI',1000,'2026-09-01'),receipt('B',' EXAMPLE   COMPANY ','SANKI',500,'2026-10-02'),receipt('C','Example Company','SAMAST',700,'2026-09-01'),receipt('D','Example Companx','SANKI',20,'2026-09-01')],loanRepayments:[{id:'P1',loanId:'A',amount:100,date:'2026-09-10'},{id:'P2',loanId:'A',amount:200,date:'2026-10-03'}]};
  const groups=loans.lenderBooks(loans.catalog(s,'2026-10-31'),{from:'2026-10-01'});assert.equal(groups.length,3);
  const g=groups.find(g=>g.nature==='SANKI'&&g.loans.length===2);assert.equal(g.borrowed,1500);assert.equal(g.outstanding,1200);assert.equal(g.openingPrincipal,900);assert.equal(g.periodReceived,500);assert.equal(g.periodRepaid,200);assert.equal(g.ledgerRows.at(-1).balance,1200);
  assert.equal(groups.find(g=>g.nature==='SAMAST').outstanding,700);assert.equal(groups.find(g=>g.name==='Example Companx').loans.length,1,'no fuzzy merge of legally different parties');
});

test('lender UI uses the protected loan repayment action and escapes party metadata',()=>{
  const l={id:'REC-UI',source:'<Lender>',nature:'SANKI',date:day,borrowed:500,repaid:200,outstanding:300,status:'partially_repaid',loanTerms:{lenderKind:'company',dueDate:'2098-11-10',lenderReference:'<Agreement>'},payments:[]};
  const data={from:day,lenders:[{name:l.source,nature:l.nature,borrowed:500,repaid:200,outstanding:300,openingPrincipal:200,periodReceived:100,periodRepaid:0,loans:[l]}],loans:[l]};
  const rendered=ui.renderParties(data);assert.match(rendered,/&lt;Lender&gt;/);assert.match(rendered,/&lt;Agreement&gt;/);assert.match(rendered,/Opening principal/);assert.match(rendered,/Record principal repayment/);assert.doesNotMatch(rendered,/openPay|Vendor advances|Void incorrect record/);
  const page=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');assert.match(page,/id="pf_source".*value="expense".*value="sourcing".*value="loan"/);assert.match(page,/data-vsource="loan"/);assert.match(page,/ExpenseLoans\.bindParties/);
});

test('same-day loan receipt precedes its repayment in the displayed running balance',()=>{
  const l={id:'REC-1',source:'Lender',nature:'SANKI',date:day,borrowed:500,repaid:200,outstanding:300,status:'partially_repaid',payments:[{id:'LOANPAY-1',date:day,amount:200}]};
  const html=ui.render({totals:l,loans:[l],canVoid:false});assert.ok(html.indexOf('>REC-1<')<html.indexOf('>LOANPAY-1<'));assert.doesNotMatch(html,/₹-200/);
});
test('personal borrowed principal cannot be allocated as rental income',()=>{
  const s={rentalSourceImport:require('../modules/rental-source.json').id,rentals:{tenancies:require('./fixtures/legacy-rentals')()},receipts:[{id:'BORROW',nature:'PERSONAL',receiptType:loans.TYPE,amount:500000,date:'2026-10-01',account:'ICICI Bank 0992'},{id:'RENT',nature:'PERSONAL',receiptType:'other_income',amount:10000,date:'2026-10-01',account:'IndusInd Bank 7883'}]};
  const view=require('../modules/rental-register').snapshot(s,'2026-10-12');assert.equal(view.receipts.some(r=>r.id==='BORROW'),false);assert.equal(view.receipts.some(r=>r.id==='RENT'),true);
});
test('UI escapes lender/notes, shows evidence and shares loan options between both entry points',()=>{
  const l={id:'REC-1',source:'<script>bad</script>',nature:'SANKI',date:day,borrowed:500,amount:500,repaid:200,outstanding:300,status:'partially_repaid',loanTerms:{lenderKind:'company',dueDate:'2098-11-10'},note:'<img src=x>',account,proof:'/api/expenses/proof/one.jpg',payments:[{id:'LOANPAY-1',date:'2098-10-11',amount:200,account,note:'Instalment',bankReference:'PAY-1'}]};const html=ui.render({totals:{borrowed:500,repaid:200,outstanding:300},loans:[l],canVoid:true});assert.ok(html.includes('&lt;script&gt;'));assert.ok(!html.includes('<script>'));assert.match(html,/Record principal repayment/);assert.match(html,/300\.00/);assert.match(html,/Void incorrect record/);assert.match(html,/Proof 1/);
  const page=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');assert.match(page,/ExpenseLoans\.mount/);assert.match(page,/id="biReceiptType".*value="loan_received"/);assert.match(page,/Record loan repayment/);assert.match(page,/ExpenseLoans\.receiptFields\('rc'\)/);assert.match(page,/ExpenseLoans\.receiptFields\('bi'\)/);
  for(const file of ['expenses.html','balance-sheet.html']){const source=fs.readFileSync(path.join(__dirname,'../public',file),'utf8');for(const [i,m]of [...source.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)].entries())if(m[1].trim())assert.doesNotThrow(()=>new vm.Script(m[1]),file+' inline script '+i);}
  assert.match(ui.liabilityRows([{lender:'<bad>',id:'REC-1',nature:'SANKI',borrowed:500,repaid:200,outstanding:300}]),/&lt;bad&gt;/);
});
test('Owner finalization persists linked loan evidence and later repayments without a new opening adjustment',async()=>{
  reset();draft('BRD-LOAN-FINAL',[{date:day,description:'Example Company',reference:'LOAN-FINAL',credit:500000,debit:0,balance:500000}],0);
  const l=invoke('POST','/api/expenses/bank-statements/create-incoming',{body:{draftId:'BRD-LOAN-FINAL',rowId:'bank-0',sourceKind:'external',receiptType:loans.TYPE,source:'Example Company',note:'Verified borrowed principal',loanTerms:{lenderKind:'company'}}}).body.receipt;
  const finalized=await throughRouter('POST','/api/expenses/bank-statements/finalize',{draftId:'BRD-LOAN-FINAL'});assert.equal(finalized.status,200,JSON.stringify(finalized.body));let s=store();assert.equal(s.receipts.filter(r=>r.id===l.id).length,1);assert.equal(s.bankStatements[account].imports.at(-1).reconciliationRows[0].linkedRecordIds[0],l.id);
  draft('BRD-PAY-FINAL',[{date:'2098-10-11',description:'Example Company repayment',reference:'PAY-FINAL',credit:0,debit:100000,balance:400000}],500000);
  const r=invoke('POST','/api/expenses/bank-statements/create-loan-repayment',{body:{draftId:'BRD-PAY-FINAL',rowId:'bank-0',loanId:l.id,note:'Verified principal returned'}});assert.equal(r.body.balanceDifference,0);assert.equal(r.body.canFinalize,true);
  const paidFinal=await throughRouter('POST','/api/expenses/bank-statements/finalize',{draftId:'BRD-PAY-FINAL'});assert.equal(paidFinal.status,200,JSON.stringify(paidFinal.body));s=store();assert.equal(s.loanRepayments.length,1);assert.equal(s.bankStatements[account].imports.at(-1).reconciliationRows[0].linkedRecordIds[0],r.body.repayment.id);assert.equal(position().liabilities.loans,400000);assert.equal(s.adjustments.length,0);
  assert.equal(invoke('POST','/api/expenses/loans/:id/void',{role:'owner',params:{id:r.body.repayment.id},body:{reason:'Cannot edit finalized principal'}}).status,409);
  assert.equal(repay(l.id,{date:'2098-10-11',reference:'LATE-PAST',requestId:'late-past-request'}).status,409);
  assert.equal(borrow({date:day,reference:'LATE-PAST-IN',requestId:'late-past-receipt'}).status,409);
  draft('BRD-NEXT-DAY',[{date:'2098-10-12',description:'Example Company repayment again',reference:'PAY-NEXT',credit:0,debit:100000,balance:300000}],400000);
  const next=invoke('POST','/api/expenses/bank-statements/create-loan-repayment',{body:{draftId:'BRD-NEXT-DAY',rowId:'bank-0',loanId:l.id,note:'Second principal instalment'}});assert.equal(next.body.currentOpening,400000);assert.equal(next.body.balanceDifference,0);assert.equal(next.body.canFinalize,true);
});
test('admin loan reconciliation still requires Owner approval before period finalization',async()=>{
  reset();draft('BRD-LOAN-ADMIN',[{date:day,description:'Example Company',reference:'LOAN-ADMIN',credit:500000,debit:0,balance:500000}],0);
  assert.equal(invoke('POST','/api/expenses/bank-statements/create-incoming',{body:{draftId:'BRD-LOAN-ADMIN',rowId:'bank-0',sourceKind:'external',receiptType:loans.TYPE,source:'Example Company',note:'Loan classified by admin',loanTerms:{lenderKind:'company'}}}).status,200);
  const requested=await throughRouter('POST','/api/expenses/bank-statements/finalize',{draftId:'BRD-LOAN-ADMIN'},'admin');assert.equal(requested.status,202,JSON.stringify(requested));assert.equal(requested.body.approvalPending,true);assert.equal(Object.keys(store().bankStatements).length,0);
});
