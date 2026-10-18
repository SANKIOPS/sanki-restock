'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const temp=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-transfer-reim-'));
process.env.DATA_PATH=path.join(temp,'data.json');
const file=path.join(temp,'expenses.json');
fs.writeFileSync(file,JSON.stringify({expenses:{}}));
const helpers=require('../modules/transfer-reimbursement-links');
const {router,appBankMovements}=require('../modules/expenses');
const {expenseFinancialLock}=require('../modules/expense-financial-lock');
test.after(()=>fs.rmSync(temp,{recursive:true,force:true}));
const bank='Axis Bank 3448',recipient='Arshpreet 1919',date='2098-06-14',route='/api/expenses/:id/reimburse-from-transfer';
function fixture(){
  const expense={id:'EX-LINK',nature:'SANKI',amount:1000,date,paidAlready:true,personalPaidAmount:1000,paidAmount:1000,approvedAt:date,status:'paid',claimant:'different-submitter',reimbursementStatus:'pending',reimbursementAmount:0,reimbursementPayments:[],payments:[{id:'PAY-001',date,amount:1000,account:recipient,personalFunds:true,proof:'',proofs:[]}]};
  const transfer={id:'TR-LINK',nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',fromAccount:bank,toAccount:recipient,classification:'internal_transfer',amount:1000,date,proof:'/api/expenses/photo/verified.jpg'};
  const row={id:'bank-1',bank:{date,debit:1005.9,credit:0,reference:'BANK-REF',transactionId:'BTX-UNIQUE'},ledger:{id:transfer.id,date,debit:1000,credit:0},linkedRecordIds:[transfer.id],decision:'Matched automatically'};
  const record={id:'BST-LINK',draftId:'BRD-LINK',finalizedAt:date+'T12:00:00Z',reconciliationRows:[row]};
  return {expenses:{[expense.id]:expense},transfers:[transfer],bankStatements:{[bank]:{imports:[record]}},adjustments:[{id:'ADJ-FEE',nature:'SANKI',account:bank,date,amount:-5.9}],openingBalances:{[bank]:5000},receivables:{},receipts:[],vendorAdvances:[],bankReconciliationDrafts:{},bankDateOverrides:{},oneTimeMigrations:{},auditLog:[],expense,transfer,row,record};
}
function reset(s=fixture()){const {expense,transfer,row,record,...data}=s;fs.writeFileSync(file,JSON.stringify(data));return s;}
function read(){return JSON.parse(fs.readFileSync(file));}
function invoke(url,{method='GET',role='admin',id='EX-LINK',body={},query={}}={}){
  const layer=router.stack.find(x=>x.route&&x.route.path===url&&x.route.methods[method.toLowerCase()]);assert.ok(layer,url);let status=200,result;
  layer.route.stack[0].handle({method,body,query,params:{id,paymentId:'REIM-001'},headers:{},get(){return '';},user:{username:role==='admin'?'prashant':'test-owner',role,roles:[role]}},{status(n){status=n;return this;},json(value){result=value;return this;}},()=>{throw Error('Unexpected next');});return{status,body:result};
}
const post=(body={},options={})=>invoke(route,{method:'POST',body:{transferId:'TR-LINK',reason:'Confirmed transfer repaid this personal expense',...body},...options});
function balances(){return invoke('/api/expenses/balances',{query:{nature:'SANKI',to:date}}).body.accounts.filter(x=>[bank,recipient].includes(x.name)).map(x=>[x.name,x.balance]);}

test('candidate is based on verified bank principal and actual funding account, not submitter name',()=>{
  reset();const result=invoke('/api/expenses/:id/reimbursement-transfers');assert.equal(result.status,200);assert.equal(result.body.transfers.length,1);assert.equal(result.body.transfers[0].date,date);assert.equal(result.body.transfers[0].amount,1000);
});
test('allocation clears liability, preserves dates/proof/bank evidence and creates zero cash movements',()=>{
  reset();const before=read(),totals=balances(),sourceBefore=invoke('/api/expenses/account-ledger',{query:{nature:'SANKI',account:bank}}).body,recipientBefore=invoke('/api/expenses/account-ledger',{query:{nature:'SANKI',account:recipient}}).body;
  const result=post();assert.equal(result.status,200,JSON.stringify(result));assert.equal(result.body.accountBalancesUnchanged,true);assert.equal(result.body.ledgerMovementCreated,false);
  const after=read(),e=after.expenses['EX-LINK'];assert.equal(e.reimbursementStatus,'reimbursed');assert.equal(e.reimbursementAmount,1000);assert.equal(e.reimbursementPayments.length,1);assert.equal(e.reimbursementPayments[0].settledByTransferId,'TR-LINK');assert.equal(e.reimbursementPayments[0].date,date);assert.equal(e.reimbursementPayments[0].proof,before.transfers[0].proof);assert.equal(e.reimbursementPayments[0].accountingExcluded,undefined);assert.equal(e.reimbursementPayments[0].historicalSettlement,undefined);assert.deepEqual(e.payments,before.expenses['EX-LINK'].payments);assert.deepEqual(after.bankStatements,before.bankStatements);assert.deepEqual(after.adjustments,before.adjustments);assert.equal(after.transfers.length,1);assert.equal(after.transfers[0].amount,1000);assert.equal(after.auditLog.filter(x=>/TRANSFER.*ALLOCATED|LINKED_TO_TRANSFER/.test(x.action)).length,2);assert.deepEqual(balances(),totals);
  for(const [account,beforeLedger] of [[bank,sourceBefore],[recipient,recipientBefore]]){const ledger=invoke('/api/expenses/account-ledger',{query:{nature:'SANKI',account}}).body;assert.equal(ledger.balance,beforeLedger.balance);assert.equal(ledger.entries.length,beforeLedger.entries.length);assert.match(ledger.entries.find(x=>x.id==='TR-LINK').description,/EX-LINK/);assert.equal(ledger.entries.some(x=>x.id.includes('/REIM')),false);}
  assert.equal(expenseFinancialLock(after,e).locked,true);
  const sheet=invoke('/api/expenses/balance-sheet',{query:{nature:'SANKI',asOf:date}}).body;assert.equal(sheet.liabilities.reimbursements,0);
});
test('same request is idempotent, and another expense cannot consume the same transfer',()=>{
  const s=fixture();s.expenses['EX-OTHER']={...s.expense,id:'EX-OTHER'};reset(s);assert.equal(post().status,200);assert.equal(post().body.already,true);assert.equal(read().expenses['EX-LINK'].reimbursementPayments.length,1);assert.equal(post({}, {id:'EX-OTHER'}).status,409);assert.equal(read().expenses['EX-OTHER'].reimbursementAmount,0);
});
test('claimants cannot list or apply allocations; reason and authorised paying source are mandatory',()=>{
  reset();assert.equal(invoke('/api/expenses/:id/reimbursement-transfers',{role:'claimant'}).status,403);assert.equal(post({}, {role:'claimant'}).status,403);assert.equal(post({reason:''}).status,400);
  const s=fixture();s.transfer.fromAccount='ICICI Bank 0992';s.transfer.fromNature='PERSONAL';reset(s);assert.equal(post().status,403);assert.equal(read().expenses['EX-LINK'].reimbursementAmount,0);
});
for(const [name,change] of Object.entries({
  'wrong principal':s=>s.transfer.amount=999,
  'unapproved expense':s=>delete s.expense.approvedAt,
  'rejected expense':s=>s.expense.status='rejected',
  'wrong recipient':s=>s.transfer.toAccount='Shivam 4807',
  'wrong date':s=>s.row.bank.date='2098-06-15',
  'wrong source evidence':s=>s.transfer.fromAccount='IndusInd Bank 8181',
  'wrong direction':s=>{s.row.bank.credit=s.row.bank.debit;s.row.bank.debit=0;},
  'non-finalized evidence':s=>delete s.record.finalizedAt,
  'bank reversal':s=>s.row.bank.reversal=true,
  'excluded transfer':s=>s.transfer.accountingExcluded=true,
  'salary advance':s=>s.transfer.salaryAdvanceId='ADV-OTHER',
  'no proof':s=>delete s.transfer.proof,
  'stale allocated metadata':s=>s.transfer.reimbursementAllocations=[{expenseId:'EX-OLD'}],
  'ambiguous bank link':s=>s.record.reconciliationRows.push({...s.row,id:'bank-2',bank:{...s.row.bank,transactionId:'BTX-OTHER'}})
}))test('invalid allocation rejected without posting: '+name,()=>{const s=fixture();change(s);reset(s);const result=post();assert.equal(result.status,409,JSON.stringify(result));assert.equal(read().expenses['EX-LINK'].reimbursementAmount,0);});
test('linked transfers and settlement rows cannot be edited, deleted or fee-split independently',()=>{
  reset();post();const edit={method:'PATCH',role:'owner',body:{id:'TR-LINK',kind:'transfer',amount:999,date,reason:'Test edit',nature:'SANKI',account:bank}};
  assert.equal(invoke('/api/expenses/ledger-entry',edit).status,409);
  assert.equal(invoke('/api/expenses/transfers/:id/delete',{method:'POST',role:'owner',id:'TR-LINK',body:{reason:'Test deletion'}}).status,409);
  assert.equal(invoke('/api/expenses/ledger-entry',{...edit,body:{...edit.body,id:'EX-LINK/REIM-001',kind:'reimbursement'}}).status,409);
  assert.equal(read().transfers[0].amount,1000);
});
test('Owner can undo only the allocation without reversing or deleting the original transfer',()=>{
  reset();const before=balances();post();assert.equal(invoke('/api/expenses/:id/reimbursements/:paymentId/undo',{method:'POST',body:{reason:'Undo allocation confirmation'},role:'admin'}).status,403);
  const result=invoke('/api/expenses/:id/reimbursements/:paymentId/undo',{method:'POST',body:{reason:'Undo allocation confirmation'},role:'owner'});assert.equal(result.status,200,JSON.stringify(result));const s=read();assert.equal(s.expenses['EX-LINK'].reimbursementStatus,'pending');assert.equal(s.transfers.length,1);assert.equal(s.transfers[0].amount,1000);assert.equal(s.transfers[0].reimbursementAllocations.length,0);assert.deepEqual(balances(),before);
});
test('linked settlement is not offered as a second bank movement',()=>{
  reset();post();const s=read(),rows=appBankMovements(s,bank,'SANKI');assert.equal(rows.filter(x=>x.id==='TR-LINK').length,1);assert.equal(rows.some(x=>x.id.includes('/REIM')),false);const receiverRows=appBankMovements(s,recipient,'SANKI');assert.equal(receiverRows.filter(x=>x.id==='TR-LINK').length,1);assert.equal(receiverRows.some(x=>x.id.includes('/REIM')),false);
});
