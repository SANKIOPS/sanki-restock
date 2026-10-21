'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const helper=require('../modules/expense-ledger-connections');
const temp=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-ledger-links-'));
process.env.DATA_PATH=path.join(temp,'data.json');
const file=path.join(temp,'expenses.json'),salaryFile=path.join(temp,'salary.json');
fs.writeFileSync(file,JSON.stringify({expenses:{},receipts:[],receivables:{},oneTimeMigrations:{}}));
const {router,applyFinalizedInternalTransfers}=require('../modules/expenses');
test.after(()=>fs.rmSync(temp,{recursive:true,force:true}));
const account='Arshpreet 1919',bank='Axis Bank 3448',date='2098-06-14';
function invoke(route,{method='GET',role='admin',query={},body={}}={}){
  const layer=router.stack.find(x=>x.route&&x.route.path===route&&x.route.methods[method.toLowerCase()]);assert.ok(layer,route);let status=200,result;
  layer.route.stack[0].handle({method,query,body,params:{},headers:{},get(){return '';},user:{role,roles:[role],username:role==='admin'?'prashant':'owner-user'}},{status(n){status=n;return this;},json(r){result=r;return this;}},()=>{throw Error('Unexpected next');});return{status,body:result};
}
function reset(extra={}){
  fs.writeFileSync(salaryFile,JSON.stringify({employees:{},advances:{}}));
  const s={expenses:{},receivables:{},receipts:[],loanRepayments:[],transfers:[],adjustments:[],vendorAdvances:[],salesRefunds:[],reconciliationExpenses:[],bankStatements:{},bankDateOverrides:{},bankReconciliationDrafts:{},oneTimeMigrations:{},openingBalances:{[account]:0,[bank]:100000},auditLog:[],...extra};fs.writeFileSync(file,JSON.stringify(s));return s;
}
function fixture(){
  const transfer={id:'TR-TEST-LINK',nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',fromAccount:bank,toAccount:account,classification:'internal_transfer',amount:123,date,bankStatementEvidence:true,reconciliationDraft:'BRD-TEST-LINK',bankRowId:'bank-2'};
  const row={id:'bank-2',bank:{date,debit:123,credit:0,reference:'REF-TEST-LINK',transactionId:'BTX-TEST-LINK'},linkedRecordIds:[],ledger:null,decision:'Reviewed',reason:'Verified transfer'};
  const record={id:'BST-TEST-LINK',draftId:'BRD-TEST-LINK',finalizedAt:'2098-06-15T10:00:00Z',finalizedBy:'owner-user',reconciliationRows:[row]};
  return{transfers:[transfer],bankStatements:{[bank]:{imports:[record]}},transfer,row,record};
}
function expense(id,amount,nature='SANKI'){
  return{id,nature,date,amount,paidAmount:amount,status:'paid',approvedAt:date,claimant:'arshpreet',vendor:nature==='PERSONAL'?'SECRET VENDOR':'Test vendor',particulars:'Test',payments:[{id:'PAY',amount,date,account,personalFunds:true}],reimbursementPayments:[{id:'REIM',amount,date,account:bank}]};
}

test('claimant account and receiving movement identity are shared, including advance-only claims',()=>{
  assert.equal(helper.claimantAccount({payments:[],claimantFundingAccount:account}),account);
  assert.equal(helper.canonicalMovementId('EX-A/REIM/RECEIVED'),'EX-A/REIM');
  assert.equal(helper.movementOverride({bankDateOverrides:{'EX-A/REIM':{bankDate:date}}},'EX-A/REIM/RECEIVED').bankDate,date);
});
test('created transfer links are repaired by exact provenance once, with audit and no balance mutation',()=>{
  const s=fixture(),before=JSON.stringify(s.transfer),events=[];
  assert.equal(helper.repairCreatedTransferLinks(s,e=>events.push(e)),true);
  assert.deepEqual(s.row.linkedRecordIds,[s.transfer.id]);assert.equal(JSON.stringify(s.transfer),before);assert.equal(s.row.bank.debit,123);assert.equal(events.length,1);
  assert.equal(s.bankDateOverrides[s.transfer.id].reconciliationDraft,s.record.id);
  assert.equal(helper.repairCreatedTransferLinks(s),false);assert.equal(s.row.correctionHistory.length,1);
});
for(const [name,mutate] of Object.entries({
  'amount-only lookalike':s=>delete s.transfer.reconciliationDraft,
  'different source account':s=>s.transfer.fromAccount='IndusInd Bank 8181',
  'different entity':s=>s.transfer.fromNature='PERSONAL',
  'different date':s=>s.transfer.date='2098-06-13',
  'different amount':s=>s.transfer.amount=122,
  'different reference':s=>s.transfer.bankReference='NOT-THE-REF',
  'bank reversal':s=>s.row.bank.reversal=true,
  'later manual correction':s=>s.row.corrected=true,
  'excluded bank decision':s=>s.row.decision='Excluded',
  'foreign active draft override':s=>s.bankDateOverrides={[s.transfer.id]:{reconciliationDraft:'BRD-OTHER'}},
  'unfinalized record':s=>delete s.record.finalizedAt,
  'another linked entry':s=>s.row.linkedRecordIds=['EX-OTHER/PAY'],
  'duplicate created transfer':s=>s.transfers.push({...s.transfer,id:'TR-DUPLICATE'}),
  'ambiguous provenance':s=>s.record.reconciliationRows.push({...s.row,bank:{...s.row.bank,transactionId:'BTX-OTHER'}})
}))test('link recovery rejects '+name,()=>{const s=fixture();mutate(s);assert.equal(helper.repairCreatedTransferLinks(s),false);});

test('legacy combined IDs are normalized; ambiguous different bank payments are never projected',()=>{
  const s=fixture();s.row.linkedRecordIds=['EX-A/REIM, EX-B/REIM'];assert.deepEqual(helper.linkedIds(s.row),['EX-A/REIM','EX-B/REIM']);assert.ok(helper.uniqueFinalizedLink(s,'EX-A/REIM/RECEIVED'));
  s.record.reconciliationRows.push({id:'bank-3',bank:{...s.row.bank,transactionId:'OTHER'},linkedRecordIds:['EX-A/REIM']});assert.equal(helper.uniqueFinalizedLink(s,'EX-A/REIM'),null);
});
test('principal and charges use paise and require the full bank amount with correct direction',()=>{
  assert.deepEqual(helper.transferSplit({debit:1005.9,credit:0},1000,5.9),{gross:1005.9,principal:1000,charge:5.9});
  for(const args of [[{debit:1005.9},500,5.9],[{credit:1005.9},1000,5.9],[{debit:1005.9},1000,-5.9],[{debit:1005.9},0,1005.9],[{debit:Infinity},Infinity,0],[{debit:1,credit:1},2,0]])assert.equal(helper.transferSplit(...args),null);
});

test('Owner/Admin summary and detailed balances include private credits without private details',()=>{
  reset({expenses:{'EX-PRIVATE':expense('EX-PRIVATE',3580,'PERSONAL')}});
  for(const role of ['admin','owner']){
    const ledger=invoke('/api/expenses/account-ledger',{role,query:{nature:'SANKI',account}}).body;
    const summary=invoke('/api/expenses/balances',{role,query:{nature:'SANKI'}}).body.accounts.find(x=>x.name===account);
    assert.equal(ledger.balance,0);assert.equal(summary.balance,ledger.balance);
    if(role==='admin'){assert.doesNotMatch(JSON.stringify(ledger),/SECRET VENDOR/);const credit=ledger.entries.find(x=>x.credit===3580);assert.equal(credit.privacyRedacted,true);assert.equal(credit.proof,'');assert.equal(credit.editable,false);}
  }
});
test('advance-only reimbursement credits are counted once in all balance views',()=>{
  reset();fs.writeFileSync(salaryFile,JSON.stringify({employees:{E:{id:'E',name:'Test employee'}},advances:{'ADV-TEST-500':{id:'ADV-TEST-500',empId:'E',employeeName:'Test employee',amount:500,date,account,active:true}}}));
  invoke('/api/expenses/config',{role:'owner'});const s=JSON.parse(fs.readFileSync(file));
  const claim=s.expenses['SALADV-CLAIM-SANKI-ADV-TEST-500'];assert.ok(claim);claim.reimbursementPayments=[{id:'REIM',amount:500,date,account:bank}];fs.writeFileSync(file,JSON.stringify(s));
  for(const role of ['owner','admin']){const ledger=invoke('/api/expenses/account-ledger',{role,query:{nature:'SANKI',account}}).body,summary=invoke('/api/expenses/balances',{role,query:{nature:'SANKI'}}).body.accounts.find(x=>x.name===account);assert.equal(ledger.balance,0);assert.equal(summary.balance,0);assert.equal(ledger.entries.filter(x=>x.credit===500).length,1);}
});
test('bank dates and entry status reach both sides, but do not finalize the receiving account',()=>{
  const e=expense('EX-DATE',4492),id='EX-DATE/REIM',old='2098-06-20',s=fixture();e.reimbursementPayments[0].date=old;s.row.linkedRecordIds=[id];s.row.ledger={id,date,debit:4492,credit:0};s.row.bank.debit=4492;
  reset({expenses:{[e.id]:e},bankStatements:s.bankStatements,bankDateOverrides:{[id]:{bankDate:date,originalDate:old,reconciliationDraft:s.record.id}}});
  for(const role of ['owner','admin']){
    const receiver=invoke('/api/expenses/account-ledger',{role,query:{nature:'SANKI',account,to:date}}).body,entry=receiver.entries.find(x=>x.id===id+'/RECEIVED');assert.equal(entry.date,date);assert.equal(entry.originalDate,old);assert.equal(entry.reconciliation.bankDate,date);assert.equal(entry.reconciliation.sourceAccountConfirmed,true);assert.equal(receiver.reconciledThrough,'');
    const header=invoke('/api/expenses/balances',{role,query:{nature:'SANKI',to:date}}).body.accounts.find(x=>x.name===account);assert.equal(header.balance,receiver.balance);
  }
  const after=JSON.parse(fs.readFileSync(file));assert.equal(after.expenses[e.id].reimbursementPayments[0].date,old,'original date is retained for audit/undo');assert.equal(after.adjustments.length,0);
});
test('draft-only date overrides cannot display finalized reconciliation status',()=>{
  const e=expense('EX-DRAFT',123);reset({expenses:{[e.id]:e},bankDateOverrides:{'EX-DRAFT/REIM':{bankDate:date,reconciliationDraft:'BRD-PENDING',provisional:true}}});
  const ledger=invoke('/api/expenses/account-ledger',{query:{nature:'SANKI',account}}).body;assert.equal(ledger.entries.find(x=>x.id==='EX-DRAFT/REIM/RECEIVED').reconciliation,undefined);assert.equal(ledger.reconciledThrough,'');
});
test('historical excluded reimbursements remain excluded; no fake credits or bank payments',()=>{
  const e=expense('EX-HISTORY',100);e.reimbursementPayments[0].accountingExcluded=true;e.reimbursementPayments[0].historicalSettlement=true;reset({expenses:{[e.id]:e}});
  const ledger=invoke('/api/expenses/account-ledger',{query:{nature:'SANKI',account}}).body,header=invoke('/api/expenses/balances',{query:{nature:'SANKI'}}).body.accounts.find(x=>x.name===account);assert.equal(ledger.balance,-100);assert.equal(header.balance,-100);assert.equal(ledger.entries.some(x=>x.credit>0),false);
});
test('protected finalized transfer split rejects Admin, unexplained gaps and wrong provenance',()=>{
  const s=fixture();s.transfer.amount=1005.9;s.row.bank.debit=1005.9;reset({transfers:s.transfers,bankStatements:s.bankStatements});invoke('/api/expenses/account-ledger',{query:{nature:'SANKI',account}});
  const route='/api/expenses/bank-statements/correct-finalized-transfer-split',body={nature:'SANKI',account:bank,recordId:s.record.id,rowId:s.row.id,appId:s.transfer.id,principalAmount:1000,chargeAmount:5.9,reason:'Verified recipient amount and fee'};
  assert.equal(invoke(route,{method:'POST',body}).status,403);
  assert.equal(invoke(route,{method:'POST',role:'owner',body:{...body,principalAmount:500}}).status,400);
  assert.equal(invoke(route,{method:'POST',role:'owner',body:{...body,rowId:'bank-999'}}).status,400);
  assert.equal(invoke(route,{method:'POST',role:'owner',body:{...body,reason:''}}).status,400);
  assert.equal(JSON.parse(fs.readFileSync(file)).transfers[0].amount,1005.9);assert.equal(JSON.parse(fs.readFileSync(file)).adjustments.length,0);
  const beforeSource=invoke('/api/expenses/balances',{role:'owner',query:{nature:'SANKI'}}).body.accounts.find(x=>x.name===bank).balance;
  const saved=invoke(route,{method:'POST',role:'owner',body});assert.equal(saved.status,200,JSON.stringify(saved.body));assert.equal(saved.body.sourceBalanceUnchanged,true);
  const after=JSON.parse(fs.readFileSync(file));assert.equal(after.transfers[0].amount,1000);assert.equal(after.adjustments.length,1);assert.equal(after.adjustments[0].amount,-5.9);assert.equal(after.reconciliationExpenses.at(-1).category,'BANK CHARGES');assert.equal(after.bankStatements[bank].imports[0].reconciliationRows[0].bank.debit,1005.9);
  const afterSource=invoke('/api/expenses/balances',{role:'owner',query:{nature:'SANKI'}}).body.accounts.find(x=>x.name===bank).balance;assert.equal(afterSource,beforeSource);
  assert.equal(invoke(route,{method:'POST',role:'owner',body}).body.already,true);assert.equal(JSON.parse(fs.readFileSync(file)).adjustments.length,1);
});
test('new bank-created transfers separate confirmed charges and are idempotent',()=>{
  const id='BRD-NEW-CHARGE',bankRow={date,debit:1005.9,credit:0,reference:'NEW-FEE'},draft={id,nature:'SANKI',account:bank,transactions:[bankRow],resolutions:{'bank-0':{action:'create_internal_transfer',otherAccount:account,principalAmount:1000,chargeAmount:5.9,reason:'Confirmed principal and fee'}}};
  // The helper is post-finalization: retain the physical row with the exact
  // draft/index provenance that the normal finalize route supplies.
  reset({bankStatements:{[bank]:{transactions:{'BTX-NEW-CHARGE':{...bankRow,id:'BTX-NEW-CHARGE',firstSeenImport:'BST-NEW-CHARGE',lastSeenImport:'BST-NEW-CHARGE',reconciliationDraftId:id,reconciliationDraftRowIndex:0}},imports:[{id:'BST-NEW-CHARGE',draftId:id,from:date,to:date,finalizedAt:date+'T12:00:00.000Z',reconciliationRows:[]}]}}});
  applyFinalizedInternalTransfers(draft,'owner-user');applyFinalizedInternalTransfers(draft,'owner-user');const s=JSON.parse(fs.readFileSync(file));assert.equal(s.transfers.length,1);assert.equal(s.transfers[0].amount,1000);assert.equal(s.adjustments.filter(x=>x.transferChargeFor===s.transfers[0].id).length,1);assert.equal(s.adjustments.at(-1).amount,-5.9);
});
test('receiving reimbursement credits are available to the normal statement comparison',()=>{
  const e=expense('EX-RECEIVING',123);reset({expenses:{[e.id]:e},bankReconciliationDrafts:{D:{id:'D',nature:'SANKI',account,transactions:[{date,credit:123,debit:0,reference:'EX-RECEIVING/REIM/RECEIVED',description:'Reimbursement received',balance:0}],summary:{from:date,to:date,openingBalance:-123,closingBalance:0,totalDebits:0,totalCredits:123,validated:true},resolutions:{},createdAt:date,matchingPolicy:'balanced_date_amount_v5'}}});
  const result=invoke('/api/expenses/bank-statements',{query:{nature:'SANKI',account,draftId:'D'}});assert.equal(result.status,200);assert.ok(result.body.draft.rows.some(r=>r.app?.id==='EX-RECEIVING/REIM/RECEIVED'||r.suggestion?.id==='EX-RECEIVING/REIM/RECEIVED'));
});
