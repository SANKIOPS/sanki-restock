'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-bulk-bank-entity-'));
process.env.DATA_PATH=path.join(dir,'data.json');const file=path.join(dir,'expenses.json');fs.writeFileSync(file,'{}');
const {router,appBankMovements}=require('../modules/expenses');
const account='IndusInd Bank 7883',recordId='BST-BULK-1',draftId='BRD-BULK-1',date='2098-06-01';
const copy=v=>JSON.parse(JSON.stringify(v)),read=()=>JSON.parse(fs.readFileSync(file,'utf8')),write=v=>fs.writeFileSync(file,JSON.stringify(v));
function invoke(method,route,{body={},query={},role='owner',username='',permissions={}}={}){
  return new Promise((resolve,reject)=>{let status=200,done=false;const timer=setTimeout(()=>reject(Error('Route timed out: '+route)),15000),finish=value=>{if(done)return;done=true;clearTimeout(timer);resolve({status,body:value});};
    const req={method,path:route,url:route,originalUrl:route,baseUrl:'',body,query,headers:{},get:()=>'',ip:'203.0.113.20',user:{role,roles:[role],username:username||(role==='admin'?'prashant':'owner-user'),permissions}};
    const res={statusCode:200,headersSent:false,status(v){status=v;this.statusCode=v;return this;},setHeader(){},getHeader(){},removeHeader(){},json(v){this.headersSent=true;finish(v);return this;},send(v){finish(v);return this;},end:finish};
    try{router.handle(req,res,error=>{if(error){clearTimeout(timer);reject(error);}else finish(undefined);});}catch(error){clearTimeout(timer);reject(error);}
  });
}
const intent=()=>({account,nature:'SAMAST',targetNature:'PERSONAL',reason:'Owner verified these transactions belong to the personal books',items:[{recordId,rowId:'bank-0'},{recordId,rowId:'bank-1'}]});
const bulk=(body={},actor={})=>invoke('POST','/api/expenses/bank-statements/retag-bulk',{...actor,body:{...intent(),...body}});
let baseline;
test.before(async()=>{const result=await invoke('GET','/api/expenses/config');assert.equal(result.status,200);baseline=read();});
test.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
function seed({draft=false,active=false}={}){
  const s=copy(baseline);Object.assign(s,{expenses:{},adjustments:[],receipts:[],transfers:[],vendorAdvances:[],vendorOpeningPayables:[],salesRefunds:[],expenseRefunds:[],receivables:{},loanRepayments:[],bankTruthMovements:[],paytmSettlements:[],paytmPayoutPostings:[],reconciliationExpenses:[],procurementAccounting:{paymentsByPo:{}},creditCardSettlementAllocations:[],bankReconciliationDrafts:{},bankStatements:{},bankDateOverrides:{},bankReconciliationLinks:{},bankMovementAuthority:{},bankAmountAuthorityApplications:{},bankAmountAuthorityProposals:{},bankReconciliationApprovals:{APPROVED:{id:'APPROVED',status:'approved',approvedBy:'owner-user',approvedAt:date}},openingBalances:{},openingBalancesByNature:{SAMAST:{[account]:10000},PERSONAL:{[account]:0}},physicalBankOpeningBalances:{[account]:10000},auditLog:[],auditSeq:0});
  s.receipts=[{id:'REC-BULK-1',nature:'SAMAST',account,date,amount:1000,receiptType:'other_income',category:'Other Income',source:'Original receipt',proof:'/receipt.jpg'}];
  s.adjustments=[{id:'ADJ-BULK-1',nature:'SAMAST',account,date,amount:-40,note:'Original charge',proof:'/charge.jpg'}];s.reconciliationExpenses=[{id:'BRE-BULK-1',nature:'SAMAST',account,date,amount:40,adjustmentId:'ADJ-BULK-1',category:'BANK CHARGES'}];
  const bank=[{date,description:'Original receipt',reference:'RECEIPT-REF-1',credit:1000,debit:0,balance:11000},{date,description:'Original charge',reference:'CHARGE-REF-1',credit:0,debit:40,balance:10960}];
  const ids=['REC-BULK-1','ADJ-BULK-1'],rows=bank.map((value,index)=>({id:'bank-'+index,originalStatus:'matched',bank:{...value,transactionId:'BTX-BULK-'+index},ledger:{id:ids[index],date,entity:'SAMAST',description:value.description,debit:value.debit,credit:value.credit},linkedRecordIds:[ids[index]],entityTag:{nature:'SAMAST',status:'manual',source:'manual',sourceNature:'SAMAST',sourceConflict:false},transactionNature:'SAMAST',decision:'Matched existing source',reason:'Original finalized reason',resolvedBy:'owner-user',resolvedAt:date}));
  rows.forEach(row=>{row.bank.entityTag=copy(row.entityTag);row.bank.transactionNature='SAMAST';});
  if(!active)s.bankStatements[account]={transactions:Object.fromEntries(bank.map((value,index)=>['KEY-'+index,{...value,id:'BTX-BULK-'+index,entityTag:copy(rows[index].entityTag),transactionNature:'SAMAST'}])),imports:[{id:recordId,draftId:'FINALIZED-DRAFT',from:date,to:date,finalizedAt:date+'T12:00:00Z',finalizedBy:'owner-user',statementSummary:{openingBalance:10000,closingBalance:10960},balanceReconciled:true,balanceDifference:0,reconciliationRows:rows}],reconciledThrough:date,lastReconciliation:{at:date,by:'owner-user',reconciled:true,balanceReconciled:true,closingBalance:10960,through:date}};
  if(draft||active)s.bankReconciliationDrafts[draftId]={id:draftId,account,nature:'SAMAST',transactions:copy(bank),summary:{from:date,to:date,openingBalance:10000,closingBalance:10960,totalCredits:1000,totalDebits:40,validated:true},resolutions:active?{'bank-0':{action:'link_existing',appId:ids[0],reason:'Previously reviewed'},'bank-1':{action:'link_existing',appId:ids[1],reason:'Previously reviewed'}}:{},transactionOwnership:{},createdBy:'owner-user',createdAt:date+'T13:00:00Z',expiresAt:'2099-01-01T00:00:00Z'};
  s.bankDateOverrides[ids[0]]={bankDate:date,originalDate:date,bankReference:'RECEIPT-REF-1',reconciliationDraft:recordId,bankTransactionId:'BTX-BULK-0'};
  for(const[name,value]of Object.entries({'salary.json':{salaryPayments:[],advances:{},employees:{}},'salary-samast.json':{},'incentives.json':{},'procurement.json':{pos:{}},'credit-cards.json':{cards:{},payments:[],statements:{}}}))fs.writeFileSync(path.join(dir,name),JSON.stringify(value));
  write(s);return s;
}
async function preview(body={},actor={}){const result=await bulk({...body,dryRun:true},actor);assert.equal(result.status,200,JSON.stringify(result.body));assert.ok(result.body.fingerprint);return result.body;}
async function denied(body={},status=409,actor={}){const before=fs.readFileSync(file,'utf8'),result=await bulk({...body,dryRun:true},actor);assert.equal(result.status,status,JSON.stringify(result.body));assert.equal(fs.readFileSync(file,'utf8'),before,'entire rejected batch is read-only');return result;}
const physical=s=>appBankMovements(s,account,'SAMAST').map(row=>({id:row.id,date:row.date,debit:row.debit,credit:row.credit})).sort((a,b)=>a.id.localeCompare(b.id));

test('Owner previews and atomically changes two finalized entities without changing bank figures, links, approval or reconciliation status',async()=>{
  const before=seed(),raw=fs.readFileSync(file,'utf8'),review=await preview();assert.equal(fs.readFileSync(file,'utf8'),raw);assert.equal(review.updated,2);assert.ok(review.changes.every(row=>row.finalized&&row.fromNature==='SAMAST'&&row.toNature==='PERSONAL'));
  const result=await bulk({fingerprint:review.fingerprint});assert.equal(result.status,200,JSON.stringify(result.body));assert.equal(result.body.updated,2);
  const after=read();assert.equal(after.receipts[0].nature,'PERSONAL');assert.equal(after.adjustments[0].nature,'PERSONAL');assert.equal(after.reconciliationExpenses[0].nature,'PERSONAL');assert.deepEqual(physical(after),physical(before));assert.deepEqual(after.bankReconciliationApprovals,before.bankReconciliationApprovals);assert.deepEqual(after.bankDateOverrides,before.bankDateOverrides);assert.deepEqual(after.bankReconciliationLinks,before.bankReconciliationLinks);
  assert.deepEqual(after.bankStatements[account].lastReconciliation,before.bankStatements[account].lastReconciliation);
  const record=after.bankStatements[account].imports[0];assert.equal(record.finalizedAt,before.bankStatements[account].imports[0].finalizedAt);assert.equal(record.balanceReconciled,true);assert.equal(record.balanceDifference,0);
  for(let index=0;index<2;index++){const row=record.reconciliationRows[index],old=before.bankStatements[account].imports[0].reconciliationRows[index];assert.deepEqual(row.linkedRecordIds,old.linkedRecordIds);assert.equal(row.decision,old.decision);assert.equal(row.reason,old.reason);assert.equal(row.resolvedAt,old.resolvedAt);assert.equal(row.ledger.entity,'PERSONAL');assert.equal(row.transactionNature,'PERSONAL');assert.equal(row.bank.date,old.bank.date);assert.equal(row.bank.debit,old.bank.debit);assert.equal(row.bank.credit,old.bank.credit);assert.equal(after.bankStatements[account].transactions['KEY-'+index].transactionNature,'PERSONAL');assert.equal(row.correctionHistory[0].type,'entity_correction');}
  const saved=fs.readFileSync(file,'utf8'),again=await bulk({fingerprint:review.fingerprint});assert.equal(again.status,409);assert.equal(fs.readFileSync(file,'utf8'),saved,'stale retry cannot duplicate corrections');
});

test('already_reconciled draft selections resolve exact finalized evidence and Owner-only permission cannot be bypassed',async()=>{
  seed({draft:true});const items=[{draftId,rowId:'bank-0'},{draftId,rowId:'bank-1'}];await denied({items},403,{role:'admin',permissions:{personalAccounting:true}});
  const review=await preview({items});assert.ok(review.changes.every(row=>row.finalized));const done=await bulk({items,fingerprint:review.fingerprint});assert.equal(done.status,200,JSON.stringify(done.body));assert.equal(done.body.draft.rows.find(row=>row.id==='bank-0').status,'already_reconciled');assert.equal(done.body.draft.rows.find(row=>row.id==='bank-0').entityTag.nature,'PERSONAL');
});

test('overlapping finalized copies get one consistent ownership correction without becoming another payment',async()=>{
  const s=seed();s.bankStatements[account].imports.push({...copy(s.bankStatements[account].imports[0]),id:'BST-OVERLAP',reconciliationRows:[copy(s.bankStatements[account].imports[0].reconciliationRows[0])]});write(s);
  const review=await preview({items:[{recordId,rowId:'bank-0'}]}),done=await bulk({items:[{recordId,rowId:'bank-0'}],fingerprint:review.fingerprint});assert.equal(done.status,200,JSON.stringify(done.body));assert.equal(done.body.updated,1);
  const after=read();assert.equal(after.bankStatements[account].imports[1].reconciliationRows[0].entityTag.nature,'PERSONAL');assert.equal(after.receipts.length,1);assert.equal(Object.keys(after.bankStatements[account].transactions).length,2);
});

test('permissions, private sources, invalid entities and forged account scope fail closed',async()=>{
  seed();await denied({},403,{role:'claimant'});seed();await denied({},403,{role:'admin'});seed();await denied({targetNature:'SANKI'},400);seed();await denied({account:'Axis Bank 3448',nature:'SANKI'},400);seed();await denied({targetNature:['PERSONAL']},400);seed();await denied({reason:'short'},400);seed();await denied({items:[{recordId,rowId:'bank-0'},{recordId,rowId:'bank-0'}]},400);
});

test('missing or stale review fingerprint rejects the whole batch without partial source or history edits',async()=>{
  seed();let before=fs.readFileSync(file,'utf8'),result=await bulk();assert.equal(result.status,409);assert.equal(fs.readFileSync(file,'utf8'),before);
  const review=await preview(),s=read();s.receipts[0].note='New evidence since preview';write(s);before=fs.readFileSync(file,'utf8');result=await bulk({fingerprint:review.fingerprint});assert.equal(result.status,409);assert.match(result.body.error,/changed|reviewed/);assert.equal(fs.readFileSync(file,'utf8'),before);
});

test('one protected transfer makes a mixed batch atomic: no preceding safe receipt is retagged',async()=>{
  const s=seed();s.adjustments=[];s.transfers=[{id:'TR-PROTECTED',nature:'SAMAST',fromNature:'SAMAST',toNature:'SANKI',fromAccount:account,toAccount:'Counter Cash',amount:40,date,classification:'internal_transfer'}];const row=s.bankStatements[account].imports[0].reconciliationRows[1];row.linkedRecordIds=['TR-PROTECTED'];row.ledger.id='TR-PROTECTED';write(s);await denied();
});

test('supported single-payment expense attribution changes while bill/payment amount, date, status and proofs remain exact',async()=>{
  const s=seed();s.adjustments=[];s.reconciliationExpenses=[];const id='EX-BULK-1';s.expenses[id]={id,nature:'SAMAST',status:'paid',date,amount:40,requestedAmount:40,paidAmount:40,approvedAt:date,ledger:'Food & Dining',vendor:'Original vendor',billPhoto:'/bill.jpg',payments:[{id:'PAY-001',amount:40,date,account,proof:'/payment.jpg'}]};const row=s.bankStatements[account].imports[0].reconciliationRows[1];row.linkedRecordIds=[id+'/PAY-001'];row.ledger.id=id+'/PAY-001';write(s);
  const review=await preview(),done=await bulk({fingerprint:review.fingerprint});assert.equal(done.status,200,JSON.stringify(done.body));const e=read().expenses[id];assert.equal(e.nature,'PERSONAL');assert.equal(e.amount,40);assert.equal(e.date,date);assert.equal(e.status,'paid');assert.equal(e.billPhoto,'/bill.jpg');assert.equal(e.payments[0].amount,40);assert.equal(e.payments[0].date,date);assert.equal(e.payments[0].proof,'/payment.jpg');
});

test('category-incompatible or complex expenses block the entire selected batch',async()=>{
  for(const extra of [{ledger:'FOOD EXPENSE'},{ledger:'Food & Dining',paidAlready:true},{ledger:'Food & Dining',payments:[{id:'PAY-001',amount:40,date,account,batchPaymentId:'BATCH-1'}]}]){
    const s=seed();s.adjustments=[];s.expenses.EX={id:'EX',nature:'SAMAST',status:'paid',date,amount:40,paidAmount:40,approvedAt:date,ledger:'Food & Dining',payments:[{id:'PAY-001',amount:40,date,account}],...extra};const row=s.bankStatements[account].imports[0].reconciliationRows[1];row.linkedRecordIds=['EX/PAY-001'];row.ledger.id='EX/PAY-001';write(s);await denied();
  }
});

test('pending Owner approval with empty resolutions blocks matching-bank mutation in every draft',async()=>{
  const s=seed({draft:true});s.bankReconciliationApprovals.PENDING={id:'PENDING',draftId,status:'pending',requestedBy:'prashant'};write(s);await denied();
  const t=seed({draft:true});t.bankReconciliationDrafts[draftId].originalStatementPeriod={from:date,to:date};write(t);await denied();
});

test('inconsistent bank identity, overlapping ownership or a second bank claim requires individual repair',async()=>{
  for(const corrupt of [
    s=>{s.bankStatements[account].imports[0].reconciliationRows[0].bank.transactionId='BTX-BULK-1';},
    s=>{s.bankStatements[account].transactions['KEY-0'].transactionNature='PERSONAL';s.bankStatements[account].transactions['KEY-0'].entityTag.nature='PERSONAL';},
    s=>{const other=copy(s.bankStatements[account].imports[0]);other.id='OTHER';other.reconciliationRows[0].entityTag.nature='PERSONAL';s.bankStatements[account].imports.push(other);},
    s=>{s.bankStatements['Axis Bank 3448']={transactions:{},imports:[{id:'FOREIGN',finalizedAt:date,reconciliationRows:[{id:'bank-other',bank:{date,debit:0,credit:1000,reference:'OTHER'},linkedRecordIds:['REC-BULK-1']}]}]};}
  ]){const s=seed();corrupt(s);write(s);await denied();}
});

test('authorized Admin can preview/retag reviewed active rows while finalized histories stay unchanged',async()=>{
  seed({active:true});const items=[{draftId,rowId:'bank-0'},{draftId,rowId:'bank-1'}],actor={role:'admin',permissions:{personalAccounting:true}};
  const review=await preview({items},actor);assert.ok(review.changes.every(row=>!row.finalized));const result=await bulk({items,fingerprint:review.fingerprint},actor);assert.equal(result.status,200,JSON.stringify(result.body));assert.equal(Object.keys(read().bankStatements).length,0);assert.equal(read().bankReconciliationDrafts[draftId].resolutions['bank-0'].action,'link_existing');assert.equal(read().bankReconciliationDrafts[draftId].resolutions['bank-0'].postingNature,'PERSONAL');
});

test('a finalized bank-only row can change ownership without inventing a linked accounting entry',async()=>{
  const s=seed();s.adjustments=[];s.reconciliationExpenses=[];const row=s.bankStatements[account].imports[0].reconciliationRows[1];row.linkedRecordIds=[];row.ledger=null;write(s);
  const before=read(),items=[{recordId,rowId:'bank-1'}],review=await preview({items}),done=await bulk({items,fingerprint:review.fingerprint});assert.equal(done.status,200,JSON.stringify(done.body));assert.deepEqual(review.changes[0].sourceKinds,[]);
  const after=read();assert.equal(after.bankStatements[account].imports[0].reconciliationRows[1].entityTag.nature,'PERSONAL');assert.deepEqual(after.bankStatements[account].imports[0].reconciliationRows[1].linkedRecordIds,[]);assert.equal(after.bankStatements[account].imports[0].reconciliationRows[1].ledger,null);assert.deepEqual(after.receipts,before.receipts);assert.deepEqual(after.adjustments,[]);assert.deepEqual(after.expenses,{});assert.deepEqual(after.bankStatements[account].lastReconciliation,before.bankStatements[account].lastReconciliation);
});

test('partially collected receivables and protected vendor-advance refunds cannot move unrelated balances',async()=>{
  const s=seed();s.receipts=[];s.receivables.RCV={id:'RCV',nature:'SAMAST',party:'Original debtor',date,amount:100000,collections:[{id:'COL-001',amount:1000,date,account}]};let row=s.bankStatements[account].imports[0].reconciliationRows[0];row.linkedRecordIds=['RCV/COL-001'];row.ledger.id='RCV/COL-001';write(s);const partial=await denied({items:[{recordId,rowId:'bank-0'}]});assert.match(partial.body.error,/partially|individual/);
  const t=seed();t.adjustments=[];t.vendorAdvances=[{id:'VADV-REFUNDED',nature:'SAMAST',account,date,amount:40,remainingAmount:40,vendor:'Original vendor',applications:[]}];t.expenseRefunds=[{id:'RF-PROTECTED',nature:'SAMAST',date,status:'received',sources:[{advanceId:'VADV-REFUNDED',amount:40}],components:[{type:'vendor_credit',vendorAdvanceId:'VADV-REFUNDED',amount:40}]}];row=t.bankStatements[account].imports[0].reconciliationRows[1];row.linkedRecordIds=['VADV-REFUNDED'];row.ledger.id='VADV-REFUNDED';write(t);await denied({items:[{recordId,rowId:'bank-1'}]});
});

test('rental, loan and settlement receipts stay in dedicated source workflows rather than being partly reclassified',async()=>{
  for(const protectedFields of [{receiptType:'rental_security',tenancyId:'TENANT-1',rentalKind:'security'},{receiptType:'loan_received',loanTerms:{lenderKind:'person'}},{paytmTransactionId:'PAYTM-PROTECTED'}]){
    const s=seed();Object.assign(s.receipts[0],protectedFields);write(s);await denied({items:[{recordId,rowId:'bank-0'}]});
  }
});
