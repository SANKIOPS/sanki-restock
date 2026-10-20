'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-no-pretag-'));
process.env.DATA_PATH=path.join(dir,'data.json');const file=path.join(dir,'expenses.json');fs.writeFileSync(file,'{}');
const {router}=require('../modules/expenses');
const account='IndusInd Bank 7883',draftId='BRD-NO-PRETAG',date='2098-06-02',amount=995.27;
const clone=v=>JSON.parse(JSON.stringify(v)),read=()=>JSON.parse(fs.readFileSync(file,'utf8')),write=v=>fs.writeFileSync(file,JSON.stringify(v));
function invoke(method,route,{body={},query={},role='owner',permissions={}}={}){
  return new Promise((resolve,reject)=>{let status=200,done=false;const timer=setTimeout(()=>reject(Error('Route timed out: '+route)),10000),finish=value=>{if(done)return;done=true;clearTimeout(timer);resolve({status,body:value});};
    const req={method,path:route,url:route,originalUrl:route,baseUrl:'',body,query,headers:{},get:()=>'',ip:'203.0.113.20',user:{role,roles:[role],username:role==='admin'?'prashant':role==='owner'?'owner-user':'bookkeeper',permissions}};
    const res={statusCode:200,headersSent:false,status(v){status=v;this.statusCode=v;return this;},setHeader(){},getHeader(){},removeHeader(){},json(v){this.headersSent=true;finish(v);return this;},send(v){finish(v);return this;},end:finish};
    try{router.handle(req,res,error=>{if(error){clearTimeout(timer);reject(error);}else finish(undefined);});}catch(error){clearTimeout(timer);reject(error);}
  });
}
const resolve=(body={},actor={})=>invoke('POST','/api/expenses/bank-statements/resolve',{...actor,body:{draftId,rowId:'bank-0',action:'create_adjustment',category:'FOOD EXPENSE',reason:'Verified expense belongs to the selected entity',...body}});
let baseline;
test.before(async()=>{const result=await invoke('GET','/api/expenses/config');assert.equal(result.status,200);baseline=read();});
test.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
function seed({tag,credit=false,source=false}={}){
  const s=clone(baseline);Object.assign(s,{expenses:{},adjustments:[],receipts:[],transfers:[],vendorAdvances:[],vendorOpeningPayables:[],salesRefunds:[],expenseRefunds:[],receivables:{},loanRepayments:[],bankTruthMovements:[],paytmSettlements:[],paytmPayoutPostings:[],reconciliationExpenses:[],procurementAccounting:{paymentsByPo:{}},creditCardSettlementAllocations:[],bankReconciliationDrafts:{},bankStatements:{},bankDateOverrides:{},bankReconciliationLinks:{},bankMovementAuthority:{},bankAmountAuthorityApplications:{},bankAmountAuthorityProposals:{},bankReconciliationApprovals:{},openingBalances:{},openingBalancesByNature:{SAMAST:{[account]:10000},PERSONAL:{[account]:0}},physicalBankOpeningBalances:{[account]:10000},auditLog:[],auditSeq:0});
  const bank={date,description:'UPI verified bank entry',reference:'NO-PRETAG-BANK',debit:credit?0:amount,credit:credit?amount:0,balance:credit?10995.27:9004.73};
  s.bankReconciliationDrafts[draftId]={id:draftId,account,nature:'SAMAST',transactions:[bank],summary:{from:date,to:date,openingBalance:10000,closingBalance:bank.balance,totalDebits:bank.debit,totalCredits:bank.credit,validated:true},resolutions:{},transactionOwnership:tag?{'bank-0':{nature:tag,reason:'Previously reviewed ownership',source:'manual',by:'owner-user',at:date}}:{},createdBy:'owner-user',createdAt:date+'T12:00:00Z',expiresAt:'2099-01-01T00:00:00Z',matchingPolicy:'balanced_date_amount_v5'};
  if(source)s.adjustments=[{id:'ADJ-EXISTING',nature:'SAMAST',account,date,amount:credit?amount:-amount,note:'Existing unrelated source reference'}];
  for(const[name,value]of Object.entries({'salary.json':{salaryPayments:[],advances:{},employees:{}},'salary-samast.json':{},'incentives.json':{},'procurement.json':{pos:{}},'credit-cards.json':{cards:{},payments:[],statements:{}}}))fs.writeFileSync(path.join(dir,name),JSON.stringify(value));
  write(s);return s;
}
async function denied(body,status,actor={}){const before=fs.readFileSync(file,'utf8'),result=await resolve(body,actor);assert.equal(result.status,status,JSON.stringify(result.body));assert.equal(fs.readFileSync(file,'utf8'),before,'failed validation never saves a tag, resolution, source, audit or bank change');return result;}

test('an untagged shared-bank expense can choose its entity in the expense form without a separate tagging step',async()=>{
  const before=seed(),result=await resolve({nature:' samast ',amount:1,date:'2000-01-01',account:'Counter Cash'},{role:'admin'});
  assert.equal(result.status,200,JSON.stringify(result.body));const after=read(),draft=after.bankReconciliationDrafts[draftId];
  assert.equal(draft.transactionOwnership['bank-0'].nature,'SAMAST');assert.equal(draft.resolutions['bank-0'].postingNature,'SAMAST');assert.equal(draft.resolutions['bank-0'].category,'FOOD EXPENSE');
  assert.deepEqual(draft.transactions,before.bankReconciliationDrafts[draftId].transactions);assert.equal(draft.resolutions['bank-0'].action,'create_adjustment');
  assert.equal(result.body.rows.find(row=>row.id==='bank-0').ownershipUnresolved,false);assert.equal(Object.keys(after.expenses).length,0);assert.equal(after.adjustments.length,0,'only a reviewed decision is staged until normal finalization');assert.equal(Object.keys(after.bankStatements).length,0);
  assert.ok(after.auditLog.some(row=>row.action==='BANK_RECONCILIATION_TRANSACTION_RETAGGED'));
});

test('split expense selection validates exact bank principal and charge before atomically recording its entity',async()=>{
  seed();const result=await resolve({action:'create_split_adjustment',nature:'SAMAST',principalAmount:990,chargeAmount:5.27,chargeCategory:'BANK CHARGES'});
  assert.equal(result.status,200,JSON.stringify(result.body));const draft=read().bankReconciliationDrafts[draftId];assert.equal(draft.resolutions['bank-0'].principalAmount,990);assert.equal(draft.resolutions['bank-0'].chargeAmount,5.27);assert.equal(draft.transactionOwnership['bank-0'].nature,'SAMAST');
  for(const body of [{category:'FORGED-CATEGORY'},{reason:''},{action:'create_split_adjustment',principalAmount:990,chargeAmount:6},{action:'create_split_adjustment',principalAmount:990,chargeAmount:5.27,chargeCategory:'FORGED-CHARGE'},{category:''}]){seed();await denied({nature:'SAMAST',...body},400);}
});

test('malformed, unauthorized and private expense entity selections cannot bypass existing permissions',async()=>{
  for(const nature of ['',null,{},['SAMAST'],'SANKI','UNKNOWN']){seed();await denied({nature},400);}
  seed();await denied({nature:'PERSONAL',category:'Food & Dining'},403,{role:'admin'});
  seed({tag:'PERSONAL'});await denied({nature:'SAMAST'},403,{role:'admin'});
  seed({tag:'PERSONAL'});await denied({},403,{role:'admin'});
  seed();await denied({nature:'SAMAST'},403,{role:'accounting'});
  seed();await denied({nature:'SAMAST'},403,{role:'samast_accounting'});
});

test('matching reliable entity tags retain existing bookkeeper access without a new retag audit',async()=>{
  const before=seed({tag:'PERSONAL'}),result=await resolve({nature:'PERSONAL',category:'Food & Dining'},{role:'accounting',permissions:{personalAccounting:true}});
  assert.equal(result.status,200,JSON.stringify(result.body));const after=read();assert.deepEqual(after.bankReconciliationDrafts[draftId].transactionOwnership,before.bankReconciliationDrafts[draftId].transactionOwnership);assert.ok(!after.auditLog.some(row=>row.action==='BANK_RECONCILIATION_TRANSACTION_RETAGGED'));
});

test('expense entity selection cannot replace an existing linked, resolved, finalized or reversal source',async()=>{
  for(const protect of [s=>{s.bankReconciliationDrafts[draftId].resolutions['bank-0']={action:'exclude',reason:'Prior decision'};},s=>{s.bankReconciliationDrafts[draftId].originalStatementPeriod={from:date,to:date};},s=>{s.bankStatements[account]={transactions:{final:{...s.bankReconciliationDrafts[draftId].transactions[0],id:'BTX-FINAL',entityTag:{nature:'SAMAST',source:'manual',status:'manual',reason:'Previously reviewed'},transactionNature:'SAMAST'}},imports:[]};},s=>{s.bankReconciliationDrafts[draftId].transactions[0].description='UPI REVERSAL failed payment';}]){const s=seed();protect(s);write(s);const before=fs.readFileSync(file,'utf8'),result=await resolve({nature:'SAMAST'});assert.ok([400,409].includes(result.status),JSON.stringify(result.body));assert.equal(fs.readFileSync(file,'utf8'),before);}
  seed({source:true});await denied({nature:'SAMAST'},409);
});

test('confirmed source matches infer the entity without requiring a preliminary manual tag',async()=>{
  for(const action of ['accept_match','link_existing']){
    const before=seed({source:true}),result=await resolve({action,category:'',appId:'ADJ-EXISTING'});assert.equal(result.status,200,JSON.stringify(result.body));
    const after=read(),draft=after.bankReconciliationDrafts[draftId];assert.equal(draft.resolutions['bank-0'].postingNature,'SAMAST');assert.deepEqual(draft.transactionOwnership,{});assert.deepEqual(after.adjustments,before.adjustments);assert.deepEqual(draft.transactions,before.bankReconciliationDrafts[draftId].transactions);assert.equal(after.transfers.length,0);
  }
});

test('ambiguous shared-bank transfer ownership stays unresolved and cannot default to the opened ledger or forged postingNature',async()=>{
  seed();const result=await denied({action:'create_internal_transfer',category:'',otherNature:'SANKI',otherAccount:'Counter Cash',principalAmount:amount,chargeAmount:0,postingNature:'SAMAST',nature:'SAMAST'},409);
  assert.match(result.body.error,/no reliable bank-side entity/);assert.doesNotMatch(result.body.error,/Tag this|beforehand/);assert.equal(read().transfers.length,0);
  seed({credit:true});const before=fs.readFileSync(file,'utf8'),incoming=await invoke('POST','/api/expenses/bank-statements/create-incoming',{body:{draftId,rowId:'bank-0',sourceKind:'internal',fromNature:'SANKI',fromAccount:'Counter Cash',postingNature:'SAMAST',note:'Unclear destination ownership'}});
  assert.equal(incoming.status,409,JSON.stringify(incoming.body));assert.match(incoming.body.error,/source account alone cannot prove/);assert.equal(fs.readFileSync(file,'utf8'),before);
});

test('known shared transfer ownership continues normal account-based transfer validation',async()=>{
  seed({tag:'SAMAST'});const result=await resolve({action:'create_internal_transfer',category:'',otherNature:'SAMAST',otherAccount:'Kirti Nagar Cash',principalAmount:amount,chargeAmount:0});
  assert.equal(result.status,200,JSON.stringify(result.body));const resolution=read().bankReconciliationDrafts[draftId].resolutions['bank-0'];assert.equal(resolution.postingNature,'SAMAST');assert.equal(resolution.otherNature,'SAMAST');assert.equal(resolution.otherAccount,'Kirti Nagar Cash');
});
