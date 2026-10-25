'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {assertUnlocked,assertPreserved,references,validDate}=require('../modules/source-correction-guards');
const clone=x=>JSON.parse(JSON.stringify(x));
const day='2098-07-13',otherDay='2098-07-14',account='Synthetic Bank 1234',id='TR-SYNTHETIC';
const options=()=>({ids:[id],scopes:[{nature:'SAMAST',account}],dates:[day,otherDay],records:[{id,date:day,amount:212.13,classification:'credit_card_payment',creditCardId:'CC-SYNTHETIC',creditCardPaymentId:'CCP-SYNTHETIC'}]});
test('guard is read-only and permits source-owned graph selflinks',()=>{
  const s={transfers:clone(options().records),auditLog:[{before:{id}}],expenses:{'EX-ORIGINAL':{payments:[{id:'PAY-SYNTHETIC'}]}},bankReconciliationLinks:{'EX-ORIGINAL/PAY-SYNTHETIC':{date:day}}},before=clone(s),o=options(),ob=clone(o);
  assert.doesNotThrow(()=>assertUnlocked(s,o));assert.deepEqual(s,before);assert.deepEqual(o,ob);
});
for(const [label,change] of [
  ['exact keyed override',s=>s.bankDateOverrides={[id]:{}}],
  ['received alias keyed override',s=>s.bankDateOverrides={[id+'/RECEIVED']:{}}],
  ['comma linked identifiers',s=>s.bankStatements={B:{imports:[{linkedRecordIds:['TR-OTHER, '+id+'/RECEIVED']}]}}],
  ['nested grouped bank statement',s=>s.bankStatements={bank:{imports:[{ledger:{sourceIds:[id]}}]}}],
  ['draft appIds',s=>s.bankReconciliationDrafts={D:{resolutions:{'bank-1':{appIds:[id]}}}}],
  ['authority sourceIds',s=>s.bankMovementAuthority={a:{sourceIds:[id]}}],
  ['pending bank amount plan',s=>s.bankAmountAuthorityProposals={p:{movementId:id}}],
  ['applied bank amount plan',s=>s.bankAmountAuthorityApplications={p:{movementId:id}}],
  ['physical other entity book',s=>s.bankStatements={['PERSONAL|'+account]:{reconciledThrough:day}}],
  ['shared arbitrary book account',s=>s.bankStatements={SHARED:{account,reconciledThrough:otherDay}}],
  ['custom book key',s=>s.bankStatements={SHARED:{reconciledThrough:day}}],
  ['invalid closed period',s=>s.bankStatements={[account]:{reconciledThrough:'2098-13-05'}}],
  ['other entity opening',s=>s.openingBalanceDatesByNature={PERSONAL:{[account]:otherDay}}],
  ['pending cash',s=>s.cashReconciliations=[{account,status:'pending',verifiedThrough:otherDay}]],
  ['approved cash unknown boundary',s=>s.cashReconciliations=[{account,status:'approved'}]],
  ['pending Owner other entity',s=>s.bankReconciliationApprovals={A:{account,nature:'PERSONAL',status:'pending',from:day,to:day}}],
  ['pending Owner new date',s=>s.bankReconciliationApprovals={A:{account,status:'pending',from:otherDay,to:otherDay}}],
  ['pending Owner draft-derived',s=>{s.bankReconciliationDrafts={D:{account,summary:{from:day,to:otherDay}}};s.bankReconciliationApprovals={A:{draftId:'D',status:'pending'}};}],
  ['pending Owner invalid boundary',s=>s.bankReconciliationApprovals={A:{account,status:'pending',from:'2098-02-30'}}],
  ['pending Owner reversed boundary',s=>s.bankReconciliationApprovals={A:{account,status:'pending',from:otherDay,to:day}}],
  ['orphan pending Owner',s=>s.bankReconciliationApprovals={A:{draftId:'MISSING',status:'pending'}}],
  ['corrupt evidence collection',s=>s.bankReconciliationLinks='unreadable']
])test('source locks fail closed: '+label,()=>{const s={},o=options();o.bookKey=()=> 'SHARED';change(s);const before=clone(s);assert.throws(()=>assertUnlocked(s,o),{status:409});assert.deepEqual(s,before);});
for(const field of ['accountingExcluded','historicalSettlement','provisional','bankReconciliationDraft','bankMovementAuthority','creditCardStatementId','settledByTransferId','salaryAdvanceId','manualSaleId','batchId'])test('record marker locked: '+field,()=>{const o=options();o.records[0][field]=true;assert.throws(()=>assertUnlocked({},o),{status:409});});
test('missing identity or invalid original/proposed dates fail closed',()=>{
  for(const patch of [{ids:[]},{scopes:[]},{dates:[]},{dates:['2098-02-30']},{scopes:[{nature:'unknown',account}]}])assert.throws(()=>assertUnlocked({},{...options(),...patch}),{status:409});
});
test('independent external linked records prevent corrections but audit prose does not',()=>{
  const o=options();o.external={salary:{payments:[{transferId:id}]}};assert.throws(()=>assertUnlocked({},o),{status:409});
  o.external={salary:{note:'example '+id+' only prose'}};assert.doesNotThrow(()=>assertUnlocked({},o));
});
test('unrelated approvals and explicitly completed approvals do not lock',()=>{
  const s={bankReconciliationApprovals:{A:{account:'Different Bank',status:'pending',from:day,to:otherDay},B:{account,status:'approved',from:day,to:otherDay},C:{account,status:'pending',from:'2098-08-01',to:'2098-08-31'}},cashReconciliations:[{account,status:'approved',verifiedThrough:'2098-06-01'}]};
  assert.doesNotThrow(()=>assertUnlocked(s,options()));
});
test('reference indexing finds values and keys without substring matching',()=>{const index=references({[id]:['CCP-SYNTHETIC',{sourceIds:['R-SYNTHETIC']}],note:'prefix XX suffix'});assert.ok(index.has(id));assert.ok(index.has('CCP-SYNTHETIC'));assert.ok(index.has('R-SYNTHETIC'));assert.equal(index.has('XX'),false);});
test('dates reject impossible calendar days',()=>{assert.equal(validDate('2098-02-29'),false);assert.equal(validDate('2096-02-29'),true);assert.equal(validDate(day),true);});
test('shared commit invariant disallows changes outside explicit source collections',()=>{
  const before={expenses:{E:{amount:10}},bankReconciliationApprovals:{A:{status:'pending'}},transfers:[],auditLog:[]};
  const after=clone(before);after.transfers.push({id});assert.doesNotThrow(()=>assertPreserved(before,after,['transfers','auditLog']));
  after.bankReconciliationApprovals.A.status='withdrawn';assert.throws(()=>assertPreserved(before,after,['transfers','auditLog']),{status:409});
  const added={...before,openingBalances:{}};assert.throws(()=>assertPreserved(before,added,['transfers','auditLog']),{status:409});
});
for(const source of ['book','opening','cash','approval'])test('physical account aliases cannot evade '+source+' locks',()=>{
  const s={},o=options();o.canonicalAccount=value=>String(value).replace(/^Old Synthetic/i,'Synthetic');
  const alias='old synthetic bank 1234';
  if(source==='book')s.bankStatements={['PERSONAL|'+alias]:{reconciledThrough:day}};
  if(source==='opening')s.openingBalanceDates={[alias]:otherDay};
  if(source==='cash')s.cashReconciliations=[{account:alias,status:'pending',verifiedThrough:day}];
  if(source==='approval')s.bankReconciliationApprovals={A:{account:alias,status:'pending',from:day,to:otherDay}};
  assert.throws(()=>assertUnlocked(s,o),{status:409});
});
