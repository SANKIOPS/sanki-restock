'use strict';

const test=require('node:test');
const assert=require('node:assert/strict');
const undoModule=require('../modules/bank-finalized-entry-undo');

const signature=row=>[row.date,Number(row.debit||0).toFixed(2),Number(row.credit||0).toFixed(2),row.reference||'',row.description||'',row.balance==null?'':Number(row.balance).toFixed(2)].join('|');

function fixture(){
  const first={date:'2026-10-10',debit:500,credit:0,reference:'REF-1',description:'Vendor debit',balance:9500};
  const second={date:'2026-10-11',debit:0,credit:1000,reference:'REF-2',description:'Other credit',balance:10500};
  const record={id:'BST-1',draftId:'BRD-1',finalizedAt:'2026-10-12T10:00:00.000Z',statementSummary:{format:'PDF',from:'2026-10-10',to:'2026-10-11',openingBalance:10000,closingBalance:10500,totalDebits:500,totalCredits:1000,validated:true},reconciliationRows:[
    {id:'bank-0',bank:{...first},linkedRecordIds:['EX-1/PAY-1'],ledger:{id:'EX-1/PAY-1'},decision:'Matched existing expense',reason:'Verified'},
    {id:'bank-1',bank:{...second},linkedRecordIds:['REC-1'],ledger:{id:'REC-1'},decision:'Matched existing receipt',reason:'Verified'}
  ]};
  const book={transactions:{one:{id:'BTX-1',...first},two:{id:'BTX-2',...second}},imports:[record]};
  const store={bankStatements:{'PERSONAL:ICICI Bank 0993':book},bankReconciliationDrafts:{},bankDateOverrides:{'EX-1/PAY-1':{reconciliationDraft:'BRD-1'}},bankReconciliationLinks:{'EX-1/PAY-1':{reconciliationDraft:'BRD-1'}},expenses:{'EX-1':{id:'EX-1',payments:[{id:'PAY-1'}]}},receipts:[{id:'REC-1'}]};
  return{store,book,record};
}

function harness(data=fixture()){
  const audits=[];
  const api=undoModule.create({
    loadStore:()=>data.store,
    saveStore:()=>{},
    audit:(_store,_req,action,subject,subjectId,payload)=>audits.push({action,subject,subjectId,payload}),
    isOwner:req=>req.user.role==='owner',
    isPrashant:req=>req.user.username==='prashant',
    canAccess:()=>true,
    nature:value=>String(value||'').toUpperCase(),
    bookKey:(nature,account)=>nature+':'+account,
    strongSignature:signature,
    findStored:(book,row)=>({transaction:Object.values(book.transactions).find(item=>signature(item)===signature(row.bank))||null})
  });
  const invoke=(handler,user,body)=>new Promise((resolve,reject)=>{let status=200;const req={user,body};const res={status(value){status=value;return this;},json(value){resolve({status,body:value});return this;}};try{handler(req,res);}catch(error){reject(error);}});
  return{...data,api,audits,invoke};
}

const prashant={username:'prashant',role:'admin'};
const owner={username:'owner',role:'owner'};
const requestBody={nature:'PERSONAL',account:'ICICI Bank 0993',recordId:'BST-1',rowId:'bank-0',reason:'Matched against the wrong expense'};

test('Prashant requests and Owner approves reopening one finalized row',async()=>{
  const h=harness();
  const requested=await h.invoke(h.api.request,prashant,requestBody);
  assert.equal(requested.status,202);
  assert.equal(requested.body.approvalPending,true);
  assert.equal(Object.keys(h.book.transactions).length,2,'a request alone changes nothing');
  assert.equal(h.record.reconciliationRows[0].linkedRecordIds[0],'EX-1/PAY-1');

  const approved=await h.invoke(h.api.decide,owner,{requestId:requested.body.request.id,decision:'approve'});
  assert.equal(approved.status,200);
  assert.equal(approved.body.reopened,true);
  assert.deepEqual(Object.keys(h.book.transactions),['two'],'only the selected stored row is reopened');
  assert.equal(h.store.expenses['EX-1'].id,'EX-1','the underlying expense is retained');
  assert.equal(h.store.bankDateOverrides['EX-1/PAY-1'],undefined);
  assert.equal(h.store.bankReconciliationLinks['EX-1/PAY-1'],undefined);
  assert.deepEqual(h.record.reconciliationRows[0].linkedRecordIds,[]);
  assert.equal(h.record.reconciliationRows[0].reopenedBy,'owner');
  const draft=h.store.bankReconciliationDrafts[approved.body.draftId];
  assert.equal(draft.transactions.length,2,'the retained statement rows reconstruct the review safely');
  assert.equal(draft.reopenedFromFinalized.targetIndex,0);
  assert.deepEqual(h.audits.map(row=>row.action),['FINALIZED_BANK_ENTRY_UNDO_REQUESTED','FINALIZED_BANK_ENTRY_UNDO_APPROVED']);
});

test('Owner can reject without changing finalized evidence',async()=>{
  const h=harness(),requested=await h.invoke(h.api.request,prashant,requestBody);
  const rejected=await h.invoke(h.api.decide,owner,{requestId:requested.body.request.id,decision:'reject',reason:'The original link is correct'});
  assert.equal(rejected.status,200);
  assert.equal(Object.keys(h.book.transactions).length,2);
  assert.equal(h.record.reconciliationRows[0].reopenedAt,undefined);
  assert.equal(rejected.body.request.status,'rejected');
});

test('approval stops if finalized evidence changed after the request',async()=>{
  const h=harness(),requested=await h.invoke(h.api.request,prashant,requestBody);
  h.record.reconciliationRows[0].reason='Changed after request';
  const approved=await h.invoke(h.api.decide,owner,{requestId:requested.body.request.id,decision:'approve'});
  assert.equal(approved.status,409);
  assert.match(approved.body.error,/changed after the request/i);
  assert.equal(Object.keys(h.book.transactions).length,2);
});

test('one-click undo is blocked when reconciliation created the accounting posting',async()=>{
  const data=fixture();data.store.adjustments=[{id:'ADJ-1',reconciliationDraft:'BRD-1',bankRowId:'bank-0'}];data.record.reconciliationRows[0].linkedRecordIds=['ADJ-1'];
  const h=harness(data),response=await h.invoke(h.api.request,prashant,requestBody);
  assert.equal(response.status,409);
  assert.match(response.body.error,/created an accounting posting/i);
  assert.equal(Object.keys(h.book.transactions).length,2);
});

test('only Prashant or Owner can request and only Owner can decide',async()=>{
  const h=harness(),other={username:'admin',role:'admin'};
  assert.equal((await h.invoke(h.api.request,other,requestBody)).status,403);
  const requested=await h.invoke(h.api.request,prashant,requestBody);
  assert.equal((await h.invoke(h.api.decide,prashant,{requestId:requested.body.request.id,decision:'approve'})).status,403);
});
