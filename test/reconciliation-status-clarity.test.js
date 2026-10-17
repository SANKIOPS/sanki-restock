'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { cashVerificationView } = require('../modules/cash-verification-status');
const { paytmPreviewStatuses, registerPaytmReports } = require('../modules/paytm-reports-routes');
const ui = require('../public/reconciliation-status');
const tx = extra => ({ transactionId: 'PT-CLARITY', date: '2026-10-09', amount: 100, commission: 1, gst: .18, platformFee: 0, settledAmount: 98.82, utr: 'UTR-CLARITY', payoutId: 'P-CLARITY', settledDate: '2026-10-10', ...extra });
const draft = extra => ({ id: 'PREVIEW-CLARITY', at: '2026-10-10T08:00:00Z', by: 'admin', sources: [{name: 'Original.csv'}], transactions: [], enrichments: [], warnings: [], ...extra });

test('cash shows unchanged submission snapshots beside current checks, equally for Owner and Admin', () => {
  const rows = [
    { id:'APPROVED', status:'approved', verifiedThrough:'2026-10-01', countedBalance:24230, ledgerBalance:24230, difference:0, approvedBy:'Owner' },
    { id:'CCR-OLD', status:'pending', verifiedThrough:'2026-10-09', countedBalance:50470, ledgerBalance:50470, difference:0, requestedBy:'admin' },
    { id:'CCR-NEW', status:'pending', verifiedThrough:'2026-10-15', countedBalance:924, ledgerBalance:924, difference:0, requestedBy:'admin' }
  ];
  const before = JSON.stringify(rows), view = cashVerificationView(rows, date => date === '2026-10-09' ? 4168 : 924);
  assert.equal(view.reconciledThrough,'2026-10-01');
  assert.deepEqual(view.reconciliationStatus.counts,{pending:2,reviewRequired:1,awaitingOwnerApproval:1});
  const changed=view.pending.find(x=>x.id==='CCR-OLD');
  assert.equal(changed.difference,0);assert.equal(changed.ledgerBalance,50470);
  assert.equal(changed.currentLedgerBalance,4168);assert.equal(changed.currentDifference,46302);assert.equal(changed.reviewRequired,true);
  assert.match(ui.cashRequest(changed),/Submitted snapshot/);assert.match(ui.cashRequest(changed),/Current check/);assert.match(ui.cashRequest(changed),/46,302/);
  assert.equal(ui.cash({...view,isOwner:true}),ui.cash({...view,isOwner:false}));assert.match(ui.cash(view),/review required/);
  assert.equal(JSON.stringify(rows),before,'status reads never rewrite submitted balances or approvals');
});

test('invalid cash numbers and dates cannot become zero-difference approval-only status',()=>{
  for(const extra of [{countedBalance:''},{countedBalance:-1},{ledgerBalance:null},{difference:3},{verifiedThrough:'2026-02-30'}]){
    const view=cashVerificationView([{id:'INVALID',status:'pending',verifiedThrough:'2026-10-15',countedBalance:0,ledgerBalance:0,difference:0,...extra}],()=>0);
    assert.equal(view.pending[0].reviewRequired,true);assert.equal(view.reconciliationStatus.code,'review_required');
  }
});

test('already-saved and duplicate-only Paytm previews are informational, never deleted on read',()=>{
  const t=tx(), store={paytmReportTransactions:{[t.transactionId]:t},paytmReportDrafts:{OLD:draft({transactions:[t]}),DUP:draft({id:'DUP',duplicates:1})}};
  const before=JSON.stringify(store), previews=paytmPreviewStatuses(store);
  assert.equal(previews.length,2);assert.ok(previews.every(x=>!x.actionable&&x.code==='already_covered'));
  assert.match(ui.paytmPreviews(previews),/not pending settlements/);assert.doesNotMatch(ui.paytmPreviews(previews),/data-pt-resume-preview/);
  assert.equal(JSON.stringify(store),before);
});

test('new rows, enrichments, source warnings and financial conflicts remain actionable even with posted payouts',()=>{
  const old=tx({utr:undefined}), full=tx();
  const cases=[
    {store:{},preview:draft({transactions:[full]}),field:'newCount'},
    {store:{[old.transactionId]:old},preview:draft({enrichments:[full]}),field:'enrichmentCount'},
    {store:{[full.transactionId]:full},preview:draft({transactions:[tx({amount:101})]}),field:'conflictCount'},
    {store:{[full.transactionId]:full},preview:draft({warnings:['Original file has a genuine financial conflict.']}),field:null}
  ];
  for(const c of cases){
    const result=paytmPreviewStatuses({paytmReportTransactions:c.store,paytmReportDrafts:{P:c.preview},paytmPayoutPostings:[{payoutId:full.payoutId,transactionIds:[full.transactionId]}]})[0];
    assert.equal(result.actionable,true);if(c.field)assert.equal(result[c.field],1);
    assert.match(ui.paytmPreviews([result]),/Review saved upload preview/);assert.ok(result.reasons.length);
  }
});

function harness(store,allowed=true){
  const handlers={}, audits=[];let saves=0;
  registerPaytmReports({get:(url,fn)=>handlers[url]=fn,post:(url,...fns)=>handlers[url]=fns.at(-1)}, {loadStore:()=>store,saveStore:()=>saves++,audit:(_s,_r,type)=>audits.push(type),canAccess:()=>allowed,upload:{array:()=>()=>{}},view:()=>({}),today:()=> '2026-10-15',orders:()=>[],saleRows:()=>[]});
  function invoke(url,extra={}){let status=200,body;handlers[url]({user:{username:'admin'},params:{id:'PREVIEW-CLARITY'},body:{draftId:'PREVIEW-CLARITY'},...extra},{status(n){status=n;return this;},json(v){body=v;return this;}});return{status,body};}
  return {invoke,audits,get saves(){return saves;}};
}
test('resume restores saved previews read-only, keeps permissions, and does not post or auto-confirm',()=>{
  const store={paytmReportDrafts:{'PREVIEW-CLARITY':draft({transactions:[tx()]})},paytmReportTransactions:{}},h=harness(store),before=JSON.stringify(store);
  const r=h.invoke('/api/expenses/paytm-reports/previews/:id');assert.equal(r.status,200);assert.equal(r.body.newTransactions,1);assert.equal(r.body.payouts.length,1);assert.equal(h.saves,0);assert.deepEqual(h.audits,[]);assert.equal(JSON.stringify(store),before);
  assert.equal(harness(store,false).invoke('/api/expenses/paytm-reports/previews/:id').status,403);
  assert.equal(h.invoke('/api/expenses/paytm-reports/previews/:id',{params:{id:'MISSING'}}).status,404);
});

test('a resumed stale preview is revalidated atomically; later conflict prevents all evidence writes',()=>{
  const a=tx({transactionId:'NEW'}),b=tx({transactionId:'CONFLICT',amount:101}),store={paytmReportTransactions:{CONFLICT:tx({transactionId:'CONFLICT'})},paytmReportDrafts:{'PREVIEW-CLARITY':draft({transactions:[a,b]})}};
  const before=JSON.stringify(store),h=harness(store),r=h.invoke('/api/expenses/paytm-reports/confirm');
  assert.equal(r.status,409);assert.match(r.body.error,/CONFLICT.*conflicts/);assert.equal(h.saves,0);assert.equal(JSON.stringify(store),before);
});

test('source-conflict warnings cannot be acknowledged away by confirming an empty duplicate preview',()=>{
  const store={paytmReportTransactions:{},paytmReportDrafts:{'PREVIEW-CLARITY':draft({warnings:['Previously imported transaction BAD has a genuine financial or identifier conflict.']})}},h=harness(store),before=JSON.stringify(store);
  assert.equal(h.invoke('/api/expenses/paytm-reports/confirm').status,409);assert.equal(h.saves,0);assert.equal(JSON.stringify(store),before);
});

test('confirming a resumed preview uses existing import audit, never posts a payout, and preserves duplicate evidence',()=>{
  const t=tx(),store={paytmReportTransactions:{},paytmReportDrafts:{'PREVIEW-CLARITY':draft({transactions:[t]})}},h=harness(store);
  const r=h.invoke('/api/expenses/paytm-reports/confirm');assert.equal(r.status,200);assert.equal(r.body.added,1);assert.equal(h.saves,1);assert.ok(h.audits.includes('PAYTM_REPORT_IMPORTED'));
  assert.equal(store.paytmPayoutPostings,undefined);assert.equal(store.paytmVerifiedSettlements,undefined);assert.equal(store.paytmReportDrafts['PREVIEW-CLARITY'],undefined);
});

test('malformed previews remain review-required instead of crashing or being treated as covered',()=>{
  for(const bad of [{transactions:null},{enrichments:{}},{transactions:[null]},{transactions:[{}]}]){
    const result=paytmPreviewStatuses({paytmReportDrafts:{P:draft(bad)}})[0];assert.equal(result.actionable,true);assert.ok(result.conflictCount);
  }
});

test('cash-file history does not falsely report physical cash as unreconciled; all new labels escape source text',()=>{
  const text=ui.bankSaved({account:'Counter Cash'});assert.match(text,/separate from physical cash verification/);assert.doesNotMatch(text,/Statement or register required/);
  assert.match(ui.bankSaved({account:'Axis Bank 3448'}),/Statement or register required/);
  const malicious=draft({id:'<script>x</script>',sources:[{name:'<img onerror=x>'}],actionable:true,reasons:['<script>x</script>']});
  assert.doesNotMatch(ui.paytmPreviews([malicious]),/<script>|<img/);assert.match(ui.paytmPreviews([malicious]),/&lt;script&gt;/);
});
