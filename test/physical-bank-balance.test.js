'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {project}=require('../modules/physical-bank-balance');
const account='IndusInd Bank 7883';
const book={reconciledThrough:'2026-10-19',lastReconciliation:{through:'2026-10-19',closingBalance:370197.82,reconciled:true},transactions:{}};
const row=(id,date,amount,entity='PERSONAL')=>({id,date,credit:amount>0?amount:0,debit:amount<0?-amount:0,entity});
test('one physical closing replaces entity openings and earlier payments; later mixed-entity spending is counted once',()=>{
 const movements=[row('rent','2026-10-15',100000,'SAMAST'),row('old','2026-10-18',-50000),row('samast','2026-10-20',-400,'SAMAST'),row('personal','2026-10-21',-600),row('business','2026-10-22',-1560,'SANKI'),row('transfer','2026-11-08',20000),row('future','2026-11-14',-999)];
 const before=JSON.stringify({book,movements});const result=project({book,movements,opening:111710.40,to:'2026-11-13'});
 assert.equal(result.balance,387637.82);assert.equal(result.rows.length,4);assert.equal(result.verified,false);assert.equal(result.reconciledThrough,'2026-10-19');assert.equal(JSON.stringify({book,movements}),before);
 assert.equal(project({book,movements,to:'2026-10-19'}).balance,370197.82);
});
test('same-value genuine rows stay distinct; repeated IDs do not double count',()=>{
 const one=row('one','2026-10-20',-5000),two=row('two','2026-10-20',-5000);
 assert.equal(project({book,movements:[one,one,two],to:'2026-10-20'}).balance,360197.82);
 assert.throws(()=>project({book,movements:[one,{...one,debit:6000}],to:'2026-10-20'}),/Conflicting/);
});
test('historical date does not borrow a future bank closing',()=>{
 const result=project({book,movements:[row('income','2026-08-23',1000),row('expense','2026-08-24',-200)],opening:500,to:'2026-08-24'});
 assert.equal(result.balance,1300);assert.equal(result.anchor,null);assert.ok(result.warnings.length);
});
test('contradictory finalized closing fails rather than manufacturing an adjustment',()=>{
 const b={...book,imports:[{id:'bad',to:'2026-10-19',closingBalance:42,reconciled:true}]};
 assert.throws(()=>project({book:b,to:'2026-11-13'}),/different closing|conflict/i);
});
test('read-only balance uses validated bank closing when its ledger difference is pending',()=>{
 const pending={reconciledThrough:'2026-11-15',lastReconciliation:{through:'2026-11-15',closingBalance:109402.35,balanceReconciled:false,closingBalanceDeferred:true,balanceDifference:-328232,draftId:'DRAFT-PENDING'},imports:[{id:'BST-PENDING',draftId:'DRAFT-PENDING',to:'2026-11-15',balanceReconciled:false,closingBalanceDeferred:true,balanceDifference:-328232,statementSummary:{to:'2026-11-15',closingBalance:109402.35,validated:true}}]};
 const movements=[row('already-covered','2026-11-15',-999999),row('rent','2026-11-18',136200,'SAMAST'),row('later-debits','2026-11-18',-80892,'PERSONAL')];
 assert.throws(()=>project({book:pending,movements,to:'2026-11-18'}),/deferred|unreconciled/i);
 const result=project({book:pending,movements,to:'2026-11-18',allowPendingBankAnchor:true});
 assert.equal(result.balance,164710.35);
 assert.equal(result.anchor.anchorRecordId,'BST-PENDING');
 assert.equal(result.anchor.balancePending,true);
 assert.equal(result.verified,false);
 assert.match(result.basis,/ledger balance pending/i);
 assert.match(result.warnings[0],/pending review/i);
});
test('pending statement display anchor remains unavailable when latest closings conflict',()=>{
 const pending={reconciledThrough:'2026-11-15',imports:[
  {id:'A',to:'2026-11-15',balanceReconciled:false,statementSummary:{to:'2026-11-15',closingBalance:100,validated:true}},
  {id:'B',to:'2026-11-15',balanceReconciled:false,statementSummary:{to:'2026-11-15',closingBalance:200,validated:true}}
 ]};
 assert.throws(()=>project({book:pending,to:'2026-11-18',allowPendingBankAnchor:true}),/deferred|unreconciled|different closing|conflict/i);
});
test('latest validated temporary statement closing fixes display while historical opening evidence remains blocked',()=>{
 const finalized={reconciledThrough:'2026-11-15',lastReconciliation:{through:'2026-11-15',closingBalance:109402.35,balanceReconciled:false,closingBalanceDeferred:true,balanceDifference:-328232,draftId:'DRAFT-OLD'},imports:[{id:'BST-OLD',draftId:'DRAFT-OLD',to:'2026-11-15',balanceReconciled:false,closingBalanceDeferred:true,statementSummary:{to:'2026-11-15',closingBalance:109402.35,validated:true}}]};
 const draft={id:'BRD-CURRENT',account,to:'2026-11-18',summary:{to:'2026-11-18',openingBalance:109402.35,totalDebits:80892,totalCredits:136200,closingBalance:164710.35,validated:true}};
 const result=project({book:finalized,pendingStatements:[draft],movements:[row('posted-rent','2026-11-18',136200,'SAMAST'),row('posted-expenses','2026-11-18',-4160,'PERSONAL')],to:'2026-11-23',allowPendingBankAnchor:true});
 assert.equal(result.balance,164710.35);
 assert.equal(result.reconciledThrough,'2026-11-18');
 assert.equal(result.anchor.anchorRecordId,'BRD-CURRENT');
 assert.equal(result.anchor.provisionalStatement,true);
 assert.match(result.basis,/temporary bank statement/i);
 assert.match(result.warnings[0],/final reconciliation is still pending/i);
});
test('conflicting temporary statement closings do not manufacture a physical balance',()=>{
 const drafts=[
  {id:'A',to:'2026-11-18',summary:{to:'2026-11-18',closingBalance:100,validated:true}},
  {id:'B',to:'2026-11-18',summary:{to:'2026-11-18',closingBalance:200,validated:true}}
 ];
 assert.throws(()=>project({book,pendingStatements:drafts,to:'2026-11-23',allowPendingBankAnchor:true}),/different closing balances/i);
});
