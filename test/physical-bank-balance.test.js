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
