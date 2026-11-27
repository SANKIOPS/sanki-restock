'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {build}=require('../modules/shared-account-entity-position');
const account='IndusInd Bank 7883';
const store=()=>({openingBalancesByNature:{SAMAST:{[account]:1000},PERSONAL:{[account]:500}},openingBalanceDatesByNature:{SAMAST:{[account]:'2026-08-22'},PERSONAL:{[account]:'2026-08-22'}}});
const movements=()=>[
  {id:'S-IN',date:'2026-08-25',entity:'SAMAST',credit:1000,debit:0},
  {id:'S-OUT',date:'2026-09-10',entity:'SAMAST',credit:0,debit:300},
  {id:'P-BEFORE',date:'2026-08-30',entity:'PERSONAL',credit:0,debit:600},
  {id:'P-PERIOD',date:'2026-09-12',entity:'PERSONAL',credit:0,debit:200}
];

test('shared account proves only the reconciled Personal deficit',()=>{
  const books=store(),before=JSON.stringify(books),result=build(books,{account,from:'2026-09-01',to:'2026-09-30',movements:movements(),physicalBalance:1400,verificationThrough:'2026-09-30'});
  assert.equal(result.proven,true);assert.deepEqual(result.openingPosition,{SAMAST:2000,PERSONAL:-100});assert.deepEqual(result.closingPosition,{SAMAST:1700,PERSONAL:-300});
  assert.equal(result.personalDeficitOpening,100);assert.equal(result.personalDeficitClosing,300);assert.equal(result.periodIncrease,200);assert.equal(result.periodRestored,0);assert.equal(result.physicalDifference,0);assert.equal(JSON.stringify(books),before);
});

test('restored shared-account money reduces the deficit without becoming negative use',()=>{
  const rows=movements().concat({id:'P-RESTORE',date:'2026-09-20',entity:'PERSONAL',credit:250,debit:0});
  const result=build(store(),{account,from:'2026-09-01',to:'2026-09-30',movements:rows,physicalBalance:1650,verificationThrough:'2026-09-30'});
  assert.equal(result.proven,true);assert.equal(result.personalDeficitOpening,100);assert.equal(result.personalDeficitClosing,50);assert.equal(result.periodIncrease,0);assert.equal(result.periodRestored,50);
});

test('missing openings, unassigned rows, authority conflicts or a physical mismatch keep the result pending',()=>{
  const noOpening=store();delete noOpening.openingBalanceDatesByNature.PERSONAL[account];
  const cases=[
    build(noOpening,{account,from:'2026-09-01',to:'2026-09-30',movements:movements(),physicalBalance:1400,verificationThrough:'2026-09-30'}),
    build(store(),{account,from:'2026-09-01',to:'2026-09-30',movements:movements().concat({id:'UNKNOWN',date:'2026-09-15',credit:50,debit:0}),physicalBalance:1450,verificationThrough:'2026-09-30'}),
    build(store(),{account,from:'2026-09-01',to:'2026-09-30',movements:movements(),physicalBalance:1400,conflicts:[{id:'AUTH'}],verificationThrough:'2026-09-30'}),
    build(store(),{account,from:'2026-09-01',to:'2026-09-30',movements:movements(),physicalBalance:1399,verificationThrough:'2026-09-30'})
  ];
  for(const result of cases)assert.equal(result.proven,false);
  assert.match(cases[0].blockers.join(' '),/Opening evidence/);assert.match(cases[1].blockers.join(' '),/entity tag/);assert.match(cases[2].blockers.join(' '),/authority conflict/);assert.match(cases[3].blockers.join(' '),/₹1.00/);
});

test('conflicting duplicate movement identities block the shared-account proof',()=>{
  const rows=movements().concat({id:'P-PERIOD',date:'2026-09-12',entity:'PERSONAL',credit:0,debit:201});
  const result=build(store(),{account,from:'2026-09-01',to:'2026-09-30',movements:rows,physicalBalance:1400,verificationThrough:'2026-09-30'});
  assert.equal(result.proven,false);assert.equal(result.movementConflicts.length,1);assert.match(result.blockers.join(' '),/duplicate movement identity/);
});

test('a balanced ledger is still not proven beyond the bank verification date',()=>{
  const result=build(store(),{account,from:'2026-09-01',to:'2026-09-30',movements:movements(),physicalBalance:1400,verificationThrough:'2026-09-18'});
  assert.equal(result.proven,false);assert.match(result.blockers.join(' '),/not verified through 2026-09-30/);
});

test('invalid movement amounts and mismatched or late opening dates fail closed',()=>{
  const badDates=store();badDates.openingBalanceDatesByNature.PERSONAL[account]='2026-09-02';
  const dates=build(badDates,{account,from:'2026-09-01',to:'2026-09-30',movements:movements(),physicalBalance:1400,verificationThrough:'2026-09-30'});
  const amounts=build(store(),{account,from:'2026-09-01',to:'2026-09-30',movements:movements().concat({id:'BAD',date:'2026-09-20',entity:'PERSONAL',debit:'not money',credit:0}),physicalBalance:1400,verificationThrough:'2026-09-30'});
  assert.equal(dates.proven,false);assert.match(dates.blockers.join(' '),/different dates|begins after/);
  assert.equal(amounts.proven,false);assert.equal(amounts.invalidMovements.length,1);assert.match(amounts.blockers.join(' '),/invalid identity, date, debit or credit/);
});

test('a movement with no reliable identity or date cannot silently disappear',()=>{
  const rows=movements().concat(
    {id:'',date:'2026-09-22',entity:'PERSONAL',debit:25,credit:0},
    {id:'BAD-DATE',date:'not-a-date',entity:'SAMAST',debit:0,credit:25}
  );
  const result=build(store(),{account,from:'2026-09-01',to:'2026-09-30',movements:rows,physicalBalance:1400,verificationThrough:'2026-09-30'});
  assert.equal(result.proven,false);assert.equal(result.invalidMovements.length,2);assert.match(result.blockers.join(' '),/2 bank movement\(s\) have an invalid/);
});
