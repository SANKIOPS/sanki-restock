'use strict';

const test=require('node:test');
const assert=require('node:assert/strict');
const {resolve}=require('../modules/finalized-report-classification');
const {buildReport}=require('../modules/pnl-report');

function expense(overrides={}){return Object.assign({id:'EX-SUSPENSE',nature:'SANKI',date:'2026-10-02',ledger:'SUSPENSE',amount:500,paidAmount:500,status:'paid',payments:[{id:'PAY-001',date:'2026-10-02',account:'Axis Bank 3448',amount:500}]},overrides);}
function store(expenseRecord,row,record={}){return{expenses:{[expenseRecord.id]:expenseRecord},bankStatements:{'SANKI|Axis Bank 3448':{imports:[Object.assign({id:'BST-FINAL',finalizedAt:'2026-10-03T10:00:00.000Z',reconciliationRows:[row]},record)]}}};}

test('an exact finalized bank decision replaces only the report Suspense label',()=>{
  const e=expense(),s=store(e,{id:'bank-0',category:'Props Expense',linkedRecordIds:['EX-SUSPENSE/PAY-001']});
  const before=JSON.stringify(s),classification=resolve(s,e,'PAY-001');
  assert.equal(classification.category,'Props Expense');assert.equal(classification.source,'finalized_bank_reconciliation');assert.equal(JSON.stringify(s),before);
  const report=buildReport({expenses:s},{from:'2026-10-01',to:'2026-10-31'}),row=report.transactions.find(item=>item.reference==='EX-SUSPENSE/PAY-001');
  assert.equal(row.category,'Props Expense');assert.equal(row.classificationSource,'finalized_bank_reconciliation');assert.equal(report.categories.find(item=>item.category==='Props Expense').amount,500);assert.equal(report.categories.some(item=>item.category==='SUSPENSE'),false);assert.equal(JSON.stringify(s),before);
});

test('pending, reopened and category-less history cannot remove Suspense',()=>{
  const e=expense();
  assert.equal(resolve(store(e,{id:'bank-0',category:'Props Expense',linkedRecordIds:['EX-SUSPENSE/PAY-001']},{finalizedAt:''}),e,'PAY-001').category,'SUSPENSE');
  assert.equal(resolve(store(e,{id:'bank-0',category:'Props Expense',linkedRecordIds:['EX-SUSPENSE/PAY-001'],reopenedAt:'2026-10-04'}),e,'PAY-001').category,'SUSPENSE');
  assert.equal(resolve(store(e,{id:'bank-0',category:'',linkedRecordIds:['EX-SUSPENSE/PAY-001']}),e,'PAY-001').category,'SUSPENSE');
});

test('each instalment requires its own exact finalized category',()=>{
  const e=expense({amount:1000,paidAmount:1000,payments:[{id:'PAY-001',date:'2026-10-02',amount:500},{id:'PAY-002',date:'2026-10-03',amount:500}]}),s=store(e,{id:'bank-0',category:'Props Expense',linkedRecordIds:['EX-SUSPENSE']});
  assert.equal(resolve(s,e,'PAY-001').category,'SUSPENSE');
  s.bankStatements['SANKI|Axis Bank 3448'].imports[0].reconciliationRows.push({id:'bank-1',category:'Props Expense',linkedRecordIds:['EX-SUSPENSE/PAY-001']});
  assert.equal(resolve(s,e,'PAY-001').category,'Props Expense');assert.equal(resolve(s,e,'PAY-002').category,'SUSPENSE');
});

test('conflicting finalized categories stay in Suspense for review',()=>{
  const e=expense(),s=store(e,{id:'bank-0',category:'Props Expense',linkedRecordIds:['EX-SUSPENSE/PAY-001']});
  s.bankStatements['SANKI|Axis Bank 3448'].imports.push({id:'BST-OTHER',finalizedAt:'2026-10-04',reconciliationRows:[{id:'bank-1',category:'Fittings',linkedRecordIds:['EX-SUSPENSE/PAY-001']}]});
  const result=resolve(s,e,'PAY-001');assert.equal(result.category,'SUSPENSE');assert.equal(result.conflict,true);
  assert.ok(buildReport({expenses:s},{from:'2026-10-01',to:'2026-10-31'}).warnings.some(item=>/Conflicting finalized reconciliation categories/.test(item.message)));
});
