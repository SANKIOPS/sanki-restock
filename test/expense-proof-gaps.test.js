'use strict';

const test=require('node:test');
const assert=require('node:assert/strict');
const {proofGaps,missingProofRow,attachProofs}=require('../modules/expense-proof-gaps');

function expense(overrides){
  return Object.assign({
    id:'EX-00999',nature:'SANKI',date:'2026-11-26',vendor:'Test Vendor',particulars:'Test expense',ledger:'Props Expense',
    amount:1200,status:'paid',bill:'printed',billPhoto:'',billPhotos:[],paidAmount:1200,account:'Axis Bank 3448',
    paymentProof:'',paymentProofs:[],payments:[{id:'PAY-001',amount:1200,date:'2026-11-26',account:'Axis Bank 3448',proof:'',proofs:[]}],
    reimbursementPayments:[]
  },overrides||{});
}

test('lists the bill and the exact payment proof that are missing',()=>{
  const gaps=proofGaps(expense());
  assert.deepEqual(gaps.map(gap=>[gap.kind,gap.paymentId]),[['bill',''],['payment','PAY-001']]);
  const row=missingProofRow(expense());
  assert.equal(row.id,'EX-00999');
  assert.equal(row.gaps.length,2);
});

test('verified bank reconciliation counts as payment evidence, not as a bill',()=>{
  const gaps=proofGaps(expense({payments:[{id:'PAY-001',amount:1200,date:'2026-11-26',account:'Axis Bank 3448',bankReconciliationDraft:'BRD-1',bankReconciliationRow:'ROW-1'}]}));
  assert.deepEqual(gaps.map(gap=>gap.kind),['bill']);
});

test('expense-level bank evidence cannot hide a second unproved payment',()=>{
  const gaps=proofGaps(expense({
    reconciliationSource:{draftId:'BRD-1',rowId:'ROW-1'},
    paidAmount:1200,
    payments:[
      {id:'PAY-001',amount:700,date:'2026-11-20',account:'Axis Bank 3448',bankReconciliationDraft:'BRD-1',bankReconciliationRow:'ROW-1'},
      {id:'PAY-002',amount:500,date:'2026-11-26',account:'Counter Cash',proof:'',proofs:[]}
    ]
  }));
  assert.deepEqual(gaps.map(gap=>[gap.kind,gap.paymentId]),[['bill',''],['payment','PAY-002']]);
});

test('proof attached to one instalment does not prove another instalment',()=>{
  const record=expense({
    billPhoto:'/api/expenses/photo/bill.jpg',billPhotos:['/api/expenses/photo/bill.jpg'],
    payments:[
      {id:'PAY-001',amount:700,date:'2026-11-20',account:'Axis Bank 3448'},
      {id:'PAY-002',amount:500,date:'2026-11-26',account:'Counter Cash'}
    ]
  });
  attachProofs(record,{kind:'payment',paymentId:'PAY-001',proofs:['/api/expenses/photo/pay-1.jpg']},'owner','2026-11-26T12:00:00.000Z');
  assert.deepEqual(proofGaps(record).map(gap=>[gap.kind,gap.paymentId]),[['payment','PAY-002']]);
  assert.deepEqual(record.paymentProofs,[]);
});

test('a reimbursement needs its own proof even when the expense payment was bank reconciled',()=>{
  const gaps=proofGaps(expense({
    billPhoto:'/api/expenses/photo/bill.jpg',billPhotos:['/api/expenses/photo/bill.jpg'],
    reconciliationSource:{draftId:'BRD-1',rowId:'ROW-1'},
    payments:[{id:'PAY-001',amount:1200,bankReconciliationDraft:'BRD-1',bankReconciliationRow:'ROW-1'}],
    reimbursementPayments:[{id:'RMB-001',amount:1200,date:'2026-11-26',account:'Axis Bank 3448'}]
  }));
  assert.deepEqual(gaps.map(gap=>[gap.kind,gap.paymentId]),[['reimbursement','RMB-001']]);
});

test('documented no-bill exception does not create a false missing-bill warning',()=>{
  const gaps=proofGaps(expense({
    bill:'none',exceptionReason:'Historical bank-statement expense',exceptionEvidence:'Uploaded bank statement',
    payments:[{id:'PAY-001',amount:1200,bankReconciliationDraft:'BRD-1',bankReconciliationRow:'ROW-1'}]
  }));
  assert.deepEqual(gaps,[]);
});

test('late proof upload is append-only and does not change finalized accounting fields',()=>{
  const record=expense({reconciliationSource:{manualFinalizationId:'FINAL-1'}});
  const financialBefore=JSON.stringify({amount:record.amount,date:record.date,account:record.account,ledger:record.ledger,reconciliationSource:record.reconciliationSource,status:record.status,paidAmount:record.paidAmount});
  let result=attachProofs(record,{kind:'bill',proofs:['/api/expenses/photo/bill-1.jpg']},'owner','2026-11-26T12:00:00.000Z');
  assert.deepEqual(result.after.map(gap=>gap.kind),['payment']);
  result=attachProofs(record,{kind:'payment',paymentId:'PAY-001',proofs:['/api/expenses/photo/pay-1.jpg']},'owner','2026-11-26T12:01:00.000Z');
  assert.deepEqual(result.after,[]);
  assert.deepEqual(record.billPhotos,['/api/expenses/photo/bill-1.jpg']);
  assert.deepEqual(record.payments[0].proofs,['/api/expenses/photo/pay-1.jpg']);
  assert.equal(record.proofHistory.length,2);
  assert.equal(JSON.stringify({amount:record.amount,date:record.date,account:record.account,ledger:record.ledger,reconciliationSource:record.reconciliationSource,status:record.status,paidAmount:record.paidAmount}),financialBefore);
});

test('rejected and deleted expenses are not included',()=>{
  assert.deepEqual(proofGaps(expense({status:'rejected'})),[]);
  assert.deepEqual(proofGaps(expense({deletedAt:'2026-11-26T10:00:00.000Z'})),[]);
  assert.equal(missingProofRow(expense({status:'rejected'})),null);
});

test('only stored proof URLs are accepted',()=>{
  assert.throws(()=>attachProofs(expense(),{kind:'bill',proofs:['https://example.com/not-stored.jpg']},'owner',new Date().toISOString()),/valid proof files/);
});
