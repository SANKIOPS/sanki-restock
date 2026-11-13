'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const settlements=require('../modules/credit-card-settlements');const {confirmed}=require('../modules/confirmed-card-repayments');
const {build}=require('../modules/compact-accounting-report');
const clone=v=>JSON.parse(JSON.stringify(v)),account='ICICI Bank 0993',date='2026-09-12',amount=2468.12,finalizedAt='2026-09-14T10:22:33.444Z',by='synthetic-owner';
const expenseId='EX-SYNTHETIC-ORIGIN',paymentId='PAY-SYNTHETIC-ORIGIN',movementId=expenseId+'/'+paymentId;
function fixture() {
  const card = { id: 'CC-SYNTHETIC-ORIGIN', name: 'Synthetic Card', last4: '9009', active: true };
  const row = { id: 'CCR-SYNTHETIC-ORIGIN', date, amount, debit: 0, credit: amount, classification: 'card_payment', narration: 'PAYMENT RECEIVED THANK YOU' };
  const statement = { id: 'CCS-SYNTHETIC-ORIGIN', cardId: card.id, status: 'review', rows: [row] };
  const payment = { id: paymentId, date, amount, account, paymentType: 'UPI', personalFunds: true, proof: '/synthetic-proof-only.jpg' };
  const expense = { id: expenseId, date, nature: 'PERSONAL', amount, paidAmount: amount, personalPaidAmount: amount, account,
    ledger: 'Credit Card', vendor: 'Credit card Payment 9009', particulars: 'Credit card Payment', status: 'paid',
    approvedAt: date + 'T12:00:00.000Z', paidAlready: true, fundedBy: 'claimant', reimbursementStatus: 'not_applicable',
    reimbursementAmount: 0, reimbursementPayments: [], paymentType: 'UPI', payments: [payment] };
  const bank = { transactionId: 'BTX-SYNTHETIC-ORIGIN', date, debit: amount, credit: 0, reference: '', description: 'Synthetic finalized bank debit' };
  const reportRow = { id: 'bank-0', bank, ledger: { id: movementId, date, debit: amount, credit: 0 }, linkedRecordIds: [movementId], decision: 'Matched with ledger entry' };
  const record = { id: 'BST-SYNTHETIC-ORIGIN', from: date, to: '2098-06-14', finalizedAt, finalizedBy: by, reconciliationRows: [reportRow] };
  const override = { originalDate: date, bankDate: date, bankReference: '', reconciliationDraft: 'BRD-SYNTHETIC-ORIGIN', remark: 'Synthetic historical review', by, at: finalizedAt };
  const last = { at: finalizedAt, by, through: record.to, draftId: override.reconciliationDraft, transactionsReconciled: true, balanceReconciled: true };
  const audit = { id: 'AUD-SYNTHETIC-ORIGIN', at: finalizedAt, user: by, action: 'BANK_RECONCILIATION_FINALIZED', subjectType: 'account', subjectId: account, account, nature: 'PERSONAL', after: clone(last) };
  const book = { imports: [record] };
  const store = { cards: { [card.id]: card }, payments: [], statements: { [statement.id]: statement } };
  const exp = { expenses: { [expense.id]: expense }, bankStatements: { ['PERSONAL|' + account]: book }, bankDateOverrides: { [movementId]: override }, auditLog: [audit] };
  return { store, exp, statement, row, payment, expense, bank, reportRow, record, override, audit, book, last };
}
function ready(){const f=fixture();f.record.draftId=f.override.reconciliationDraft;const c=settlements.candidates(f.store,f.exp,f.statement,f.row,()=>true).find(c=>c.id==='legacy_expense_payment:'+movementId);assert.equal(c.eligible,true,c.blockedReason);Object.assign(f.row,{confirmed:true,settlementMode:'link_existing',settlementLegacyConfirmed:true,settlementSourceId:c.id,settlementSourceFingerprint:c.fingerprint});f.statement.status='finalized';return f;}
test('only evidence-validated finalized legacy repayment clears the report warning without adding an expense',()=>{const f=ready(),before=JSON.stringify(f);assert.ok(confirmed(f.store,f.exp).has(movementId));const report=build({expenses:f.exp,creditCards:f.store,orders:{orders:{}},purchases:{pos:{}},salary:{},feeds:[]},{entity:'PERSONAL',from:'2026-08-22',to:'2026-11-13',today:'2026-11-13'});assert.equal(report.totals.expenses,0);assert.equal(report.otherMovements[0].treatment,'Confirmed credit-card repayment');assert.ok(!report.warnings.some(w=>w.includes('repayment/interest')));assert.equal(JSON.stringify(f),before);});
test('changed source, missing confirmation, duplicate or reopened statement cannot clear warning',()=>{for(const change of [f=>{f.payment.amount=100;},f=>{f.row.confirmed=false;},f=>{f.statement.status='review';},f=>{f.row.duplicateOf={statementId:'other'};},f=>{f.row.settlementSourceFingerprint='stale';},f=>{f.payment.accountingExcluded=true;}]){const f=ready();change(f);assert.equal(confirmed(f.store,f.exp).size,0);}});
