'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { buildProjection } = require('../modules/paytm-ledger-status');
const { summarizePayouts } = require('../modules/paytm-report');
const CLEARING = 'Paytm Settlement Clearing';
const BANK = 'Axis Bank 3448';
function fixture() {
  return { store: { paytmReportTransactions: {}, paytmOrderLinks: {}, paytmManualResolutions: {},
    paytmExcludedTransactions: {}, paytmRoundingReviews: {}, paytmHistoricalEvidenceLinks: {},
    paytmVerifiedSettlements: [], paytmPayoutPostings: [], paytmSettlements: [],
    adjustments: [], receipts: [], transfers: [], bankTruthMovements: [], bankDateOverrides: {},
    bankStatements: { [BANK]: { transactions: {}, imports: [] } } }, sales: [] };
}
function sale(f, { id = 'SALE-1', orderId = 'ORDER-1', date = '2026-10-13', amount = 100 } = {}) {
  const row = { id, orderId, orderNumber: orderId, date, amount, account: CLEARING };
  f.sales.push(row);
  return row;
}
function posted(f, { id = 'TX-1', orderId = 'ORDER-1', amount = 100, date = '2026-10-13', settledDate = '2026-10-14', bankDate = settledDate } = {}) {
  const tx = { transactionId: id, date, amount, commission: 0, platformFee: 0, gst: 0,
    settledAmount: amount, payoutId: 'SOURCE-' + id, utr: 'UTR-' + id, settledDate,
    isCustomerPayment: true };
  f.store.paytmReportTransactions[id] = tx;
  f.store.paytmOrderLinks[id] = { transactionId: id, orderId, orderNumber: orderId, amount };
  const payout = summarizePayouts([tx])[0], bankTransactionId = 'BANK-' + id;
  const posting = { ...payout, id: 'POST-' + id, date: bankDate, bankAccount: BANK, bankTransactionId,
    postedAt: '2026-10-19T08:00:00Z', postedBy: 'synthetic-admin', orderIds: [orderId], orderNumbers: [orderId] };
  f.store.paytmPayoutPostings.push(posting);
  f.store.bankStatements[BANK].transactions[bankTransactionId] = {
    id: bankTransactionId, date: bankDate, credit: amount, debit: 0, reference: payout.utr
  };
  f.store.bankDateOverrides[posting.id] = { bankDate, originalDate: settledDate,
    bankTransactionId, bankReference: payout.utr, reconciliationDraft: posting.id,
    by: posting.postedBy, at: posting.postedAt };
  return { tx, payout, posting };
}
function legacy(f, { id = 'BANK-ONLY', date = '2026-10-18', amount = 100 } = {}) {
  const bank = { id: 'BANK-' + id, date, credit: amount, debit: 0, reference: 'UTR-' + id };
  const settlement = { id, date, bankAccount: BANK, bankTransactionId: bank.id,
    bankReference: bank.reference, netAmount: amount, grossAmount: amount, chargeAmount: 0,
    paytmChargeAmount: 0, hiddenChargeAmount: 0, bankTruthOnly: true, provisional: false,
    source: 'axis_3448_paytm_bank_narration', orderIds: [], transferIds: [], otherReceipts: [] };
  const row = { bank, linkedRecordIds: [id] };
  f.store.paytmSettlements.push(settlement);
  f.store.bankStatements[BANK].transactions[bank.id] = bank;
  f.store.bankStatements[BANK].imports.push({ id: 'BST-' + id, finalizedAt: '2026-10-19T08:00:00Z',
    finalizedBy: 'synthetic-owner', reconciliationRows: [row] });
  return { bank, settlement, row };
}
function projection(f, to = '2026-10-19') { return buildProjection(f.store, f.sales, { to }); }
function receiptState(p, row) { return p.forRow({ id: 'PAYTM-SALE/' + row.id, kind: 'paytm_customer_receipt' }); }
function settlementState(p, id) { return p.forRow({ id, kind: 'paytm_settlement' }); }

test('pure projection is read-only and gives a correctly posted receipt its existing exact evidence', () => {
  const f = fixture(), row = sale(f), saved = posted(f), before = JSON.stringify(f);
  const p = projection(f);
  assert.equal(receiptState(p, row).code, 'posted');
  assert.equal(settlementState(p, saved.posting.id).code, 'posted');
  assert.equal(p.coverage.reportPaymentsThrough, '2026-10-13');
  assert.equal(p.coverage.reportSettlementsThrough, '2026-10-14');
  assert.equal(p.coverage.pendingReceipts, 0);
  assert.equal(JSON.stringify(f), before);
});

test('bank-only evidence is confirmed only when its finalized credit belongs exclusively to that settlement', () => {
  const f = fixture(), saved = legacy(f), before = JSON.stringify(f), p = projection(f);
  assert.equal(settlementState(p, saved.settlement.id).code, 'bank_confirmed_report_pending');
  assert.equal(p.coverage.pendingBankOnlySettlements, 1);
  assert.equal(p.coverage.bankSettlementsThrough, '2026-10-18');
  assert.equal(p.coverage.reportSettlementsThrough, '');
  assert.equal(JSON.stringify(f), before);
});

const competingClaims = {
  'a second linked ledger ID on the finalized row': (f, x) => x.row.linkedRecordIds.push('FOREIGN'),
  'an adjustment using the bank transaction': (f, x) => f.store.adjustments.push({ id: 'FOREIGN', bankTransactionId: x.bank.id }),
  'a receipt using the bank transaction': (f, x) => f.store.receipts.push({ id: 'FOREIGN', bankTransactionId: x.bank.id }),
  'a bank-truth movement using the bank transaction': (f, x) => f.store.bankTruthMovements.push({ id: 'FOREIGN', bankTransactionId: x.bank.id }),
  'a foreign date override using the bank transaction': (f, x) => { f.store.bankDateOverrides.FOREIGN = { bankTransactionId: x.bank.id }; },
  'another settlement using the bank transaction': (f, x) => f.store.paytmSettlements.push({ ...x.settlement, id: 'FOREIGN' })
};
for (const [name, change] of Object.entries(competingClaims)) test('bank-only confirmation rejects ' + name, () => {
  const f = fixture(), saved = legacy(f);
  change(f, saved);
  const before = JSON.stringify(f), p = projection(f), state = settlementState(p, saved.settlement.id);
  assert.notEqual(state.code, 'bank_confirmed_report_pending');
  assert.notEqual(state.code, 'posted');
  assert.equal(p.coverage.pendingBankOnlySettlements, 0, 'a conflicting claim is not confirmed evidence');
  assert.equal(p.coverage.bankSettlementsThrough, '');
  assert.equal(JSON.stringify(f), before);
});

test('carried copies of the same exclusively claimed bank row are not mistaken for a second bank receipt', () => {
  const f = fixture(), saved = legacy(f), book = f.store.bankStatements[BANK];
  book.imports.push({ id: 'BST-CARRIED', finalizedAt: '2026-10-19T09:00:00Z', finalizedBy: 'synthetic-owner',
    carriedReconciliationRows: [JSON.parse(JSON.stringify(saved.row))] });
  const p = projection(f);
  assert.equal(settlementState(p, saved.settlement.id).code, 'bank_confirmed_report_pending');
  assert.equal(p.coverage.pendingBankOnlySettlements, 1);
});

test('an order reduced from 120 to 100 cannot retain two fully posted allocations totaling 120', () => {
  const f = fixture(), row = sale(f, { amount: 100 });
  posted(f, { id: 'FIRST', amount: 100 });
  posted(f, { id: 'SECOND', amount: 20 });
  const before = JSON.stringify(f), p = projection(f);
  assert.notEqual(receiptState(p, row).code, 'posted', 'the exact 100 submatch cannot hide an additional allocated 20');
  assert.ok(p.sharedStatus.settlements.every(s => s.code !== 'posted'), 'the whole overallocated order remains under review');
  assert.equal(p.coverage.pendingReceipts, 1);
  assert.equal(p.coverage.reportPaymentsThrough, '');
  assert.equal(p.coverage.reportSettlementsThrough, '');
  assert.equal(JSON.stringify(f), before);
});

test('an actual bank credit on 16 October cannot be bank-posted in a view ending 14 October', () => {
  const f = fixture(), row = sale(f), saved = posted(f, { bankDate: '2026-10-16' });
  let p = projection(f, '2026-10-14');
  assert.notEqual(receiptState(p, row).code, 'posted');
  assert.notEqual(settlementState(p, saved.posting.id).code, 'posted');
  assert.equal(p.coverage.reportPaymentsThrough, '');
  assert.equal(p.coverage.reportSettlementsThrough, '');
  assert.equal(p.coverage.bankSettlementsThrough, '');
  p = projection(f, '2026-10-16');
  assert.equal(receiptState(p, row).code, 'posted');
  assert.equal(p.coverage.reportSettlementsThrough, '2026-10-14');
  assert.equal(p.coverage.bankSettlementsThrough, '2026-10-16', 'bank date is separate from Paytm settled date');
});

test('two payments against the same order are scoped to their own receipt dates', () => {
  const f = fixture(), first = sale(f, { id: 'SALE-EARLY', date: '2026-10-12', amount: 60 });
  const second = sale(f, { id: 'SALE-LATER', date: '2026-10-15', amount: 40 });
  posted(f, { id: 'FIRST', date: first.date, settledDate: '2026-10-13', amount: 60 });
  posted(f, { id: 'SECOND', date: second.date, settledDate: '2026-10-16', amount: 40 });
  let p = projection(f, '2026-10-13');
  assert.equal(receiptState(p, first).code, 'posted');
  assert.notEqual(receiptState(p, second).code, 'posted');
  assert.equal(p.coverage.importedPaymentsThrough, '2026-10-12');
  assert.equal(p.coverage.reportPaymentsThrough, '2026-10-12');
  assert.equal(p.coverage.reportSettlementsThrough, '2026-10-13');
  p = projection(f, '2026-10-16');
  assert.equal(receiptState(p, first).code, 'posted');
  assert.equal(receiptState(p, second).code, 'posted');
  assert.equal(p.coverage.reportPaymentsThrough, '2026-10-15');
  assert.equal(p.coverage.reportSettlementsThrough, '2026-10-16');
});

test('an earlier incomplete batch blocks a later good batch from advancing coverage past the gap', () => {
  const f = fixture(), early = sale(f, { id: 'SALE-EARLY', orderId: 'ORDER-EARLY', date: '2026-10-10' });
  const late = sale(f, { id: 'SALE-LATE', orderId: 'ORDER-LATE', date: '2026-10-13' });
  posted(f, { id: 'EARLY', orderId: early.orderId, date: early.date, settledDate: '2026-10-11' });
  posted(f, { id: 'LATE', orderId: late.orderId, date: late.date });
  delete f.store.paytmOrderLinks.EARLY;
  const before = JSON.stringify(f), p = projection(f);
  assert.notEqual(receiptState(p, early).code, 'posted');
  assert.equal(receiptState(p, late).code, 'posted', 'later valid evidence remains independently visible');
  assert.equal(p.sharedStatus.settlements.find(s => s.settlementId === 'UTR-EARLY').code, 'incomplete');
  assert.equal(p.coverage.reportPaymentsThrough, '');
  assert.equal(p.coverage.reportSettlementsThrough, '');
  assert.ok(p.coverage.pendingSettlements >= 1);
  assert.equal(JSON.stringify(f), before);
});
