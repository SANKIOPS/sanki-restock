'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildProjection } = require('../modules/paytm-ledger-status');
const { summarizePayouts } = require('../modules/paytm-report');
const { settlementStatus } = require('../modules/paytm-accounting');
const CLEARING = 'Paytm Settlement Clearing';
const BANK = 'Axis Bank 3448';

function fixture() {
  return { store: { paytmReportTransactions: {}, paytmOrderLinks: {}, paytmManualResolutions: {},
    paytmExcludedTransactions: {}, paytmRoundingReviews: {}, paytmHistoricalEvidenceLinks: {},
    paytmVerifiedSettlements: [], paytmPayoutPostings: [], paytmSettlements: [],
    adjustments: [], receipts: [], transfers: [], bankTruthMovements: [], bankDateOverrides: {},
    bankStatements: { [BANK]: { transactions: {}, imports: [] } } }, sales: [] };
}
function receipt(f, amount, { id = 'COMPONENT-1', orderId = 'ORDER-1', date = '2026-10-13' } = {}) {
  const row = { id, orderId, orderNumber: orderId, date, amount, account: CLEARING };
  f.sales.push(row);
  return row;
}
function posted(f, amount, { id = 'TX-1', orderId = 'ORDER-1', date = '2026-10-13', settledDate = '2026-10-14' } = {}) {
  const tx = { transactionId: id, amount, date, settledDate, utr: 'UTR-' + id,
    payoutId: 'PAYOUT-' + id, commission: 0, platformFee: 0, gst: 0,
    settledAmount: amount, isCustomerPayment: true };
  f.store.paytmReportTransactions[id] = tx;
  f.store.paytmOrderLinks[id] = { transactionId: id, orderId, orderNumber: orderId, amount };
  const payout = summarizePayouts([tx])[0], bankTransactionId = 'BANK-' + id;
  const record = { ...payout, id: 'POST-' + id, date: settledDate, bankTransactionId,
    bankAccount: BANK, postedAt: '2026-10-19T10:00:00Z', postedBy: 'synthetic-admin',
    orderIds: [orderId], orderNumbers: [orderId] };
  f.store.paytmPayoutPostings.push(record);
  f.store.bankStatements[BANK].transactions[bankTransactionId] = {
    id: bankTransactionId, date: settledDate, credit: amount, debit: 0, reference: payout.utr
  };
  f.store.bankDateOverrides[record.id] = { bankDate: settledDate, bankTransactionId,
    originalDate: settledDate, bankReference: payout.utr, reconciliationDraft: record.id,
    by: record.postedBy, at: record.postedAt };
  return { payout, record };
}
function bankOnly(f, date) {
  const id = 'LEGACY-' + date, bank = { id: 'BANK-' + id, date,
    credit: 50, debit: 0, reference: 'UTR-' + id };
  f.store.paytmSettlements.push({ id, date, bankAccount: BANK, bankTransactionId: bank.id,
    bankReference: bank.reference, netAmount: 50, grossAmount: 50, chargeAmount: 0,
    paytmChargeAmount: 0, hiddenChargeAmount: 0, bankTruthOnly: true,
    source: 'axis_3448_paytm_bank_narration', orderIds: [], transferIds: [], otherReceipts: [] });
  f.store.bankStatements[BANK].transactions[bank.id] = bank;
  f.store.bankStatements[BANK].imports.push({ id: 'BST-' + id,
    finalizedAt: '2026-10-19T10:00:00Z', finalizedBy: 'synthetic-admin',
    reconciliationRows: [{ bank, linkedRecordIds: [id] }] });
  return id;
}
const project = f => buildProjection(f.store, f.sales, { to: '2026-10-19' });
const receiptState = (p, row) => p.forRow({ id: 'PAYTM-SALE/' + row.id, kind: 'paytm_customer_receipt' });

test('sub-rupee Shopify remainder stays pending without downgrading exact validated Paytm report and bank posting', () => {
  for (const [sourceAmount, reportAmount, residual] of [
    [10706.40, 10706, 0.40], [19194.10, 19194, 0.10], [1299.99, 1299, 0.99]
  ]) {
    const f = fixture(), row = receipt(f, sourceAmount), saved = posted(f, reportAmount);
    assert.equal(settlementStatus(f.store, saved.payout, f.sales).code, 'posted', 'Existing strict report and bank checks pass');
    const before = JSON.stringify(f), p = project(f), state = receiptState(p, row);
    assert.equal(p.sharedStatus.settlements[0].code, 'posted');
    assert.equal(p.forRow({ id: saved.record.id, kind: 'paytm_settlement' }).code, 'posted');
    assert.equal(state.code, 'report_pending');
    assert.equal(state.confirmedReportAmount, reportAmount);
    assert.equal(state.receiptAmount, sourceAmount);
    assert.equal(state.unverifiedAmount, residual);
    assert.match(state.reasons.join(' '), new RegExp('remaining ₹' + residual.toFixed(2).replace('.', '\\.')));
    assert.equal(p.coverage.reportPaymentsThrough, '2026-10-13');
    assert.equal(p.coverage.reportSettlementsThrough, '2026-10-14');
    assert.equal(p.coverage.pendingReceipts, 1);
    assert.equal(p.coverage.overallReconciledThrough, '', 'The partial receipt must not claim complete account coverage');
    assert.equal(JSON.stringify(f), before, 'No rounding income, missing receipt or balancing adjustment is created');
  }
});

test('two already validated payments retain their independent bank evidence while an overallocated receipt explains the exact excess', () => {
  const f = fixture(), row = receipt(f, 3657);
  const first = posted(f, 40, { id: 'FIRST' }), second = posted(f, 3657, { id: 'SECOND' });
  for (const saved of [first, second]) assert.equal(settlementStatus(f.store, saved.payout, f.sales).code, 'posted');
  const before = JSON.stringify(f), p = project(f), state = receiptState(p, row);
  assert.deepEqual(p.sharedStatus.settlements.map(s => s.code), ['posted', 'posted']);
  assert.equal(state.code, 'incomplete');
  assert.match(state.reasons.join(' '), /₹3697\.00/);
  assert.match(state.reasons.join(' '), /₹3657\.00/);
  assert.match(state.reasons.join(' '), /₹40\.00 excess/);
  assert.equal(p.coverage.reportSettlementsThrough, '2026-10-14');
  assert.equal(p.coverage.pendingReceipts, 1);
  assert.equal(p.coverage.overallReconciledThrough, '');
  assert.equal(JSON.stringify(f), before);
});

test('historical bank-only and unrelated receipt gaps remain visible separately from imported report verification dates', () => {
  const f = fixture(), row = receipt(f, 299);
  posted(f, 299);
  const oldBank = bankOnly(f, '2026-08-22'), laterBank = bankOnly(f, '2026-10-18');
  receipt(f, 80, { id: 'UNREPORTED', orderId: 'UNREPORTED', date: '2026-08-23' });
  const before = JSON.stringify(f), p = project(f);
  assert.equal(receiptState(p, row).code, 'posted');
  assert.equal(p.coverage.reportPaymentsThrough, '2026-10-13');
  assert.equal(p.coverage.reportSettlementsThrough, '2026-10-14');
  assert.equal(p.coverage.latestVerifiedSettlementDate, '2026-10-14');
  assert.equal(p.coverage.bankSettlementsThrough, '2026-10-18');
  assert.equal(p.coverage.pendingBankOnlySettlements, 2);
  assert.equal(p.coverage.firstPendingBankOnlyDate, '2026-08-22');
  assert.equal(p.coverage.firstPendingReceiptDate, '2026-08-23');
  assert.equal(p.coverage.receiptCoverageThrough, '');
  assert.equal(p.coverage.overallReconciledThrough, '');
  for (const id of [oldBank, laterBank]) assert.equal(p.forRow({ id, kind: 'paytm_settlement' }).code, 'bank_confirmed_report_pending');
  assert.equal(JSON.stringify(f), before);
});

test('a later Shopify refund does not turn an originally covered receipt into an overallocated or newly posted payment', () => {
  const f = fixture(), row = receipt(f, 299);
  row.refundAmount = 50;
  posted(f, 299);
  const before = JSON.stringify(f), p = project(f);
  const display = p.forRow({ id: 'PAYTM-SALE/' + row.id, kind: 'paytm_customer_receipt', credit: 249, debit: 0 });
  assert.equal(display.code, 'posted', 'Receipt verification uses original ₹299 evidence, not net sales after the separate refund');
  assert.equal(p.coverage.pendingReceipts, 0);
  assert.equal(p.coverage.reportSettlementsThrough, '2026-10-14');
  assert.equal(JSON.stringify(f), before);
});
