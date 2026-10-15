'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { CLEARING, roundingReview, applyRoundingReview, approvedBaseAmount, validateOrderReceiptTotals } = require('../modules/paytm-rounding');

const clone = value => JSON.parse(JSON.stringify(value));
const at = '2026-10-14T16:00:00.000Z';
const reason = 'Owner-confirmed customer receipt rounding; preserve the original Shopify sale amount.';
function fixture(number = '2824', receiptAmount = 3199, sourceAmount = 3198.99) {
  const transactionId = 'T' + number, orderId = 'O' + number, date = '2026-09-20';
  const tx = { transactionId, date, time: date + ' 12:00:00', amount: receiptAmount, commission: 0, gst: 0, platformFee: 0, settledAmount: receiptAmount, payoutId: 'P' + number, settledDate: '2026-09-21', utr: 'UTR' + number, transactionType: 'ACQUIRING', isCustomerPayment: true, source: 'paytm.csv' };
  const link = { transactionId, orderId, orderNumber: number, amount: receiptAmount, matchBasis: 'transaction_id_suffix_in_shopify_note', by: 'automatic', at: '2026-10-01T12:00:00Z' };
  const store = { paytmReportTransactions: { [transactionId]: tx }, paytmOrderLinks: { [transactionId]: link }, receipts: [], receiptSeq: 7 };
  const sales = [{ id: 'SHOPIFY/' + orderId + '/NONCASH', orderId, orderNumber: number, date, account: CLEARING, amount: sourceAmount }];
  return { store, sales, transactionId, orderId, tx, link };
}
function approve(f) {
  return applyRoundingReview(f.store, f.transactionId, f.sales, { difference: roundingReview(f.store, f.transactionId, f.sales).difference, reason, username: 'owner', at });
}
function blockedWithoutMutation(f, options = {}) {
  const before = JSON.stringify(f.store);
  assert.throws(() => applyRoundingReview(f.store, f.transactionId, f.sales, { difference: 0.01, reason, username: 'owner', at, ...options }));
  assert.equal(JSON.stringify(f.store), before);
}

test('five authorized rounding cases create ₹3.05 other income without rewriting sales or Paytm amounts', () => {
  const cases = [['2824', 3199, 3198.99, .01], ['2825', 3200, 3199.98, .02], ['2833', 4745, 4743.99, 1.01], ['2834', 2300, 2299.99, .01], ['2840', 200, 198, 2]];
  let totalCents = 0;
  for (const [number, received, original, delta] of cases) {
    const f = fixture(number, received, original), beforeTx = clone(f.tx), beforeLink = clone(f.link), beforeSales = clone(f.sales);
    f.store.expenses = { untouched: { amount: 100, status: 'paid' } };
    f.store.transfers = [{ id: 'TR-existing', amount: 500 }];
    const preview = roundingReview(f.store, f.transactionId, f.sales);
    assert.deepEqual([preview.eligible, preview.approved, preview.difference, preview.receiptAmount, preview.sourceAmount], [true, false, delta, received, original]);
    assert.throws(() => validateOrderReceiptTotals(f.store, f.orderId, f.sales), /exceed/);
    const saved = approve(f);
    assert.equal(saved.created, true);
    assert.deepEqual([saved.receipt.id, saved.receipt.nature, saved.receipt.account, saved.receipt.receiptType, saved.receipt.category, saved.receipt.amount, saved.receipt.date], ['REC-00008', 'SANKI', CLEARING, 'other_income', 'Rounding Off', delta, f.tx.date]);
    assert.equal(saved.receipt.paytmTransactionId, undefined, 'rounding must not masquerade as another full customer payment');
    assert.equal(saved.receipt.paytmRoundingTransactionId, f.transactionId);
    assert.deepEqual(saved.receipt.paytmReportEvidence.sourceRows, [{ id: f.sales[0].id, amount: original }]);
    assert.equal(approvedBaseAmount(f.store, f.link), original);
    assert.deepEqual(validateOrderReceiptTotals(f.store, f.orderId, f.sales), { orderId: f.orderId, receiptAmount: received, sourceAmount: original, approvedDifference: delta });
    assert.equal(roundingReview(f.store, f.transactionId, f.sales).approved, true);
    assert.deepEqual(f.tx, beforeTx); assert.deepEqual(f.link, beforeLink); assert.deepEqual(f.sales, beforeSales);
    assert.deepEqual(f.store.expenses, { untouched: { amount: 100, status: 'paid' } });
    assert.deepEqual(f.store.transfers, [{ id: 'TR-existing', amount: 500 }]);
    totalCents += Math.round(saved.receipt.amount * 100);
  }
  assert.equal(totalCents, 305);
});

test('repeat approval is idempotent and preserves receipt numbering and original provenance', () => {
  const f = fixture(), first = approve(f), before = JSON.stringify(f.store), repeated = approve(f);
  assert.equal(repeated.created, false); assert.equal(repeated.receipt.id, first.receipt.id);
  assert.equal(JSON.stringify(f.store), before);
  blockedWithoutMutation(f, { difference: .02 });
});

test('receipt numbering safely skips an existing receipt ID without overwriting it', () => {
  const f = fixture(); f.store.receipts.push({ id: 'REC-00008', amount: 999 });
  const saved = approve(f);
  assert.equal(saved.receipt.id, 'REC-00009'); assert.equal(f.store.receiptSeq, 9);
  assert.equal(f.store.receipts[0].amount, 999);
});

test('zero, negative and ₹5-or-more differences are never accepted as rounding', () => {
  for (const delta of [0, -.01, 5, 5.01, 10]) {
    const f = fixture('1', 100 + delta, 100);
    assert.equal(roundingReview(f.store, f.transactionId, f.sales).eligible, false);
    blockedWithoutMutation(f, { difference: delta });
  }
  const f = fixture('2', 104.99, 100);
  assert.equal(roundingReview(f.store, f.transactionId, f.sales).eligible, true);
});

test('the current difference, explicit reason and approval provenance must all be supplied', () => {
  for (const override of [{ difference: .02 }, { difference: '' }, { reason: 'short' }, { username: '' }, { at: 'not-a-date' }]) blockedWithoutMutation(fixture(), override);
});

test('missing, zero, cash-only and wrong-account Shopify components cannot be rounded', () => {
  for (const change of [f => { f.sales = []; }, f => { f.sales[0].amount = 0; }, f => { f.sales[0].account = 'Counter Cash'; }, f => { f.sales[0].account = 'Axis Bank 3448'; }, f => { f.sales[0].amount = null; }, f => { f.sales.push(clone(f.sales[0])); }]) {
    const f = fixture(); change(f);
    assert.equal(roundingReview(f.store, f.transactionId, f.sales).eligible, false);
    blockedWithoutMutation(f);
  }
});

test('multiple verified Paytm components are summed, but unrelated cash and other-order rows are not', () => {
  const f = fixture('split', 300, 198.99);
  f.sales.push({ ...f.sales[0], id: 'SHOPIFY/Osplit/PAYMENT-2', amount: 100 });
  f.sales.push({ ...f.sales[0], id: 'SHOPIFY/Osplit/CASH', account: 'Counter Cash', amount: 1000 });
  f.sales.push({ ...f.sales[0], id: 'SHOPIFY/other', orderId: 'other', amount: 2000 });
  const preview = roundingReview(f.store, f.transactionId, f.sales);
  assert.equal(preview.sourceAmount, 298.99); assert.equal(preview.difference, 1.01);
  approve(f); assert.equal(approvedBaseAmount(f.store, f.link), 298.99);
  assert.doesNotThrow(() => validateOrderReceiptTotals(f.store, f.orderId, f.sales));
});

test('customer identity, exact linked amount and actual payment date are mandatory', () => {
  for (const change of [f => { delete f.store.paytmReportTransactions[f.transactionId]; }, f => { f.tx.transactionId = 'other'; }, f => { f.tx.amount = 3198.98; }, f => { f.link.amount = 3200; }, f => { f.link.transactionId = 'other'; }, f => { f.tx.date = ''; }, f => { f.tx.date = '2026-02-31'; }, f => { f.tx.isCustomerPayment = false; }, f => { f.tx.transactionType = 'VAS Deductions'; }, f => { delete f.store.paytmOrderLinks[f.transactionId]; }]) {
    const f = fixture(); change(f); assert.equal(roundingReview(f.store, f.transactionId, f.sales).eligible, false); blockedWithoutMutation(f);
  }
});

test('excluded, manual, bank-linked, finalized and posted records are not eligible for new treatment', () => {
  for (const change of [f => { f.store.paytmExcludedTransactions = { [f.transactionId]: { reason: 'Non-Shopify' } }; }, f => { f.store.paytmManualResolutions = { [f.transactionId]: { type: 'manual_sale' } }; }, f => { f.store.bankDateOverrides = { [f.sales[0].id]: { bankTransactionId: 'BTX-1' } }; }, f => { f.store.paytmVerifiedSettlements = [{ transactionIds: [f.transactionId] }]; }, f => { f.store.paytmPayoutPostings = [{ payoutId: f.tx.utr }]; }, f => { f.store.paytmSettlements = [{ payoutId: f.tx.payoutId }]; }]) {
    const f = fixture(); change(f); assert.equal(roundingReview(f.store, f.transactionId, f.sales).eligible, false); blockedWithoutMutation(f);
  }
});

test('a valid approved receipt remains valid after settlement finalization and posting', () => {
  const f = fixture(); approve(f);
  f.store.paytmVerifiedSettlements = [{ transactionIds: [f.transactionId], payoutId: f.tx.utr }];
  f.store.paytmPayoutPostings = [{ transactionIds: [f.transactionId], payoutId: f.tx.utr }];
  assert.equal(roundingReview(f.store, f.transactionId, f.sales).approved, true);
  assert.equal(approvedBaseAmount(f.store, f.link), 3198.99);
  assert.doesNotThrow(() => validateOrderReceiptTotals(f.store, f.orderId, f.sales));
  assert.equal(approve(f).created, false);
});

test('changed Shopify source amount, date, ID or account invalidates the approval without creating another receipt', () => {
  for (const change of [f => { f.sales[0].amount = 3198.98; }, f => { f.sales[0].date = '2026-09-19'; }, f => { f.sales[0].id += '/changed'; }, f => { f.sales[0].account = 'Counter Cash'; }, f => { f.sales.push({ ...f.sales[0], id: 'new-component', amount: .01 }); }]) {
    const f = fixture(); approve(f); change(f);
    assert.equal(roundingReview(f.store, f.transactionId, f.sales).approved, false);
    assert.throws(() => validateOrderReceiptTotals(f.store, f.orderId, f.sales));
    blockedWithoutMutation(f);
  }
});

test('report amount, date, settlement evidence or order-link changes invalidate durable approval', () => {
  for (const change of [f => { f.tx.amount = 3200; }, f => { f.tx.date = '2026-09-19'; }, f => { f.tx.utr = 'UTR-CHANGED'; }, f => { f.tx.commission = .01; }, f => { f.tx.settledAmount = 3198.99; }, f => { f.link.orderId = 'different'; }, f => { f.link.amount = 3200; }, f => { f.link.reason = 'changed after approval'; }, f => { f.store.bankDateOverrides = { [f.sales[0].id]: { bankTransactionId: 'B1' } }; }]) {
    const f = fixture(); approve(f); change(f);
    assert.equal(roundingReview(f.store, f.transactionId, f.sales).approved, false);
    assert.throws(() => approvedBaseAmount(f.store, f.link));
    blockedWithoutMutation(f);
  }
});

test('changed or missing receipt identity, classification, amount, date or provenance invalidates approval', () => {
  for (const change of [r => { r.nature = 'PERSONAL'; }, r => { r.account = 'Counter Cash'; }, r => { r.amount = .02; }, r => { r.date = '2026-09-21'; }, r => { r.receiptType = 'product_sale'; }, r => { r.category = 'Sales'; }, r => { r.id = 'REC-changed'; }, r => { r.source = 'Unknown'; }, r => { r.paytmRoundingTransactionId = 'other'; }, r => { r.paytmTransactionId = 'T2824'; }, r => { r.accountingExcluded = true; }, r => { r.paytmReportEvidence.sourceAmount = 3198; }, r => { r.note += 'changed'; }, r => { r.createdBy = 'other'; }]) {
    const f = fixture(); approve(f); change(f.store.receipts[0]);
    assert.equal(roundingReview(f.store, f.transactionId, f.sales).approved, false);
    assert.throws(() => approvedBaseAmount(f.store, f.link));
    blockedWithoutMutation(f);
  }
  const missing = fixture(); approve(missing); missing.store.receipts = [];
  assert.throws(() => approvedBaseAmount(missing.store, missing.link)); blockedWithoutMutation(missing);
  const duplicate = fixture(); approve(duplicate); duplicate.store.receipts.push(clone(duplicate.store.receipts[0]));
  assert.throws(() => approvedBaseAmount(duplicate.store, duplicate.link)); blockedWithoutMutation(duplicate);
});

test('the stored source baseline and its receipt evidence cannot be rewritten independently', () => {
  for (const change of [review => { review.sourceBaseline[0].amount = 3000; }, review => { review.sourceRows[0].amount = 3000; }, review => { review.sourceFingerprint = 'forged'; }, review => { review.transactionEvidence.amount = 3000; }, review => { review.linkedEvidence[0].link.amount = 3000; }, review => { review.receiptFingerprint = 'forged'; }, review => { review.difference = 2; }, review => { review.by = ''; }]) {
    const f = fixture(); approve(f); change(f.store.paytmRoundingReviews[f.transactionId]);
    assert.equal(roundingReview(f.store, f.transactionId, f.sales).approved, false);
    assert.throws(() => approvedBaseAmount(f.store, f.link)); blockedWithoutMutation(f);
  }
});

test('a second linked payment for the same order invalidates approved rounding across payouts', () => {
  const f = fixture(); approve(f);
  f.store.paytmReportTransactions.EXTRA = { ...clone(f.tx), transactionId: 'EXTRA', amount: 1, settledAmount: 1, payoutId: 'OTHER', utr: 'UTR-OTHER', date: '2026-09-21' };
  f.store.paytmOrderLinks.EXTRA = { ...clone(f.link), transactionId: 'EXTRA', amount: 1 };
  assert.equal(roundingReview(f.store, f.transactionId, f.sales).approved, false);
  assert.throws(() => approvedBaseAmount(f.store, f.link), /linked receipt set/);
  assert.throws(() => validateOrderReceiptTotals(f.store, f.orderId, f.sales), /linked receipt set/);
  blockedWithoutMutation(f);
});

test('multiple linked receipts require manual aggregate review, never a per-payout rounding tolerance', () => {
  const f = fixture('split', 60, 100);
  f.store.paytmReportTransactions.EXTRA = { ...clone(f.tx), transactionId: 'EXTRA', amount: 44.99, settledAmount: 44.99, payoutId: 'SECOND', utr: 'UTR-SECOND' };
  f.store.paytmOrderLinks.EXTRA = { ...clone(f.link), transactionId: 'EXTRA', amount: 44.99 };
  assert.match(roundingReview(f.store, f.transactionId, f.sales).error, /multiple linked Paytm receipts/);
  assert.throws(() => validateOrderReceiptTotals(f.store, f.orderId, f.sales), /exceed/);
  blockedWithoutMutation(f, { difference: 4.99 });
  f.store.paytmReportTransactions.EXTRA.amount = f.store.paytmReportTransactions.EXTRA.settledAmount = 40;
  f.store.paytmOrderLinks.EXTRA.amount = 40;
  assert.doesNotThrow(() => validateOrderReceiptTotals(f.store, f.orderId, f.sales), 'an exact aggregate is valid without a rounding approval');
});

test('unreviewed original link is preserved, while an orphan rounding receipt blocks duplicate approval', () => {
  const f = fixture(); assert.equal(approvedBaseAmount(f.store, f.link), 3199);
  f.store.receipts.push({ id: 'REC-orphan', paytmRoundingTransactionId: f.transactionId, amount: .01 });
  assert.equal(roundingReview(f.store, f.transactionId, f.sales).eligible, false); blockedWithoutMutation(f);
});

test('unverified #2776 and excluded rental deduction remain untouched while another order is treated', () => {
  const f = fixture();
  f.store.paytmReportTransactions.T2776 = { transactionId: 'T2776', date: '2026-09-04', amount: 4998 };
  f.store.paytmOrderLinks.T2776 = { transactionId: 'T2776', orderId: 'O2776', orderNumber: '2776', amount: 4998 };
  f.store.paytmReportTransactions.RENTAL = { transactionId: 'RENTAL', date: '2026-08-26', amount: 352, transactionType: 'VAS Deductions', isCustomerPayment: false };
  f.store.paytmExcludedTransactions = { RENTAL: { reason: 'Monthly rental' } };
  f.sales.push({ id: 'PAYTM-PENDING/O2776', orderId: 'O2776', orderNumber: '2776', account: CLEARING, date: '2026-09-04', amount: 0 });
  const before = { tx: clone(f.store.paytmReportTransactions), links: clone(f.store.paytmOrderLinks), excluded: clone(f.store.paytmExcludedTransactions), sales: clone(f.sales) };
  assert.equal(roundingReview(f.store, 'T2776', f.sales).eligible, false);
  assert.equal(roundingReview(f.store, 'RENTAL', f.sales).eligible, false);
  approve(f);
  assert.deepEqual(f.store.paytmReportTransactions, before.tx); assert.deepEqual(f.store.paytmOrderLinks, before.links);
  assert.deepEqual(f.store.paytmExcludedTransactions, before.excluded); assert.deepEqual(f.sales, before.sales);
  assert.equal(f.store.receipts.length, 1);
  assert.deepEqual(Object.keys(f.store.paytmRoundingReviews), [f.transactionId]);
});

test('an orphaned approval blocks the original order even after its link is removed or replaced with a smaller receipt', () => {
  for (const replacement of [false, true]) {
    const f = fixture(); approve(f);
    delete f.store.paytmOrderLinks[f.transactionId];
    if (replacement) {
      f.store.paytmReportTransactions.REPLACEMENT = { ...clone(f.tx), transactionId: 'REPLACEMENT', amount: 100, settledAmount: 100, payoutId: 'NEW-PAYOUT', utr: 'NEW-UTR' };
      f.store.paytmOrderLinks.REPLACEMENT = { ...clone(f.link), transactionId: 'REPLACEMENT', amount: 100 };
    }
    const before = JSON.stringify(f.store);
    assert.throws(() => validateOrderReceiptTotals(f.store, f.orderId, f.sales), /existing verified Shopify order link/i, 'a smaller replacement must not hide the old auditable rounding receipt');
    assert.equal(JSON.stringify(f.store), before, 'rejected validation must not delete or change evidence');
    assert.equal(f.store.receipts.length, 1);
    assert.equal(f.store.paytmRoundingReviews[f.transactionId].receiptId, f.store.receipts[0].id);
  }
});

test('allowExcluded does not let a settlement bypass an active approved rounding receipt', () => {
  const { validateSettlementReview } = require('../modules/paytm-accounting');
  const f = fixture(); approve(f);
  f.store.paytmExcludedTransactions = { [f.transactionId]: { reason: 'Excluded after the rounding receipt was approved' } };
  const before = JSON.stringify(f.store);
  assert.throws(() => validateSettlementReview(f.store, f.tx.utr, f.sales, { allowExcluded: true }), /excluded payment retains an audited rounding receipt/i);
  assert.equal(JSON.stringify(f.store), before);
  assert.equal(f.store.receipts.length, 1);
});
