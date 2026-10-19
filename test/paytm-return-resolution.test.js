const test = require('node:test');
const assert = require('node:assert/strict');
const { summarizePayouts } = require('../modules/paytm-report');
const { applyReturnResolution, reverseReturnResolution, validateReturnResolution, validatePayoutReturnResolutions, returnResolutionView } = require('../modules/paytm-refund-resolutions');
const { validateSettlementReview, settlementStatus } = require('../modules/paytm-accounting');

function fixture() {
  const base = { date: '2026-10-16', settledDate: '2026-10-17', payoutDate: '2026-10-17', utr: 'SYNTHETIC-RETURN-UTR', payoutId: 'SYNTHETIC-RETURN-BATCH', commission: 0, platformFee: 0, gst: 0 };
  const receipt = { ...base, transactionId: 'SYNTHETIC-ACCIDENTAL', time: '16-10-2026 20:15:15', amount: 200, settledAmount: 200, isCustomerPayment: true, transactionType: 'ACQUIRING' };
  const refund = { ...base, transactionId: 'SYNTHETIC-REFUND', time: '16-10-2026 21:36:24', amount: 200, settledAmount: 200, isCustomerPayment: false, transactionType: 'REFUND' };
  const sale = { ...base, transactionId: 'SYNTHETIC-SALE', time: '16-10-2026 17:33:33', amount: 8116, settledAmount: 8116, isCustomerPayment: true, transactionType: 'ACQUIRING' };
  const store = { paytmReportTransactions: Object.fromEntries([receipt, refund, sale].map(tx => [tx.transactionId, tx])), paytmExcludedTransactions: { [receipt.transactionId]: { reason: 'Accidental transfer by a customer.', by: 'admin', at: '2026-10-18T00:00:00Z' } }, paytmOrderLinks: { [sale.transactionId]: { transactionId: sale.transactionId, orderId: 'SYNTHETIC-ORDER', amount: 8116 } } };
  const body = { receiptTransactionId: receipt.transactionId, refundTransactionId: refund.transactionId, reason: 'Confirmed accidental receipt returned to the same customer.', confirmed: true };
  return { store, receipt, refund, sale, body, rows: [{ id: 'SHOPIFY/SYNTHETIC-ORDER', orderId: 'SYNTHETIC-ORDER', account: 'Paytm Settlement Clearing', amount: 8116 }] };
}
const actor = { username: 'admin', at: '2026-10-19T00:00:00Z' };
test('REFUND is separate from actual VAS and does not become a service charge', () => {
  const f = fixture(), vat = { ...f.refund, transactionId: 'SYNTHETIC-VAS', transactionType: 'VAS', amount: 10, settledAmount: 10 };
  const p = summarizePayouts([...Object.values(f.store.paytmReportTransactions), vat])[0];
  assert.equal(p.customerGross, 8316); assert.equal(p.refundAmount, 200); assert.equal(p.refundCount, 1);
  assert.equal(p.nonCustomerAmount, 10); assert.equal(p.nonCustomerCount, 1); assert.equal(p.net, 8106);
});
test('explicit return review creates metadata only and retains exclusion/imported evidence byte for byte', () => {
  const f = fixture(), before = JSON.stringify(f.store), result = applyReturnResolution(f.store, f.body, actor);
  assert.equal(result.created, true); assert.equal(result.resolution.amount, 200);
  const { paytmReturnResolutions, ...unchanged } = f.store;
  assert.equal(JSON.stringify(unchanged), before);
  assert.deepEqual(result.resolution.exclusion, f.store.paytmExcludedTransactions[f.receipt.transactionId]);
  assert.equal(validateReturnResolution(f.store, result.resolution).settlementId, f.receipt.utr);
});
test('valid repeated request is idempotent, even after finalization', () => {
  const f = fixture(); applyReturnResolution(f.store, f.body, actor);
  f.store.paytmVerifiedSettlements = [{ settlementId: f.receipt.utr }];
  const before = JSON.stringify(f.store), retry = applyReturnResolution(f.store, f.body, actor);
  assert.equal(retry.created, false); assert.equal(JSON.stringify(f.store), before);
});
test('unresolved refund blocks payout review instead of being assumed to be an expense', () => {
  const f = fixture(); assert.throws(() => validateSettlementReview(f.store, f.receipt.utr, f.rows, { allowExcluded: true }), /unresolved REFUND/);
});
test('a verified returned receipt resolves the exclusion without inventing a Shopify sale', () => {
  const f = fixture(); applyReturnResolution(f.store, f.body, actor);
  const { payout, linked } = validateSettlementReview(f.store, f.receipt.utr, f.rows);
  assert.equal(payout.effectiveCustomerGross, 8116); assert.equal(payout.customerGross, 8316); assert.equal(payout.returnedReceiptAmount, 200); assert.equal(payout.nonCustomerAmount, 0); assert.equal(payout.refundAmount, 200); assert.equal(linked.length, 1);
  assert.equal(f.store.paytmOrderLinks[f.receipt.transactionId], undefined);
});
for (const [label, mutate] of [
  ['wrong amount', f => { f.refund.amount = 199.99; }],
  ['another UTR', f => { f.refund.utr = 'SYNTHETIC-OTHER'; }],
  ['another settled date', f => { f.refund.settledDate = '2026-10-18'; }],
  ['refund before receipt', f => { f.refund.time = '16-10-2026 19:00:00'; }],
  ['same-time ambiguity', f => { f.refund.time = f.receipt.time; }],
  ['missing same-day time', f => { delete f.refund.time; }],
  ['not a refund', f => { f.refund.transactionType = 'VAS'; }],
  ['non-zero fee', f => { f.receipt.commission = .01; }],
  ['different net', f => { f.refund.settledAmount = 199.99; }],
  ['not excluded', f => { f.store.paytmExcludedTransactions = {}; }],
  ['Shopify claim', f => { f.store.paytmOrderLinks[f.receipt.transactionId] = { orderId: 'OTHER' }; }],
  ['refund manual claim', f => { f.store.paytmManualResolutions = { [f.refund.transactionId]: { type: 'manual_sale' } }; }],
  ['rounding claim', f => { f.store.paytmRoundingReviews = { [f.receipt.transactionId]: {} }; }],
  ['accounting receipt claim', f => { f.store.receipts = [{ paytmTransactionId: f.receipt.transactionId }]; }],
  ['historical transaction claim', f => { f.store.paytmSettlements = [{ transactionIds: [f.receipt.transactionId] }]; }],
  ['posted payout', f => { f.store.paytmPayoutPostings = [{ settlementId: f.receipt.utr }]; }],
  ['finalized review', f => { f.store.paytmVerifiedSettlements = [{ settlementId: f.receipt.utr }]; }],
  ['already attached', f => { f.store.paytmHistoricalEvidenceLinks = { [f.receipt.utr]: {} }; }],
  ['missing confirmation', f => { f.body.confirmed = false; }],
  ['string confirmation', f => { f.body.confirmed = 'true'; }],
  ['missing reason', f => { f.body.reason = ''; }]
]) test('unsafe return is rejected without writes: ' + label, () => {
  const f = fixture(); mutate(f); const before = JSON.stringify(f.store);
  assert.throws(() => applyReturnResolution(f.store, f.body, actor)); assert.equal(JSON.stringify(f.store), before);
});
for (const key of ['amount', 'time', 'transactionType', 'payoutId', 'comments', 'rrn']) test('review fails closed after imported evidence changes: ' + key, () => {
  const f = fixture(), { resolution } = applyReturnResolution(f.store, f.body, actor);
  f.refund[key] = key === 'amount' ? 201 : 'CHANGED';
  assert.throws(() => validateReturnResolution(f.store, resolution));
  assert.throws(() => validatePayoutReturnResolutions(f.store, summarizePayouts(Object.values(f.store.paytmReportTransactions))[0]));
});
test('candidate projection never automatically resolves even an exact amount pair', () => {
  const f = fixture(), before = JSON.stringify(f.store), result = returnResolutionView(f.store);
  assert.equal(result.candidates.length, 1); assert.equal(result.resolutions.length, 0); assert.equal(JSON.stringify(f.store), before);
  applyReturnResolution(f.store, f.body, actor); const done = returnResolutionView(f.store);
  assert.equal(done.candidates.length, 0); assert.equal(done.resolutions[0].valid, true);
});
test('changing the exclusion reason invalidates the saved audit resolution', () => {
  const f = fixture(), { resolution } = applyReturnResolution(f.store, f.body, actor);
  f.store.paytmExcludedTransactions[f.receipt.transactionId].reason = 'Another reason changed afterwards';
  assert.throws(() => validateReturnResolution(f.store, resolution), /changed after review/);
});
test('legacy posted refunds are flagged without silently migrating existing entries', () => {
  const f = fixture(); f.store.paytmPayoutPostings = [{ settlementId: f.receipt.utr, customerGross: 8316, nonCustomerAmount: 200 }];
  const before = JSON.stringify(f.store), status = settlementStatus(f.store, summarizePayouts(Object.values(f.store.paytmReportTransactions))[0], f.rows);
  assert.equal(status.code, 'incomplete'); assert.equal(JSON.stringify(f.store), before);
});
test('changing a posted refund source to VAS cannot hide an invalid return review', () => {
  const f = fixture(); applyReturnResolution(f.store, f.body, actor);
  f.store.paytmPayoutPostings = [{ settlementId: f.receipt.utr, customerGross: 8116, rawCustomerGross: 8316, returnedReceiptAmount: 200, refundAmount: 200, nonCustomerAmount: 0 }];
  f.refund.transactionType = 'VAS';
  const before = JSON.stringify(f.store), status = settlementStatus(f.store, summarizePayouts(Object.values(f.store.paytmReportTransactions))[0], f.rows);
  assert.equal(status.code, 'incomplete'); assert.equal(JSON.stringify(f.store), before);
});
test('unfinalized mistaken return can be corrected with original audit/exclusion evidence retained', () => {
  const f = fixture(), { resolution } = applyReturnResolution(f.store, f.body, actor), sources = JSON.stringify(f.store.paytmReportTransactions), exclusion = JSON.stringify(f.store.paytmExcludedTransactions);
  const history = reverseReturnResolution(f.store, { receiptTransactionId: f.receipt.transactionId, reason: 'Correcting an incorrectly reviewed customer return.' }, actor);
  assert.deepEqual(history.resolution, resolution); assert.equal(f.store.paytmReturnResolutions[f.receipt.transactionId], undefined);
  assert.equal(f.store.paytmReturnResolutionHistory.length, 1); assert.equal(JSON.stringify(f.store.paytmReportTransactions), sources); assert.equal(JSON.stringify(f.store.paytmExcludedTransactions), exclusion);
  assert.equal(returnResolutionView(f.store).candidates.length, 1);
});
for (const key of ['paytmVerifiedSettlements', 'paytmPayoutPostings', 'paytmHistoricalEvidenceLinks']) test('correction cannot undo existing finalized accounting: ' + key, () => {
  const f = fixture(); applyReturnResolution(f.store, f.body, actor);
  f.store[key] = key === 'paytmHistoricalEvidenceLinks' ? { [f.receipt.utr]: { settlementId: f.receipt.utr } } : [{ settlementId: f.receipt.utr }];
  const before = JSON.stringify(f.store);
  assert.throws(() => reverseReturnResolution(f.store, { receiptTransactionId: f.receipt.transactionId, reason: 'Attempting to change final accounting.' }, actor), /finalized/);
  assert.equal(JSON.stringify(f.store), before); assert.equal(returnResolutionView(f.store).resolutions[0].canReverse, false);
});
