const { createHash } = require('crypto');

const cents = value => Math.round(Number(value || 0) * 100);
const transactionIsRefund = tx => String(tx && tx.transactionType || '').trim().toUpperCase() === 'REFUND';
const settlementKey = tx => String(tx && (tx.utr || tx.payoutId) || '');
const fields = ['transactionId', 'date', 'time', 'status', 'amount', 'commission', 'gst', 'platformFee', 'settledAmount', 'payoutId', 'payoutDate', 'utr', 'settledDate', 'transactionType', 'comments', 'paymentMode', 'posId', 'merchantOrderId', 'rrn', 'isCustomerPayment'];
function fingerprint(receipt, refund, exclusion) {
  const evidence = tx => Object.fromEntries(fields.map(key => [key, tx[key] === undefined ? null : tx[key]]));
  return createHash('sha256').update(JSON.stringify({ receipt: evidence(receipt), refund: evidence(refund), exclusion })).digest('hex');
}
function resolutionForTransaction(store, id) {
  return Object.values(store.paytmReturnResolutions || {}).find(review => review.receiptTransactionId === id || review.refundTransactionId === id);
}
function sourceMoment(tx) {
  const date = String(tx.date || ''), clock = String(tx.time || '').match(/(?:^|T|\s)(\d{1,2}):(\d{2})(?::(\d{2}))?/);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error('The actual receipt and refund dates are required.');
  const day = Date.parse(date + 'T00:00:00Z');
  if (!Number.isFinite(day) || new Date(day).toISOString().slice(0, 10) !== date) throw new Error('The receipt or refund date is invalid.');
  if (!clock) return { day, time: null };
  const hour = Number(clock[1]), minute = Number(clock[2]), second = Number(clock[3] || 0);
  if (hour > 23 || minute > 59 || second > 59) throw new Error('The receipt or refund time is invalid.');
  return { day, time: hour * 3600 + minute * 60 + second };
}
function checkUnclaimed(store, ids) {
  for (const id of ids) {
    if ((store.paytmOrderLinks || {})[id] || (store.paytmManualResolutions || {})[id] || (store.paytmRoundingReviews || {})[id]) throw new Error('A receipt/refund already has a Shopify, manual or rounding classification. Correct that classification first.');
    if (['receipts', 'transfers', 'adjustments', 'reconciliationExpenses', 'salesRefunds'].some(key => (store[key] || []).some(record => record.paytmTransactionId === id || record.paytmRoundingTransactionId === id || record.paytmRefundTransactionId === id))) throw new Error('A receipt/refund already has an accounting record. Review it before pairing.');
  }
}
function pairEvidence(store, receiptId, refundId) {
  const transactions = store.paytmReportTransactions || {}, receipt = transactions[receiptId], refund = transactions[refundId];
  if (!receipt || !refund || receiptId === refundId) throw new Error('Choose two distinct imported receipt and refund transactions.');
  if (receipt.transactionId !== receiptId || refund.transactionId !== refundId) throw new Error('The imported receipt/refund transaction identity changed. Review the source evidence.');
  if (receipt.isCustomerPayment === false || transactionIsRefund(receipt) || !transactionIsRefund(refund) || refund.isCustomerPayment !== false) throw new Error('Pair an original customer receipt with an imported REFUND row only.');
  const exclusion = (store.paytmExcludedTransactions || {})[receiptId];
  if (!exclusion || String(exclusion.reason || '').trim().length < 10) throw new Error('Exclude the accidental receipt from Shopify with its actual reason before reviewing its return.');
  if ((store.paytmExcludedTransactions || {})[refundId]) throw new Error('The refund must retain its original REFUND classification, not an exclusion.');
  if (!settlementKey(receipt) || settlementKey(receipt) !== settlementKey(refund) || !receipt.utr || receipt.utr !== refund.utr || !receipt.settledDate || receipt.settledDate !== refund.settledDate) throw new Error('The receipt and refund must have the same exact settlement UTR and settled date.');
  if (![receipt.amount, refund.amount, receipt.settledAmount, refund.settledAmount].every(value => Number.isFinite(Number(value))) || !(Number(receipt.amount) > 0) || cents(receipt.amount) !== cents(refund.amount)) throw new Error('The accidental receipt and refund must have the same exact positive amount.');
  for (const tx of [receipt, refund]) {
    if (['commission', 'platformFee', 'gst'].some(key => cents(tx[key]) !== 0) || cents(tx.settledAmount) !== cents(tx.amount)) throw new Error('A returned accidental receipt must have no unreviewed fees or net difference. Review the original report.');
  }
  const before = sourceMoment(receipt), after = sourceMoment(refund);
  if (before.day > after.day || before.day === after.day && (before.time === null || after.time === null || before.time >= after.time)) throw new Error('The report must show that the receipt occurred before its refund, including times when on the same day.');
  checkUnclaimed(store, [receiptId, refundId]);
  return { receipt, refund, exclusion, settlementId: settlementKey(receipt), amount: cents(receipt.amount) / 100 };
}
function validateReturnResolution(store, review) {
  if (!review || review.kind !== 'accidental_receipt_refund' || !review.by || !review.at || String(review.reason || '').trim().length < 10 || review.confirmed !== true) throw new Error('The accidental receipt/refund review is incomplete.');
  const evidence = pairEvidence(store, review.receiptTransactionId, review.refundTransactionId);
  if (review.settlementId !== evidence.settlementId || cents(review.amount) !== cents(evidence.amount) || review.evidenceFingerprint !== fingerprint(evidence.receipt, evidence.refund, evidence.exclusion)) throw new Error('The imported receipt/refund evidence or exclusion changed after review. Review the pair again before finalizing.');
  const overlaps = Object.values(store.paytmReturnResolutions || {}).filter(other => other !== review && (other.receiptTransactionId === review.receiptTransactionId || other.refundTransactionId === review.refundTransactionId || other.receiptTransactionId === review.refundTransactionId || other.refundTransactionId === review.receiptTransactionId));
  if (overlaps.length) throw new Error('A receipt/refund is claimed by more than one return review.');
  return evidence;
}
function returnCorrectionBlock(store, review) {
  const ids = [review.receiptTransactionId, review.refundTransactionId], payouts = new Set([review.settlementId, ...ids.map(id => settlementKey((store.paytmReportTransactions || {})[id]))].filter(Boolean));
  if ([...(store.paytmPayoutPostings || []), ...(store.paytmVerifiedSettlements || [])].some(record => payouts.has(record.settlementId) || payouts.has(record.payoutId) || (record.transactionIds || []).some(id => ids.includes(id))) || (store.paytmSettlements || []).some(record => (record.transactionIds || []).some(id => ids.includes(id))) || Object.entries(store.paytmHistoricalEvidenceLinks || {}).some(([key, link]) => payouts.has(key) || payouts.has(link.settlementId))) return 'The return is in a finalized, posted or historically attached settlement. Its existing accounting must be reviewed before correction.';
  return '';
}
function reverseReturnResolution(store, body, actor = {}) {
  const receiptId = String(body.receiptTransactionId || '').trim(), reason = String(body.reason || '').trim(), review = (store.paytmReturnResolutions || {})[receiptId];
  if (!review) throw new Error('Active accidental receipt/refund review not found.');
  if (reason.length < 10 || !actor.username) throw new Error('Enter a correction reason of at least 10 characters.');
  const blocked = returnCorrectionBlock(store, review);
  if (blocked) throw new Error(blocked);
  const history = { resolution: JSON.parse(JSON.stringify(review)), reversedBy: actor.username, reversedAt: actor.at || new Date().toISOString(), reason };
  store.paytmReturnResolutionHistory = store.paytmReturnResolutionHistory || [];
  store.paytmReturnResolutionHistory.push(history);
  delete store.paytmReturnResolutions[receiptId];
  return history;
}
function applyReturnResolution(store, body, actor = {}) {
  const receiptId = String(body.receiptTransactionId || '').trim(), refundId = String(body.refundTransactionId || '').trim(), reason = String(body.reason || '').trim();
  if (body.confirmed !== true || reason.length < 10 || !actor.username) throw new Error('Confirm that this refund returned the same accidental receipt and enter a review reason of at least 10 characters.');
  const existing = resolutionForTransaction(store, receiptId) || resolutionForTransaction(store, refundId);
  if (existing) {
    if (existing.receiptTransactionId !== receiptId || existing.refundTransactionId !== refundId) throw new Error('One of these transactions already belongs to another reviewed return.');
    validateReturnResolution(store, existing);
    return { created: false, resolution: existing };
  }
  const evidence = pairEvidence(store, receiptId, refundId), ids = [receiptId, refundId];
  if ([...(store.paytmPayoutPostings || []), ...(store.paytmVerifiedSettlements || [])].some(record => record.settlementId === evidence.settlementId || record.payoutId === evidence.settlementId || (record.transactionIds || []).some(id => ids.includes(id))) || (store.paytmHistoricalEvidenceLinks || {})[evidence.settlementId]) throw new Error('This settlement was already posted, finalized or attached. Correct its existing review before adding a return resolution.');
  if ((store.paytmSettlements || []).some(record => (record.transactionIds || []).some(id => ids.includes(id)))) throw new Error('An existing historical settlement already claims this receipt/refund. Review that accounting record first.');
  const resolution = { kind: 'accidental_receipt_refund', receiptTransactionId: receiptId, refundTransactionId: refundId, settlementId: evidence.settlementId, amount: evidence.amount, reason, confirmed: true, by: actor.username, at: actor.at || new Date().toISOString(), exclusion: JSON.parse(JSON.stringify(evidence.exclusion)), evidenceFingerprint: fingerprint(evidence.receipt, evidence.refund, evidence.exclusion) };
  store.paytmReturnResolutions = store.paytmReturnResolutions || {};
  store.paytmReturnResolutions[receiptId] = resolution;
  return { created: true, resolution };
}
function validatePayoutReturnResolutions(store, payout) {
  const ids = payout.transactionIds || [], reviews = Object.values(store.paytmReturnResolutions || {}).filter(review => review.settlementId === payout.settlementId || ids.includes(review.receiptTransactionId) || ids.includes(review.refundTransactionId));
  let returnedCents = 0;
  for (const review of reviews) {
    validateReturnResolution(store, review);
    if (!ids.includes(review.receiptTransactionId) || !ids.includes(review.refundTransactionId) || review.settlementId !== payout.settlementId) throw new Error('The reviewed receipt/refund no longer belongs to this exact payout.');
    returnedCents += cents(review.amount);
  }
  const refunds = ids.filter(id => transactionIsRefund((store.paytmReportTransactions || {})[id]));
  if (refunds.some(id => !reviews.some(review => review.refundTransactionId === id))) throw new Error('This payout contains an unresolved REFUND. Review its original receipt/refund treatment; a refund is not a Paytm charge.');
  if (returnedCents !== cents(payout.refundAmount)) throw new Error('Reviewed accidental returns do not equal this payout’s imported refund total.');
  return { reviews, returnedReceiptAmount: returnedCents / 100, resolvedReceiptIds: reviews.map(review => review.receiptTransactionId), resolvedRefundIds: reviews.map(review => review.refundTransactionId) };
}
function returnResolutionView(store) {
  const resolutions = Object.values(store.paytmReturnResolutions || {}).map(review => {
    const reverseBlockedReason = returnCorrectionBlock(store, review), correction = { canReverse: !reverseBlockedReason, reverseBlockedReason };
    try { validateReturnResolution(store, review); return { ...review, ...correction, valid: true }; }
    catch (error) { return { ...review, ...correction, valid: false, error: error.message }; }
  });
  const candidates = [], transactions = Object.values(store.paytmReportTransactions || {});
  for (const receipt of transactions.filter(tx => tx.isCustomerPayment !== false && (store.paytmExcludedTransactions || {})[tx.transactionId] && !resolutionForTransaction(store, tx.transactionId))) {
    for (const refund of transactions.filter(transactionIsRefund)) {
      if (resolutionForTransaction(store, refund.transactionId)) continue;
      try {
        const evidence = pairEvidence(store, receipt.transactionId, refund.transactionId);
        if ([...(store.paytmPayoutPostings || []), ...(store.paytmVerifiedSettlements || [])].some(record => record.settlementId === evidence.settlementId || record.payoutId === evidence.settlementId || (record.transactionIds || []).includes(receipt.transactionId)) || (store.paytmHistoricalEvidenceLinks || {})[evidence.settlementId]) continue;
        candidates.push({ receiptTransactionId: receipt.transactionId, refundTransactionId: refund.transactionId, settlementId: evidence.settlementId, amount: evidence.amount, receiptDate: receipt.date, receiptTime: receipt.time || '', refundDate: refund.date, refundTime: refund.time || '', exclusionReason: evidence.exclusion.reason });
      } catch { /* Equal amounts are never sufficient: expose only evidence-compatible pairs. */ }
    }
  }
  return { resolutions, candidates };
}

module.exports = { transactionIsRefund, resolutionForTransaction, applyReturnResolution, reverseReturnResolution, validateReturnResolution, validatePayoutReturnResolutions, returnResolutionView };
