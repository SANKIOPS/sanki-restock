'use strict';

const crypto = require('node:crypto');

const CLEARING = 'Paytm Settlement Clearing';
const ROUNDING_SOURCE = 'Paytm receipt rounding';
const VERSION = 1;
const money = value => {
  const number = Number(value);
  if (value == null || value === '' || !Number.isFinite(number) || !Number.isSafeInteger(Math.round(number * 100)) || Math.abs(number * 100 - Math.round(number * 100)) > 0.000001) throw new Error('An amount is missing or is not an exact two-decimal monetary value.');
  return Math.round(number * 100);
};
const rupees = value => value / 100;
const text = value => String(value == null ? '' : value);
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().filter(key => value[key] !== undefined).map(key => [key, canonical(value[key])]));
  return value;
}
const fingerprint = value => crypto.createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const clone = value => JSON.parse(JSON.stringify(value));
function validDate(date) {
  return /^\d{4}-\d{2}-\d{2}$/.test(text(date)) && !Number.isNaN(Date.parse(date + 'T00:00:00Z')) && new Date(date + 'T00:00:00Z').toISOString().slice(0, 10) === date;
}
function transactionEvidence(tx) {
  const fields = ['transactionId', 'date', 'time', 'amount', 'commission', 'gst', 'platformFee', 'settledAmount', 'payoutId', 'payoutDate', 'utr', 'settledDate', 'transactionType', 'comments', 'paymentMode', 'posId', 'merchantOrderId', 'rrn', 'source', 'isCustomerPayment'];
  return Object.fromEntries(fields.filter(key => tx[key] !== undefined).map(key => [key, tx[key]]));
}
function linkedEvidence(store, orderId) {
  return Object.entries(store.paytmOrderLinks || {}).filter(([, link]) => text(link.orderId) === text(orderId)).map(([id, link]) => ({ transactionId: id, link: clone(link), transaction: transactionEvidence((store.paytmReportTransactions || {})[id] || {}) })).sort((a, b) => a.transactionId.localeCompare(b.transactionId));
}
function sourceEvidence(saleRows, orderId) {
  const rows = (saleRows || []).filter(row => text(row.orderId) === text(orderId) && row.account === CLEARING);
  if (!rows.length) throw new Error('No verified Shopify payment component exists in Paytm Settlement Clearing. Confirm the payment split first.');
  const ids = new Set();
  const baseline = rows.map(row => {
    const id = text(row.id);
    if (!id || ids.has(id) || row.accountingExcluded || money(row.amount) < 0) throw new Error('The Shopify Paytm payment components are missing, duplicated or invalid. Review the source records.');
    ids.add(id);
    return { id, amount: rupees(money(row.amount)), date: text(row.date), account: row.account, orderId: text(row.orderId) };
  }).sort((a, b) => a.id.localeCompare(b.id));
  const amount = baseline.reduce((sum, row) => sum + money(row.amount), 0);
  if (!(amount > 0)) throw new Error('The Shopify Paytm component is zero or unverified. Confirm the payment split first.');
  return { baseline, amount, fingerprint: fingerprint(baseline) };
}
function bankOverride(store, baseline) {
  return baseline.some(row => (store.bankDateOverrides || {})[row.id]);
}
function finished(store, tx, orderId) {
  const payoutIds = new Set([text(tx.payoutId), text(tx.utr)].filter(Boolean));
  return ['paytmVerifiedSettlements', 'paytmPayoutPostings', 'paytmSettlements'].some(key => (store[key] || []).some(row =>
    (row.transactionIds || []).map(text).includes(text(tx.transactionId)) ||
    payoutIds.has(text(row.payoutId)) || payoutIds.has(text(row.settlementId)) ||
    (row.orderIds || []).map(text).includes(text(orderId))));
}
function linkedContext(store, transactionId) {
  const id = text(transactionId), tx = (store.paytmReportTransactions || {})[id], link = (store.paytmOrderLinks || {})[id];
  if (!id || !tx || text(tx.transactionId) !== id) throw new Error('Paytm transaction not found or its report identity changed.');
  if (tx.isCustomerPayment === false || (tx.transactionType && text(tx.transactionType).toUpperCase() !== 'ACQUIRING')) throw new Error('Rounding is only available for a Paytm customer payment, not a deduction or adjustment.');
  if (!link || !link.orderId || text(link.transactionId) !== id || link.type) throw new Error('An existing verified Shopify order link is required for Paytm rounding.');
  if ((store.paytmExcludedTransactions || {})[id] || (store.paytmManualResolutions || {})[id]) throw new Error('Excluded or manually classified Paytm transactions cannot receive Shopify rounding treatment.');
  if (!(money(tx.amount) > 0) || money(link.amount) !== money(tx.amount)) throw new Error('The linked Paytm amount no longer equals the customer receipt. Review the link first.');
  if (!validDate(tx.date)) throw new Error('The actual Paytm customer receipt date is missing or invalid.');
  return { id, tx, link, linked: linkedEvidence(store, link.orderId) };
}
function receiptProjection(receipt) {
  return {
    id: receipt.id, nature: receipt.nature, account: receipt.account, amount: receipt.amount,
    receiptType: receipt.receiptType, category: receipt.category, source: receipt.source,
    date: receipt.date, note: receipt.note, paytmRoundingTransactionId: receipt.paytmRoundingTransactionId,
    paytmRoundingOrderId: receipt.paytmRoundingOrderId, paytmReportEvidence: receipt.paytmReportEvidence,
    createdBy: receipt.createdBy, createdAt: receipt.createdAt, proofException: receipt.proofException,
    accountingExcluded: receipt.accountingExcluded, paytmTransactionId: receipt.paytmTransactionId
  };
}
function validateRecordedReview(store, context, review, source) {
  const { id, tx, link, linked } = context;
  const changed = message => { throw new Error('Recorded Paytm rounding changed or lost its evidence: ' + message + '. Review it again before finalizing.'); };
  if (!review || review.version !== VERSION || review.status !== 'approved' || review.transactionId !== id || text(review.orderId) !== text(link.orderId) || text(review.orderNumber) !== text(link.orderNumber)) changed('order link');
  const delta = money(review.difference), sourceAmount = money(review.sourceAmount), receiptAmount = money(review.receiptAmount);
  if (!(delta > 0 && delta < 500) || !(sourceAmount > 0) || receiptAmount !== money(tx.amount) || receiptAmount - sourceAmount !== delta || review.date !== tx.date || text(review.utr) !== text(tx.utr)) changed('amount or receipt date');
  if (!Array.isArray(review.sourceBaseline) || !review.sourceBaseline.length || review.sourceBaseline.some(row => row.account !== CLEARING || text(row.orderId) !== text(link.orderId)) || review.sourceBaseline.reduce((sum, row) => sum + money(row.amount), 0) !== sourceAmount || review.sourceFingerprint !== fingerprint(review.sourceBaseline)) changed('Shopify source baseline');
  const sourceRows = review.sourceBaseline.map(row => ({ id: row.id, amount: row.amount }));
  if (fingerprint(sourceRows) !== fingerprint(review.sourceRows)) changed('Shopify source rows');
  if (source && source.fingerprint !== review.sourceFingerprint) changed('Shopify payment components');
  if (bankOverride(store, source ? source.baseline : review.sourceBaseline)) changed('the Shopify component now has a bank link');
  if (review.transactionFingerprint !== fingerprint(transactionEvidence(tx)) || review.transactionFingerprint !== fingerprint(review.transactionEvidence)) changed('Paytm report');
  if (review.linkedFingerprint !== fingerprint(linked) || review.linkedFingerprint !== fingerprint(review.linkedEvidence) || linked.length !== 1 || linked[0].transactionId !== id) changed('the linked receipt set');
  if (!text(review.by).trim() || text(review.reason).trim().length < 10 || Number.isNaN(Date.parse(review.at))) changed('approval provenance');
  const matching = (store.receipts || []).filter(receipt => receipt.id === review.receiptId || receipt.paytmRoundingTransactionId === id);
  if (matching.length !== 1) changed('missing or duplicate rounding income receipt');
  const receipt = matching[0];
  if (receipt.id !== review.receiptId || receipt.nature !== 'SANKI' || receipt.account !== CLEARING || receipt.receiptType !== 'other_income' || receipt.category !== 'Rounding Off' || receipt.source !== ROUNDING_SOURCE || receipt.date !== tx.date || money(receipt.amount) !== delta || receipt.paytmRoundingTransactionId !== id || text(receipt.paytmRoundingOrderId) !== text(link.orderId) || receipt.paytmTransactionId !== undefined || receipt.accountingExcluded || receipt.createdBy !== review.by || receipt.createdAt !== review.at) changed('rounding income receipt');
  const evidence = receipt.paytmReportEvidence;
  if (!evidence || evidence.transactionId !== id || text(evidence.orderId) !== text(link.orderId) || evidence.date !== tx.date || money(evidence.difference) !== delta || money(evidence.receiptAmount) !== receiptAmount || money(evidence.sourceAmount) !== sourceAmount || evidence.sourceFingerprint !== review.sourceFingerprint || evidence.transactionFingerprint !== review.transactionFingerprint || evidence.linkedFingerprint !== review.linkedFingerprint || evidence.reviewedBy !== review.by || evidence.reviewedAt !== review.at || fingerprint(evidence.sourceRows) !== fingerprint(review.sourceRows)) changed('rounding receipt provenance');
  if (review.receiptFingerprint !== fingerprint(receiptProjection(receipt))) changed('rounding income receipt fingerprint');
  return receipt;
}

/** Read-only review. Missing or unsafe data is returned as an explicit blocker. */
function roundingReview(store, transactionId, saleRows) {
  const result = { eligible: false, approved: false, transactionId: text(transactionId) };
  try {
    const context = linkedContext(store, transactionId), { id, tx, link, linked } = context;
    const source = sourceEvidence(saleRows, link.orderId);
    const received = linked.reduce((sum, row) => sum + money(row.link.amount), 0), delta = received - source.amount;
    Object.assign(result, {
      difference: rupees(delta), receiptAmount: rupees(money(tx.amount)), sourceAmount: rupees(source.amount),
      sourceRows: source.baseline.map(row => ({ id: row.id, amount: row.amount })),
      orderId: text(link.orderId), orderNumber: text(link.orderNumber), date: tx.date,
      utr: text(tx.utr), payoutId: text(tx.payoutId),
      linkedTransactions: linked.map(row => ({ transactionId: row.transactionId, amount: row.link.amount }))
    });
    const existing = (store.paytmRoundingReviews || {})[id];
    if (existing) {
      const receipt = validateRecordedReview(store, context, existing, source);
      return { ...result, eligible: true, approved: true, receiptId: receipt.id, by: existing.by, at: existing.at, reason: existing.reason };
    }
    if (linked.length !== 1) throw new Error('This Shopify order has multiple linked Paytm receipts. Review the aggregate payment split manually; automatic rounding approval is unavailable.');
    if (finished(store, tx, link.orderId)) throw new Error('This Paytm settlement is already finalized or posted. Rounding cannot be added to completed records.');
    if (bankOverride(store, source.baseline)) throw new Error('A Shopify payment component is already linked directly to a bank transaction. Correct that link before approving Paytm rounding.');
    if (!(delta > 0 && delta < 500)) throw new Error('Paytm rounding requires a positive aggregate receipt difference below ₹5. Zero, negative and ₹5-or-more differences need a separate review.');
    if ((store.receipts || []).some(receipt => receipt.paytmRoundingTransactionId === id)) throw new Error('A rounding income receipt already exists without a valid approval record. Review it before adding anything.');
    return { ...result, eligible: true };
  } catch (error) {
    return { ...result, error: error.message };
  }
}

/** Creates exactly one other-income receipt after explicit, current-data review. */
function applyRoundingReview(store, transactionId, saleRows, options = {}) {
  const preview = roundingReview(store, transactionId, saleRows);
  if (!preview.eligible) throw new Error(preview.error || 'Paytm transaction is not eligible for rounding treatment.');
  if (money(options.difference) !== money(preview.difference)) throw new Error('The rounding difference changed. Refresh and confirm the exact current difference.');
  if (preview.approved) return { review: (store.paytmRoundingReviews || {})[preview.transactionId], receipt: (store.receipts || []).find(row => row.id === preview.receiptId), created: false };
  const reason = text(options.reason).trim(), username = text(options.username).trim(), at = text(options.at);
  if (reason.length < 10 || !username || !at || Number.isNaN(Date.parse(at))) throw new Error('An explicit review reason, reviewer and approval time are required for Paytm rounding.');
  const context = linkedContext(store, transactionId), { id, tx, link, linked } = context, source = sourceEvidence(saleRows, link.orderId);
  let sequence = Number(store.receiptSeq || 0);
  if (!Number.isSafeInteger(sequence) || sequence < 0) throw new Error('Receipt numbering is invalid. Repair it before recording rounding.');
  let receiptId;
  do { sequence++; receiptId = 'REC-' + String(sequence).padStart(5, '0'); } while ((store.receipts || []).some(receipt => receipt.id === receiptId));
  const review = {
    version: VERSION, status: 'approved', transactionId: id, orderId: text(link.orderId), orderNumber: text(link.orderNumber),
    difference: preview.difference, receiptAmount: preview.receiptAmount, sourceAmount: preview.sourceAmount,
    date: tx.date, utr: text(tx.utr), payoutId: text(tx.payoutId), receiptId, by: username, at, reason,
    sourceRows: preview.sourceRows, sourceBaseline: source.baseline, sourceFingerprint: source.fingerprint,
    transactionEvidence: transactionEvidence(tx), transactionFingerprint: fingerprint(transactionEvidence(tx)),
    linkedEvidence: linked, linkedFingerprint: fingerprint(linked)
  };
  const receipt = {
    id: receiptId, nature: 'SANKI', account: CLEARING, amount: preview.difference, receiptType: 'other_income', category: 'Rounding Off',
    source: ROUNDING_SOURCE, date: tx.date,
    note: `Paytm ${id} / Shopify #${text(link.orderNumber)}: actual receipt ₹${preview.receiptAmount.toFixed(2)} less verified Shopify Paytm component ₹${preview.sourceAmount.toFixed(2)} = ₹${preview.difference.toFixed(2)} rounding income. ${reason}`,
    proof: '', proofs: [], proofException: 'Verified imported Paytm report and Shopify payment component evidence; see paytmReportEvidence.',
    paytmRoundingTransactionId: id, paytmRoundingOrderId: text(link.orderId),
    paytmReportEvidence: {
      transactionId: id, orderId: text(link.orderId), orderNumber: text(link.orderNumber), date: tx.date, utr: text(tx.utr), payoutId: text(tx.payoutId),
      receiptAmount: preview.receiptAmount, sourceAmount: preview.sourceAmount, difference: preview.difference,
      sourceRows: preview.sourceRows, sourceFingerprint: review.sourceFingerprint, transactionFingerprint: review.transactionFingerprint,
      linkedFingerprint: review.linkedFingerprint, reviewedBy: username, reviewedAt: at
    }, createdBy: username, createdAt: at
  };
  review.receiptFingerprint = fingerprint(receiptProjection(receipt));
  // All checks above are read-only. Commit the receipt and approval together.
  store.receiptSeq = sequence;
  store.receipts = store.receipts || [];
  store.paytmRoundingReviews = store.paytmRoundingReviews || {};
  store.receipts.push(receipt);
  store.paytmRoundingReviews[id] = review;
  return { review, receipt, created: true };
}

/** Durable baseline for sales derivation; never promotes rounding into sales. */
function approvedBaseAmount(store, link) {
  const id = text(link && link.transactionId), review = (store.paytmRoundingReviews || {})[id];
  if (!review) return rupees(money(link.amount));
  const context = linkedContext(store, id);
  if (fingerprint(context.link) !== fingerprint(link)) throw new Error('The supplied Paytm link differs from the approved rounding link.');
  validateRecordedReview(store, context, review);
  return rupees(money(link.amount) - money(review.difference));
}

/** Strict aggregate check across all payouts. There is no implicit tolerance. */
function validateOrderReceiptTotals(store, orderId, saleRows) {
  const source = sourceEvidence(saleRows, orderId), linked = linkedEvidence(store, orderId);
  for (const review of Object.values(store.paytmRoundingReviews || {}).filter(row => text(row.orderId) === text(orderId))) {
    validateRecordedReview(store, linkedContext(store, review.transactionId), review, source);
  }
  let received = 0, approvedDelta = 0;
  for (const row of linked) {
    const context = linkedContext(store, row.transactionId);
    received += money(context.link.amount);
    const review = (store.paytmRoundingReviews || {})[row.transactionId];
    if (review) { validateRecordedReview(store, context, review, source); approvedDelta += money(review.difference); }
  }
  if (received > source.amount + approvedDelta) throw new Error(`Shopify order ${linked[0]?.link.orderNumber || orderId} Paytm receipts exceed its verified Paytm payment component. Review the exact rounding or payment split first.`);
  return { orderId: text(orderId), receiptAmount: rupees(received), sourceAmount: rupees(source.amount), approvedDifference: rupees(approvedDelta) };
}

module.exports = { CLEARING, roundingReview, applyRoundingReview, approvedBaseAmount, validateOrderReceiptTotals };
