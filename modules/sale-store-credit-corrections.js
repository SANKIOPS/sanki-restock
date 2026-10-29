'use strict';

const crypto = require('crypto');
const TREATMENT = 'exchange_store_credit';
const WALLET_PENDING = 'pending_external_user_action';
const own = (value, key) => value && Object.prototype.hasOwnProperty.call(value, key) ? value[key] : undefined;
const copy = value => value === undefined ? undefined : JSON.parse(JSON.stringify(value));
const text = value => String(value == null ? '' : value).trim();
const money = value => Math.round((Number(value) || 0) * 100) / 100;
const isStoreCredit = value => /store[\s_-]*credit|gift[\s_-]*card/i.test(text(value));
const isCash = value => /^cash$/i.test(text(value));
const successfulReceipt = tx => ['sale', 'capture'].includes(text(tx.kind).toLowerCase()) && text(tx.status).toLowerCase() === 'success' && Number(tx.amount) > 0;
const successfulRefund = tx => (!tx.kind || text(tx.kind).toLowerCase() === 'refund') && (!tx.status || text(tx.status).toLowerCase() === 'success') && Number(tx.amount) > 0;
function fail(message, status = 400) { const error = new Error(message); error.status = status; throw error; }
function cents(value, label) {
  const n = typeof value === 'number' || typeof value === 'string' && value.trim() !== '' ? Number(value) : NaN;
  if (!Number.isFinite(n) || n < 0 || !Number.isSafeInteger(Math.round(n * 100)) || Math.abs(n * 100 - Math.round(n * 100)) > 0.000001) fail(label + ' must be a valid INR amount with at most two decimal places.');
  return Math.round(n * 100);
}
function list(ordersMap) {
  if (ordersMap instanceof Map) return Array.from(ordersMap.values());
  if (Array.isArray(ordersMap)) return ordersMap;
  return Object.values(ordersMap || {});
}
function byId(ordersMap, id) { return list(ordersMap).find(order => order && text(order.id) === text(id)); }
function orderNumber(order) { return text(order.orderNumber || order.number || order.name || order.id); }
function normalizedNumber(value) {
  const s = text(value).replace(/^#\s*/, '').toLowerCase();
  return /^\d+$/.test(s) ? s.replace(/^0+(?=\d)/, '') : s;
}
function transactionSnapshot(tx) {
  return { id:text(tx.id), parentId:text(tx.parentId || tx.parent_id), refundId:text(tx.refundId), kind:text(tx.kind).toLowerCase(), status:text(tx.status).toLowerCase(), gateway:text(tx.gateway || tx.payment_gateway).toLowerCase(), amount:Number(tx.amount), currency:text(tx.currency).toUpperCase(), processedAt:text(tx.processedAt || tx.processed_at || tx.created_at) };
}
function sorted(rows) { return rows.slice().sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))); }
function refunds(order) {
  const rows = [...(Array.isArray(order.refundTransactions) ? order.refundTransactions : []), ...(order.paymentTransactions || []).filter(tx => text(tx.kind).toLowerCase() === 'refund')];
  (order.refunds || []).forEach(refund => (refund.transactions || []).forEach(tx => rows.push({ ...tx, refundId:tx.refundId || refund.id })));
  const seen = new Set();
  return rows.filter(tx => {
    const key = text(tx.id) || JSON.stringify(transactionSnapshot(tx));
    if (seen.has(key)) return false;
    seen.add(key); return true;
  });
}
// Unrelated notes, addresses and updatedAt changes are not payment evidence.
function fingerprint(order) {
  if (!order || typeof order !== 'object') return '';
  const snapshot = {
    id:text(order.id), orderNumber:normalizedNumber(orderNumber(order)), customerId:text(order.customer && order.customer.id),
    channel:text(order.channel).toUpperCase(), currency:text(order.currency || 'INR').toUpperCase(),
    createdAt:text(order.createdAt || order.created_at), processedAt:text(order.processedAt || order.processed_at),
    cancelledAt:text(order.cancelledAt || order.cancelled_at || order.canceledAt), financialStatus:text(order.financialStatus || order.financial_status).toLowerCase(),
    total:Number(order.total), refundAmount:Number(order.refundAmount || 0), moneyRefunded:Number(order.moneyRefunded || 0), storeCreditIssued:Number(order.storeCreditIssued || 0),
    gateways:sorted((order.paymentGateways || []).map(value => text(value).toLowerCase())),
    transactions:sorted((order.paymentTransactions || []).map(transactionSnapshot)), refunds:sorted(refunds(order).map(transactionSnapshot)),
    creditNotes:sorted((order.refunds || []).map(refund => ({ id:text(refund.id), amount:Number(refund.amount || 0), adjustmentAmount:Number(refund.adjustmentAmount || 0), lineItems:sorted((refund.lineItems || []).map(row => ({ id:text(row.lineItemId || row.id), qty:Number(row.qty || row.quantity || 0), subtotal:Number(row.subtotal || 0), tax:Number(row.tax || 0) }))) })))
  };
  return crypto.createHash('sha256').update(JSON.stringify(snapshot)).digest('hex');
}
function hasOrderReference(record, orderId, saleId) {
  if (!record || typeof record !== 'object') return false;
  return text(record.orderId) === orderId || text(record.saleId) === saleId || (record.orderIds || []).some(id => text(id) === orderId) || (record.saleIds || []).some(id => text(id) === saleId);
}
function conflicts(store, saleId, orderId) {
  for (const key of ['saleAllocationOverrides', 'saleComponentAmountOverrides']) {
    if (Object.keys(store[key] || {}).some(id => id === saleId || id.startsWith(saleId + '/'))) fail('This sale already has a payment allocation or component correction. Review its existing source correction first.', 409);
  }
  for (const key of ['paytmOrderLinks', 'paytmRoundingReviews', 'paytmPayoutPostings', 'paytmVerifiedSettlements', 'gokwikOrderLinks', 'gokwikPostings']) {
    if (Object.values(store[key] || {}).some(record => hasOrderReference(record, orderId, saleId))) fail('This sale has reviewed Paytm/GoKwik payment-provider evidence or an audited rounding receipt. Correct its settlement source before changing the tender.', 409);
  }
  const payment = own(store.paytmShopifyPayments, orderId);
  if (payment && Number(payment.paytmAmount) > 0) fail('This sale has a verified Paytm payment. Review that payment before correcting the tender.', 409);
}
function assertSource(record, replacement, original) {
  if (record.sourceFingerprint && record.sourceFingerprint !== fingerprint(replacement) || record.originalFingerprint && record.originalFingerprint !== fingerprint(original)) fail('The Shopify payment or original credit evidence changed. Review the current orders before approving this correction.', 409);
}
function sameIntent(left, right) {
  return text(left.saleId) === right.saleId && text(left.originalOrderId) === right.originalOrderId && money(left.storeCreditAmount) === right.storeCreditAmount && text(left.reason) === right.reason && left.treatment === TREATMENT;
}
function prepare(ordersMap, accountingStore, input, options = {}) {
  const store = accountingStore || {}, body = input || {}, saleId = text(body.saleId);
  if (!/^SHOPIFY\/[^/]+$/.test(saleId)) fail('Choose the complete Shopify replacement order.');
  const orderId = saleId.slice(8), replacement = byId(ordersMap, orderId);
  if (!replacement) fail('The Shopify replacement order was not found.');
  const targetNumber = normalizedNumber(body.originalOrderNumber);
  if (!targetNumber) fail('Enter the original exchanged order number.');
  const matches = list(ordersMap).filter(order => order && [order.orderNumber, order.number, order.name].some(value => value != null && normalizedNumber(value) === targetNumber));
  if (matches.length !== 1) fail(matches.length ? 'The original order number is ambiguous. Review the order evidence.' : 'The original exchanged order was not found.');
  const original = matches[0];
  if (text(original.id) === orderId) fail('Choose a different original order for this exchange.');
  assertSource(body, replacement, original);
  if ([replacement, original].some(order => text(order.channel).toUpperCase() !== 'POS')) fail('Store-credit exchange corrections require two POS orders.');
  if ([replacement, original].some(order => text(order.currency || 'INR').toUpperCase() !== 'INR')) fail('Only INR store-credit exchanges are supported.');
  const customerId = text(replacement.customer && replacement.customer.id);
  if (!customerId || customerId !== text(original.customer && original.customer.id)) fail('Both orders must have the same confirmed Shopify customer ID.');
  if (replacement.cancelledAt || replacement.cancelled_at || replacement.canceledAt || /cancelled|canceled|voided/i.test(text(replacement.financialStatus))) fail('A cancelled replacement order cannot use this correction.', 409);
  const targetRefundCents = ['refundAmount', 'moneyRefunded', 'storeCreditIssued'].map(key => replacement[key] == null ? 0 : cents(replacement[key], 'Replacement refund'));
  if (targetRefundCents.some(amount => amount > 0) || refunds(replacement).some(successfulRefund) || (replacement.refunds || []).some(row => Number(row.amount) > 0 || (row.lineItems || []).some(item => Number(item.qty || item.quantity) > 0))) fail('The replacement order already has a refund or return. Review it in its source workflow.', 409);
  const amountCents = cents(body.storeCreditAmount, 'Store-credit amount'), totalCents = cents(replacement.total, 'Replacement total');
  if (!amountCents || amountCents !== totalCents) fail('This correction requires store credit equal to the full replacement order total.');
  const reason = text(body.reason);
  if (!reason || reason.length > 2000) fail('Enter a correction reason of no more than 2,000 characters.');
  const transactions = (replacement.paymentTransactions || []).filter(successfulReceipt);
  if (!transactions.length || transactions.some(tx => !isCash(tx.gateway || tx.payment_gateway))) fail('The replacement must have only successful cash receipt transactions.');
  const transactionIds = transactions.map(tx => text(tx.id));
  if (transactionIds.some(id => !id) || new Set(transactionIds).size !== transactionIds.length) fail('The replacement cash transaction IDs are missing or duplicated. Review the Shopify source.', 409);
  if (transactions.some(tx => tx.currency && text(tx.currency).toUpperCase() !== 'INR') || transactions.reduce((sum, tx) => sum + cents(tx.amount, 'Cash receipt'), 0) !== totalCents) fail('Successful cash receipts must exactly match the replacement order total.');
  const issuedCents = cents(original.storeCreditIssued, 'Original store credit issued');
  if (issuedCents < amountCents) fail('The original order does not show enough issued store credit for this exchange.', 409);
  const originalRefunds = refunds(original).filter(successfulRefund);
  const evidencedCreditCents = originalRefunds.filter(tx => isStoreCredit(tx.gateway || tx.payment_gateway)).reduce((sum, tx) => sum + cents(tx.amount, 'Original store-credit refund'), 0);
  const creditCapacityCents = originalRefunds.length ? Math.min(issuedCents, evidencedCreditCents) : issuedCents;
  if (creditCapacityCents < amountCents) fail('The original refund transaction evidence does not support this store-credit amount.', 409);
  conflicts(store, saleId, orderId);
  const proposal = { saleId, orderId, orderNumber:orderNumber(replacement), originalOrderId:text(original.id), originalOrderNumber:orderNumber(original), date:text(replacement.processedAt || replacement.createdAt).slice(0, 10), gross:totalCents / 100, cashAmount:0, nonCashAmount:0, storeCreditAmount:amountCents / 100, transactionIds:transactionIds.sort(), sourceFingerprint:fingerprint(replacement), originalFingerprint:fingerprint(original), reason, walletStatus:WALLET_PENDING, treatment:TREATMENT };
  const existing = own(store.saleStoreCreditCorrections, saleId);
  if (existing) {
    assertSource(existing, replacement, original);
    if (!sameIntent(existing, proposal)) fail('This sale already has a different approved store-credit correction. Review the existing correction first.', 409);
  }
  let usedCents = 0;
  Object.entries(store.saleStoreCreditCorrections || {}).forEach(([key, record]) => {
    if (key !== saleId && text(record.originalOrderId) === proposal.originalOrderId) usedCents += cents(record.storeCreditAmount, 'Previously allocated credit');
  });
  (store.requests || []).filter(request => request.kind === 'sale_store_credit' && request.status === 'pending' && !(text(options.excludeRequestId) && text(request.id) === text(options.excludeRequestId))).forEach(request => {
    const meta = request.meta || {};
    if (text(meta.saleId) === saleId) {
      assertSource(meta, replacement, original);
      if (!sameIntent(meta, proposal)) fail('A different correction for this replacement is already awaiting Owner approval.', 409);
      return;
    }
    if (text(meta.originalOrderId) === proposal.originalOrderId) usedCents += cents(meta.storeCreditAmount, 'Reserved credit');
  });
  if (usedCents + amountCents > creditCapacityCents) fail('This original credit is already allocated or reserved for another replacement order.', 409);
  // Local approval cannot establish that Shopify has debited the wallet.
  return proposal;
}
function correctionFor(order, store) { return own(store && store.saleStoreCreditCorrections, 'SHOPIFY/' + text(order && order.id)); }
function metadata(order, correction, ordersMap) {
  const original = ordersMap ? byId(ordersMap, correction.originalOrderId) : null;
  const originalFingerprintStale = !!ordersMap && (!original || fingerprint(original) !== correction.originalFingerprint);
  return { ...copy(correction), transactionIds:[...(correction.transactionIds || [])], stale:fingerprint(order) !== correction.sourceFingerprint || originalFingerprintStale, originalFingerprintStale };
}
function projectOrder(order, accountingStore, ordersMap) {
  const projected = copy(order);
  if (!order || !projected) return projected;
  const correction = correctionFor(order, accountingStore);
  if (!correction || correction.treatment !== TREATMENT) return projected;
  const meta = metadata(order, correction, ordersMap), ids = new Set((correction.transactionIds || []).map(String));
  projected.appStoreCreditCorrection = meta;
  projected.paymentTransactions = (projected.paymentTransactions || []).map(tx => {
    // Keep disputed cash excluded when source evidence changes. The stale
    // flag requires review; the immutable raw Shopify source remains intact.
    if (!successfulReceipt(tx) || !ids.has(text(tx.id)) && !isCash(tx.gateway || tx.payment_gateway)) return tx;
    return { ...tx, gateway:'store_credit', appStoreCreditCorrection:copy(meta) };
  });
  return projected;
}
function auditRows(accountingStore, ordersMap) {
  return Object.entries(accountingStore && accountingStore.saleStoreCreditCorrections || {}).filter(([, record]) => record && record.treatment === TREATMENT).map(([saleId, correction]) => {
    const order = byId(ordersMap, correction.orderId), meta = metadata(order, correction, ordersMap);
    return { id:'STORE-CREDIT/' + text(correction.orderId), saleId, kind:'sale_store_credit_correction', account:'Counter Cash', date:text(correction.date).slice(0, 10) || text(order && (order.processedAt || order.createdAt)).slice(0, 10), orderId:text(correction.orderId), orderNumber:text(correction.orderNumber), originalOrderId:text(correction.originalOrderId), originalOrderNumber:text(correction.originalOrderNumber), credit:0, debit:0, cashAmount:0, nonCashAmount:0, storeCreditAmount:money(correction.storeCreditAmount), gross:money(correction.gross), description:'Exchange paid using store credit · ' + text(correction.orderNumber) + ' · original ' + text(correction.originalOrderNumber), reason:text(correction.reason), walletStatus:correction.walletStatus || WALLET_PENDING, stale:meta.stale, appStoreCreditCorrection:meta };
  });
}

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
}
function equal(left, right) { return JSON.stringify(canonical(left)) === JSON.stringify(canonical(right)); }
function protectedRequests(store) { return (Array.isArray(store && store.requests) ? store.requests : []).filter(request => request && request.kind === 'sale_store_credit'); }
function protectedIds(store) {
  const ids = new Set(Object.keys(store && store.saleStoreCreditCorrections || {}));
  Object.values(store && store.saleStoreCreditCorrections || {}).forEach(record => {
    if (record && record.saleId) ids.add(text(record.saleId));
    if (record && record.approvalRequestId) ids.add(text(record.approvalRequestId));
  });
  protectedRequests(store).forEach(request => {
    if (request.id) ids.add(text(request.id));
    if (request.meta && request.meta.saleId) ids.add(text(request.meta.saleId));
  });
  return ids;
}
function protectedAudit(store, ids = protectedIds(store)) {
  return (Array.isArray(store && store.auditLog) ? store.auditLog : []).filter(event => event && (/^SALE_STORE_CREDIT/.test(text(event.action)) || ids.has(text(event.subjectId))));
}
// This revision concerns only the protected exchange records. Unrelated
// accounting changes are deliberately outside this source-preservation guard.
function sourceRevision(store) {
  return crypto.createHash('sha256').update(JSON.stringify(canonical({
    corrections:store && store.saleStoreCreditCorrections || {},
    requests:protectedRequests(store), audit:protectedAudit(store)
  }))).digest('hex');
}
function preservationFailure(part) {
  fail('This accounting save would remove or overwrite saved store-credit ' + part + '. Reload current accounting data; nothing was saved.', 409);
}
function requestIdentity(request, decision) {
  const ignored = new Set(['status', 'decidedBy', 'decidedAt']);
  if (decision === 'approved') ignored.add('correction');
  return Object.fromEntries(Object.entries(request).filter(([key]) => !ignored.has(key)));
}
function assertPreserved(next, persisted) {
  const before = persisted || {}, after = next || {};
  for (const [saleId, record] of Object.entries(before.saleStoreCreditCorrections || {})) {
    if (!own(after.saleStoreCreditCorrections, saleId) || !equal(record, own(after.saleStoreCreditCorrections, saleId))) preservationFailure('correction ' + saleId);
  }
  const nextRequests = Array.isArray(after.requests) ? after.requests : [];
  let previousRequestIndex = -1;
  for (const request of protectedRequests(before)) {
    const matches = nextRequests.map((row, index) => ({row,index})).filter(({row}) => row && text(row.id) === text(request.id));
    if (!request.id || matches.length !== 1 || matches[0].index < previousRequestIndex) preservationFailure('request history');
    previousRequestIndex = matches[0].index;
    const candidate = matches[0].row;
    if (equal(request, candidate)) continue;
    if (request.status !== 'pending' || !['approved','rejected'].includes(candidate.status) || !equal(requestIdentity(request, candidate.status), requestIdentity(candidate, candidate.status))) preservationFailure('request ' + request.id);
  }
  const nextAudit = Array.isArray(after.auditLog) ? after.auditLog : [], allBeforeAudit = Array.isArray(before.auditLog) ? before.auditLog : [];
  let previousAuditIndex = -1;
  for (const event of protectedAudit(before)) {
    const identity = text(event.id), previousIdentityCount = identity ? allBeforeAudit.filter(row => row && text(row.id) === identity).length : 0;
    const matches = nextAudit.map((row,index) => ({row,index})).filter(({row}) => row && (identity ? text(row.id) === identity : equal(event,row)));
    const retained = matches.find(({row,index}) => index > previousAuditIndex && equal(event,row));
    if (!retained || identity && matches.length !== previousIdentityCount) preservationFailure('audit history');
    previousAuditIndex = retained.index;
  }
  return true;
}

module.exports = { fingerprint, prepare, projectOrder, auditRows, assertPreserved, sourceRevision, protectedAuditSubjects:protectedIds };
