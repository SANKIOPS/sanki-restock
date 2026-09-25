const { summarizePayouts } = require('./paytm-report');

const CLEARING = 'Paytm Settlement Clearing';
const BANK = 'Axis Bank 3448';
const cents = value => Math.round(Number(value || 0) * 100);
const dayGap = (a, b) => Math.abs((Date.parse(`${a}T00:00:00Z`) - Date.parse(`${b}T00:00:00Z`)) / 86400000);
function transactionSuffix(id) { const digits = String(id || '').replace(/\D/g, ''); return digits.length >= 6 ? digits.slice(-6) : ''; }
function noteHasTransactionSuffix(note, suffix) { return !!suffix && (String(note || '').match(/\d{6,}/g) || []).some(token => token.endsWith(suffix)); }
function isOriginalPaymentOrder(order) {
  if(!order||order.cancelledAt)return false;
  if(['paid','partially_paid','partially_refunded','refunded'].includes(String(order.financialStatus||'').toLowerCase()))return true;
  return (order.paymentTransactions||[]).some(tx=>['sale','capture'].includes(String(tx.kind||'').toLowerCase())&&String(tx.status||'').toLowerCase()==='success'&&Number(tx.amount||0)>0);
}

function summarizeShopifyPayments(transactions) {
  const result = { paytmAmount: 0, cashAmount: 0, storeCreditAmount: 0, otherAmount: 0, transactions: [] };
  for (const tx of transactions || []) {
    if (!['sale', 'capture'].includes(String(tx.kind || '').toLowerCase()) || String(tx.status || '').toLowerCase() !== 'success') continue;
    const amount = Number(tx.amount || 0), gateway = String(tx.gateway || tx.payment_gateway || '').toLowerCase();
    if (!(amount > 0)) continue;
    const type = /store.?credit|gift.?card/.test(gateway) ? 'storeCreditAmount' : /paytm/.test(gateway) ? 'paytmAmount' : /cash/.test(gateway) ? 'cashAmount' : 'otherAmount';
    result[type] = Math.round((result[type] + amount) * 100) / 100;
    result.transactions.push({ id: String(tx.id || ''), kind: tx.kind, gateway: tx.gateway || tx.payment_gateway || '', amount, processedAt: tx.processed_at || '' });
  }
  return result;
}

function getPayout(store, payoutId) {
  return summarizePayouts(Object.values(store.paytmReportTransactions || {})).find(p => p.payoutId === payoutId || p.utr === payoutId || p.sourcePayoutIds.includes(payoutId));
}

function validateOrderLink(store, tx, orderId, orders, saleRows, reason) {
  if (!tx) throw new Error('Paytm transaction not found. Import its detailed report first.');
  if (tx.isCustomerPayment === false) throw new Error('This is a Paytm adjustment, not a customer payment, and must not be linked to Shopify.');
  const orderKey = String(orderId).replace(/^#/, '').trim();
  const matches = orders.filter(x => String(x.id) === orderKey || String(x.orderNumber || x.number || x.name || '').replace(/\D/g, '').replace(/^0+/, '') === orderKey);
  if (matches.length !== 1) throw new Error(matches.length ? 'Order number is ambiguous; use the Shopify order ID.' : 'Shopify order not found.');
  const order = matches[0];
  if (!isOriginalPaymentOrder(order)) throw new Error('Choose a non-cancelled Shopify order with an original successful payment. Paid, partially paid, partially refunded and refunded orders are supported.');
  const links = store.paytmOrderLinks || {};
  const row = saleRows.find(x => String(x.orderId) === String(order.id) && x.account !== 'Counter Cash');
  const orderDate = String(order.processedAt || order.createdAt || row && row.date || '').slice(0, 10);
  if (!orderDate || dayGap(orderDate, tx.date) > 1) throw new Error('Shopify and Paytm dates differ by more than one day. Review the source records.');
  // A later Shopify refund to store credit is a customer liability, not a
  // reversal of the Paytm money originally collected. Match against the
  // original order/payment components; actual Paytm refunds are separate
  // Paytm transactions and must be reconciled separately.
  const total = cents(Number(order.total || 0)) || cents(row && row.gross || row && row.amount);
  const verified = (store.paytmShopifyPayments || {})[order.id] || {};
  const cash = cents((store.saleAllocationOverrides || {})['SHOPIFY/' + order.id]?.cashAmount ?? verified.cashAmount ?? order.cashAmount ?? 0);
  const storeCredit = cents(verified.storeCreditAmount ?? order.storeCreditAmount ?? order.storeCreditUsed ?? 0);
  const available = Math.min(total - cash - storeCredit, verified.paytmAmount > 0 ? cents(verified.paytmAmount) : total);
  const otherLinked = Object.entries(links).filter(([id, link]) => id !== tx.transactionId && String(link.orderId) === String(order.id)).reduce((sum, [, link]) => sum + cents(link.amount), 0);
  if (cents(tx.amount) <= 0 || cents(tx.amount) + otherLinked > available) throw new Error('Shopify order is already linked or Paytm receipts exceed the amount after cash and store credit. Review the split first.');
  const note = String(order.note || ''), suffix = transactionSuffix(tx.transactionId) || transactionSuffix(tx.rrn);
  const containsId = text => noteHasTransactionSuffix(text, suffix);
  const hasId = containsId(note), uniqueId = hasId && orders.filter(candidate => containsId(candidate.note)).length === 1;
  if (verified.transactions && !verified.paytmAmount && String(reason || '').trim().length < 10) throw new Error('Shopify does not identify a Paytm payment component for this order. Enter a review reason explaining the external Paytm collection.');
  if (!uniqueId && String(reason || '').trim().length < 10) throw new Error('The Paytm ID is missing or repeated in Shopify notes. Enter a review reason of at least 10 characters.');
  const refundedAmount = Math.max(Number(order.refundAmount || 0), Number(order.moneyRefunded || 0));
  return { transactionId: tx.transactionId, transactionSuffix: suffix, orderId: String(order.id), orderNumber: String(order.orderNumber || order.number || order.name || row && row.orderNumber || '').replace(/^#/, ''), amount: tx.amount, orderTotal: total / 100, partial: cents(tx.amount) < total, storeCreditExcluded: storeCredit / 100, subsequentlyRefunded: refundedAmount, refundStatus: String(order.financialStatus || '').toLowerCase(), matchBasis: verified.transactions&&!verified.paytmAmount?'external_paytm_reviewed':uniqueId ? ((note.match(/\d{6,}/g)||[]).includes(String(tx.transactionId))||((note.match(/\d{6,}/g)||[]).includes(String(tx.rrn||'')))?'transaction_id_in_shopify_note':'transaction_id_suffix_in_shopify_note') : 'owner_reviewed', reason: uniqueId&&!(verified.transactions&&!verified.paytmAmount) ? '' : String(reason).trim() };
}

function autoMatchShopifyNotes(store, transactions, orders, saleRows) {
  store.paytmOrderLinks = store.paytmOrderLinks || {};
  const all = Object.values(transactions || {}), candidates = new Map(), suffixCounts = new Map();
  all.forEach(tx => { const suffix = transactionSuffix(tx.transactionId)||transactionSuffix(tx.rrn); if(suffix)suffixCounts.set(suffix,(suffixCounts.get(suffix)||0)+1); });
  for (const tx of all) {
    if (tx.isCustomerPayment === false) continue;
    if (store.paytmOrderLinks[tx.transactionId] || (store.paytmManualResolutions||{})[tx.transactionId] || (store.paytmExcludedTransactions||{})[tx.transactionId]) continue;
    const suffix = transactionSuffix(tx.transactionId)||transactionSuffix(tx.rrn);
    if (!suffix || suffixCounts.get(suffix) !== 1) continue;
    const matches = orders.filter(order => noteHasTransactionSuffix(order.note,suffix));
    if (matches.length === 1) candidates.set(tx.transactionId,matches[0]);
  }
  const matched = [];
  for (const tx of all) {
    const order = candidates.get(tx.transactionId);
    if (!order) continue;
    try {
      const link = validateOrderLink(store,tx,String(order.id),orders,saleRows,'');
      if (!['transaction_id_suffix_in_shopify_note','transaction_id_in_shopify_note'].includes(link.matchBasis)) continue;
      store.paytmOrderLinks[tx.transactionId] = { ...link, by:'automatic', at:new Date().toISOString() };
      matched.push(link);
    } catch { /* Amount, date, split or duplicate is not safe: leave for review. */ }
  }
  return matched;
}

function validateSettlementReview(store, payoutId, saleRows) {
  const payout = getPayout(store, payoutId);
  if (!payout) throw new Error('Payout not found. Import the Paytm report first.');
  if (payout.transactionIds.some(id => (store.paytmReportTransactions || {})[id]?.isCustomerPayment !== false && (store.paytmExcludedTransactions || {})[id])) throw new Error('This payout contains an excluded payment (customer receipt). Restore or separately resolve it before posting the payout.');
  if (!payout.utr || !payout.settledDate) throw new Error('Settlement UTR and settled date are required for an exact bank link.');
  const links = store.paytmOrderLinks || {}, manual = store.paytmManualResolutions || {};
  const customerIds = payout.transactionIds.filter(id => (store.paytmReportTransactions || {})[id]?.isCustomerPayment !== false);
  const linked = customerIds.map(id => links[id] || manual[id]);
  if (linked.some(x => !x)) throw new Error('Every Paytm payment in this payout must be linked to Shopify or classified as a verified manual sale/cash transfer before posting.');
  if (linked.reduce((sum, x) => sum + cents(x.amount), 0) !== cents(payout.customerGross)) throw new Error('Reviewed customer receipts do not equal the customer-payment gross for this settlement.');
  for (const link of linked) {
    if (link.type === 'manual_sale' || link.type === 'cash_transfer') {
      const source = link.type === 'manual_sale' ? store.receipts : store.transfers;
      const record = (source || []).find(x => x.id === link.recordId && x.paytmTransactionId === link.transactionId);
      if (!record || cents(record.amount) !== cents(link.amount) || !record.proof) throw new Error(`Reviewed Paytm ${link.transactionId} record changed or lost its proof. Review it before posting.`);
      continue;
    }
    const row = saleRows.find(x => String(x.orderId) === link.orderId && x.account !== 'Counter Cash');
    if (!row || cents(row.amount) < cents(link.amount)) throw new Error(`Shopify order ${link.orderNumber} changed since it was linked. Review it again.`);
    if ((store.bankDateOverrides || {})[row.id]) throw new Error(`Shopify order ${link.orderNumber} is already linked to a bank transaction. Correct that existing link before posting its Paytm payout.`);
  }
  if (cents(payout.gross) - cents(payout.commission) - cents(payout.platformFee) - cents(payout.gst) - cents(payout.nonCustomerAmount) !== cents(payout.net)) throw new Error('Paytm customer gross, commission, platform fee, GST, VAS deductions and bank net do not balance.');
  return { payout, linked };
}

function validatePayoutPosting(store, payoutId, bankTransactionId, saleRows) {
  const { payout, linked } = validateSettlementReview(store, payoutId, saleRows);
  if ((store.paytmPayoutPostings || []).some(x => x.payoutId === payout.payoutId || payout.sourcePayoutIds.includes(x.payoutId) || x.settlementId === payout.settlementId || (x.transactionIds || []).some(id => payout.transactionIds.includes(id)))) throw new Error('This Paytm settlement was already posted.');
  if ((store.paytmSettlements || []).some(x => x.payoutId === payoutId || x.bankTransactionId === bankTransactionId)) throw new Error('An existing Paytm settlement already uses this payout or bank transaction.');
  const bank = Object.values(((store.bankStatements || {})[BANK] || {}).transactions || {}).find(x => x.id === bankTransactionId);
  if (!bank || cents(bank.credit) !== cents(payout.net) || cents(bank.debit) !== 0) throw new Error('Choose an Axis 3448 credit equal to the exact Paytm payout net.');
  if (!String(bank.reference || bank.description || '').includes(payout.utr)) throw new Error('The Axis bank reference must contain the Paytm payout UTR.');
  if (dayGap(bank.date, payout.settledDate) > 3) throw new Error('Axis credit and Paytm settled dates differ by more than three days.');
  if ((store.paytmPayoutPostings || []).some(x => x.bankTransactionId === bankTransactionId)) throw new Error('This Axis bank credit is already used by a Paytm payout.');
  if (Object.values(store.bankDateOverrides || {}).some(x => x.bankTransactionId === bankTransactionId)) throw new Error('This Axis credit is already linked to another ledger entry. Correct that link first.');
  const book = (store.bankStatements || {})[BANK] || {};
  if ((book.imports || []).some(record => [].concat(record.reconciliationRows || [], record.carriedReconciliationRows || []).some(row => row.bank && row.bank.id === bankTransactionId && (row.linkedRecordIds || []).length))) throw new Error('This Axis credit was already linked in a finalized reconciliation. Correct that link first.');
  if ([...(store.adjustments || []), ...(store.receipts || []), ...(store.bankTruthMovements || [])].some(x => x.bankTransactionId === bankTransactionId)) throw new Error('This Axis credit already has a ledger posting. Review it before posting Paytm.');
  return { payout, bank, linked };
}

module.exports = { CLEARING, BANK, getPayout, summarizeShopifyPayments, transactionSuffix, noteHasTransactionSuffix, isOriginalPaymentOrder, autoMatchShopifyNotes, validateOrderLink, validateSettlementReview, validatePayoutPosting };
