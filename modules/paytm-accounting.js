const { summarizePayouts } = require('./paytm-report');
const { validateOrderReceiptTotals } = require('./paytm-rounding');
const { validatePayoutReturnResolutions, resolutionForTransaction } = require('./paytm-refund-resolutions');

const CLEARING = 'Paytm Settlement Clearing';
const BANK = 'Axis Bank 3448';
const LINK_TOLERANCE_CENTS = 499;
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

// Bank evidence belongs to the whole settlement, regardless of Shopify links.
function matchPayoutBank(store, payout) {
  const rows = Object.values(((store.bankStatements || {})[BANK] || {}).transactions || {});
  const amountDate = rows.filter(row => cents(row.credit) === cents(payout.net) && cents(row.debit) === 0 && dayGap(row.date, payout.settledDate) <= 3);
  const exact = amountDate.filter(row => payout.utr && `${row.reference || ''} ${row.description || ''}`.includes(payout.utr));
  const candidates = exact.length ? exact : amountDate;
  // Amount alone is a suggestion; a unique UTR, amount and date confirms evidence.
  const matched = exact.length === 1;
  return { bankMatched: matched, bankMatch: matched ? 'Matched to Axis bank' : candidates.length > 1 ? 'ambiguous' : candidates.length ? 'amount/date candidate' : 'not found', bankCandidates: candidates.map(row => ({ id: row.id, date: row.date, credit: Number(row.credit), reference: row.reference || row.description || '' })) };
}

function assertNoStoreCreditExchange(store, orderId, orderNumber) {
  const saleId = 'SHOPIFY/' + String(orderId), label = '#' + String(orderNumber || orderId).replace(/^#/, '');
  if (Object.prototype.hasOwnProperty.call(store.saleStoreCreditCorrections || {}, saleId)) {
    throw new Error('Shopify order ' + label + ' has an approved store-credit exchange correction: cash and Paytm are zero. Review its source correction instead of linking or posting another Paytm receipt.');
  }
  if ((store.requests || []).some(request => request.kind === 'sale_store_credit' && request.status === 'pending' && request.meta && request.meta.saleId === saleId)) {
    throw new Error('Shopify order ' + label + ' has a store-credit exchange correction awaiting Owner approval. Resolve that request before linking or posting a Paytm receipt.');
  }
}

function validateOrderLink(store, tx, orderId, orders, saleRows, reason) {
  if (!tx) throw new Error('Paytm transaction not found. Import its detailed report first.');
  if (resolutionForTransaction(store, tx.transactionId)) throw new Error('This transaction belongs to an audited accidental receipt/refund return and cannot be linked to Shopify.');
  if (tx.isCustomerPayment === false) throw new Error('This is a Paytm adjustment, not a customer payment, and must not be linked to Shopify.');
  const orderKey = String(orderId).replace(/^#/, '').trim();
  const matches = orders.filter(x => String(x.id) === orderKey || String(x.orderNumber || x.number || x.name || '').replace(/\D/g, '').replace(/^0+/, '') === orderKey);
  if (matches.length !== 1) throw new Error(matches.length ? 'Order number is ambiguous; use the Shopify order ID.' : 'Shopify order not found.');
  const order = matches[0];
  if (!isOriginalPaymentOrder(order)) throw new Error('Choose a non-cancelled Shopify order with an original successful payment. Paid, partially paid, partially refunded and refunded orders are supported.');
  assertNoStoreCreditExchange(store, order.id, order.orderNumber || order.number || order.name);
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
  const remaining = available - otherLinked, receipt = cents(tx.amount);
  // Paytm may round the collected amount to whole rupees while Shopify keeps
  // paise. Permit a variance below ₹5 only while an unallocated component
  // remains; this must never turn a fully linked order into a duplicate link.
  if (receipt <= 0 || remaining <= 0 || receipt > remaining + LINK_TOLERANCE_CENTS) throw new Error('Shopify order is already linked or Paytm receipts exceed the amount after cash and store credit. Review the split first.');
  const note = String(order.note || ''), suffix = transactionSuffix(tx.transactionId) || transactionSuffix(tx.rrn);
  const containsId = text => noteHasTransactionSuffix(text, suffix);
  const hasId = containsId(note), uniqueId = hasId && orders.filter(candidate => containsId(candidate.note)).length === 1;
  const externalPaytm=verified.transactions&&!verified.paytmAmount;
  if (externalPaytm && !uniqueId && String(reason || '').trim().length < 10) throw new Error('Shopify does not identify a Paytm payment component for this order. Enter a review reason explaining the external Paytm collection.');
  if (!uniqueId && String(reason || '').trim().length < 10) throw new Error('The Paytm ID is missing or repeated in Shopify notes. Enter a review reason of at least 10 characters.');
  const refundedAmount = Math.max(Number(order.refundAmount || 0), Number(order.moneyRefunded || 0));
  return { transactionId: tx.transactionId, transactionSuffix: suffix, orderId: String(order.id), orderNumber: String(order.orderNumber || order.number || order.name || row && row.orderNumber || '').replace(/^#/, ''), amount: tx.amount, orderTotal: total / 100, partial: cents(tx.amount) < total, storeCreditExcluded: storeCredit / 100, subsequentlyRefunded: refundedAmount, refundStatus: String(order.financialStatus || '').toLowerCase(), matchBasis: uniqueId ? ((note.match(/\d{6,}/g)||[]).includes(String(tx.transactionId))||((note.match(/\d{6,}/g)||[]).includes(String(tx.rrn||'')))?'transaction_id_in_shopify_note':'transaction_id_suffix_in_shopify_note') : externalPaytm?'external_paytm_reviewed':'owner_reviewed', reason: uniqueId ? '' : String(reason).trim() };
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

function validateSettlementReview(store, payoutId, saleRows, options = {}) {
  const payout = getPayout(store, payoutId);
  if (!payout) throw new Error('Payout not found. Import the Paytm report first.');
  if (payout.transactionIds.some(id => (store.paytmRoundingReviews || {})[id] && (store.paytmExcludedTransactions || {})[id])) throw new Error('An excluded payment retains an audited rounding receipt. Reverse or correct that rounding treatment before posting.');
  const excludedIds = payout.transactionIds.filter(id => (store.paytmReportTransactions || {})[id]?.isCustomerPayment !== false && (store.paytmExcludedTransactions || {})[id]);
  const returns = validatePayoutReturnResolutions(store, payout);
  const unresolvedExcludedIds = excludedIds.filter(id => !returns.resolvedReceiptIds.includes(id));
  if (unresolvedExcludedIds.length && !options.allowExcluded) throw new Error('This payout contains an excluded payment (customer receipt). Restore or separately resolve it before finalizing the payout.');
  payout.returnedReceiptAmount = returns.returnedReceiptAmount;
  payout.effectiveCustomerGross = Math.round((payout.customerGross - returns.returnedReceiptAmount) * 100) / 100;
  if (!payout.utr || !payout.settledDate) throw new Error('Settlement UTR and settled date are required for an exact bank link.');
  const links = store.paytmOrderLinks || {}, manual = store.paytmManualResolutions || {};
  const customerIds = payout.transactionIds.filter(id => (store.paytmReportTransactions || {})[id]?.isCustomerPayment !== false && !excludedIds.includes(id));
  const linked = customerIds.map(id => links[id] || manual[id]);
  if (linked.some(x => !x)) throw new Error('Every Paytm payment in this payout must be linked to Shopify or classified as a verified manual sale/cash transfer before posting.');
  const excludedAmount=excludedIds.reduce((sum,id)=>sum+Number((store.paytmReportTransactions||{})[id]?.amount||0),0);
  if (linked.reduce((sum, x) => sum + cents(x.amount), 0) !== cents(payout.customerGross-excludedAmount)) throw new Error('Reviewed customer receipts plus excluded payments do not equal the customer-payment gross for this settlement.');
  const roundedOrders = new Set();
  for (const link of linked) {
    if (link.orderId) assertNoStoreCreditExchange(store, link.orderId, link.orderNumber);
    if (link.type === 'manual_sale' || link.type === 'cash_transfer') {
      const source = link.type === 'manual_sale' ? store.receipts : store.transfers;
      const record = (source || []).find(x => x.id === link.recordId && x.paytmTransactionId === link.transactionId);
      if (!record || cents(record.amount) !== cents(link.amount) || !record.proof) throw new Error(`Reviewed Paytm ${link.transactionId} record changed or lost its proof. Review it before posting.`);
      continue;
    }
    // Rounding is an explicit, evidence-bound accounting receipt, never a
    // general tolerance. Check the order's aggregate links across all payouts.
    if (Object.values(store.paytmRoundingReviews || {}).some(review => String(review.orderId) === String(link.orderId))) {
      if (!roundedOrders.has(String(link.orderId))) validateOrderReceiptTotals(store, link.orderId, saleRows);
      roundedOrders.add(String(link.orderId));
      continue;
    }
    const row = saleRows.find(x => String(x.orderId) === link.orderId && x.account !== 'Counter Cash');
    if (!row || cents(row.amount) < cents(link.amount)) throw new Error(`Shopify order ${link.orderNumber} changed since it was linked. Review it again.`);
    if ((store.bankDateOverrides || {})[row.id]) throw new Error(`Shopify order ${link.orderNumber} is already linked to a bank transaction. Correct that existing link before posting its Paytm payout.`);
  }
  if (cents(payout.gross) - cents(payout.commission) - cents(payout.platformFee) - cents(payout.gst) - cents(payout.nonCustomerAmount) - cents(payout.refundAmount) !== cents(payout.net)) throw new Error('Paytm customer gross, commission, platform fee, GST, VAS deductions, refunds and bank net do not balance.');
  return { payout, linked, excludedIds, excludedAmount, returnResolutions: returns.reviews };
}

// A settlement whose customer rows have all been reviewed is already a real
// Paytm outflow.  Keep the explicit Finalize action as the audit checkpoint,
// but do not hide that movement from the operational ledger while the user is
// waiting to link the matching Axis statement credit.
function reviewedUnpostedSettlements(store, saleRows) {
  const verified = new Set((store.paytmVerifiedSettlements || []).flatMap(x => [String(x.settlementId || ''), String(x.payoutId || '')]));
  const posted = new Set((store.paytmPayoutPostings || []).flatMap(x => [String(x.settlementId || ''), String(x.payoutId || '')]));
  const rows = [];
  for (const candidate of summarizePayouts(Object.values(store.paytmReportTransactions || {}))) {
    if (verified.has(String(candidate.settlementId || '')) || verified.has(String(candidate.payoutId || '')) || posted.has(String(candidate.settlementId || '')) || posted.has(String(candidate.payoutId || ''))) continue;
    try {
      const { payout, linked } = validateSettlementReview(store, candidate.payoutId, saleRows);
      let historicalEvidenceLink=null;
      if((store.paytmHistoricalEvidenceLinks||{})[payout.settlementId]) { validateHistoricalSettlementAttachment(store,payout.payoutId,saleRows); historicalEvidenceLink=store.paytmHistoricalEvidenceLinks[payout.settlementId]; }
      rows.push({ id: `PTMV-REVIEWED-${payout.settlementId}`, settlementId: payout.settlementId, payoutId: payout.payoutId, utr: payout.utr, settledDate: payout.settledDate, transactionIds: payout.transactionIds, orderIds: linked.map(x => x.orderId).filter(Boolean), orderNumbers: linked.map(x => x.orderNumber).filter(Boolean), gross: payout.effectiveCustomerGross, customerGross: payout.effectiveCustomerGross, rawCustomerGross: payout.customerGross, returnedReceiptAmount: payout.returnedReceiptAmount, refundAmount: payout.refundAmount, nonCustomerAmount: payout.nonCustomerAmount, commission: payout.commission, platformFee: payout.platformFee, gst: payout.gst, net: payout.net, historicalEvidenceLink, reviewedNotFinalized: !historicalEvidenceLink });
    } catch { /* Incomplete or unsafe batches stay out of the ledger. */ }
  }
  return rows;
}

function validatePayoutPosting(store, payoutId, bankTransactionId, saleRows) {
  const { payout, linked, excludedIds, excludedAmount } = validateSettlementReview(store, payoutId, saleRows, { allowExcluded: true });
  if ((store.paytmPayoutPostings || []).some(x => x.payoutId === payout.payoutId || payout.sourcePayoutIds.includes(x.payoutId) || x.settlementId === payout.settlementId || (x.transactionIds || []).some(id => payout.transactionIds.includes(id)))) throw new Error('This Paytm settlement was already posted.');
  if ((store.paytmSettlements || []).some(x => x.payoutId === payoutId || x.bankTransactionId === bankTransactionId)) throw new Error('An existing Paytm settlement already uses this payout or bank transaction.');
  const bank = Object.values(((store.bankStatements || {})[BANK] || {}).transactions || {}).find(x => x.id === bankTransactionId);
  if (!bank || cents(bank.credit) !== cents(payout.net) || cents(bank.debit) !== 0) throw new Error('Choose an Axis 3448 credit equal to the exact Paytm payout net.');
  if (!String(bank.reference || bank.description || '').includes(payout.utr)) throw new Error('The Axis bank reference must contain the Paytm payout UTR.');
  if (excludedIds.length && bank.date !== payout.settledDate) throw new Error('A payout containing excluded payments can be linked only when the Axis credit is on the exact Paytm settled date.');
  if (dayGap(bank.date, payout.settledDate) > 3) throw new Error('Axis credit and Paytm settled dates differ by more than three days.');
  if ((store.paytmPayoutPostings || []).some(x => x.bankTransactionId === bankTransactionId)) throw new Error('This Axis bank credit is already used by a Paytm payout.');
  if (Object.values(store.bankDateOverrides || {}).some(x => x.bankTransactionId === bankTransactionId)) throw new Error('This Axis credit is already linked to another ledger entry. Correct that link first.');
  const book = (store.bankStatements || {})[BANK] || {};
  if ((book.imports || []).some(record => [].concat(record.reconciliationRows || [], record.carriedReconciliationRows || []).some(row => row.bank && (row.bank.id === bankTransactionId || sameSettlementCredit(row.bank,bank,payout.utr)) && (row.linkedRecordIds || []).length))) throw new Error('This Axis credit was already linked in a finalized reconciliation. Correct that link first.');
  if ([...(store.adjustments || []), ...(store.receipts || []), ...(store.bankTruthMovements || [])].some(x => x.bankTransactionId === bankTransactionId)) throw new Error('This Axis credit already has a ledger posting. Review it before posting Paytm.');
  return { payout, bank, linked, excludedIds, excludedAmount };
}
function sameSettlementCredit(a,b,utr) {
  return !!utr&&a.date===b.date&&cents(a.credit)===cents(b.credit)&&cents(a.debit)===0&&cents(b.debit)===0&&
    `${a.reference||''} ${a.description||''}`.includes(utr)&&`${b.reference||''} ${b.description||''}`.includes(utr);
}

// Attach detailed report evidence to an ALREADY finalized historical posting.
// This is not permission to post over a duplicate bank credit: every original
// amount, fee, UTR, date and finalized ledger claim must agree exactly.
function validateHistoricalSettlementAttachment(store, payoutId, saleRows) {
  const {payout,linked}=validateSettlementReview(store,payoutId,saleRows);
  if ((store.paytmPayoutPostings||[]).some(x=>x.settlementId===payout.settlementId||(x.transactionIds||[]).some(id=>payout.transactionIds.includes(id)))) throw new Error('This detailed settlement already has a payout posting.');
  const match=matchPayoutBank(store,payout);
  if(!match.bankMatched||match.bankCandidates.length!==1)throw new Error('A unique exact Axis UTR and net credit is required to attach historical evidence.');
  const bank=match.bankCandidates[0];
  if(bank.date!==payout.settledDate)throw new Error('Historical bank evidence must be on the exact settled date.');
  const fees=cents(payout.commission)+cents(payout.platformFee)+cents(payout.gst)+cents(payout.nonCustomerAmount);
  const candidates=(store.paytmSettlements||[]).filter(x=>x.bankAccount===BANK&&String(x.date).slice(0,10)===payout.settledDate&&
    String(x.utr||x.bankReference||x.reference||'')===payout.utr&&cents(x.netAmount)===cents(payout.net)&&
    ((cents(x.grossAmount)===cents(payout.effectiveCustomerGross)&&cents(x.chargeAmount)===fees)||pendingHistoricalDetail(x)));
  if(candidates.length!==1)throw new Error('The existing historical settlement must match the exact customer gross, charges, net, date and UTR.');
  const legacy=candidates[0],book=(store.bankStatements||{})[BANK]||{};
  const completePendingDetail=pendingHistoricalDetail(legacy);
  if(completePendingDetail&&(store.reconciliationExpenses||[]).some(x=>x.settlementId===legacy.id||x.paytmHistoricalSettlementId===legacy.id))throw new Error('This pending settlement already has a charge record. Review it before completing the report detail.');
  const finalized=(book.imports||[]).flatMap(record=>[].concat(record.reconciliationRows||[],record.carriedReconciliationRows||[])
    .filter(row=>row.bank&&(row.bank.id===bank.id||sameSettlementCredit(row.bank,bank,payout.utr))&&(row.linkedRecordIds||[]).length)
    .map(row=>({record,row})));
  if(!finalized.length||finalized.some(({record,row})=>!record.finalizedAt||!record.finalizedBy||
    (row.linkedRecordIds||[]).length!==1||row.linkedRecordIds[0]!==legacy.id||
    cents(row.bank.credit)!==cents(payout.net)||cents(row.bank.debit)!==0||row.bank.date!==bank.date||
    !String(row.bank.reference||row.bank.description||'').includes(payout.utr)))throw new Error('The exact bank row must already be finalized against this historical settlement only.');
  if(Object.entries(store.bankDateOverrides||{}).some(([id,x])=>x.bankTransactionId===bank.id&&id!==legacy.id)||
    [...(store.adjustments||[]),...(store.receipts||[]),...(store.bankTruthMovements||[])].some(x=>x.bankTransactionId===bank.id))throw new Error('Another accounting record already claims this bank credit.');
  if(Object.values(store.paytmHistoricalEvidenceLinks||{}).some(x=>x.settlementId!==payout.settlementId&&(x.bankTransactionId===bank.id||x.historicalSettlementId===legacy.id)))throw new Error('This historical evidence is already attached to another detailed settlement.');
  return {payout,linked,bank,historicalSettlementId:legacy.id,reconciliationImportId:finalized[0].record.id,completePendingDetail};
}
function pendingHistoricalDetail(x) {
  return x.bankTruthOnly===true&&x.source==='axis_3448_paytm_bank_narration'&&x.reportDetailCompleted!==true&&
    cents(x.grossAmount)===cents(x.netAmount)&&cents(x.chargeAmount)===0&&cents(x.paytmChargeAmount)===0&&cents(x.hiddenChargeAmount)===0&&
    !(x.orderIds||[]).length&&!(x.transferIds||[]).length&&!(x.otherReceipts||[]).length;
}

// Status is a read-only projection of the very same validators used to post.
// It deliberately has no user/role argument and creates no ledger movements.
function settlementStatus(store, payout, saleRows) {
  const posted = (store.paytmPayoutPostings || []).find(x => x.settlementId === payout.settlementId || x.payoutId === payout.payoutId);
  const verified = (store.paytmVerifiedSettlements || []).find(x => x.settlementId === payout.settlementId);
  const bank = matchPayoutBank(store, payout), reasons = [], bankReasons = [];
  const attachment=(store.paytmHistoricalEvidenceLinks||{})[payout.settlementId];
  let historicalCandidate=null,attachmentValid=false;
  if(attachment){
    try {const evidence=validateHistoricalSettlementAttachment(store,payout.payoutId,saleRows);
      if(attachment.historicalSettlementId!==evidence.historicalSettlementId||attachment.bankTransactionId!==evidence.bank.id||attachment.reconciliationImportId!==evidence.reconciliationImportId)throw new Error('Saved historical evidence changed. Review the attachment again.');
      attachmentValid=true;
    }catch(error){reasons.push(error.message);}
  }
  let code = posted || attachmentValid ? 'posted' : attachment ? 'incomplete' : verified ? 'finalized' : 'ready';
  const hasReturnReview = Object.values(store.paytmReturnResolutions || {}).some(review => review.settlementId === payout.settlementId || payout.transactionIds.includes(review.receiptTransactionId) || payout.transactionIds.includes(review.refundTransactionId));
  if ((posted || verified) && (payout.refundAmount > 0 || hasReturnReview)) {
    try {
      const reviewed = validateSettlementReview(store, payout.payoutId, saleRows).payout, existing = posted || verified;
      if (cents(existing.returnedReceiptAmount) !== cents(reviewed.returnedReceiptAmount) || cents(existing.refundAmount) !== cents(reviewed.refundAmount) || cents(existing.customerGross) !== cents(reviewed.effectiveCustomerGross) || cents(existing.nonCustomerAmount) !== cents(reviewed.nonCustomerAmount)) throw new Error('This previously finalized settlement contains a REFUND recorded without the current return treatment. Review the existing accounting; no historical posting was changed.');
    } catch (error) { code = 'incomplete'; reasons.push(error.message); }
  }
  if (!posted && !verified) {
    try { validateSettlementReview(store, payout.payoutId, saleRows); }
    catch (error) { code = 'incomplete'; reasons.push(error.message); }
  }
  let bankLinkReady = false;
  if (!posted&&!attachmentValid) {
    if (!bank.bankMatched) bankReasons.push(bank.bankMatch === 'ambiguous' ?
      'More than one possible Axis 3448 credit exists. A unique UTR, amount and settled-date match is required.' : bank.bankMatch === 'amount/date candidate' ?
      'An amount/date candidate exists, but its bank reference does not contain the Paytm settlement UTR.' :
      'No Axis 3448 credit matches this settlement UTR and exact net amount in the available bank evidence.');
    else {
      try { validatePayoutPosting(store, payout.payoutId, bank.bankCandidates[0].id, saleRows); bankLinkReady = true; }
      catch (error) { bankReasons.push(error.message); }
    }
    if(code!=='incomplete')try{const evidence=validateHistoricalSettlementAttachment(store,payout.payoutId,saleRows);historicalCandidate={completePendingDetail:evidence.completePendingDetail,fees:Math.round((payout.commission+payout.platformFee+payout.gst+payout.nonCustomerAmount)*100)/100,historicalSettlementId:evidence.historicalSettlementId,bankTransactionId:evidence.bank.id,reconciliationImportId:evidence.reconciliationImportId};}catch{/* A genuinely conflicting claim remains blocked by the posting validator. */}
  }
  const blockedTransactions = payout.transactionIds.filter(id => {
    const tx = (store.paytmReportTransactions || {})[id];
    const returnReview = resolutionForTransaction(store, id);
    if (returnReview) { try { validatePayoutReturnResolutions(store, payout); return false; } catch { return true; } }
    return tx && tx.isCustomerPayment !== false && ((store.paytmExcludedTransactions || {})[id] ||
      !((store.paytmOrderLinks || {})[id] || (store.paytmManualResolutions || {})[id]));
  }).map(id => ({ transactionId: id, reason: (store.paytmExcludedTransactions || {})[id] ?
    'Excluded from Shopify: ' + (store.paytmExcludedTransactions[id].reason || 'Excluded payment') : 'No confirmed Shopify link or verified manual sale/cash-transfer classification.' }));
  return { code, label: { posted: 'Linked and posted to Axis 3448', finalized: 'Paytm review finalized — bank link pending',
    ready: 'Ready for Paytm finalization', incomplete: 'Incomplete — review required' }[code], reasons, bankReasons, bankLinkReady,
    payoutId: payout.payoutId, settlementId: payout.settlementId, utr: payout.utr, settledDate: payout.settledDate, net: payout.net,
    blockedTransactions,historicalCandidate,historicalEvidenceLink:attachmentValid?attachment:null, finalizedBy: verified && verified.finalizedBy || posted && posted.postedBy || attachmentValid&&attachment.by || '',
    finalizedAt: verified && verified.finalizedAt || posted && posted.postedAt || attachmentValid&&attachment.at || '' };
}
function reconciliationStatus(store, saleRows) {
  const settlements = summarizePayouts(Object.values(store.paytmReportTransactions || {})).map(p => settlementStatus(store, p, saleRows));
  const counts = { total: settlements.length, posted: 0, finalized: 0, ready: 0, incomplete: 0, bankLinkPending: 0 };
  settlements.forEach(s => { counts[s.code]++; if (s.code !== 'posted') counts.bankLinkPending++; });
  return { counts, settlements, incompleteSettlements: settlements.filter(s => s.code === 'incomplete'),
    lastAttempt: store.paytmLastFinalizationAttempt || null };
}
module.exports = { CLEARING, BANK, LINK_TOLERANCE_CENTS, getPayout, matchPayoutBank, summarizeShopifyPayments, transactionSuffix, noteHasTransactionSuffix, isOriginalPaymentOrder, autoMatchShopifyNotes, validateOrderLink, validateSettlementReview, reviewedUnpostedSettlements, validatePayoutPosting, validateHistoricalSettlementAttachment, settlementStatus, reconciliationStatus };
