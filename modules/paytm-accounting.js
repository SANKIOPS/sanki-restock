const { summarizePayouts } = require('./paytm-report');

const CLEARING = 'Paytm Settlement Clearing';
const BANK = 'Axis Bank 3448';
const cents = value => Math.round(Number(value || 0) * 100);
const dayGap = (a, b) => Math.abs((Date.parse(`${a}T00:00:00Z`) - Date.parse(`${b}T00:00:00Z`)) / 86400000);

function getPayout(store, payoutId) {
  return summarizePayouts(Object.values(store.paytmReportTransactions || {})).find(p => p.payoutId === payoutId);
}

function validateOrderLink(store, tx, orderId, orders, saleRows, reason) {
  if (!tx) throw new Error('Paytm transaction not found. Import its detailed report first.');
  if (tx.posId === 'DEFAULT') throw new Error('This payment is not marked as a POS transaction. Review its channel separately.');
  const orderKey = String(orderId).replace(/^#/, '').trim();
  const matches = orders.filter(x => String(x.id) === orderKey || String(x.orderNumber || x.number || x.name || '').replace(/\D/g, '').replace(/^0+/, '') === orderKey);
  if (matches.length !== 1) throw new Error(matches.length ? 'Order number is ambiguous; use the Shopify order ID.' : 'Shopify order not found.');
  const order = matches[0];
  if (!order || order.cancelledAt || String(order.financialStatus || '').toLowerCase() !== 'paid') throw new Error('Choose a paid, non-cancelled Shopify order.');
  const row = order && saleRows.find(x => String(x.orderId) === String(order.id) && x.account !== 'Counter Cash');
  if (!row || cents(row.amount) !== cents(tx.amount)) throw new Error('The Shopify non-cash sale must equal the Paytm gross amount exactly. Review split cash or refunds first.');
  if (dayGap(row.date, tx.date) > 1) throw new Error('Shopify and Paytm dates differ by more than one day. Review the source records.');
  const links = store.paytmOrderLinks || {};
  if (Object.entries(links).some(([id, link]) => id !== tx.transactionId && String(link.orderId) === String(order.id))) throw new Error('This Shopify order is already linked to another Paytm payment.');
  const hasId = String(order.note || '').includes(tx.transactionId);
  if (!hasId && String(reason || '').trim().length < 10) throw new Error('Add the Paytm transaction ID to the Shopify order note, or enter a review reason of at least 10 characters.');
  return { transactionId: tx.transactionId, orderId: String(order.id), orderNumber: row.orderNumber, amount: tx.amount, matchBasis: hasId ? 'transaction_id_in_shopify_note' : 'owner_reviewed', reason: hasId ? '' : String(reason).trim() };
}

function validatePayoutPosting(store, payoutId, bankTransactionId, saleRows) {
  const payout = getPayout(store, payoutId);
  if (!payout) throw new Error('Payout not found. Import the Paytm report first.');
  if (payout.transactionIds.some(id => (store.paytmExcludedTransactions || {})[id])) throw new Error('This payout contains an excluded payment. Restore or separately resolve that payment before posting the payout.');
  if (!payout.utr || !payout.payoutDate) throw new Error('Payout UTR and date are required for an exact bank link.');
  if ((store.paytmPayoutPostings || []).some(x => x.payoutId === payoutId)) throw new Error('This Paytm payout was already posted.');
  if ((store.paytmSettlements || []).some(x => x.payoutId === payoutId || x.bankTransactionId === bankTransactionId)) throw new Error('An existing Paytm settlement already uses this payout or bank transaction.');
  const links = store.paytmOrderLinks || {};
  const linked = payout.transactionIds.map(id => links[id]);
  if (linked.some(x => !x)) throw new Error('Every Paytm payment in this payout must be linked to a Shopify order before posting.');
  if (new Set(linked.map(x => x.orderId)).size !== linked.length) throw new Error('A Shopify order cannot be used twice in one payout.');
  if (linked.reduce((sum, x) => sum + cents(x.amount), 0) !== cents(payout.gross)) throw new Error('Linked Shopify sales do not equal the payout gross.');
  for (const link of linked) {
    const row = saleRows.find(x => String(x.orderId) === link.orderId && x.account !== 'Counter Cash');
    if (!row || cents(row.amount) !== cents(link.amount)) throw new Error(`Shopify order ${link.orderNumber} changed since it was linked. Review it again.`);
    if ((store.bankDateOverrides || {})[row.id]) throw new Error(`Shopify order ${link.orderNumber} is already linked to a bank transaction. Correct that existing link before posting its Paytm payout.`);
  }
  if (cents(payout.gross) - cents(payout.commission) - cents(payout.gst) !== cents(payout.net)) throw new Error('Paytm gross, commission, GST and net do not balance.');
  const bank = Object.values(((store.bankStatements || {})[BANK] || {}).transactions || {}).find(x => x.id === bankTransactionId);
  if (!bank || cents(bank.credit) !== cents(payout.net) || cents(bank.debit) !== 0) throw new Error('Choose an Axis 3448 credit equal to the exact Paytm payout net.');
  if (!String(bank.reference || bank.description || '').includes(payout.utr)) throw new Error('The Axis bank reference must contain the Paytm payout UTR.');
  if (dayGap(bank.date, payout.payoutDate) > 3) throw new Error('Axis credit and Paytm payout dates differ by more than three days.');
  if ((store.paytmPayoutPostings || []).some(x => x.bankTransactionId === bankTransactionId)) throw new Error('This Axis bank credit is already used by a Paytm payout.');
  if (Object.values(store.bankDateOverrides || {}).some(x => x.bankTransactionId === bankTransactionId)) throw new Error('This Axis credit is already linked to another ledger entry. Correct that link first.');
  const book = (store.bankStatements || {})[BANK] || {};
  if ((book.imports || []).some(record => [].concat(record.reconciliationRows || [], record.carriedReconciliationRows || []).some(row => row.bank && row.bank.id === bankTransactionId && (row.linkedRecordIds || []).length))) throw new Error('This Axis credit was already linked in a finalized reconciliation. Correct that link first.');
  if ([...(store.adjustments || []), ...(store.receipts || []), ...(store.bankTruthMovements || [])].some(x => x.bankTransactionId === bankTransactionId)) throw new Error('This Axis credit already has a ledger posting. Review it before posting Paytm.');
  return { payout, bank, linked };
}

module.exports = { CLEARING, BANK, getPayout, validateOrderLink, validatePayoutPosting };
