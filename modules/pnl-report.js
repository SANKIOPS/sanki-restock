'use strict';

// Read-only management report. COGS follows the sold SKU; all other costs
// follow actual approved payments. Never write to accounting/order stores.
const purchaseCosts = require('../public/purchase-costs');
const START = '2026-08-22';
const number = value => Number.isFinite(Number(value)) ? Number(value) : 0;
const money = value => Math.round((number(value) + Number.EPSILON) * 100) / 100;
const present = value => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value));
const skuKey = value => String(value || '').trim().toUpperCase();
const list = value => Array.isArray(value) ? value : Object.values(value || {});
const business = value => ['SANKI', 'A3'].includes(String(value || '').toUpperCase());
function validDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value)) && !Number.isNaN(Date.parse(value + 'T00:00:00Z')) && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value;
}
function day(value) {
  if (!value) return '';
  const text = String(value);
  if (validDate(text)) return text;
  const timestamp = Date.parse(text);
  return Number.isFinite(timestamp) ? new Date(timestamp + 19800000).toISOString().slice(0, 10) : '';
}
function channel(value) {
  const text = String(value || '').toLowerCase();
  return /pos|walk.in/.test(text) ? 'POS' : /website|online|web/.test(text) ? 'Website' : 'Other';
}
const gstRate = unitValue => number(unitValue) <= 2500 ? 0.05 : 0.18;

// The slab is tested on the per-piece taxable sale value, not an order's
// advance, quantity total or tax-inclusive MRP. Ambiguous inclusive amounts
// must be reviewed rather than silently put into a convenient slab.
function lineTax(line, taxesIncluded = true) {
  const qty = number(line.qty), discount = number(line.discount);
  const base = Math.max(0, number(line.price ?? line.unitPrice) * qty - discount);
  const recorded = Array.isArray(line.taxLines) && line.taxLines.length ? money(line.taxLines.reduce((sum, tax) => sum + number(tax.price), 0)) : null;
  if (line.taxable === false) return { gross: money(base), tax: 0, rate: 0, recorded, status: 'Exempt', valid: true };
  if (!(qty > 0)) return { gross: money(base), tax: null, rate: null, recorded, status: 'Missing quantity', valid: false };
  const sourceRates = [...new Set((line.taxLines || []).map(t => number(t.rate)))];
  // CGST + SGST are combined; IGST already contains the entire rate.
  const invoiceRate = sourceRates.length ? (line.taxLines || []).reduce((sum, t) => sum + number(t.rate), 0) : null;
  let rate;
  if (!taxesIncluded) rate = gstRate(base / qty);
  else if (present(line.unitTaxableValue)) rate = gstRate(line.unitTaxableValue);
  else if (invoiceRate > 0) rate = gstRate((base - number(recorded)) / qty);
  else {
    const candidates = [0.05, 0.18].filter(r => gstRate(base / qty / (1 + r)) === r);
    if (candidates.length !== 1) return { gross: money(base), tax: null, rate: null, recorded, status: 'Taxable unit value needs review', valid: false };
    rate = candidates[0];
  }
  const calculated = money(taxesIncluded ? base - base / (1 + rate) : base * rate);
  const valid = recorded === null || Math.abs(recorded - calculated) <= 0.02;
  const tax = valid ? (recorded === null ? calculated : recorded) : null;
  return { gross: money(base + (taxesIncluded ? 0 : number(recorded ?? calculated))), tax, rate, recorded, calculated, status: valid ? (recorded === null ? 'Calculated from confirmed price basis' : 'Invoice verified') : 'Invoice GST differs from expected GST', valid };
}

function completion(order, dispatch = {}) {
  const explicit = day(order.completedAt || order.deliveredAt || dispatch.deliveredAt);
  if (explicit) return { date: explicit, evidence: 'Recorded completion date' };
  if (order.cancelledAt || ['cancelled', 'rto'].includes(dispatch.packingStatus)) return null;
  if (order.channel === 'POS' && ['paid', 'partially_refunded', 'refunded'].includes(order.financialStatus)) {
    return { date: day(order.processedAt || order.createdAt), evidence: 'Completed POS sale' };
  }
  if (dispatch.packingStatus === 'delivered' || order.deliveryComplete) {
    return { date: '', evidence: 'Delivery date missing' };
  }
  return null;
}
function row(values) {
  return Object.assign({ id: '', date: '', kind: '', category: '', channel: 'Shared', particulars: '', reference: '', account: '', method: '', proof: '', source: '', grossSales: 0, discounts: 0, returns: 0, gst: 0, netSales: 0, otherIncome: 0, cogs: 0, expenses: 0, taxComplete: true, costComplete: true, details: [] }, values);
}
function costLots(purchases = {}, opening = {}) {
  const lots = [];
  for (const entry of list(opening.lots)) {
    if (entry.verified !== true || !skuKey(entry.sku) || !(number(entry.qty) > 0) || !present(entry.unitCost) || number(entry.unitCost) < 0) continue;
    lots.push({ id: entry.id || 'OPEN/' + skuKey(entry.sku), sku: skuKey(entry.sku), date: day(entry.date) || START, qty: number(entry.qty), cost: number(entry.unitCost), opening: true });
  }
  for (const po of list(purchases.pos)) {
    if (po.accountingExcluded || !['received', 'awaiting_approval', 'posting_partial', 'posted'].includes(po.status)) continue;
    const date = day(po.dateReceive || po.receivedAt || po.postedAt);
    if (!date || date < START) continue; // Pre-boundary balances require verified opening quantities.
    const costs = purchaseCosts(po, purchases.settings);
    (po.lines || []).forEach((line, index) => {
      const cost = present(line.landedUnitCost) ? number(line.landedUnitCost) : costs.lines[index]?.perPiece;
      const qty = present(line.receivedQty) ? number(line.receivedQty) : number(line.qty);
      if (!skuKey(line.sku) || !(qty > 0) || !(cost > 0) || line.accountingExcluded) return;
      lots.push({ id: po.id + '/' + (line.id || index), sku: skuKey(line.sku), date, qty, cost, purchaseId: po.id });
    });
  }
  return lots.sort((a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id));
}

function buildReport(sources = {}, options = {}) {
  if (options.from && !validDate(options.from) || options.to && !validDate(options.to)) throw Object.assign(new Error('Choose a valid date range.'), { status: 400 });
  const start = validDate(options.start) && options.start > START ? options.start : START;
  const from = options.from < start ? start : options.from || start;
  const to = options.to || day(new Date().toISOString());
  if (!validDate(from) || !validDate(to) || from > to) throw Object.assign(new Error('Choose a valid date range on or after 22 August 2026.'), { status: 400 });
  const selectedChannel = options.channel || 'All';
  if (!['All', 'POS', 'Website', 'Other'].includes(selectedChannel)) throw Object.assign(new Error('Choose a valid sales channel.'), { status: 400 });
  const events = [], warnings = [], tasks = [], taxRows = [], collections = [];
  const books = sources.expenses || {}, orderStore = sources.orders || {}, seen = new Set();
  const warning = (reference, message, date = '', scope = 'data', ch = 'Shared') => warnings.push({ reference, message, date, scope, channel: ch });
  const add = entry => { if (!seen.has(entry.id) && entry.date && entry.date >= start && entry.date <= to) { seen.add(entry.id); events.push(entry); } };
  const lots = costLots(sources.purchases, sources.opening).map(lot => ({ ...lot, remaining: lot.qty }));
  const allocations = new Map();
  const allocate = (sku, qty, date, reference, ch) => {
    let remaining = qty; const parts = [];
    for (const lot of lots) {
      if (lot.sku !== sku || lot.date > date || lot.remaining <= 0) continue;
      const used = Math.min(lot.remaining, remaining);
      lot.remaining -= used; remaining -= used;
      parts.push({ lotId: lot.id, purchaseId: lot.purchaseId || 'Opening stock', qty: used, unitCost: money(lot.cost), amount: money(used * lot.cost), exactCost: lot.cost, returned: 0 });
      if (remaining <= 0) break;
    }
    if (remaining > 0) warning(reference, 'Verified stock/cost missing for ' + (sku || 'unmapped SKU') + ': ' + remaining + ' unit(s). Profit is unavailable until resolved.', date, 'cogs', ch);
    return { parts, qty, missing: remaining, cost: money(parts.reduce((sum, part) => sum + part.qty * part.exactCost, 0)) };
  };
  const returnCost = (key, qty, date, reference, ch) => {
    const allocation = allocations.get(key);
    if (!allocation) return { cost: 0, complete: false };
    let remaining = qty, cost = 0;
    for (const part of allocation.parts) {
      const used = Math.min(remaining, part.qty - part.returned);
      if (!(used > 0)) continue;
      part.returned += used; remaining -= used; cost += used * part.exactCost;
      lots.push({ id: reference + '/' + part.lotId, sku: allocation.sku, date, qty: used, remaining: used, cost: part.exactCost, purchaseId: part.purchaseId });
      if (remaining <= 0) break;
    }
    lots.sort((a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id));
    if (remaining > 0) warning(reference, 'Returned units exceed the verified original cost allocation. Review SKU return quantities.', date, 'cogs', ch);
    return { cost: money(cost), complete: remaining === 0 };
  };

  function queueSale(order, dispatch, source) {
    if (order.accountingExcluded || ['2720'].includes(String(order.number || order.orderNumber || '').replace(/^#/, ''))) return;
    const ch = channel(order.channel), id = source + '/' + order.id, reference = order.name || order.id;
    const made = completion(order, dispatch);
    const transactions = [...new Map((order.paymentTransactions || []).filter(t => ['sale', 'capture'].includes(t.kind) && t.status === 'success').map(t => [t.id || JSON.stringify(t), t])).values()];
    const paymentDetailsKnown = (transactions.length > 0 || order.financialStatus === 'pending') && transactions.every(t => day(t.processedAt));
    const periodPayments = transactions.filter(t => day(t.processedAt) && day(t.processedAt) <= to);
    const collected = paymentDetailsKnown ? money(periodPayments.reduce((sum, t) => sum + number(t.amount), 0)) : null;
    const periodRefunds = (order.refundTransactions || []).filter(t => day(t.processedAt) && day(t.processedAt) <= to);
    const cashRefunded = money(periodRefunds.filter(t => !/store.?credit|gift.?card/i.test(t.gateway)).reduce((sum, t) => sum + number(t.amount), 0));
    const placed = day(order.processedAt || order.createdAt);
    const completedInTime = made?.date && made.date <= to;
    const cancelledInTime = order.cancelledAt ? day(order.cancelledAt) <= to : ['cancelled', 'rto'].includes(dispatch.packingStatus) && !completedInTime;
    const refundedValue = money(list(order.refunds).filter(r => day(r.date || r.createdAt) && day(r.date || r.createdAt) <= to).reduce((sum, r) => sum + number(r.amount), 0));
    if (placed >= start && placed <= to) collections.push({ id, reference, date: completedInTime ? made.date : placed, channel: ch, orderValue: money(order.total), collected, cashRefunded, balanceToCollect: cancelledInTime ? 0 : collected === null ? null : money(Math.max(0, number(order.total) - refundedValue - collected + cashRefunded)), status: completedInTime ? 'Completed sale' : (cancelledInTime ? 'Cancelled / RTO' : 'Awaiting completion'), customerAdvance: collected === null ? null : (completedInTime ? 0 : money(Math.max(0, collected - cashRefunded))), payments: periodPayments.map(t => ({ id: t.id, date: day(t.processedAt), method: t.gateway, amount: money(t.amount) })), source });
    if (!made) return;
    if (!made.date) { warning(reference, 'Delivery is marked complete but its actual date is missing. This sale is excluded until the source date is confirmed.', '', 'sales', ch); return; }
    if (made.date < start || made.date > to) return;
    tasks.push({ date: made.date, sort: 1, id, run() {
      const lines = order.lineItems || [], taxesIncluded = order.taxesIncluded !== false;
      const detail = [], taxDetail = []; let gst = 0, taxComplete = lines.length > 0, cost = 0, costComplete = lines.length > 0, billed = 0;
      lines.forEach((line, index) => {
        const key = id + '/' + (line.id || index), sku = skuKey(line.sku), qty = number(line.qty);
        const tax = lineTax(line, taxesIncluded), allocation = allocate(sku, Math.max(0, qty), made.date, reference, ch);
        allocations.set(key, { ...allocation, sku });
        detail.push({ sku, qty, lineId: line.id || index, gross: tax.gross, gst: tax.tax, netSales: tax.valid ? money(tax.gross - tax.tax) : null, cogs: allocation.missing ? null : allocation.cost, allocations: allocation.parts.map(({ exactCost, returned, ...part }) => part) });
        const taxRecord = { id: key, date: made.date, reference, sku, qty, channel: ch, ...tax, taxesIncluded };
        taxRows.push(taxRecord); taxDetail.push(taxRecord);
        taxComplete = taxComplete && tax.valid; costComplete = costComplete && qty > 0 && allocation.missing === 0;
        billed += tax.gross; gst += number(tax.tax); cost += allocation.cost;
        if (!tax.valid) warning(reference, tax.status + ' for ' + sku + '.', made.date, 'gst', ch);
      });
      const shipping = number(order.shipping);
      if (shipping > 0) {
        const shippingTax = present(order.shippingTax) ? number(order.shippingTax) : null;
        billed += shipping + (!taxesIncluded ? number(shippingTax) : 0); gst += number(shippingTax);
        if (shippingTax === null) { taxComplete = false; warning(reference, 'Shipping tax is missing; no garment rate has been guessed for shipping.', made.date, 'gst', ch); }
      }
      if (!lines.length) warning(reference, 'Sale has no SKU lines. GST and COGS need source details.', made.date, 'sales', ch);
      if (order.taxesIncluded !== true && order.taxesIncluded !== false) {
        taxComplete = false; warning(reference, 'Tax-inclusive/exclusive price basis is missing. Refresh or confirm the source invoice before finalising profit.', made.date, 'gst', ch);
      }
      if (order.currency && order.currency !== 'INR') {
        taxComplete = false; warning(reference, 'Non-INR sale requires a verified exchange-rate allocation.', made.date, 'sales', ch);
      }
      if (Math.abs(money(billed) - number(order.total)) > 0.03) {
        taxComplete = false; warning(reference, 'Line values, discounts, shipping and invoice total do not reconcile. Review the source invoice.', made.date, 'gst', ch);
      }
      add(row({ id, date: made.date, kind: 'sale', category: ch + ' sales', channel: ch, particulars: order.customer?.name || 'Completed sale', reference, source, grossSales: money(number(order.total) + number(order.discount)), discounts: money(order.discount), gst: money(gst), netSales: taxComplete ? money(number(order.total) - gst) : null, cogs: money(cost), taxComplete, costComplete, details: detail }));
    } });
    const refunds = list(order.refunds);
    if (!refunds.length && (order.refundTransactions || []).length) {
      for (const tx of order.refundTransactions) refunds.push({ id: tx.id, date: day(tx.processedAt), amount: number(tx.amount), lineItems: [], legacy: true });
    } else if (!refunds.length && number(order.refundAmount) > 0) warning(reference, 'Refund amount exists without a dated refund/credit note. Review the source before treating profit as complete.', '', 'sales', ch);
    for (const refund of refunds) {
      const date = day(refund.date || refund.createdAt), rid = id + '/REFUND/' + refund.id;
      if (!date || date < made.date) { warning(reference, 'Refund date is missing or precedes the completed sale.', '', 'sales', ch); continue; }
      tasks.push({ date, sort: 2, id: rid, run() {
        let amount = number(refund.amount), refundGst = 0, reversedCost = 0, taxComplete = true, costComplete = true; const detail = [];
        for (const line of refund.lineItems || []) {
          const originalIndex = linesIndex(order.lineItems, line.lineItemId);
          const original = (order.lineItems || [])[originalIndex];
          if (!original) { taxComplete = false; costComplete = false; continue; }
          const qty = number(line.qty), tax = present(line.tax) ? number(line.tax) : null;
          refundGst += number(tax); taxComplete = taxComplete && tax !== null;
          if (line.restockType === 'return') {
            const returned = returnCost(id + '/' + (original.id || originalIndex), qty, date, rid, ch);
            reversedCost += returned.cost; costComplete = costComplete && returned.complete;
          }
          detail.push({ sku: original.sku, qty, returnedToStock: line.restockType === 'return', subtotal: line.subtotal, gst: tax });
        }
        if (!(amount > 0)) amount = money((refund.lineItems || []).reduce((sum, l) => sum + number(l.subtotal) + number(l.tax), 0) + number(refund.adjustmentAmount));
        if (!(amount > 0)) return;
        if (!(refund.lineItems || []).length) {
          taxComplete = false;
          warning(reference, 'Refund has no item/tax breakdown. Revenue deduction is shown; GST and any returned SKU cost need review.', date, 'gst', ch);
        }
        if (present(refund.shippingTax)) refundGst += number(refund.shippingTax);
        if (!taxComplete) warning(reference, 'Refund GST/item linkage is incomplete. Review the source credit note.', date, 'gst', ch);
        if (amount > number(order.total) + .01 || refundGst > amount + .01 || refundGst < 0) {
          taxComplete = false; warning(reference, 'Refund amount or GST exceeds the original invoice allocation.', date, 'gst', ch);
        }
        if (!costComplete) warning(reference, 'Refund could not be linked to its original SKU cost allocation.', date, 'cogs', ch);
        taxRows.push({ id: rid, date, reference, sku: 'Refund / credit note', qty: 0, channel: ch, gross: -amount, tax: taxComplete ? -refundGst : null, recorded: taxComplete ? -refundGst : null, calculated: null, rate: null, valid: taxComplete, status: taxComplete ? 'Source refund tax' : 'Refund tax needs review' });
        add(row({ id: rid, date, kind: 'return', category: 'Returns / refunds', channel: ch, particulars: 'Refund / credit note for ' + reference, reference, source, returns: money(amount), gst: -money(refundGst), netSales: taxComplete ? -money(amount - refundGst) : null, cogs: -money(reversedCost), taxComplete, costComplete, details: detail }));
      } });
    }
  }
  for (const order of list(orderStore.orders)) queueSale(order, orderStore.dispatch?.[order.id] || {}, 'Shopify');
  for (const sale of list(sources.manualSales?.sales)) {
    if (sale.voided || sale.shopifyOrderId || sale.orderId) continue;
    queueSale({ ...sale, taxesIncluded: sale.taxesIncluded ?? true, channel: channel(sale.channel), financialStatus: 'paid', processedAt: sale.day || sale.ts, completedAt: sale.day || sale.ts, lineItems: (sale.items || []).map((l, i) => ({ ...l, id: l.id || String(i), price: l.unitPrice, discount: (sale.items || []).reduce((s, x) => s + number(x.lineTotal), 0) ? number(sale.discount) * number(l.lineTotal) / (sale.items || []).reduce((s, x) => s + number(x.lineTotal), 0) : 0 })), paymentTransactions: [{ id: sale.id, kind: 'sale', status: 'success', amount: sale.total, gateway: sale.paymentMode, processedAt: sale.ts || sale.day }] }, {}, 'Manual sale');
  }
  tasks.sort((a, b) => a.date.localeCompare(b.date) || a.sort - b.sort || a.id.localeCompare(b.id)).filter(task => task.date <= to).forEach(task => task.run());

  const activeDraft = record => !!(record.reconciliationDraft && books.bankReconciliationDrafts?.[record.reconciliationDraft]);
  const paidIds = new Set(), refundPayments = new Map();
  function paymentExpense(expense, payment, amount, kind = 'expense') {
    const date = day(payment.date || payment.paidAt), id = expense.id + '/' + payment.id;
    if (!date) { warning(expense.id, 'Actual payment date is missing; this payment is excluded.', '', 'expense'); return; }
    paidIds.add(id);
    if (date >= start && date <= to && amount > 0) refundPayments.set(expense.id + '/' + (payment.refundPaymentId || payment.id), { date, remaining: money(amount) });
    const type = ['fixed', 'running', 'variable', 'marketing'].includes(expense.type) ? expense.type : 'variable';
    add(row({ id, date, kind, category: expense.ledger || expense.category || 'Unclassified paid expense', group: type, channel: ['POS', 'Website'].includes(expense.channel) ? expense.channel : 'Shared', particulars: expense.particulars || expense.vendor || '', reference: expense.id + '/' + payment.id, expenseDate: day(expense.date), account: payment.account || expense.account || '', method: payment.paymentType || '', proof: payment.proof || expense.billPhoto || '', source: 'Expenses', expenses: money(amount), details: [{ vendor: expense.vendor || '', paymentAmount: money(payment.amount), includedAmount: money(amount), note: payment.note || '' }] }));
  }
  for (const expense of list(books.expenses)) {
    if (!business(expense.nature) || expense.accountingExcluded || expense.nonExpenseReimbursement || expense.salaryAdvanceId || expense.inventoryPurchase || expense.procurementPoId || expense.purchaseOrderId || !['approved', 'partially_paid', 'paid'].includes(expense.status)) continue;
    let remaining = Math.max(0, number(expense.amount));
    const regularPayments = (expense.payments || []).filter(p => !p.voided && !p.accountingExcluded && !(p.bankReconciliationDraft && books.bankReconciliationDrafts?.[p.bankReconciliationDraft])).map((p, i) => ({ ...p, id: p.id || 'PAY-LEGACY/' + i }));
    const advanceApplications = (expense.vendorAdvanceApplications || []).map((p, i) => ({ ...p, id: 'ADV-APPLICATION/' + (p.id || (p.vendorAdvanceId || 'LEGACY') + '/' + (p.batchPaymentId || p.appliedAt || i)), refundPaymentId: 'ADV-' + p.vendorAdvanceId + '-' + i, paymentType: 'Applied vendor advance' })).filter(p => !p.voided && !p.accountingExcluded && !activeDraft((books.vendorAdvances || []).find(a => a.id === p.vendorAdvanceId) || {}));
    const payments = [...regularPayments, ...advanceApplications].sort((a, b) => day(a.date || a.paidAt).localeCompare(day(b.date || b.paidAt)) || String(a.id).localeCompare(String(b.id)));
    const unique = new Set();
    for (const payment of payments) {
      if (unique.has(payment.id) || !(number(payment.amount) > 0)) continue;
      unique.add(payment.id);
      const included = Math.min(remaining, number(payment.amount)); remaining -= included;
      if (included > 0) paymentExpense(expense, payment, included);
    }
    if (number(expense.paidAmount) > money(payments.reduce((sum, p) => sum + number(p.amount), 0))) warning(expense.id, 'Paid total exceeds available payment evidence. Only documented payments are included.', day(expense.date), 'expense');
    if (!payments.length && number(expense.paidAmount) > 0) warning(expense.id, 'Paid amount has no payment record. It is excluded until amount/date evidence is restored.', day(expense.date), 'expense');
  }
  for (const expense of list(books.reconciliationExpenses)) {
    if (!business(expense.nature) || expense.accountingExcluded || activeDraft(expense)) continue;
    // A card credit can be the evidence for a linked supplier refund, not a
    // second expense reversal. Recognise the dated source allocation below.
    if (expense.expenseRefundReceiptId || list(books.expenseRefunds).some(r => r.status === 'received' && (r.components || []).some(c => c.externalMovementId === expense.id))) {
      if (!list(books.expenseRefunds).some(r => r.status === 'received' && (r.components || []).some(c => expense.expenseRefundReceiptId && c.receiptId === expense.expenseRefundReceiptId || c.externalMovementId === expense.id))) warning(expense.id, 'Card refund evidence has no received source allocation. Review its expense-refund link.', day(expense.date), 'expense');
      continue;
    }
    paymentExpense({ ...expense, ledger: expense.category }, { id: 'BANK', refundPaymentId: expense.creditCardId ? 'STATEMENT' : 'BANK', date: expense.date, amount: expense.amount, account: expense.account, paymentType: expense.creditCardId ? 'Credit card' : 'Bank statement' }, number(expense.amount));
  }
  const receivedRefunds = list(books.expenseRefunds).filter(r => business(r.nature) && r.status === 'received' && !r.accountingExcluded && !activeDraft(r) && !r.creditNote).sort((a, b) => String(a.date || '').localeCompare(String(b.date || '')) || String(a.id).localeCompare(String(b.id)));
  for (const refund of receivedRefunds) {
    const date = day(refund.date);
    if (date && (date < start || date > to)) continue;
    for (const [index, source] of (refund.sources || []).entries()) {
      if (source.kind === 'advance' || !source.expenseId) continue; // Returned advances are balance-sheet movements.
      const expense = books.expenses?.[source.expenseId] || list(books.reconciliationExpenses).find(e => e.id === source.expenseId);
      if (!expense || expense.accountingExcluded || !business(expense.nature)) { warning(refund.id, 'Received expense refund has no included business source. Review the original paid expense.', date, 'expense'); continue; }
      if (expense.inventoryPurchase || expense.procurementPoId || expense.purchaseOrderId) { warning(refund.id, 'Inventory supplier refund needs a verified SKU/lot cost correction, not an operating-expense deduction.', date, 'cogs'); continue; }
      const ch = ['POS', 'Website'].includes(expense.channel) ? expense.channel : 'Shared';
      const parts = source.paymentAllocations || []; let reversed = 0;
      for (const allocation of parts) {
        const paid = refundPayments.get(source.expenseId + '/' + allocation.paymentId), amount = number(allocation.amount);
        const included = date && paid && paid.date <= date ? Math.min(paid.remaining, Math.max(0, amount)) : 0;
        if (paid && included) paid.remaining = money(paid.remaining - included);
        reversed += included;
        if (money(included) < money(amount)) warning(refund.id, 'Refund allocation exceeds or cannot identify the original included payment. Only verified paid costs have been reversed.', date, 'expense', ch);
      }
      if (Math.abs(money(reversed) - number(source.amount)) > .01 || !parts.length) warning(refund.id, 'Received supplier refund is not fully linked to paid-expense evidence. Review its source allocation.', date, 'expense', ch);
      if (!(reversed > 0) || !date) continue;
      const type = ['fixed', 'running', 'variable', 'marketing'].includes(expense.type) ? expense.type : 'variable';
      add(row({ id: 'EXPENSE-REFUND/' + refund.id + '/' + index, date, kind: 'expense_refund', category: expense.ledger || expense.category || source.category || 'Unclassified paid expense', group: type, channel: ['POS', 'Website'].includes(expense.channel) ? expense.channel : 'Shared', particulars: 'Supplier refund · ' + (refund.vendor || expense.vendor || '') + ' · ' + (refund.reason || ''), reference: refund.id, expenseDate: day(expense.date), account: (refund.components || []).map(c => c.account || c.issuer || c.mode).filter(Boolean).join(' + '), method: (refund.components || []).map(c => c.mode).join(' + '), proof: refund.proofs?.[0] || '', source: 'Expense refunds', expenses: -money(reversed), details: [{ expenseId: expense.id, paymentAllocations: parts }] }));
    }
  }
  for (const payment of list(sources.incentives?.payments)) {
    if (payment.active === false || payment.accountingExcluded || !(number(payment.amount) > 0)) continue;
    paymentExpense({ id: payment.id, date: payment.date, type: 'variable', ledger: 'Sales incentives', vendor: payment.salesperson, particulars: 'Sales incentive · ' + payment.salesperson, channel: 'POS' }, { ...payment, proof: payment.proofs?.[0] || '', paymentType: 'Incentive payment' }, number(payment.amount), 'incentive');
  }
  for (const payment of list(sources.salary?.salaryPayments)) {
    if (payment.active === false || payment.accountingExcluded || !(number(payment.amount) > 0)) continue;
    if (payment.linkedLedgerEntryId) {
      if (!paidIds.has(payment.linkedLedgerEntryId) && !events.some(e => e.id.startsWith(payment.linkedLedgerEntryId + '/'))) warning(payment.id, 'Salary links to a ledger entry not included in paid expenses. Review its classification to avoid missing or double-counted salary.', day(payment.date), 'expense');
      continue;
    }
    const employee = sources.salary?.employees?.[payment.empId] || {};
    paymentExpense({ id: payment.id, date: payment.date, type: 'fixed', ledger: 'Salary', vendor: payment.employeeName, particulars: 'Salary · ' + payment.employeeName, channel: employee.channel }, { ...payment, paymentType: 'Salary payment' }, number(payment.amount), 'salary');
  }
  const manualReceipts = new Map();
  for (const receipt of list(books.receipts)) {
    if (!business(receipt.nature) || receipt.accountingExcluded || activeDraft(receipt)) continue;
    if (['bank_interest', 'other_income'].includes(receipt.receiptType)) add(row({ id: 'RECEIPT/' + receipt.id, date: day(receipt.date), kind: 'income', category: receipt.category || 'Other income', particulars: receipt.source || '', reference: receipt.id, account: receipt.account || '', proof: receipt.proof || '', source: 'Receipts', otherIncome: money(receipt.amount) }));
    else if (receipt.receiptType === 'product_sale' && !receipt.orderId && !receipt.shopifyOrderId) {
      const key = receipt.manualSaleId || receipt.id, group = manualReceipts.get(key) || []; group.push(receipt); manualReceipts.set(key, group);
    }
    else if (['asset_sale', 'refund'].includes(receipt.receiptType)) warning(receipt.id, 'Receipt requires profit classification (' + receipt.receiptType + '); it has not been treated as ordinary income automatically.', day(receipt.date), 'income');
  }
  for (const [id, receipts] of manualReceipts) {
    if (list(sources.manualSales?.sales).some(sale => sale.id === id)) continue;
    const date = day(receipts[0].date);
    const amount = money(receipts.reduce((sum, r) => sum + number(r.amount), 0));
    add(row({ id: 'MANUAL-RECEIPT/' + id, date, kind: 'sale', channel: 'Other', category: 'Other sales', particulars: receipts[0].source, reference: id, source: 'Receipts', grossSales: amount, netSales: null, taxComplete: false, costComplete: false }));
    warning(id, 'Manual product sale has no SKU/tax allocation. Its gross value is shown; net profit is unavailable until mapped.', date, 'sales', 'Other');
  }
  for (const refund of list(books.salesRefunds)) {
    if (!business(refund.nature) || refund.accountingExcluded || refund.voided || activeDraft(refund)) continue;
    const date = day(refund.date), ref = String(refund.saleReference || '').replace(/^#/, '');
    const linked = events.some(e => e.kind === 'return' && e.date === date && String(e.reference).replace(/^#/, '') === ref && Math.abs(e.returns - number(refund.amount)) < .01);
    if (linked || !(number(refund.amount) > 0)) continue;
    add(row({ id: 'LEDGER-REFUND/' + refund.id, date, kind: 'return', channel: 'Other', category: 'Returns / refunds', particulars: refund.reason, reference: refund.saleReference || refund.id, account: refund.refundAccount, proof: refund.proof, source: 'Ledger refund', returns: money(refund.amount), netSales: null, taxComplete: false, costComplete: false }));
    warning(refund.id, 'Ledger-recorded customer refund is not linked to a source credit note/SKU return. Review tax, original order and restock evidence before profit is final.', date, 'sales', 'Other');
  }
  const choose = entry => selectedChannel === 'All' || entry.channel === selectedChannel;
  const inRange = (entry, lo, hi) => entry.date >= lo && entry.date <= hi && choose(entry);
  const included = events.filter(entry => inRange(entry, from, to));
  const scopedWarnings = warnings.filter(w => (!w.date || w.date >= from && w.date <= to) && choose(w));
  const sourceErrors = (sources.feeds || []).filter(feed => (feed.required || feed.status === 'unreadable') && feed.status !== 'ready');
  for (const feed of sourceErrors) scopedWarnings.push({ reference: feed.name, message: 'Source ' + feed.name + ' is ' + feed.status + '. Profit is unavailable.', scope: 'source' });
  function summarize(rows, issues = []) {
    const fields = ['grossSales', 'discounts', 'returns', 'gst', 'netSales', 'otherIncome', 'cogs', 'expenses'];
    const totals = Object.fromEntries(fields.map(field => [field, money(rows.reduce((sum, entry) => sum + number(entry[field]), 0))]));
    totals.taxComplete = rows.every(e => e.taxComplete) && !issues.some(w => ['gst', 'sales', 'source'].includes(w.scope));
    totals.costComplete = rows.every(e => e.costComplete) && !issues.some(w => ['cogs', 'source'].includes(w.scope));
    totals.complete = totals.taxComplete && totals.costComplete && !issues.some(w => ['expense', 'income'].includes(w.scope));
    totals.knownCogs = totals.cogs; totals.knownGst = totals.gst;
    if (!totals.taxComplete) { totals.gst = null; totals.netSales = null; }
    if (!totals.costComplete) totals.cogs = null;
    totals.totalIncome = totals.taxComplete ? money(totals.netSales + totals.otherIncome) : null;
    totals.grossProfit = totals.taxComplete && totals.costComplete ? money(totals.netSales - totals.cogs) : null;
    totals.netProfit = totals.complete ? money(totals.totalIncome - totals.cogs - totals.expenses) : null;
    totals.margin = totals.netProfit !== null && totals.netSales > 0 ? totals.netProfit / totals.netSales : null;
    totals.salesCount = rows.filter(e => e.kind === 'sale').length;
    return totals;
  }
  const span = (Date.parse(to) - Date.parse(from)) / 86400000 + 1;
  const previousTo = new Date(Date.parse(from) - 86400000).toISOString().slice(0, 10);
  const previousFrom = new Date(Date.parse(from) - span * 86400000).toISOString().slice(0, 10);
  const previousRows = events.filter(e => inRange(e, previousFrom < start ? start : previousFrom, previousTo));
  const previous = previousTo < start ? null : { from: previousFrom < start ? start : previousFrom, to: previousTo, partial: previousFrom < start, totals: summarize(previousRows, [...warnings.filter(w => (!w.date || w.date >= previousFrom && w.date <= previousTo) && choose(w)), ...scopedWarnings.filter(w => w.scope === 'source')]) };
  const categories = new Map();
  for (const entry of included.filter(e => ['expense', 'expense_refund', 'salary', 'incentive'].includes(e.kind))) {
    const key = entry.group + '/' + entry.category, group = categories.get(key) || { id: key, group: entry.group, category: entry.category, amount: 0, count: 0 };
    group.amount = money(group.amount + entry.expenses); group.count++; categories.set(key, group);
  }
  const months = [...new Set(included.map(e => e.date.slice(0, 7)))].sort();
  return { success: true, range: { from, to, requestedFrom: options.from || from, partial: from !== options.from && !!options.from, start }, channel: selectedChannel, basis: 'Completed sales excluding GST − FIFO SKU COGS − approved paid operating expenses', totals: summarize(included, scopedWarnings), previous, categories: [...categories.values()].sort((a, b) => a.group.localeCompare(b.group) || a.category.localeCompare(b.category)), transactions: included.sort((a, b) => b.date.localeCompare(a.date) || a.id.localeCompare(b.id)), taxes: taxRows.filter(e => inRange(e, from, to)), collections: collections.filter(e => inRange(e, from, to)), trends: months.map(month => ({ month, ...summarize(included.filter(e => e.date.startsWith(month)), scopedWarnings.filter(w => !w.date || w.date.startsWith(month))) })), warnings: scopedWarnings, feeds: sources.feeds || [], policy: { operatingExpenses: 'Approved payments only, including paid portions. Reimbursements and vendor advances are not counted twice.', cogs: 'Received inventory allocated by SKU using FIFO, irrespective of supplier payment. Missing opening costs are never estimated.', gst: 'Per-piece taxable sale value ≤ ₹2,500: 5%; above ₹2,500: 18%. Invoice differences require review.', sales: 'POS completion or recorded delivery date. Customer advances and courier/bank settlements are not extra sales.', channels: selectedChannel === 'All' ? 'All channels and Shared overhead included.' : 'Direct channel only. Shared overhead is excluded, not silently allocated.' } };
}
function linesIndex(lines, id) { return (lines || []).findIndex(line => String(line.id) === String(id)); }
module.exports = { START, validDate, day, money, gstRate, lineTax, completion, costLots, buildReport };
