'use strict';

// Expense refunds are linked accounting events, not deletions of payments and
// not ordinary income. All validation is repeated on the server in integer paise.
const crypto = require('crypto');
const MONEY_MODES = new Set(['cash', 'upi', 'bank', 'card']);
const CREDIT_MODES = new Set(['voucher', 'store_credit', 'vendor_credit']);
const MODES = [...MONEY_MODES, ...CREDIT_MODES, 'credit_note'];
const REASONS = ['return', 'cancellation', 'price_correction', 'overpayment'];
const text = value => String(value || '').trim();
const nature = value => text(value).toUpperCase() || 'SANKI';
const sameVendor = (a, b) => text(a).toLowerCase().replace(/\s+/g, ' ') === text(b).toLowerCase().replace(/\s+/g, ' ');
const money = value => Math.round((Number(value) || 0) * 100) / 100;
const cents = value => Math.round((Number(value) || 0) * 100);
const active = refund => refund && refund.status !== 'voided';
const received = refund => active(refund) && refund.status === 'received';
const records = store => store.expenseRefunds || [];
function fail(message, status = 400) { const error = new Error(message); error.status = status; throw error; }
function amount(value) {
  if (value === '' || value == null || !Number.isFinite(Number(value)) || Number(value) <= 0 || !Number.isSafeInteger(cents(value)) || Math.abs(Number(value) * 100 - cents(value)) > .00001) fail('Enter a positive amount with at most two decimal places.');
  return money(value);
}
function validDate(value) {
  const date = text(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(date) && Number.isFinite(Date.parse(date + 'T00:00:00Z')) && new Date(date + 'T00:00:00Z').toISOString().slice(0, 10) === date;
}
function init(store) {
  store.expenseRefunds = Array.isArray(store.expenseRefunds) ? store.expenseRefunds : [];
  store.receipts = Array.isArray(store.receipts) ? store.receipts : [];
  store.vendorAdvances = Array.isArray(store.vendorAdvances) ? store.vendorAdvances : [];
}
function sourcesForExpense(store, expense) {
  const rows = (expense.payments || []).filter(p => !p.accountingExcluded && cents(p.amount) > 0).map(p => ({
    paymentId: text(p.id), amount: money(p.amount), date: p.date || expense.date,
    account: p.account || expense.account || '', personal: !!p.personalFunds,
    claimant: expense.reimbursementClaimant || expense.claimant || expense.createdBy || '',
    reference: p.bankReference || p.reference || '', batchPaymentId: p.batchPaymentId || '',
    grossAmount: money(p.grossPaymentAmount || p.batchTotal || p.amount), creditCardId: p.creditCardId || expense.creditCardId || ''
  }));
  (expense.vendorAdvanceApplications || []).forEach((application, index) => {
    if (application.accountingExcluded) return;
    const advance = (store.vendorAdvances || []).find(a => a.id === application.vendorAdvanceId);
    rows.push({ paymentId: 'ADV-' + application.vendorAdvanceId + '-' + index,
      amount: money(application.amount), date: application.date || expense.date,
      account: advance && advance.account || 'Vendor advance', personal: false, creditOnly: !!(advance && advance.creditOnly),
      reference: application.vendorAdvanceId, grossAmount: money(application.amount) });
  });
  if (!rows.length && cents(expense.paidAmount) > 0) rows.push({ paymentId: 'LEGACY', amount: money(expense.paidAmount),
    date: String(expense.paidAt || expense.date || '').slice(0, 10), account: expense.account || expense.claimantFundingAccount || '',
    personal: !!expense.paidAlready, claimant: expense.claimant || expense.createdBy || '', grossAmount: money(expense.paidAmount) });
  return rows;
}
function refundSources(refund) { return refund.sources || []; }
function expenseTotals(store, expenseId, asOf = '') {
  let billCredit = 0, refunded = 0, personalReturned = 0;
  records(store).filter(r => received(r) && (!asOf || r.date <= asOf)).forEach(r => refundSources(r).filter(s => s.expenseId === expenseId).forEach(s => {
    billCredit += cents(s.billCreditAmount);
    if (!r.creditNote) refunded += cents(s.amount);
    personalReturned += cents(s.personalReturned);
  }));
  return { billCredit: billCredit / 100, refunded: refunded / 100, personalReturned: personalReturned / 100 };
}
function expenseDue(store, expense, asOf = '') {
  const total = expenseTotals(store, expense.id, asOf);
  const paid = asOf ? sourcesForExpense(store, expense).filter(p => p.date <= asOf).reduce((sum, p) => sum + p.amount, 0) : Number(expense.paidAmount || 0);
  return money(Math.max(0, Number(expense.amount || 0) - total.billCredit - (paid - total.refunded)));
}
function reimbursementPosition(store, expense, asOf = '') {
  const total = expenseTotals(store, expense.id, asOf);
  const originalPersonal = asOf ? sourcesForExpense(store, expense).filter(p => p.personal && p.date <= asOf).reduce((sum, p) => sum + p.amount, 0) : Number(expense.personalPaidAmount || 0);
  const netPersonal = money(Math.max(0, originalPersonal - total.personalReturned));
  const reimbursed = asOf ? (expense.reimbursementPayments || []).filter(p => p.date <= asOf && (!p.accountingExcluded || p.historicalSettlement)).reduce((sum, p) => sum + Number(p.amount || 0), 0) : Number(expense.reimbursementAmount || 0);
  const recovered = (store.receipts || []).filter(r => !r.accountingExcluded && r.refundRecoveryExpenseId === expense.id && (!asOf || r.date <= asOf)).reduce((sum, r) => sum + Number(r.amount || 0), 0);
  return { netPersonalPaid: netPersonal, pending: money(Math.max(0, netPersonal - reimbursed)),
    recoverable: money(Math.max(0, reimbursed - netPersonal - recovered)), recovered: money(recovered) };
}
function decorateExpense(store, expense) {
  const totals = expenseTotals(store, expense.id), position = reimbursementPosition(store, expense);
  // The bill view exposes its own allocation, not other bills or private
  // receiving-account details from a consolidated supplier refund.
  const history = records(store).filter(r => active(r) && refundSources(r).some(s => s.expenseId === expense.id)).map(r => ({
    id: r.id, date: r.date, status: r.status, reason: r.reason,
    amount: money(refundSources(r).filter(s => s.expenseId === expense.id).reduce((sum, s) => sum + s.amount, 0))
  }));
  return Object.assign({}, expense, { refundHistory: history, refundedAmount: totals.refunded, expenseCreditAmount: totals.billCredit,
    netExpenseAmount: money(Number(expense.amount || 0) - totals.billCredit), balanceDue: expenseDue(store, expense),
    refundStatus: totals.refunded || totals.billCredit ? (cents(totals.billCredit) >= cents(expense.amount) ? 'fully_refunded' : 'partially_refunded') : history.length ? 'refund_pending' : '',
    refundAdjustedPersonalPaid: position.netPersonalPaid, refundReimbursementRecoverable: position.recoverable });
}
function allocatedRefund(store, expenseId, paymentId) {
  return records(store).filter(received).reduce((sum, r) => sum + refundSources(r).filter(s => s.expenseId === expenseId)
    .reduce((n, s) => n + (s.paymentAllocations || []).filter(a => a.paymentId === paymentId).reduce((m, a) => m + cents(a.amount), 0), 0), 0);
}
function catalog(store, expenseRows) {
  const rows = [];
  expenseRows.filter(e => (e.approvedAt || ['approved', 'partially_paid', 'paid'].includes(e.status)) && cents(e.amount) > 0).forEach(e => {
    const payments = sourcesForExpense(store, e).map(p => Object.assign({}, p, { remaining: Math.max(0, cents(p.amount) - allocatedRefund(store, e.id, p.paymentId)) / 100 }));
    const remaining = money(payments.reduce((sum, p) => sum + p.remaining, 0));
    const base = { kind: 'expense', expenseId: e.id, nature: nature(e.nature), vendor: e.vendor || '', date: e.date,
      particulars: e.particulars || e.category || '', category: e.ledger || e.category || '', originalAmount: money(e.amount), paidAmount: money(e.paidAmount),
      refundedAmount: expenseTotals(store, e.id).refunded, remaining, unpaidAmount: expenseDue(store, e),
      payments, personal: payments.some(p => p.personal), mixedPayers: new Set(payments.map(p => p.personal ? 'payer' : 'company')).size > 1,
      statementBacked: !!e.statementBacked };
    rows.push(Object.assign({}, base, { key: 'expense:' + e.id, reference: e.id }));
    payments.forEach(p => rows.push(Object.assign({}, base, { kind: 'payment', key: 'payment:' + e.id + '/' + p.paymentId,
      paymentId: p.paymentId, reference: e.id + '/' + p.paymentId, date: p.date, remaining: p.remaining, originalPaymentAmount: p.amount,
      account: p.account, personal: p.personal, mixedPayers: false, batchPaymentId: p.batchPaymentId, grossPaymentAmount: p.grossAmount, payments: [p] })));
  });
  (store.vendorAdvances || []).filter(a => !a.accountingExcluded && cents(a.remainingAmount) > 0).forEach(a => rows.push({
    key: 'advance:' + a.id, kind: 'advance', advanceId: a.id, reference: a.id, nature: nature(a.nature), vendor: a.vendor || '',
    date: a.date, category: '', particulars: a.creditOnly ? 'Convert remaining refund credit to a refund' : 'Unallocated vendor advance / overpayment',
    originalAmount: money(a.amount), paidAmount: money(a.amount), remaining: money(a.remainingAmount), unpaidAmount: 0,
    account: a.account || '', personal: !!a.personalFunds, issuer: a.issuer || a.vendor || '', creditOnly: !!a.creditOnly, payments: []
  }));
  return rows;
}
function childTotal(store, parentId) { return records(store).filter(r => active(r) && r.parentId === parentId).reduce((sum, r) => sum + cents(r.amount), 0) / 100; }
function viewRecord(store, record, today) {
  const pending = record.status === 'pending' ? money(Math.max(0, record.amount - childTotal(store, record.id))) : 0;
  return Object.assign({}, record, { pendingAmount: pending, displayStatus: record.status === 'pending' && !pending ? 'completed' : record.status,
    pendingSources: record.status === 'pending' ? record.sources.map(s => ({ key: s.key, amount: money(Math.max(0, s.amount - records(store).filter(r => received(r) && r.parentId === record.id).reduce((sum, r) => sum + r.sources.filter(x => x.key === s.key).reduce((n, x) => n + x.amount, 0), 0))) })) : [],
    components: (record.components || []).map(c => {
      const advance = (store.vendorAdvances || []).find(a => a.id === c.vendorAdvanceId);
      return Object.assign({}, c, { remainingCredit: advance ? money(advance.remainingAmount) : 0,
        expired: !!(c.expiryDate && c.expiryDate < today), applications: advance && advance.applications || [] });
    }) });
}
function fingerprint(input) { return crypto.createHash('sha256').update(JSON.stringify(input)).digest('hex'); }
function prepare(store, input, context) {
  init(store);
  const today = context.today, date = text(input.date), reason = text(input.reason), status = input.status === 'pending' ? 'pending' : 'received';
  if (!validDate(date) || date > today) fail('Enter a valid date, not later than today.');
  if (!reason) fail('A reason is required. Proof is optional.');
  if (!REASONS.includes(input.reasonType)) fail('Select the reason for the refund or return.');
  const key = text(input.requestId);
  if (!/^[a-zA-Z0-9-]{8,100}$/.test(key)) fail('A request identity is required. Refresh this form and retry.');
  const choices = Array.isArray(input.sources) ? input.sources : [{ key: input.sourceKey, amount: input.amount }];
  const inputSignature = fingerprint({ choices, components: status === 'pending' ? [] : input.components,
    date, reason, reasonType: input.reasonType, status, parentId: input.parentId || '' });
  const priorRequest = records(store).find(r => r.requestId === key);
  if (priorRequest) {
    if (priorRequest.inputSignature !== inputSignature) fail('This request was already used with different details. Start a new refund.', 409);
    context.checkRecord(priorRequest);
    return { already: priorRequest };
  }
  if (!choices.length || choices.length > 30 || new Set(choices.map(s => text(s.key))).size !== choices.length) fail('Select distinct bill/payment allocations (up to 30).');
  const available = catalog(store, context.expenses), sourceKeys = new Set(), sources = [];
  let chosenNature = '', chosenVendor = '';
  for (const choice of choices) {
    const source = available.find(s => s.key === text(choice.key));
    if (!source) fail('The selected expense or payment is no longer available. Refresh the source list.', 409);
    context.checkSource(source);
    if (chosenNature && (chosenNature !== source.nature || !sameVendor(chosenVendor, source.vendor))) fail('A combined refund must use the same entity and vendor.');
    chosenNature = source.nature; chosenVendor = source.vendor;
    const amountValue = amount(choice.amount);
    const entityId = source.expenseId || source.advanceId;
    if (sourceKeys.has(entityId)) fail('Select a bill OR its payment allocation, not both.');
    sourceKeys.add(entityId);
    if (date < source.date) fail('The refund/return date cannot precede its original expense or payment.');
    sources.push(Object.assign({}, source, { amount: amountValue }));
  }
  const rawComponents = status === 'pending' ? [] : input.components;
  if (status !== 'pending' && (!Array.isArray(rawComponents) || !rawComponents.length || rawComponents.length > 20)) fail('Enter at least one refund mode and amount.');
  const creditNote = status !== 'pending' && rawComponents.every(c => c.mode === 'credit_note');
  if (status !== 'pending' && rawComponents.some(c => c.mode === 'credit_note') && !creditNote) fail('Log a credit note separately from money or vouchers received.');
  const sourceAmount = money(sources.reduce((sum, s) => sum + s.amount, 0));
  if (new Set(sources.flatMap(s => s.payments).filter(p => p.personal).map(p => p.claimant + '|' + p.account)).size > 1) fail('Log refunds for different personal payers/accounts separately, so reimbursement adjustments reach the right person.');
  const components = (rawComponents || []).map((raw, index) => {
    const mode = text(raw.mode);
    if (!MODES.includes(mode)) fail('Select a supported refund mode.');
    const value = amount(raw.amount), receiver = raw.receiver === 'payer' ? 'payer' : 'company';
    if (receiver === 'payer' && sources.some(s => !s.personal || s.mixedPayers)) fail('Personal refunds must be linked to the actual personal payment allocation.');
    if (sources.some(s => s.mixedPayers)) fail('This expense used personal and company funds. Select the specific payment allocation.');
    if (receiver === 'company' && CREDIT_MODES.has(mode) && sources.some(s => s.personal)) fail('For a personally paid expense, record a voucher with the original payer; a company cash/bank refund may be recorded separately.');
    const account = MONEY_MODES.has(mode) ? context.checkAccount(sources, raw, mode, receiver) : '';
    const issuer = CREDIT_MODES.has(mode) ? text(raw.issuer) || chosenVendor : '';
    const reference = text(raw.reference), expiryDate = text(raw.expiryDate);
    if (CREDIT_MODES.has(mode) && !reference) fail('Enter the voucher or credit reference so it can be tracked and redeemed.');
    if (expiryDate && (!validDate(expiryDate) || expiryDate < date)) fail('Credit expiry cannot precede the date it was received.');
    if (mode === 'credit_note' && receiver !== 'company') fail('A credit note adjusts the bill; it is not a personal receipt.');
    return { id: 'PART-' + String(index + 1).padStart(3, '0'), mode, amount: value, receiver, account, issuer, reference, expiryDate,
      externalMovementId: text(raw.externalMovementId) };
  });
  if (status !== 'pending' && cents(components.reduce((sum, c) => sum + c.amount, 0)) !== cents(sourceAmount)) fail('Refund-mode amounts must equal the selected bill/payment allocations.');
  if (creditNote && input.reasonType === 'overpayment') fail('A credit note reduces an unpaid bill; it is not an overpayment refund.');
  let personalCents = components.filter(c => c.receiver === 'payer').reduce((sum, c) => sum + cents(c.amount), 0);
  for (const source of sources) {
    const limit = creditNote ? source.unpaidAmount : status === 'pending' ? money(source.unpaidAmount + source.remaining) : source.remaining;
    if (cents(source.amount) > cents(limit)) fail('Refund exceeds the remaining ' + (creditNote ? 'unpaid bill' : 'refundable amount') + ' for ' + source.reference + ' (₹' + limit.toFixed(2) + ').', 409);
    if (creditNote && source.kind === 'advance') fail('Select an unpaid expense for a credit note.');
    if (source.kind !== 'advance' && input.reasonType === 'overpayment') fail('Select the unallocated vendor advance for an overpayment refund; do not reopen a paid bill.');
    source.billCreditAmount = source.kind === 'advance' ? 0 : source.amount;
    if (source.expenseId) {
      const expense = context.expenses.find(e => e.id === source.expenseId);
      const remainingBill = cents(expense.amount) - cents(expenseTotals(store, expense.id).billCredit);
      if (cents(source.billCreditAmount) > remainingBill) fail('The return/credit exceeds the remaining original bill amount.', 409);
    }
    source.paymentAllocations = [];
    let left = creditNote || status === 'pending' ? 0 : cents(source.amount);
    for (const payment of source.payments) {
      const take = Math.min(left, cents(payment.remaining));
      if (!take) continue;
      if (date < payment.date) fail('Refund cannot precede the selected payment date.');
      source.paymentAllocations.push({ paymentId: payment.paymentId, amount: take / 100, date: payment.date, account: payment.account, personal: payment.personal });
      left -= take;
    }
    if (left && source.kind !== 'advance') fail('The original payment allocations changed. Refresh before recording this refund.', 409);
    source.personalReturned = source.personal ? Math.min(personalCents, cents(source.amount)) / 100 : 0;
    personalCents -= cents(source.personalReturned);
  }
  const parent = input.parentId && records(store).find(r => r.id === input.parentId && r.status === 'pending' && active(r));
  if (input.parentId && (!parent || status === 'pending' || parent.nature !== chosenNature || !sameVendor(parent.vendor, chosenVendor))) fail('Choose an active pending return before logging its receipt.');
  if (parent) {
    if (date < parent.date || cents(sourceAmount) > cents(parent.amount - childTotal(store, parent.id))) fail('The receipt exceeds the outstanding pending return or precedes its date.', 409);
    const parentIds = new Set(parent.sources.map(s => s.key));
    if (sources.some(s => !parentIds.has(s.key))) fail('The receipt must use the sources selected on its pending return.');
    sources.forEach(s => {
      const original = parent.sources.find(p => p.key === s.key);
      const used = records(store).filter(r => received(r) && r.parentId === parent.id).reduce((sum, r) => sum + r.sources.filter(p => p.key === s.key).reduce((n, p) => n + cents(p.amount), 0), 0);
      if (cents(s.amount) > cents(original.amount) - used) fail('Receipt exceeds this bill allocation on the pending return.', 409);
    });
  }
  const proofs = Array.from(new Set([].concat(input.proofs || [], input.proof || []).map(text).filter(Boolean)));
  if (proofs.some(p => !/^\/api\/expenses\/photo\/[a-zA-Z0-9_.-]+$/.test(p))) fail('Attach proof through the normal expense upload control.');
  const canonical = { sources: sources.map(s => ({ key: s.key, amount: s.amount })), components, date, reason, reasonType: input.reasonType, status, parentId: input.parentId || '' };
  const signature = fingerprint(canonical), prior = records(store).find(r => r.requestId === key);
  if (prior) { if (prior.signature !== signature) fail('This request was already used with different details. Start a new refund.', 409); return { already: prior }; }
  const duplicate = records(store).find(r => active(r) && r.signature === signature);
  if (duplicate) fail('This refund/return is already recorded as ' + duplicate.id + '. Check its history before recording another.', 409);
  for (const component of components) {
    if (component.externalMovementId && components.filter(c => c.externalMovementId === component.externalMovementId).length > 1) fail('Link each existing receipt once per refund; combine its allocation into one refund-mode row.');
    if (component.reference && components.filter(c => c.mode === component.mode && c.account === component.account && c.issuer === component.issuer && c.reference.toLowerCase() === component.reference.toLowerCase()).length > 1) fail('Use one refund-mode row for each transaction/voucher reference.');
    const duplicateReference = component.reference && records(store).filter(received).some(r => r.components.some(c => c.reference && text(c.reference).toLowerCase() === component.reference.toLowerCase() && c.mode === component.mode && c.account === component.account && c.issuer === component.issuer));
    if (duplicateReference && !component.externalMovementId) fail('That refund transaction/voucher reference is already recorded. Link its existing receipt instead of recording it twice.', 409);
    if (component.externalMovementId) context.checkExistingMovement(component, chosenNature, store);
  }
  return { date, reason, reasonType: input.reasonType, status, sources, components, amount: sourceAmount,
    nature: chosenNature, vendor: chosenVendor, creditNote, proofs, requestId: key, signature, inputSignature, parentId: parent && parent.id || '' };
}
function post(store, prepared, context) {
  if (prepared.already) return { refund: prepared.already, already: true };
  init(store);
  store.expenseRefundSeq = Number(store.expenseRefundSeq || 0) + 1;
  const id = 'RF-' + String(store.expenseRefundSeq).padStart(5, '0'), now = context.now || new Date().toISOString();
  const refund = Object.assign({}, prepared, { id, createdBy: context.username, createdAt: now });
  refund.sources = refund.sources.map(s => ({ key: s.key, kind: s.kind, expenseId: s.expenseId || '', paymentId: s.paymentId || '', advanceId: s.advanceId || '',
    reference: s.reference, originalAmount: s.originalAmount, originalPaymentAmount: s.originalPaymentAmount || s.paidAmount,
    amount: s.amount, billCreditAmount: s.billCreditAmount, personalReturned: s.personalReturned,
    paymentAllocations: s.paymentAllocations, sourceDate: s.date, category: s.category, personal: s.personal }));
  if (refund.status === 'received') {
    refund.sources.filter(s => s.kind === 'advance').forEach(source => {
      const advance = store.vendorAdvances.find(a => a.id === source.advanceId);
      advance.remainingAmount = money(advance.remainingAmount - source.amount);
      advance.refundIds = [...(advance.refundIds || []), id];
    });
    refund.components.forEach(component => {
      if (MONEY_MODES.has(component.mode) && !component.externalMovementId && component.receiver === 'company') {
        component.receiptId = id + '/' + component.id;
        store.receipts.push({ id: component.receiptId, expenseRefundId: id, nature: refund.nature,
          account: component.account, amount: component.amount, date: refund.date, receiptType: 'expense_refund',
          category: 'Expense refund — linked source', source: 'Expense refund · ' + refund.vendor + ' · ' + refund.sources.map(s => s.reference).join(', '),
          bankReference: component.reference, proof: refund.proofs[0] || '', proofs: refund.proofs,
          proofException: refund.proofs.length ? '' : 'Refund proof optional — entered without attachment',
          personalRefund: component.receiver === 'payer', creditCardRefund: component.mode === 'card',
          note: refund.reason, createdBy: context.username, createdAt: now });
      }
      if (CREDIT_MODES.has(component.mode) && component.receiver === 'company') {
        component.vendorAdvanceId = id + '/' + component.id + '/CREDIT';
        store.vendorAdvances.push({ id: component.vendorAdvanceId, expenseRefundId: id, nature: refund.nature, vendor: component.issuer,
          issuer: component.issuer, date: refund.date, amount: component.amount, remainingAmount: component.amount,
          account: 'Refund credit', paymentType: component.mode, creditOnly: true, expiryDate: component.expiryDate,
          paymentReference: component.reference, note: 'Non-cash refund credit · ' + refund.reason, applications: [], createdBy: context.username, createdAt: now });
      }
    });
  }
  store.expenseRefunds.push(refund);
  return { refund, already: false };
}
function billCreditEntries(store, asOf = '') {
  return records(store).filter(r => received(r) && (!asOf || r.date <= asOf)).flatMap(r => {
    let covered = r.components.filter(c => c.externalMovementId && c.mode === 'card').reduce((sum, c) => sum + cents(c.amount), 0);
    return r.sources.filter(s => cents(s.billCreditAmount) > 0).map(s => {
      const take = Math.min(covered, cents(s.billCreditAmount)); covered -= take;
      return { id: r.id + '/' + s.expenseId, expenseId: s.expenseId, nature: r.nature, vendor: r.vendor,
        date: r.date, amount: s.billCreditAmount, plAmount: (cents(s.billCreditAmount) - take) / 100,
        category: (store.expenses || {})[s.expenseId] && store.expenses[s.expenseId].ledger || s.category, reason: r.reason, refundId: r.id };
    });
  });
}
function vendorLedgerRows(store) {
  return records(store).filter(received).flatMap(r => [
    ...r.sources.filter(s => cents(s.billCreditAmount) > 0).map(s => ({ nature: r.nature, vendor: r.vendor, date: r.date,
      particulars: 'Return / bill credit · ' + s.reference + ' · ' + r.reason, type: 'Return / credit note', reference: r.id,
      in: s.billCreditAmount, out: 0, kind: 'expense_return', order: 30, category: s.category })),
    ...r.sources.filter(s => !r.creditNote).map(s => ({ nature: r.nature, vendor: r.vendor, date: r.date,
      particulars: 'Refund received · ' + s.reference + ' · ' + r.components.map(c => c.mode).join(' + '), type: 'Refund received', reference: r.id,
      in: 0, out: s.amount, expenseId: s.expenseId || '', kind: 'expense_refund', order: 40, category: s.category }))
  ]);
}

function hasSource(store, expenseId) {
  const expense = (store.expenses || {})[expenseId];
  return records(store).some(r => active(r) && r.sources.some(s => {
    if (s.expenseId === expenseId) return true;
    const advance = s.advanceId && (store.vendorAdvances || []).find(a => a.id === s.advanceId);
    return !!(advance && expense && ((advance.paymentReference || '').startsWith(expenseId + '/') || advance.batchPaymentId && (expense.payments || []).some(p => p.batchPaymentId === advance.batchPaymentId)));
  }));
}
function hasCreditApplication(store, expenseId) {
  return (store.vendorAdvances || []).some(a => a.expenseRefundId && !a.accountingExcluded && (a.applications || []).some(p => !p.accountingExcluded && p.expenseId === expenseId));
}
function protectsMovement(store, id) {
  const expenseId = String(id).split('/')[0];
  return hasSource(store,expenseId) || hasCreditApplication(store,expenseId) || records(store).some(r => active(r) && (r.components.some(c => c.receiptId === id || c.externalMovementId === id || c.vendorAdvanceId === id) || r.sources.some(s => s.advanceId === id || (s.paymentAllocations || []).some(p => s.expenseId + '/' + p.paymentId === id))));
}
function manualCardRefunds(store, cardId, cardName) {
  return (store.receipts || []).filter(r => !r.accountingExcluded && r.creditCardRefund && r.account === cardName && !(store.reconciliationExpenses || []).some(e => e.creditCardId === cardId && e.expenseRefundReceiptId === r.id));
}

module.exports = { MONEY_MODES, CREDIT_MODES, MODES, REASONS, money, cents, active, received, validDate, init, records,
  expenseTotals, expenseDue, reimbursementPosition, decorateExpense, catalog, prepare, post, viewRecord, billCreditEntries, vendorLedgerRows, sameVendor, hasSource, hasCreditApplication, protectsMovement, manualCardRefunds };
