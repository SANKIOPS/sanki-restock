'use strict';

// Borrowed principal is a balance-sheet liability, never income or an expense.
// The receipt is the single cash posting; repayments are separate linked events.
const TYPE = 'loan_received';
const CATEGORY = 'Loan received (liability)';
const text = v => String(v == null ? '' : v).trim();
const cents = v => Math.round(Number(v) * 100);
const money = v => cents(v) / 100;
const active = r => r && !r.accountingExcluded && r.status !== 'voided';
const isLoan = r => r && r.receiptType === TYPE;
const loans = s => (s.receipts || []).filter(r => isLoan(r) && active(r));
const repayments = s => (s.loanRepayments || []).filter(active);
function fail(message, status = 400) { const e = new Error(message); e.status = status; throw e; }
function amount(value) {
  if (value === '' || value == null || !Number.isFinite(Number(value)) || Number(value) <= 0 ||
      !Number.isSafeInteger(cents(value)) || Math.abs(Number(value) * 100 - cents(value)) > .00001)
    fail('Enter a positive principal amount with at most two decimal places.');
  return money(value);
}
function validDate(v) {
  const d = text(v), n = Date.parse(d + 'T00:00:00Z');
  return /^\d{4}-\d{2}-\d{2}$/.test(d) && Number.isFinite(n) && new Date(n).toISOString().slice(0, 10) === d;
}
function principal(value, account) {
  const valueMoney = amount(value);
  // Existing cash ledgers operate in whole rupees. Reject fractional cash
  // principal rather than silently rounding cash while retaining paise debt.
  if (/cash/i.test(account) && cents(valueMoney) % 100) fail('Cash loan principal must be in whole rupees. Use a bank account for a payment containing paise.');
  return valueMoney;
}
function date(value) { if (!validDate(value)) fail('Enter a valid transaction date.'); return text(value); }
function terms(body, receivedDate) {
  const b = body.loanTerms || {}, lenderKind = text(b.lenderKind), dueDate = text(b.dueDate);
  if (!['person', 'company', 'entity'].includes(lenderKind)) fail('Choose whether the lender is a person, company or other entity.');
  if (dueDate && (!validDate(dueDate) || dueDate < receivedDate)) fail('The repayment due date must be on or after the loan receipt date.');
  const method = text(b.interestMethod || 'none').toLowerCase();
  if (!['none', 'flat', 'reducing'].includes(method)) fail('Choose flat or reducing-balance interest.');
  const result = { lenderKind, dueDate, lenderReference: text(b.lenderReference), interestMethod: method };
  if (method === 'none') return result;
  const annualRate = Number(b.annualRate), frequency = text(b.frequency || 'monthly').toLowerCase();
  const installmentCount = Number(b.installmentCount), firstPaymentDate = text(b.firstPaymentDate);
  if (!Number.isFinite(annualRate) || annualRate < 0 || annualRate > 100) fail('Enter an annual interest rate between 0 and 100%.');
  if (!['monthly', 'daily'].includes(frequency)) fail('Choose monthly or daily instalments.');
  if (!Number.isInteger(installmentCount) || installmentCount < 1 || installmentCount > (frequency === 'daily' ? 3650 : 600)) fail('Enter a valid number of instalments.');
  if (!validDate(firstPaymentDate) || firstPaymentDate < receivedDate) fail('Choose a valid first instalment date on or after the loan receipt date.');
  const override = b.installmentAmount === '' || b.installmentAmount == null ? 0 : amount(b.installmentAmount);
  return Object.assign(result, { annualRate: money(annualRate), frequency, installmentCount, firstPaymentDate,
    installmentAmount: override || 0, calculation: override ? 'manual_override' : 'automatic' });
}
function isoDate(value) { return new Date(value + 'T00:00:00Z'); }
function addInstallments(value, count, frequency) {
  const d = isoDate(value);
  if (frequency === 'daily') d.setUTCDate(d.getUTCDate() + count);
  else {
    const original = d.getUTCDate(); d.setUTCDate(1); d.setUTCMonth(d.getUTCMonth() + count);
    d.setUTCDate(Math.min(original, new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0)).getUTCDate()));
  }
  return d.toISOString().slice(0, 10);
}
function configured(loan) { return loan && loan.loanTerms && ['flat', 'reducing'].includes(loan.loanTerms.interestMethod); }
function schedule(loan) {
  if (!configured(loan)) return [];
  const t = loan.loanTerms, n = Number(t.installmentCount), principalCents = cents(loan.amount);
  const periods = t.frequency === 'daily' ? 365 : 12, rate = Number(t.annualRate) / 100 / periods;
  let paymentCents;
  if (cents(t.installmentAmount) > 0) paymentCents = cents(t.installmentAmount);
  else if (t.interestMethod === 'flat') paymentCents = Math.round((principalCents + principalCents * rate * n) / n);
  else paymentCents = rate === 0 ? Math.round(principalCents / n) : Math.round(principalCents * rate * Math.pow(1 + rate, n) / (Math.pow(1 + rate, n) - 1));
  let balance = principalCents, rows = [];
  for (let i = 0; i < n && balance > 0; i++) {
    const interest = t.interestMethod === 'flat' ? Math.round(principalCents * rate) : Math.round(balance * rate);
    let principal = Math.max(0, paymentCents - interest);
    if (i === n - 1 || principal >= balance) principal = balance;
    if (!principal && balance) fail('The instalment amount must be greater than the calculated interest.');
    balance -= principal;
    rows.push({ number: i + 1, dueDate: addInstallments(t.firstPaymentDate, i, t.frequency),
      principal: principal / 100, interest: interest / 100, amount: (principal + interest) / 100, balance: balance / 100 });
  }
  if (balance > 0) fail('The manual instalment amount does not repay the loan within the selected number of instalments.');
  return rows;
}
function allocation(s, loan, paymentAmount) {
  const total = cents(paymentAmount), prior = repayments(s).filter(p => p.loanId === loan.id);
  const plan = schedule(loan);
  if (!plan.length) return { principalAmount: total / 100, interestAmount: 0, chargeAmount: 0 };
  let consumedPrincipal = prior.reduce((n, p) => n + cents(p.principalAmount == null ? p.amount : p.principalAmount), 0);
  let consumedInterest = prior.reduce((n, p) => n + cents(p.interestAmount || 0), 0), remaining = total, principal = 0, interest = 0;
  for (const row of plan) {
    let rowInterest = cents(row.interest), rowPrincipal = cents(row.principal);
    const usedInterest = Math.min(consumedInterest, rowInterest); consumedInterest -= usedInterest; rowInterest -= usedInterest;
    const usedPrincipal = Math.min(consumedPrincipal, rowPrincipal); consumedPrincipal -= usedPrincipal; rowPrincipal -= usedPrincipal;
    const addInterest = Math.min(remaining, rowInterest); interest += addInterest; remaining -= addInterest;
    const addPrincipal = Math.min(remaining, rowPrincipal); principal += addPrincipal; remaining -= addPrincipal;
    if (!remaining) break;
  }
  if (remaining) fail('Payment exceeds the remaining scheduled principal and interest.');
  return { principalAmount: principal / 100, interestAmount: interest / 100, chargeAmount: 0 };
}
function schedulePosition(s, loan, asOf = '') {
  const plan = schedule(loan), payments = repayments(s).filter(p => p.loanId === loan.id && (!asOf || p.date <= asOf));
  let principalPaid = payments.reduce((n, p) => n + cents(p.principalAmount == null ? p.amount : p.principalAmount), 0) / 100;
  let interestPaid = payments.reduce((n, p) => n + cents(p.interestAmount || 0), 0) / 100;
  const rows = plan.map(row => {
    const principalApplied = Math.min(row.principal, principalPaid); principalPaid = money(principalPaid - principalApplied);
    const interestApplied = Math.min(row.interest, interestPaid); interestPaid = money(interestPaid - interestApplied);
    const remaining = money(row.amount - principalApplied - interestApplied);
    return Object.assign({}, row, { principalPaid: principalApplied, interestPaid: interestApplied, remaining,
      status: remaining <= 0 ? 'paid' : asOf && row.dueDate < asOf ? 'overdue' : 'upcoming' });
  });
  return rows;
}
function position(s, loan, asOf = '') {
  const payments = repayments(s).filter(p => p.loanId === loan.id && (!asOf || p.date <= asOf));
  const borrowed = active(loan) && (!asOf || loan.date <= asOf) ? cents(loan.amount) : 0;
  const repaid = payments.reduce((n, p) => n + cents(p.principalAmount == null ? p.amount : p.principalAmount), 0);
  const interestPaid = payments.reduce((n, p) => n + cents(p.interestAmount || 0), 0);
  const totalPaid = payments.reduce((n, p) => n + cents(p.amount), 0), plan = schedulePosition(s, loan, asOf);
  const overdue = plan.filter(r => r.status === 'overdue').reduce((n, r) => n + cents(r.remaining), 0);
  const next = plan.find(r => r.status !== 'paid');
  return { borrowed: borrowed / 100, repaid: repaid / 100, principalRepaid: repaid / 100, interestPaid: interestPaid / 100,
    totalPaid: totalPaid / 100, outstanding: Math.max(0, borrowed - repaid) / 100, overdue: overdue / 100,
    nextPayment: next || null, schedule: plan,
    status: !borrowed ? 'not_received' : repaid >= borrowed ? 'repaid' : repaid ? 'partially_repaid' : 'outstanding', payments };
}
function catalog(s, asOf = '') {
  return loans(s).filter(l => !asOf || l.date <= asOf).map(l => Object.assign({}, l, position(s, l, asOf)))
    .sort((a, b) => String(b.date + b.id).localeCompare(String(a.date + a.id)));
}
// A lender may have several agreements. Group only exact normalized names in
// the same borrowing entity; never net principal against that party's bills.
function lenderBooks(entries, { from = '', search = '' } = {}) {
  const groups = new Map(), normalize = v => text(v).normalize('NFKC').replace(/\s+/g, ' ').toLowerCase();
  for (const loan of entries) {
    const key = normalize(loan.nature) + '|' + normalize(loan.source);
    let group = groups.get(key);
    if (!group) {
      group = { key, name: loan.source, nature: loan.nature, partyType: 'loan', borrowed: 0, repaid: 0, principalRepaid: 0, interestPaid: 0, totalPaid: 0, outstanding: 0, loans: [] };
      groups.set(key, group);
    }
    group.borrowed = money(group.borrowed + loan.borrowed);
    group.repaid = money(group.repaid + loan.repaid);
    group.principalRepaid = money(group.principalRepaid + loan.principalRepaid);
    group.interestPaid = money(group.interestPaid + loan.interestPaid);
    group.totalPaid = money(group.totalPaid + loan.totalPaid);
    group.outstanding = money(group.outstanding + loan.outstanding);
    group.loans.push(loan);
  }
  const query = normalize(search);
  return [...groups.values()].filter(group => !query || normalize([group.name, group.nature, ...group.loans.flatMap(l =>
    [l.id, l.note, l.bankReference, l.loanTerms && l.loanTerms.lenderReference, ...(l.payments || []).flatMap(p => [p.id, p.note, p.bankReference])])].join(' ')).includes(query))
    .map(group => {
      const events = group.loans.flatMap(l => [{ id: l.id, loanId: l.id, date: l.date, account: l.account, note: l.note,
        reference: l.bankReference || l.id, received: l.borrowed, repaid: 0 }].concat((l.payments || []).map(p => ({
        id: p.id, loanId: l.id, date: p.date, account: p.account, note: p.note || '', reference: p.bankReference || p.id, received: 0,
        repaid: p.principalAmount == null ? p.amount : p.principalAmount, interest: p.interestAmount || 0, paid: p.amount }))));
      events.sort((a, b) => a.date.localeCompare(b.date) || (a.repaid > 0) - (b.repaid > 0) || a.id.localeCompare(b.id));
      const openingPrincipal = money(events.filter(e => from && e.date < from).reduce((n, e) => n + e.received - e.repaid, 0));
      let balance = openingPrincipal;
      const ledgerRows = events.filter(e => !from || e.date >= from).map(e => {
        balance = money(balance + e.received - e.repaid); return { ...e, balance };
      });
      return { ...group, openingPrincipal, ledgerRows, periodReceived: money(ledgerRows.reduce((n, e) => n + e.received, 0)),
        periodRepaid: money(ledgerRows.reduce((n, e) => n + e.repaid, 0)), periodInterest: money(ledgerRows.reduce((n, e) => n + (e.interest || 0), 0)),
        periodPaid: money(ledgerRows.reduce((n, e) => n + (e.paid || 0), 0)) };
    }).sort((a, b) => a.name.localeCompare(b.name) || a.nature.localeCompare(b.nature));
}
function protectsMovement(s, id) {
  return (s.receipts || []).some(r => isLoan(r) && r.id === id) || (s.loanRepayments || []).some(p => p.id === id);
}
const reference = r => text(r.bankReference || r.reference || r.bankReconciliationEvidence && r.bankReconciliationEvidence.reference).toUpperCase();
function assertUniqueMovement(movements, entry, direction) {
  const ref = reference(entry);
  if (!ref) return;
  const match = movements.find(r => reference(r) === ref && (direction === 'credit' ? Number(r.credit) > 0 : Number(r.debit) > 0));
  if (match) fail('This bank reference is already recorded as ' + match.id + '. Link that existing ledger entry in reconciliation instead of recording it again.', 409);
}
function replay(s, body, candidate, collection) {
  const requestId = text(body.requestId);
  if (!requestId) return null;
  if (!/^[A-Za-z0-9_-]{8,100}$/.test(requestId)) fail('Invalid request identifier. Refresh the form and try again.');
  const previous = collection.find(r => r.requestId === requestId);
  if (!previous) return null;
  const fields = ['loanId', 'nature', 'account', 'amount', 'date', 'source', 'bankReference', 'note', 'historicalOpening'];
  if (!active(previous) || fields.some(k => text(previous[k]) !== text(candidate[k])) ||
      JSON.stringify(previous.loanTerms || {}) !== JSON.stringify(candidate.loanTerms || {}))
    fail('This submission identifier was already used for a different transaction. Refresh the form.', 409);
  return previous;
}
function prepareReceipt(s, body, base, movements) {
  const historicalOpening = body.historicalOpening === true;
  const candidate = Object.assign({}, base, { receiptType: TYPE, category: CATEGORY, amount: principal(base.amount, base.account),
    date: date(base.date), source: text(base.source), note: text(base.note), bankReference: historicalOpening ? '' : reference(base), historicalOpening });
  if (!candidate.source) fail('Enter the lender name.');
  if (!candidate.note) fail('Enter a note explaining the loan and repayment arrangement.');
  candidate.loanTerms = terms(body, candidate.date);
  if (!historicalOpening && !/cash/i.test(candidate.account) && !candidate.bankReference && !candidate.bankReconciliationEvidence)
    fail('Enter the bank transaction reference for this loan receipt.');
  const existing = replay(s, body, candidate, s.receipts || []);
  if (existing) return { existing };
  if (!historicalOpening) assertUniqueMovement(movements, candidate, 'credit');
  if (body.requestId) candidate.requestId = text(body.requestId);
  return { candidate };
}
function prepareRepayment(s, loan, body, base, movements) {
  if (!loan || !isLoan(loan) || !active(loan)) fail('Choose an active recorded loan.', 404);
  const candidate = Object.assign({}, base, { loanId: loan.id, nature: loan.nature, source: loan.source,
    amount: principal(base.amount, base.account), date: date(base.date), note: text(base.note), bankReference: reference(base) });
  if (!candidate.note) fail('Enter a repayment note for the audit trail.');
  if (candidate.date < loan.date) fail('A repayment cannot precede the loan receipt.');
  const existing = replay(s, body, candidate, s.loanRepayments || []);
  if (existing) return { existing };
  // Check both the historical and current position: backdating cannot overpay
  // the loan once later repayments are included.
  Object.assign(candidate, allocation(s, loan, candidate.amount));
  if (cents(candidate.principalAmount) > cents(position(s, loan, candidate.date).outstanding) ||
      cents(candidate.principalAmount) > cents(position(s, loan).outstanding)) fail('Repayment exceeds the outstanding loan principal.');
  if (!/cash/i.test(candidate.account) && !candidate.bankReference && !candidate.bankReconciliationEvidence)
    fail('Enter the bank transaction reference for this repayment.');
  assertUniqueMovement(movements, candidate, 'debit');
  if (body.requestId) candidate.requestId = text(body.requestId);
  return { candidate };
}
function addRepayment(s, candidate) {
  s.loanRepaymentSeq = Math.max(Number(s.loanRepaymentSeq) || 0,
    ...(s.loanRepayments || []).map(p => Number(String(p.id).replace(/^LOANPAY-/, '')) || 0)) + 1;
  const p = Object.assign({ id: 'LOANPAY-' + String(s.loanRepaymentSeq).padStart(5, '0') }, candidate);
  s.loanRepayments = s.loanRepayments || []; s.loanRepayments.push(p); return p;
}
module.exports = { TYPE, CATEGORY, isLoan, loans, repayments, catalog, position, lenderBooks, protectsMovement,
  amount, validDate, date, terms, schedule, schedulePosition, allocation, prepareReceipt, prepareRepayment, addRepayment };
