'use strict';

// This is deliberately a read-only predicate. Editing an expense's descriptive
// fields must not weaken the payment/reconciliation evidence used to freeze its
// amount and date. No role (including Owner) is an exemption from this lock.
const text = value => String(value == null ? '' : value).trim();
const list = value => Array.isArray(value) ? value : [];
const values = value => value && typeof value === 'object' ? Object.values(value) : [];
const positive = value => Number.isFinite(Number(value)) && Number(value) > 0;
const nonCashAccounts = new Set(['refund credit', 'vendor credit', 'store credit', 'vendor advance', 'voucher', 'coupon']);
const monetary = payment => !!payment && positive(payment.amount) && !payment.accountingExcluded &&
  !payment.historicalSettlement && !payment.creditOnly && !payment.refundCredit &&
  !nonCashAccounts.has(text(payment.account).toLowerCase());
const qualified = (expense, payment) => {
  const id = text(payment.id), expenseId = text(expense.id);
  return id && expenseId ? (id.startsWith(expenseId + '/') ? id : expenseId + '/' + id) : '';
};
const date = value => {
  const result = text(value).slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(result) && Number.isFinite(Date.parse(result + 'T00:00:00Z')) ? result : '';
};
const identifiers = payment => [payment.batchPaymentId, payment.batchId, payment.settledByTransferId, ...list(payment.batchPaymentIds)].map(text).filter(Boolean);
const refsFor = (expense, payment, reimbursement = false) => {
  const reference = qualified(expense, payment);
  const refs = new Set([reference, ...identifiers(payment)].filter(Boolean));
  // PAY-001 / REIM-001 are expense-local: matching their bare values would lock
  // unrelated expenses. Imported global/batch IDs are safe as raw references.
  if (/^(?:BPAY-|RB-|PPB-|ADV-|REC-)/.test(text(payment.id))) refs.add(text(payment.id));
  if (reimbursement && reference) refs.add(reference + '/RECEIVED');
  return refs;
};

function reconciliationEvidence(store, supplied) {
  const refs = new Map(), activeDrafts = store.bankReconciliationDrafts || {};
  const add = (id, reason) => { id = text(id); if (id && !refs.has(id)) refs.set(id, reason); };
  const addMany = (ids, reason) => list(ids).forEach(id => add(id, reason));
  if (supplied && typeof supplied[Symbol.iterator] === 'function') {
    for (const id of supplied) add(id, 'This expense has a payment linked to reconciliation.');
  }
  const ledgerRefs = (ledger, reason) => {
    if (!ledger || typeof ledger !== 'object') return;
    add(ledger.id, reason);
    addMany(ledger.sourceIds, reason);
    addMany(ledger.linkedEntryIds, reason);
    list(ledger.linkedEntries).forEach(entry => ledgerRefs(entry, reason));
  };
  values(store.bankStatements).forEach(book => list(book && book.imports).forEach(record => {
    const reason = 'This expense has a payment in a finalized reconciliation. Its amount and date cannot be changed.';
    [...list(record.reconciliationRows), ...list(record.carriedReconciliationRows)].forEach(row => {
      addMany(row && row.linkedRecordIds, reason);
      ledgerRefs(row && row.ledger, reason);
    });
    list(record.carryForwardMovements).forEach(movement => ledgerRefs(movement, reason));
  }));
  values(activeDrafts).forEach(draft => values(draft && draft.resolutions).forEach(resolution => {
    if (!resolution) return;
    const reason = 'This expense has a payment linked to an active reconciliation. Its amount and date cannot be changed.';
    add(resolution.appId, reason);
    addMany(resolution.appIds, reason);
  }));
  // Old undo workflows could leave provisional markers after their draft link
  // was removed. Such orphan markers in a still-active draft are not evidence.
  // Finalized/legacy overrides remain authoritative even without their draft.
  [store.bankDateOverrides, store.bankReconciliationLinks].forEach(collection => {
    Object.entries(collection || {}).forEach(([id, record]) => {
      if (!record || typeof record !== 'object') return;
      const draftId = text(record.reconciliationDraft || record.draftId);
      if (!draftId || !activeDrafts[draftId] || refs.has(text(id))) {
        add(id, 'This expense has a bank-reconciled payment. Its amount and date cannot be changed.');
      }
    });
  });
  return refs;
}

function expenseFinancialLock(store, expense, options = {}) {
  store = store || {};
  if (!expense || typeof expense !== 'object') return { locked: false, reason: '', references: [] };
  if (expense.gokwikPostingId && (store.gokwikPostings || {})[expense.gokwikPostingId]) return {locked:true,reason:'This expense is part of an Owner-approved GoKwik settlement. Its amount, date and payment evidence are locked.',references:list(expense.payments).map(p=>qualified(expense,p))};
  const evidence = reconciliationEvidence(store, options.blockedReferences), movements = [];
  const allExpenses = values(store.expenses), ownId = text(expense.id);
  const addMovement = (payment, reimbursement = false) => {
    if (!monetary(payment)) return;
    const refs = refsFor(expense, payment, reimbursement), batches = new Set(identifiers(payment));
    // A consolidated payment may be reconciled under its first allocation, its
    // batch ID, or a gross-payment advance reference. Every allocation belongs
    // to the same immutable monetary event.
    if (batches.size) {
      allExpenses.forEach(other => {
        [...list(other && other.payments), ...list(other && other.reimbursementPayments)].filter(monetary).forEach(peer => {
          if (identifiers(peer).some(id => batches.has(id))) refsFor(other, peer, list(other.reimbursementPayments).includes(peer)).forEach(id => refs.add(id));
        });
      });
      list(store.vendorAdvances).filter(advance => monetary(advance) && identifiers(advance).some(id => batches.has(id))).forEach(advance => {
        [advance.id, advance.paymentReference, ...identifiers(advance)].map(text).filter(Boolean).forEach(id => refs.add(id));
      });
    }
    const reference = qualified(expense, payment) || ownId;
    const direct = payment.bankReconciliationDraft || payment.creditCardStatementId;
    if (direct && reference) {
      refs.add(reference);
      evidence.set(reference, 'This expense has a statement-linked payment. Its amount and date cannot be changed.');
    }
    movements.push({ refs, reference, account: text(payment.account || (reimbursement ? '' : expense.account)), date: date(payment.date || payment.paidAt || expense.paidAt || expense.date) });
  };
  list(expense.payments).forEach(payment => addMovement(payment));
  list(expense.reimbursementPayments).forEach(payment => addMovement(payment, true));

  // Applying a cash/bank-funded vendor advance is still linked to that monetary
  // payment. Redeeming a refund coupon/credit is not a new monetary payment.
  list(expense.vendorAdvanceApplications).forEach(application => {
    if (!monetary(application)) return;
    const advance = list(store.vendorAdvances).find(item => text(item && item.id) === text(application.vendorAdvanceId));
    if (!monetary(advance)) return;
    const refs = new Set([advance.id, advance.paymentReference, ...identifiers(advance)].map(text).filter(Boolean));
    if (advance.bankReconciliationDraft && text(advance.id)) evidence.set(text(advance.id), 'This expense uses a statement-linked vendor advance. Its amount and date cannot be changed.');
    movements.push({ refs, reference: text(advance.paymentReference || advance.id), account: text(advance.account), date: date(advance.date) });
  });

  // Legacy expenses can have a recorded payment total but no payment rows. Do
  // not invent PAY-001, or confuse an all-credit redemption with a cash payout.
  if (!list(expense.payments).length && !list(expense.vendorAdvanceApplications).length && positive(expense.paidAmount)) {
    movements.push({ refs: new Set([ownId, ownId && ownId + '/LEGACY'].filter(Boolean)), reference: ownId,
      account: text(expense.account || expense.claimantFundingAccount), date: date(expense.paidAt || expense.date) });
  }
  // Older bank links sometimes name the whole expense, even after payment
  // arrays were introduced. This alias is valid only once a monetary source
  // exists, so a credit-only redemption is never mistaken for a bank payout.
  if (ownId) movements.forEach(movement => movement.refs.add(ownId));
  if (movements.length && expense.reconciliationSource && expense.reconciliationSource.draftId && ownId) {
    evidence.set(ownId, 'This expense was created from bank reconciliation. Its amount and date cannot be changed.');
    movements[0].refs.add(ownId);
  }
  const blocked = new Set(), reasons = [];
  movements.forEach(movement => {
    movement.refs.forEach(id => {
      if (evidence.has(id)) { blocked.add(id); reasons.push(evidence.get(id)); }
    });
    const closedCash = list(store.cashReconciliations).filter(record => record && record.status === 'approved' &&
      text(record.account).toLowerCase() === movement.account.toLowerCase() && movement.account &&
      date(record.verifiedThrough) && (!movement.date || movement.date <= date(record.verifiedThrough)))
      .sort((a, b) => text(b.verifiedThrough).localeCompare(text(a.verifiedThrough)))[0];
    if (closedCash) {
      if (movement.reference) blocked.add(movement.reference);
      reasons.push('This expense has a payment in an Owner-approved cash period through ' + date(closedCash.verifiedThrough) + '. Its amount and date cannot be changed.');
    }
  });
  return { locked: reasons.length > 0, reason: reasons[0] || '', references: [...blocked].sort() };
}

module.exports = { expenseFinancialLock };
