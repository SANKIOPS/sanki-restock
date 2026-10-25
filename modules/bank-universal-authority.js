'use strict';

// Bank cash is an independent fact. Mutable ordinary payments use their native
// source adapter; imported sales/provider/payroll documents remain evidence and
// receive one account-scoped cash representation, never a fabricated fee/entry.
const cardAuthority = require('./bank-card-authority');
const cashAuthority = require('./bank-movement-authority');
const own = (object, key) => Object.prototype.hasOwnProperty.call(object || {}, key);
const list = value => Array.isArray(value) ? value : [];
const copy = value => JSON.parse(JSON.stringify(value));
const text = value => String(value == null ? '' : value).trim();
function fail(message, status = 409) { const error = new Error(message); error.status = status; throw error; }
function frozen(value) { if (value && typeof value === 'object') { Object.values(value).forEach(frozen); Object.freeze(value); } return value; }
function safePath(path) { if (!Array.isArray(path) || !path.length || path.length > 12 || path.some(key => !['string', 'number'].includes(typeof key) || ['__proto__', 'constructor', 'prototype'].includes(String(key)) || typeof key === 'number' && (!Number.isSafeInteger(key) || key < 0))) fail('The linked correction path is invalid.'); }
function read(object, path) { safePath(path); return path.reduce((value, key) => value && own(value, key) ? value[key] : undefined, object); }
function write(object, path, value) { safePath(path); const parent = read(object, path.slice(0, -1).length ? path.slice(0, -1) : [path[0]]); if (path.length === 1) object[path[0]] = value; else { if (!parent || typeof parent !== 'object') fail('A linked correction target disappeared.'); parent[path.at(-1)] = value; } }
function normalized(row) {
  return { id: text(row.id), date: text(row.originalDate || row.date), debit: Number(row.debit || 0), credit: Number(row.credit || 0), entity: text(row.entity), description: text(row.description), reference: text(row.reference), proof: text(row.proof), sourceIds: [...new Set([...list(row.sourceIds), ...list(row.linkedEntryIds), ...list(row.linkedEntries).map(item => item.id)].map(text).filter(Boolean))].sort() };
}
function sourceLabel(row, transfer) {
  if (transfer && transfer.classification === 'credit_card_payment') return 'Credit-card payment';
  if (transfer) return transfer.classification === 'money_exchange' ? 'Money exchange' : 'Linked transfer';
  if (/^ADV-/.test(row.id)) return 'Salary advance';
  if (/^SAL|^INCP-/.test(row.id)) return 'Salary or incentive payment';
  if (/^RB-/.test(row.id) || /\/REIM-/.test(row.id)) return 'Reimbursement payment';
  if (/^PO-|^PPB-|^VADV-/.test(row.id)) return 'Purchase or vendor payment';
  if (/^LOANPAY-|^PF-/.test(row.id)) return 'Loan or funding movement';
  if (/^RCV-/.test(row.id)) return 'Receivable collection';
  if (/^SRF-|^RF-/.test(row.id)) return 'Refund movement';
  if (/PTM|PAYTM|GKW|VELOCITY|VEL/i.test(row.id)) return 'Settlement cash movement';
  if (/SHOPIFY|^SALE\//.test(row.id)) return 'Sales receipt';
  return 'Bank cash movement';
}
function sourceFamily(store, movement, transfer) {
  if (transfer) return 'transfer';
  if (list(store.vendorAdvances).some(row => [text(row.id), text(row.paymentReference)].includes(text(movement.id)))) return 'vendor_advance';
  const ids = new Set([text(movement.id), ...normalized(movement).sourceIds]);
  for (const expense of Object.values(store.expenses || {})) {
    if (list(expense.payments).some(row => ids.has(expense.id + '/' + row.id))) return 'expense_payment';
    if (list(expense.reimbursementPayments).some(row => ids.has(expense.id + '/' + row.id) || ids.has(expense.id + '/' + row.id + '/RECEIVED') || row.batchId && ids.has(row.batchId))) return 'reimbursement';
  }
  return 'bank_movement';
}

module.exports = function createUniversal(base) {
  const { cents } = base;
  const fingerprint = value => base.fingerprint(value === undefined ? null : value);
  const idFor = body => 'BAA-' + fingerprint(body).slice(0, 32);
  function exactMovement(store, draft, supplied, options) {
    if (typeof options.resolveMovement !== 'function') fail('Refresh this account before reviewing its linked source.');
    const found = options.resolveMovement(store, draft, supplied.id);
    const matches = Array.isArray(found) ? found : found ? [found] : [];
    if (matches.length !== 1 || fingerprint(normalized(matches[0])) !== fingerprint(normalized(supplied))) fail('The selected bank movement changed or is not unique. Refresh its candidates.');
    return matches[0];
  }
  function unclaimed(store, draft, movement, linkedIds = []) {
    const physical = value => text(value).replace(/\s+/g, ' ').toLowerCase();
    const account = physical(draft.account), bookAccount = (key, book) => text(book.account || key.split('|').at(-1));
    const books = Object.entries(store.bankStatements || {}), accountForOrigin = value => {
      if (!value) return '';
      const active = (store.bankReconciliationDrafts || {})[value]; if (active) return active.account;
      for (const [key, book] of books) if (list(book.imports).some(item => item.id === value) || Object.values(book.transactions || {}).some(item => item.id === value)) return bookAccount(key, book);
      return '';
    };
    const otherEvidence = record => {
      if (!record) return false;
      return [record.reconciliationDraft, record.bankReconciliationDraft, record.bankTransactionId, record.bankStatementEvidence && (record.bankStatementEvidence.bankTransactionId || record.bankStatementEvidence.reconciliationDraft || record.bankStatementEvidence.importId)].some(value => accountForOrigin(value) && physical(accountForOrigin(value)) !== account);
    };
    const clean = record => {
      const next = { ...record };
      for (const field of ['reconciliationDraft', 'bankReconciliationDraft', 'bankTransactionId']) if (accountForOrigin(next[field]) && physical(accountForOrigin(next[field])) !== account) delete next[field];
      const evidence = next.bankStatementEvidence;
      if (evidence && otherEvidence({ bankStatementEvidence: evidence })) delete next.bankStatementEvidence;
      return next;
    };
    const scoped = { ...store,
      bankStatements: Object.fromEntries(books.filter(([key, book]) => physical(bookAccount(key, book)) === account)),
      bankReconciliationDrafts: Object.fromEntries(Object.entries(store.bankReconciliationDrafts || {}).filter(([, value]) => physical(value.account) === account)) };
    for (const field of ['bankDateOverrides', 'bankReconciliationLinks']) scoped[field] = Object.fromEntries(Object.entries(store[field] || {}).filter(([, value]) => !otherEvidence(value)));
    const ids = new Set([movement.id, ...normalized(movement).sourceIds, ...linkedIds].map(text));
    const records = [];
    for (const collection of ['transfers', 'receipts', 'adjustments', 'vendorAdvances', 'salesRefunds', 'loanRepayments', 'bankTruthMovements', 'paytmSettlements', 'paytmPayoutPostings']) for (const record of list(store[collection])) if (ids.has(text(record.id)) || ids.has(text(record.paymentReference))) records.push(record);
    for (const expense of Object.values(store.expenses || {})) for (const payment of [...list(expense.payments), ...list(expense.reimbursementPayments)]) if (ids.has(expense.id + '/' + payment.id) || payment.batchId && ids.has(payment.batchId)) records.push(expense, payment);
    // Fee records are an exact tenant-linked view of their retained bank row.
    // The universal fallback must not detach cash from that fee or bypass its
    // dedicated review/undo guard. Validation rebuilds this check as well.
    if (movement.rentalFeeId || records.some(record => record.rentalFeeId) || Object.values(store.expenses || {}).some(expense => expense.rentalFeeId && ids.has(text(expense.id)))) fail('Tenant fee payments and receipts must be corrected through their original tenant fee reconciliation review.');
    for (const id of ids) base.assertUnclaimed(scoped, { type: 'bank_movement', record: {} }, id, text(draft.id));
    for (const record of records) base.assertUnclaimed(scoped, { type: 'bank_movement', record: clean(record) }, text(movement.id), text(draft.id));
    if ((store.bankMovementAuthority || {})[cashAuthority.key(draft.account, draft.nature, movement.id)]) fail('This bank movement already has authoritative finalized evidence. Review its existing reconciliation instead of correcting it again.');
    const otherAccountEvidence = books.filter(([key, book]) => physical(bookAccount(key, book)) !== account).some(([, book]) => list(book.imports).some(record => [...list(record.reconciliationRows), ...list(record.carriedReconciliationRows), ...list(record.carryForwardMovements)].some(row => [...list(row.linkedRecordIds), row.ledger && row.ledger.id].some(id => ids.has(text(id)))))) || Object.values(store.bankReconciliationDrafts || {}).some(value => physical(value.account) !== account && Object.values(value.resolutions || {}).some(row => [row.appId, ...list(row.appIds)].some(id => ids.has(text(id))))) || records.some(otherEvidence);
    return { otherAccountEvidence };
  }
  function bankPlan(store, draft, bank, supplied, options) {
    if (!draft || !text(draft.id) || !supplied || !text(supplied.id)) fail('A verified bank row, draft and source movement are required.', 400);
    const mode = options.mode || 'auto', rowId = text(options.bankRowId), match = rowId.match(/^bank-(\d+)$/);
    if (!['auto', 'manual'].includes(mode) || !match || !list(draft.transactions)[Number(match[1])] || fingerprint(draft.transactions[Number(match[1])]) !== fingerprint(bank)) fail('The selected bank evidence changed. Refresh the reconciliation.');
    const bankSide = base.side(bank), sourceSide = base.side(supplied), difference = bankSide.amount - sourceSide.amount;
    if (bankSide.direction !== sourceSide.direction) fail('Bank and ledger movements must have the same money direction.', 400);
    if (mode === 'auto' && Math.abs(difference) > 100) fail('Automatic bank amount authority is limited to an inclusive ₹1.00 difference. Review this match manually.');
    base.date(bank.date);
    if (bank.reversal || /(?:REVERSED\s*:|\bREVERSAL\b|^RVS\b)/i.test(text(bank.description))) fail('Failed payments and reversals require their exact reversal workflow.');
    const movement = exactMovement(store, draft, supplied, options);
    if (movement.pendingPosting) fail('This report-backed movement has not posted yet. Finish its verified report allocation first; no source or bank evidence was changed.');
    const claims = unclaimed(store, draft, movement);
    const transfer = list(store.transfers).find(row => row.id === movement.id);
    const externalStores = options.externalStores || {};
    let native, retainedCardEvidence = false;
    if (!claims.otherAccountEvidence && transfer && (transfer.classification === 'credit_card_payment' || transfer.creditCardPaymentId)) {
      try {
        native = cardAuthority.build(store, { source: transfer, movement, draft, bank, bankAmount: bankSide.amount / 100, bankDate: bank.date, delta: difference / 100, allocations: options.amountAuthorityAllocations, externalStores, cents, fingerprint });
      } catch (error) {
        if (error.code === 'CARD_ALLOCATION_REVIEW_REQUIRED') {
          const payment = list(externalStores.creditCards && externalStores.creditCards.payments).find(row => row.id === transfer.creditCardPaymentId);
          error.allocationRequired = true; error.allocationChoices = list(payment && payment.allocations).map(row => ({ nature: row.nature, amount: row.amount })); error.bankAmount = bankSide.amount / 100; error.bankDate = bank.date; error.sourceLabel = 'Credit-card payment';
          throw error;
        }
        if (error.code !== 'CARD_STATEMENT_LOCKED') throw error;
        retainedCardEvidence = true;
      }
    }
    if (native) unclaimed(store, draft, movement, native.linkedIds);
    const label = sourceLabel(movement, transfer), sourceNature = text(native && native.sourceNature || movement.entity || draft.nature || 'SANKI').toUpperCase();
    const sourceEffects = native ? 'The bank transfer, linked card payment and ownership allocations change together. Card purchase expenses stay unchanged.' : retainedCardEvidence ? 'The actual cash movement for this bank changes. The finalized card statement, card payment evidence and ownership allocations stay unchanged; any card-side difference remains explicit.' : 'The actual cash movement for this bank changes. Original sale, invoice, provider report, earned payroll and other-account evidence stay unchanged. This is not a new payment, expense or bank-charge entry.';
    const before = native ? native.beforeSummary : { amount: sourceSide.amount / 100, date: normalized(movement).date };
    const after = native ? native.afterSummary : { amount: bankSide.amount / 100, date: bank.date };
    const body = copy({ version: 2, mode, draftId: text(draft.id), bankRowId: rowId, nature: text(draft.nature || 'SANKI').toUpperCase(), sourceNature, sourceFamily: sourceFamily(store,movement,transfer), account: text(draft.account), movementId: text(movement.id), sourceType: native ? native.sourceType : 'bank_movement', sourceId: text(movement.id), paymentId: null, bankFingerprint: fingerprint(bank), bankSnapshot: bank, sourceFingerprint: fingerprint(normalized(movement)), sourceSnapshot: normalized(movement), originalAmount: sourceSide.amount / 100, bankAmount: bankSide.amount / 100, amountDifference: difference / 100, direction: bankSide.direction, before, after, sourceLabel: label, sourceEffects, effect: native ? 'linked_sources' : 'bank_cash_only', retainedCardEvidence, allocations: options.amountAuthorityAllocations || null, patches: native ? native.patches : [], guards: native ? native.guards : retainedCardEvidence ? [{ store: 'creditCards', path: ['statements'], fingerprint: fingerprint(externalStores.creditCards && externalStores.creditCards.statements || {}) }] : [], linkedIds: native ? native.linkedIds : normalized(movement).sourceIds });
    return frozen({ ...body, id: idFor(body) });
  }
  function plan(store, draft, bank, movement, options = {}) {
    if (!options.universal) return base.plan(store, draft, bank, movement, options);
    // Keep existing exact source updates for ordinary expenses and transfers.
    // Other families receive a linked adapter or an explicit bank-cash fact.
    try { return base.plan(store, draft, bank, movement, options); } catch (ordinaryError) {
      return bankPlan(store, draft, bank, movement, options);
    }
  }
  function validate(store, candidate, options = {}) {
    if (!candidate || candidate.version !== 2) return base.validate(store, candidate, options);
    const body = { ...candidate }; delete body.id;
    if (idFor(body) !== candidate.id) fail('The bank correction plan changed. Review it again.');
    const applied = own(store.bankAmountAuthorityApplications, candidate.id) && store.bankAmountAuthorityApplications[candidate.id];
    if (applied) { if (applied.planFingerprint !== fingerprint(candidate)) fail('The correction audit does not match its reviewed plan.'); return { alreadyApplied: true, plan: candidate }; }
    const draft = store.bankReconciliationDrafts && store.bankReconciliationDrafts[candidate.draftId], match = text(candidate.bankRowId).match(/^bank-(\d+)$/), bank = draft && match && list(draft.transactions)[Number(match[1])];
    if (!draft || !bank || draft.account !== candidate.account || text(draft.nature || 'SANKI').toUpperCase() !== candidate.nature || fingerprint(bank) !== candidate.bankFingerprint) fail('The bank evidence changed after review. Nothing was corrected.');
    const movement = candidate.sourceSnapshot;
    const rebuilt = bankPlan(store, draft, bank, movement, { ...options, universal: true, mode: candidate.mode, bankRowId: candidate.bankRowId, amountAuthorityAllocations: candidate.allocations });
    if (fingerprint(rebuilt) !== fingerprint(candidate)) fail('A linked source or allocation changed after review. Nothing was corrected.');
    return { alreadyApplied: false, plan: candidate };
  }
  function applyAll(store, candidates, options = {}) {
    if (!Array.isArray(candidates)) fail('Supply the reviewed correction plans.', 400);
    const ids = new Set(); for (const item of candidates) { if (!item) fail('A reviewed correction plan is required.'); for (const id of new Set([item.movementId, ...list(item.sourceSnapshot && item.sourceSnapshot.sourceIds), ...list(item.linkedIds)])) { const key = text(item.account) + '/' + text(id); if (ids.has(key)) fail('Overlapping movements or allocations cannot be corrected twice in one finalization.'); ids.add(key); } }
    const checked = candidates.map(item => validate(store, item, options));
    if (!candidates.some(item => item.version === 2)) return base.applyAll(store, candidates, options);
    const working = copy(store), external = copy(options.externalStores || {}), ordinary = base.applyAll(working, candidates.filter(item => item.version !== 2), options), audits = [...ordinary.audits], touched = new Set();
    const pending = checked.filter(item => !item.alreadyApplied && item.plan.version === 2).map(item => item.plan);
    // Every linked target is checked before ANY real object/file is changed.
    for (const item of pending) for (const patch of item.patches) {
      if (!['expenses', 'creditCards'].includes(patch.store)) fail('The linked correction store is invalid.');
      const root = patch.store === 'expenses' ? working : external[patch.store], pathKey = patch.store + '/' + JSON.stringify(patch.path);
      if (touched.has(pathKey) || !root || fingerprint(read(root, patch.path)) !== fingerprint(patch.before)) fail('Linked correction targets overlap or changed after review. Nothing was corrected.');
      touched.add(pathKey);
    }
    for (const item of pending) {
      for (const patch of item.patches) write(patch.store === 'expenses' ? working : external[patch.store], patch.path, copy(patch.after));
      if (item.effect === 'bank_cash_only') {
        working.bankMovementAuthority = working.bankMovementAuthority || {};
        working.bankMovementAuthority[cashAuthority.key(item.account, item.nature, item.movementId)] = { movementId: item.movementId, account: item.account, nature: item.nature, sourceNature: item.sourceNature, sourceFamily: item.sourceFamily, original: { debit: item.sourceSnapshot.debit, credit: item.sourceSnapshot.credit, date: item.sourceSnapshot.date }, after: { debit: item.direction === 'debit' ? item.bankAmount : 0, credit: item.direction === 'credit' ? item.bankAmount : 0, date: item.after.date }, sourceIds: item.sourceSnapshot.sourceIds, description: item.sourceSnapshot.description, reference: text(item.bankSnapshot.reference), entity: item.sourceSnapshot.entity, planId: item.id, effect: item.effect, sourceLabel: item.sourceLabel, sourceEffects: item.sourceEffects, ...(options.at ? { at: options.at } : {}) };
      }
      const event = { planId: item.id, draftId: item.draftId, bankRowId: item.bankRowId, movementId: item.movementId, sourceType: item.sourceType, sourceId: item.sourceId, sourceNature: item.sourceNature, mode: item.mode, originalAmount: item.originalAmount, bankAmount: item.bankAmount, amountDifference: item.amountDifference, bankDate: item.after.date, bankReference: text(item.bankSnapshot.reference), before: copy(item.before), after: copy(item.after), effect: item.effect, sourceEffects: item.sourceEffects, linkedCount: item.patches.length };
      audits.push(event); working.bankAmountAuthorityApplications = working.bankAmountAuthorityApplications || {}; working.bankAmountAuthorityApplications[item.id] = { version: 2, planFingerprint: fingerprint(item), draftId: item.draftId, movementId: item.movementId, bankRowId: item.bankRowId, before: event.before, after: event.after, effect: item.effect };
    }
    const externalStoreChanges = Object.keys(external).filter(key => fingerprint(external[key]) !== fingerprint((options.externalStores || {})[key])).map(key => ({ store: key, before: options.externalStoreText && options.externalStoreText[key], after: JSON.stringify(external[key]) }));
    for (const key of new Set([...Object.keys(store), ...Object.keys(working)])) if (fingerprint(store[key]) !== fingerprint(working[key])) store[key] = working[key];
    return { applied: ordinary.applied + pending.length, alreadyApplied: checked.filter(item => item.alreadyApplied).length, audits, plans: candidates, externalStoreChanges };
  }
  return { plan, validate, applyAll };
};
