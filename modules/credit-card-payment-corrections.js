'use strict';

// Correct one existing settlement graph, never create a payment, relink a
// statement, or change the bank evidence used to approve a reconciliation.
const crypto = require('crypto');
const NATURES = ['SANKI', 'SAMAST', 'PERSONAL'];
const list = value => Array.isArray(value) ? value : [];
const values = value => Array.isArray(value) ? value : Object.values(value || {});
const text = value => String(value == null ? '' : value).trim();
const clone = value => JSON.parse(JSON.stringify(value));
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const fail = (message, status = 409) => { const error = new Error(message); error.status = status; throw error; };
function cents(value) {
  if (!['string', 'number'].includes(typeof value) || !/^\d+(?:\.\d{1,2})?$/.test(String(value))) return null;
  const [whole, fraction = ''] = String(value).split('.'), result = Number(whole) * 100 + Number(fraction.padEnd(2, '0'));
  return Number.isSafeInteger(result) && result > 0 ? result : null;
}
function validDate(value) { return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value + 'T00:00:00Z')) && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value; }
const canonical = value => text(value).toLowerCase().replace(/\s+/g, ' ');
const cardName = card => text(card.name) + ' ' + text(card.last4);
function contains(value, ids) {
  if (typeof value === 'string') return ids.has(value) || value.split(/\s*,\s*/).some(item => ids.has(item) || item.endsWith('/RECEIVED') && ids.has(item.replace(/\/RECEIVED$/, '')));
  if (Array.isArray(value)) return value.some(item => contains(item, ids));
  return !!value && typeof value === 'object' && Object.entries(value).some(([key, item]) => ids.has(key) || contains(item, ids));
}
function active(record) { return !!record && record.active !== false && !record.accountingExcluded && !record.deletedAt && !record.voidedAt && !record.reversedAt && !record.supersededBy && !['void', 'voided', 'deleted', 'reversed', 'cancelled', 'canceled'].includes(record.status); }
function assertStatementEvidence(cards, cardId, records) {
  const reference = value => text(value).toLowerCase().replace(/[^a-z0-9]/g, '');
  const refs = new Set(records.flatMap(record => [record.reference, record.bankReference, record.utr]).map(reference).filter(value => value.length >= 5));
  for (const statement of values(cards.statements)) {
    if (statement.cardId !== cardId || !(statement.status === 'finalized' || statement.finalizedAt)) continue;
    for (const row of list(statement.rows)) {
      const isPayment = row.classification === 'card_payment' || Number(row.credit) > 0 && /payment|autopay|thank\s*you/i.test(text(row.narration));
      if (!isPayment) continue;
      const rowDates = [row.date, row.postingDate].filter(validDate);
      if ([row.reference, row.settlementReference].some(value => refs.has(reference(value))) || records.some(record => cents(record.amount) !== null && cents(row.amount || row.credit) === cents(record.amount) && rowDates.some(date => Math.abs((Date.parse(date) - Date.parse(record.date)) / 86400000) <= 3))) fail('A finalized card statement contains indistinguishable payment evidence. Its printed figures and existing reconciliation must remain unchanged.');
    }
  }
}
function create(deps) {
  const tokenSecret = crypto.randomBytes(32);
  function policy() { const result = typeof deps.policy === 'function' ? deps.policy() : deps; if (!result || typeof result.read !== 'function') fail('The audited correction service is unavailable. No records were changed.'); return result; }
  function canAccess(req, card, p) {
    return !!card && p.canEdit(req) && (typeof p.canAccessCard === 'function'
      ? p.canAccessCard(req, card) === true : !card.ownerOnly || p.isOwner(req));
  }
  function cardFor(snapshot, req, cardId, p) {
    if (!p.canEdit(req)) fail('Manage transactions access is required for corrections.', 403);
    const cards = snapshot.cards, matches = values(cards && cards.cards).filter(card => card.id === cardId);
    if (matches.length !== 1 || !canAccess(req, matches[0], p)) fail('This card is unavailable or inaccessible.', 404);
    return matches[0];
  }
  function visibleScopes(req, scopes, p) {
    for (const scope of scopes) if (!NATURES.includes(scope.nature) || !scope.account || !p.natures(req).includes(scope.nature) || scope.nature === 'PERSONAL' && !p.canPersonal(req) || !p.accountVisible(req, scope.account) || p.personalAccount && p.personalAccount(scope.account, scope.nature) && !p.canPersonal(req)) fail('The saved payment or one of its ownership accounts is inaccessible.', 403);
  }
  function select(snapshot, req, cardId, sourceId, p) {
    const card = cardFor(snapshot, req, cardId, p), cards = snapshot.cards, store = snapshot.store, parts = String(sourceId || '').split(':');
    if (!Array.isArray(store.transfers) || cards.payments != null && !Array.isArray(cards.payments) || store.creditCardSettlementAllocations != null && !Array.isArray(store.creditCardSettlementAllocations) || cards.statements != null && (typeof cards.statements !== 'object' || Array.isArray(cards.statements))) fail('The saved payment graph has an invalid structure. No correction is permitted.');
    for (const [container, key] of [[store, 'auditLog'], [cards, 'audit']]) if (container[key] != null && !Array.isArray(container[key]) || container.auditSeq != null && (!Number.isSafeInteger(container.auditSeq) || container.auditSeq < 0) || container.auditSeq == null && list(container[key]).length) fail('The existing audit history or sequence cannot be verified. Nothing was changed.');
    if (parts.length !== 2 || !['payment', 'transfer'].includes(parts[0]) || !parts[1]) fail('Choose one exact existing card payment or legacy transfer.', 400);
    let payment = null, transfer = null;
    if (parts[0] === 'payment') {
      const matches = list(cards.payments).filter(item => item.id === parts[1]);
      if (matches.length !== 1 || matches[0].cardId !== card.id) fail('The card payment identity is missing or ambiguous.');
      payment = matches[0];
      const matchesTransfer = list(store.transfers).filter(item => item.creditCardPaymentId === payment.id || payment.transferId && item.id === payment.transferId || payment.creditCardTransferId && item.id === payment.creditCardTransferId);
      if (matchesTransfer.length !== 1) fail('The payment must have exactly one linked bank transfer before it can be corrected.');
      transfer = matchesTransfer[0];
    } else {
      const matches = list(store.transfers).filter(item => item.id === parts[1]);
      if (matches.length !== 1) fail('The legacy transfer identity is missing or ambiguous.');
      transfer = matches[0];
      if (transfer.creditCardPaymentId || list(cards.payments).some(item => item.transferId === transfer.id || item.creditCardTransferId === transfer.id)) fail('This transfer belongs to a card-payment record. Correct its complete payment graph instead.');
    }
    const nature = text(transfer.fromNature || transfer.nature), receivingNature = text(transfer.toNature || transfer.nature), account = text(transfer.fromAccount), receivingAccount = text(transfer.toAccount);
    const scopes = [{ nature, account }, { nature: receivingNature, account: receivingAccount }];
    visibleScopes(req, scopes, p);
    if (transfer.reportingNature) visibleScopes(req, [{ nature: transfer.reportingNature, account }], p);
    if (list(store.transfers).filter(item => item.id === transfer.id).length !== 1 || !transfer.id) fail('The linked transfer identity is ambiguous.');
    if (!active(transfer) || payment && !active(payment) || card.active === false) fail('Inactive, excluded, reversed or superseded payments cannot be corrected.');
    if (transfer.classification !== 'credit_card_payment') fail('This is not a dedicated credit-card settlement.');
    const matchingCards = values(cards.cards).filter(item => canonical(cardName(item)) === canonical(receivingAccount));
    if (matchingCards.length !== 1 || matchingCards[0].id !== card.id || transfer.creditCardId && transfer.creditCardId !== card.id) fail('The receiving card identity is missing, ambiguous or inconsistent.');
    if (receivingNature !== nature || transfer.nature && transfer.nature !== nature) fail('The saved settlement entity fields do not agree. Resolve its source identity first.');
    if (!validDate(transfer.date) || cents(transfer.amount) === null) fail('The saved transfer date or exact amount is invalid.');
    if (!payment && list(cards.payments).some(item => item.cardId === card.id && active(item) && (item.date === transfer.date && cents(item.amount) === cents(transfer.amount) || text(transfer.reference || transfer.bankReference) && canonical(item.reference) === canonical(transfer.reference || transfer.bankReference)))) fail('A card-payment record already has indistinguishable legacy transfer evidence. Resolve the source identity before correcting either record.');
    let allocations = [];
    if (payment) {
      visibleScopes(req, [{ nature: payment.nature, account: payment.account }], p);
      if (transfer.creditCardPaymentId !== payment.id || payment.cardId !== card.id || payment.account !== account || payment.nature !== nature || payment.date !== transfer.date || cents(payment.amount) !== cents(transfer.amount) || cents(payment.amount) === null) fail('The card payment and bank transfer disagree. No partial correction is permitted.');
      if (payment.reference != null && transfer.reference != null && payment.reference !== transfer.reference) fail('The card payment and transfer references disagree.');
      if (payment.transferId && payment.transferId !== transfer.id || payment.creditCardTransferId && payment.creditCardTransferId !== transfer.id) fail('The stored payment transfer identity disagrees.');
      const splits = list(payment.allocations), seen = new Set();
      if (!splits.length || splits.some(item => !NATURES.includes(item.nature) || cents(item.amount) === null || seen.has(item.nature) || !seen.add(item.nature)) || splits.reduce((total, item) => total + cents(item.amount), 0) !== cents(payment.amount)) fail('The payment ownership allocations are missing or inconsistent.');
      for (const item of splits) visibleScopes(req, [{ nature: item.nature, account }], p);
      allocations = list(store.creditCardSettlementAllocations).filter(item => item.creditCardPaymentId === payment.id);
      if (allocations.length !== splits.length) fail('The linked ownership allocation graph is incomplete or ambiguous.');
      for (const item of splits) {
        const matches = allocations.filter(allocation => allocation.beneficiaryNature === item.nature), allocation = matches[0];
        if (matches.length !== 1 || !allocation.id || list(store.creditCardSettlementAllocations).filter(row => row.id === allocation.id).length !== 1 || allocation.cardId !== card.id || allocation.cardName !== cardName(card) || allocation.payerNature !== nature || allocation.payingAccount !== account || allocation.date !== payment.date || cents(allocation.amount) !== cents(item.amount) || !active(allocation) || allocation.status !== (item.nature === nature ? 'self' : 'due')) fail('The linked ownership allocation no longer matches this payment.');
        if (allocation.settledAt || allocation.settledBy || allocation.settledByTransferId || allocation.paidAmount || allocation.settledAmount || allocation.reimbursedAmount || allocation.settlementId || list(allocation.payments).length || list(allocation.settlements).length || list(allocation.linkedEntries).length || allocation.statementId || allocation.rowId) fail('A settled or statement-linked ownership allocation cannot be corrected here.');
      }
    } else if (list(store.creditCardSettlementAllocations).some(item => item.transferId === transfer.id || item.linkedTransferId === transfer.id || item.settlementSourceId === sourceId)) fail('This legacy transfer has linked ownership allocations; no independent edit is permitted.');
    const ids = [sourceId, transfer.id, 'transfer:' + transfer.id, ...(payment ? [payment.id, 'payment:' + payment.id] : []), ...allocations.map(item => item.id)];
    if (contains(cards.statements, new Set(ids))) fail('This payment is already linked to a card-statement review or finalized statement. Its evidence and reconciliation must remain unchanged.');
    const records = [transfer, ...(payment ? [payment] : []), ...allocations];
    const forbidden = ['creditCardStatementId', 'statementId', 'statementRowId', 'bankReconciliationDraft', 'bankReconciliationEvidence', 'reconciliationDraft', 'bankMovementAuthority', 'historicalSettlement', 'provisional', 'batchId', 'batchPaymentId', 'salaryAdvanceId', 'expenseRefundId', 'loanId', 'loanRepaymentId', 'routedThroughIntermediary', 'convertedFromExpense', 'gokwikPostingId', 'paytmSettlementId', 'settledThroughPaytm', 'openingBalanceTreatment', 'reconciled', 'reconciledAt', 'reconciliationId', 'finalizedAt', 'locked', 'bankTransactionId', 'bankStatementEvidence', 'settledByTransferId', 'settlementId', 'confirmedBankSplit'];
    if (records.some(record => forbidden.some(field => !!record[field]) || ['payments', 'settlements', 'sourceIds', 'linkedEntries'].some(field => list(record[field]).length))) fail('This settlement has protected source, historical or reconciliation links. Resolve that source workflow first.');
    assertStatementEvidence(cards, card.id, records);
    const otherStore = { ...store, transfers: list(store.transfers).filter(item => item !== transfer), creditCardSettlementAllocations: list(store.creditCardSettlementAllocations).filter(item => !allocations.includes(item)) };
    delete otherStore.auditLog; delete otherStore.audit;
    const otherCards = { ...cards, payments: list(cards.payments).filter(item => item !== payment) }; delete otherCards.audit;
    if (contains(otherStore, new Set(ids)) || contains(otherCards, new Set(ids))) fail('Another accounting record depends on this payment. No isolated correction is permitted.');
    return { sourceId, card, payment, transfer, allocations, scopes, ids, records };
  }
  function check(snapshot, selected, date, p) {
    const external = Object.fromEntries(Object.entries(snapshot.external || {}).filter(([key]) => !/(?:^|[\\/])credit-cards\.json$/i.test(key) && key !== 'cards'));
    const assertUnlocked = p.assertUnlocked || deps.assertUnlocked;
    if (typeof assertUnlocked !== 'function') fail('The reconciliation protection service is unavailable.');
    assertUnlocked(snapshot.store, { ids: selected.ids, scopes: selected.scopes, dates: [...new Set([selected.transfer.date, date].filter(Boolean))], records: selected.records, external, bookKey: p.bookKey });
    // A transfer-only source is never converted into a fabricated CCP record.
  }
  function graph(selected) { return clone({ payment: selected.payment, transfer: selected.transfer, allocations: selected.allocations }); }
  function fields(selected) { const source = selected.payment || selected.transfer; return { amount: source.amount, date: source.date, reference: source.reference == null ? text(selected.transfer.reference) : source.reference, note: source.note == null ? text(selected.transfer.note) : source.note }; }
  function proposal(snapshot, req, cardId, sourceId, body, p) {
    if (!body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).some(key => !['changes', 'reason', 'fingerprint'].includes(key))) fail('Only correction fields and their review reason are accepted.', 400);
    const reason = typeof body.reason === 'string' ? body.reason.trim() : '';
    if (reason.length < 10 || reason.length > 2000) fail('Explain the evidence for this correction in 10 to 2,000 characters.', 400);
    const changes = body.changes;
    if (!changes || typeof changes !== 'object' || Array.isArray(changes) || !Object.keys(changes).length || Object.keys(changes).some(key => !['amount', 'date', 'reference', 'note'].includes(key))) fail('Only amount, date, reference and note can be corrected. Accounts, identities and allocations are protected.', 400);
    const selected = select(snapshot, req, cardId, sourceId, p), normalized = {};
    if (own(changes, 'amount')) { const value = cents(changes.amount); if (value === null) fail('Enter a positive amount with no more than two decimal places.', 400); normalized.amount = value / 100; }
    if (own(changes, 'date')) { if (!validDate(changes.date)) fail('Enter a valid date as YYYY-MM-DD.', 400); normalized.date = changes.date; }
    for (const field of ['reference', 'note']) if (own(changes, field)) { if (typeof changes[field] !== 'string' || changes[field].length > (field === 'reference' ? 300 : 4000)) fail('The correction text is invalid or too long.', 400); normalized[field] = changes[field]; }
    const before = graph(selected), old = fields(selected);
    for (const field of Object.keys(normalized)) if (normalized[field] === old[field]) delete normalized[field];
    if (own(normalized, 'amount') && selected.payment && selected.payment.allocations.length !== 1) fail('This payment has multiple ownership allocations. Its amount cannot be corrected without a separately reviewed allocation correction.');
    if (own(normalized, 'reference') && selected.records.some(record => record.bankReference || record.utr)) fail('This source has a separately retained bank reference. Correct its reference through the evidence workflow, not this payment editor.');
    check(snapshot, selected, normalized.date || selected.transfer.date, p);
    const after = clone(before), targets = [after.transfer, ...(after.payment ? [after.payment] : [])];
    for (const target of targets) Object.assign(target, normalized);
    if (own(normalized, 'amount') && after.payment) { after.payment.allocations[0].amount = normalized.amount; after.allocations[0].amount = normalized.amount; }
    if (own(normalized, 'date')) after.allocations.forEach(item => { item.date = normalized.date; });
    // Prevent a correction from turning an existing source into a duplicate.
    const next = { ...old, ...normalized }, otherPayments = list(snapshot.cards.payments).filter(item => !selected.payment || item.id !== selected.payment.id), otherTransfers = list(snapshot.store.transfers).filter(item => item.id !== selected.transfer.id);
    assertStatementEvidence(snapshot.cards, cardId, [next]);
    const same = item => active(item) && ((text(next.reference) && canonical(item.reference || item.bankReference) === canonical(next.reference)) || item.date === next.date && cents(item.amount) === cents(next.amount));
    if (Object.keys(normalized).length && (otherPayments.some(item => item.cardId === cardId && same(item)) || otherTransfers.some(item => item.classification === 'credit_card_payment' && (item.creditCardId === cardId || canonical(item.toAccount) === canonical(cardName(selected.card))) && same(item)))) fail('The proposed correction would duplicate another recorded card payment. No records were changed.');
    const fingerprint = crypto.createHmac('sha256', tokenSecret).update(JSON.stringify({ raw: snapshot.raw, externalRaw: snapshot.externalRaw, cardRaw: snapshot.cardRaw, cards: snapshot.cards, user: req.user, cardId, sourceId, changes: normalized, reason })).digest('hex');
    return { selected, before, after, changes: normalized, reason, fingerprint };
  }
  function summary(snapshot, req, selected, p) {
    let blockedReason = ''; try { check(snapshot, selected, selected.transfer.date, p); } catch (error) { blockedReason = error.message; }
    return { id: selected.sourceId, transferId: selected.transfer.id, paymentId: selected.payment && selected.payment.id || '', cardId: selected.card.id, ...fields(selected), nature: selected.scopes[0].nature, account: selected.scopes[0].account, allocations: clone(selected.payment && selected.payment.allocations || []), editable: !blockedReason, blockedReason };
  }
  function listPayments(snapshot, req, cardId, p) {
    const card = cardFor(snapshot, req, cardId, p), ids = [];
    for (const payment of list(snapshot.cards.payments)) if (payment.cardId === cardId) ids.push('payment:' + payment.id);
    for (const transfer of list(snapshot.store.transfers)) if (transfer.classification === 'credit_card_payment' && (transfer.creditCardId === cardId || canonical(transfer.toAccount) === canonical(cardName(card))) && !transfer.creditCardPaymentId) ids.push('transfer:' + transfer.id);
    const entries = [];
    for (const id of new Set(ids)) {
      const [kind, recordId] = id.split(':'), records = (kind === 'payment' ? list(snapshot.cards.payments) : list(snapshot.store.transfers)).filter(item => item.id === recordId);
      if (records.length !== 1) continue;
      const record = records[0];
      try { visibleScopes(req, [{ nature: record.fromNature || record.nature, account: record.fromAccount || record.account }], p); } catch { continue; }
      try { entries.push(summary(snapshot, req, select(snapshot, req, cardId, id, p), p)); }
      catch (error) {
        if (error.status === 403 || error.status === 404) continue;
        // Do not expose amounts or private account names from a damaged graph.
        entries.push({ id, editable: false, blockedReason: error.message });
      }
    }
    return { card: { id: card.id, name: cardName(card) }, entries };
  }
  function apply(snapshot, req, prepared, p) {
    if (!Object.keys(prepared.changes).length) return { unchanged: true };
    const s = clone(snapshot.store), cards = clone(snapshot.cards), { selected, before, after, reason } = prepared, at = new Date().toISOString(), by = req.user.username;
    after.transfer.editedAt = at; after.transfer.editedBy = by;
    s.transfers[s.transfers.findIndex(item => item.id === selected.transfer.id)] = after.transfer;
    if (selected.payment) {
      after.payment.editedAt = at; after.payment.editedBy = by;
      cards.payments[cards.payments.findIndex(item => item.id === selected.payment.id)] = after.payment;
      for (const allocation of after.allocations) s.creditCardSettlementAllocations[s.creditCardSettlementAllocations.findIndex(item => item.id === allocation.id)] = allocation;
    }
    const audit = { at, user: by, action: 'CREDIT_CARD_PAYMENT_CORRECTED', subjectType: 'credit_card_payment', subjectId: selected.sourceId, nature: selected.scopes[0].nature, account: selected.scopes[0].account, cardId: selected.card.id, note: reason, before: clone(before), after: clone(after), changedFields: Object.keys(prepared.changes) };
    if (list(s.auditLog).some(item => item.id === 'AUD-' + String((s.auditSeq || 0) + 1).padStart(6, '0')) || list(cards.audit).some(item => item.id === 'CCA-' + String((cards.auditSeq || 0) + 1).padStart(6, '0'))) fail('The next audit identity is already in use. No correction was saved.');
    if (typeof p.audit === 'function') p.audit(s, req, audit.action, audit.subjectType, audit.subjectId, { nature: audit.nature, account: audit.account, cardId: audit.cardId, note: reason, before: audit.before, after: audit.after, changedFields: audit.changedFields });
    else { s.auditSeq = (Number(s.auditSeq) || 0) + 1; s.auditLog = list(s.auditLog).concat({ ...audit, id: 'AUD-' + String(s.auditSeq).padStart(6, '0') }); }
    Object.assign(s.auditLog.at(-1), { cardId: selected.card.id, changedFields: audit.changedFields });
    cards.auditSeq = (Number(cards.auditSeq) || 0) + 1; cards.audit = list(cards.audit).concat({ id: 'CCA-' + String(cards.auditSeq).padStart(6, '0'), at, by, action: audit.action, subjectId: selected.sourceId, details: clone(s.auditLog.at(-1)) });
    p.commit(snapshot, s, cards);
    return { unchanged: false, audit: { id: s.auditLog.at(-1).id, at, by, reason }, before, after };
  }
  const replyError = (res, error) => res.status(error.status || 409).json({ success: false, error: error.message || 'The correction could not be verified. No records were changed.' });
  function register(router) {
    router.get('/api/expenses/credit-cards/:cardId/payment-corrections', (req, res) => {
      try { const p = policy(); if (!p.canEdit(req)) fail('Manage transactions access is required.', 403); res.json({ success: true, ...listPayments(p.read(), req, req.params.cardId, p) }); } catch (error) { replyError(res, error); }
    });
    for (const action of ['preview', 'apply']) router.post('/api/expenses/credit-cards/:cardId/payment-corrections/:sourceId/' + action, (req, res) => {
      try {
        const p = policy(); if (!p.canEdit(req)) fail('Manage transactions access is required.', 403);
        const snapshot = p.read(), prepared = proposal(snapshot, req, req.params.cardId, req.params.sourceId, req.body || {}, p);
        if (action === 'preview') return res.json({ success: true, fingerprint: prepared.fingerprint, before: prepared.before, after: prepared.after, changes: prepared.changes, reason: prepared.reason, unchanged: !Object.keys(prepared.changes).length });
        if (typeof req.body.fingerprint !== 'string' || req.body.fingerprint !== prepared.fingerprint) fail('The reviewed proposal, permissions or accounting evidence changed. Preview the correction again.');
        res.json({ success: true, ...apply(snapshot, req, prepared, p) });
      } catch (error) { replyError(res, error); }
    });
  }
  return { register, select, proposal, apply, listPayments };
}
module.exports = { create, cents, validDate };
