'use strict';

// Discovery only. Existing source editors remain the sole write authority.
// Never call loadStore/migrations, save, approve or reconcile from this module.
const text = value => String(value == null ? '' : value).trim();
const list = value => Array.isArray(value) ? value : [];
const values = value => value && typeof value === 'object' ? Object.values(value) : [];
const fail = (message, status = 400) => { throw Object.assign(new Error(message), { status }); };
const MAX_RESULTS = 50, MAX_REFERENCE = 128;
const NATURES = ['SANKI', 'SAMAST', 'PERSONAL'];
function normalizeReference(value) {
  const raw = text(value).toUpperCase().replace(/[\u2010-\u2015\u2212]/g, '-');
  const match = /^(?:(?:TRANSFER|PAYMENT):)?(EX|TR|REC|CCP|RF)[ -]?(\d+)(?:\s*\/\s*PAY[ -]?(\d+))?$/.exec(raw);
  if (!match || match[3] && match[1] !== 'EX') return raw;
  return match[1] + '-' + match[2].replace(/^0+(?=\d)/, '').padStart(5, '0') + (match[3] ? '/PAY-' + match[3].replace(/^0+(?=\d)/, '').padStart(3, '0') : '');
}
function queryReference(value) {
  if (typeof value !== 'string' || value.length > MAX_REFERENCE || /[\u0000-\u001f\u007f]/.test(value)) fail('Enter one transaction reference of up to 128 characters.');
  const query = normalizeReference(value);
  if (query && query.length < 3) fail('Enter at least three characters, or a full transaction reference.');
  return query;
}
function referenceRank(query, aliases) {
  const refs = [...new Set(aliases.map(normalizeReference).filter(Boolean))];
  if (refs.includes(query)) return 0;
  if (/^\d+$/.test(query)) {
    const needle = query.replace(/^0+(?=\d)/, '');
    const numbers = refs.flatMap(ref => { const match = /^(?:EX|TR|REC|CCP|RF)-(\d+)(?:\/PAY-(\d+))?$/.exec(ref); return match ? [match[1], match[2]].filter(Boolean).map(n => n.replace(/^0+(?=\d)/, '')) : /^\d+$/.test(ref) ? [ref] : []; });
    if (numbers.includes(needle)) return 1;
    return needle.length >= 3 && numbers.some(n => n.startsWith(needle)) ? 2 : 99;
  }
  if (/^(?:EX|TR|REC|CCP|RF)-\d+(?:\/PAY-\d+)?$/.test(query)) return 99;
  return query.length >= 3 && refs.some(ref => ref.startsWith(query)) ? 2 : 99;
}
function references(record) {
  return [record.id, record.reference, record.bankReference, record.transactionReference, record.utr,
    record.bankReconciliationEvidence && record.bankReconciliationEvidence.reference].map(text).filter(Boolean);
}
function uniqueRows(rows) {
  const counts = new Map();
  for (const row of rows) { const key = normalizeReference(row && row.id); if (key) counts.set(key, (counts.get(key) || 0) + 1); }
  return rows.filter(row => row && text(row.id) && counts.get(normalizeReference(row.id)) === 1);
}
function inactive(record) { return record.accountingExcluded || record.active === false || record.deletedAt || record.voidedAt || record.reversedAt || record.supersededBy || ['void', 'voided', 'deleted', 'reversed', 'rejected', 'cancelled', 'canceled'].includes(record.status); }
function safeAmount(value) { if (value == null || value === '') return null; const n = Number(value); return Number.isFinite(n) && Math.abs(n * 100 - Math.round(n * 100)) < 0.000001 ? n : null; }

/** All callbacks are read-only. read() returns unmigrated {store,cards,external}.
 * cardEntries(snapshot,req,cardId) returns the existing correction-service list.
 * authorizeRefund(req,record,snapshot) must authorize every current source,
 * source copy, account, card and linked component; role-only checks are unsafe.
 * Missing authorization dependencies fail closed, without role/name fallback.
 */
function create(deps) {
  for (const key of ['canEdit', 'read', 'natures', 'canPersonal', 'accountVisible', 'personalAccount', 'canViewExpense', 'canAccessExpense', 'canAccessCard', 'cardEntries', 'authorizeRefund']) if (typeof deps[key] !== 'function') throw new Error('Missing transaction finder dependency: ' + key);
  if (!deps.ledgerEditor || typeof deps.ledgerEditor.authorizeTarget !== 'function' || typeof deps.ledgerEditor.eligibility !== 'function' || typeof deps.ledgerEditor.evidence !== 'function') throw new Error('The exact-source ledger authorization service is required.');
  function scopeVisible(req, nature, account) {
    return NATURES.includes(nature) && list(deps.natures(req)).includes(nature) && !!text(account) && deps.accountVisible(req, account) === true && (!(nature === 'PERSONAL' || deps.personalAccount(account, nature)) || deps.canPersonal(req) === true);
  }
  function cardAccountRows(cards, account) {
    const canonical = value => text(value).toLowerCase().replace(/\s+/g, ' ');
    return text(account) ? values(cards && cards.cards).filter(card => [card.account, [text(card.name), text(card.last4)].filter(Boolean).join(' ')].filter(Boolean).some(name => canonical(name) === canonical(account))) : [];
  }
  function cardScopeVisible(req, cards, account, cardId = '', required = false) {
    const rows = values(cards && cards.cards), matches = cardAccountRows(cards, account);
    if (matches.length > 1 || required && matches.length !== 1 || matches.some(card => deps.canAccessCard(req, card) !== true)) return false;
    if (text(cardId)) {
      const byId = rows.filter(card => normalizeReference(card.id) === normalizeReference(cardId));
      if (byId.length !== 1 || deps.canAccessCard(req, byId[0]) !== true || matches.length && matches[0] !== byId[0]) return false;
    }
    return true;
  }
  function expenseVisible(req, expense, cards) {
    if (!expense || !NATURES.includes(expense.nature) || !list(deps.natures(req)).includes(expense.nature) || expense.nature === 'PERSONAL' && deps.canPersonal(req) !== true || deps.canViewExpense(req, expense) !== true || deps.canAccessExpense(req, expense) !== true) return false;
    const cardAccess = id => !!text(id) && cardScopeVisible(req, cards, '', id);
    const accountAccess = (account, nature = expense.nature, cardId = '') => {
      return (!text(account) || scopeVisible(req, nature, account)) && cardScopeVisible(req, cards, account, cardId);
    };
    if (!accountAccess(expense.account, expense.nature, expense.creditCardId) || !accountAccess(expense.claimantFundingAccount)) return false;
    if (expense.creditCardStatementId) {
      const statements = values(cards && cards.statements).filter(statement => statement.id === expense.creditCardStatementId);
      if (statements.length !== 1 || !cardAccess(statements[0].cardId) || expense.creditCardId && statements[0].cardId !== expense.creditCardId) return false;
    }
    if (expense.payments != null && !Array.isArray(expense.payments)) return false;
    return list(expense.payments).every(payment => payment && accountAccess(payment.account || expense.account, payment.nature || expense.nature, payment.creditCardId || expense.creditCardId) && (!payment.personalFunds || deps.canPersonal(req) === true));
  }
  function search(req, reference) {
    if (deps.canEdit(req) !== true) fail('Manage transactions access is required.', 403);
    const query = queryReference(reference);
    if (!query) return { success: true, reference: '', entries: [], truncated: false };
    const snapshot = deps.read(), store = snapshot && snapshot.store, cards = snapshot && snapshot.cards;
    if (!store || typeof store !== 'object' || Array.isArray(store) || !cards || typeof cards !== 'object' || Array.isArray(cards)) fail('The accounting sources cannot be verified. No records were changed.', 409);
    for (const key of ['transfers', 'receipts', 'reconciliationExpenses', 'vendorAdvances', 'creditCardSettlementAllocations', 'expenseRefunds']) if (store[key] != null && !Array.isArray(store[key])) fail('The accounting sources cannot be verified. No records were changed.', 409);
    if (store.expenses != null && (typeof store.expenses !== 'object' || Array.isArray(store.expenses))) fail('The accounting sources cannot be verified. No records were changed.', 409);
    if (cards.payments != null && !Array.isArray(cards.payments) || cards.cards != null && (typeof cards.cards !== 'object' || Array.isArray(cards.cards))) fail('The card sources cannot be verified. No records were changed.', 409);
    const output = new Map(), evidence = deps.ledgerEditor.evidence(store, snapshot.external || {}), cardLists = new Map();
    const transfers = uniqueRows(list(store.transfers)), receipts = uniqueRows(list(store.receipts));
    const expenses = uniqueRows(values(store.expenses).concat(list(store.reconciliationExpenses)));
    const cardRows = uniqueRows(values(cards.cards));
    const add = (row, aliases) => {
      const rank = referenceRank(query, aliases);
      if (rank === 99) return;
      const existing = output.get(row.key);
      if (!existing || rank < existing.rank) output.set(row.key, { ...row, rank });
    };
    const link = (tab, extra = {}) => ({ tab, ...extra });
    function protectedAction(reason, destination) { return { type: 'source_workflow', reason, destination }; }
    function ordinary(record, kind, id, extraAliases = [], expense) {
      if (referenceRank(query, [id, ...references(record), ...extraAliases]) === 99) return;
      let selected;
      try { selected = deps.ledgerEditor.authorizeTarget(store, req, { id, kind }); } catch { return; }
      if (selected.scopes.some(scope => !scopeVisible(req, scope.nature, scope.account) || !cardScopeVisible(req, cards, scope.account)) || !cardScopeVisible(req, cards, selected.context.account, record.creditCardId) || expense && !expenseVisible(req, expense, cards)) return;
      // Legacy card refunds and misclassified transfers still carry private
      // card scope even when they have no parent refund/payment graph.
      if (record.creditCardRefund && !cardScopeVisible(req, cards, selected.context.account, record.creditCardId, true)) return;
      const scope = selected.context;
      const eligibility = deps.ledgerEditor.eligibility(store, req, { id, kind, nature: scope.nature, account: scope.account }, snapshot.external || {}, evidence);
      const cardBacked = !!text(record.creditCardId || expense && expense.creditCardId) || selected.scopes.some(item => cardAccountRows(cards, item.account).length > 0);
      const editable = !inactive(record) && !cardBacked && eligibility.canEditLedgerTransaction === true;
      const blockedReason = inactive(record) ? 'This record is inactive or excluded. Its history is retained and cannot be edited here.' : cardBacked ? 'This movement touches a credit-card source. Review its source workflow; it cannot be corrected as an ordinary ledger transaction.' : editable ? '' : text(eligibility.ledgerEditBlockedReason) || 'This movement requires its source workflow.';
      let destination = link('cash', { nature: scope.nature, account: scope.account, date: text(record.date) });
      if (cardBacked) {
        const explicitIds = [record.creditCardId, expense && expense.creditCardId].map(normalizeReference).filter(Boolean);
        const resolvedCards = [...new Set(selected.scopes.flatMap(item => cardAccountRows(cards, item.account)).concat(values(cards.cards).filter(card => explicitIds.includes(normalizeReference(card.id)))))];
        // Do not send an unclassified card movement to a generic ledger editor
        // or invent a payment source. A multi-card source needs manual review.
        destination = resolvedCards.length === 1 && cardRows.includes(resolvedCards[0]) ? link('creditcards', { cardId: resolvedCards[0].id }) : undefined;
      }
      const action = editable ? { type: 'ledger_edit', id, kind, nature: scope.nature, account: scope.account } : protectedAction(blockedReason, destination);
      add({ key: kind + ':' + id, id, kind, reference: id, date: text(record.date), nature: scope.nature, account: scope.account,
        amount: safeAmount(record.amount), title: expense ? text(expense.vendor || expense.particulars) : kind === 'transfer' ? text(record.fromAccount) + ' → ' + text(record.toAccount) : text(record.source || record.note), editable, blockedReason, action }, [id, ...references(record), ...extraAliases]);
    }
    function cardList(card) {
      if (!cardLists.has(card.id)) {
        try { const result = deps.cardEntries(snapshot, req, card.id); cardLists.set(card.id, list(result && result.entries)); }
        catch { cardLists.set(card.id, []); }
      }
      return cardLists.get(card.id);
    }
    // One result per whole card-payment graph. TR and CCP are aliases, never
    // separate edit buttons that could correct just half of a payment.
    for (const card of cardRows) {
      if (deps.canAccessCard(req, card) !== true) continue;
      for (const entry of cardList(card)) {
        const parts = text(entry.id).split(':');
        if (parts.length !== 2 || !['transfer', 'payment'].includes(parts[0])) continue;
        const payments = parts[0] === 'payment' ? uniqueRows(list(cards.payments)).filter(row => row.id === parts[1] && row.cardId === card.id) : [];
        if (parts[0] === 'payment' && payments.length !== 1) continue;
        const payment = payments[0];
        const graph = transfers.filter(row => parts[0] === 'transfer' ? row.id === parts[1] : row.creditCardPaymentId === payment.id || row.id === payment.transferId || row.id === payment.creditCardTransferId);
        if (graph.length !== 1) continue;
        const transfer = graph[0];
        let selected; try { selected = deps.ledgerEditor.authorizeTarget(store, req, { id: transfer.id, kind: 'transfer' }); } catch { continue; }
        if (selected.scopes.some(scope => !scopeVisible(req, scope.nature, scope.account) || !cardScopeVisible(req, cards, scope.account))) continue;
        if (payment && (!scopeVisible(req, payment.nature, payment.account) || !cardScopeVisible(req, cards, payment.account, payment.creditCardId) || list(payment.allocations).some(row => !scopeVisible(req, row.nature, payment.account)))) continue;
        const allocations = list(store.creditCardSettlementAllocations).filter(row => payment && row.creditCardPaymentId === payment.id || row.transferId === transfer.id || row.linkedTransferId === transfer.id);
        if (allocations.some(row => !scopeVisible(req, row.payerNature, row.payingAccount) || !scopeVisible(req, row.beneficiaryNature, row.payingAccount) || !cardScopeVisible(req, cards, row.payingAccount))) continue;
        const editable = entry.editable === true && !inactive(transfer) && (!payment || !inactive(payment));
        const blockedReason = editable ? '' : text(entry.blockedReason) || 'This card payment is protected by its source workflow.';
        const action = editable ? { type: 'credit_card', cardId: card.id, sourceId: entry.id } : protectedAction(blockedReason, link('creditcards', { cardId: card.id, sourceId: entry.id }));
        add({ key: 'credit_card:' + card.id + ':' + entry.id, kind: 'credit_card', id: parts[1], reference: entry.transferId || parts[1],
          date: text(entry.date), nature: text(entry.nature), account: text(entry.account), amount: entry.amount == null ? null : safeAmount(entry.amount),
          title: text(card.name) + ' ' + text(card.last4), editable, blockedReason, action }, [...references(transfer), ...(payment ? references(payment) : [])]);
      }
    }
    for (const transfer of transfers) if (transfer.classification !== 'credit_card_payment' && !transfer.creditCardId && !transfer.creditCardPaymentId) ordinary(transfer, 'transfer', transfer.id);
    for (const expense of expenses) {
      if (!expenseVisible(req, expense, cards)) continue;
      const paymentRows = list(expense.payments);
      // Do not expose hidden paying-account context through a bill result.
      if (paymentRows.some(row => !scopeVisible(req, expense.nature, text(row.account || expense.account)))) continue;
      const isStoredExpense = (store.expenses || {})[expense.id] === expense;
      const action = isStoredExpense && !inactive(expense) ? { type: 'expense', id: expense.id } : protectedAction('This expense belongs to its original source workflow.', link('list', { focus: expense.id }));
      add({ key: 'expense_record:' + expense.id, id: expense.id, kind: 'expense_record', reference: expense.id, date: text(expense.date), nature: expense.nature,
        account: '', amount: safeAmount(expense.amount), title: text(expense.vendor || expense.particulars), editable: false,
        blockedReason: 'Expense amounts and payment edits are checked in the expense editor; reconciled financial fields stay locked.', action }, [...references(expense), expense.billNo, expense.billNumber]);
      if (isStoredExpense) for (const payment of uniqueRows(paymentRows)) ordinary(payment, payment.personalFunds ? 'personal_expense' : 'expense', expense.id + '/' + payment.id, [expense.id], expense);
    }
    // A refund-owned receipt is an alias of the RF graph, never an independent
    // receipt amount editor. Missing/denied parents cannot become editable.
    const refunds = uniqueRows(list(store.expenseRefunds));
    for (const refund of refunds) {
      try {
        if (deps.authorizeRefund(req, refund, snapshot) !== true || !list(deps.natures(req)).includes(refund.nature) || refund.nature === 'PERSONAL' && deps.canPersonal(req) !== true) continue;
        if (!Array.isArray(refund.sources) || !Array.isArray(refund.components) || refund.components.some(row => row.account && !scopeVisible(req, refund.nature, row.account))) continue;
        let accessible = true;
        for (const source of refund.sources) {
          const sourceExpense = source.expenseId && expenses.find(row => row.id === source.expenseId);
          if (source.expenseId && !expenseVisible(req, sourceExpense, cards) || source.nature && (!list(deps.natures(req)).includes(source.nature) || source.nature === 'PERSONAL' && deps.canPersonal(req) !== true) || list(source.paymentAllocations).some(row => row.personal && deps.canPersonal(req) !== true || row.account && row.account !== 'Refund credit' && !scopeVisible(req, source.nature || refund.nature, row.account))) accessible = false;
        }
        if (!accessible) continue;
        const relatedReceipts = receipts.filter(row => row.expenseRefundId === refund.id || refund.components.some(component => component.receiptId === row.id || component.externalMovementId === row.id));
        if (relatedReceipts.some(row => !scopeVisible(req, row.nature, row.account))) continue;
        const blockedReason = inactive(refund) ? 'This refund is inactive or excluded; its audit history is retained.' : 'Use the audited refund correction preview; source, approval and reconciliation locks still apply.';
        add({ key: 'refund:' + refund.id, kind: 'refund', id: refund.id, reference: refund.id, date: text(refund.date), nature: refund.nature,
          account: refund.components.length === 1 ? text(refund.components[0].account) : '', amount: safeAmount(refund.amount), title: text(refund.vendor), editable: false, blockedReason,
          action: inactive(refund) ? protectedAction(blockedReason, link('refunds', { nature: refund.nature, refundId: refund.id })) : { type: 'refund', id: refund.id, nature: refund.nature } }, [...references(refund), ...refund.sources.flatMap(row => [row.reference, row.expenseId]), ...refund.components.flatMap(references), ...relatedReceipts.flatMap(references)]);
      } catch { /* Inaccessible or unverifiable sources reveal no identifiers. */ }
    }
    const refundReceiptIds = new Set(list(store.expenseRefunds).flatMap(row => list(row && row.components).flatMap(component => [component.receiptId, component.externalMovementId]).filter(Boolean)));
    for (const receipt of receipts) if (!receipt.expenseRefundId && !refundReceiptIds.has(receipt.id)) ordinary(receipt, 'receipt', receipt.id);
    const entries = [...output.values()].sort((a, b) => a.rank - b.rank || b.date.localeCompare(a.date) || a.key.localeCompare(b.key));
    const result = { success: true, reference: query, entries: entries.slice(0, MAX_RESULTS).map(({ rank, ...row }) => row), truncated: entries.length > MAX_RESULTS };
    if (result.truncated) result.message = 'More matches are available. Enter the complete reference to narrow this search.';
    return result;
  }
  function register(router) {
    router.get('/api/expenses/transactions', (req, res) => {
      try { if (Object.keys(req.query || {}).some(key => key !== 'reference')) fail('Search accepts only one transaction reference.'); res.json(search(req, (req.query || {}).reference == null ? '' : req.query.reference)); }
      catch (error) { res.status([400, 403, 409].includes(error.status) ? error.status : 409).json({ success: false, error: [400, 403].includes(error.status) ? error.message : 'The accounting sources could not be verified. No records were changed.' }); }
    });
  }
  return { search, register };
}
module.exports = { create, normalizeReference, referenceRank };
