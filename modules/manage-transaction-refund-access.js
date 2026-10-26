'use strict';

// Visibility only: no store initialization, migrations or correction workflows.
// Authorize saved source copies AND their uniquely identified current sources.
const object = value => !!value && typeof value === 'object' && !Array.isArray(value);
const text = value => typeof value === 'string' ? value.trim() : '';
const key = value => text(value).toUpperCase().replace(/^(EX|PAY|RF|CCP|REC|TR)-0+(?=\d+$)/, '$1-');
const same = (a, b) => key(a) === key(b);
const NATURES = new Set(['SANKI', 'SAMAST', 'PERSONAL']);
const MONEY_MODES = new Set(['cash', 'upi', 'bank', 'card']);
const CREDIT_MODES = new Set(['voucher', 'store_credit', 'vendor_credit', 'credit_note']);

function authorizeRefund(req, record, snapshot, deps) {
  try {
    for (const name of ['natures', 'canPersonal', 'accountVisible', 'personalAccount', 'canViewExpense', 'canAccessExpense', 'canAccessCard']) if (!deps || typeof deps[name] !== 'function') return false;
    if (!object(snapshot) || !object(snapshot.store) || !object(snapshot.cards) || !object(record)) return false;
    const store = snapshot.store, cards = snapshot.cards;
    const array = value => value == null ? [] : Array.isArray(value) && value.every(object) ? value : null;
    const values = value => value == null ? [] : object(value) && Object.values(value).every(object) ? Object.values(value) : null;
    const expenses = values(store.expenses), reconciled = array(store.reconciliationExpenses), refunds = array(store.expenseRefunds), advances = array(store.vendorAdvances), receipts = array(store.receipts), cardRows = values(cards.cards), statements = values(cards.statements);
    if ([expenses, reconciled, refunds, advances, receipts, cardRows, statements].some(rows => !rows)) return false;
    const allExpenses = expenses.concat(reconciled), allowed = deps.natures(req), personal = deps.canPersonal(req) === true;
    if (!Array.isArray(allowed)) return false;
    const unique = (rows, id) => { if (!text(id)) return null; const matches = rows.filter(row => same(row.id, id)); return matches.length === 1 ? matches[0] : null; };
    const entity = nature => NATURES.has(nature) && allowed.includes(nature) && (nature !== 'PERSONAL' || personal);
    const cardName = card => [text(card.name), text(card.last4)].filter(Boolean).join(' ');
    function account(name, nature, required = false, cardId = '', forceCard = false) {
      if (!entity(nature)) return false;
      if (!text(name)) return !required && !text(cardId) && !forceCard;
      if (deps.accountVisible(req, name) !== true || deps.personalAccount(name, nature) && !personal) return false;
      const namedCards = cardRows.filter(card => same(cardName(card), name) || text(card.account) && same(card.account, name));
      if (namedCards.length > 1 || namedCards.some(card => deps.canAccessCard(req, card) !== true)) return false;
      if (text(cardId)) {
        const card = unique(cardRows, cardId);
        if (!card || deps.canAccessCard(req, card) !== true || namedCards.length && namedCards[0] !== card) return false;
      } else if (forceCard && namedCards.length !== 1) return false;
      return true;
    }
    function personalFlags(row) { return !(row.personal || row.personalFunds || row.personalRefund || row.paidAlready || row.receiver === 'payer') || personal; }
    function expenseAccess(expense) {
      if (!expense || !entity(expense.nature) || deps.canViewExpense(req, expense) !== true || deps.canAccessExpense(req, expense) !== true || !personalFlags(expense)) return false;
      if (!account(expense.account || expense.claimantFundingAccount || '', expense.nature, false, expense.creditCardId || '')) return false;
      if (expense.claimantFundingAccount && !account(expense.claimantFundingAccount, expense.nature, true)) return false;
      if (expense.creditCardStatementId) {
        const statement = unique(statements, expense.creditCardStatementId), card = statement && unique(cardRows, statement.cardId);
        if (!card || deps.canAccessCard(req, card) !== true || expense.creditCardId && !same(expense.creditCardId, card.id)) return false;
      }
      const payments = array(expense.payments);
      return !!payments && payments.every(payment => unique(payments, payment.id) && personalFlags(payment) && account(payment.account || expense.account || '', payment.nature || expense.nature, true, payment.creditCardId || expense.creditCardId || ''));
    }
    function advanceAccess(advance, seen) {
      if (!advance || !entity(advance.nature) || !personalFlags(advance) || !account(advance.account || '', advance.nature, !advance.creditOnly)) return false;
      const applications = array(advance.applications);
      if (!applications || applications.some(item => !expenseAccess(unique(allExpenses, item.expenseId)))) return false;
      if (advance.creditOnly && !text(advance.expenseRefundId)) return false;
      return !advance.expenseRefundId || refundAccess(unique(refunds, advance.expenseRefundId), seen);
    }
    function originalPayments(expense, seen) {
      const payments = array(expense.payments), applications = array(expense.vendorAdvanceApplications);
      if (!payments || !applications) return null;
      const rows = payments.map(payment => ({ ...payment, paymentId: payment.id, account: payment.account || expense.account || '', personal: !!payment.personalFunds, creditCardId: payment.creditCardId || expense.creditCardId || '' }));
      if (expense.creditCardStatementId && !rows.length) rows.push({ paymentId: 'STATEMENT', account: expense.account, creditCardId: expense.creditCardId, personal: false });
      for (let index = 0; index < applications.length; index++) {
        const application = applications[index], advance = unique(advances, application.vendorAdvanceId);
        if (!advanceAccess(advance, seen)) return null;
        rows.push({ paymentId: 'ADV-' + application.vendorAdvanceId + '-' + index, account: advance.account || 'Vendor advance', personal: !!advance.personalFunds, creditOnly: !!advance.creditOnly });
      }
      if (!rows.length && Number(expense.paidAmount) > 0) rows.push({ paymentId: 'LEGACY', account: expense.account || expense.claimantFundingAccount || '', personal: !!expense.paidAlready });
      return rows;
    }
    function sourceAccess(source, nature, seen) {
      // post() intentionally omits nature/account/payments from its source copy.
      // Derive its entity from the actual source, never a permissive default.
      if (source.nature != null && source.nature !== nature || !personalFlags(source)) return false;
      if (!account(source.account || '', nature, false, source.creditCardId || '')) return false;
      if (source.kind === 'advance') {
        const advance = unique(advances, source.advanceId);
        const copies = array(source.payments), allocations = array(source.paymentAllocations);
        return !source.expenseId && !source.paymentId && copies && !copies.length && allocations && !allocations.length && advance && advance.nature === nature && advanceAccess(advance, seen);
      }
      if (!['expense', 'payment'].includes(source.kind) || source.advanceId) return false;
      const expense = unique(allExpenses, source.expenseId);
      if (!expenseAccess(expense) || expense.nature !== nature) return false;
      const originals = originalPayments(expense, seen), copies = array(source.payments), allocations = array(source.paymentAllocations);
      if (!originals || !copies || !allocations) return false;
      const original = id => { const matches = originals.filter(row => same(row.paymentId, id)); return text(id) && matches.length === 1 ? matches[0] : null; };
      if (source.kind === 'payment' && !original(source.paymentId)) return false;
      for (const payment of copies.concat(allocations)) {
        const saved = original(payment.paymentId);
        if (!saved || !personalFlags(saved) || !personalFlags(payment) || payment.personal !== undefined && !!payment.personal !== !!saved.personal) return false;
        if (text(payment.account) && !same(payment.account, saved.account)) return false;
        if (!saved.creditOnly && !account(saved.account, expense.nature, true, saved.creditCardId || '')) return false;
        if (!saved.creditOnly && !account(payment.account || saved.account, expense.nature, true, payment.creditCardId || '')) return false;
      }
      return true;
    }
    function movementAccess(movement, nature) {
      return !!movement && movement.nature === nature && personalFlags(movement) && account(movement.account, nature, true, movement.creditCardId || '', !!movement.creditCardRefund) && (!reconciled.includes(movement) || expenseAccess(movement));
    }
    function refundAccess(refund, seen) {
      if (!refund || unique(refunds, refund.id) !== refund || seen.has(key(refund.id)) || !entity(refund.nature) || !personalFlags(refund)) return false;
      const next = new Set(seen).add(key(refund.id)), sources = array(refund.sources), components = array(refund.components);
      if (!sources || !sources.length || !components || !['pending', 'received', 'voided'].includes(refund.status)) return false;
      const sourceIds = sources.map(source => key(source.expenseId || source.advanceId));
      if (sourceIds.some(id => !id) || new Set(sourceIds).size !== sourceIds.length) return false;
      if (refund.status === 'pending' ? components.length !== 0 : components.length === 0) return false;
      if (refund.parentId && !refundAccess(unique(refunds, refund.parentId), next)) return false;
      if (!sources.every(source => sourceAccess(source, refund.nature, next))) return false;
      for (const component of components) {
        if (!['company', 'payer'].includes(component.receiver) || !personalFlags(component) || !MONEY_MODES.has(component.mode) && !CREDIT_MODES.has(component.mode)) return false;
        if (!account(component.account || '', refund.nature, MONEY_MODES.has(component.mode), component.creditCardId || '', component.mode === 'card')) return false;
        for (const id of [component.receiptId, component.externalMovementId].filter(Boolean)) {
          const movement = unique(receipts.concat(reconciled), id);
          if (!movementAccess(movement, refund.nature) || !same(movement.account, component.account) || component.receiptId === id && movement.expenseRefundId && !same(movement.expenseRefundId, refund.id)) return false;
        }
        if (component.vendorAdvanceId) {
          const advance = unique(advances, component.vendorAdvanceId);
          if (!advance || !advance.creditOnly || !same(advance.expenseRefundId, refund.id) || advance.nature !== refund.nature || !personalFlags(advance)) return false;
        }
      }
      return true;
    }
    return refundAccess(record, new Set());
  } catch { return false; }
}

module.exports = authorizeRefund;
