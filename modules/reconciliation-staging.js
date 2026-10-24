'use strict';

const cents = value => Math.round((Number(value) || 0) * 100);
const money = value => value / 100;
const text = value => String(value || '').trim();
const reference = value => text(value).toLowerCase().replace(/[^a-z0-9]/g, '');
const date = value => text(value).slice(0, 10);
const ids = row => [...new Set([row && row.id].concat(row && row.sourceIds || []).filter(Boolean).map(String))];
const net = row => cents(row && row.credit) - cents(row && row.debit);
const bankAccount = key => text(key).replace(/^(?:SANKI|SAMAST|PERSONAL)\|/, '');
const rowIds = row => [...new Set([].concat(row && row.linkedRecordIds || [], ids(row && row.ledger), (row && row.ledger && row.ledger.linkedEntries || []).flatMap(ids)).filter(Boolean))];
const sameOrigin = (record, draft, rowId) => !!(record && text(draft.id) && text(rowId) && text(record.reconciliationDraft || record.bankReconciliationDraft || record.bankReconciliationEvidence && record.bankReconciliationEvidence.draftId) === text(draft.id) && text(record.bankRowId || record.bankReconciliationEvidence && record.bankReconciliationEvidence.rowId) === text(rowId));

function signature(bank) {
  return [date(bank && bank.date), cents(bank && bank.debit), cents(bank && bank.credit),
    reference(bank && bank.reference), text(bank && bank.description).toLowerCase().replace(/\s+/g, ' '),
    bank && bank.balance != null && Number.isFinite(Number(bank.balance)) ? cents(bank.balance) : ''].join('|');
}

function booksForAccount(store, account) {
  return Object.entries(store.bankStatements || {}).filter(([key]) => bankAccount(key) === account).map(([, book]) => book);
}

// A physical identity requires one stored bank transaction, not just equal
// amount/date. Its complete narration and balance distinguish repeated rows.
function physicalRows(store, draft, bank) {
  if (!bank || !date(bank.date) || !(cents(bank.debit) || cents(bank.credit))) return [];
  const result = [];
  for (const book of booksForAccount(store, draft.account)) {
    const matches = Object.values(book.transactions || {}).filter(row => signature(row) === signature(bank));
    if (matches.length !== 1) continue;
    const transaction = matches[0];
    if (!transaction.id) continue;
    result.push({ book, transaction });
  }
  return result;
}

function recordMatchesBankRow(record, store, draft, rowId, bank, physical) {
  if (sameOrigin(record, draft, rowId)) return true;
  const origin = text(record.reconciliationDraft || record.bankReconciliationDraft || record.bankReconciliationEvidence && record.bankReconciliationEvidence.draftId);
  const originRow = text(record.bankRowId || record.bankReconciliationEvidence && record.bankReconciliationEvidence.rowId);
  for (const { book, transaction } of physical) {
    if (text(record.bankTransactionId) === text(transaction.id)) return true;
    if (origin && originRow && origin === text(transaction.reconciliationDraftId) && originRow === 'bank-' + transaction.reconciliationDraftRowIndex) return true;
    for (const imported of book.imports || []) {
      if (!imported.finalizedAt) continue;
      for (const row of [].concat(imported.reconciliationRows || [], imported.carriedReconciliationRows || [])) {
        if (!row.bank || text(row.bank.transactionId) !== text(transaction.id) || signature(row.bank) !== signature(transaction)) continue;
        if (rowIds(row).includes(text(record.id))) return true;
        if (origin && originRow && origin === text(imported.draftId) && originRow === text(row.id)) return true;
      }
    }
    // Older split-charge adjustments retained their exact bank transaction
    // on the spending companion, but omitted bankRowId on the adjustment.
    if ((store.reconciliationExpenses || []).some(expense => text(expense.adjustmentId) === text(record.id) && text(expense.bankTransactionId) === text(transaction.id) && expense.account === draft.account)) return true;
  }
  return false;
}

/** Return only actual projected ledger movements already posted for this row. */
function postedBankRowMovements({ store = {}, draft = {}, rowId, bank, movements = [] } = {}) {
  const physical = physicalRows(store, draft, bank), records = [];
  for (const [collection, postingKind] of [['transfers', 'transfer'], ['adjustments', 'adjustment'], ['paytmSettlements', 'settlement'], ['vendorAdvances', 'advance'], ['bankTruthMovements', 'bank_truth']]) {
    for (const record of store[collection] || []) {
      if (record.accountingExcluded || !recordMatchesBankRow(record, store, draft, rowId, bank, physical)) continue;
      if (date(record.date) !== date(bank && bank.date)) continue;
      if (postingKind === 'transfer' ? ![record.fromAccount, record.toAccount].includes(draft.account) : text(record.account || record.bankAccount) !== text(draft.account)) continue;
      records.push({ record, postingKind });
    }
  }
  const result = [], seen = new Set();
  for (const movement of movements) {
    if (!movement || movement.pendingPosting || seen.has(text(movement.id))) continue;
    const match = records.find(item => ids(movement).includes(text(item.record.id)));
    if (!match || date(movement.date) !== date(bank && bank.date)) continue;
    seen.add(text(movement.id));
    const record = match.record, spending = (store.reconciliationExpenses || []).filter(expense => text(expense.adjustmentId) === text(record.id));
    const bankChargeComponent = match.postingKind === 'adjustment' && (!!record.automaticAxisTransferCharge || !!record.transferChargeFor || !!record.splitBankCharge || spending.length > 0 && spending.every(expense => expense.category === 'BANK CHARGES') && spending.reduce((sum, expense) => sum + cents(expense.amount), 0) === Math.abs(cents(record.amount)));
    result.push({ ...movement, postingKind: match.postingKind, bankChargeComponent, record });
  }
  return result;
}

/** Pending arithmetic is separate from matching: never infer identity from money. */
function isFeeComponent(movement, expected) {
  const record = movement.record || {};
  return movement.postingKind === 'adjustment' && net(movement) < 0 && (movement.bankChargeComponent || record.automaticAxisTransferCharge || record.transferChargeFor || record.splitBankCharge || Math.abs(net(movement)) === Math.abs(expected));
}

function pendingReconciliationAmount({ row = {}, postedMovements = [], companyAdjusted = false } = {}) {
  if (!row.bank || row.status === 'already_reconciled') return 0;
  const resolution = row.resolution || {}, action = resolution.action;
  let expected = 0, kinds = [], feeOnly = false;
  if (row.status === 'matched' && row.axisTransferCharge) {
    expected = -cents(row.chargeAmount); kinds = ['adjustment']; feeOnly = true;
  } else if (action === 'split_allocation') {
    expected = -cents(resolution.chargeAmount); kinds = ['adjustment']; feeOnly = true;
  } else if (action === 'opening_vendor_payable_split') {
    expected = -cents(resolution.openingPayableAmount); kinds = ['adjustment'];
  } else if (action === 'link_with_rounding') {
    expected = cents(resolution.roundingAmount); kinds = ['adjustment'];
  } else if (action === 'vendor_advance_split') {
    expected = -cents(resolution.advanceAmount); kinds = ['advance'];
  } else if (['create_adjustment', 'create_split_adjustment', 'create_internal_transfer', 'paytm_settlement', 'exclude'].includes(action) && !(companyAdjusted && action === 'exclude')) {
    expected = net(row.bank) - cents(resolution.linkedTransferAmount);
    kinds = action === 'create_internal_transfer' ? ['transfer', 'adjustment'] : action === 'paytm_settlement' ? ['settlement'] : action === 'exclude' ? ['bank_truth'] : ['adjustment'];
  }
  if (!expected) return 0;
  const posted = postedMovements.filter(movement => kinds.includes(movement.postingKind) && Math.sign(net(movement)) === Math.sign(expected) && (!feeOnly || isFeeComponent(movement, expected))).reduce((sum, movement) => sum + Math.abs(net(movement)), 0);
  // Never let a duplicated posted entry manufacture an opposite pending entry.
  // The actual duplicate remains visible in the ledger and requires review.
  return money(Math.sign(expected) * Math.max(0, Math.abs(expected) - posted)) || 0;
}

/** Keep duplicate posted components visible even when other ledger errors offset them. */
function postedReconciliationConflicts({ row = {}, postedMovements = [] } = {}) {
  if (!row.bank) return [];
  const action = row.resolution && row.resolution.action, automaticFee = row.axisTransferCharge && ['matched', 'resolved'].includes(row.status);
  const singleTransfer = action === 'create_internal_transfer' || action === 'split_allocation' || automaticFee;
  const conflicts = [], add = (type, message, movements) => conflicts.push({ type, bankRowIds: [row.id], appIds: movements.flatMap(ids), message });
  const principals = postedMovements.filter(movement => movement.postingKind === 'transfer' && Math.sign(net(movement)) === Math.sign(net(row.bank)));
  if (singleTransfer && principals.length > 1) add('duplicate_posted_principal', 'More than one posted transfer claims this bank transaction. Review the existing transfers before finalizing; no duplicate has been hidden or merged.', principals);
  const expectedFee = cents(automaticFee ? row.chargeAmount : row.resolution && row.resolution.chargeAmount);
  const expectedPrincipal = cents(row.resolution && row.resolution.principalAmount) || (automaticFee && row.app ? Math.abs(net(row.app)) : Math.abs(net(row.bank)) - expectedFee);
  const postedPrincipals = postedMovements.filter(movement => movement.postingKind === 'transfer');
  if (singleTransfer && postedPrincipals.some(movement => Math.sign(net(movement)) !== Math.sign(net(row.bank)) || Math.abs(net(movement)) !== expectedPrincipal)) add('mismatched_posted_principal', 'A posted transfer amount or direction disagrees with the reviewed principal. Correct that existing transfer before finalizing; a partial principal cannot be silently completed or replaced.', postedPrincipals);
  if (automaticFee || ['create_internal_transfer', 'split_allocation'].includes(action)) {
    const fees = postedMovements.filter(movement => isFeeComponent(movement, expectedFee));
    if (fees.reduce((sum, movement) => sum + Math.abs(net(movement)), 0) > expectedFee) add('excess_posted_bank_fee', 'Posted bank charges for this transaction exceed its reviewed charge. Review the existing charge entries before finalizing.', fees);
    // A confirmed internal-transfer split deliberately owns one charge ID.
    // Do not silently create a second fee component that this metadata cannot
    // represent, or take a charge already owned by another transfer.
    if (action === 'create_internal_transfer' && fees.length) {
      if (fees.length !== 1 || Math.abs(net(fees[0])) !== expectedFee) add('incomplete_posted_transfer_fee', 'This internal transfer already has partial or multiple bank-charge entries. Correct them to one reviewed full charge before finalizing; the transfer split cannot safely own multiple charge records.', fees);
      const principalIds = new Set(postedPrincipals.flatMap(ids));
      if (fees.some(movement => movement.record && movement.record.transferChargeFor && !principalIds.has(text(movement.record.transferChargeFor)))) add('conflicting_posted_transfer_fee', 'An existing bank-charge entry belongs to another transfer. Review that ownership before finalizing; the charge has not been reassigned.', fees);
    }
  }
  if (['create_internal_transfer', 'create_adjustment', 'create_split_adjustment'].includes(action)) {
    const components = postedMovements.filter(movement => ['transfer', 'adjustment'].includes(movement.postingKind) && Math.sign(net(movement)) === Math.sign(net(row.bank)));
    if (components.reduce((sum, movement) => sum + Math.abs(net(movement)), 0) > Math.abs(net(row.bank))) add('excess_posted_bank_amount', 'Posted components for this bank transaction exceed its bank amount. Review the existing entries before finalizing.', components);
  }
  if (['create_adjustment', 'create_split_adjustment'].includes(action)) {
    const adjustments = postedMovements.filter(movement => movement.postingKind === 'adjustment');
    if (adjustments.length && adjustments.reduce((sum, movement) => sum + net(movement), 0) !== net(row.bank)) add('mismatched_posted_adjustment', 'The existing adjustment amount or direction does not equal this bank transaction. Correct the existing adjustment before finalizing; a partial adjustment cannot be silently completed or replaced.', adjustments);
  }
  return conflicts;
}

function reliableReference(value) {
  const ref = reference(value);
  return /^\d{12,}$/.test(ref) || ref.length >= 8 && /[a-z]/.test(ref) && /\d/.test(ref) ? ref : '';
}

function transferBankEvidence(store, transfer, account) {
  const evidence = [], origin = text(transfer.reconciliationDraft || transfer.bankReconciliationEvidence && transfer.bankReconciliationEvidence.draftId), originRow = text(transfer.bankRowId || transfer.bankReconciliationEvidence && transfer.bankReconciliationEvidence.rowId);
  let invalid = false;
  const active = (store.bankReconciliationDrafts || {})[origin], match = originRow.match(/^bank-(\d+)$/);
  if (active && active.account === account && match && active.transactions && active.transactions[Number(match[1])]) evidence.push(active.transactions[Number(match[1])]);
  for (const book of booksForAccount(store, account)) {
    for (const imported of book.imports || []) {
      if (!imported.finalizedAt) continue;
      for (const row of [].concat(imported.reconciliationRows || [], imported.carriedReconciliationRows || [])) {
        if (!rowIds(row).includes(text(transfer.id)) && !(origin && origin === text(imported.draftId) && originRow === text(row.id))) continue;
        if (!row.bank || row.bank.reversal || row.resolutionAction === 'bank_reversal_pair' || /excluded|failed payment|timing difference/i.test(text(row.decision))) { invalid = true; continue; }
        const stored = Object.values(book.transactions || {}).filter(value => text(value.id) === text(row.bank.transactionId));
        const repeatedReference = reliableReference(row.bank.reference) && Object.values(book.transactions || {}).filter(value => date(value.date) === date(row.bank.date) && reliableReference(value.reference) === reliableReference(row.bank.reference)).length !== 1;
        if (stored.length === 1 && signature(stored[0]) === signature(row.bank) && Object.values(book.transactions || {}).filter(value => signature(value) === signature(row.bank)).length === 1 && !repeatedReference) evidence.push(row.bank);
        else invalid = true;
      }
    }
  }
  return invalid ? [{}] : evidence.filter((value, index, all) => all.findIndex(other => signature(other) === signature(value)) === index);
}

function verifiedDistinct(bank, evidence) {
  const currentRef = reliableReference(bank && bank.reference);
  if (!currentRef || evidence.length !== 1) return false;
  const other = evidence[0], oldRef = reliableReference(other.reference);
  return !!oldRef && oldRef !== currentRef && date(other.date) === date(bank.date) && (cents(other.debit) > 0) === (cents(bank.debit) > 0);
}

function transferAmountCandidate(bank, principal, account) {
  const total = cents(bank.debit) + cents(bank.credit), difference = total - cents(principal);
  return difference === 0 || /\baxis\b/i.test(account) && cents(bank.debit) > 0 && [295, 590].includes(difference);
}

/**
 * Return review blockers only. This function never links, merges, restores or
 * edits transfers, and the existing managed PERSONAL policy remains in charge.
 */
function findTransferCreationConflicts({ store = {}, draft = {}, rowId, bank, fromAccount, toAccount, fromNature, toNature, principalAmount } = {}) {
  if (!bank || fromNature === 'PERSONAL' && toNature === 'PERSONAL') return [];
  const incoming = cents(bank.credit) > 0, conflicts = [], expectedDate = date(bank.date);
  const matches = transfer => {
    if (date(transfer.date) !== expectedDate) return false;
    const side = incoming ? transfer.toAccount : transfer.fromAccount;
    const sideNature = incoming ? transfer.toNature || transfer.nature : transfer.fromNature || transfer.nature;
    if (side !== draft.account || sideNature !== (incoming ? toNature : fromNature)) return false;
    const sameRoute = transfer.fromAccount === fromAccount && transfer.toAccount === toAccount;
    const sameRef = reliableReference(bank.reference) && reliableReference(transfer.bankReference) === reliableReference(bank.reference);
    return (sameRoute || sameRef) && (cents(transfer.amount) === cents(principalAmount) || transferAmountCandidate(bank, transfer.amount, draft.account));
  };
  for (const transfer of store.transfers || []) {
    if (!matches(transfer)) continue;
    if (!sameOrigin(transfer, draft, rowId) && verifiedDistinct(bank, transferBankEvidence(store, transfer, draft.account))) continue;
    conflicts.push({ type: 'posted', id: transfer.id, draftId: transfer.reconciliationDraft || '', rowId: transfer.bankRowId || '', excluded: !!transfer.accountingExcluded, sameOrigin: sameOrigin(transfer, draft, rowId), reason: 'An existing transfer represents or may represent this bank transaction; review and link its evidence before creating another transfer.' });
  }
  for (const other of Object.values(store.bankReconciliationDrafts || {})) {
    for (const [otherRowId, resolution] of Object.entries(other.resolutions || {})) {
      if (!resolution || resolution.action !== 'create_internal_transfer' || text(other.id) === text(draft.id) && otherRowId === rowId) continue;
      const match = otherRowId.match(/^bank-(\d+)$/), otherBank = match && other.transactions && other.transactions[Number(match[1])];
      if (!otherBank) continue;
      const otherIncoming = cents(otherBank.credit) > 0, nature = resolution.postingNature || other.nature, counterpartNature = resolution.otherNature || nature;
      const candidate = { date: otherBank.date, amount: cents(resolution.principalAmount) > 0 ? resolution.principalAmount : money(cents(otherBank.debit) + cents(otherBank.credit) - cents(resolution.chargeAmount)), fromAccount: otherIncoming ? resolution.otherAccount : other.account, toAccount: otherIncoming ? other.account : resolution.otherAccount, fromNature: otherIncoming ? counterpartNature : nature, toNature: otherIncoming ? nature : counterpartNature, bankReference: otherBank.reference };
      if (!matches(candidate) || other.account === draft.account && verifiedDistinct(bank, [otherBank])) continue;
      conflicts.push({ type: 'pending', id: '', draftId: other.id, rowId: otherRowId, reason: 'This transfer is already pending in another bank reconciliation decision; review that decision before creating another transfer.' });
    }
  }
  return conflicts;
}

module.exports = { postedBankRowMovements, pendingReconciliationAmount, postedReconciliationConflicts, findTransferCreationConflicts };
