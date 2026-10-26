'use strict';

// Exact-record restoration, not a transfer creator. The authenticated route
// must enforce Owner access and save the caller's isolated store only once.
const crypto = require('crypto');
const VERSION = 1;
const list = value => Array.isArray(value) ? value : [];
const text = value => typeof value === 'string' ? value.trim() : '';
const clone = value => JSON.parse(JSON.stringify(value));
const cents = value => {
  const amount = Number(value), result = Math.round(amount * 100);
  return typeof value !== 'boolean' && Number.isFinite(amount) && Number.isSafeInteger(result) && Math.abs(amount * 100 - result) < .00001 ? result : NaN;
};
const validDate = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value + 'T00:00:00Z')) && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value;
const referenceIsStrong = value => /^\d{12}$/.test(text(value));
const nature = value => text(value).toUpperCase();
const fail = message => { const error = new Error(message); error.status = 409; throw error; };
const hash = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const linkedIds = row => [...new Set([row && row.appId, row && row.transferId, ...list(row && row.appIds), ...list(row && row.linkedRecordIds), row && row.ledger && row.ledger.id].flatMap(value => text(value).split(/\s*,\s*/)).filter(Boolean))];
const requiredHelpers = ['bankReference', 'bankSignature', 'bookKey', 'postingNature', 'transferCandidate', 'resolutionCandidate', 'protectionError', 'movements'];

function assertHelpers(helpers) {
  if (!helpers || requiredHelpers.some(key => typeof helpers[key] !== 'function')) fail('Restoration evidence checks are unavailable. Nothing was changed.');
}
function normalizeIntent(input) {
  const intent = { draftId: text(input && input.draftId), rowId: text(input && input.rowId), transferId: text(input && input.transferId) };
  if (!intent.draftId || !intent.transferId || !/^bank-(0|[1-9]\d*)$/.test(intent.rowId) || !Number.isSafeInteger(Number(intent.rowId.slice(5)))) fail('Choose an existing excluded transfer and an exact bank transaction.');
  return intent;
}
function exactBank(bank, account, transfer, helpers) {
  const direction = account === transfer.fromAccount ? 'debit' : account === transfer.toAccount ? 'credit' : '';
  const opposite = direction === 'debit' ? 'credit' : 'debit';
  return !!direction && !!bank && !bank.reversal && bank.date === transfer.date && cents(bank[direction]) === cents(transfer.amount) && cents(bank[opposite] || 0) === 0 && referenceIsStrong(helpers.bankReference(bank));
}
function containsReference(value, id) {
  if (typeof value === 'string') return value === id;
  if (Array.isArray(value)) return value.some(item => containsReference(item, id));
  return !!value && typeof value === 'object' && Object.values(value).some(item => containsReference(item, id));
}
function protectedRecord(state, transfer, helpers) {
  const protectedKeys = ['confirmedBankSplit', 'creditAbsorbedIntoOpeningAccounts', 'openingBalanceTreatment', 'openingBalanceTreatmentNote', 'salaryAdvanceId', 'creditCardId', 'creditCardPaymentId', 'creditCardStatementId', 'gokwikPostingId', 'gokwikSettlementId', 'paytmTransactionId', 'paytmSettlementId', 'settledThroughPaytm', 'originalToAccount', 'settlementId', 'loanId', 'refundId', 'expenseRefundId', 'reimbursementAllocationId', 'settledByTransferId'];
  if (protectedKeys.some(key => transfer[key] != null && transfer[key] !== false && transfer[key] !== '' && (!Array.isArray(transfer[key]) || transfer[key].length)) || Number(transfer.adjustmentAmount || 0) !== 0 || transfer.receivedAmount != null && cents(transfer.receivedAmount) !== cents(transfer.amount)) fail('This transfer is allocated, split or absorbed into another accounting workflow. Correct that workflow first.');
  const protection = helpers.protectionError(state, transfer);
  if (protection) fail(String(protection));
  for (const key of ['expenses', 'receipts', 'adjustments', 'vendorAdvances', 'loanRepayments', 'salesRefunds', 'paytmSettlements', 'paytmPayoutPostings', 'gokwikPostings', 'bankAmountAuthorityProposals', 'bankMovementAuthority', 'bankReconciliationLinks']) {
    const records = state[key];
    if (records && (Object.prototype.hasOwnProperty.call(records, transfer.id) || containsReference(records, transfer.id))) fail('This transfer is referenced by ' + key + '. Review that dependency before restoring it.');
  }
}
function matchingHistoryRows(book, bank, helpers) {
  const ref = helpers.bankReference(bank);
  return list(book && book.imports).filter(record => record.finalizedAt).flatMap(record => [...list(record.reconciliationRows), ...list(record.carriedReconciliationRows)].filter(row => row.bank && row.bank.date === bank.date && helpers.bankReference(row.bank) === ref).map(row => ({ record, row })));
}
function assertNoForeignFinalizedClaim(book, bank, transfer, account, helpers, correctableRow) {
  for (const { row } of matchingHistoryRows(book, bank, helpers)) {
    const correctingThisExactRow = row === correctableRow && !linkedIds(row).some(id => id !== transfer.id);
    if (!exactBank(row.bank, account, transfer, helpers) || helpers.bankSignature(row.bank) !== helpers.bankSignature(bank) || row.resolutionAction === 'bank_reversal_pair' || /timing difference|failed payment/i.test(String(row.decision || '')) || /excluded/i.test(String(row.decision || '')) && !correctingThisExactRow || linkedIds(row).some(id => id !== transfer.id)) fail('Finalized bank history has conflicting evidence, an exclusion or another ledger claim. Correct that history before restoring this transfer.');
  }
}
function replayState(state, intent, fingerprint, helpers) {
  const transfers = list(state.transfers).filter(item => item.id === intent.transferId);
  if (transfers.length !== 1) return null;
  const transfer = transfers[0];
  if (transfer.accountingExcluded) return null;
  const previous = list(transfer.restorationHistory).find(item => item.version === VERSION && item.fingerprint === fingerprint && item.intent && Object.keys(intent).every(key => item.intent[key] === intent[key]));
  if (!previous || !previous.after || previous.after.identityHash !== hash({ id: transfer.id, date: transfer.date, amount: transfer.amount, fromAccount: transfer.fromAccount, toAccount: transfer.toAccount, fromNature: transfer.fromNature || transfer.nature, toNature: transfer.toNature || transfer.nature, classification: transfer.classification || 'internal_transfer' })) return null;
  const draft = state.bankReconciliationDrafts && state.bankReconciliationDrafts[intent.draftId];
  if (!draft) return null;
  const bank = list(draft.transactions)[Number(intent.rowId.slice(5))], resolution = draft.resolutions && draft.resolutions[intent.rowId];
  if (!bank || helpers.bankSignature(bank) !== previous.selected.signature || !resolution || resolution.action !== 'link_existing' || resolution.appId !== transfer.id || resolution.restorationFingerprint !== fingerprint) return null;
  return { restored: false, alreadyRestored: true, transferId: transfer.id, draftId: draft.id, rowId: intent.rowId };
}

function buildRestorePlan(state, input, helpers) {
  assertHelpers(helpers);
  const intent = normalizeIntent(input), draft = state.bankReconciliationDrafts && state.bankReconciliationDrafts[intent.draftId];
  if (!draft || draft.id !== intent.draftId) fail('This reconciliation draft is unavailable. Refresh the statement.');
  if (draft.originalStatementPeriod || ['awaiting_approval', 'pending_approval', 'finalized'].includes(draft.status)) fail('This reconciliation is locked or awaiting approval. Withdraw its approval request before restoring an entry.');
  const transfers = list(state.transfers).filter(item => item.id === intent.transferId);
  if (transfers.length !== 1) fail('The excluded transfer ID is missing or duplicated.');
  const transfer = transfers[0];
  if (!transfer.accountingExcluded) {
    for (const previous of list(transfer.restorationHistory).slice().reverse()) {
      const replay = replayState(state, intent, previous.fingerprint, helpers);
      if (replay) return { version: VERSION, intent, fingerprint: previous.fingerprint, alreadyRestored: true, transfer: { id: transfer.id, date: transfer.date, amount: transfer.amount, fromAccount: transfer.fromAccount, toAccount: transfer.toAccount } };
    }
    fail('This transfer is not excluded. Link the existing transfer instead of restoring or creating another one.');
  }
  if (nature(transfer.fromNature || transfer.nature) !== 'PERSONAL' || nature(transfer.toNature || transfer.nature) !== 'PERSONAL' || (transfer.classification || 'internal_transfer') !== 'internal_transfer' || !text(transfer.fromAccount) || !text(transfer.toAccount) || transfer.fromAccount === transfer.toAccount || !Number.isSafeInteger(cents(transfer.amount)) || !(cents(transfer.amount) > 0) || !validDate(transfer.date)) fail('Only an existing, unsplit PERSONAL internal transfer with a valid date and amount can be restored here.');
  if (!transfer.accountingExclusion || typeof transfer.accountingExclusion !== 'object') fail('The original exclusion audit is missing. Review the record before restoring it.');
  protectedRecord(state, transfer, helpers);
  const bank = list(draft.transactions)[Number(intent.rowId.slice(5))];
  if (helpers.postingNature(draft, intent.rowId) !== 'PERSONAL' || !exactBank(bank, draft.account, transfer, helpers)) fail('The bank date, exact principal, direction or PERSONAL ownership does not agree with this transfer.');
  const selected = { account: draft.account, direction: draft.account === transfer.fromAccount ? 'debit' : 'credit', date: bank.date, amount: transfer.amount, reference: helpers.bankReference(bank), signature: helpers.bankSignature(bank) };
  const dateOverride = state.bankDateOverrides && state.bankDateOverrides[transfer.id];
  if (dateOverride && (dateOverride.bankDate !== transfer.date || dateOverride.originalDate && dateOverride.originalDate !== transfer.date || referenceIsStrong(dateOverride.bankReference) && dateOverride.bankReference !== selected.reference || list(dateOverride.bankReferences).some(reference => !referenceIsStrong(reference) || reference !== selected.reference))) fail('An existing bank-date override does not agree with this exact transaction. Review it before restoring the transfer.');
  const currentResolution = draft.resolutions && draft.resolutions[intent.rowId];
  if (currentResolution) fail('This bank row already has a review decision. Undo or review it before restoring this entry.');
  const candidate = helpers.transferCandidate(state, transfer), physical = candidate && candidate.physicalEvidence;
  if (!candidate || !physical || list(physical.invalidAccounts).length) fail('The original transfer has missing or conflicting retained bank evidence.');
  const oppositeAccount = draft.account === transfer.fromAccount ? transfer.toAccount : transfer.fromAccount;
  const oppositeEvidence = list(physical.evidence).filter(item => item.account === oppositeAccount);
  if (oppositeEvidence.length !== 1 || !referenceIsStrong(oppositeEvidence[0].reference) || oppositeEvidence[0].reference !== selected.reference || oppositeEvidence[0].date !== transfer.date || oppositeEvidence[0].direction !== (selected.direction === 'debit' ? 'credit' : 'debit') || !text(oppositeEvidence[0].identity)) fail('A unique retained opposite-bank transaction with the same verified UPI reference is required.');
  if (list(physical.evidence).some(item => ![transfer.fromAccount, transfer.toAccount].includes(item.account) || item.reference !== selected.reference || item.date !== transfer.date)) fail('The transfer has contradictory bank identities.');
  const oppositeBook = state.bankStatements && state.bankStatements[helpers.bookKey('PERSONAL', oppositeAccount)], oppositeMatches = Object.values(oppositeBook && oppositeBook.transactions || {}).filter(item => item.id === oppositeEvidence[0].identity);
  if (oppositeMatches.length !== 1 || !exactBank(oppositeMatches[0], oppositeAccount, transfer, helpers) || helpers.bankSignature(oppositeMatches[0]) !== oppositeEvidence[0].signature || helpers.bankReference(oppositeMatches[0]) !== selected.reference) fail('The opposite-side physical statement row is missing or no longer agrees with its verified link.');
  const oppositeBank = oppositeMatches[0];
  const oppositeHistory = matchingHistoryRows(oppositeBook, oppositeBank, helpers).filter(({ row }) => row.bank.transactionId === oppositeBank.id && helpers.bankSignature(row.bank) === helpers.bankSignature(oppositeBank));
  if (oppositeHistory.length !== 1) fail('The opposite-bank statement must contain one exact finalized bank record for this transfer.');
  const oppositeHistoryRecord = oppositeHistory[0];
  const oppositeAlreadyLinked = linkedIds(oppositeHistoryRecord.row).includes(transfer.id);
  const oppositeCorrectable = !oppositeAlreadyLinked && oppositeHistoryRecord.row.resolutionAction !== 'bank_reversal_pair' && !/timing difference|failed payment/i.test(String(oppositeHistoryRecord.row.decision || '')) && !linkedIds(oppositeHistoryRecord.row).some(id => id !== transfer.id);
  if (!oppositeAlreadyLinked && !oppositeCorrectable) fail('The opposite-bank finalized record has another claim or a conflicting decision.');

  for (const account of [transfer.fromAccount, transfer.toAccount]) {
    const book = state.bankStatements && state.bankStatements[helpers.bookKey('PERSONAL', account)];
    const stored = Object.values(book && book.transactions || {}).filter(item => item.date === transfer.date && helpers.bankReference(item) === selected.reference);
    if (stored.length > 1 || stored.some(item => !exactBank(item, account, transfer, helpers))) fail('The statement contains ambiguous or contradictory physical transactions for this UPI reference.');
    const expected = account === draft.account ? bank : oppositeBank;
    if (stored.some(item => helpers.bankSignature(item) !== helpers.bankSignature(expected))) fail('Stored bank evidence differs from this reviewed transaction.');
    assertNoForeignFinalizedClaim(book, expected, transfer, account, helpers, account === oppositeAccount && oppositeCorrectable ? oppositeHistoryRecord.row : null);
  }
  // Current bank-truth consumers count the complete collection, even when a
  // row carries accountingExcluded. Do not flag a BTR as excluded and pretend
  // that removes its cash leg. A separate exact replacement workflow is needed.
  if (list(state.bankTruthMovements).some(item => [transfer.fromAccount, transfer.toAccount].includes(item.account) && (containsReference(item, transfer.id) || item.date === transfer.date && (helpers.bankReference(item) === selected.reference || cents(item.debit) === cents(transfer.amount) || cents(item.credit) === cents(transfer.amount))))) fail('An exclusion-generated bank-truth movement may already count this cash movement. Review its exact replacement before restoring the transfer; no adjustment or duplicate entry was created.');
  for (const account of [transfer.fromAccount, transfer.toAccount]) {
    // Shared 7883 has one physical SAMAST/PERSONAL book. Inspect both raw and
    // projected cash rows so a receipt, payment or authority projection cannot
    // silently coexist with the restored transfer's same physical movement.
    const accountNature = helpers.bookKey('PERSONAL', account) === account ? 'SAMAST' : 'PERSONAL';
    const movements = [...list(helpers.movements(state, account, accountNature, { raw: true })), ...list(helpers.movements(state, account, accountNature))];
    if (movements.some(row => row.id !== transfer.id && row.date === transfer.date && helpers.bankReference(row) === selected.reference && (cents(row.debit) > 0 || cents(row.credit) > 0))) fail('An active ledger payment, receipt or projected movement already uses this bank identity. Review it before restoring a duplicate.');
  }
  for (const authority of [...Object.values(state.bankAmountAuthorityProposals || {}), ...Object.values(state.bankMovementAuthority || {})]) {
    const sameRow = (authority.draftId || authority.reconciliationDraft) === draft.id && (authority.bankRowId || authority.rowId) === intent.rowId;
    const snapshot = authority.bankSnapshot || authority.after || authority;
    const samePhysical = [transfer.fromAccount, transfer.toAccount].includes(authority.account) && snapshot.date === transfer.date && helpers.bankReference(authority.bankSnapshot || authority) === selected.reference;
    if (sameRow || samePhysical) fail('A bank-amount authority decision already protects this physical transaction. Review it before restoring this transfer.');
  }
  const exclusionsToClear = [], affectedDrafts = new Set([draft.id]);
  for (const other of Object.values(state.bankReconciliationDrafts || {})) {
    const sameAccount = [transfer.fromAccount, transfer.toAccount].includes(other.account);
    if (sameAccount) {
      const matches = list(other.transactions).filter(row => row.date === transfer.date && helpers.bankReference(row) === selected.reference);
      if (matches.length > 1 || matches.some(row => !exactBank(row, other.account, transfer, helpers) || helpers.bankSignature(row) !== helpers.bankSignature(other.account === draft.account ? bank : oppositeBank))) fail('Another preview has ambiguous or contradictory bank evidence for this transfer.');
      if (matches.length) affectedDrafts.add(other.id);
    }
    for (const [rowId, resolution] of Object.entries(other.resolutions || {})) {
      const claims = linkedIds(resolution), row = /^bank-\d+$/.test(rowId) ? list(other.transactions)[Number(rowId.slice(5))] : null;
      if (claims.includes(transfer.id)) {
        affectedDrafts.add(other.id);
        const snapshot = resolution.excludedEntry, posting = helpers.postingNature(other, rowId), snapshotNature = nature(snapshot && (snapshot.entity || snapshot.nature));
        const personalSnapshot = (!posting || posting === 'PERSONAL') && (!snapshotNature || snapshotNature === 'PERSONAL') && (posting === 'PERSONAL' || snapshotNature === 'PERSONAL');
        const exactSnapshot = sameAccount && snapshot && snapshot.id === transfer.id && snapshot.date === transfer.date && cents(snapshot[other.account === transfer.fromAccount ? 'debit' : 'credit']) === cents(transfer.amount) && cents(snapshot[other.account === transfer.fromAccount ? 'credit' : 'debit'] || 0) === 0 && !list(snapshot.sourceIds).some(id => id !== transfer.id) && personalSnapshot;
        if (resolution.action === 'exclude' && !row && claims.length === 1 && exactSnapshot) exclusionsToClear.push({ draftId: other.id, rowId });
        else if (!row || !sameAccount || !exactBank(row, other.account, transfer, helpers) || helpers.bankReference(row) !== selected.reference || claims.some(id => id !== transfer.id) || !['link_existing', 'accept_match', 'create_transfer'].includes(resolution.action)) fail('Another review decision already claims or excludes this transfer. Correct that decision first.');
      } else if (sameAccount && row && row.date === transfer.date && helpers.bankReference(row) === selected.reference) fail('Another decision already uses this physical bank transaction.');
      const pending = helpers.resolutionCandidate(other, rowId, resolution);
      if (pending && pending.key === candidate.key) {
        const pendingEvidence = pending.bankEvidence;
        const distinct = pendingEvidence && pendingEvidence.account === selected.account && referenceIsStrong(pendingEvidence.reference) && pendingEvidence.reference !== selected.reference;
        if (!distinct) fail('A pending transfer creation may duplicate this transfer. Undo that pending creation first.');
      }
    }
  }
  for (const other of list(state.transfers)) {
    if (other.id === transfer.id || other.accountingExcluded) continue;
    const sameRoute = other.fromAccount === transfer.fromAccount && other.toAccount === transfer.toAccount && other.date === transfer.date && cents(other.amount) === cents(transfer.amount);
    const otherCandidate = helpers.transferCandidate(state, other), otherPhysical = otherCandidate && otherCandidate.physicalEvidence;
    if (otherPhysical && list(otherPhysical.evidence).some(item => [transfer.fromAccount, transfer.toAccount].includes(item.account) && item.reference === selected.reference && item.date === transfer.date)) fail('An active transfer already claims this bank identity. Link or correct it instead of restoring a duplicate.');
    if (sameRoute) {
      const sameSide = list(otherPhysical && otherPhysical.evidence).filter(item => item.account === selected.account && item.direction === selected.direction);
      if (!otherPhysical || list(otherPhysical.invalidAccounts).length || sameSide.length !== 1 || !referenceIsStrong(sameSide[0].reference) || sameSide[0].reference === selected.reference) fail('Another active same-date/amount transfer has ambiguous bank evidence. Review it before restoring this record.');
    }
  }
  for (const approval of Object.values(state.bankReconciliationApprovals || {})) if (approval.status === 'pending' && affectedDrafts.has(approval.draftId)) fail('An affected statement is awaiting Owner approval. Withdraw that request before restoring a transfer.');
  for (const id of affectedDrafts) {
    const affected = state.bankReconciliationDrafts[id];
    if (affected && (affected.originalStatementPeriod || ['awaiting_approval', 'pending_approval', 'finalized'].includes(affected.status))) fail('An affected statement is locked or awaiting approval.');
  }
  const oppositeCorrection = oppositeCorrectable ? { bookKey: helpers.bookKey('PERSONAL', oppositeAccount), recordId: oppositeHistoryRecord.record.id, rowId: oppositeHistoryRecord.row.id } : null;
  const result = { version: VERSION, intent, alreadyRestored: false, transfer: { id: transfer.id, date: transfer.date, amount: transfer.amount, fromAccount: transfer.fromAccount, toAccount: transfer.toAccount }, exclusion: clone(transfer.accountingExclusion), selected, opposite: { ...clone(oppositeEvidence[0]), transactionId: oppositeBank.id }, oppositeCorrection, exclusionsToClear };
  result.fingerprint = hash({ version: VERSION, intent, state });
  return result;
}

function applyRestorePlan(state, plan, actor, helpers) {
  assertHelpers(helpers);
  if (!plan || plan.version !== VERSION || !/^[a-f0-9]{64}$/.test(plan.fingerprint || '')) fail('Preview the excluded transfer restoration before confirming it.');
  const intent = normalizeIntent(plan.intent), user = text(actor && actor.user), reason = text(actor && actor.reason), at = text(actor && actor.at);
  if (!user || reason.length < 10 || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(at) || !Number.isFinite(Date.parse(at))) fail('A named reviewer, a detailed restoration reason and an exact audit time are required.');
  const replay = replayState(state, intent, plan.fingerprint, helpers);
  if (replay) return replay;
  const current = buildRestorePlan(state, intent, helpers);
  if (current.alreadyRestored || current.fingerprint !== plan.fingerprint) fail('The transfer, exclusion or bank evidence changed after preview. Refresh and review it again; nothing was changed.');
  const transfer = state.transfers.find(item => item.id === intent.transferId), draft = state.bankReconciliationDrafts[intent.draftId], before = clone(transfer), selectedExclusion = draft.matchingExclusions && draft.matchingExclusions[intent.rowId];
  const history = { version: VERSION, fingerprint: current.fingerprint, intent, selected: clone(current.selected), opposite: clone(current.opposite), previousExclusion: clone(transfer.accountingExclusion), previousMatchingExclusion: selectedExclusion ? clone(selectedExclusion) : null, clearedDecisions: current.exclusionsToClear.map(item => ({ ...item, previous: clone(state.bankReconciliationDrafts[item.draftId].resolutions[item.rowId]) })), reason, by: user, at };
  transfer.accountingExcluded = false;
  transfer.accountingExclusion = null;
  history.after = { identityHash: hash({ id: transfer.id, date: transfer.date, amount: transfer.amount, fromAccount: transfer.fromAccount, toAccount: transfer.toAccount, fromNature: transfer.fromNature || transfer.nature, toNature: transfer.toNature || transfer.nature, classification: transfer.classification || 'internal_transfer' }) };
  transfer.restorationHistory = list(transfer.restorationHistory); transfer.restorationHistory.push(history);
  if (current.oppositeCorrection) {
    const correction = current.oppositeCorrection, book = state.bankStatements[correction.bookKey], record = list(book && book.imports).find(item => item.id === correction.recordId);
    const row = record && [...list(record.reconciliationRows), ...list(record.carriedReconciliationRows)].find(item => item.id === correction.rowId);
    if (!row) fail('The opposite-bank finalized record changed after preview. Refresh and review it again; nothing was changed.');
    const previous = clone(row), bank = row.bank || {}, direction = correction.bookKey.endsWith('|' + transfer.fromAccount) ? 'debit' : 'credit';
    row.linkedRecordIds = [transfer.id];
    row.ledger = { id: transfer.id, date: transfer.date, description: 'Restored internal transfer · ' + transfer.fromAccount + ' to ' + transfer.toAccount, reference: current.selected.reference, debit: direction === 'debit' ? transfer.amount : 0, credit: direction === 'credit' ? transfer.amount : 0 };
    row.decision = 'Corrected finalized link · restored excluded transfer'; row.reason = reason; row.resolvedBy = user; row.resolvedAt = at; row.corrected = true;
    row.correctionHistory = list(row.correctionHistory); row.correctionHistory.push({ type: 'restore_excluded_transfer', before: previous, after: { linkedRecordIds: clone(row.linkedRecordIds), ledger: clone(row.ledger), decision: row.decision }, reason, by: user, at, restorationFingerprint: current.fingerprint });
  }
  for (const item of history.clearedDecisions) {
    const other = state.bankReconciliationDrafts[item.draftId]; delete other.resolutions[item.rowId];
    other.decisionAudit = list(other.decisionAudit); other.decisionAudit.push({ action: 'restore_excluded_transfer', rowId: item.rowId, transferId: transfer.id, previous: clone(item.previous), reason, by: user, at, restorationFingerprint: current.fingerprint });
  }
  if (selectedExclusion && (!selectedExclusion.appId || selectedExclusion.appId === transfer.id)) delete draft.matchingExclusions[intent.rowId];
  draft.resolutions = draft.resolutions || {};
  draft.resolutions[intent.rowId] = { action: 'link_existing', appId: transfer.id, appIds: [], postingNature: 'PERSONAL', reason, remark: reason, by: user, at, restorationFingerprint: current.fingerprint };
  draft.decisionAudit = list(draft.decisionAudit); draft.decisionAudit.push({ action: 'restore_excluded_transfer_and_link', rowId: intent.rowId, transferId: transfer.id, previous: null, after: clone(draft.resolutions[intent.rowId]), reason, by: user, at, restorationFingerprint: current.fingerprint });
  state.auditLog = list(state.auditLog); state.auditSeq = Number(state.auditSeq || 0) + 1;
  state.auditLog.push({ id: 'AUD-' + String(state.auditSeq).padStart(6, '0'), seq: state.auditSeq, at, user, action: 'EXCLUDED_PERSONAL_TRANSFER_RESTORED', subjectType: 'transfer', subjectId: transfer.id, nature: 'PERSONAL', account: draft.account, draftId: draft.id, bankRowId: intent.rowId, before, after: clone(transfer), note: reason, device: 'Web', restorationFingerprint: current.fingerprint });
  return { restored: true, alreadyRestored: false, transferId: transfer.id, draftId: draft.id, rowId: intent.rowId };
}

module.exports = { buildRestorePlan, applyRestorePlan };
