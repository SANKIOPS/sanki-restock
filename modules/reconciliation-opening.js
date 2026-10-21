'use strict';

function amount(value) {
  const parsed = parseFloat(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function cents(value) {
  return Math.round(amount(value) * 100);
}

function calendarDate(value) {
  const date = String(value || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return '';
  const parsed = new Date(date + 'T00:00:00Z');
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === date ? date : '';
}

function identities(movement) {
  if (!movement || typeof movement !== 'object') return [];
  return Array.from(new Set([movement.id].concat(movement.sourceIds || [])
    .filter(value => value != null && String(value)).map(String)));
}

/**
 * Verify that a saved closing really belongs to the book's furthest cutoff.
 * Older historical finalizations overwrote lastReconciliation's amount while
 * relabeling its through date with the later cutoff. Its own import retains
 * the actual period, so that evidence takes priority over the relabeled date.
 */
function verifiedReconciliationAnchor(book = {}) {
  const unavailable = { cutoffDate: '', previousClosing: null, anchorSource: 'unavailable', anchorRecordId: '' };
  const cutoffDate = calendarDate(book.reconciledThrough);
  if (!cutoffDate) return unavailable;
  const imports = (book.imports || []).filter(record => record && typeof record === 'object');
  const last = book.lastReconciliation || {};
  const finiteAmount = value => ['number', 'string'].includes(typeof value) && String(value).trim() !== ''
    && Number.isFinite(Number(value)) ? cents(Number(value)) / 100 : null;
  const firstAmount = values => values.map(finiteAmount).find(value => value != null) ?? null;
  const period = record => {
    const values = [record.to, record.statementSummary && record.statementSummary.to]
      .filter(value => value != null && String(value).trim() !== '');
    const dates = values.map(calendarDate);
    return { date: dates[0] || '', contradictory: dates.some(date => !date) || new Set(dates).size > 1 };
  };
  const matchingImports = last.draftId == null || !String(last.draftId) ? [] : imports
    .filter(record => record.draftId != null && String(record.draftId) === String(last.draftId));
  const matchingPeriods = matchingImports.map(period);
  const datedMatches = matchingPeriods.filter(value => value.date);
  const contradictoryImport = matchingPeriods.some(value => value.contradictory || value.date && value.date !== cutoffDate);
  const lastDate = datedMatches.length ? datedMatches[0].date
    : last.through != null && String(last.through).trim() ? calendarDate(last.through) : cutoffDate;
  const lastClosing = firstAmount([last.companyAdjustedClosingBalance, last.closingBalance]);
  if (!contradictoryImport && lastDate === cutoffDate && lastClosing != null) {
    return { cutoffDate, previousClosing: lastClosing, anchorSource: 'last_reconciliation',
      anchorRecordId: String(matchingImports[0] && matchingImports[0].id || '') };
  }

  const candidates = imports.flatMap(record => {
    const evidence = period(record);
    if (evidence.contradictory || evidence.date !== cutoffDate) return [];
    let closing = firstAmount([record.companyAdjustedClosingBalance,
      record.statementSummary && record.statementSummary.closingBalance, record.closingBalance]);
    // A deferred or unreconciled ledger close cannot stand in for missing bank
    // closing evidence. Accept a ledger-only snapshot only when it was proved
    // reconciled, with no retained evidence of a difference.
    if (closing == null && (record.balanceReconciled === true || record.reconciled === true)
      && record.closingBalanceDeferred !== true
      && (record.balanceDifference == null || finiteAmount(record.balanceDifference) === 0)) {
      closing = finiteAmount(record.ledgerClosingBalance);
    }
    return closing == null ? [] : [{ record, closing }];
  });
  if (!candidates.length || new Set(candidates.map(candidate => cents(candidate.closing))).size !== 1) return unavailable;
  const selected = candidates[0];
  return { cutoffDate, previousClosing: selected.closing, anchorSource: 'finalized_import',
    anchorRecordId: String(selected.record.id || '') };
}

/**
 * Return ledger identities supported by this account's finalized statement
 * book. Provenance from another account's book is deliberately insufficient:
 * confirming a transfer debit in Axis does not confirm its credit in Indus.
 */
function finalizedMovementCoverageIds(store = {}, book = {}) {
  const covered = new Set();
  const finalizedOrigins = new Set();
  const add = (target, value) => {
    if (value == null) return;
    const id = String(value).trim();
    // Composite report IDs are display text, not an authoritative new ledger
    // identity. Actual IDs come from linkedRecordIds/sourceIds/linkedEntries.
    if (id && !id.includes(',')) target.add(id);
  };
  const addLedger = ledger => {
    if (!ledger || typeof ledger !== 'object') return;
    add(covered, ledger.id);
    (ledger.sourceIds || []).forEach(id => add(covered, id));
    (ledger.linkedEntries || []).forEach(addLedger);
  };
  (book.imports || []).forEach(record => {
    add(finalizedOrigins, record.id);
    add(finalizedOrigins, record.draftId);
    (record.combinedReconciliationIds || []).forEach(id => add(finalizedOrigins, id));
    [].concat(record.reconciliationRows || [], record.carriedReconciliationRows || []).forEach(row => {
      (row.linkedRecordIds || []).forEach(id => add(covered, id));
      addLedger(row.ledger);
    });
  });
  if (book.lastReconciliation) add(finalizedOrigins, book.lastReconciliation.draftId);
  const isFinalizedOrigin = value => value != null && finalizedOrigins.has(String(value).trim());
  const hasFinalizedProvenance = record => record && (
    isFinalizedOrigin(record.reconciliationDraft)
    || isFinalizedOrigin(record.bankReconciliationDraft)
    || record.bankReconciliationEvidence && (
      isFinalizedOrigin(record.bankReconciliationEvidence.draftId)
      || isFinalizedOrigin(record.bankReconciliationEvidence.recordId)
    )
  );

  // Some finalization wrappers create rows after the report snapshot. Their
  // IDs are absent from report.linkedRecordIds, but their finalized same-book
  // origin still proves that their amount is already in the prior closing.
  ['transfers', 'receipts', 'adjustments', 'vendorAdvances', 'paytmSettlements',
    'bankTruthMovements', 'paytmPayoutPostings', 'salesRefunds', 'loanRepayments'].forEach(name => {
    (store[name] || []).forEach(record => {
      if (!hasFinalizedProvenance(record)) return;
      addLedger(record);
      add(covered, record.paymentReference);
    });
  });
  (store.reconciliationExpenses || []).forEach(record => {
    if (!hasFinalizedProvenance(record)) return;
    add(covered, record.adjustmentId);
    add(covered, record.settlementId);
  });
  Object.values(store.expenses || {}).forEach(expense => {
    [].concat(expense.payments || [], expense.reimbursementPayments || []).forEach(payment => {
      if (!hasFinalizedProvenance(payment)) return;
      if (expense.id && payment.id) add(covered, expense.id + '/' + payment.id);
      add(covered, payment.batchId);
    });
  });
  ['bankDateOverrides', 'bankReconciliationLinks'].forEach(name => {
    Object.entries(store[name] || {}).forEach(([id, record]) => {
      if (record && record.provisional !== true && hasFinalizedProvenance(record)) add(covered, id);
    });
  });
  return covered;
}

/**
 * Historical carry-forward is ledger arithmetic, not proof of a bank match.
 * Keep those exact identities available for a later evidence-only link even
 * when their ledger date is outside a statement's normal candidate window.
 * Never invent a deleted row or silently use an edited amount from the past.
 */
function pendingCarryForwardCandidates(store = {}, book = {}, movements = []) {
  const covered = finalizedMovementCoverageIds(store, book);
  const candidateById = new Map();
  const conflicts = [];
  const conflictKeys = new Set();
  const addConflict = (type, snapshot, record, message) => {
    const key = type + '|' + String(snapshot.id || '');
    if (conflictKeys.has(key)) return;
    conflictKeys.add(key);
    conflicts.push({ type, appId: String(snapshot.id || ''), reconciliationId: String(record.id || ''), message });
  };
  (book.imports || []).forEach(record => {
    (record.carryForwardMovements || []).forEach(snapshot => {
      const snapshotIds = identities(snapshot);
      if (!snapshotIds.length || snapshotIds.some(id => covered.has(id))) return;
      const exact = movements.filter(movement => movement && String(movement.id) === String(snapshot.id));
      const linked = exact.length ? exact : movements.filter(movement => identities(movement).some(id => snapshotIds.includes(id)));
      if (linked.length !== 1) {
        addConflict(linked.length ? 'ambiguous_carry_forward_identity' : 'missing_carry_forward_movement', snapshot, record,
          linked.length ? 'A carried ledger identity now identifies multiple entries. Review its original carry-forward before linking bank evidence.'
            : 'A carried ledger entry no longer exists. Review its original carry-forward; no replacement entry has been created.');
        return;
      }
      const movement = linked[0];
      if (identities(movement).some(id => covered.has(id))) return;
      if (cents(movement.debit) !== cents(snapshot.debit) || cents(movement.credit) !== cents(snapshot.credit)) {
        addConflict('changed_carry_forward_amount', snapshot, record,
          'A carried ledger amount changed after finalization. Review its original carry-forward before linking bank evidence.');
        return;
      }
      const key = String(movement.id);
      const existing = candidateById.get(key);
      const candidate = existing || Object.assign({}, movement, {
        sourceIds: (movement.sourceIds || []).slice(),
        priorPeriodCarryForward: true,
        carryForwardOriginalDate: movement.originalDate || movement.date || snapshot.date || '',
        carryForwardSourcePeriods: [],
      });
      const period = { reconciliationId: String(record.id || ''), from: String(record.from || ''),
        to: String(record.to || ''), ledgerDate: String(snapshot.date || '') };
      if (!candidate.carryForwardSourcePeriods.some(source => source.reconciliationId === period.reconciliationId)) {
        candidate.carryForwardSourcePeriods.push(period);
      }
      candidateById.set(key, candidate);
    });
  });
  return { candidates: Array.from(candidateById.values()), conflicts };
}

/**
 * Bring an existing ledger movement into the next statement's opening when
 * its date lies in the uncovered interval. The previous finalized bank
 * closing alone is not the ledger opening when a late cutoff-day payment was
 * recorded after that reconciliation.
 *
 * This only calculates a preview. It must never create a balance adjustment,
 * change payment dates, or mark the carried movements as bank-confirmed.
 * Callers supply the date represented by the previous closing and movements
 * already scoped to the account. A closing on or after the statement opening
 * cannot anchor that opening. Without an earlier closing, rebuild it from the
 * ledger base and every earlier movement, including finalized money and fees.
 * Finalized linked/covered IDs are excluded only from forward carry because
 * only that calculation starts from a balance already containing those IDs.
 */
function calculateReconciliationOpening(options = {}) {
  const cutoffDate = calendarDate(options.cutoffDate);
  const statementFrom = calendarDate(options.statementFrom);
  const hasPreviousClosing = !!cutoffDate && !!statementFrom && cutoffDate < statementFrom
    && options.previousClosing != null && Number.isFinite(parseFloat(options.previousClosing));
  const baselineCents = cents(hasPreviousClosing ? options.previousClosing : options.baseOpening);
  const excluded = new Set([].concat(Array.from(options.previouslyLinkedIds || []),
    Array.from(options.coveredMovementIds || [])).map(String));
  const carryForwardMovements = [];
  const historicalOpeningMovements = [];
  const seen = new Set();

  if (statementFrom) {
    // Prefer a consolidated movement to its individual aliases. A reimbursement
    // batch and one of its source payment rows represent the same money.
    const eligible = (options.movements || []).map((movement, index) => ({ movement, index, ids: identities(movement) }))
      .filter(({ movement, ids }) => {
        if (!movement || movement.accountingExcluded) return false;
        // An explicit accounting date can differ from a reconciliation bank
        // date. Use it for chronological reconstruction, never for carry.
        const date = calendarDate(!hasPreviousClosing && movement.accountingDate != null
          ? movement.accountingDate : movement.date);
        if (!date || date >= statementFrom) return false;
        return !hasPreviousClosing || date >= cutoffDate
          && !movement.automaticAxisTransferCharge && !ids.some(id => excluded.has(id));
      })
      .sort((a, b) => b.ids.length - a.ids.length || a.index - b.index);

    eligible.forEach(({ movement, ids }) => {
      if (ids.some(id => seen.has(id))) return;
      ids.forEach(id => seen.add(id));
      const target = hasPreviousClosing ? carryForwardMovements : historicalOpeningMovements;
      target.push(Object.assign({}, movement,
        movement.sourceIds ? { sourceIds: movement.sourceIds.slice() } : {}));
    });
    [carryForwardMovements, historicalOpeningMovements].forEach(rows => rows.sort((a, b) =>
      String(!hasPreviousClosing && a.accountingDate != null ? a.accountingDate : a.date)
        .localeCompare(String(!hasPreviousClosing && b.accountingDate != null ? b.accountingDate : b.date))
        || String(a.createdAt || '').localeCompare(String(b.createdAt || ''))
        || String(a.id || '').localeCompare(String(b.id || ''))));
  }

  const carryForwardCents = carryForwardMovements.reduce((total, movement) =>
    total + cents(movement.credit) - cents(movement.debit), 0);
  const historicalOpeningCents = historicalOpeningMovements.reduce((total, movement) =>
    total + cents(movement.credit) - cents(movement.debit), 0);
  const currentOpeningCents = baselineCents + historicalOpeningCents + carryForwardCents;
  const continuityComparisonAvailable = !!statementFrom && options.statementOpening != null
    && Number.isFinite(parseFloat(options.statementOpening));

  return {
    currentOpening: currentOpeningCents / 100,
    previousClosing: hasPreviousClosing ? baselineCents / 100 : null,
    openingSource: hasPreviousClosing ? 'previous_reconciliation' : statementFrom ? 'ledger_history' : 'base_opening',
    openingBaseline: baselineCents / 100,
    openingBaselineDate: hasPreviousClosing ? cutoffDate : '',
    historicalOpeningMovements,
    historicalOpeningNet: historicalOpeningCents / 100,
    carryForwardMovements,
    carryForwardNet: carryForwardCents / 100,
    continuityComparisonAvailable,
    openingContinuityGap: continuityComparisonAvailable
      ? (cents(options.statementOpening) - currentOpeningCents) / 100 : null,
  };
}

module.exports = { calculateReconciliationOpening, verifiedReconciliationAnchor,
  finalizedMovementCoverageIds, pendingCarryForwardCandidates };
