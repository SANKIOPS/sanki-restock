'use strict';

// Presentation and approval policy only. The engine prepares immutable source
// corrections; callers still enforce account access, ownership and finalization.
const crypto = require('crypto');
module.exports = function createPolicy(engine, deps) {
  const money = value => Math.round(Number(value) * 100) / 100;
  const net = row => engine.cents(row.credit || 0) - engine.cents(row.debit || 0);
  const amount = row => Math.abs(net(row));
  const reference = row => deps.reference(row.reference || row.bankDateOverride && row.bankDateOverride.bankReference || '');
  function prepare(store, draft, bank, movement, rowId, mode) {
    if (!deps.accountEligible(draft)) throw new Error('This amount rule is for bank accounts, not cash, credit cards, wallets or settlement clearing accounts.');
    if (deps.knownCharge && deps.knownCharge(bank, movement, draft)) throw new Error('This bank total includes a recognized transfer charge. Preserve the separate principal and Bank Charges allocation.');
    return engine.plan(store, draft, bank, movement, { ...deps.options(store, draft), bankRowId: rowId, mode });
  }
  function publicPlan(plan) {
    // Access to the physical bank does not grant access to a private invoice.
    // All operators can review the bank movement/delta; its PERSONAL bill total
    // remains in the private proposal, expense history and source-nature audit.
    const privateExpense = plan.sourceType === 'expense_payment' && plan.sourceNature === 'PERSONAL';
    return { eligible: true, fingerprint: plan.id, mode: plan.mode,
      originalAmount: plan.originalAmount, bankAmount: plan.bankAmount,
      amountDifference: plan.amountDifference, bankDate: plan.after.date,
      ...(privateExpense ? { privacyRestricted: true } : {
        expenseAmountBefore: plan.before.expense && plan.before.expense.amount,
        expenseAmountAfter: plan.after.expense && plan.after.expense.amount }) };
  }
  function automaticIdentity(bank, movement, banks, movements) {
    const bankRef = reference(bank), movementRef = reference(movement);
    if (bankRef.length >= 8 && bankRef === movementRef) {
      return banks.filter(row => reference(row) === bankRef).length === 1 &&
        movements.filter(row => reference(row) === bankRef).length === 1;
    }
    const near = (left, right) => {
      try { return left.date === right.date && Math.sign(net(left)) === Math.sign(net(right)) &&
        Math.abs(amount(left) - amount(right)) <= 100 && deps.identity(left, right); } catch { return false; }
    };
    return near(bank, movement) && movements.filter(row => near(bank, row)).length === 1 &&
      banks.filter(row => near(row, movement)).length === 1;
  }
  function decorate(store, draft, view) {
    const plans = [], candidates = [], originalStatuses = new Map();
    const enabled = deps.accountEligible(draft), movements = enabled ? deps.movements(store, draft) : [];
    const banks = (draft.transactions || []), errors = [];
    for (const row of view.rows || []) {
      if (!row.bank || !row.app || row.status === 'already_reconciled') continue;
      const resolution = row.resolution, pointer = resolution && resolution.bankAmountAuthority;
      if (pointer) {
        // Never serialize the immutable plan/source snapshot into a public row.
        // Persisted decisions carry only an opaque reference to a private plan.
        const planId = String(pointer.planId || '');
        row.resolution = { ...resolution, bankAmountAuthority: { planId } };
        try {
          const saved = Object.prototype.hasOwnProperty.call(store.bankAmountAuthorityProposals || {}, planId) && store.bankAmountAuthorityProposals[planId];
          if (!planId || !saved || saved.id !== planId) throw new Error('The private bank amount proposal is missing.');
          if (!enabled || !['accept_match', 'link_existing'].includes(resolution.action) || saved.movementId !== row.app.id || saved.bankRowId !== row.id) throw new Error('The saved bank amount decision no longer identifies this one-to-one match.');
          if (saved.draftId !== draft.id || saved.bankFingerprint !== engine.fingerprint(row.bank)) throw new Error('The private bank amount proposal belongs to different or changed statement evidence.');
          engine.validate(store, saved, deps.options(store, draft));
          row.bankAmountAuthority = publicPlan(saved); plans.push(saved);
        } catch (error) { row.bankAmountAuthorityBlocked = error.message; errors.push({ type: 'stale_bank_amount_authority', bankRowIds: [row.id], message: error.message + ' Undo and review this amount decision again; nothing was posted.' }); }
        continue;
      }
      if (!enabled || resolution || row.reversalReviewRequired || row.ledgerClaimedElsewhere || row.ownershipUnresolved) continue;
      let difference;
      try { difference = amount(row.bank) - amount(row.app); } catch { continue; }
      if (!difference) continue;
      let manual;
      try { manual = prepare(store, draft, row.bank, row.app, row.id, 'manual'); row.bankAmountAuthority = publicPlan(manual); }
      catch (error) { row.bankAmountAuthorityBlocked = error.message; }
      // Exact-reference or unique-party evidence is required in addition to
      // proximity. A small difference alone never establishes a match.
      const automatic = manual && Math.abs(difference) <= 100 && !row.axisTransferCharge && automaticIdentity(row.bank, row.app, banks, movements);
      if (automatic) {
        try {
          const plan = prepare(store, draft, row.bank, row.app, row.id, 'auto');
          originalStatuses.set(row.id, row.status); row.status = 'matched';
          row.bankAmountAuthority = publicPlan(plan); plans.push(plan); candidates.push(row);
          row.matchExplanation = [row.matchExplanation, 'Verified bank amount is authoritative within ₹1; correction posts only with Owner finalization'].filter(Boolean).join('. ');
        } catch (error) { row.bankAmountAuthorityBlocked = error.message; }
      } else if (row.status === 'matched' && !row.axisTransferCharge) {
        row.status = 'amount_mismatch';
      }
    }
    if (plans.length) {
      try {
        const sandbox = JSON.parse(JSON.stringify(store));
        engine.applyAll(sandbox, plans, deps.options(sandbox, draft));
      } catch (error) {
        errors.push({ type: 'bank_amount_authority_conflict', bankRowIds: plans.map(plan => plan.bankRowId), message: error.message + ' Review the selected source amounts; nothing was posted.' });
        candidates.forEach(row => { row.status = originalStatuses.get(row.id) || 'amount_mismatch'; row.bankAmountAuthorityBlocked = error.message; delete row.bankAmountAuthority; });
        plans.length = 0;
      }
    }
    if (errors.length) view.reconciliationConflicts.push(...errors);
    let delta = 0;
    for (const plan of plans) {
      const row = view.rows.find(value => value.id === plan.bankRowId);
      delta += net(row.bank) - net(row.app);
    }
    view.ledgerClosing = money(view.ledgerClosing + delta / 100);
    view.balanceDifference = money(view.bankClosing - view.ledgerClosing);
    view.balanceResolved = engine.cents(view.balanceDifference) === 0;
    view.summary = view.rows.reduce((result, row) => { result[row.status] = (result[row.status] || 0) + 1; return result; }, {});
    view.unresolved = view.rows.filter(row => !['matched', 'resolved', 'already_reconciled'].includes(row.status) || row.ownershipUnresolved).length + view.reconciliationConflicts.length;
    view.canFinalizeTransactions = view.unresolved === 0;
    view.canFinalize = view.canFinalizeTransactions && view.balanceResolved;
    view.bankAmountAuthorityCount = plans.length;
    view.bankAmountAuthorityFingerprint = plans.length ? crypto.createHash('sha256').update(plans.map(plan => plan.id).sort().join('|')).digest('hex') : '';
    Object.defineProperty(view, '_bankAmountAuthorityPlans', { value: plans });
    return view;
  }
  return { prepare, publicPlan, decorate };
};
