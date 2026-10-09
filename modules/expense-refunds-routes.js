'use strict';
const refunds = require('./expense-refunds');
const reject = (message, status = 400) => { const error = new Error(message); error.status = status; throw error; };
const clone = value => JSON.parse(JSON.stringify(value));

function checkRecordAccess(req, record, store, deps) {
    if (!deps.natures(req).includes(record.nature)) reject('You cannot access refunds for this entity.', 403);
    if (record.components.some(c => c.account && !deps.accountVisible(req, c.account))) reject('A receiving account is restricted to the Owner.', 403);
    const expenses = deps.expenses(store);
    record.sources.forEach(source => {
      const expense = source.expenseId && expenses.find(e => e.id === source.expenseId);
      if (expense && (!deps.canView(req, expense) || expense.ownerOnly && !deps.isOwner(req))) reject('A refund source is restricted to the Owner.', 403);
      const advance = source.advanceId && store.vendorAdvances.find(a => a.id === source.advanceId);
      const accounts = [...(source.paymentAllocations || []).map(p => p.account), advance && !advance.creditOnly && advance.account].filter(a => a && a !== 'Refund credit');
      if (accounts.some(a => !deps.accountVisible(req, a))) reject('The original paying account is restricted.', 403);
    });
}
function register(router, deps) {
  function authorized(req) { if (!deps.isAdmin(req)) reject('Only Admin or Owner can manage expense refunds and returns.', 403); }
  const checkRecord = (req, record, store) => checkRecordAccess(req, record, store, deps);
  function context(req, store, date = '') {
    const expenses = deps.expenses(store).filter(e => deps.canView(req, e) && deps.natures(req).includes(deps.nature(e.nature)));
    return { today: deps.today(), username: req.user.username, expenses,
      checkRecord: record => checkRecord(req, record, store),
      checkSource(source) {
        if (!deps.natures(req).includes(source.nature)) reject('You cannot log a refund for this entity.', 403);
        const expense = source.expenseId && expenses.find(e => e.id === source.expenseId);
        if (source.expenseId && !expense) reject('Expense access is restricted.', 403);
        if (expense && expense.ownerOnly && !deps.isOwner(req)) reject('This source is restricted to the Owner.', 403);
        const accounts = [source.creditOnly ? '' : source.account, ...(source.payments || []).filter(p => !p.creditOnly).map(p => p.account)].filter(Boolean);
        if (accounts.some(account => !deps.accountVisible(req, account))) reject('The original paying account is restricted.', 403);
      },
      checkAccount(sources, component, mode, receiver) {
        const entity = sources[0].nature, selected = String(component.account || '').trim();
        let account;
        if (receiver === 'payer') {
          const originals = sources.flatMap(s => s.payments || []).filter(p => p.personal).map(p => p.account);
          if (!selected || !originals.includes(selected) || !deps.accountVisible(req, selected)) reject('Select the original personal paying account; another person’s account cannot receive this refund.');
          account = selected;
        } else if (mode === 'card') {
          const originalCards = sources.flatMap(s => s.payments || []).map(p => p.creditCardId).filter(Boolean);
          const card = deps.card(req, selected);
          if (!card || !originalCards.length || originalCards.some(id => id !== card.id)) reject('A card reversal must go back to the original credit card.');
          account = deps.cardName(card);
        } else {
          account = deps.companyAccount(store, entity, selected);
          if (!account || !deps.accountVisible(req, account)) reject('Select an authorized receiving account for this entity.', 403);
        }
        if (mode === 'cash' && !/cash/i.test(account)) reject('Choose a cash account for a cash refund.');
        if (['upi', 'bank'].includes(mode) && (/cash/i.test(account) || deps.card(req, account))) reject('Choose a bank account for a UPI/bank refund.');
        if (!component.externalMovementId && date && deps.closedThrough(store, entity, account) >= date) reject('This receiving date is in a finalized reconciliation. Reopen the affected period through its normal correction workflow before adding a backdated refund.', 409);
        return account;
      },
      checkExistingMovement(component, entity, current) {
        const receipt = (current.receipts || []).find(r => r.id === component.externalMovementId && !r.accountingExcluded);
        const cardEntry = (current.reconciliationExpenses || []).find(r => r.id === component.externalMovementId && Number(r.amount) < 0 && r.creditCardStatementId);
        const movement = receipt || cardEntry;
        if (!movement || deps.nature(movement.nature) !== entity || movement.account !== component.account) reject('Select an existing refund credit in the same entity and receiving account.');
        if (receipt && !['refund', 'expense_refund'].includes(receipt.receiptType)) reject('Only an existing refund receipt can be linked here; other receipts must be corrected in their source workflow.');
        if (component.receiver === 'payer' && (!receipt || !receipt.personalRefund)) reject('An existing company receipt cannot be treated as a refund to a personal payer. Correct that receipt in its source workflow first.');
        if (receipt && (receipt.manualSaleId || deps.receiptUsed(current, receipt.id))) reject('This receipt is already used by another accounting workflow.', 409);
        if (cardEntry && component.mode !== 'card') reject('Link a card-statement credit only as a card reversal.');
        if (cardEntry && cardEntry.expenseRefundReceiptId) reject('This statement credit is already linked to a recorded refund receipt. Correct that refund in its original workflow instead of recording it again.', 409);
        const used = refunds.records(current).filter(refunds.received).flatMap(r => r.components).filter(c => c.externalMovementId === component.externalMovementId || c.receiptId === component.externalMovementId).reduce((sum, c) => sum + refunds.cents(c.amount), 0);
        if (used + refunds.cents(component.amount) > Math.abs(refunds.cents(movement.amount))) reject('The existing refund movement is already fully allocated.', 409);
        if (movement.date !== date) reject('Use the actual receiving date of the existing refund movement.');
        const reference = movement.bankReference || movement.reference || '';
        if (reference && component.reference && reference !== component.reference) reject('The reference does not match the existing refund movement.');
      } };
  }
  function view(req, store, query = {}) {
    const allowed = deps.natures(req), selected = query.nature || '', today = deps.today();
    if (selected && !allowed.includes(selected)) reject('You cannot view this accounting entity.', 403);
    const permitted = r => {
      if (!allowed.includes(r.nature) || selected && r.nature !== selected) return false;
      try { checkRecord(req, r, store); return true; } catch { return false; }
    };
    const all = refunds.records(store).filter(permitted).map(r => refunds.viewRecord(store, r, today));
    const search = String(query.search || '').trim().toLowerCase(), from = query.from || '', to = query.to || '', status = query.status || '';
    const list = all.filter(r => (!from || r.date >= from) && (!to || r.date <= to) && (!status || r.displayStatus === status) &&
      (!search || [r.id, r.vendor, r.reason, ...r.sources.map(s => s.reference), ...r.components.flatMap(c => [c.reference, c.issuer, c.account])].join(' ').toLowerCase().includes(search)))
      .sort((a, b) => (b.date + b.id).localeCompare(a.date + a.id));
    const activeList = list.filter(refunds.active), receivedList = activeList.filter(refunds.received);
    const summary = { moneyReceived: 0, creditsIssued: 0, creditsAvailable: 0, creditsExpired: 0, pending: 0, creditNotes: 0, personalReceived: 0 };
    receivedList.forEach(r => r.components.forEach(c => {
      if (c.mode === 'credit_note') summary.creditNotes += c.amount;
      else if (c.receiver === 'payer') summary.personalReceived += c.amount;
      else if (refunds.MONEY_MODES.has(c.mode)) summary.moneyReceived += c.amount;
      else { summary.creditsIssued += c.amount; summary[c.expired ? 'creditsExpired' : 'creditsAvailable'] += c.remainingCredit; }
    }));
    activeList.forEach(r => summary.pending += r.pendingAmount);
    Object.keys(summary).forEach(k => summary[k] = refunds.money(summary[k]));
    const ctx = context(req, store), sources = refunds.catalog(store, ctx.expenses).filter(s => {
      try { ctx.checkSource(s); return (!selected || s.nature === selected) && allowed.includes(s.nature); } catch { return false; }
    });
    const recoverables = ctx.expenses.filter(e => { try { ctx.checkSource(refunds.catalog(store, [e])[0]); return true; } catch { return false; } }).map(e => ({ expenseId: e.id, nature: deps.nature(e.nature), claimant: deps.claimant(e),
      amount: refunds.reimbursementPosition(store, e).recoverable })).filter(r => r.amount > 0 && (!selected || r.nature === selected));
    const existingMovements = (store.receipts || []).filter(r => !r.accountingExcluded && ['refund', 'expense_refund'].includes(r.receiptType) && allowed.includes(deps.nature(r.nature)) && deps.accountVisible(req, r.account))
      .map(r => ({ id: r.id, nature: deps.nature(r.nature), date: r.date, amount: r.amount, account: r.account, reference: r.bankReference || '', source: r.source, mode: r.creditCardRefund ? 'card' : /cash/i.test(r.account) ? 'cash' : 'bank' }));
    (store.reconciliationExpenses || []).filter(r => r.creditCardStatementId && !r.expenseRefundReceiptId && Number(r.amount) < 0 && allowed.includes(deps.nature(r.nature)) && deps.accountVisible(req,r.account) && (!r.ownerOnly || deps.isOwner(req)))
      .forEach(r => existingMovements.push({ id: r.id, nature: deps.nature(r.nature), date: r.date, amount: Math.abs(r.amount), account: r.account, reference: '', source: r.particulars, mode: 'card' }));
    existingMovements.forEach(m => {
      const used = refunds.records(store).filter(refunds.received).flatMap(r => r.components).filter(c => c.externalMovementId === m.id || c.receiptId === m.id).reduce((sum, c) => sum + c.amount, 0);
      m.remaining = refunds.money(Math.max(0, m.amount - used));
    });
    const recoveries = (store.receipts || []).filter(r => r.refundRecoveryExpenseId && allowed.includes(deps.nature(r.nature)) && (!selected || r.nature === selected) && deps.accountVisible(req, r.account));
    return { success: true, refunds: list, summary, sources, existingMovements: existingMovements.filter(m => m.remaining > 0), recoverables, recoveries, modes: refunds.MODES, reasons: refunds.REASONS, today };
  }
  function handler(fn) { return (req, res) => { try { authorized(req); fn(req, res); } catch (error) { res.status(error.status || 400).json({ success: false, error: error.message }); } }; }
  router.get('/api/expenses/expense-refunds', handler((req, res) => res.json(view(req, deps.loadStore(), req.query))));
  router.post('/api/expenses/expense-refunds/preview', handler((req, res) => {
    const store = deps.loadStore(), ctx = context(req, store, req.body.date), prepared = refunds.prepare(store, req.body, ctx);
    res.json({ success: true, preview: prepared, already: !!prepared.already,
      impact: { moneyIntoAccounts: (prepared.components || []).filter(c => refunds.MONEY_MODES.has(c.mode) && !c.externalMovementId && c.receiver === 'company').map(c => ({ account: c.account, amount: c.amount, personal: false })),
        billCredit: prepared.status === 'pending' ? 0 : refunds.money((prepared.sources || []).reduce((sum, s) => sum + s.billCreditAmount, 0)),
        personalReimbursementReview: (prepared.sources || []).filter(s => s.personalReturned > 0).map(s => ({ expenseId: s.expenseId, refundToPayer: s.personalReturned, prior: refunds.reimbursementPosition(store, store.expenses[s.expenseId]) })) } });
  }));
  router.post('/api/expenses/expense-refunds', handler((req, res) => {
    const store = deps.loadStore(), ctx = context(req, store, req.body.date), prepared = refunds.prepare(store, req.body, ctx);
    const result = refunds.post(store, prepared, ctx);
    if (!result.already) {
      result.refund.components.filter(c => c.vendorAdvanceId).forEach(c => deps.ensureVendor(store,result.refund.nature,c.issuer));
      deps.audit(store, req, result.refund.status === 'pending' ? 'EXPENSE_RETURN_LOGGED' : 'EXPENSE_REFUND_RECORDED', 'expense_refund', result.refund.id,
        { nature: result.refund.nature, after: result.refund, note: result.refund.reason + (result.refund.proofs.length ? '' : ' · proof not attached (optional)') });
      deps.saveStore(store);
    }
    res.json({ success: true, ...result, view: view(req, store, { nature: result.refund.nature }) });
  }));
  router.post('/api/expenses/expense-refunds/:id/void', handler((req, res) => {
    const store = deps.loadStore(), record = refunds.records(store).find(r => r.id === req.params.id), reason = String(req.body.reason || '').trim();
    if (!record || !refunds.active(record)) reject('Choose an active refund/return.');
    checkRecord(req, record, store);
    if (!reason) reject('A correction reason is required.');
    if (refunds.records(store).some(r => refunds.active(r) && r.parentId === record.id)) reject('Reverse the linked refund receipts first; their pending return cannot be voided.', 409);
    if (record.sources.some(s => (store.receipts || []).some(r => !r.accountingExcluded && r.refundRecoveryExpenseId === s.expenseId))) reject('Undo the employee recovery first, before reversing the refund that created it.', 409);
    const ids = record.components.map(c => c.receiptId || c.externalMovementId).filter(Boolean);
    if (ids.some(id => deps.movementLocked(store, id))) reject('This refund is linked to reconciliation. Undo/reopen its match through the normal reconciliation workflow before correcting it.', 409);
    record.components.forEach(c => {
      const credit = store.vendorAdvances.find(a => a.id === c.vendorAdvanceId);
      if (credit && (credit.applications || []).some(a => !a.accountingExcluded)) reject('This credit has been redeemed. Correct its redemption before voiding the original refund.', 409);
      if (credit && (credit.refundIds || []).length) reject('This credit has been converted to a refund. Reverse that refund first.', 409);
    });
    const before = clone(record);
    record.sources.filter(s => s.advanceId).forEach(s => {
      const advance = store.vendorAdvances.find(a => a.id === s.advanceId);
      if (!advance) reject('The original advance is missing; review its history before correcting this refund.', 409);
      if (record.status === 'received') advance.remainingAmount = refunds.money(advance.remainingAmount + s.amount);
      advance.refundIds = (advance.refundIds || []).filter(id => id !== record.id);
    });
    (store.receipts || []).filter(r => r.expenseRefundId === record.id).forEach(r => { r.accountingExcluded = true; r.voidedBy = req.user.username; r.voidReason = reason; });
    store.vendorAdvances.filter(a => a.expenseRefundId === record.id).forEach(a => { a.accountingExcluded = true; a.remainingAmount = 0; });
    Object.assign(record, { status: 'voided', voidReason: reason, voidedBy: req.user.username, voidedAt: new Date().toISOString() });
    deps.audit(store, req, 'EXPENSE_REFUND_VOIDED', 'expense_refund', record.id, { nature: record.nature, before, after: record, note: reason });
    deps.saveStore(store); res.json({ success: true, refund: record, view: view(req, store, { nature: record.nature }) });
  }));
  router.post('/api/expenses/expense-refunds/:id/redeem', handler((req, res) => {
    const store = deps.loadStore(), record = refunds.records(store).find(r => r.id === req.params.id && refunds.received(r));
    const plan = refunds.prepareRedemption(store, record, req.body, { today: deps.today(),
      checkRecord: r => checkRecord(req, r, store), checkExpense: expense => {
        if (!deps.canView(req, expense) || !deps.natures(req).includes(deps.nature(expense.nature))) reject('Select an accessible credit and approved expense.');
        if (expense.ownerOnly && !deps.isOwner(req)) reject('The receiving expense is restricted to the Owner.', 403);
      } });
    if (plan.prior) return res.json({ success: true, already: true, view: view(req, store, { nature: record.nature }) });
    const application = refunds.applyRedemption(store, plan, { username: req.user.username });
    deps.audit(store, req, 'EXPENSE_REFUND_CREDIT_REDEEMED', 'expense_refund', record.id, { nature: record.nature, after: application, note: plan.reason + ' · no bank/cash movement' });
    deps.saveStore(store); res.json({ success: true, view: view(req, store, { nature: record.nature }) });
  }));
  router.post('/api/expenses/expense-refunds/:id/redemptions/:useId/void', handler((req, res) => {
    const store = deps.loadStore(), record = refunds.records(store).find(r => r.id === req.params.id && refunds.received(r));
    if (!record) reject('Refund credit not found.'); checkRecord(req, record, store);
    const credit = store.vendorAdvances.find(a => a.expenseRefundId === record.id && (a.applications || []).some(x => x.id === req.params.useId && !x.accountingExcluded));
    const use = credit && credit.applications.find(x => x.id === req.params.useId && !x.accountingExcluded), reason = String(req.body.reason || '').trim();
    if (!use || !reason) reject('Choose an active redemption and enter a correction reason.');
    const expense = store.expenses[use.expenseId];
    if (!expense || refunds.hasSource(store, expense.id)) reject('Reverse refunds linked to the redeemed expense before correcting its credit application.', 409);
    const application = (expense.vendorAdvanceApplications || []).find(a => a.vendorAdvanceId === credit.id && a.id === use.id);
    if (!application) reject('The linked application is missing. Review its history before correction.', 409);
    const before = clone(use);
    Object.assign(use, { accountingExcluded: true, voidReason: reason, voidedBy: req.user.username, voidedAt: new Date().toISOString() });
    Object.assign(application, use);
    credit.remainingAmount = refunds.money(credit.remainingAmount + use.amount);
    expense.paidAmount = refunds.money(expense.paidAmount - use.amount);
    expense.status = refunds.expenseDue(store, expense) === 0 ? 'paid' : expense.paidAmount > 0 ? 'partially_paid' : 'approved';
    deps.audit(store, req, 'EXPENSE_REFUND_REDEMPTION_VOIDED', 'expense_refund', record.id, { nature: record.nature, before, after: use, note: reason });
    deps.saveStore(store); res.json({ success: true, view: view(req, store, { nature: record.nature }) });
  }));
  router.post('/api/expenses/expense-refunds/recoveries', handler((req, res) => {
    const store = deps.loadStore(), b = req.body, ctx = context(req, store, b.date), expense = ctx.expenses.find(e => e.id === b.expenseId);
    if (!expense || !refunds.hasSource(store, expense.id)) reject('Select a refunded expense with an employee recovery due.');
    ctx.checkSource(refunds.catalog(store, [expense])[0]);
    const key = String(b.requestId || ''), value = Number(b.amount), reason = String(b.reason || '').trim();
    if (!/^[a-zA-Z0-9-]{8,100}$/.test(key) || !refunds.validDate(b.date) || b.date > deps.today() || !reason || !Number.isFinite(value) || value <= 0 || Math.abs(value * 100 - refunds.cents(value)) > .00001) reject('Enter a valid date, recovery amount and reason.');
    const prior = store.receipts.find(r => r.refundRecoveryRequestId === key);
    if (prior) {
      if (prior.refundRecoveryExpenseId !== expense.id || prior.amount !== value || prior.date !== b.date || prior.account !== b.account) reject('This request was already used for a different recovery.', 409);
      return res.json({ success: true, already: true, view: view(req, store, { nature: expense.nature }) });
    }
    const position = refunds.reimbursementPosition(store, expense, b.date);
    if (value > Math.min(position.recoverable, refunds.reimbursementPosition(store, expense).recoverable)) reject('Recovery exceeds the employee amount still due.', 409);
    const earliest = refunds.records(store).filter(refunds.received).filter(r => r.sources.some(s => s.expenseId === expense.id && s.personalReturned > 0)).map(r => r.date).sort()[0];
    if (!earliest || b.date < earliest) reject('Recovery cannot precede the refund received by the employee.');
    const account = ctx.checkAccount([{ nature: deps.nature(expense.nature) }], b, b.mode, 'company');
    if (!['cash', 'upi', 'bank'].includes(b.mode)) reject('Select cash, UPI or bank for an employee recovery.');
    const id = 'RFREC-' + key;
    const receipt = { id, nature: deps.nature(expense.nature), account, amount: value, date: b.date, receiptType: 'refund_recovery', refundRecoveryExpenseId: expense.id,
      refundRecoveryRequestId: key, source: 'Employee recovery · ' + deps.claimant(expense) + ' · ' + expense.id, note: reason,
      bankReference: String(b.reference || ''), proof: '', proofException: 'Refund recovery proof optional', createdBy: req.user.username, createdAt: new Date().toISOString() };
    store.receipts.push(receipt); deps.audit(store, req, 'EXPENSE_REFUND_EMPLOYEE_RECOVERY', 'expense', expense.id, { nature: expense.nature, after: receipt, note: reason });
    deps.saveStore(store); res.json({ success: true, view: view(req, store, { nature: expense.nature }) });
  }));
  router.post('/api/expenses/expense-refunds/recoveries/:id/void', handler((req, res) => {
    const store = deps.loadStore(), receipt = store.receipts.find(r => r.id === req.params.id && r.refundRecoveryExpenseId && !r.accountingExcluded), reason = String(req.body.reason || '').trim();
    if (!receipt || !reason) reject('Choose an active recovery and enter a correction reason.');
    if (!deps.natures(req).includes(deps.nature(receipt.nature)) || !deps.accountVisible(req, receipt.account)) reject('Recovery access is restricted.', 403);
    if (deps.movementLocked(store, receipt.id)) reject('Undo/reopen this recovery’s reconciliation before correcting it.', 409);
    const before = clone(receipt); Object.assign(receipt, { accountingExcluded: true, voidReason: reason, voidedBy: req.user.username, voidedAt: new Date().toISOString() });
    deps.audit(store, req, 'EXPENSE_REFUND_RECOVERY_VOIDED', 'expense', receipt.refundRecoveryExpenseId, { nature: receipt.nature, before, after: receipt, note: reason });
    deps.saveStore(store); res.json({ success: true, view: view(req, store, { nature: receipt.nature }) });
  }));
}
module.exports = { register, checkRecordAccess };
