'use strict';

const crypto = require('crypto');
function calendarDate(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return '';
  const date = new Date(value + 'T00:00:00Z');
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value ? value : '';
}
const own = (obj, key) => Object.prototype.hasOwnProperty.call(obj || {}, key);
const keyFor = (nature, account) => JSON.stringify([nature, account]);
const digest = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
function fail(status, message) { throw Object.assign(Error(message), { status }); }
function opening(store, nature, account) {
  const amounts = nature === 'SANKI' ? store.openingBalances : (store.openingBalancesByNature || {})[nature];
  const dates = nature === 'SANKI' ? store.openingBalanceDates : (store.openingBalanceDatesByNature || {})[nature];
  return { amount: own(amounts, account) ? Number(amounts[account]) : null, effectiveDate: (dates || {})[account] || '' };
}
function context(store, nature, account, bookKey) {
  const drafts = Object.values(store.bankReconciliationDrafts || {}).filter(d => d.nature === nature && d.account === account);
  const book = (store.bankStatements || {})[bookKey(nature, account)] || {};
  const cash = (store.cashReconciliations || []).filter(c => (c.nature || 'SANKI') === nature && (c.account || 'Counter Cash') === account && ['pending','approved'].includes(c.status));
  const locked = !!(book.reconciledThrough || book.lastReconciliation || (book.imports || []).length || cash.length);
  const reason = drafts.length ? 'An active statement exists. Stage its opening / carry-forward in universal reconciliation; do not create a second opening correction.' : locked ? 'This ledger has finalized reconciliation or pending/approved cash verification. Correct it through the reconciliation workflow, not by replacing its opening.' : '';
  const current = opening(store, nature, account), proposal = (store.openingBalanceProposals || {})[keyFor(nature, account)] || null;
  const baseRevision = digest({ current, book, cash, drafts });
  return { current, proposal, baseRevision, revision: digest({ baseRevision, proposal }), locked, reason, drafts: drafts.map(d => ({ id: d.id, from: d.summary && d.summary.from || '', to: d.summary && d.summary.to || '', openingResolution: d.openingResolution ? { amount: d.openingResolution.amount, reason: d.openingResolution.reason, by: d.openingResolution.by, at: d.openingResolution.at } : null })) };
}
function input(body, today) {
  const raw = typeof body.amount === 'number' ? String(body.amount) : typeof body.amount === 'string' ? body.amount.trim() : '';
  if (!/^-?\d+(?:\.\d{1,2})?$/.test(raw) || !Number.isSafeInteger(Math.round(Number(raw) * 100))) fail(400, 'Enter an opening balance with at most two decimal places. Zero and negative balances are allowed.');
  const effectiveDate = calendarDate(body.effectiveDate);
  if (!effectiveDate || effectiveDate > today) fail(400, 'Choose a valid opening date that is not in the future.');
  const reason = typeof body.reason === 'string' ? body.reason.trim() : '';
  if (!reason || reason.length > 1000) fail(400, 'Explain the source of the opening balance (up to 1,000 characters).');
  return { amount: Number(raw), effectiveDate, reason };
}
function registerOpeningBalances(router, deps) {
  const base = '/api/expenses/opening-balances';
  function actor(req) { if (!deps.isAdmin(req)) fail(403, 'Owner/Admin only.'); }
  function scope(req, store, body) {
    const nature = String(body.nature || '');
    if (!deps.natures(req).includes(nature)) fail(403, 'You cannot access this accounting entity.');
    const names = deps.accounts(req, store, nature);
    const account = names.find(name => name === body.account);
    if (!account) fail(403, 'Select a ledger available to you in this entity.');
    return { nature, account, row: context(store, nature, account, deps.bookKey) };
  }
  function handle(work) { return (req, res) => { try { work(req, res); } catch (error) { res.status(error.status || 503).json({ success: false, error: error.status ? error.message : 'Opening balances could not be saved safely. Refresh and try again.' }); } }; }
  router.get(base, handle((req, res) => {
    actor(req);
    const nature = String(req.query.nature || 'SANKI');
    if (!deps.natures(req).includes(nature)) fail(403, 'You cannot access this accounting entity.');
    // Strict migration-free reads: opening management must never run historical
    // financial repairs, and an unreadable file must never become a blank store.
    const store = deps.read();
    const rows = deps.accounts(req, store, nature).map(account => ({ account, ...context(store, nature, account, deps.bookKey) }));
    res.json({ success: true, nature, canApply: deps.isOwner(req), rows });
  }));
  router.post(base + '/stage', handle((req, res) => {
    actor(req);
    const values = input(req.body || {}, deps.today()), store = deps.read();
    const { nature, account, row } = scope(req, store, req.body || {});
    if (row.reason) fail(409, row.reason);
    if (req.body.revision !== row.revision) fail(409, 'This opening balance or proposal changed. Refresh and review the latest values.');
    const proposal = { id: crypto.randomUUID(), ...values, baseRevision: row.baseRevision, stagedBy: req.user.username, stagedAt: new Date().toISOString() };
    store.openingBalanceProposals = store.openingBalanceProposals || {};
    store.openingBalanceProposals[keyFor(nature, account)] = proposal;
    deps.audit(store, req, 'OPENING_BALANCE_STAGED', 'account', account, { nature, account, before: row.proposal, after: proposal, note: values.reason + ' · Draft only; recorded balances and reconciliations unchanged.' });
    deps.save(store);
    res.json({ success: true, stagedOnly: true, row: { account, ...context(store, nature, account, deps.bookKey) } });
  }));
  router.post(base + '/apply', handle((req, res) => {
    actor(req);
    if (!deps.isOwner(req)) fail(403, 'Only the Owner can apply a staged opening balance.');
    if ((req.body || {}).acknowledge !== true) fail(400, 'Explicitly acknowledge that applying changes the recorded ledger balance.');
    const store = deps.read(), { nature, account, row } = scope(req, store, req.body || {});
    if (row.reason) fail(409, row.reason);
    if (!row.proposal || req.body.proposalId !== row.proposal.id || req.body.revision !== row.revision || row.proposal.baseRevision !== row.baseRevision) fail(409, 'The staged opening or ledger changed. Refresh and stage a fresh proposal.');
    const values = input(row.proposal, deps.today());
    if (nature === 'SANKI') {
      store.openingBalances = store.openingBalances || {}; store.openingBalanceDates = store.openingBalanceDates || {};
      store.openingBalances[account] = values.amount; store.openingBalanceDates[account] = values.effectiveDate;
    } else {
      store.openingBalancesByNature = store.openingBalancesByNature || {}; store.openingBalanceDatesByNature = store.openingBalanceDatesByNature || {};
      store.openingBalancesByNature[nature] = store.openingBalancesByNature[nature] || {}; store.openingBalanceDatesByNature[nature] = store.openingBalanceDatesByNature[nature] || {};
      store.openingBalancesByNature[nature][account] = values.amount; store.openingBalanceDatesByNature[nature][account] = values.effectiveDate;
    }
    delete store.openingBalanceProposals[keyFor(nature, account)];
    deps.audit(store, req, 'OPENING_BALANCE_CHANGED', 'account', account, { nature, account, before: row.current, after: values, proposalId: row.proposal.id, stagedBy: row.proposal.stagedBy, note: values.reason });
    deps.save(store);
    res.json({ success: true, row: { account, ...context(store, nature, account, deps.bookKey) } });
  }));
}
module.exports = { registerOpeningBalances, input, opening, context };
