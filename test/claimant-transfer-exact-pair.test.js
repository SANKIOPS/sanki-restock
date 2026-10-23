'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Account labels are application policy constants. All records, references,
// dates, amounts and proof paths below are synthetic, never a production export.
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-CLAIMANT-PAIR', rows: [] } };
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'claimant-exact-pair-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
const file = path.join(temp, 'expenses.json');
fs.writeFileSync(file, JSON.stringify({ expenses: {}, receipts: [], receivables: {}, oneTimeMigrations: {} }));
const { router, applyFinalizedInternalTransfers } = require('../modules/expenses');
const bank = 'IndusInd Bank 7883', claimant = 'Arshpreet 1919';
const date = '2098-06-14', amount = 731.25, opening = 10000;
const admin = { role: 'admin', username: 'prashant' };
const clone = value => JSON.parse(JSON.stringify(value));
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));
let baseline, sequence = 0;

function dispatch(route, body = {}, options = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, finished = false;
    const finish = payload => { if (!finished) { finished = true; resolve({ status, body: payload }); } };
    const headers = {};
    const req = {
      method: options.method || 'POST', path: route, url: route, originalUrl: route, baseUrl: '', body,
      query: options.query || {}, params: {}, headers: { 'user-agent': 'Synthetic exact-pair regression' },
      get(name) { return this.headers[String(name).toLowerCase()] || ''; },
      user: { role: options.role || 'owner', roles: [options.role || 'owner'], username: options.username || 'synthetic-owner', permissions: options.permissions || {} }
    };
    const res = {
      statusCode: 200, headersSent: false,
      status(value) { status = this.statusCode = value; return this; },
      setHeader(key, value) { headers[key] = value; }, getHeader(key) { return headers[key]; }, removeHeader(key) { delete headers[key]; },
      json(payload) { this.headersSent = true; finish(payload); return this; },
      send(payload) { this.headersSent = true; finish(payload); return this; },
      end(payload) { this.headersSent = true; finish(payload); return this; }
    };
    router.handle(req, res, error => error ? reject(error) : finish(undefined));
  });
}

test.before(async () => {
  assert.equal((await dispatch('/api/expenses/config', {}, { method: 'GET' })).status, 200);
  baseline = read();
});
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));

function reset(draft) {
  const store = {
    ...clone(baseline), expenses: {}, transfers: [], receipts: [], receivables: {}, adjustments: [],
    reconciliationExpenses: [], loanRepayments: [], vendorAdvances: [], vendorOpeningPayables: [],
    salesRefunds: [], bankTruthMovements: [], bankDateOverrides: {}, bankReconciliationLinks: {},
    bankReconciliationDrafts: draft ? { [draft.id]: clone(draft) } : {}, bankReconciliationApprovals: {},
    bankStatements: { 'Tiana 0425': { imports: [{ id: 'BST-SYNTHETIC-PRESERVE', reconciliationRows: [] }], transactions: {}, lastReconciliation: { to: '2098-05-01', closingBalance: 19 } } },
    openingBalances: {}, openingBalancesByNature: { PERSONAL: { [bank]: opening }, SAMAST: { [bank]: opening } },
    physicalBankOpeningBalances: { [bank]: opening }, auditLog: [], auditSeq: 0, transferSeq: 0, adjSeq: 0
  };
  write(store);
  return store;
}

function fixture({ incoming = false, manual = false, nature = 'SAMAST', account = bank, extra = {} } = {}) {
  const id = 'BRD-SYNTHETIC-PAIR-' + (++sequence), debit = incoming ? 0 : amount, credit = incoming ? amount : 0;
  const draft = {
    id, nature, account,
    transactions: [{ date, debit, credit, balance: opening + credit - debit, reference: 'SYNTHETIC-PAIR-REF-' + sequence, description: 'Synthetic claimant transfer' }],
    summary: { from: date, to: date, openingBalance: opening, closingBalance: opening + credit - debit, totalDebits: debit, totalCredits: credit, validated: true },
    resolutions: {}, matchingPolicy: 'balanced_date_amount_v5', createdBy: 'prashant', createdAt: date + 'T10:00:00Z', expiresAt: '2099-01-01T00:00:00Z',
    ...extra
  };
  if (manual) draft.transactionOwnership = { 'bank-0': { nature: 'SAMAST', source: 'manual', reason: 'Synthetic confirmed business attribution', by: 'synthetic-owner', at: date + 'T09:00:00Z' } };
  reset(draft);
  return draft;
}

const expectedClass = (fromNature, toNature) => fromNature === toNature ? 'internal_transfer' : fromNature === 'PERSONAL' ? 'owner_contribution' : 'owner_withdrawal';
function manualBody(bankNature, claimantNature, reverse = false, extra = {}) {
  const fromNature = reverse ? claimantNature : bankNature, toNature = reverse ? bankNature : claimantNature;
  return {
    claimantTransfer: true, fromNature, fromAccount: reverse ? claimant : bank,
    toNature, toAccount: reverse ? bank : claimant, classification: expectedClass(fromNature, toNature),
    amount, date, proof: '/synthetic-claimant-proof.jpg', note: 'Synthetic verified claimant funds transfer', ...extra
  };
}
function resolveBody(draft, claimantNature, extra = {}) {
  return { draftId: draft.id, rowId: 'bank-0', action: 'create_internal_transfer', claimantTransfer: true, otherNature: claimantNature, otherAccount: claimant, principalAmount: amount, chargeAmount: 0, reason: 'Synthetic verified claimant transfer', ...extra };
}
function incomingBody(draft, claimantNature, extra = {}) {
  return { draftId: draft.id, rowId: 'bank-0', sourceKind: 'internal', claimantTransfer: true, fromNature: claimantNature, fromAccount: claimant, note: 'Synthetic claimant transfer received', ...extra };
}
function assertNoDerivedEntries(store = read()) {
  assert.deepEqual(store.expenses, {}, 'a claimant transfer must not create an expense');
  assert.deepEqual(store.receipts, [], 'a claimant transfer must not create income');
  assert.deepEqual(store.adjustments, []);
  assert.equal(store.reconciliationExpenses.filter(row => row.date === date).length, 0);
  assert.deepEqual(store.loanRepayments, []);
  assert.equal(store.bankStatements['Tiana 0425'].imports[0].id, 'BST-SYNTHETIC-PRESERVE');
}
async function assertRejectedUnchanged(route, body, options = {}) {
  // Complete ordinary load-time normalization before comparing the write boundary.
  await dispatch('/api/expenses/config', {}, { method: 'GET' });
  const before = fs.readFileSync(file, 'utf8');
  const result = await dispatch(route, body, options);
  assert.ok(result.status >= 400, JSON.stringify(result));
  assert.equal(fs.readFileSync(file, 'utf8'), before, 'a rejected request must leave every accounting record unchanged');
  return result;
}

test('Prashant receives six exact claimant routes without global ownership or PERSONAL visibility', async () => {
  reset();
  const result = await dispatch('/api/expenses/config', {}, { ...admin, method: 'GET' });
  assert.equal(result.status, 200);
  const cfg = result.body, routes = cfg.claimantTransferRoutes;
  assert.equal(routes.length, 6);
  const actual = routes.map(row => [row.fromNature, row.fromAccount, row.toNature, row.toAccount, row.classification].join('|')).sort();
  const expected = [['PERSONAL', 'SANKI'], ['PERSONAL', 'SAMAST'], ['SAMAST', 'SAMAST']].flatMap(([a, b]) => [false, true].map(reverse => {
    const row = manualBody(a, b, reverse);
    return [row.fromNature, row.fromAccount, row.toNature, row.toAccount, row.classification].join('|');
  })).sort();
  assert.deepEqual(actual, expected);
  assert.ok(!(cfg.ownTransferAccounts || []).some(row => row.account === claimant), 'claimant account is not an owned bank');
  assert.ok(!cfg.approvalNatures.includes('PERSONAL'));
  for (const key of ['accountsByNature', 'bankAccountsByNature', 'ledgerAccountsByNature']) assert.deepEqual(cfg[key].PERSONAL, [], key);
  assert.equal((await dispatch('/api/expenses/account-ledger', {}, { ...admin, method: 'GET', query: { nature: 'PERSONAL', account: bank } })).status, 403);
  assertNoDerivedEntries();
});

for (const [bankNature, claimantNature] of [['PERSONAL', 'SANKI'], ['PERSONAL', 'SAMAST'], ['SAMAST', 'SAMAST']]) {
  for (const reverse of [false, true]) test('Admin Prashant records exact manual ' + bankNature + ' bank / ' + claimantNature + ' claimant pair ' + (reverse ? 'to bank' : 'from bank'), async () => {
    reset();
    const body = manualBody(bankNature, claimantNature, reverse);
    const result = await dispatch('/api/expenses/transfers', body, admin);
    assert.equal(result.status, 200, JSON.stringify(result.body));
    const transfers = read().transfers;
    assert.equal(transfers.length, 1);
    for (const key of ['fromNature', 'fromAccount', 'toNature', 'toAccount', 'classification', 'amount', 'date', 'proof']) assert.equal(transfers[0][key], body[key], key);
    assertNoDerivedEntries();
  });
}

test('Owner and explicitly delegated Admin can use the same exact-pair capability', async () => {
  for (const actor of [{}, { role: 'admin', username: 'synthetic-delegate', permissions: { personalAccounting: true } }]) {
    reset();
    const result = await dispatch('/api/expenses/transfers', manualBody('PERSONAL', 'SAMAST'), actor);
    assert.equal(result.status, 200, JSON.stringify(result.body));
    assert.equal(read().transfers.length, 1);
    assertNoDerivedEntries();
  }
});

test('the narrow capability rejects other roles, arbitrary personal banks, other claimants and altered book pairs', async () => {
  const cases = [
    { actor: { role: 'admin', username: 'synthetic-unrelated-admin' } },
    { actor: { role: 'accounting', username: 'synthetic-accountant' } },
    { actor: { role: 'claimant', username: 'prashant' } },
    { actor: { role: 'claimant', username: 'arshpreet' } },
    { extra: { fromAccount: 'ICICI Bank 0993' } }, { extra: { fromAccount: 'Gagan Personal Cash' } },
    { extra: { toAccount: 'Shivam 4807' } }, { extra: { toAccount: 'Arshpreet Cash' } },
    { extra: { toNature: 'PERSONAL' } }, { extra: { fromNature: 'SANKI' } },
    { extra: { fromNature: 'SAMAST', toNature: 'SANKI', classification: 'inter_entity_loan' } },
    { extra: { classification: 'inter_entity_loan' } }
  ];
  for (const scenario of cases) {
    reset();
    await assertRejectedUnchanged('/api/expenses/transfers', manualBody('PERSONAL', 'SAMAST', false, scenario.extra), scenario.actor || admin);
  }
});

test('the new flag cannot promote a claimant to the generic own-account workflow', async () => {
  reset();
  await assertRejectedUnchanged('/api/expenses/transfers', manualBody('PERSONAL', 'SANKI', false, { claimantTransfer: false }), admin);
  reset();
  await assertRejectedUnchanged('/api/expenses/transfers', manualBody('PERSONAL', 'SANKI', false, { claimantTransfer: false, ownAccountTransfer: true }), admin);
});

for (const claimantNature of ['SANKI', 'SAMAST']) test('untagged 7883 debit uses canonical PERSONAL with explicit ' + claimantNature + ' claimant choice and posts once', async () => {
  const draft = fixture();
  const reviewed = await dispatch('/api/expenses/bank-statements/resolve', resolveBody(draft, claimantNature), admin);
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  assert.equal(read().transfers.length, 0, 'review is not a financial posting');
  const decision = read().bankReconciliationDrafts[draft.id].resolutions['bank-0'], selection = decision.claimantTransferSelection;
  assert.equal(selection.version, '7883-arsh-claimant-v1');
  assert.ok(selection.bankSignature);
  const submitted = await dispatch('/api/expenses/bank-statements/finalize', { draftId: draft.id }, admin);
  assert.equal(submitted.status, 202, JSON.stringify(submitted.body));
  assert.equal(read().transfers.length, 0);
  const approved = await dispatch('/api/expenses/bank-statements/finalize', { draftId: draft.id, approveRequestId: submitted.body.approval.id });
  assert.equal(approved.status, 200, JSON.stringify(approved.body));
  const saved = read();
  assert.equal(saved.transfers.length, 1);
  assert.deepEqual([saved.transfers[0].fromNature, saved.transfers[0].toNature, saved.transfers[0].classification], ['PERSONAL', claimantNature, 'owner_contribution']);
  assert.equal(saved.transfers[0].amount, amount);
  assertNoDerivedEntries(saved);
  applyFinalizedInternalTransfers({ ...draft, resolutions: { 'bank-0': decision } }, 'synthetic-owner');
  assert.ok((await dispatch('/api/expenses/bank-statements/finalize', { draftId: draft.id })).status >= 400);
  assert.equal(read().transfers.length, 1);
});

for (const incoming of [false, true]) test('manual SAMAST attribution is retained on claimant ' + (incoming ? 'receipt' : 'debit') + ' and finalization', async () => {
  const draft = fixture({ incoming, manual: true });
  const route = incoming ? '/api/expenses/bank-statements/create-incoming' : '/api/expenses/bank-statements/resolve';
  const payload = incoming ? incomingBody(draft, 'SAMAST') : resolveBody(draft, 'SAMAST');
  const reviewed = await dispatch(route, payload, admin);
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  assert.deepEqual(read().bankReconciliationDrafts[draft.id].transactionOwnership, draft.transactionOwnership);
  const finalized = await dispatch('/api/expenses/bank-statements/finalize', { draftId: draft.id });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const saved = read();
  assert.equal(saved.transfers.length, 1);
  assert.deepEqual([saved.transfers[0].fromNature, saved.transfers[0].toNature, saved.transfers[0].classification], ['SAMAST', 'SAMAST', 'internal_transfer']);
  assert.deepEqual([saved.transfers[0].fromAccount, saved.transfers[0].toAccount], incoming ? [claimant, bank] : [bank, claimant]);
  assertNoDerivedEntries(saved);
});

for (const claimantNature of ['SANKI', 'SAMAST']) test('untagged incoming claimant funds select ' + claimantNature + ' explicitly without reclassifying bank ownership', async () => {
  const draft = fixture({ incoming: true });
  const payload = incomingBody(draft, claimantNature);
  const made = await dispatch('/api/expenses/bank-statements/create-incoming', payload, admin);
  assert.equal(made.status, 200, JSON.stringify(made.body));
  assert.equal(read().transfers.length, 1);
  const finalized = await dispatch('/api/expenses/bank-statements/finalize', { draftId: draft.id });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const saved = read();
  assert.equal(saved.transfers.length, 1);
  assert.deepEqual([saved.transfers[0].fromNature, saved.transfers[0].toNature, saved.transfers[0].classification], [claimantNature, 'PERSONAL', 'owner_withdrawal']);
  assertNoDerivedEntries(saved);
});

test('manual SAMAST cannot be coerced into a different claimant book or canonical PERSONAL inference', async () => {
  for (const incoming of [false, true]) {
    const draft = fixture({ incoming, manual: true });
    await assertRejectedUnchanged(incoming ? '/api/expenses/bank-statements/create-incoming' : '/api/expenses/bank-statements/resolve', incoming ? incomingBody(draft, 'SANKI') : resolveBody(draft, 'SANKI'), admin);
    assert.deepEqual(read().bankReconciliationDrafts[draft.id].transactionOwnership, draft.transactionOwnership);
  }
});

test('changed authoritative manual attribution blocks finalization without partial accounting writes', async () => {
  const draft = fixture({ manual: true });
  assert.equal((await dispatch('/api/expenses/bank-statements/resolve', resolveBody(draft, 'SAMAST'), admin)).status, 200);
  const store = read();
  store.bankReconciliationDrafts[draft.id].transactionOwnership['bank-0'].reason = 'Synthetic changed manual evidence';
  write(store);
  await assertRejectedUnchanged('/api/expenses/bank-statements/finalize', { draftId: draft.id });
});

test('stale or forged source identity and claimant selection fail prefinalization', async () => {
  for (const change of ['reference', 'claimant account', 'claimant entity', 'selection version']) {
    const draft = fixture();
    assert.equal((await dispatch('/api/expenses/bank-statements/resolve', resolveBody(draft, 'SAMAST'), admin)).status, 200);
    const store = read(), saved = store.bankReconciliationDrafts[draft.id], decision = saved.resolutions['bank-0'];
    if (change === 'reference') saved.transactions[0].reference = 'SYNTHETIC-CHANGED-SOURCE';
    if (change === 'claimant account') decision.otherAccount = 'Shivam 4807';
    if (change === 'claimant entity') decision.otherNature = 'SANKI';
    if (change === 'selection version') decision.claimantTransferSelection.version = 'forged-version';
    write(store);
    await assertRejectedUnchanged('/api/expenses/bank-statements/finalize', { draftId: draft.id });
  }
});

test('a new matching expense after review blocks inferred claimant posting', async () => {
  const draft = fixture();
  assert.equal((await dispatch('/api/expenses/bank-statements/resolve', resolveBody(draft, 'SAMAST'), admin)).status, 200);
  const store = read();
  store.expenses['EX-SYNTHETIC-CONFLICT'] = { id: 'EX-SYNTHETIC-CONFLICT', nature: 'PERSONAL', date, amount, status: 'paid', paidAmount: amount, approvedAt: date, vendor: 'Synthetic conflict vendor', particulars: 'Synthetic independent existing bill', ledger: 'Shopping', payments: [{ id: 'PAY-TEST-001', date, amount, account: bank, bankReference: draft.transactions[0].reference }], reimbursementPayments: [] };
  write(store);
  await assertRejectedUnchanged('/api/expenses/bank-statements/finalize', { draftId: draft.id });
  assert.equal(read().transfers.length, 0);
});

test('incoming claimant provenance is bound to the exact created transfer before finalization', async () => {
  const draft = fixture({ incoming: true });
  assert.equal((await dispatch('/api/expenses/bank-statements/create-incoming', incomingBody(draft, 'SAMAST'), admin)).status, 200);
  const store = read();
  store.transfers[0].amount = amount + 1;
  write(store);
  await assertRejectedUnchanged('/api/expenses/bank-statements/finalize', { draftId: draft.id });
});

test('retained staged claimant movements cannot hide changed amounts, dates, routes or duplicate source rows', async () => {
  for (const change of ['amount', 'date', 'account', 'classification', 'reference', 'excluded', 'duplicate']) {
    const draft = fixture();
    assert.equal((await dispatch('/api/expenses/bank-statements/resolve', resolveBody(draft, 'SAMAST'), admin)).status, 200);
    const store = read(), selected = store.bankReconciliationDrafts[draft.id].resolutions['bank-0'].claimantTransferSelection;
    const transfer = {
      id: 'TR-SYNTHETIC-RETAINED', nature: selected.fromNature,
      fromNature: selected.fromNature, fromAccount: selected.fromAccount, toNature: selected.toNature, toAccount: selected.toAccount,
      classification: selected.classification, amount, date, bankReference: draft.transactions[0].reference,
      bankRowId: 'bank-0', reconciliationDraft: draft.id, bankStatementEvidence: true,
      claimantTransferSelection: clone(selected), createdBy: 'synthetic-owner', note: 'Synthetic retained claimant source'
    };
    if (change === 'amount') transfer.amount += 1;
    if (change === 'date') transfer.date = '2098-06-15';
    if (change === 'account') transfer.toAccount = 'Shivam 4807';
    if (change === 'classification') transfer.classification = 'inter_entity_loan';
    if (change === 'reference') transfer.bankReference = 'SYNTHETIC-WRONG-REFERENCE';
    if (change === 'excluded') transfer.accountingExcluded = { reason: 'Synthetic exclusion' };
    store.transfers.push(transfer);
    if (change === 'duplicate') store.transfers.push({ ...clone(transfer), id: 'TR-SYNTHETIC-DUPLICATE' });
    write(store);
    await assertRejectedUnchanged('/api/expenses/bank-statements/finalize', { draftId: draft.id });
  }
});

test('removing incoming resolution metadata cannot bypass the created transfer provenance', async () => {
  const draft = fixture({ incoming: true, manual: true });
  assert.equal((await dispatch('/api/expenses/bank-statements/create-incoming', incomingBody(draft, 'SAMAST'), admin)).status, 200);
  const store = read();
  assert.ok(store.transfers[0].claimantTransferSelection);
  delete store.bankReconciliationDrafts[draft.id].resolutions['bank-0'].claimantTransferSelection;
  write(store);
  await assertRejectedUnchanged('/api/expenses/bank-statements/finalize', { draftId: draft.id });
});

test('pending Owner approval freezes both new claimant transfer creation paths', async () => {
  for (const incoming of [false, true]) {
    const draft = fixture({ incoming, manual: true });
    const store = read();
    store.bankReconciliationApprovals['BRA-SYNTHETIC-PENDING'] = { id: 'BRA-SYNTHETIC-PENDING', draftId: draft.id, status: 'pending', nature: draft.nature, account: draft.account, from: date, to: date };
    write(store);
    await assertRejectedUnchanged(incoming ? '/api/expenses/bank-statements/create-incoming' : '/api/expenses/bank-statements/resolve', incoming ? incomingBody(draft, 'SAMAST') : resolveBody(draft, 'SAMAST'), admin);
  }
});

test('pending Owner approval cannot authorize a changed claimant route', async () => {
  const draft = fixture();
  assert.equal((await dispatch('/api/expenses/bank-statements/resolve', resolveBody(draft, 'SAMAST'), admin)).status, 200);
  const submitted = await dispatch('/api/expenses/bank-statements/finalize', { draftId: draft.id }, admin);
  assert.equal(submitted.status, 202, JSON.stringify(submitted.body));
  const store = read();
  store.bankReconciliationDrafts[draft.id].resolutions['bank-0'].otherNature = 'SANKI';
  write(store);
  await assertRejectedUnchanged('/api/expenses/bank-statements/finalize', { draftId: draft.id, approveRequestId: submitted.body.approval.id });
  assert.equal(read().transfers.length, 0);
});
