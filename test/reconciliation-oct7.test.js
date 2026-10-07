'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-oct7-reconciliation-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
fs.writeFileSync(path.join(tempDir, 'expenses.json'), '{}');
const { router } = require('../modules/expenses');
const { finalizedMovementCoverageIds, pendingCarryForwardCandidates } = require('../modules/reconciliation-opening');
const expenseFile = path.join(tempDir, 'expenses.json');
const account = 'IndusInd Bank 8181', draftId = 'BRD-OCT7-FAILED-AND-RETRY';
const successPaymentId = 'EX-OCT7-SUCCESS/PAY-001';

test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

function request(method, routePath, options = {}) {
  const role = options.role || 'owner';
  return { method, path: routePath, url: routePath, originalUrl: routePath, baseUrl: '',
    body: options.body || {}, params: {}, query: options.query || {},
    headers: { 'user-agent': 'Reconciliation regression test' },
    get(name) { return this.headers[String(name).toLowerCase()] || ''; }, ip: '127.0.0.1',
    user: { username: role === 'admin' ? 'prashant' : 'owner-regression', role, roles: [role] } };
}

function invoke(method, routePath, options = {}) {
  const layer = router.stack.find(item => item.route && item.route.path === routePath && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, routePath);
  let status = 200, body;
  const res = { status(code) { status = code; return this; }, json(value) { body = value; return this; } };
  layer.route.stack[0].handle(request(method, routePath, options), res, () => { throw new Error('Unexpected next()'); });
  return { status, body };
}

function throughRouter(method, routePath, options = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, done = false;
    const headers = {}, finish = value => { if (!done) { done = true; resolve({ status, body: value }); } };
    const res = { statusCode: 200, headersSent: false,
      status(code) { status = code; this.statusCode = code; return this; },
      setHeader(name, value) { headers[String(name).toLowerCase()] = value; },
      getHeader(name) { return headers[String(name).toLowerCase()]; },
      removeHeader(name) { delete headers[String(name).toLowerCase()]; },
      json(value) { this.headersSent = true; finish(value); return this; },
      send(value) { this.headersSent = true; finish(value); return this; },
      end(value) { this.headersSent = true; finish(value); return this; } };
    router.handle(request(method, routePath, options), res, error => { if (error) reject(error); else finish(undefined); });
  });
}

function paidExpense(id, date, amount, reference) {
  return { id, date, nature: 'SANKI', status: 'paid', amount, paidAmount: amount,
    approvedAt: date + 'T08:00:00Z', vendor: 'Oct7 Supplier', particulars: 'Office supplies',
    payments: [{ id: 'PAY-001', date, amount, account, reference, transactionReference: reference, proof: '/test-proof.jpg' }] };
}

function seed(options = {}) {
  invoke('GET', '/api/expenses/config');
  const stored = JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
  const resolutions = {
    'bank-2': { action: 'create_internal_transfer', otherAccount: 'Axis Bank 3448', reason: 'Genuine business funding' }
  };
  if (options.legacy) Object.assign(resolutions, {
    'bank-0': { action: 'link_existing', appId: successPaymentId, reason: 'Mistakenly linked failed attempt' },
    'bank-1': { action: 'create_internal_transfer', otherAccount: 'Axis Bank 3448', reason: 'Mistook reversal for incoming funds' },
    'bank-3': { action: 'link_existing', appId: successPaymentId, reason: 'Correct successful retry' }
  });
  const transactions = [
    { date: '2026-10-07', timestamp: '2026-10-07T10:00:00', reference: '627976594321', description: 'UPI/627976594321/Oct7 Supplier', debit: 665, credit: 0, balance: -567.33 },
    { date: '2026-10-07', timestamp: '2026-10-07T10:01:00', reference: '627976594321', description: 'REVERSED: UPI/627976594321/Oct7 Supplier', reversal: true, debit: 0, credit: 665, balance: 97.67 },
    { date: '2026-10-07', timestamp: '2026-10-07T10:02:00', reference: '627976594320', description: 'IMPS business funding Axis Bank 3448', debit: 0, credit: 2000, balance: 2097.67 },
    { date: '2026-10-07', timestamp: '2026-10-07T10:03:00', reference: '627976594322', description: 'UPI/627976594322/Oct7 Supplier', debit: 665, credit: 0, balance: 1432.67 }
  ];
  Object.assign(stored, {
    expenses: {
      'EX-OCT6-LATE': paidExpense('EX-OCT6-LATE', '2026-10-06', 500, '627876594319'),
      'EX-OCT7-SUCCESS': paidExpense('EX-OCT7-SUCCESS', '2026-10-07', 665, '627976594322')
    }, adjustments: [], transfers: [], receipts: [], vendorAdvances: [], bankTruthMovements: [],
    reconciliationExpenses: [], paytmSettlements: [], vendorOpeningPayables: [], receivables: {},
    bankDateOverrides: {}, bankReconciliationLinks: {}, bankReconciliationApprovals: {},
    openingBalances: { [account]: 0 }, transferSeq: 0, adjSeq: 0,
    bankStatements: { [account]: { reconciledThrough: '2026-10-06', transactions: {},
      imports: [{ id: 'BST-OCT6', draftId: 'BRD-OCT6', reconciliationRows: [] }],
      lastReconciliation: { through: '2026-10-06', closingBalance: 597.67, draftId: 'BRD-OCT6' } } },
    bankReconciliationDrafts: { [draftId]: { id: draftId, account, nature: 'SANKI', transactions,
      summary: { accountLast4: '8181', from: '2026-10-07', to: '2026-10-07', openingBalance: 97.67,
        closingBalance: 1432.67, totalDebits: 1330, totalCredits: 2665, validated: true },
      resolutions, matchingPolicy: 'balanced_date_amount_v5', createdAt: '2026-10-07T11:00:00Z', createdBy: 'prashant' } }
  });
  fs.writeFileSync(expenseFile, JSON.stringify(stored));
  return stored;
}

function read() { return JSON.parse(fs.readFileSync(expenseFile, 'utf8')); }
function preview() {
  const result = invoke('GET', '/api/expenses/bank-statements', { query: { nature: 'SANKI', account, draftId } });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  return result.body.draft;
}
function financialSnapshot(store) {
  const expenses = Object.values(store.expenses).map(expense => ({ id: expense.id, date: expense.date,
    amount: expense.amount, paidAmount: expense.paidAmount, status: expense.status,
    payments: expense.payments.map(payment => ({ id: payment.id, date: payment.date, amount: payment.amount,
      account: payment.account, reference: payment.reference, transactionReference: payment.transactionReference })) }));
  return JSON.stringify({ expenses, transfers: store.transfers, receipts: store.receipts,
    adjustments: store.adjustments, vendorAdvances: store.vendorAdvances, bankTruthMovements: store.bankTruthMovements });
}
function confirmPair() {
  return invoke('POST', '/api/expenses/bank-statements/resolve', { role: 'admin', body: {
    draftId, rowId: 'bank-0', action: 'bank_reversal_pair', bankRowIds: ['bank-0', 'bank-1'],
    reason: 'Official statement confirms failed payment and its reversal; net zero'
  } });
}

test('Oct7 opening carries the existing late Oct6 payment without inventing reconciliation or money', () => {
  seed();
  const before = financialSnapshot(read()), viewed = preview();
  assert.equal(viewed.previousClosing, 597.67);
  assert.equal(viewed.carryForwardNet, -500);
  assert.equal(viewed.currentOpening, 97.67);
  assert.equal(viewed.openingContinuityGap, 0);
  assert.deepEqual(viewed.carryForwardMovements.map(row => row.id), ['EX-OCT6-LATE/PAY-001']);
  assert.equal(viewed.carryForwardMovements[0].reconciliation, undefined);
  assert.equal(read().bankStatements[account].reconciledThrough, '2026-10-06');
  assert.equal(financialSnapshot(read()), before);
});

test('legacy duplicate failed/retry links and a reversal transfer cannot be finalized or hidden by an opening override', async () => {
  seed({ legacy: true });
  const before = financialSnapshot(read()), viewed = preview();
  assert.equal(viewed.currentOpening, 97.67);
  assert.ok(viewed.reconciliationConflicts.some(conflict => conflict.type === 'duplicate_ledger_link'));
  assert.ok(viewed.reconciliationConflicts.some(conflict => conflict.type === 'unreviewed_bank_reversal'));
  assert.equal(viewed.canFinalizeTransactions, false);
  const balanceOverride = invoke('POST', '/api/expenses/bank-statements/resolve-balance', { body: { draftId, amount: -567.33, reason: 'Do not allow a balancing plug to hide duplicate decisions' } });
  assert.equal(balanceOverride.status, 409);
  const finalized = await throughRouter('POST', '/api/expenses/bank-statements/finalize', { body: { draftId, deferClosingBalance: true } });
  assert.equal(finalized.status, 409, JSON.stringify(finalized.body));
  assert.ok(read().bankReconciliationDrafts[draftId]);
  assert.equal(financialSnapshot(read()), before);
});

test('confirming the reversal clears mistaken legacy decisions and retains only the successful payment link', async () => {
  seed({ legacy: true });
  const before = financialSnapshot(read()), paired = confirmPair();
  assert.equal(paired.status, 200, JSON.stringify(paired.body));
  assert.equal(paired.body.reconciliationConflicts.length, 0);
  assert.equal(paired.body.currentOpening, 97.67);
  assert.equal(paired.body.ledgerClosing, 1432.67);
  assert.equal(paired.body.bankClosing, 1432.67);
  assert.equal(paired.body.balanceDifference, 0);
  assert.equal(paired.body.canFinalize, true);
  const draft = read().bankReconciliationDrafts[draftId];
  for (const id of ['bank-0', 'bank-1']) assert.equal(draft.resolutions[id].action, 'bank_reversal_pair');
  assert.equal(draft.resolutions['bank-3'].appId, successPaymentId);
  assert.equal(Object.values(draft.resolutions).filter(resolution => resolution.appId === successPaymentId).length, 1);
  assert.equal(draft.resolutions['bank-2'].action, 'create_internal_transfer');
  assert.equal(draft.decisionAudit[0].previous['bank-1'].action, 'create_internal_transfer');
  assert.equal(financialSnapshot(read()), before, 'reviewing the pair posts nothing');
  const finalized = await throughRouter('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read();
  assert.equal(after.transfers.length, 1);
  assert.equal(after.transfers[0].amount, 2000);
  assert.equal(after.transfers[0].toAccount, account);
  assert.equal(after.transfers[0].fromAccount, 'Axis Bank 3448');
  assert.equal(after.receipts.length, 0);
  assert.equal(after.adjustments.length, 0);
  assert.equal(after.vendorAdvances.length, 0);
  assert.equal(after.expenses['EX-OCT7-SUCCESS'].payments.length, 1);
  assert.equal(after.expenses['EX-OCT7-SUCCESS'].paidAmount, 665);
  assert.equal(after.bankDateOverrides[successPaymentId].bankDate, '2026-10-07');
  assert.equal(after.bankStatements[account].lastReconciliation.closingBalance, 1432.67);
  const finalizedRecord = after.bankStatements[account].imports.find(record => record.draftId === draftId);
  assert.deepEqual(finalizedRecord.carryForwardMovements.map(movement => movement.id), ['EX-OCT6-LATE/PAY-001']);
  assert.equal(finalizedRecord.carryForwardMovements[0].bankEvidencePending, true);
  assert.equal(finalizedMovementCoverageIds(after, after.bankStatements[account]).has('EX-OCT6-LATE/PAY-001'), false);
  assert.equal(Object.values(after.bankStatements[account].transactions).length, 4);
  assert.equal(after.bankReconciliationDrafts[draftId], undefined);
});

test('a clean review pairs failed rows separately and can link the successful retry without duplicate money', () => {
  seed();
  const paired = confirmPair();
  assert.equal(paired.status, 200, JSON.stringify(paired.body));
  const linked = invoke('POST', '/api/expenses/bank-statements/resolve', { role: 'admin', body: {
    draftId, rowId: 'bank-3', action: 'link_existing', appId: successPaymentId, reason: 'Successful retry reference matches payment proof'
  } });
  assert.equal(linked.status, 200, JSON.stringify(linked.body));
  assert.equal(linked.body.ledgerClosing, linked.body.bankClosing);
  assert.equal(linked.body.balanceDifference, 0);
  assert.equal(linked.body.reconciliationConflicts.length, 0);
  assert.equal(linked.body.canFinalize, true);
  assert.equal(read().transfers.length, 0, 'staged incoming money is not posted until finalization');
});

test('future bank evidence can link an old carried payment without moving its ledger date or hiding a genuine gap', async () => {
  const initial = seed({ legacy: true }), carryId = 'EX-OCT6-CARRY/PAY-001', futureDraftId = 'BRD-OCT25-CARRY-EVIDENCE';
  initial.expenses['EX-OCT6-CARRY'] = { ...initial.expenses['EX-OCT6-LATE'], id: 'EX-OCT6-CARRY' };
  delete initial.expenses['EX-OCT6-LATE'];
  fs.writeFileSync(expenseFile, JSON.stringify(initial));
  assert.equal(confirmPair().status, 200);
  const finalized = await throughRouter('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const future = read(), originalRecord = future.bankStatements[account].imports.find(record => record.draftId === draftId);
  assert.equal(originalRecord.carryForwardMovements[0].id, carryId);
  assert.equal(originalRecord.carryForwardMovements[0].date, '2026-10-06');
  assert.equal(originalRecord.carryForwardMovements[0].bankEvidencePending, true);
  assert.equal(finalizedMovementCoverageIds(future, future.bankStatements[account]).has(carryId), false);
  future.bankReconciliationDrafts[futureDraftId] = { id: futureDraftId, account, nature: 'SANKI',
    transactions: [{ date: '2026-10-25', reference: '111122223333', description: 'Later bank evidence of Oct6 supplies', debit: 500, credit: 0, balance: 932.67 }],
    summary: { accountLast4: '8181', from: '2026-10-25', to: '2026-10-25', openingBalance: 1432.67,
      closingBalance: 932.67, totalDebits: 500, totalCredits: 0, validated: true },
    resolutions: {}, matchingPolicy: 'balanced_date_amount_v5', createdAt: '2026-10-25T11:00:00Z', createdBy: 'prashant' };
  fs.writeFileSync(expenseFile, JSON.stringify(future));
  const result = invoke('GET', '/api/expenses/bank-statements', { query: { nature: 'SANKI', account, draftId: futureDraftId } });
  assert.equal(result.status, 200);
  const viewed = result.body.draft, candidate = viewed.linkCandidates.find(movement => movement.id === carryId);
  assert.ok(candidate, 'a pending Oct6 carry is available even beyond the normal +/-7-day date window');
  assert.equal(candidate.priorPeriodCarryForward, true);
  assert.equal(candidate.date, '2026-10-06');
  assert.equal(viewed.currentOpening, 1432.67);
  assert.equal(viewed.carryForwardNet, 0, 'the old carried payment is not deducted from opening again');
  assert.equal(viewed.ledgerClosing, 1432.67);
  assert.equal(viewed.bankClosing, 932.67);
  assert.equal(viewed.balanceDifference, -500);
  const before = financialSnapshot(read());
  const linked = invoke('POST', '/api/expenses/bank-statements/resolve', { role: 'admin', body: {
    draftId: futureDraftId, rowId: 'bank-0', action: 'link_existing', appId: carryId,
    reason: 'Link later bank evidence to already-carried original payment; retain genuine balance gap'
  } });
  assert.equal(linked.status, 200, JSON.stringify(linked.body));
  assert.equal(linked.body.ledgerClosing, 1432.67);
  assert.equal(linked.body.bankClosing, 932.67);
  assert.equal(linked.body.balanceDifference, -500, 'evidence-only linking must not make an artificial zero difference');
  assert.equal(linked.body.balanceResolved, false);
  assert.equal(linked.body.canFinalize, false);
  assert.equal(linked.body.canFinalizeTransactions, true);
  const pending = read(), override = pending.bankDateOverrides[carryId];
  assert.equal(override.bankDate, '2026-10-25');
  assert.equal(override.originalDate, '2026-10-06');
  assert.equal(override.provisional, true);
  assert.equal(pending.expenses['EX-OCT6-CARRY'].payments[0].date, '2026-10-06');
  assert.equal(finalizedMovementCoverageIds(pending, pending.bankStatements[account]).has(carryId), false, 'a provisional evidence link is not finalized coverage');
  assert.equal(financialSnapshot(pending), before);
  const ledger = invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account, from: '2026-10-01', to: '2026-10-25' } });
  assert.equal(ledger.status, 200);
  const carriedEntries = ledger.body.entries.filter(entry => entry.id === carryId);
  assert.equal(carriedEntries.length, 1);
  assert.equal(carriedEntries[0].date, '2026-10-06');
  assert.equal(carriedEntries[0].bankDateOverride.bankDate, '2026-10-25');
  assert.equal(carriedEntries[0].debit, 500);
  const rejected = await throughRouter('POST', '/api/expenses/bank-statements/finalize', { body: { draftId: futureDraftId } });
  assert.equal(rejected.status, 409, 'a genuine closing-balance gap still blocks normal finalization');
  assert.equal(read().bankStatements[account].reconciledThrough, '2026-10-07');
  assert.equal(finalizedMovementCoverageIds(read(), read().bankStatements[account]).has(carryId), false);
  const deferred = await throughRouter('POST', '/api/expenses/bank-statements/finalize', { body: { draftId: futureDraftId, deferClosingBalance: true } });
  assert.equal(deferred.status, 200, JSON.stringify(deferred.body));
  const after = read();
  assert.equal(after.bankStatements[account].lastReconciliation.balanceDifference, -500);
  assert.equal(after.bankStatements[account].lastReconciliation.balanceReconciled, false);
  assert.equal(finalizedMovementCoverageIds(after, after.bankStatements[account]).has(carryId), true, 'only actual finalization supplies finalized bank-evidence coverage');
  assert.equal(pendingCarryForwardCandidates(after, after.bankStatements[account], [carriedEntries[0]]).candidates.length, 0);
  assert.equal(financialSnapshot(after), before, 'finalizing evidence creates no additional money movement');
  const finalizedLedger = invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account, from: '2026-10-01', to: '2026-10-25' } });
  assert.equal(finalizedLedger.body.entries.find(entry => entry.id === carryId).date, '2026-10-06');
});

test('accounts UI compiles and exposes reversal review and genuine carry-forward evidence', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  const scripts = Array.from(html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi));
  let compiled = 0;
  for (const [index, script] of scripts.entries()) {
    if (/\bsrc\s*=|\btype\s*=\s*["'](?:application\/json|importmap)["']/i.test(script[1]) || !script[2].trim()) continue;
    new vm.Script(script[2], { filename: 'expenses-inline-' + index + '.js' });
    compiled++;
  }
  assert.ok(compiled > 0);
  assert.match(html, /Existing ledger carry-forward — bank evidence pending/);
  assert.match(html, /These entries are not marked reconciled/);
  assert.match(html, /No opening adjustment can clear these conflicts/);
  assert.match(html, /action:'bank_reversal_pair'/);
  assert.match(html, /bankReconForm\.bankRowIds/);
});
