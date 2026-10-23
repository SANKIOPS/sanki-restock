'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const refunds = require('../modules/expense-refunds');
const corrections = require('../modules/expense-refund-corrections');
const routes = require('../modules/expense-refunds-routes');
const clone = value => JSON.parse(JSON.stringify(value));
const account = 'Synthetic company bank', originalAccount = 'Synthetic paying bank';
const req = { user: { username: 'synthetic-admin', role: 'admin' } };
function context(request, store) {
  return { today: '2098-06-30', username: request.user.username, expenses: Object.values(store.expenses), checkRecord() {}, checkSource() {}, checkAccount(sources, component) { return component.account; }, checkExistingMovement() {} };
}
function fixture(mode = 'bank', status = 'received') {
  const expense = { id: 'EX-SYNTHETIC', date: '2098-06-01', nature: 'SANKI', vendor: 'Synthetic supplier', amount: 713, paidAmount: mode === 'credit_note' ? 0 : 713,
    status: mode === 'credit_note' ? 'approved' : 'paid', approvedAt: '2098-06-01T00:00:00Z', payments: mode === 'credit_note' ? [] : [{ id: 'PAY-SYNTHETIC', amount: 713, date: '2098-06-02', account: originalAccount }] };
  const store = { expenses: { [expense.id]: expense }, receipts: [], vendorAdvances: [], expenseRefunds: [], auditLog: [], bankStatements: {}, bankDateOverrides: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, cashReconciliations: [] };
  const creation = { requestId: 'synthetic-refund-request', date: '2098-06-10', reason: 'Synthetic returned items', reasonType: 'return', status,
    sources: [{ key: 'expense:' + expense.id, amount: 213 }], components: [{ mode, amount: 213, receiver: 'company', account, reference: 'SYNTHETIC-REF', issuer: 'Synthetic supplier', expiryDate: refunds.CREDIT_MODES.has(mode) ? '2098-12-31' : '' }], proofs: ['/api/expenses/photo/synthetic-proof.png'] };
  const record = refunds.post(store, refunds.prepare(store, creation, context(req, store)), { username: req.user.username, now: '2098-06-10T00:00:00Z' }).refund;
  return { store, record, creation };
}
function snapshot(store, external = {}) { return { store, raw: JSON.stringify(store), external, externalRaw: { synthetic: JSON.stringify(external) }, cards: {}, cardRaw: '{}' }; }
function dependencies(overrides = {}) {
  return { context, today: () => '2098-06-30', policy: { canEdit: () => true, isOwner: () => false, canPersonal: () => true, natures: () => ['SANKI'], accountVisible: () => true,
    bookKey: (nature, bank) => nature + '|' + bank, ...overrides } };
}
function proposed(snap, id, deps = dependencies(), patch = {}) {
  const inspected = corrections.inspect(snap, req, id, deps);
  assert.equal(inspected.editable, true, inspected.blockedReason);
  return { amount: '211.37', date: '2098-06-11', reference: 'CORRECTED-SYNTHETIC', description: 'Corrected synthetic return details', reason: 'Verified synthetic original refund evidence', baseFingerprint: inspected.baseFingerprint, ...patch };
}
test('bank refund correction preserves original expense/payment/proof/IDs and previews without mutation', () => {
  const { store, record } = fixture(), snap = snapshot(store), before = clone(store), deps = dependencies();
  const payload = proposed(snap, record.id, deps), plan = corrections.prepare(snap, req, record.id, payload, deps);
  assert.deepEqual(store, before);
  assert.deepEqual(plan.store.expenses, before.expenses);
  assert.deepEqual(plan.store.bankReconciliationDrafts, before.bankReconciliationDrafts);
  assert.equal(plan.after.refund.id, record.id); assert.equal(plan.after.receipts[0].id, record.components[0].receiptId);
  assert.equal(plan.after.refund.amount, 211.37); assert.equal(plan.after.refund.sources[0].amount, 211.37);
  assert.equal(plan.after.refund.sources[0].billCreditAmount, 211.37); assert.equal(plan.after.refund.sources[0].paymentAllocations[0].amount, 211.37);
  assert.equal(plan.after.receipts[0].amount, 211.37); assert.equal(plan.after.receipts[0].bankReference, payload.reference);
  assert.deepEqual(plan.after.refund.proofs, before.expenseRefunds[0].proofs);
  assert.deepEqual(plan.after.receipts[0].proofs, before.receipts[0].proofs);
  assert.equal(plan.after.refund.inputSignature, record.inputSignature); assert.notEqual(plan.after.refund.signature, record.signature);
  const applied = corrections.prepare(snap, req, record.id, { ...payload, fingerprint: plan.fingerprint }, deps, true);
  assert.deepEqual(applied.after, plan.after);
});
test('fingerprint binds exact proposal, actor, all source bytes and external evidence', () => {
  const { store, record } = fixture(), snap = snapshot(store), deps = dependencies(), payload = proposed(snap, record.id, deps);
  const plan = corrections.prepare(snap, req, record.id, payload, deps), accepted = { ...payload, fingerprint: plan.fingerprint };
  for (const tamper of [{ amount: '210' }, { date: '2098-06-12' }, { reference: 'different' }, { description: 'different' }, { reason: 'different' }, { fingerprint: '' }]) assert.throws(() => corrections.prepare(snap, req, record.id, { ...accepted, ...tamper }, deps, true), /changed after preview/);
  assert.throws(() => corrections.prepare(snap, { user: { username: 'other-admin' } }, record.id, accepted, deps, true), /changed after preview/);
  assert.throws(() => corrections.prepare(snap, { user: { ...req.user, permissions: { personalAccounting: true } } }, record.id, accepted, deps, true), /changed after preview/);
  assert.throws(() => corrections.prepare({ ...snap, externalRaw: { synthetic: 'new evidence' } }, req, record.id, accepted, deps, true), /changed after preview/);
  assert.throws(() => corrections.prepare({ ...snap, raw: snap.raw + ' ' }, req, record.id, payload, deps), /form opened/);
});
test('strict payload rejects forbidden identity changes, invalid money/date and empty reasons without writes', () => {
  const { store, record } = fixture(), snap = snapshot(store), deps = dependencies(), base = proposed(snap, record.id, deps), before = clone(store);
  for (const patch of [{ account: 'Other' }, { nature: 'SAMAST' }, { components: [] }, { proofs: [] }, { amount: true }, { amount: '2e2' }, { amount: '211.371' }, { amount: '-1' }, { amount: 0 }, { amount: Number.MAX_SAFE_INTEGER }, { amount: [] }, { date: '2098-02-30' }, { date: '2098-07-01' }, { description: '' }, { reason: '' }, { reason: [] }]) assert.throws(() => corrections.prepare(snap, req, record.id, { ...base, ...patch }, deps));
  assert.deepEqual(store, before);
});
test('capacity is recalculated excluding only current refund and existing originals stay untouched', () => {
  const { store, record } = fixture(), deps = dependencies();
  const other = clone(record); other.id = 'RF-OTHER'; other.components[0].receiptId = 'RF-OTHER/PART-001'; other.requestId = 'other-refund-request'; other.sources[0].amount = 413; other.sources[0].billCreditAmount = 413; other.sources[0].paymentAllocations[0].amount = 413; other.amount = 413;
  store.expenseRefunds.push(other);
  const snap = snapshot(store), payload = proposed(snap, record.id, deps, { amount: '300.01' });
  assert.throws(() => corrections.prepare(snap, req, record.id, payload, deps), /remaining refundable/);
  assert.equal(corrections.prepare(snap, req, record.id, { ...payload, amount: '300' }, deps).after.refund.amount, 300);
});
test('reconciled original expense payment is allowed but any owned receipt bank match is locked', () => {
  const { store, record } = fixture(), deps = dependencies();
  store.bankDateOverrides[originalAccount] = { 'EX-SYNTHETIC/PAY-SYNTHETIC': '2098-06-02' };
  let snap = snapshot(store), payload = proposed(snap, record.id, deps);
  const plan = corrections.prepare(snap, req, record.id, payload, deps);
  assert.deepEqual(plan.store.bankDateOverrides, store.bankDateOverrides);
  store.bankDateOverrides[account] = { [record.components[0].receiptId]: '2098-06-10' };
  snap = snapshot(store); assert.equal(corrections.inspect(snap, req, record.id, deps).editable, false);
  assert.throws(() => corrections.prepare(snap, req, record.id, payload, deps), /reconciled|active bank review/);
});
test('Owner cannot bypass finalized dates, pending approval, cash locks or external source links', () => {
  const mutations = [
    store => { store.bankStatements[account] = { reconciledThrough: '2098-06-10' }; },
    store => { store.bankReconciliationDrafts.D = { account, summary: { from: '2098-06-01', to: '2098-06-20' } }; store.bankReconciliationApprovals.A = { status: 'pending', draftId: 'D' }; },
    store => { store.cashReconciliations.push({ status: 'pending', account, verifiedThrough: '2098-06-10' }); },
    store => { store.receipts[0].creditCardStatementId = 'CC-SYNTHETIC'; },
    store => { store.reconciliationExpenses = [{ expenseRefundReceiptId: store.receipts[0].id, creditCardStatementId: 'CC-SYNTHETIC' }]; }
  ];
  for (const mutate of mutations) { const { store, record } = fixture(); mutate(store); assert.equal(corrections.inspect(snapshot(store), req, record.id, dependencies({ isOwner: () => true })).editable, false); }
  const { store, record } = fixture(); assert.equal(corrections.inspect(snapshot(store, { cards: { link: record.components[0].receiptId } }), req, record.id, dependencies()).editable, false);
});
test('dependencies and ambiguous split identities fail closed', () => {
  const mutations = [
    (s, r) => { r.parentId = 'RF-PARENT'; },
    (s, r) => { s.expenseRefunds.push({ ...clone(r), id: 'RF-CHILD', parentId: r.id }); },
    (s, r) => { r.sources.push(clone(r.sources[0])); },
    (s, r) => { r.components.push(clone(r.components[0])); },
    (s, r) => { r.components[0].externalMovementId = 'EXTERNAL'; },
    (s, r) => { r.components[0].receiver = 'payer'; },
    (s, r) => { s.receipts.push({ id: 'RECOVERY', refundRecoveryExpenseId: r.sources[0].expenseId }); },
    (s, r) => { s.expenseRefunds.push(clone(r)); },
    s => { s.receipts.push({ ...clone(s.receipts[0]), expenseRefundId: 'OTHER' }); },
    s => { s.expenses['EX-SYNTHETIC'].payments.push(clone(s.expenses['EX-SYNTHETIC'].payments[0])); },
    (s, r) => { r.amount = 213.001; },
    (s, r) => { r.sources[0].billCreditAmount = 210; },
    (s, r) => { r.sources[0].personalReturned = 1; },
    (s, r) => { r.sources[0].paymentAllocations[0].amount = 210; },
    s => { s.receipts[0].amount = 213.001; },
    s => { s.receipts[0].bankReference = 'CHANGED'; }
  ];
  for (const mutate of mutations) { const { store, record } = fixture(); mutate(store, record); const before = clone(store); assert.equal(corrections.inspect(snapshot(store), req, record.id, dependencies()).editable, false, mutate.toString()); assert.deepEqual(store, before); }
});
test('permissions and privacy fail before exposing correction details', () => {
  for (const policy of [{ canEdit: () => false }, { natures: () => [] }, { accountVisible: () => false }]) {
    const { store, record } = fixture(); assert.throws(() => corrections.inspect(snapshot(store), req, record.id, dependencies(policy)), error => error.status === 403);
  }
  const { store, record } = fixture(); store.expenses['EX-SYNTHETIC'].ownerOnly = true;
  assert.throws(() => corrections.inspect(snapshot(store), req, record.id, dependencies()), error => error.status === 403);
  delete store.expenses['EX-SYNTHETIC']; assert.throws(() => corrections.inspect(snapshot(store), req, record.id, dependencies()), error => error.status === 403);
});
test('malformed, missing, stale and exhausted audit identities block refund correction without altering history', () => {
  const malformed = [
    { auditLog: { retained: 'must not disappear' }, auditSeq: 0 },
    { auditLog: [{ id: 'AUD-000004' }] },
    { auditLog: [{ id: 'AUD-000004' }], auditSeq: 3 },
    { auditLog: [{ id: 'AUD-000004' }, { id: 'AUD-000004' }], auditSeq: 4 },
    { auditLog: ['retained malformed entry'], auditSeq: 4 },
    { auditLog: [], auditSeq: '4' },
    { auditLog: [], auditSeq: Number.MAX_SAFE_INTEGER }
  ];
  for (const invalid of malformed) {
    const { store, record } = fixture(), deps = dependencies(), valid = snapshot(store), payload = proposed(valid, record.id, deps);
    Object.assign(store, invalid); const before = clone(store), snap = snapshot(store), result = corrections.inspect(snap, req, record.id, deps);
    assert.equal(result.editable, false); assert.match(result.blockedReason, /audit/i);
    assert.throws(() => corrections.prepare(snap, req, record.id, payload, deps), /audit/i); assert.deepEqual(store, before);
  }
  const { store, record } = fixture(); store.auditLog = [{ id: 'AUD-000004' }]; store.auditSeq = 4;
  assert.equal(corrections.inspect(snapshot(store), req, record.id, dependencies()).editable, true);
});
test('unused noncash credit correction updates the same wallet amount and remaining credit, never original payments', () => {
  for (const mode of ['voucher', 'store_credit', 'vendor_credit']) {
    const { store, record } = fixture(mode), deps = dependencies(), snap = snapshot(store), payload = proposed(snap, record.id, deps);
    const plan = corrections.prepare(snap, req, record.id, payload, deps);
    assert.equal(plan.after.credits.length, 1); assert.equal(plan.after.receipts.length, 0);
    assert.equal(plan.after.credits[0].id, store.vendorAdvances[0].id); assert.equal(plan.after.credits[0].amount, 211.37); assert.equal(plan.after.credits[0].remainingAmount, 211.37);
    assert.deepEqual(plan.store.expenses, store.expenses); assert.equal(plan.store.receipts.length, 0);
    store.vendorAdvances[0].applications.push({ id: 'USE-SYNTHETIC', amount: 1 }); store.vendorAdvances[0].remainingAmount--;
    assert.equal(corrections.inspect(snapshot(store), req, record.id, deps).editable, false);
  }
});
test('safe pending return and unpaid credit note can be corrected without creating any movement', () => {
  for (const [mode, status] of [['bank', 'pending'], ['credit_note', 'received']]) {
    const { store, record } = fixture(mode, status), deps = dependencies(), snap = snapshot(store), payload = proposed(snap, record.id, deps, { reference: '' });
    const plan = corrections.prepare(snap, req, record.id, payload, deps);
    assert.equal(plan.after.refund.amount, 211.37); assert.equal(plan.after.receipts.length, 0); assert.equal(plan.after.credits.length, 0);
    assert.deepEqual(plan.store.expenses, store.expenses); assert.deepEqual(plan.after.refund.sources[0].paymentAllocations, []);
    store.bankReconciliationDrafts.D = { nature: 'SANKI', account, summary: { from: '2098-06-01', to: '2098-06-30' } };
    store.bankReconciliationApprovals.A = { nature: 'SANKI', status: 'pending', draftId: 'D' };
    assert.equal(corrections.inspect(snapshot(store), req, record.id, deps).editable, false);
  }
});
test('credit expiry, source date and original allocation identity cannot be bypassed', () => {
  let { store, record } = fixture('voucher'), deps = dependencies(), snap = snapshot(store), payload = proposed(snap, record.id, deps);
  assert.throws(() => corrections.prepare(snap, req, record.id, { ...payload, date: '2098-06-01' }, deps), /payment date/);
  record.components[0].expiryDate = '2098-06-10'; store.vendorAdvances[0].expiryDate = '2098-06-10'; snap = snapshot(store); payload = proposed(snap, record.id, deps);
  assert.throws(() => corrections.prepare(snap, req, record.id, payload, deps), /expiry/);
  ({ store, record } = fixture()); store.expenses['EX-SYNTHETIC'].amount = 1113; store.expenses['EX-SYNTHETIC'].paidAmount = 1113;
  store.expenses['EX-SYNTHETIC'].payments.push({ id: 'PAY-SECOND', amount: 400, date: '2098-06-03', account: originalAccount }); snap = snapshot(store); payload = proposed(snap, record.id, deps, { amount: '714' });
  assert.throws(() => corrections.prepare(snap, req, record.id, payload, deps), /original payment allocation/);
});
test('corrected canonical signature prevents duplicate creation and original request safely replays', () => {
  const { store, record, creation } = fixture(), deps = dependencies(), snap = snapshot(store), payload = proposed(snap, record.id, deps);
  const plan = corrections.prepare(snap, req, record.id, payload, deps);
  const duplicate = { ...creation, requestId: 'second-refund-request', date: payload.date, reason: payload.description, sources: [{ key: creation.sources[0].key, amount: Number(payload.amount) }], components: [{ ...creation.components[0], amount: Number(payload.amount), reference: payload.reference }] };
  assert.throws(() => refunds.prepare(clone(plan.store), duplicate, context(req, plan.store)), /already recorded/);
  const retry = refunds.prepare(clone(plan.store), creation, context(req, plan.store)); assert.equal(retry.already.id, record.id); assert.equal(retry.already.amount, 211.37);
});

test('registered API previews are read-only and apply updates only target links with complete audit and stale replay rejection', async () => {
  const registered = {}, router = Object.fromEntries(['get', 'post', 'patch'].map(method => [method, (url, handler) => { registered[method + ' ' + url] = handler; }]));
  const { store: initial, record } = fixture(); let current = clone(initial), commits = 0;
  const policy = { ...dependencies().policy, read: () => snapshot(clone(current)), commit: (snap, next) => { assert.equal(snap.raw, JSON.stringify(current)); current = clone(next); commits++; },
    audit: (store, request, action, type, id, detail) => { store.auditLog.push({ username: request.user.username, action, type, id, ...clone(detail) }); } };
  routes.register(router, { correctionPolicy: policy, isAdmin: () => true, isOwner: () => false, natures: () => ['SANKI'], expenses: store => Object.values(store.expenses),
    nature: value => value, canView: () => true, accountVisible: () => { throw new Error('Legacy account lookup must not run'); }, today: () => '2098-06-30', companyAccount: (store, nature, name) => name,
    card: () => { throw new Error('Legacy card loader must not run'); }, closedThrough: () => '', receiptUsed: () => false });
  function call(method, path, body) { let result, status = 200; registered[method + ' /api/expenses/expense-refunds/:id/' + path]({ ...req, params: { id: record.id }, body }, { status(value) { status = value; return this; }, json(value) { result = value; } }); return { status, ...result }; }
  const get = call('get', 'correction-preview'); assert.equal(get.success, true); assert.equal(commits, 0);
  const payload = { amount: '211.37', reason: 'Verified synthetic receipt', baseFingerprint: get.baseFingerprint };
  const preview = call('post', 'correction-preview', payload); assert.equal(preview.success, true, preview.error); assert.equal(commits, 0); assert.deepEqual(current, initial);
  const applied = call('patch', 'correction', { ...payload, fingerprint: preview.fingerprint }); assert.equal(applied.success, true, applied.error); assert.equal(commits, 1);
  assert.deepEqual(current.expenses, initial.expenses); assert.deepEqual(current.bankStatements, initial.bankStatements); assert.deepEqual(current.bankReconciliationDrafts, initial.bankReconciliationDrafts);
  assert.equal(current.auditLog[0].action, 'EXPENSE_REFUND_CORRECTED'); assert.equal(current.auditLog[0].before.receipts[0].amount, 213); assert.equal(current.auditLog[0].after.receipts[0].amount, 211.37);
  assert.equal(current.auditLog[0].note, payload.reason); assert.equal(current.expenseRefunds[0].correctionRevision, 1);
  assert.equal(call('patch', 'correction', { ...payload, fingerprint: preview.fingerprint }).status, 409); assert.equal(commits, 1);
});
