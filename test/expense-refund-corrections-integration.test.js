'use strict';
// Invoke the actual registered Express handlers directly: no production data,
// network socket or legacy write-on-read endpoint is needed for these checks.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const tempParent = path.join(__dirname, '..', '.test-tmp'); fs.mkdirSync(tempParent, { recursive: true });
const temp = fs.mkdtempSync(path.join(tempParent, 'refund-correction-policy-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
const RealDate = Date;
global.Date = class extends RealDate { constructor(...args) { super(...(args.length ? args : ['2098-06-30T12:00:00Z'])); } static now() { return RealDate.parse('2098-06-30T12:00:00Z'); } };
const expenses = require('../modules/expenses'), refunds = require('../modules/expense-refunds');
test.after(() => { global.Date = RealDate; fs.rmSync(temp, { recursive: true, force: true }); });
const account = 'IndusInd Bank 8181', file = path.join(temp, 'expenses.json'), cardFile = path.join(temp, 'credit-cards.json');
const user = { username: 'synthetic-admin', role: 'admin', roles: ['admin'] };
function seed() {
  const expense = { id: 'EX-SYNTHETIC', nature: 'SANKI', date: '2098-06-01', vendor: 'Synthetic supplier', amount: 713, paidAmount: 713, status: 'paid', approvedAt: '2098-06-01T00:00:00Z', payments: [{ id: 'PAY-SYNTHETIC', date: '2098-06-02', amount: 713, account }] };
  const store = { expenses: { [expense.id]: expense }, receipts: [], vendorAdvances: [], expenseRefunds: [], auditLog: [], auditSeq: 0, bankStatements: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, bankDateOverrides: {}, cashReconciliations: [], transfers: [], reconciliationExpenses: [] };
  const context = { today: '2098-06-30', username: user.username, expenses: [expense], checkRecord() {}, checkSource() {}, checkAccount: () => account };
  const record = refunds.post(store, refunds.prepare(store, { requestId: 'synthetic-integration-refund', date: '2098-06-10', reason: 'Synthetic return', reasonType: 'return', status: 'received', sources: [{ key: 'expense:' + expense.id, amount: 213 }], components: [{ mode: 'bank', receiver: 'company', account, amount: 213, reference: 'SYNTHETIC-REF' }] }, context), context).refund;
  fs.writeFileSync(file, JSON.stringify(store)); fs.writeFileSync(cardFile, JSON.stringify({ cards: {}, statements: {}, payments: [] }));
  return { store, record };
}
function api(method, suffix, id, body, actor = user) {
  const route = expenses.router.stack.find(layer => layer.route && layer.route.path === '/api/expenses/expense-refunds/:id/' + suffix && layer.route.methods[method]);
  assert.ok(route, method + ' ' + suffix); let result, status = 200;
  route.route.stack[0].handle({ user: actor, params: { id }, body }, { status(code) { status = code; return this; }, json(value) { result = value; } });
  return { status, ...result };
}
test('actual Admin Manage policy previews bytes read-only, commits same refund/receipt IDs and preserves complete audit history', () => {
  const { store, record } = seed(); store.auditLog = Array.from({ length: 10005 }, (_, i) => ({ id: 'SYNTHETIC-OLD-' + i }));
  store.bankDateOverrides['EX-SYNTHETIC/PAY-SYNTHETIC'] = '2098-06-02'; fs.writeFileSync(file, JSON.stringify(store));
  const before = fs.readFileSync(file, 'utf8'), cardBefore = fs.readFileSync(cardFile, 'utf8');
  const opening = api('get', 'correction-preview', record.id); assert.equal(opening.status, 200, opening.error); assert.equal(opening.editable, true, opening.blockedReason);
  const body = { amount: '211.37', reason: 'Verified synthetic source evidence', baseFingerprint: opening.baseFingerprint };
  const preview = api('post', 'correction-preview', record.id, body); assert.equal(preview.success, true, preview.error);
  assert.equal(fs.readFileSync(file, 'utf8'), before); assert.equal(fs.readFileSync(cardFile, 'utf8'), cardBefore);
  const apply = api('patch', 'correction', record.id, { ...body, fingerprint: preview.fingerprint }); assert.equal(apply.success, true, apply.error);
  const after = JSON.parse(fs.readFileSync(file, 'utf8'));
  assert.equal(after.receipts[0].id, store.receipts[0].id); assert.equal(after.receipts[0].amount, 211.37); assert.equal(after.expenseRefunds[0].amount, 211.37);
  assert.deepEqual(after.expenses, store.expenses); assert.deepEqual(after.bankDateOverrides, store.bankDateOverrides);
  assert.deepEqual(after.bankReconciliationDrafts, store.bankReconciliationDrafts); assert.equal(fs.readFileSync(cardFile, 'utf8'), cardBefore);
  assert.equal(after.auditLog.length, 10006); assert.deepEqual(after.auditLog.slice(0, -1), store.auditLog);
  assert.equal(after.auditLog.at(-1).action, 'EXPENSE_REFUND_CORRECTED');
  assert.equal(api('patch', 'correction', record.id, { ...body, fingerprint: preview.fingerprint }).status, 409);
});
test('actual policy enforces Owner approval, denied roles, corrupt external evidence and pending journal without any write', () => {
  const { store, record } = seed();
  const forbidden = api('get', 'correction-preview', record.id, undefined, { username: 'synthetic-accounting', role: 'accounting', roles: ['accounting'] }); assert.equal(forbidden.status, 403);
  store.bankReconciliationDrafts.D = { account, nature: 'SANKI', summary: { from: '2098-06-01', to: '2098-06-30' } };
  store.bankReconciliationApprovals.A = { draftId: 'D', status: 'pending' }; fs.writeFileSync(file, JSON.stringify(store));
  const before = fs.readFileSync(file, 'utf8');
  const locked = api('get', 'correction-preview', record.id, undefined, { username: 'synthetic-owner', role: 'owner', roles: ['owner'] });
  assert.equal(locked.editable, false); assert.match(locked.blockedReason, /Owner approval/); assert.equal(fs.readFileSync(file, 'utf8'), before);
  fs.writeFileSync(cardFile, '{invalid'); const invalid = api('get', 'correction-preview', record.id); assert.equal(invalid.success, false); assert.equal(fs.readFileSync(cardFile, 'utf8'), '{invalid'); assert.equal(fs.readFileSync(file, 'utf8'), before);
  fs.writeFileSync(cardFile, '{}'); const journal = path.join(temp, '.bank-authority-transaction.json'); fs.writeFileSync(journal, '{synthetic incomplete journal');
  const pending = api('get', 'correction-preview', record.id); assert.equal(pending.success, false); assert.match(pending.error, /recovery/); assert.equal(fs.readFileSync(journal, 'utf8'), '{synthetic incomplete journal'); assert.equal(fs.readFileSync(file, 'utf8'), before);
});
