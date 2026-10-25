'use strict';

// Runtime route checks use fictional records only. In particular, deliberately
// unmigrated stores detect indirect writes that a static no-save check misses.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-EVIDENCE-ROUTE', asOf: '2098-01-01', rows: [] } };
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'evidence-route-'));
process.env.DATA_PATH = path.join(directory, 'data.json');
const file = path.join(directory, 'expenses.json');
fs.writeFileSync(file, '{}');
const { router } = require('../modules/expenses');
const route = '/api/expenses/bank-reconciliation-evidence';
const handler = router.stack.find(layer => layer.route && layer.route.path === route && layer.route.methods.get).route.stack[0].handle;
const claimantGate = router.stack.find(layer => !layer.route).handle;
const account = 'ICICI Bank 0993', foreignAccount = 'ICICI Bank 0992';
const movementId = 'EX-SYNTHETIC-EVIDENCE/PAY-ONLY', foreignId = 'EX-SYNTHETIC-FOREIGN/PAY-ONLY';
const draftId = 'BRD-SYNTHETIC-EVIDENCE', finalizedAt = '2098-04-03T12:34:56.000Z';
const owner = { username: 'synthetic-owner', roles: ['owner'] };
const admin = { username: 'synthetic-admin', roles: ['admin'], permissions: { personalAccounting: true } };
function fixture() {
  const bank = { transactionId: 'BTX-SYNTHETIC-EVIDENCE', date: '2098-04-02', debit: 123.45, credit: 0, reference: 'SYNTHETIC-REF', description: 'Fictional selected bank row' };
  const row = { id: 'bank-0', bank, ledger: { id: movementId, date: bank.date, debit: bank.debit, credit: 0, privateMemo: 'SECRET-LEDGER-MEMO' }, linkedRecordIds: [movementId] };
  return {
    expenses: {}, receipts: [], transfers: [], oneTimeMigrations: {},
    bankStatements: {
      ['PERSONAL|' + account]: { transactions: { TX: { ...bank, id: bank.transactionId } }, imports: [{ id: 'BST-SYNTHETIC-EVIDENCE', draftId, to: bank.date, finalizedAt, finalizedBy: owner.username, file: 'SECRET-FILE', reconciliationRows: [row] }] },
      ['PERSONAL|' + foreignAccount]: { imports: [{ id: 'BST-FOREIGN', finalizedAt, reconciliationRows: [{ id: 'bank-0', bank: { ...bank, description: 'SECRET-FOREIGN-BOOK' }, ledger: { id: foreignId }, linkedRecordIds: [foreignId] }] }] }
    },
    bankReconciliationDrafts: {
      [draftId]: { id: draftId, nature: 'PERSONAL', account, transactions: [bank, { ...bank, description: 'SECRET-OTHER-ROW' }], resolutions: { 'bank-0': { action: 'create_internal_transfer', principalAmount: bank.debit, reason: 'Synthetic selected decision' }, 'bank-1': { reason: 'SECRET-OTHER-DECISION' } } },
      'BRD-FOREIGN': { id: 'BRD-FOREIGN', nature: 'PERSONAL', account: foreignAccount, transactions: [bank], resolutions: {} }
    },
    bankDateOverrides: { [movementId]: { bankDate: bank.date, reconciliationDraft: draftId } },
    auditLog: [
      { id: 'AUD-SELECTED', action: 'BANK_RECONCILIATION_FINALIZED', account, nature: 'PERSONAL', ip: 'SECRET-IP', after: { at: finalizedAt, draftId, through: bank.date } },
      { id: 'AUD-UNRELATED', action: 'BANK_RECONCILIATION_FINALIZED', account, nature: 'PERSONAL', after: { at: '2098-04-04T00:00:00.000Z', draftId: 'BRD-UNRELATED', note: 'SECRET-UNRELATED-AUDIT' } }
    ]
  };
}
function reset() {
  for (const name of fs.readdirSync(directory)) if (name !== 'expenses.json' && fs.statSync(path.join(directory, name)).isFile()) fs.unlinkSync(path.join(directory, name));
  fs.writeFileSync(file, JSON.stringify(fixture(), null, 2));
}
function disk() {
  return Object.fromEntries(fs.readdirSync(directory).sort().map(name => [name, fs.statSync(path.join(directory, name)).isFile() ? fs.readFileSync(path.join(directory, name)).toString('base64') : 'DIRECTORY']));
}
function invoke(query = {}, user = owner) {
  let status = 200, body, continued = false;
  const req = { method: 'GET', path: route, url: route, originalUrl: route, query: { nature: 'PERSONAL', account, ...query }, params: {}, body: {}, headers: {}, user, get() { return ''; } };
  const res = { status(code) { status = code; return this; }, json(value) { body = value; return this; } };
  claimantGate(req, res, () => { continued = true; });
  if (continued) handler(req, res);
  return { status, body };
}
function readOnly(query, user, expectedStatus) {
  const before = disk(), result = invoke(query, user);
  assert.equal(result.status, expectedStatus, JSON.stringify(result.body));
  assert.deepEqual(disk(), before, 'GET must not recover, migrate, save or create any accounting file');
  return result.body;
}
test.after(() => fs.rmSync(directory, { recursive: true, force: true }));

test('Owner and explicitly authorized Admin get the same bounded movement evidence without migrations', () => {
  reset();
  const a = readOnly({ movementId }, owner, 200), b = readOnly({ movementId }, admin, 200);
  assert.deepEqual(a, b); assert.equal(a.readOnly, true); assert.equal(a.movement.records.length, 1);
  assert.equal(a.movement.finalizationAudit.length, 1);
  assert.doesNotMatch(JSON.stringify(a), /SECRET-|FOREIGN-BOOK/);
  a.movement.records[0].rows[0].bank.reference = 'response-only';
  assert.equal(readOnly({ movementId }, owner, 200).movement.records[0].rows[0].bank.reference, 'SYNTHETIC-REF');
});

test('exact pending row shows a staged claim but does not invent a posted transfer or expose another row', () => {
  reset();
  const body = readOnly({ draftId, rowId: 'bank-0' }, admin, 200);
  assert.equal(body.pending.decision.action, 'create_internal_transfer');
  assert.deepEqual(body.pending.postedTransfers, []); assert.equal(body.movement, undefined);
  assert.doesNotMatch(JSON.stringify(body), /SECRET-OTHER/);
});

test('unauthorized roles and non-PERSONAL requests are denied before even parsing a damaged store', () => {
  reset(); fs.writeFileSync(file, '{synthetic-invalid-json');
  for (const user of [undefined, { roles: ['admin'] }, { roles: ['accounting'] }, { roles: ['claimant'] }, { roles: ['personal_claimant'] }, { roles: ['admin'], permissions: { personalAccounting: 'true' } }]) {
    const body = readOnly({ movementId }, user === undefined ? null : user, 403);
    assert.equal(body.movement, undefined); assert.equal(body.pending, undefined);
  }
  for (const nature of ['SANKI', 'SAMAST', 'invalid', '']) readOnly({ movementId, nature }, owner, 403);
});

test('movement and saved-row selectors cannot disclose foreign books, mismatched drafts or implicit row zero', () => {
  reset();
  for (const query of [
    { movementId: foreignId }, { movementId, account: foreignAccount },
    { draftId: 'BRD-FOREIGN', rowId: 'bank-0' }, { draftId, rowId: 'bank-999' }
  ]) {
    const body = readOnly(query, owner, 404);
    assert.equal(body.movement, undefined); assert.equal(body.pending, undefined);
    assert.doesNotMatch(JSON.stringify(body), /SECRET-/);
  }
  for (const rowId of ['', '-1', 'bank--1', 'bank-0x', '0']) readOnly({ movementId, draftId, rowId }, owner, 400);
  readOnly({ movementId, account: 'Unknown synthetic bank 9999' }, owner, 403);
  readOnly({ movementId, account: 'IndusInd Bank 7883' }, owner, 403, 'shared bank retains its canonical book restriction');
});

test('a pending bank authority journal fails closed without executing recovery, including for authorized users', () => {
  reset(); const journal = path.join(directory, '.bank-authority-transaction.json');
  fs.writeFileSync(journal, JSON.stringify({ syntheticOnly: true, staged: 'SECRET-JOURNAL' }));
  for (const user of [owner, admin]) {
    const body = readOnly({ movementId }, user, 409);
    assert.match(body.error, /protected recovery workflow/); assert.doesNotMatch(body.error, /SECRET-|syntheticOnly/);
    assert.equal(body.movement, undefined);
  }
});

test('missing, corrupt or unfinished linked stores return a sanitized conflict and never replace files', () => {
  for (const damage of [
    () => fs.unlinkSync(file),
    () => fs.writeFileSync(file, '{synthetic-invalid-json'),
    () => fs.writeFileSync(file, JSON.stringify({ ...fixture(), _bankAuthorityExternal: { secret: 'SECRET-INCOMPLETE' } })),
    () => fs.writeFileSync(path.join(directory, 'credit-cards.json'), '{synthetic-invalid-linked-json'),
    () => fs.writeFileSync(path.join(directory, 'salary.json'), 'null')
  ]) {
    reset(); damage(); const body = readOnly({ movementId }, owner, 409);
    assert.match(body.error, /cannot be read safely/); assert.doesNotMatch(body.error, /SECRET-|SyntaxError|ENOENT|expenses\.json/);
    assert.equal(body.movement, undefined);
  }
});
