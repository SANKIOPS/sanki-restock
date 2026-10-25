'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-OPENING-EVIDENCE', asOf: '2098-01-01', rows: [] } };
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'opening-evidence-route-'));
process.env.DATA_PATH = path.join(directory, 'data.json');
const file = path.join(directory, 'expenses.json');
fs.writeFileSync(file, '{}');
// server.js initializes auth before registering accounting routes. Mirror that
// startup here so the fixture's session-key setup is not mistaken for a GET.
require('../auth');
const { router } = require('../modules/expenses');
const transaction = require('../modules/bank-authority-transaction');
const route = '/api/expenses/bank-opening-anchor-evidence';
const handler = router.stack.find(layer => layer.route && layer.route.path === route && layer.route.methods.get).route.stack[0].handle;
const claimantGate = router.stack.find(layer => !layer.route).handle;
const account = 'Axis Bank 3448', draftId = 'BRD-SYNTHETIC-OPENING';
const owner = { username: 'synthetic-owner', roles: ['owner'] }, admin = { username: 'synthetic-admin', roles: ['admin'] };
function fixture(nature = 'SANKI', selectedAccount = account) {
  const book = { reconciledThrough: '2098-10-18', imports: [
    { id: 'BST-SYNTHETIC-A', draftId: 'D-A', to: '2098-10-18', statementSummary: { to: '2098-10-18', closingBalance: 248209.82, validated: true },
      balanceReconciled: true, reconciliationRows: [{ bank: { transactionId: 'BTX-A', description: 'SECRET-PRIVATE-DESCRIPTION' } }] },
    { id: 'BST-SYNTHETIC-B', draftId: 'D-B', to: '2098-10-18', statementSummary: { to: '2098-10-18', closingBalance: 165673.07, validated: true },
      balanceReconciled: true, reconciliationBankCoverageComplete: false, file: '/SECRET-PATH.pdf', hash: 'b'.repeat(64),
      reconciliationRows: [{ bank: { transactionId: 'BTX-B' } }] }
  ], lastReconciliation: { draftId: 'D-A', through: '2098-10-18', closingBalance: 248209.82, balanceReconciled: true } };
  return { expenses: {}, receipts: [], transfers: [], oneTimeMigrations: {},
    bankStatements: { [nature === 'PERSONAL' ? 'PERSONAL|' + selectedAccount : selectedAccount]: book,
      'PERSONAL|SECRET-FOREIGN-ACCOUNT': { imports: [{ id: 'SECRET-FOREIGN-IMPORT' }] } },
    bankReconciliationDrafts: { [draftId]: { id: draftId, nature, account: selectedAccount,
      summary: { from: '2098-10-19', to: '2098-10-24', openingBalance: 165673.07, totalDebits: 19507.7,
        totalCredits: 103372.83, closingBalance: 249538.2, validated: true }, transactions: [],
      fileHash: 'b'.repeat(64), temporaryFile: '/SECRET-DRAFT.pdf', resolutions: { 'bank-0': { reason: 'SECRET-REVIEW' } } } } };
}
function reset(store = fixture()) {
  for (const name of fs.readdirSync(directory)) if (!['expenses.json', 'session_secret.key'].includes(name) && fs.statSync(path.join(directory, name)).isFile()) fs.unlinkSync(path.join(directory, name));
  fs.writeFileSync(file, JSON.stringify(store, null, 2));
}
function disk() {
  return Object.fromEntries(fs.readdirSync(directory).sort().map(name => [name, fs.statSync(path.join(directory, name)).isFile() ? fs.readFileSync(path.join(directory, name)).toString('base64') : 'DIRECTORY']));
}
function invoke(query = {}, user = owner) {
  let status = 200, body, continued = false;
  const req = { method: 'GET', path: route, url: route, originalUrl: route, query: { nature: 'SANKI', account, draftId, ...query }, params: {}, body: {}, headers: {}, user, get() { return ''; } };
  const res = { status(code) { status = code; return this; }, json(value) { body = value; return this; } };
  claimantGate(req, res, () => { continued = true; });
  if (continued) handler(req, res);
  return { status, body };
}
function readOnly(query, user, expectedStatus) {
  const before = disk(), recover = transaction.recover;
  let recoveryCalls = 0, result;
  transaction.recover = () => { recoveryCalls++; throw new Error('SECRET-RECOVERY-WAS-CALLED'); };
  try { result = invoke(query, user); } finally { transaction.recover = recover; }
  assert.equal(recoveryCalls, 0, 'permission helpers must use the card snapshot instead of invoking recovery');
  assert.equal(result.status, expectedStatus, JSON.stringify(result.body));
  assert.deepEqual(disk(), before, 'GET must not repair, recover, migrate, save, or create files');
  return result.body;
}
test.after(() => fs.rmSync(directory, { recursive: true, force: true }));

test('Owner and Admin read identical SANKI evidence from an unmigrated store without writes or recovery', () => {
  reset();
  const a = readOnly({}, owner, 200), b = readOnly({}, admin, 200);
  assert.deepEqual(a, b); assert.equal(a.readOnly, true); assert.equal(a.candidateCount, 2);
  assert.equal(a.anchor.previousClosing, null); assert.equal(a.anchor.anchorConflict.type, 'ambiguous_opening_anchor');
  assert.doesNotMatch(JSON.stringify(a), /SECRET-/);
  a.candidates[0].coverage.physicalBankTransactionIds.push('response-only');
  assert.equal(readOnly({}, owner, 200).candidates[0].coverage.physicalBankTransactionIds.length, 1);
});

test('unauthorized roles are denied before parsing a damaged store', () => {
  reset(); fs.writeFileSync(file, '{SECRET-DAMAGED-STORE');
  for (const user of [null, { roles: [] }, { roles: ['accounting'] }, { roles: ['samast_accounting'] }, { roles: ['claimant'] }, { roles: ['personal_claimant'] }]) {
    const body = readOnly({}, user, 403); assert.equal(body.candidates, undefined);
  }
});

test('required selectors do not normalize invalid entities, arrays, whitespace, or missing exact draft identities', () => {
  reset();
  for (const query of [{ nature: '' }, { nature: 'invalid' }, { nature: 'sanki' }, { nature: ['SANKI'] },
    { account: '' }, { account: ['Axis Bank 3448'] }, { account: ' Axis Bank 3448' }, { account: 'x'.repeat(257) },
    { draftId: '' }, { draftId: [] }, { draftId: ' ' }, { draftId: 'x'.repeat(257) }]) readOnly(query, owner, 400);
  for (const query of [{ draftId: 'BRD-MISSING' }, { draftId: '__proto__' }, { account: 'IndusInd Bank 8181' }]) readOnly(query, owner, 404);
  readOnly({ account: 'Unknown Synthetic Bank' }, owner, 403);
  const store = fixture(); store.bankReconciliationDrafts[draftId].nature = 'SAMAST'; reset(store);
  readOnly({}, owner, 404);
});

test('private books and accounts retain existing permission boundaries', () => {
  const privateAccount = 'ICICI Bank 0992'; reset(fixture('PERSONAL', privateAccount));
  const query = { nature: 'PERSONAL', account: privateAccount };
  readOnly(query, admin, 403);
  readOnly(query, { ...admin, permissions: { personalAccounting: 'true' } }, 403);
  for (const user of [owner, { ...admin, permissions: { personalAccounting: true } }]) assert.equal(readOnly(query, user, 200).nature, 'PERSONAL');
  reset(fixture('SANKI', privateAccount)); readOnly({ account: privateAccount }, admin, 403);
});

test('restricted cards use snapshot permissions and shared-bank canonical-book restrictions remain intact', () => {
  const cardAccount = 'Synthetic Card 9099'; reset(fixture('SANKI', cardAccount));
  fs.writeFileSync(path.join(directory, 'credit-cards.json'), JSON.stringify({ cards: { CARD: { id: 'CARD', name: 'Synthetic Card', last4: '9099', active: true, ownerOnly: true } } }));
  readOnly({ account: cardAccount }, admin, 403);
  readOnly({ account: cardAccount }, owner, 200);
  reset(fixture('SAMAST', 'IndusInd Bank 7883'));
  readOnly({ account: 'IndusInd Bank 7883', nature: 'SAMAST' }, admin, 403);
  readOnly({ account: 'IndusInd Bank 7883', nature: 'PERSONAL' }, owner, 403);
  readOnly({ account: 'IndusInd Bank 7883', nature: 'SAMAST' }, owner, 200);
  const conflicted = fixture('SAMAST', 'IndusInd Bank 7883'); conflicted.sharedBankReconciliationConflicts = { 'IndusInd Bank 7883': { secret: 'SECRET-CONFLICT' } }; reset(conflicted);
  readOnly({ account: 'IndusInd Bank 7883', nature: 'SAMAST' }, owner, 403);
});

test('pending journals and damaged stores fail closed with sanitized errors and no accounting recovery', () => {
  for (const damage of [
    () => fs.writeFileSync(path.join(directory, '.bank-authority-transaction.json'), '{"secret":"SECRET-JOURNAL"}'),
    () => fs.unlinkSync(file),
    () => fs.writeFileSync(file, '{SECRET-DAMAGED-STORE'),
    () => fs.writeFileSync(file, JSON.stringify({ ...fixture(), _bankAuthorityExternal: { secret: 'SECRET-PENDING' } })),
    () => fs.writeFileSync(path.join(directory, 'credit-cards.json'), '{SECRET-DAMAGED-CARDS'),
    () => fs.writeFileSync(path.join(directory, 'salary.json'), 'null')
  ]) {
    reset(); damage(); const body = readOnly({}, owner, 409);
    assert.match(body.error, /cannot be read safely/); assert.doesNotMatch(body.error, /SECRET-|ENOENT|SyntaxError/);
    assert.equal(body.candidates, undefined);
  }
});

test('malformed stored evidence is rejected without changing the current draft, opening, or review', () => {
  const store = fixture(); store.bankStatements[account].imports[0].reconciliationBankTransactionIds = { secret: 'SECRET-BAD-COVERAGE' }; reset(store);
  const body = readOnly({}, owner, 409);
  assert.match(body.error, /cannot be interpreted safely/); assert.doesNotMatch(body.error, /SECRET-/);
});

test('route is outside repairing bank-statements middleware and uses only snapshot access', () => {
  const source = fs.readFileSync(require.resolve('../modules/expenses'), 'utf8');
  const start = source.indexOf("router.get('/api/expenses/bank-opening-anchor-evidence'"), end = source.indexOf("router.get('/api/expenses/bank-reconciliation-evidence'", start);
  const selected = source.slice(start, end);
  assert.ok(start > 0 && end > start); assert.match(selected, /readLedgerEditSnapshot\(\)/);
  assert.match(selected, /canAccessBankReconciliation\(req,s,nature,account,cards\)/);
  assert.doesNotMatch(selected, /loadStore\(|saveStore\(|draftReconciliation\(|recoverStored|loadCreditCards\(/);
});
