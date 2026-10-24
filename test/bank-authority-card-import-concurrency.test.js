'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-card-import-concurrency-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const cardFile = path.join(tempDir, 'credit-cards.json'), expenseFile = path.join(tempDir, 'expenses.json');
const transaction = require('../modules/bank-authority-transaction');
let resolveParse, signalParse;
const expensesPath = require.resolve('../modules/expenses');
// The only substituted dependency is asynchronous PDF parsing: route logic,
// real private files, atomic correction commit and statement saving are real.
require.cache[expensesPath] = { id: expensesPath, filename: expensesPath, loaded: true, exports: { parseBankStatementUpload: () => { signalParse(); return new Promise(resolve => { resolveParse = resolve; }); } } };
const { router } = require('../modules/credit-cards');
const importHandler = router.stack.find(layer => layer.route && layer.route.path === '/api/expenses/credit-cards/statements/import').route.stack.at(-1).handle;
const clone = value => JSON.parse(JSON.stringify(value));
const readCards = () => JSON.parse(fs.readFileSync(cardFile, 'utf8'));
function seed() {
  const cards = { cards: { 'CC-1': { id: 'CC-1', name: 'Test Card', last4: '7376', active: true, ownerOnly: false } }, payments: [{ id: 'CCP-1', cardId: 'CC-1', amount: 500, date: '2098-06-01', active: true, account: 'IndusInd Bank 8181' }], statements: {}, merchantRules: {}, audit: [], auditSeq: 0, statementSeq: 0 };
  const expenses = { expenses: {}, transfers: [{ id: 'TR-1', amount: 500, date: '2098-06-01' }], receipts: [], expenseRefunds: [], reconciliationExpenses: [] };
  fs.writeFileSync(cardFile, JSON.stringify(cards)); fs.writeFileSync(expenseFile, JSON.stringify(expenses));
  const uploaded = path.join(tempDir, 'upload-' + crypto.randomBytes(4).toString('hex') + '.pdf');
  fs.writeFileSync(uploaded, 'isolated local fixture');
  return { cards, expenses, uploaded };
}
function beginImport(f, role = 'owner') {
  const entered = new Promise(resolve => { signalParse = resolve; });
  const done = new Promise((resolve, reject) => {
    let status = 200;
    const req = { body: { cardId: 'CC-1', kind: 'billed' }, file: { path: f.uploaded, originalname: 'fixture.pdf', mimetype: 'application/pdf' }, user: { role, roles: [role], username: role + '-user' } };
    const res = { status(value) { status = value; return this; }, json(body) { resolve({ status, body }); return this; } };
    Promise.resolve(importHandler(req, res)).catch(reject);
  });
  return { entered, done, finish() { resolveParse([{ date: '2098-06-03', description: 'Statement purchase', reference: 'REF-12345', debit: 100, credit: 0, balance: 100 }]); } };
}
test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));
test('a statement parse cannot overwrite a native bank correction committed while parsing', async () => {
  const f = seed(), pending = beginImport(f); await pending.entered;
  const cards = clone(f.cards), expenses = clone(f.expenses); cards.payments[0].amount = 499.63; cards.payments[0].date = '2098-06-02'; cards.audit.push({ id: 'BANK-AUTHORITY', action: 'BANK_CORRECTION' }); cards.auditSeq = 1;
  expenses.transfers[0].amount = 499.63; expenses.transfers[0].date = '2098-06-02';
  transaction.commit(tempDir, [{ fileName: 'expenses.json', before: JSON.stringify(f.expenses), after: JSON.stringify(expenses) }, { fileName: 'credit-cards.json', before: JSON.stringify(f.cards), after: JSON.stringify(cards) }]);
  pending.finish(); const result = await pending.done;
  assert.equal(result.status, 200, JSON.stringify(result.body)); const after = readCards();
  assert.equal(after.payments[0].amount, 499.63); assert.equal(after.payments[0].date, '2098-06-02');
  assert.ok(after.audit.some(row => row.id === 'BANK-AUTHORITY')); assert.equal(after.auditSeq, 2);
  assert.equal(Object.keys(after.statements).length, 1); assert.deepEqual(JSON.parse(fs.readFileSync(expenseFile, 'utf8')), expenses);
});
test('duplicate statement imported while parsing is detected from the fresh store', async () => {
  const f = seed(), pending = beginImport(f); await pending.entered;
  const cards = clone(f.cards), hash = crypto.createHash('sha256').update(fs.readFileSync(f.uploaded)).digest('hex');
  cards.statements['CCS-00009'] = { id: 'CCS-00009', cardId: 'CC-1', fileHash: hash, rows: [], status: 'review' }; cards.statementSeq = 9;
  fs.writeFileSync(cardFile, JSON.stringify(cards)); pending.finish(); const result = await pending.done;
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.alreadyImported, true); assert.equal(result.body.statement.id, 'CCS-00009'); assert.deepEqual(readCards(), cards);
});
test('card accessibility is revalidated after parsing without broadening existing permissions', async () => {
  const f = seed(), pending = beginImport(f, 'accounting'); await pending.entered;
  const cards = clone(f.cards); cards.cards['CC-1'].ownerOnly = true; fs.writeFileSync(cardFile, JSON.stringify(cards));
  pending.finish(); const result = await pending.done;
  assert.equal(result.status, 400); assert.match(result.body.error, /no longer accessible/i); assert.deepEqual(readCards(), cards); assert.equal(fs.existsSync(f.uploaded), false);
});
test('a card removed during parsing cannot be recreated by saving a stale snapshot', async () => {
  const f = seed(), pending = beginImport(f); await pending.entered;
  const cards = clone(f.cards); delete cards.cards['CC-1']; fs.writeFileSync(cardFile, JSON.stringify(cards));
  pending.finish(); const result = await pending.done;
  assert.equal(result.status, 400); assert.match(result.body.error, /no longer accessible/i); assert.deepEqual(readCards(), cards);
});
