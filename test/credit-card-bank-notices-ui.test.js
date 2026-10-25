'use strict';

// Synthetic bank notices only; never load or publish a customer's statement.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const start = html.indexOf('    function renderCreditCardBankNotices(');
const end = html.indexOf('    window.editCreditCard=', start);
assert.ok(start >= 0 && end > start);
const esc = value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
const notice = () => ({ date: '2026-06-12', reference: '12345678901', description: 'Auto Dr.Retn-Insuff. fund', cardLast4: '4321', amount: 0, debit: 0, credit: 0, sourceRow: 1, reason: 'zero_value_bank_notice', explanation: 'ICICI auto-debit return notice: insufficient funds; no money moved and no ledger entry is posted.' });
function context() {
  const ctx = vm.createContext({ esc, fmt: amount => 'INR ' + Number(amount).toFixed(2), ccData: { natures: ['SANKI'] }, CreditCardReview: { rowCells: row => '<td>' + esc(row.narration) + '</td>' }, ReconciliationStatus: { card: () => '' } });
  vm.runInContext(html.slice(start, end), ctx);
  return ctx;
}
function statement() { return { id: 'CCS-SYNTHETIC', status: 'review', reviewRevision: 2, kind: 'billed', rows: [{ id: 'CCR-SYNTHETIC', narration: 'SYNTHETIC PURCHASE', debit: 123.45, credit: 0 }], totals: { debit: 123.45, credit: 0 }, summary: { closingBalance: 123.45, nonPostingEntries: [notice()] } }; }

test('no bank notice block is invented for old or ordinary statement summaries', () => {
  const ctx = context();
  for (const summary of [undefined, null, {}, { nonPostingEntries: [] }, { nonPostingEntries: {} }]) assert.equal(ctx.renderCreditCardBankNotices(summary), '');
});
test('bank notice retains date, reference, source card and explanation in its own evidence table', () => {
  const ctx = context(), output = ctx.renderCreditCardBankNotices({ nonPostingEntries: [notice()] });
  for (const text of ['Bank notices — no money movement (1)', '2026-06-12', '12345678901', '4321', 'Auto Dr.Retn-Insuff. fund', 'INR 0.00', 'no ledger entry is posted', 'separate from the transaction totals']) assert.ok(output.includes(text), text);
  assert.doesNotMatch(output, /class="ccrow"|<input|<select|onclick=|fetch\(|api\(/);
});
test('bank notice remains visible in review and finalized evidence without becoming a financial review row', () => {
  const ctx = context();
  for (const status of ['review', 'finalized']) {
    const st = statement(); st.status = status;
    const output = ctx.renderCreditCardReview(st);
    assert.match(output, /1 transactions/);
    assert.equal((output.match(/class="ccrow"/g) || []).length, 1);
    assert.match(output, /Bank notices — no money movement \(1\)/);
    assert.match(output, /Card balance movement \(DR − CR\), not expenses <b>INR 123\.45/);
    assert.match(output, /Statement amount due <b>INR 123\.45/);
    assert.equal(st.rows.length, 1);
  }
});
test('every text field in bank evidence is escaped rather than executed', () => {
  const ctx = context(), bad = '<img src=x onerror=alert(1)>';
  const entry = notice(); for (const key of ['date', 'reference', 'description', 'cardLast4', 'explanation']) entry[key] = bad;
  const output = ctx.renderCreditCardBankNotices({ nonPostingEntries: [entry] });
  assert.doesNotMatch(output, /<img|<script/); assert.equal((output.match(/&lt;img/g) || []).length, 5);
});
test('rendering bank notices changes no saved source or financial fields', () => {
  const ctx = context(), st = statement(), before = JSON.stringify(st);
  ctx.renderCreditCardReview(st); assert.equal(JSON.stringify(st), before);
});
test('retained notice metadata is saved by the import path only as source evidence, never as a posted transaction', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../modules/credit-cards.js'), 'utf8');
  const from = source.indexOf("router.post('/api/expenses/credit-cards/statements/import'");
  const to = source.indexOf("router.post('/api/expenses/credit-cards/statements/manual'", from);
  assert.ok(from >= 0 && to > from);
  const st = statement(), parsed = [{ date: '2026-06-13', description: 'SYNTHETIC PURCHASE', reference: '12345678902', debit: 123.45, credit: 0, balance: 123.45 }];
  parsed.statementSummary = st.summary;
  const cards = { cards: { 'CC-SYNTHETIC': { id: 'CC-SYNTHETIC', name: 'Synthetic Card', last4: '4321' } }, statements: {}, statementSeq: 0 };
  const expenses = { untouched: true }, expenseBefore = JSON.stringify(expenses);
  let handler, saved, result, syncCalls = 0;
  const ctx = vm.createContext({
    router: { post(route, upload, callback) { handler = callback; } }, upload: { single: () => () => {} },
    load: () => cards, visibleCard: () => true, parseBankStatementUpload: async () => parsed,
    fs: { readFileSync: () => Buffer.from('SYNTHETIC FILE BYTES'), unlinkSync() { throw new Error('Valid synthetic import must retain its original file'); } },
    crypto: require('node:crypto'), path, loadExpenses: () => expenses,
    inferClassification: () => 'expense', suggestRule: () => null, round2: value => Math.round(Number(value || 0) * 100) / 100,
    duplicateWarnings: () => [], cardAccounting: { merchantName: () => 'Synthetic merchant', matchRows: () => {}, syncPostings: () => { syncCalls++; } },
    audit: () => {}, save: value => { saved = JSON.parse(JSON.stringify(value)); }, statementView: (store, value) => value,
    Date, console
  });
  vm.runInContext(source.slice(from, to), ctx);
  await handler({ body: { cardId: 'CC-SYNTHETIC', kind: 'billed' }, file: { path: '/synthetic/source.pdf', originalname: 'synthetic.pdf', mimetype: 'application/pdf' }, user: { username: 'synthetic-reviewer' } }, { status() { return this; }, json(value) { result = value; } });
  assert.equal(result.success, true); assert.equal(result.statement.status, 'review');
  assert.equal(result.statement.rows.length, 1); assert.ok(result.statement.rows[0].amount > 0);
  assert.deepEqual(saved.statements[result.statement.id].summary.nonPostingEntries, st.summary.nonPostingEntries);
  assert.equal(JSON.stringify(expenses), expenseBefore); assert.equal(syncCalls, 0);
});
