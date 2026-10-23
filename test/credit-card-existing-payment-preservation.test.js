'use strict';

// Synthetic router-level regressions. No live statement, source document or
// production accounting snapshot is loaded by this test file.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-CARD-PRESERVATION', rows: [] } };
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'synthetic-card-payment-preservation-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
const cardsFile = path.join(temp, 'credit-cards.json');
const expensesFile = path.join(temp, 'expenses.json');
fs.writeFileSync(expensesFile, JSON.stringify({ expenses: {}, receipts: [], receivables: {}, oneTimeMigrations: {} }));
const { router } = require('../modules/credit-cards');
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
const clone = value => JSON.parse(JSON.stringify(value));
const read = file => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = (file, value) => fs.writeFileSync(file, JSON.stringify(value));
const snapshot = () => [cardsFile, expensesFile].map(file => fs.readFileSync(file, 'utf8'));
const cardId = 'CC-SYNTHETIC-1', statementId = 'CCS-SYNTHETIC-24', rowId = 'CCR-SYNTHETIC-SETTLEMENT';
const account = 'Axis Bank 3448', date = '2098-07-20', amount = 1234.56;
const route = '/api/expenses/credit-cards/statements';

function dispatch(method, url, { body = {}, query = {}, role = 'owner', username = 'synthetic-reviewer', permissions = {} } = {}) {
  const req = { method, path: url, url, originalUrl: url, baseUrl: '', body, query, params: {}, headers: { 'user-agent': 'Synthetic card preservation test' }, user: { username, role, roles: [role], permissions } };
  return new Promise((resolve, reject) => {
    let status = 200, finished = false;
    const headers = {}, finish = result => { if (!finished) { finished = true; resolve({ status, body: result }); } };
    const res = { headersSent: false, statusCode: 200,
      status(code) { this.statusCode = status = code; return this; },
      setHeader(name, value) { headers[String(name).toLowerCase()] = value; },
      getHeader(name) { return headers[String(name).toLowerCase()]; },
      removeHeader(name) { delete headers[String(name).toLowerCase()]; },
      json(result) { this.headersSent = true; finish(result); return this; },
      send(result) { this.headersSent = true; finish(result); return this; },
      end(result) { this.headersSent = true; finish(result); return this; }
    };
    router.handle(req, res, error => error ? reject(error) : finish(undefined));
  });
}
function fixture({ source = true } = {}) {
  const expenses = Array.from({ length: 23 }, (_, index) => ({
    id: 'CCR-SYNTHETIC-EXPENSE-' + index, date, postingDate: date,
    narration: 'Synthetic merchant transaction ' + index, merchant: 'Synthetic merchant ' + index,
    reference: 'SYNTHETIC-PURCHASE-' + index, amount: 10 + index, debit: 10 + index, credit: 0,
    classification: 'expense', category: 'Subscriptions', type: 'running', nature: 'SANKI', channel: 'Both',
    confirmed: true, bill: 'synthetic-proof-' + index, claimant: '', duplicateResolution: '',
    reviewerNote: 'Preserve this synthetic saved row ' + index
  }));
  const settlement = { id: rowId, date, postingDate: date, narration: 'PAYMENT RECEIVED THANK YOU', merchant: 'Synthetic issuer', reference: 'SYNTHETIC-CARD-CREDIT',
    amount, debit: 0, credit: amount, classification: 'card_payment', settlementMode: 'pending', settlementSourceId: '',
    settlementSourceFingerprint: '', settlementReason: 'Synthetic original saved settlement remark', confirmed: false, category: '', nature: '', channel: '', type: '' };
  const statement = { id: statementId, cardId, status: 'review', kind: 'billed', originalName: 'Synthetic statement fixture',
    storedName: '', mimeType: '', fileHash: 'synthetic-not-a-document-hash', uploadedAt: date + 'T10:00:00.000Z', uploadedBy: 'synthetic-reviewer',
    periodFrom: date, periodTo: date, reviewRevision: 19, statementSourceOfTruth: true, rows: expenses.concat(settlement) };
  const payment = { id: 'CCP-SYNTHETIC-1', cardId, nature: 'SANKI', account, amount, date, active: true, reference: 'SYNTHETIC-SETTLEMENT', proof: 'synthetic-bank-proof' };
  const transfer = { id: 'TR-SYNTHETIC-1', classification: 'credit_card_payment', creditCardPaymentId: payment.id, creditCardId: cardId,
    nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: account, toAccount: 'Synthetic Test Card 9001',
    amount, date, reference: payment.reference, note: 'Synthetic recorded card settlement' };
  const cards = { cards: { [cardId]: { id: cardId, name: 'Synthetic Test Card', last4: '9001', openingOutstanding: 5000, active: true } },
    statements: { [statementId]: statement }, payments: source ? [payment] : [], audit: [], auditSeq: 0, merchantRules: {} };
  const store = { expenses: {}, transfers: source ? [transfer] : [], receipts: [], reconciliationExpenses: [], creditCardSettlementAllocations: [], auditLog: [] };
  write(cardsFile, cards); write(expensesFile, store);
  return { cards, store, statement, payment, transfer, settlement };
}
async function current(options = {}) {
  const result = await dispatch('GET', route, { query: { cardId }, ...options });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  return result.body.statements.find(statement => statement.id === statementId);
}
async function choice(options) { return (await current(options)).rows.find(row => row.id === rowId).settlementCandidates[0]; }
function link(candidate, extra = {}) {
  return { id: rowId, merchant: 'Synthetic issuer', classification: 'card_payment', settlementMode: 'link_existing',
    settlementSourceId: candidate && candidate.id || 'payment:CCP-SYNTHETIC-1', settlementSourceFingerprint: candidate && candidate.fingerprint || 'missing',
    settlementReason: 'Synthetic verified recorded settlement', ...extra };
}
function review(rows, revision = 19, options = {}) { return dispatch('POST', route + '/' + statementId + '/review', { body: { rows, reviewRevision: revision }, ...options }); }
async function finalize(options = {}) { const statement = await current(); return dispatch('POST', route + '/' + statementId + '/finalize', { body: { reviewRevision: statement.reviewRevision }, ...options }); }
function unchanged(before, message) { assert.deepEqual(snapshot(), before, message); assert.equal(read(cardsFile).statements[statementId].rows.length, 24); }
function legacyFixture() {
  const saved = fixture({ source: false }), personalAccount = 'ICICI Bank 0993';
  const payment = { id: 'PAY-SYNTHETIC-LEGACY', date, amount, account: personalAccount, paymentType: 'UPI', personalFunds: true, proof: '/synthetic-existing-proof.jpg' };
  const expense = { id: 'EX-SYNTHETIC-LEGACY', date, amount, paidAmount: amount, personalPaidAmount: amount, account: personalAccount,
    nature: 'PERSONAL', ledger: 'Credit Card', vendor: 'Credit card Payment 9001', particulars: 'Credit card Payment', telegramNarration: 'Credit card -9001 Payment',
    status: 'paid', approvedAt: date + 'T10:00:00.000Z', paidAlready: true, fundedBy: 'claimant', paymentType: 'UPI',
    reimbursementStatus: 'not_applicable', reimbursementAmount: 0, reimbursementPayments: [], payments: [payment] };
  const movementId = expense.id + '/' + payment.id, bank = { transactionId: 'BTX-SYNTHETIC-LEGACY', date, debit: amount, credit: 0, reference: '', description: 'Synthetic self bank transfer' };
  saved.store.expenses[expense.id] = expense;
  saved.store.bankStatements = { ['PERSONAL|' + personalAccount]: { imports: [{ id: 'BST-SYNTHETIC-LEGACY', draftId: 'BRD-SYNTHETIC-LEGACY', finalizedAt: date + 'T11:00:00.000Z',
    reconciliationRows: [{ id: 'bank-0', bank, ledger: { id: movementId, date, debit: amount, credit: 0 }, linkedRecordIds: [movementId], decision: 'Matched with ledger entry' }] }] } };
  saved.store.bankDateOverrides = { [movementId]: { originalDate: date, bankDate: date, reconciliationDraft: 'BRD-SYNTHETIC-LEGACY' } };
  write(expensesFile, saved.store);
  return saved;
}

test('repeated GET candidate refresh preserves all 24 saved rows and both stores byte-for-byte', async () => {
  const saved = fixture(), before = snapshot();
  for (let i = 0; i < 3; i++) {
    const refreshed = await current();
    assert.equal(refreshed.status, 'review'); assert.equal(refreshed.reviewRevision, 19);
    assert.equal(refreshed.rows.length, 24);
    assert.equal(refreshed.rows.at(-1).settlementCandidates.length, 1);
    assert.equal(refreshed.rows.at(-1).settlementCandidates[0].eligible, true);
    unchanged(before, 'a GET must not save classifications, sources, audit events or revisions');
  }
  assert.deepEqual(read(cardsFile).statements[statementId], saved.statement);
});

test('an empty exact-card source catalog neither invents a payment nor destroys the saved review', async () => {
  const saved = fixture({ source: false });
  // An unrelated transfer is not sufficient card identity, even at the same amount/date.
  saved.store.transfers.push({ ...saved.transfer, classification: 'internal_transfer', creditCardId: undefined, creditCardPaymentId: undefined });
  write(expensesFile, saved.store);
  const before = snapshot(), refreshed = await current();
  assert.deepEqual(refreshed.rows.at(-1).settlementCandidates, []);
  assert.equal((await review([link(null)])).status, 409);
  unchanged(before, 'unavailable sources must fail without changing any saved row');
  assert.deepEqual(read(cardsFile).statements[statementId], saved.statement);
});

test('failed source validation is atomic even when another row in the same request is edited first', async () => {
  const saved = fixture(), candidate = await choice(), before = snapshot();
  const expenseEdit = { ...saved.statement.rows[0], merchant: 'Unsaved replacement merchant', category: 'Different category' };
  const rejected = await review([expenseEdit, link(candidate, { settlementSourceFingerprint: 'forged-fingerprint' })]);
  assert.equal(rejected.status, 409); assert.match(rejected.body.error, /changed|refresh/i);
  unchanged(before, 'a rejected settlement must not partially save earlier expense edits');
  assert.deepEqual(read(cardsFile).statements[statementId], saved.statement);
});

test('stale and omitted revisions cannot overwrite or finalize a 24-row saved review', async () => {
  fixture(); const candidate = await choice(), before = snapshot();
  for (const revision of [18, undefined]) {
    const reviewRejected = await dispatch('POST', route + '/' + statementId + '/review', { body: { rows: [link(candidate)], reviewRevision: revision } });
    assert.equal(reviewRejected.status, 409);
    unchanged(before);
    const rejected = await dispatch('POST', route + '/' + statementId + '/finalize', { body: { reviewRevision: revision } });
    assert.equal(rejected.status, 409); unchanged(before);
  }
});

test('existing-source linking still rejects one-paise, card-identity and out-of-window date changes', async () => {
  for (const mutate of [
    saved => { saved.payment.amount += 0.01; saved.transfer.amount += 0.01; },
    saved => { saved.transfer.creditCardId = 'CC-SYNTHETIC-WRONG'; },
    saved => { saved.payment.date = saved.transfer.date = '2098-07-16'; }
  ]) {
    const saved = fixture(); mutate(saved); write(cardsFile, saved.cards); write(expensesFile, saved.store);
    const candidate = await choice(); assert.equal(candidate.eligible, false);
    const before = snapshot(), rejected = await review([link(candidate)]);
    assert.equal(rejected.status, 409); unchanged(before);
  }
});

test('source availability and current role permissions are rechecked rather than trusting a previously displayed option', async () => {
  fixture(); const candidate = await choice();
  const restricted = { role: 'accounting', username: 'synthetic-accountant' };
  assert.deepEqual((await current(restricted)).rows.at(-1).settlementCandidates, []);
  const before = snapshot(), rejected = await review([link(candidate)], 19, restricted);
  assert.equal(rejected.status, 409); unchanged(before);
  const claimant = await dispatch('GET', route, { role: 'claimant', username: 'synthetic-claimant' });
  assert.equal(claimant.status, 403); unchanged(before, 'the actual router guard must still run');
});

test('a permitted payment date offset still needs an explicit reason and preserves all other saved rows', async () => {
  const saved = fixture(); saved.payment.date = saved.transfer.date = '2098-07-18';
  write(cardsFile, saved.cards); write(expensesFile, saved.store);
  const candidate = await choice(); assert.equal(candidate.eligible, true); assert.equal(candidate.dateReasonRequired, true);
  const before = snapshot(), rejected = await review([link(candidate, { settlementReason: '' })]);
  assert.equal(rejected.status, 409); assert.match(rejected.body.error, /date difference/); unchanged(before);
  const accepted = await review([link(candidate, { settlementReason: 'Synthetic issuer posted two days after the bank payment' })]);
  assert.equal(accepted.status, 200, JSON.stringify(accepted.body));
  assert.deepEqual(read(cardsFile).statements[statementId].rows.slice(0, 23), saved.statement.rows.slice(0, 23));
});

test('another active statement claim prevents repeat linking without modifying either review', async () => {
  const saved = fixture(), candidate = await choice();
  const other = clone(saved.statement); other.id = 'CCS-SYNTHETIC-OTHER';
  other.rows = [{ ...other.rows.at(-1), ...link(candidate), id: 'CCR-SYNTHETIC-CLAIMED' }];
  saved.cards.statements[other.id] = other; write(cardsFile, saved.cards);
  const refreshed = await choice(); assert.equal(refreshed.eligible, false); assert.match(refreshed.blockedReason, /already linked/);
  const before = snapshot(), rejected = await review([link(candidate)]);
  assert.equal(rejected.status, 409); unchanged(before);
});

test('linking a verified source preserves the other 23 saved rows and creates no accounting movement', async () => {
  const saved = fixture(), candidate = await choice(), beforeExpenses = fs.readFileSync(expensesFile, 'utf8');
  const result = await review([link(candidate)]);
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const stored = read(cardsFile).statements[statementId];
  assert.equal(stored.status, 'review'); assert.equal(stored.rows.length, 24); assert.equal(stored.reviewRevision, 20);
  assert.deepEqual(stored.rows.slice(0, 23), saved.statement.rows.slice(0, 23));
  assert.equal(stored.rows.at(-1).settlementSourceId, candidate.id);
  assert.equal(fs.readFileSync(expensesFile, 'utf8'), beforeExpenses, 'saving a source link is not a bank withdrawal or expense posting');
});

test('a saved source fingerprint change blocks finalization without losing the 24-row review', async () => {
  fixture(); const candidate = await choice(); assert.equal((await review([link(candidate)])).status, 200);
  const cards = read(cardsFile); cards.payments[0].reference = 'SYNTHETIC-CHANGED-AFTER-REVIEW'; write(cardsFile, cards);
  const before = snapshot(), rejected = await finalize();
  assert.equal(rejected.status, 409); assert.match(rejected.body.error, /changed|refresh/i); unchanged(before);
  assert.equal(read(cardsFile).statements[statementId].status, 'review');
});

test('a missing previously saved source remains recorded in the review while refresh and finalization fail closed', async () => {
  fixture(); const candidate = await choice(); assert.equal((await review([link(candidate)])).status, 200);
  const cards = read(cardsFile), expenses = read(expensesFile); cards.payments = []; expenses.transfers = [];
  write(cardsFile, cards); write(expensesFile, expenses);
  const before = snapshot(), refreshed = await current(), row = refreshed.rows.at(-1);
  assert.deepEqual(row.settlementCandidates, []); assert.equal(row.settlementSourceId, candidate.id);
  assert.equal(row.settlementSourceFingerprint, candidate.fingerprint); unchanged(before);
  const rejected = await finalize(); assert.equal(rejected.status, 409); unchanged(before);
  assert.equal(read(cardsFile).statements[statementId].rows.at(-1).settlementSourceId, candidate.id);
});

test('synthetic valid finalization links the repayment once without a second withdrawal or expense offset', async () => {
  const saved = fixture(), candidate = await choice(); assert.equal((await review([link(candidate)])).status, 200);
  const before = read(expensesFile), result = await finalize();
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = read(expensesFile), stored = read(cardsFile).statements[statementId];
  assert.equal(stored.status, 'finalized'); assert.equal(stored.rows.length, 24);
  assert.deepEqual(after.transfers, before.transfers, 'statement finalization must not add or rewrite a bank transfer');
  const postings = after.reconciliationExpenses.filter(row => row.creditCardStatementId === statementId);
  assert.equal(postings.length, 23); assert.equal(postings.some(row => row.creditCardRowId === rowId), false);
  assert.equal(postings.reduce((sum, row) => sum + row.amount, 0), saved.statement.rows.slice(0, 23).reduce((sum, row) => sum + row.amount, 0));
  assert.equal(postings.some(row => row.amount < 0), false, 'repayment cannot offset the genuine purchase expenses');
});

test('legacy PERSONAL source discovery and rejected acknowledgments preserve all 24 saved rows and finalized bank evidence', async () => {
  const saved = legacyFixture(), before = snapshot(), candidate = await choice();
  assert.equal(candidate.sourceKind, 'legacy_expense_payment'); assert.equal(candidate.eligible, true);
  assert.equal(candidate.reference, '', 'a blank parsed bank reference must not be fabricated'); unchanged(before);
  for (const confirmation of [undefined, false, 'true', 1]) {
    const rejected = await review([link(candidate, { settlementLegacyConfirmed: confirmation })]);
    assert.equal(rejected.status, 409, JSON.stringify(rejected.body)); unchanged(before);
  }
  const rejected = await review([link(candidate, { settlementLegacyConfirmed: true, settlementSourceFingerprint: 'stale-source' })]);
  assert.equal(rejected.status, 409); unchanged(before);
  assert.deepEqual(read(cardsFile).statements[statementId], saved.statement);
});

test('saving the explicit legacy link retains every other saved row and cannot inherit a missing raw acknowledgment', async () => {
  const saved = legacyFixture(), candidate = await choice(), beforeExpenses = fs.readFileSync(expensesFile, 'utf8');
  const accepted = await review([link(candidate, { settlementLegacyConfirmed: true })]);
  assert.equal(accepted.status, 200, JSON.stringify(accepted.body));
  const stored = read(cardsFile).statements[statementId];
  assert.equal(stored.status, 'review'); assert.equal(stored.rows.length, 24); assert.equal(stored.reviewRevision, 20);
  assert.deepEqual(stored.rows.slice(0, 23), saved.statement.rows.slice(0, 23));
  assert.equal(stored.rows.at(-1).settlementLegacyConfirmed, true);
  assert.equal(fs.readFileSync(expensesFile, 'utf8'), beforeExpenses);
  const before = snapshot(), missingAck = await review([link(candidate)], 20);
  assert.equal(missingAck.status, 409); unchanged(before, 'a raw omitted checkbox cannot inherit an earlier stored true value');
});

test('synthetic legacy finalization preserves the original expense and bank records, with no additional withdrawal or expense offset', async () => {
  legacyFixture(); const candidate = await choice();
  assert.equal((await review([link(candidate, { settlementLegacyConfirmed: true })])).status, 200);
  const before = read(expensesFile), result = await finalize();
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = read(expensesFile);
  for (const key of ['expenses', 'transfers', 'receipts', 'bankStatements', 'bankDateOverrides', 'creditCardSettlementAllocations']) assert.deepEqual(after[key], before[key], key + ' must be retained unchanged');
  const postings = after.reconciliationExpenses.filter(row => row.creditCardStatementId === statementId);
  assert.equal(postings.length, 23); assert.equal(postings.some(row => row.creditCardRowId === rowId || row.amount < 0), false);
  assert.equal(read(cardsFile).statements[statementId].rows.length, 24);
});

test('purchase, fee and contradictory-card descriptions cannot masquerade as the narrow legacy repayment source', async () => {
  for (const description of ['Credit Card Purchase Payment 9001', 'Credit Card membership payment 9001', 'Credit Card fee payment 9001', 'Credit Card subscription payment 9001', 'Credit Card Payment 9002']) {
    legacyFixture(); const exp = read(expensesFile), expense = exp.expenses['EX-SYNTHETIC-LEGACY'];
    expense.vendor = description; write(expensesFile, exp);
    const before = snapshot(); assert.deepEqual((await current()).rows.at(-1).settlementCandidates, []); unchanged(before);
  }
});

test('a carried copy of the same identified finalized bank transaction is evidence, not a second repayment', async () => {
  legacyFixture(); const exp = read(expensesFile), book = exp.bankStatements['PERSONAL|ICICI Bank 0993'], original = book.imports[0];
  book.imports.push({ id: 'BST-SYNTHETIC-CARRIED', draftId: 'BRD-SYNTHETIC-CARRIED', finalizedAt: '2098-07-21T12:00:00.000Z', carriedReconciliationRows: clone(original.reconciliationRows) });
  write(expensesFile, exp); const before = snapshot(), candidate = await choice();
  assert.equal(candidate.eligible, true); unchanged(before);
});
