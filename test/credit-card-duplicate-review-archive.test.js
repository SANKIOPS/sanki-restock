'use strict';
// Synthetic isolated stores only; never import or mutate live reconciliation data.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-card-review-archive-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
fs.writeFileSync(path.join(temp, 'expenses.json'), '{}');
const { router } = require('../modules/credit-cards');
const expenseModule = require('../modules/expenses');
const accounting = require('../modules/credit-card-accounting');
require('../auth');
const cardsFile = path.join(temp, 'credit-cards.json'), expensesFile = path.join(temp, 'expenses.json');
const owner = { username: 'archive-owner', role: 'owner', roles: ['owner'] };
const admin = { username: 'archive-admin', role: 'admin', roles: ['admin'], permissions: {} };
const reason = 'Archive only this exact duplicate review and retain every original posting.';
const clone = value => JSON.parse(JSON.stringify(value));
const read = file => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = (file, value) => fs.writeFileSync(file, JSON.stringify(value));
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
function invoke(operation = 'archive', options = {}) {
  const suffix = ['review', 'finalize'].includes(operation) ? operation : operation + '-review';
  const route = router.stack.find(layer => layer.route && layer.route.path === '/api/expenses/credit-cards/statements/:id/' + suffix && layer.route.methods.post);
  assert.ok(route); let status = 200, body;
  route.route.stack[0].handle({ body: options.body || { reviewRevision: operation === 'restore' ? 8 : 7, reason }, params: { id: options.id || 'CCS-DUPLICATE' }, user: options.user || owner }, { status(code) { status = code; return this; }, json(value) { body = value; return this; } });
  return { status, body };
}
function bytes(directory = temp, prefix = '') {
  const result = {};
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const relative = prefix + entry.name;
    if (entry.isDirectory()) Object.assign(result, bytes(path.join(directory, entry.name), relative + '/'));
    else result[relative] = fs.readFileSync(path.join(directory, entry.name)).toString('base64');
  }
  return result;
}
function fixture() {
  for (const name of ['salary.json', 'salary-samast.json', 'incentives.json', 'procurement.json', '.bank-authority-transaction.json']) if (fs.existsSync(path.join(temp, name))) fs.unlinkSync(path.join(temp, name));
  const card = { id: 'CC-SYNTHETIC', name: 'Synthetic Card', issuingBank: 'Synthetic Bank', last4: '4321', ownerOnly: false };
  const row = { id: 'CCR-SHARED', date: '2026-06-12', postingDate: '2026-06-13', narration: 'SYNTHETIC PURCHASE', reference: 'SYNTHETIC-REFERENCE', amount: 123.45, debit: 123.45, credit: 0, balance: 123.45, classification: 'expense', merchant: 'Synthetic merchant', category: 'Original category', type: 'running', nature: 'SANKI', channel: 'Both', confirmed: true, duplicateResolution: '', allocations: [] };
  const source = { id: 'CCS-SOURCE', cardId: card.id, status: 'finalized', kind: 'billed', originalName: 'original.pdf', storedName: 'original.pdf', fileHash: 'a'.repeat(64), uploadedAt: '2026-06-14T10:00:00.000Z', finalizedAt: '2026-06-15T10:00:00.000Z', finalizedBy: owner.username, rows: [row] };
  const duplicate = { id: 'CCS-DUPLICATE', cardId: card.id, status: 'review', kind: 'billed', reviewRevision: 7, originalName: 'duplicate.pdf', storedName: 'duplicate.pdf', fileHash: 'b'.repeat(64), uploadedAt: '2026-06-16T10:00:00.000Z', summary: { marker: 'Retain the original summary' }, rows: [{ ...clone(row), merchant: 'Saved merchant choice', category: 'Saved category choice', type: 'marketing', confirmed: false, duplicateOf: { statementId: source.id, rowId: row.id }, merchantAutofill: { version: 1, touchedFields: ['category'] } }] };
  const cards = { cards: { [card.id]: card }, statements: { [source.id]: source, [duplicate.id]: duplicate }, payments: [], merchantRules: { original: { category: 'Retain unchanged' } }, audit: [{ id: 'CCA-000001', subjectId: source.id, action: 'STATEMENT_FINALIZED', at: source.finalizedAt, by: owner.username }], auditSeq: 1 };
  const expenses = { expenses: {}, transfers: [], receipts: [], reconciliationExpenses: [], bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, requests: [], auditLog: [{ id: 'AUD-000001', action: 'PRESERVED' }], auditSeq: 1 };
  accounting.syncPostings(cards, expenses); write(cardsFile, cards); write(expensesFile, expenses);
  for (const name of ['original.pdf', 'duplicate.pdf']) fs.writeFileSync(path.join(temp, 'credit-card-statements', name), 'SYNTHETIC EVIDENCE ' + name);
  return { card, cards, source, duplicate, expenses };
}
function rejected(mutate, operation = 'archive', options = {}) {
  const state = fixture();
  if (operation === 'restore') { assert.equal(invoke().status, 200); state.cards = read(cardsFile); state.card = state.cards.cards[state.card.id]; state.duplicate = state.cards.statements[state.duplicate.id]; state.source = state.cards.statements[state.source.id]; }
  mutate(state); write(cardsFile, state.cards); write(expensesFile, state.expenses);
  const before = bytes(), result = invoke(operation, options);
  assert.ok(result.status >= 400, JSON.stringify(result.body)); assert.equal(result.body.success, false); assert.deepEqual(bytes(), before, 'rejection must not modify any upload, accounting store, audit or journal');
  return result;
}
for (const user of [owner, admin]) test('authorized ' + user.role + ' archives and restores only retained duplicate metadata; source, choices, evidence and accounting bytes are unchanged', () => {
  const state = fixture(), original = clone(state.cards), beforeExpenses = fs.readFileSync(expensesFile, 'utf8'), beforeFiles = bytes();
  const result = invoke('archive', { user }); assert.equal(result.status, 200, result.body.error);
  let cards = read(cardsFile), archived = cards.statements[state.duplicate.id];
  assert.equal(archived.status, 'archived'); assert.equal(archived.reviewRevision, 8); assert.deepEqual(archived.rows, state.duplicate.rows); assert.deepEqual(archived.summary, state.duplicate.summary);
  assert.equal(result.body.statement.canDiscardReview, false); assert.equal(result.body.statement.canArchiveReview, false); assert.equal(result.body.statement.canRestoreReview, true);
  assert.deepEqual(cards.statements[state.source.id], original.statements[state.source.id]);
  for (const key of ['cards', 'payments', 'merchantRules']) assert.deepEqual(cards[key], original[key]);
  assert.equal(fs.readFileSync(expensesFile, 'utf8'), beforeExpenses);
  assert.equal(cards.audit.at(-1).action, 'STATEMENT_REVIEW_ARCHIVED'); assert.equal(cards.audit.at(-1).details.reason, reason); assert.deepEqual(cards.audit.at(-1).details.before, state.duplicate);
  const restored = invoke('restore', { user }); assert.equal(restored.status, 200, restored.body.error);
  cards = read(cardsFile); const review = cards.statements[state.duplicate.id];
  assert.equal(review.status, 'review'); assert.equal(review.reviewRevision, 9); assert.equal(review.reviewArchive, undefined); assert.deepEqual(review.rows, state.duplicate.rows);
  assert.deepEqual(cards.statements[state.source.id], state.source); assert.equal(fs.readFileSync(expensesFile, 'utf8'), beforeExpenses); assert.equal(cards.audit.at(-1).action, 'STATEMENT_REVIEW_RESTORED');
  const afterFiles = bytes(); delete afterFiles['credit-cards.json']; delete beforeFiles['credit-cards.json']; assert.deepEqual(afterFiles, beforeFiles);
});
for (const [label, mutate] of [
  ['ordinary unmatched review', s => { delete s.duplicate.rows[0].duplicateOf; }],
  ['partly duplicated review', s => { s.duplicate.rows.push({ ...clone(s.duplicate.rows[0]), id: 'CCR-UNMATCHED', duplicateOf: undefined }); }],
  ['finalized review', s => { s.duplicate.status = 'finalized'; }],
  ['reopened review', s => { s.duplicate.reopenedAt = '2026-06-17T00:00:00.000Z'; }],
  ['prior finalization audit', s => { s.cards.audit.push({ id: 'CCA-000002', subjectId: s.duplicate.id, action: 'STATEMENT_FINALIZED' }); s.cards.auditSeq = 2; }],
  ['source in review', s => { s.source.status = 'review'; }],
  ['source missing', s => { delete s.cards.statements[s.source.id]; }],
  ['source amount changed', s => { s.source.rows[0].debit = s.source.rows[0].amount = 124.45; }],
  ['source reference changed', s => { s.source.rows[0].reference = 'DIFFERENT'; }],
  ['superseded source', s => { s.source.rows[0].supersededBy = { statementId: 'CCS-NEW', rowId: 'CCR-NEW' }; }],
  ['ambiguous canonical source', s => { s.source.rows.push(clone(s.source.rows[0])); }],
  ['source card changed', s => { s.source.cardId = 'CC-OTHER'; }],
  ['duplicate active resolution', s => { s.duplicate.rows[0].duplicateResolution = 'replace'; }],
  ['own posted record', s => { s.expenses.reconciliationExpenses.push({ ...clone(s.expenses.reconciliationExpenses[0]), id: 'CCE-CCS-DUPLICATE-CCR-SHARED', creditCardStatementId: s.duplicate.id }); }],
  ['incoming receipt', s => { s.expenses.receipts.push({ id: 'RC-DEPENDENT', statementId: s.duplicate.id }); }],
  ['incoming qualified row link', s => { s.expenses.bankReconciliationLinks['CCS-DUPLICATE/CCR-SHARED'] = { bankId: 'BANK-DEPENDENT' }; }],
  ['incoming raw row link', s => { s.expenses.bankReconciliationLinks['CCR-SHARED'] = { bankId: 'BANK-DEPENDENT' }; }],
  ['pending approval', s => { s.expenses.bankReconciliationApprovals.PENDING = { status: 'pending', account: 'Synthetic Card 4321', from: '2026-06-01', to: '2026-06-30' }; }],
  ['closed account period', s => { s.expenses.bankStatements['Synthetic Card 4321'] = { account: 'Synthetic Card 4321', reconciledThrough: '2026-06-30' }; }],
  ['another statement depends on duplicate', s => { s.cards.statements.EXTRA = { id: 'EXTRA', cardId: s.card.id, rows: [{ linkedStatementId: s.duplicate.id }] }; }]
]) for (const operation of ['archive', 'restore']) test(operation + ' fails closed for ' + label, () => rejected(mutate, operation));
for (const [label, mutate] of [
  ['archive hash mismatch', s => { s.duplicate.rows[0].category = 'Unexpected change'; }],
  ['missing archive metadata', s => { delete s.duplicate.reviewArchive; }],
  ['metadata has unknown field', s => { s.duplicate.reviewArchive.ignoreChecks = true; }],
  ['missing archive audit', s => { s.cards.audit.pop(); s.cards.auditSeq--; }],
  ['wrong archive audit', s => { s.cards.audit.at(-1).details.reason = 'Different reason'; }],
  ['revision no longer matches archive', s => { s.duplicate.reviewRevision++; }]
]) test('restore rejects ' + label + ' without writes', () => rejected(mutate, 'restore'));
for (const operation of ['archive', 'restore']) {
  for (const [label, body] of [['missing revision', { reason }], ['stale revision', { reviewRevision: 2, reason }], ['string revision', { reviewRevision: '7', reason }], ['short reason', { reviewRevision: operation === 'restore' ? 8 : 7, reason: 'no' }], ['unexpected bypass field', { reviewRevision: operation === 'restore' ? 8 : 7, reason, allowLinked: true }]]) test(operation + ' requires valid ' + label, () => rejected(() => {}, operation, { body }));
  test(operation + ' rejects private source access and restricted cards for ungranted Admin', () => {
    rejected(s => { s.source.rows[0].nature = 'PERSONAL'; }, operation, { user: admin });
    rejected(s => { s.card.ownerOnly = true; }, operation, { user: admin });
    rejected(() => {}, operation, { user: { username: 'claimant', role: 'staff', roles: ['staff'] } });
    rejected(() => {}, operation, { user: { role: 'owner', roles: ['owner'] } });
  });
  for (const kind of ['expenses', 'cards', 'external']) test(operation + ' respects concurrent ' + kind + ' evidence and performs no partial write', () => {
    fixture(); if (operation === 'restore') assert.equal(invoke().status, 200);
    if (kind === 'external') write(path.join(temp, 'salary.json'), { payments: [] });
    const originalRead = expenseModule.sourceCorrectionPolicy.read; let reads = 0, concurrent;
    expenseModule.sourceCorrectionPolicy.read = () => { if (++reads === 2) { const file = kind === 'expenses' ? expensesFile : kind === 'cards' ? cardsFile : path.join(temp, 'salary.json'); const value = read(file); value.concurrentMarker = 'keep'; write(file, value); concurrent = bytes(); } return originalRead(); };
    try { const result = invoke(operation); assert.equal(result.status, 409); assert.ok(concurrent); assert.deepEqual(bytes(), concurrent); } finally { expenseModule.sourceCorrectionPolicy.read = originalRead; }
  });
  test(operation + ' leaves unfinished journal untouched', () => {
    fixture(); if (operation === 'restore') assert.equal(invoke().status, 200);
    fs.writeFileSync(path.join(temp, '.bank-authority-transaction.json'), '{unfinished synthetic journal');
    const before = bytes(), result = invoke(operation); assert.equal(result.status, 409); assert.deepEqual(bytes(), before);
  });
}
for (const operation of ['review', 'finalize', 'discard']) test('archived review cannot use ' + operation + ' as an archive bypass', () => {
  fixture(); assert.equal(invoke().status, 200); const before = bytes();
  const result = invoke(operation, { body: { reviewRevision: 8, reason, rows: [] } });
  assert.ok(result.status >= 400); assert.deepEqual(bytes(), before);
});
test('archived duplicate evidence remains a dependency of its original source', () => {
  fixture(); assert.equal(invoke().status, 200);
  const cards = read(cardsFile), guard = require('../modules/source-correction-guards');
  assert.ok(guard.references(cards.statements['CCS-DUPLICATE']).has('CCS-SOURCE'));
  const source = cards.statements['CCS-SOURCE']; source.status = 'review';
  const fresh = { id: 'CCS-FRESH', cardId: source.cardId, kind: 'billed', status: 'review', rows: [{ ...clone(source.rows[0]) }] };
  accounting.matchRows(cards, fresh); assert.equal(fresh.rows[0].duplicateOf, undefined, 'the retained archive is not an active canonical matching source');
});
