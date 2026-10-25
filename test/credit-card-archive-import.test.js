'use strict';
// Only parser input is synthetic; the real import route must retain an archived
// same-file review instead of making it actionable again or duplicating it.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-archived-card-import-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
const modulePath = require.resolve('../modules/expenses');
require.cache[modulePath] = { id: modulePath, filename: modulePath, loaded: true, exports: { parseBankStatementUpload: async () => [{ date: '2026-06-12', description: 'Synthetic source purchase', debit: 123.45, credit: 0 }] } };
const { router } = require('../modules/credit-cards');
const handler = router.stack.find(layer => layer.route && layer.route.path === '/api/expenses/credit-cards/statements/import').route.stack.at(-1).handle;
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
test('same-file import retains archived duplicate, revision, choices, source and accounting bytes without reactivation', async () => {
  const uploaded = path.join(temp, 'reupload.pdf'), sourceFile = path.join(temp, 'credit-card-statements', 'original.pdf'), retained = path.join(temp, 'credit-card-statements', 'duplicate.pdf'), content = 'SYNTHETIC RETAINED DUPLICATE UPLOAD';
  fs.writeFileSync(uploaded, content); fs.writeFileSync(retained, content); fs.writeFileSync(sourceFile, 'SYNTHETIC SOURCE EVIDENCE');
  const row = { id: 'CCR-1', date: '2026-06-12', amount: 123.45, debit: 123.45, credit: 0, narration: 'Synthetic source purchase', category: 'Saved choice', merchant: 'Saved merchant', duplicateOf: { statementId: 'CCS-SOURCE', rowId: 'CCR-1' } };
  const archived = { id: 'CCS-ARCHIVED', cardId: 'CC-1', status: 'archived', reviewRevision: 8, uploadedAt: '2026-06-15', storedName: 'duplicate.pdf', fileHash: crypto.createHash('sha256').update(content).digest('hex'), reviewArchive: { version: 1, reason: 'Retained duplicate only' }, rows: [row] };
  const cards = { cards: { 'CC-1': { id: 'CC-1', name: 'Synthetic Card', last4: '4321' } }, statements: { 'CCS-SOURCE': { id: 'CCS-SOURCE', cardId: 'CC-1', status: 'finalized', storedName: 'original.pdf', rows: [{ ...row, duplicateOf: undefined }] }, 'CCS-ARCHIVED': archived }, payments: [], merchantRules: {}, audit: [], auditSeq: 0, statementSeq: 2 };
  const cardsFile = path.join(temp, 'credit-cards.json'), expensesFile = path.join(temp, 'expenses.json');
  fs.writeFileSync(cardsFile, JSON.stringify(cards)); fs.writeFileSync(expensesFile, JSON.stringify({ expenses: {}, reconciliationExpenses: [{ id: 'SOURCE-POSTING', amount: 123.45, creditCardStatementId: 'CCS-SOURCE' }] }));
  const beforeCards = fs.readFileSync(cardsFile, 'utf8'), beforeExpenses = fs.readFileSync(expensesFile, 'utf8'); let status = 200, body;
  await handler({ user: { username: 'synthetic-owner', role: 'owner', roles: ['owner'] }, body: { cardId: 'CC-1' }, file: { path: uploaded, originalname: 'duplicate.pdf', mimetype: 'application/pdf' } }, { status(value) { status = value; return this; }, json(value) { body = value; return this; } });
  assert.equal(status, 200, body.error); assert.equal(body.alreadyImported, true); assert.equal(body.statement.id, 'CCS-ARCHIVED'); assert.equal(body.statement.status, 'archived'); assert.equal(body.statement.reviewRevision, 8); assert.equal(body.statement.rows[0].category, 'Saved choice');
  assert.equal(fs.readFileSync(cardsFile, 'utf8'), beforeCards); assert.equal(fs.readFileSync(expensesFile, 'utf8'), beforeExpenses);
  assert.equal(fs.readFileSync(retained, 'utf8'), content); assert.equal(fs.readFileSync(sourceFile, 'utf8'), 'SYNTHETIC SOURCE EVIDENCE'); assert.equal(fs.existsSync(uploaded), false, 'only the redundant temporary upload is removed by the existing import deduplication');
});
