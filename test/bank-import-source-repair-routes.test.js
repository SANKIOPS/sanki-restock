'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const register = require('../modules/bank-import-source-repair-routes');
const clone = value => JSON.parse(JSON.stringify(value));
const endpoint = '/api/expenses/bank-statements/repair-import';
const draftId = 'BRD-SYNTHETIC-SOURCE-REPAIR';
const account = 'IndusInd Bank 8181';
const reason = 'Original retained PDF verifies the debit column as 490, not 90.';

function sourceRows() {
  // Synthetic surrounding rows reproduce the verified 90 -> 490 failure and
  // its exact statement totals without reading any live bank statement.
  let balance = 8692.70;
  const rows = [490, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 201].map((debit, index) => {
    balance = Math.round((balance - debit) * 100) / 100;
    const reference = String(123456789012 + index);
    return { date: index < 5 ? '2026-10-19' : '2026-10-20', reference,
      description: 'UPI/' + reference + '/SYNTHETIC VENDOR ' + index, debit, credit: 0, balance };
  }).reverse();
  rows.statementSummary = { format: 'IndusInd mobile PDF', parserVersion: 'positioned-cells-v1',
    accountLast4: '8181', from: '2026-10-19', to: '2026-10-20', openingBalance: 8692.70,
    openingBalanceInferred: true, closingBalance: 1.70, totalDebits: 8691, totalCredits: 0, validated: true };
  return rows;
}

function harness(t, options = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bank-source-repair-route-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  // Real retained bytes are hashed by the route; parsing is the sole injected
  // I/O service. The real planner and apply function are never mocked.
  const pdf = path.join(dir, 'retained.pdf');
  fs.writeFileSync(pdf, '%PDF-1.4\nSynthetic retained source for isolated route tests\n%%EOF');
  const fileHash = crypto.createHash('sha256').update(fs.readFileSync(pdf)).digest('hex');
  const parsed = sourceRows(), transactions = clone(parsed);
  transactions[9].debit = 90;
  const draft = { id: draftId, nature: 'SANKI', account, temporaryFile: 'retained.pdf',
    originalName: 'original-indusind.pdf', fileHash, transactions,
    summary: { ...parsed.statementSummary, openingBalance: 8292.70, totalDebits: 8291 },
    resolutions: {}, createdBy: 'owner-user', createdAt: '2026-10-21T05:00:00Z' };
  let state = { bankReconciliationDrafts: { [draftId]: draft }, bankStatements: {},
    expenses: { 'EX-PRESERVED': { id: 'EX-PRESERVED', amount: 490, paidAmount: 490,
      payments: [{ id: 'PAY-001', amount: 490, date: '2026-10-19', account, proof: '/preserved.jpg' }] } },
    adjustments: [{ id: 'ADJ-PRESERVED', account, amount: -5.9 }], transfers: [{ id: 'TR-PRESERVED', amount: 1000 }],
    receipts: [], bankDateOverrides: {}, bankReconciliationLinks: {}, bankReconciliationApprovals: {},
    openingBalances: { [account]: 47844.86 }, openingBalancesByNature: { PERSONAL: { 'Namita 5464': 123 } },
    physicalBankOpeningBalances: { 'IndusInd Bank 7883': 456 }, auditLog: [] };
  let handler, saved = 0, parsedCount = 0, accessCount = 0;
  const reviews = [], audits = [], parseCalls = [];
  const control = { get: () => clone(state), mutate(fn) { fn(state); }, pdf, fileHash, parsed,
    get saved() { return saved; }, get parsedCount() { return parsedCount; }, get accessCount() { return accessCount; }, reviews, audits, parseCalls };
  if (options.setup) options.setup(control);
  register({ post(route, fn) { assert.equal(route, endpoint); handler = fn; } }, {
    statementDraftDir: dir,
    reconciliationFile(root, filename) { return filename && path.basename(filename) === filename ? path.join(root, filename) : null; },
    loadStore() { return clone(state); }, saveStore(store) { state = clone(store); saved++; },
    canAccessBankDraft(req, store, current) { accessCount++; return options.access ? options.access({ req, store, current, count: accessCount }) : req.user.role === 'owner'; },
    async parseBankStatementUpload(file, name, password) {
      parsedCount++; parseCalls.push({ file, name, password });
      await new Promise(resolve => setImmediate(resolve));
      if (options.parseHook) await options.parseHook(control);
      const rows = clone(parsed); rows.statementSummary = clone(parsed.statementSummary); return rows;
    },
    draftReconciliation(store, current, config) {
      reviews.push({ store: clone(store), current: clone(current), config });
      if (options.reviewHook) options.reviewHook(store, current);
      return { rows: current.transactions.map((bank, index) => ({ id: 'bank-' + index, bank, status: 'missing_in_app' })) };
    },
    audit(store, req, action, type, subject, details) {
      const event = { action, type, subject, actor: req.user.username, ...details };
      audits.push(event); store.auditLog.push(event);
    }
  });
  control.call = async (mode = 'preview', extra = {}, user = { role: 'owner', username: 'owner-user' }) => {
    let status = 200, body;
    await handler({ body: { draftId, mode, ...extra }, user }, {
      status(value) { status = value; return this; }, json(value) { body = value; return this; }
    });
    return { status, body };
  };
  return control;
}

function financialSnapshot(store) {
  const { bankReconciliationDrafts, auditLog, ...accounting } = store;
  return accounting;
}
async function preview(h) {
  const result = await h.call(); assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.equal(result.body.success, true); return result.body;
}

test('preview verifies retained bytes, corrects only the imported 90 -> 490 row and writes nothing', async t => {
  const h = harness(t, { reviewHook(store, draft) { store.expenses['EX-PRESERVED'].amount = 999; draft.accidentalReviewMutation = true; } });
  const before = h.get(), plan = await preview(h);
  assert.equal(plan.changed, true); assert.match(plan.fingerprint, /^[a-f0-9]{64}$/);
  assert.equal(plan.sourceHash, h.fileHash); assert.equal(plan.changes.length, 1);
  assert.equal(plan.changes[0].rowId, 'bank-9'); assert.equal(plan.changes[0].before.debit, 90); assert.equal(plan.changes[0].after.debit, 490);
  assert.equal(plan.beforeSummary.openingBalance, 8292.70); assert.equal(plan.afterSummary.openingBalance, 8692.70);
  assert.equal(plan.beforeSummary.totalDebits, 8291); assert.equal(plan.afterSummary.totalDebits, 8691);
  assert.equal(plan.afterSummary.closingBalance, 1.70);
  assert.deepEqual(h.get(), before); assert.equal(h.saved, 0); assert.equal(h.audits.length, 0);
  assert.deepEqual(h.reviews[0].config, { readOnly: true }); assert.equal(h.accessCount, 2);
  assert.deepEqual(h.parseCalls, [{ file: h.pdf, name: 'original-indusind.pdf', password: '' }]);
});

test('apply requires the reviewed fingerprint, audits original values and never changes expenses, payments or account openings', async t => {
  const h = harness(t), before = h.get(), plan = await preview(h);
  const result = await h.call('apply', { fingerprint: plan.fingerprint, reason, amount: 999999, debit: 1, openingBalance: 0 });
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.applied, true);
  const after = h.get(), repaired = after.bankReconciliationDrafts[draftId];
  assert.deepEqual(financialSnapshot(after), financialSnapshot(before));
  assert.deepEqual(repaired.transactions.slice(0, 9), before.bankReconciliationDrafts[draftId].transactions.slice(0, 9));
  assert.equal(repaired.transactions[9].debit, 490); assert.equal(repaired.transactions[9].balance, 8202.70);
  assert.equal(repaired.summary.openingBalance, 8692.70); assert.equal(repaired.summary.totalDebits, 8691); assert.equal(repaired.summary.closingBalance, 1.70);
  assert.equal(repaired.sourceAmountRepairHistory.length, 1); assert.equal(repaired.sourceAmountRepair.by, 'owner-user');
  assert.equal(repaired.sourceAmountRepair.reason, reason); assert.equal(repaired.sourceAmountRepair.ledgerRecordsUnchanged, true);
  assert.equal(h.audits.length, 1); assert.equal(h.audits[0].action, 'BANK_STATEMENT_IMPORT_REPAIRED');
  assert.equal(h.audits[0].sourceHash, h.fileHash); assert.match(h.audits[0].note, /account openings unchanged/);
  assert.equal(h.saved, 1);
  const repeated = await h.call('apply', { fingerprint: plan.fingerprint, reason });
  assert.equal(repeated.status, 200); assert.equal(repeated.body.changed, false);
  assert.deepEqual(h.get(), after); assert.equal(h.saved, 1); assert.equal(h.audits.length, 1);
});

test('missing drafts and denied access cannot read retained evidence or write anything', async t => {
  for (const [extra, user, status] of [
    [{ draftId: 'MISSING' }, { role: 'owner', username: 'owner-user' }, 404],
    [{}, { role: 'claimant', username: 'not-authorized' }, 403]
  ]) {
    const h = harness(t), before = h.get(), result = await h.call('preview', extra, user);
    assert.equal(result.status, status); assert.equal(h.parsedCount, 0); assert.equal(h.saved, 0); assert.deepEqual(h.get(), before);
  }
});

test('only explicit preview/apply modes are accepted', async t => {
  for (const mode of ['', 'repair', 'finalize', null]) {
    const h = harness(t), before = h.get(), result = await h.call(mode);
    assert.equal(result.status, 400); assert.equal(h.parsedCount, 0); assert.deepEqual(h.get(), before);
  }
});

for (const [name, setup] of [
  ['unavailable PDF', h => h.mutate(store => { store.bankReconciliationDrafts[draftId].temporaryFile = 'missing.pdf'; })],
  ['missing saved hash', h => h.mutate(store => { delete store.bankReconciliationDrafts[draftId].fileHash; })],
  ['wrong saved hash', h => h.mutate(store => { store.bankReconciliationDrafts[draftId].fileHash = '0'.repeat(64); })],
  ['non-PDF source', h => h.mutate(store => { store.bankReconciliationDrafts[draftId].originalName = 'import.csv'; })],
  ['unsafe retained path', h => h.mutate(store => { store.bankReconciliationDrafts[draftId].temporaryFile = '../retained.pdf'; })],
  ['merged source files', h => h.mutate(store => { store.bankReconciliationDrafts[draftId].sourceStatements = [{}, {}]; })]
]) test(name + ' blocks the repair before parsing', async t => {
  const h = harness(t, { setup }), before = h.get(), result = await h.call();
  assert.equal(result.status, 409); assert.equal(h.parsedCount, 0); assert.equal(h.saved, 0); assert.deepEqual(h.get(), before);
});

test('a single retained source statement uses its original filename and bytes', async t => {
  const h = harness(t, { setup(h) { h.mutate(store => { const draft = store.bankReconciliationDrafts[draftId];
    draft.sourceStatements = [{ temporaryFile: draft.temporaryFile, originalName: 'source-one.PDF', fileHash: draft.fileHash }]; }); } });
  assert.equal((await preview(h)).changed, true); assert.equal(h.parseCalls[0].name, 'source-one.PDF');
});

test('source PDF changes during async parsing invalidate the entire operation', async t => {
  const h = harness(t, { parseHook(h) { fs.appendFileSync(h.pdf, '\nchanged source'); } }), before = h.get();
  const result = await h.call(); assert.equal(result.status, 409); assert.match(result.body.error, /PDF changed/);
  assert.equal(h.saved, 0); assert.deepEqual(h.get(), before);
});

for (const removed of [false, true]) test('a draft ' + (removed ? 'deleted' : 'reviewed') + ' during parsing cannot be overwritten', async t => {
  const h = harness(t, { parseHook(h) { h.mutate(store => {
    if (removed) delete store.bankReconciliationDrafts[draftId];
    else store.bankReconciliationDrafts[draftId].resolutions['bank-9'] = { action: 'link_existing', appId: 'EX-PRESERVED/PAY-001' };
  }); } });
  const result = await h.call(); assert.equal(result.status, 409); assert.match(result.body.error, /review changed/);
  assert.equal(h.saved, 0);
  if (removed) assert.equal(h.get().bankReconciliationDrafts[draftId], undefined);
  else assert.equal(h.get().bankReconciliationDrafts[draftId].resolutions['bank-9'].appId, 'EX-PRESERVED/PAY-001');
});

test('access is rechecked against the fresh store after asynchronous parsing', async t => {
  const h = harness(t, { access: ({ count }) => count === 1 }), before = h.get(), result = await h.call();
  assert.equal(result.status, 403); assert.equal(h.parsedCount, 1); assert.equal(h.accessCount, 2);
  assert.equal(h.saved, 0); assert.deepEqual(h.get(), before);
});

test('preview sees concurrent accounting updates and a later apply preserves every such update', async t => {
  let changed = false;
  const h = harness(t, { parseHook(h) { if (!changed) { changed = true; h.mutate(store => {
    store.expenses['EX-CONCURRENT'] = { id: 'EX-CONCURRENT', amount: 777, payments: [{ amount: 777, date: '2026-10-21' }] };
    store.openingBalances['Another account'] = 321;
  }); } } });
  const plan = await preview(h), fresh = h.get();
  assert.equal(h.reviews[0].store.expenses['EX-CONCURRENT'].amount, 777);
  const result = await h.call('apply', { fingerprint: plan.fingerprint, reason });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.deepEqual(financialSnapshot(h.get()), financialSnapshot(fresh));
});

test('an accounting change during apply invalidates an earlier preview fingerprint without losing the concurrent update', async t => {
  let parsingApply = false;
  const h = harness(t, { parseHook(h) { if (parsingApply) h.mutate(store => { store.adjustments.push({ id: 'ADJ-CONCURRENT', amount: -12 }); }); } });
  const plan = await preview(h); parsingApply = true;
  const result = await h.call('apply', { fingerprint: plan.fingerprint, reason });
  assert.equal(result.status, 409); assert.match(result.body.error, /changed|preview/i);
  assert.equal(h.saved, 0); assert.equal(h.get().adjustments.at(-1).id, 'ADJ-CONCURRENT');
  assert.equal(h.get().bankReconciliationDrafts[draftId].transactions[9].debit, 90);
});

test('apply rejects blank/overlong reasons and missing/stale fingerprints without financial mutation', async t => {
  for (const invalid of [{ reason: '' }, { reason: '  ' }, { reason: 'x'.repeat(2001) }, { fingerprint: undefined }, { fingerprint: 'stale' }]) {
    const h = harness(t), plan = await preview(h), before = h.get();
    const result = await h.call('apply', { reason, fingerprint: plan.fingerprint, ...invalid });
    assert.equal(result.status, Object.hasOwn(invalid, 'reason') ? 400 : 409);
    assert.equal(h.saved, 0); assert.deepEqual(h.get(), before);
  }
});

test('a newly posted source discovered after parsing blocks repair even when the draft itself did not change', async t => {
  const h = harness(t, { parseHook(h) { h.mutate(store => { store.adjustments.push({ id: 'ADJ-NEW-SOURCE',
    account, amount: -90, reconciliationDraft: draftId, bankRowId: 'bank-9' }); }); } });
  const result = await h.call(); assert.equal(result.status, 409); assert.match(result.body.error, /linked|posted/);
  assert.equal(h.saved, 0); assert.equal(h.get().adjustments.at(-1).id, 'ADJ-NEW-SOURCE');
  assert.equal(h.get().bankReconciliationDrafts[draftId].transactions[9].debit, 90);
});

test('parser failures return a safe error without audit or save', async t => {
  const h = harness(t, { parseHook() { throw new Error('Synthetic original-column evidence unavailable'); } }), before = h.get();
  const result = await h.call(); assert.equal(result.status, 409); assert.match(result.body.error, /evidence unavailable/);
  assert.equal(h.saved, 0); assert.equal(h.audits.length, 0); assert.deepEqual(h.get(), before);
});
