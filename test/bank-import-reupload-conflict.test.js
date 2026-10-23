'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const { parseIndusIndMobileStatement } = require('../modules/indusind-mobile-statement');

const clone = value => JSON.parse(JSON.stringify(value));
const account = 'IndusInd Bank 8181', draftId = 'BRD-SYNTHETIC-OLD-PARSER';
const oldFile = 'synthetic-drafts/retained.pdf', newFile = 'synthetic-drafts/reupload.pdf';
const bytes = Buffer.from('%PDF synthetic identical retained and reuploaded evidence');
const fileHash = crypto.createHash('sha256').update(bytes).digest('hex');
const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');

function section(start, end) {
  const from = source.indexOf(start), to = source.indexOf(end, from);
  assert.ok(from >= 0 && to > from, 'actual import implementation remains available: ' + start);
  return source.slice(from, to);
}

// Execute the production create/merge/cleanup functions, but never initialize
// expenses.js, its startup migrations, filesystem stores or production services.
const implementation = [
  section('function reconciliationReference(', 'function reconciliationLegacyPriorCanMatch('),
  section('function reconciliationRowSetsHaveAmbiguousNarration(', 'function stagePaytmSettlementsFromAxisDraft('),
  section('function reconciliationDraftHasReviewState(', 'function extendPendingDraftThroughFinalizedCoverage('),
  section('async function createBankReconciliationDraft(', 'async function restoreApprovalTrimmedDraft('),
  section('const createSingleBankReconciliationDraft=', 'registerBankImportSourceRepair(router,')
].join('\n');

function positionedPdf() {
  const headings = ['Date', 'Particulars', 'Chq No/Ref No', 'Withdrawal', 'Deposit', 'Balance'];
  const columns = [20, 180, 450, 670, 800, 930];
  const item = (str, x, y) => ({ str, width: str.length * 6, transform: [1, 0, 0, 1, x, y] });
  let balance = 8692.70;
  const records = [490, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 201].map((debit, index) => {
    balance = Math.round((balance - debit) * 100) / 100;
    return [index < 5 ? '19 Oct 2026' : '20 Oct 2026',
      'UPI/' + (123456789012 + index) + '/DR/SYNTHETIC', 'S' + (1234567 + index),
      debit.toFixed(2), '0.00', balance.toFixed(2)];
  }).reverse();
  const items = headings.map((value, index) => item(value, columns[index], 700));
  records.forEach((record, index) => record.forEach((value, column) => {
    const x = column >= 3 ? columns[column] + (headings[column].length - value.length) * 6 : columns[column];
    items.push(item(value, x, 650 - index * 36));
  }));
  const header = 'Account Statement\n111111118181\nINDUS PRIVILEGE MAX\nTransaction History\nStatement Period: 19 Oct 2026 - 20 Oct 2026\n';
  return { text: header + records.map(record => record.join('')).join('\n'), pages: [{ items }] };
}

function fixture() {
  const parsed = parseIndusIndMobileStatement(positionedPdf()), transactions = clone(parsed);
  transactions[9].debit = 90;
  const draft = { id: draftId, nature: 'SANKI', account, temporaryFile: oldFile,
    originalName: 'original.pdf', fileHash, createdAt: '2026-10-21T00:00:00Z', createdBy: 'synthetic-owner',
    matchingPolicy: 'balanced_date_amount_v5', transactions, resolutions: {},
    summary: { ...parsed.statementSummary, openingBalance: 8292.70, totalDebits: 8291 } };
  return {
    bankReconciliationDrafts: { [draftId]: draft },
    expenses: { 'EX-SYNTHETIC-KEEP': { amount: 490, paidAmount: 490, payments: [{ id: 'PAY-001', amount: 490, account }] } },
    openingBalances: { [account]: 0.01 },
    transfers: [{ id: 'TR-SYNTHETIC-EXCLUDED', nature: 'PERSONAL', amount: 2000,
      fromAccount: 'Synthetic Source 7883', toAccount: 'Synthetic Destination 0993', accountingExcluded: true,
      accountingExclusion: { by: 'synthetic-owner', at: '2026-09-13T00:00:00Z', reason: 'Preserve Owner decision' } }],
    adjustments: [{ id: 'ADJ-SYNTHETIC-KEEP', account, amount: -5.9 }],
    receipts: [], vendorAdvances: [], vendorOpeningPayables: [], bankTruthMovements: [], reconciliationExpenses: [],
    paytmSettlements: [{ id: 'PTM-OTHER-DRAFT', provisional: true, reconciliationDraft: 'BRD-OTHER' }],
    bankDateOverrides: { KEEP: { reconciliationDraft: 'BRD-OTHER', provisional: true, bankDate: '2026-09-02' } },
    bankReconciliationLinks: { KEEP: { reconciliationDraft: 'BRD-OTHER', provisional: true, recordId: 'BST-SYNTHETIC-KEEP' } },
    bankReconciliationApprovals: {},
    bankStatements: { 'PERSONAL|Synthetic Destination 0993': { reconciledThrough: '2026-09-13',
      imports: [{ id: 'BST-SYNTHETIC-KEEP', finalizedAt: '2026-09-13T00:00:00Z',
        reconciliationRows: [{ id: 'ledger-0', ledger: { id: 'TR-SYNTHETIC-EXCLUDED', date: '2026-09-02', credit: 2000 },
          decision: 'Excluded', reason: 'Preserve Owner decision', resolvedBy: 'synthetic-owner' }] }], transactions: {} } },
    auditLog: [{ action: 'LEDGER_ENTRY_EXCLUDED', subjectId: 'TR-SYNTHETIC-EXCLUDED', note: 'Preserve Owner decision' }]
  };
}

function harness(initial = fixture()) {
  let state = clone(initial), parses = 0;
  const files = new Map([[oldFile, bytes], [newFile, bytes]]), deleted = [], saves = [];
  const context = vm.createContext({
    path, crypto,
    fs: {
      readFileSync(file) { assert.ok(files.has(file), 'only synthetic retained files may be read'); return files.get(file); },
      existsSync: file => files.has(file),
      unlinkSync(file) { assert.ok(files.has(file)); deleted.push(file); files.delete(file); }
    },
    STATEMENT_DRAFT_DIR: 'synthetic-drafts',
    reconciliationFile: (directory, file) => file && file.startsWith(directory + '/') ? file : null,
    num: value => Number.parseFloat(value) || 0,
    roundMoney: value => Math.round((Number.parseFloat(value) || 0) * 100) / 100,
    normalizedNature: value => ['SAMAST', 'PERSONAL'].includes(String(value).toUpperCase()) ? String(value).toUpperCase() : 'SANKI',
    creditCardByAccount: () => null,
    reconciliationAccountsForNature: () => [account],
    async parseBankStatementUpload(file) {
      assert.equal(file, newFile); parses++;
      return parseIndusIndMobileStatement(positionedPdf());
    },
    loadStore: () => clone(state),
    saveStore(store) { state = clone(store); saves.push(clone(store)); },
    draftReconciliation: (store, draft) => ({ draftId: draft.id, rows: draft.transactions.map((bank, index) => ({ id: 'bank-' + index, bank })) }),
    audit() { assert.fail('a rejected reupload must not create an accounting/merge audit event'); }
  });
  vm.runInContext(implementation, context);
  return { context, files, deleted, saves, get: () => clone(state), get parses() { return parses; },
    upload: () => context.createBankReconciliationDraft({ filePath: newFile, originalName: 'reupload.pdf', account, nature: 'SANKI', username: 'synthetic-owner' }) };
}

function freshDraft() {
  const parsed = parseIndusIndMobileStatement(positionedPdf());
  return { id: 'BRD-SYNTHETIC-FRESH', account, nature: 'SANKI', transactions: clone(parsed),
    summary: clone(parsed.statementSummary), resolutions: {}, fileHash,
    temporaryFile: newFile, originalName: 'reupload.pdf', createdAt: '2026-10-23T00:00:00Z' };
}

test('the same-source reupload parses 490 but cannot merge with an unreviewed stale 90 draft', () => {
  const store = fixture(), fresh = freshDraft(), h = harness(store);
  assert.equal(fresh.fileHash, store.bankReconciliationDrafts[draftId].fileHash);
  assert.equal(fresh.transactions[9].debit, 490);
  assert.equal(fresh.transactions[9].bankReference, 'S1234567');
  assert.equal(fresh.transactions[9].balance, 8202.70);
  assert.equal(fresh.summary.openingBalance, 8692.70);
  assert.equal(fresh.summary.totalDebits, 8691);
  assert.equal(fresh.summary.closingBalance, 1.70);
  assert.deepEqual(fresh.transactions.slice(0, 9), store.bankReconciliationDrafts[draftId].transactions.slice(0, 9),
    'all nine other rows are identical; the conflict is only the stale parsed debit');
  store.bankReconciliationDrafts[fresh.id] = fresh;
  const before = JSON.stringify(store);
  assert.equal(h.context.mergeActiveBankReconciliationDrafts(store, account, 'SANKI'), null);
  assert.equal(JSON.stringify(store), before, 'failed merge leaves both candidates and all accounting evidence byte-identical');
});

test('identical correct unreviewed previews still merge without doubling their ten bank rows', () => {
  const store = fixture(), first = freshDraft(), second = clone(first), h = harness(store);
  second.id = 'BRD-SYNTHETIC-SECOND'; second.createdAt = '2026-10-24T00:00:00Z';
  store.bankReconciliationDrafts = { [first.id]: first, [second.id]: second };
  const merged = h.context.mergeActiveBankReconciliationDrafts(store, account, 'SANKI');
  assert.ok(merged); assert.equal(merged.transactions.length, 10);
  assert.equal(merged.summary.totalDebits, 8691); assert.equal(merged.summary.closingBalance, 1.70);
  assert.deepEqual(Object.keys(store.bankReconciliationDrafts), [first.id]);
});

for (const [name, setup] of [
  ['unreviewed stale amounts', () => {}],
  ['saved row decisions and matching exclusions', store => {
    const draft = store.bankReconciliationDrafts[draftId];
    draft.resolutions['bank-0'] = { action: 'exclude', reason: 'Preserve reviewed row', by: 'synthetic-owner' };
    draft.matchingExclusions = { 'bank-1': { reason: 'Different transaction', by: 'synthetic-owner' } };
    draft.decisionAudit = [{ rowId: 'bank-0', previous: null, at: '2026-10-21T01:00:00Z' }];
  }],
  ['pending Owner approval', store => {
    store.bankReconciliationApprovals.PENDING = { id: 'PENDING', draftId, status: 'pending', requestedBy: 'synthetic-reviewer' };
  }],
  ['reviewed statement opening', store => {
    store.bankReconciliationDrafts[draftId].openingResolution = { amount: 8292.70, reason: 'Preserve existing reviewed opening' };
  }],
  ['a linked accounting source', store => {
    store.adjustments.push({ id: 'ADJ-LINKED-KEEP', reconciliationDraft: draftId, bankRowId: 'bank-0', amount: -5.9 });
  }]
]) test('reupload rejection preserves ' + name + ', exclusions, finalized history and expenses', async () => {
  const before = fixture(); setup(before);
  const h = harness(before), result = await h.upload();
  assert.equal(h.parses, 1, 'same file hash must not silently reopen stale parsed amounts');
  assert.equal(result.success, false);
  assert.match(result.error, /could not be combined safely/);
  assert.equal(h.saves.length, 2, 'fresh candidate is created and safely cleaned up, never committed over old evidence');
  const staged = Object.values(h.saves[0].bankReconciliationDrafts).find(draft => draft.id !== draftId);
  assert.ok(staged); assert.equal(staged.transactions[9].debit, 490);
  assert.equal(staged.fileHash, fileHash); assert.equal(staged.summary.parserVersion, 'positioned-cells-v1');
  assert.deepEqual(h.get(), before, 'every existing draft, review, exclusion, balance, posting and audit entry is preserved');
  assert.deepEqual(h.deleted, [newFile], 'cleanup is restricted to the rejected fresh upload');
  assert.equal(h.files.has(oldFile), true, 'original retained PDF is preserved');
  assert.equal(h.get().bankReconciliationDrafts[draftId].transactions[9].debit, 90);
  assert.equal(h.get().expenses['EX-SYNTHETIC-KEEP'].payments[0].amount, 490);
});
