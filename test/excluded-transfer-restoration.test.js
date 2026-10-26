'use strict';

// Pure, synthetic fixtures only: no production snapshots, statements or IDs.
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const restoration = require('../modules/excluded-transfer-restoration');
const clone = value => JSON.parse(JSON.stringify(value));
const date = '2098-04-17';
const amount = 1750.25;
const reference = '900000005321';
const fromAccount = 'Namita 5464';
const toAccount = 'ICICI Bank 0992';
const transferId = 'TR-SYNTHETIC-RESTORE';
const draftId = 'BRD-SYNTHETIC-RESTORE';
const intent = { draftId, rowId: 'bank-0', transferId };
const actor = { user: 'synthetic-owner', reason: 'Owner confirms the original exclusion was accidental; restore this exact verified transfer.', at: '2098-04-20T12:00:00.000Z' };
const cents = value => Math.round(Number(value || 0) * 100);
const bankReference = bank => String(bank?.reference || '');
const bankSignature = bank => crypto.createHash('sha256').update(JSON.stringify([bank?.date, cents(bank?.debit), cents(bank?.credit), bankReference(bank), cents(bank?.balance), bank?.description || ''])).digest('hex');
const bookKey = (nature, account) => nature === 'PERSONAL' ? 'PERSONAL|' + account : account;
function candidate(fields) {
  if ((fields.fromNature || fields.nature) !== 'PERSONAL' || (fields.toNature || fields.nature) !== 'PERSONAL' || fields.classification !== 'internal_transfer' || !fields.fromAccount || !fields.toAccount || fields.fromAccount === fields.toAccount || !(Number(fields.amount) > 0)) return null;
  return { fromNature: 'PERSONAL', toNature: 'PERSONAL', fromAccount: fields.fromAccount, toAccount: fields.toAccount, date: fields.date, amount: Number(fields.amount), key: [fields.fromAccount.toLowerCase(), fields.toAccount.toLowerCase(), fields.date, Number(fields.amount).toFixed(2)].join('|') };
}
const helpers = {
  bankReference, bankSignature, bookKey,
  movements(state, account) { return (state.syntheticMovements || []).filter(row => row.account === account); },
  postingNature(draft, rowId) { return draft.transactionOwnership?.[rowId]?.nature || draft.nature; },
  transferCandidate(state, transfer) {
    const result = candidate(transfer);
    if (!result) return null;
    const evidence = [], invalidAccounts = [];
    for (const [key, book] of Object.entries(state.bankStatements || {})) {
      const account = key.replace(/^PERSONAL\|/, '');
      if (![result.fromAccount, result.toAccount].includes(account)) continue;
      const direction = account === result.fromAccount ? 'debit' : 'credit';
      for (const record of book.imports || []) for (const row of [...(record.reconciliationRows || []), ...(record.carriedReconciliationRows || [])]) {
        if (!record.finalizedAt || !(row.linkedRecordIds || []).includes(transfer.id)) continue;
        const stored = book.transactions?.[row.bank?.transactionId];
        if (!stored || bankSignature(stored) !== bankSignature(row.bank) || stored.date !== result.date || cents(stored[direction]) !== cents(result.amount) || Number(stored[direction === 'debit' ? 'credit' : 'debit']) !== 0 || stored.reversal) { invalidAccounts.push(account); continue; }
        const value = { account, direction, date: stored.date, reference: bankReference(stored), signature: bankSignature(stored), identity: stored.id };
        if (!evidence.some(item => item.account === account && item.identity === value.identity)) evidence.push(value);
      }
    }
    return { ...result, physicalEvidence: { evidence, invalidAccounts: [...new Set(invalidAccounts)], savedSources: [] } };
  },
  resolutionCandidate(draft, rowId, resolution) {
    if (resolution.action !== 'create_internal_transfer') return null;
    const bank = draft.transactions?.[Number(rowId.replace('bank-', ''))];
    if (!bank) return null;
    const incoming = Number(bank.credit) > 0;
    const value = candidate({ fromNature: 'PERSONAL', toNature: 'PERSONAL', fromAccount: incoming ? resolution.otherAccount : draft.account, toAccount: incoming ? draft.account : resolution.otherAccount, date: bank.date, amount: resolution.principalAmount || Number(bank.debit) + Number(bank.credit), classification: resolution.classification || 'internal_transfer' });
    return value && { ...value, draftId: draft.id, rowId, bankEvidence: { account: draft.account, direction: incoming ? 'credit' : 'debit', date: bank.date, reference: bankReference(bank), signature: bankSignature(bank) } };
  },
  protectionError(state) { return state.syntheticProtectionError || ''; }
};
function fixture() {
  const transfer = { id: transferId, nature: 'PERSONAL', fromNature: 'PERSONAL', toNature: 'PERSONAL', fromAccount, toAccount, classification: 'internal_transfer', date, amount, bankStatementEvidence: true, bankReference: '9000000000', reconciliationDraft: 'BRD-SYNTHETIC-ORIGIN', bankRowId: 'bank-0', proof: '/synthetic-proof.jpg', note: 'Original saved narration must remain unchanged', createdBy: 'synthetic-owner', createdAt: '2098-04-18T09:00:00.000Z', accountingExcluded: true, accountingExclusion: { reason: 'Synthetic mistaken exclusion', by: 'synthetic-owner', at: '2098-04-19T09:00:00.000Z' } };
  const debit = { date, debit: amount, credit: 0, reference, description: 'Synthetic verified debit to the receiving personal account', balance: 8249.75 };
  const credit = { id: 'BTX-SYNTHETIC-RECEIVING', date, debit: 0, credit: amount, reference, description: 'Synthetic verified receiving-side bank credit', balance: 4750.25 };
  const historyRow = { id: 'bank-0', bank: { ...credit, transactionId: credit.id }, ledger: { id: transferId, date, debit: 0, credit: amount, reference }, linkedRecordIds: [transferId], decision: 'Bank-confirmed internal transfer', reason: 'Original receiving-side review', correctionHistory: [{ reason: 'Earlier review retained', by: 'synthetic-owner' }] };
  const state = {
    transfers: [transfer, { id: 'TR-SYNTHETIC-PRESERVED', nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: 'Axis Bank 3448', toAccount: 'Counter Cash', amount: 77, date: '2098-01-01', classification: 'internal_transfer' }], transferSeq: 101, adjSeq: 17,
    expenses: { 'EX-SYNTHETIC-PRESERVED': { id: 'EX-SYNTHETIC-PRESERVED', nature: 'SANKI', amount: 12, date: '2098-01-01', payments: [{ id: 'PAY-PRESERVED', amount: 12, account: 'Counter Cash' }] } },
    receipts: [{ id: 'REC-SYNTHETIC-PRESERVED', amount: 23, account: 'Counter Cash' }], adjustments: [], reconciliationExpenses: [], bankTruthMovements: [], vendorAdvances: [], salesRefunds: [],
    openingBalances: { 'Counter Cash': 100 }, openingBalancesByNature: { PERSONAL: { [fromAccount]: 10000, [toAccount]: 3000 } }, physicalBankOpeningBalances: {},
    bankStatements: { [bookKey('PERSONAL', toAccount)]: { transactions: { [credit.id]: credit }, imports: [{ id: 'BST-SYNTHETIC-ORIGIN', draftId: transfer.reconciliationDraft, from: date, to: date, finalizedAt: '2098-04-18T10:00:00.000Z', finalizedBy: 'synthetic-owner', reconciliationRows: [historyRow] }], lastReconciliation: { id: 'BST-SYNTHETIC-ORIGIN', to: date, closingBalance: 4750.25 } } },
    bankReconciliationDrafts: { [draftId]: { id: draftId, nature: 'PERSONAL', account: fromAccount, transactions: [debit], resolutions: {}, matchingExclusions: { 'bank-0': { appId: transferId, reason: 'Explicitly unmatched before reviewing accidental exclusion', by: 'synthetic-owner', at: '2098-04-20T09:00:00.000Z' } }, summary: { from: date, to: date, openingBalance: 10000, closingBalance: 8249.75, totalDebits: amount, totalCredits: 0, validated: true }, createdAt: '2098-04-20T09:00:00.000Z', createdBy: 'synthetic-owner', expiresAt: '2099-01-01T00:00:00.000Z' } },
    bankReconciliationApprovals: {}, bankReconciliationLinks: { 'EX-SYNTHETIC-PRESERVED/PAY-PRESERVED': { reconciliationDraft: 'BST-SYNTHETIC-PRESERVED' } }, bankDateOverrides: { 'EX-SYNTHETIC-PRESERVED/PAY-PRESERVED': { bankDate: '2098-01-01', originalDate: '2098-01-01' } },
    auditSeq: 1, auditLog: [{ id: 'AUD-SYNTHETIC-ORIGINAL', action: 'EXISTING_AUDIT', subjectType: 'transfer', subjectId: transferId, note: 'Earlier history must not be rewritten.' }]
  };
  return state;
}
const plan = state => restoration.buildRestorePlan(state, intent, helpers);
const apply = (state, prepared = plan(state), options = {}) => restoration.applyRestorePlan(state, prepared, { ...actor, ...options }, helpers);
function immutableFinancialFields(before, after) {
  for (const key of ['expenses', 'receipts', 'adjustments', 'reconciliationExpenses', 'bankTruthMovements', 'vendorAdvances', 'salesRefunds', 'openingBalances', 'openingBalancesByNature', 'physicalBankOpeningBalances', 'bankStatements', 'bankReconciliationApprovals', 'transferSeq', 'adjSeq']) assert.deepEqual(after[key], before[key], key + ' must not change');
  assert.deepEqual(after.transfers[1], before.transfers[1], 'unrelated transfer must not change');
  for (const key of ['id', 'nature', 'fromNature', 'toNature', 'fromAccount', 'toAccount', 'date', 'amount', 'classification', 'bankReference', 'reconciliationDraft', 'bankRowId', 'proof', 'note', 'createdAt', 'createdBy']) assert.deepEqual(after.transfers[0][key], before.transfers[0][key], 'existing transfer ' + key + ' must not change');
  assert.equal(after.transfers.length, before.transfers.length, 'restoration must never append a replacement transfer');
  assert.deepEqual(after.auditLog.slice(0, before.auditLog.length), before.auditLog, 'prior audit events remain immutable');
}
function rejected(mutator, pattern = /./) {
  const state = fixture(); mutator(state); const before = clone(state);
  assert.throws(() => plan(state), pattern); assert.deepEqual(state, before, 'rejected preview must leave all state untouched');
}

test('restoration preview is read-only and identifies exact existing transfer and both bank sides', () => {
  const state = fixture(), before = clone(state), result = plan(state);
  assert.equal(result.version, 1); assert.ok(result.fingerprint); assert.equal(result.transfer.id, transferId);
  assert.deepEqual(state, before);
});

test('explicit restoration retains the original ID, value and dates with no duplicate or adjustment', () => {
  const state = fixture(), before = clone(state), result = apply(state);
  assert.equal(result.restored, true); assert.equal(result.transferId, transferId);
  assert.equal(state.transfers[0].accountingExcluded, false);
  const resolution = state.bankReconciliationDrafts[draftId].resolutions['bank-0'];
  assert.equal(resolution.action, 'link_existing');
  assert.ok([resolution.appId, ...(resolution.appIds || [])].includes(transferId));
  assert.equal(state.bankReconciliationDrafts[draftId].matchingExclusions['bank-0'], undefined);
  immutableFinancialFields(before, state);
  assert.ok(state.transfers[0].restorationHistory?.length || state.transfers[0].accountingRestorationHistory?.length, 'restoration keeps its own before/after evidence');
});

test('replaying the exact successful restoration is a no-op rather than another financial write', () => {
  const state = fixture(), prepared = plan(state); apply(state, prepared); const before = clone(state);
  const replay = apply(state, prepared); assert.equal(replay.alreadyRestored, true); assert.deepEqual(state, before);
});

for (const [name, mutate] of [
  ['amount', s => { s.transfers[0].amount += 1; }],
  ['date', s => { s.transfers[0].date = '2098-04-18'; }],
  ['exclusion reason', s => { s.transfers[0].accountingExclusion.reason = 'Changed after preview'; }],
  ['bank reference', s => { s.bankReconciliationDrafts[draftId].transactions[0].reference = '900000005322'; }],
  ['retained bank evidence', s => { Object.values(s.bankStatements)[0].transactions['BTX-SYNTHETIC-RECEIVING'].balance += 1; }]
]) test('stale ' + name + ' rejects apply atomically', () => {
  const state = fixture(), prepared = plan(state); mutate(state); const before = clone(state);
  assert.throws(() => apply(state, prepared), /./); assert.deepEqual(state, before);
});

for (const weak of ['', '9000000000', 'USER-TYPED-REFERENCE']) test('current bank identity must be an actual reliable UTR: ' + (weak || 'blank'), () => rejected(s => { s.bankReconciliationDrafts[draftId].transactions[0].reference = weak; }));

for (const [name, mutate] of [
  ['different UTR', s => { const book = Object.values(s.bankStatements)[0]; book.transactions['BTX-SYNTHETIC-RECEIVING'].reference = '900000005399'; book.imports[0].reconciliationRows[0].bank.reference = '900000005399'; }],
  ['missing stored evidence', s => { Object.values(s.bankStatements)[0].transactions = {}; }],
  ['unfinalized evidence', s => { delete Object.values(s.bankStatements)[0].imports[0].finalizedAt; }],
  ['reversed direction', s => { const bank = s.bankReconciliationDrafts[draftId].transactions[0]; bank.credit = bank.debit; bank.debit = 0; }],
  ['principal plus unhandled charge', s => { s.bankReconciliationDrafts[draftId].transactions[0].debit += 5.9; }],
  ['already active record', s => { s.transfers[0].accountingExcluded = false; }],
  ['business classification', s => { s.transfers[0].fromNature = 'SANKI'; s.transfers[0].classification = 'owner_withdrawal'; }],
  ['existing confirmed fee dependency', s => { s.transfers[0].confirmedBankSplit = { principal: amount, charge: 5.9, gross: amount + 5.9, chargeId: 'ADJ-SYNTHETIC-FEE' }; }],
  ['selected row has another saved decision', s => { s.bankReconciliationDrafts[draftId].resolutions['bank-0'] = { action: 'exclude', reason: 'Keep this bank movement excluded' }; }],
  ['locked source statement recovery', s => { s.bankReconciliationDrafts[draftId].originalStatementPeriod = { from: date, to: date }; }],
  ['protected financial dependency', s => { s.syntheticProtectionError = 'Synthetic linked refund/payment dependency must be resolved explicitly.'; }]
]) test('restoration fails closed for ' + name, () => rejected(mutate));

test('restoration supersedes a pending Owner review instead of leaving a deadlock', () => {
  const state = fixture(); state.bankReconciliationApprovals['BRA-SYNTHETIC-PENDING'] = { id: 'BRA-SYNTHETIC-PENDING', draftId, status: 'pending', requestedBy: 'prashant' };
  const prepared = plan(state); assert.deepEqual(prepared.approvalsToSupersede, ['BRA-SYNTHETIC-PENDING']);
  apply(state, prepared);
  assert.equal(state.bankReconciliationApprovals['BRA-SYNTHETIC-PENDING'].status, 'superseded');
  assert.equal(state.bankReconciliationApprovals['BRA-SYNTHETIC-PENDING'].supersededBy, actor.user);
  assert.equal(state.transfers[0].accountingExcluded, false);
});

test('two same-day current-bank UTR occurrences are ambiguous and cannot restore', () => rejected(s => {
  s.bankReconciliationDrafts[draftId].transactions.push({ ...s.bankReconciliationDrafts[draftId].transactions[0], balance: 6499.5 });
}));

test('two opposite-bank occurrences cannot be collapsed just because their UTR matches', () => rejected(s => {
  const book = Object.values(s.bankStatements)[0], other = clone(book.transactions['BTX-SYNTHETIC-RECEIVING']);
  other.id = 'BTX-SYNTHETIC-SECOND-OCCURRENCE'; other.balance += amount; book.transactions[other.id] = other;
}));

test('a finalized opposite-side claim for another ledger record blocks restoration', () => rejected(s => {
  Object.values(s.bankStatements)[0].imports[0].reconciliationRows[0].linkedRecordIds.push('REC-SYNTHETIC-COMPETING');
}));

test('a conflicting carried copy cannot be hidden behind the same physical transaction ID', () => rejected(s => {
  const book = Object.values(s.bankStatements)[0], carried = clone(book.imports[0]); carried.id = 'BST-SYNTHETIC-CONFLICTING-COPY';
  carried.reconciliationRows[0].bank.balance += 1; book.imports.push(carried);
}));

test('a consistent overlapping statement copy retains evidence without adding money', () => {
  const state = fixture(), book = Object.values(state.bankStatements)[0], carried = clone(book.imports[0]); carried.id = 'BST-SYNTHETIC-CONSISTENT-COPY'; book.imports.push(carried);
  const before = clone(state); apply(state); immutableFinancialFields(before, state);
});

test('a modified preview fingerprint cannot authorize restoration', () => {
  const state = fixture(), prepared = plan(state); prepared.fingerprint = 'not-the-reviewed-evidence'; const before = clone(state);
  assert.throws(() => apply(state, prepared), /./); assert.deepEqual(state, before);
});

test('an exact-ID restoration cannot be redirected to another transfer with the old preview', () => {
  const state = fixture(), prepared = plan(state); prepared.intent.transferId = 'TR-SYNTHETIC-PRESERVED'; const before = clone(state);
  assert.throws(() => apply(state, prepared), /./); assert.deepEqual(state, before);
});

test('a competing active transfer cannot coexist with restoring the excluded original', () => rejected(s => {
  s.transfers.push({ ...clone(s.transfers[0]), id: 'TR-SYNTHETIC-COMPETING', accountingExcluded: false });
}));

test('a competing staged create decision cannot post a duplicate after restoration', () => rejected(s => {
  const pending = clone(s.bankReconciliationDrafts[draftId]); pending.id = 'BRD-SYNTHETIC-COMPETING'; pending.resolutions = { 'bank-0': { action: 'create_internal_transfer', otherNature: 'PERSONAL', otherAccount: toAccount, principalAmount: amount, chargeAmount: 0, classification: 'internal_transfer' } };
  s.bankReconciliationDrafts[pending.id] = pending;
}));

for (const account of [fromAccount, toAccount]) test('a standalone bank-truth movement on ' + account + ' is never silently counted twice or rewritten', () => rejected(s => {
  s.bankTruthMovements.push({ id: 'BTR-SYNTHETIC-ALREADY-POSTED', nature: 'PERSONAL', account, date, debit: account === fromAccount ? amount : 0, credit: account === toAccount ? amount : 0, reference, classification: 'excluded_bank_truth' });
}));

test('an unrelated bank-truth movement remains unchanged and does not block restoration', () => {
  const state = fixture(); state.bankTruthMovements.push({ id: 'BTR-SYNTHETIC-UNRELATED', nature: 'PERSONAL', account: fromAccount, date: '2098-01-01', debit: 37, credit: 0, reference: '900000007777' });
  const before = clone(state); apply(state); immutableFinancialFields(before, state);
});

test('only exact ledger-only exclusion decisions are cleared with retained decision audit', () => {
  const state = fixture(), draft = state.bankReconciliationDrafts[draftId];
  draft.resolutions['app-0'] = { action: 'exclude', appId: transferId, excludedEntry: { id: transferId, date, debit: amount, credit: 0 }, reason: 'Synthetic mistaken ledger exclusion', by: 'synthetic-owner' };
  draft.resolutions['app-1'] = { action: 'exclude', appId: 'TR-SYNTHETIC-PRESERVED', reason: 'Unrelated deliberate exclusion', by: 'synthetic-owner' };
  const before = clone(state), prepared = plan(state); apply(state, prepared);
  assert.equal(draft.resolutions['app-0'], undefined);
  assert.deepEqual(draft.resolutions['app-1'], before.bankReconciliationDrafts[draftId].resolutions['app-1']);
  assert.ok(JSON.stringify(draft.decisionAudit || []).includes('Synthetic mistaken ledger exclusion'));
  immutableFinancialFields(before, state);
});

test('a ledger-only exclusion without its original entry snapshot is not silently erased', () => rejected(s => {
  s.bankReconciliationDrafts[draftId].resolutions['app-0'] = { action: 'exclude', appId: transferId, reason: 'Missing original ledger snapshot' };
}));

for (const options of [{ user: '' }, { reason: '' }, { reason: 'yes' }, { at: 'not-a-timestamp' }]) test('restoration requires complete explicit audit fields: ' + JSON.stringify(options), () => {
  const state = fixture(), prepared = plan(state), before = clone(state);
  assert.throws(() => apply(state, prepared, options), /reviewer|reason|audit time/i); assert.deepEqual(state, before);
});

for (const account of [fromAccount, toAccount]) test('a non-transfer active bank movement on ' + account + ' blocks restoring the same UTR', () => rejected(s => {
  s.syntheticMovements = [{ id: account === fromAccount ? 'EX-SYNTHETIC-CLAIM/PAY-001' : 'REC-SYNTHETIC-CLAIM', account, date, reference, debit: account === fromAccount ? amount : 0, credit: account === toAccount ? amount : 0 }];
}));

for (const invalidDate of ['2098-02-30', '2098-13-01', 'not-a-date']) test('invalid calendar dates fail with a controlled conflict: ' + invalidDate, () => {
  const state = fixture(); state.transfers[0].date = invalidDate; const before = clone(state);
  assert.throws(() => plan(state), error => error.status === 409); assert.deepEqual(state, before);
});

for (const field of ['amount', 'selected debit', 'retained credit']) test('fractional-paise ' + field + ' cannot be rounded into a restoration match', () => rejected(s => {
  if (field === 'amount') s.transfers[0].amount += 0.001;
  else if (field === 'selected debit') s.bankReconciliationDrafts[draftId].transactions[0].debit += 0.001;
  else { const book = Object.values(s.bankStatements)[0]; book.transactions['BTX-SYNTHETIC-RECEIVING'].credit += 0.001; book.imports[0].reconciliationRows[0].bank.credit += 0.001; }
}));

for (const key of ['bankAmountAuthorityProposals', 'bankMovementAuthority']) for (const identity of ['same row', 'same physical bank reference']) test(key + ' protects ' + identity + ' without being overwritten', () => rejected(s => {
  s[key] = { 'SYNTHETIC-AUTHORITY': identity === 'same row' ? { id: 'SYNTHETIC-AUTHORITY', draftId, bankRowId: 'bank-0', status: 'pending' } : { id: 'SYNTHETIC-AUTHORITY', account: fromAccount, bankSnapshot: { date, reference, debit: amount, credit: 0 }, status: 'approved' } };
}));

for (const override of [{ bankDate: '2098-04-18' }, { originalDate: '2098-04-18' }, { bankDate: date, bankReference: '900000005999' }]) test('conflicting retained date override fails closed: ' + JSON.stringify(override), () => rejected(s => {
  s.bankDateOverrides[transferId] = override;
}));

test('a matching retained bank-date override is preserved exactly during restoration', () => {
  const state = fixture(); state.bankDateOverrides[transferId] = { bankDate: date, originalDate: date, bankReference: reference, reconciliationDraft: 'BST-SYNTHETIC-ORIGIN', note: 'Preserve original verified date evidence' };
  const before = clone(state); apply(state); immutableFinancialFields(before, state); assert.deepEqual(state.bankDateOverrides, before.bankDateOverrides);
});

for (const [name, mutate] of [
  ['foreign account', draft => { draft.account = 'Axis Bank 3448'; }],
  ['different date', draft => { draft.resolutions['app-0'].excludedEntry.date = '2098-04-18'; }],
  ['different amount', draft => { draft.resolutions['app-0'].excludedEntry.debit += 1; }],
  ['wrong direction', draft => { draft.resolutions['app-0'].excludedEntry.credit = amount; draft.resolutions['app-0'].excludedEntry.debit = 0; }],
  ['business entity', draft => { draft.resolutions['app-0'].excludedEntry.entity = 'SAMAST'; }],
  ['business posting', draft => { draft.nature = 'SAMAST'; }],
  ['foreign source ID', draft => { draft.resolutions['app-0'].excludedEntry.sourceIds = [transferId, 'EX-SYNTHETIC-FOREIGN/PAY-001']; }]
]) test('ledger-only exclusion with ' + name + ' cannot be cleared by restoring another record', () => rejected(state => {
  const old = { id: 'BRD-SYNTHETIC-OLD-EXCLUSION', nature: 'PERSONAL', account: fromAccount, transactions: [], resolutions: { 'app-0': { action: 'exclude', appId: transferId, excludedEntry: { id: transferId, date, debit: amount, credit: 0 }, reason: 'Retained earlier exclusion' } } };
  mutate(old); state.bankReconciliationDrafts[old.id] = old;
}));
