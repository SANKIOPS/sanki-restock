'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const authority = require('../modules/bank-movement-authority');
const account = 'IndusInd Bank 7883';
const originalDate = '2026-10-18', bankDate = '2026-10-19';
const copy = value => JSON.parse(JSON.stringify(value));
function freeze(value) { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; }
function movement(id, amount = 500, date = originalDate, direction = 'debit') {
  return { id, date, debit: direction === 'debit' ? amount : 0, credit: direction === 'credit' ? amount : 0 };
}
function fixture(id = 'ANY-MOVEMENT', amount = 499.5, direction = 'debit', metadata = {}) {
  const row = movement(id, 500, originalDate, direction);
  const record = { movementId: id, account, nature: 'SAMAST', sourceNature: 'PERSONAL',
    original: { debit: row.debit, credit: row.credit, date: row.date },
    after: { debit: direction === 'debit' ? amount : 0, credit: direction === 'credit' ? amount : 0, date: bankDate },
    planId: 'PLAN-' + id, ...metadata };
  return { row, record, store: { bankMovementAuthority: { [authority.key(account, 'SAMAST', id)]: record } } };
}
function add(store, id, before, after, extra = {}) {
  const record = { movementId: id, account, nature: 'SAMAST', sourceNature: 'SAMAST', original: before, after, planId: 'PLAN-' + id, ...extra };
  store.bankMovementAuthority[authority.key(account, 'SAMAST', id)] = record;
  return record;
}

for (const id of [
  'EX-00001/PAY-001', 'EX-00001/REIM-001', 'EX-00001/REIM-001/RECEIVED',
  'RB-20261019120000-TEST', 'VADV-00001', 'TR-00001', 'LOANPAY-00001',
  'RCPT-00001', 'RCV-00001/COL-001', 'ADV-00001', 'SALB-00001-001',
  'INCP-00001', 'PO-00001/PPAY-001', 'PPB-123-TEST', 'SRF-00001',
  'RF-00001/MODE-001', 'PF-00001/REPAY-001', 'PTMR-POST-123',
  'PTM-AUTO-EXAMPLE', 'GKW-PAYMENT/12345', 'VELOCITY-123abc',
  'VELOCITY-123abc/WALLET', 'SHOPIFY/123/NONCASH', 'SALE/LOCAL-1',
  'FUTURE-TRANSACTION-FAMILY'
]) test('verified physical bank cash flow projects without family rejection: ' + id, () => {
  const f = fixture(id), storeBefore = copy(f.store), rows = freeze([f.row]), before = copy(rows);
  freeze(f.store);
  const result = authority.project(f.store, account, 'PERSONAL', rows);
  assert.equal(result.length, 1);
  assert.equal(result[0].id, id);
  assert.equal(result[0].debit, 499.5);
  assert.equal(result[0].credit, 0);
  assert.equal(result[0].date, bankDate);
  assert.equal(result[0].bankMovementAuthority.sourceChanged, false);
  assert.equal(result.bankMovementAuthorityConflicts.length, 0);
  assert.deepEqual(f.store, storeBefore);
  assert.deepEqual(rows, before);
  assert.equal(authority.delta(f.store, account, 'SAMAST'), .5);
  assert.equal(authority.delta(f.store, account, 'PERSONAL', { sourceRows: rows }), .5);
});

test('physical keys ignore shared entity scope but isolate accounts and movement identity', () => {
  assert.equal(authority.key(account, 'SAMAST', 'ONE'), authority.key('  INDUSIND   BANK 7883 ', 'PERSONAL', 'ONE'));
  assert.notEqual(authority.key(account, 'SAMAST', 'ONE'), authority.key('Axis Bank 3448', 'SAMAST', 'ONE'));
  assert.notEqual(authority.key(account, 'SAMAST', 'ONE'), authority.key(account, 'SAMAST', 'TWO'));
  const f = fixture();
  assert.deepEqual(authority.project(f.store, 'Axis Bank 3448', 'SAMAST', [f.row]), [f.row]);
  assert.equal(authority.delta(f.store, 'Axis Bank 3448', 'SAMAST'), 0);
});

test('incoming cash and large manual amount differences retain exact positive direction and paise', () => {
  const f = fixture('ANY-CREDIT', 1234567.89, 'credit');
  const projected = authority.project(f.store, account, 'SAMAST', [f.row]);
  assert.equal(projected[0].credit, 1234567.89);
  assert.equal(projected[0].debit, 0);
  assert.equal(authority.delta(f.store, account, 'SAMAST'), 1234067.89);
});

test('date-window deltas remove the old movement and add the bank amount on the actual bank day', () => {
  const f = fixture();
  for (const options of [{}, { sourceRows: [f.row] }]) {
    assert.equal(authority.delta(f.store, account, 'SAMAST', { ...options, from: originalDate, to: originalDate }), 500);
    assert.equal(authority.delta(f.store, account, 'SAMAST', { ...options, from: bankDate, to: bankDate }), -499.5);
    assert.equal(authority.delta(f.store, account, 'SAMAST', { ...options, to: '2026-10-17' }), 0);
    assert.equal(authority.delta(f.store, account, 'SAMAST', { ...options, from: '2026-10-20' }), 0);
    assert.equal(authority.delta(f.store, account, 'SAMAST', options), .5);
  }
});

test('source amount/date drift retains finalized bank truth and produces an explicit minimal conflict', () => {
  const f = fixture(), changed = { ...f.row, debit: 901.23, date: '2026-10-20' };
  const result = authority.project(f.store, account, 'SAMAST', [changed]);
  assert.equal(result[0].debit, 499.5);
  assert.equal(result[0].date, bankDate);
  assert.equal(result[0].bankMovementAuthority.sourceChanged, true);
  assert.equal(result.bankMovementAuthorityConflicts[0].type, 'bank_movement_authority_source_changed');
  assert.equal(authority.delta(f.store, account, 'SAMAST', { sourceRows: [changed] }), 401.73);
  assert.equal(authority.delta(f.store, account, 'SAMAST', { sourceRows: [changed], from: bankDate, to: bankDate }), -499.5);
});

test('projecting an already projected canonical row is idempotent and cannot compound its delta', () => {
  const f = fixture(), once = authority.project(f.store, account, 'SAMAST', [f.row]);
  const twice = authority.project(f.store, account, 'PERSONAL', once);
  assert.deepEqual(twice, once);
  assert.equal(twice.bankMovementAuthorityConflicts.length, 0);
  assert.equal(authority.delta(f.store, account, 'SAMAST', { sourceRows: once }), 0);
});

test('a stale projection marker cannot conceal a later changed amount', () => {
  const f = fixture(), once = authority.project(f.store, account, 'SAMAST', [f.row]);
  once[0].debit = 900;
  const next = authority.project(f.store, account, 'SAMAST', once);
  assert.equal(next[0].debit, 499.5);
  assert.equal(next[0].bankMovementAuthority.sourceChanged, true);
  assert.equal(next.bankMovementAuthorityConflicts[0].type, 'bank_movement_authority_source_changed');
});

test('an existing bank-date override is not mistaken for a changed original source date', () => {
  const f = fixture(), row = { ...f.row, date: bankDate, originalDate,
    bankDateOverride: { bankDate, originalDate, provisional: false, reconciliationDraft: 'FINALIZED' } };
  const projected = authority.project(f.store, account, 'SAMAST', [row]);
  assert.equal(projected[0].debit, 499.5);
  assert.equal(projected[0].date, bankDate);
  assert.equal(projected[0].bankMovementAuthority.sourceChanged, false);
  assert.equal(projected.bankMovementAuthorityConflicts.length, 0);
  assert.deepEqual(authority.project(f.store, account, 'SAMAST', projected), projected);
  assert.equal(authority.delta(f.store, account, 'SAMAST', { sourceRows: [row], from: bankDate, to: bankDate }), .5);
});

test('a canonical aggregate override replaces only its known child allocations exactly once', () => {
  const f = fixture('RB-ONE', 700.25, 'debit', { sourceIds: ['EX-A/REIM-001', 'EX-B/REIM-001'], description: 'Reimbursement batch', reference: 'BANK-REF' });
  const rows = [movement('UNRELATED', 22), movement('EX-A/REIM-001', 200), movement('EX-B/REIM-001', 300)];
  const result = authority.project(f.store, account, 'SAMAST', rows);
  assert.equal(result.length, 2);
  assert.equal(result[0].id, 'UNRELATED');
  const batch = result.find(row => row.id === 'RB-ONE');
  assert.equal(batch.debit, 700.25);
  assert.deepEqual(batch.sourceIds, ['EX-A/REIM-001', 'EX-B/REIM-001']);
  assert.equal(batch.reference, 'BANK-REF');
  assert.equal(result.some(row => row.id === 'EX-A/REIM-001' || row.id === 'EX-B/REIM-001'), false);
  assert.equal(result.bankMovementAuthorityConflicts.length, 0);
  assert.equal(authority.delta(f.store, account, 'SAMAST', { sourceRows: rows }), -200.25);
  assert.deepEqual(authority.project(f.store, account, 'SAMAST', result), result);
});

test('linkedEntryIds and linkedEntries identifiers support the same canonical aggregate mapping', () => {
  for (const metadata of [{ linkedEntryIds: ['A', 'B'] }, { linkedEntries: [{ id: 'A' }, { id: 'B' }] }]) {
    const f = fixture('PPB-ONE', 499.5, 'debit', metadata);
    const projected = authority.project(f.store, account, 'SAMAST', [movement('A', 200), movement('B', 300)]);
    assert.equal(projected.length, 1);
    assert.equal(projected[0].id, 'PPB-ONE');
    assert.equal(projected[0].debit, 499.5);
    assert.deepEqual(projected[0].sourceIds, ['A', 'B']);
  }
});

test('an aggregate and its separately represented known members do not count as multiple cash movements', () => {
  const f = fixture('BATCH', 499.5, 'debit', { sourceIds: ['A', 'B'] });
  const rows = [{ ...movement('BATCH'), sourceIds: ['A', 'B'] }, movement('A', 200), movement('B', 300), movement('UNRELATED', 50)];
  const result = authority.project(f.store, account, 'SAMAST', rows);
  assert.equal(result.length, 2);
  assert.deepEqual(result.map(row => row.id), ['BATCH', 'UNRELATED']);
  assert.equal(result[0].debit, 499.5);
  assert.deepEqual(result[0].sourceIds, ['A', 'B']);
  assert.equal(authority.delta(f.store, account, 'SAMAST', { sourceRows: rows }), 500.5);
});

test('changed aggregate membership retains only finalized members and never swallows newly attached cash', () => {
  const f = fixture('BATCH', 499.5, 'debit', { sourceIds: ['A', 'B'] });
  const rows = [{ ...movement('BATCH'), sourceIds: ['A', 'C'], linkedEntryIds: ['A', 'C'], linkedEntries: [{ id: 'A' }, { id: 'C' }] },
    movement('A', 200), movement('B', 300), movement('C', 450)];
  const result = authority.project(f.store, account, 'SAMAST', rows);
  assert.deepEqual(result.map(row => row.id), ['BATCH', 'C']);
  assert.equal(result[0].debit, 499.5);
  assert.deepEqual(result[0].sourceIds, ['A', 'B']);
  assert.deepEqual(result[0].linkedEntryIds, ['A', 'B']);
  assert.deepEqual(result[0].linkedEntries, [{ id: 'A' }, { id: 'B' }]);
  assert.equal(result[0].bankMovementAuthority.sourceChanged, true);
  assert.equal(result[1].debit, 450);
  assert.deepEqual(authority.project(f.store, account, 'SAMAST', result), result);
});

test('a newly attached child cannot become part of finalized single-source evidence', () => {
  const f = fixture('SINGLE');
  const result = authority.project(f.store, account, 'SAMAST', [{ ...f.row, sourceIds: ['NEW'] }, movement('NEW', 50)]);
  assert.deepEqual(result.map(row => row.id), ['SINGLE', 'NEW']);
  assert.deepEqual(result[0].sourceIds, []);
  assert.equal(result[0].bankMovementAuthority.sourceChanged, true);
  assert.equal(result[1].debit, 50);
});

test('child corrections inside a rendered aggregate are summed exactly once without exposing snapshots', () => {
  const store = { bankMovementAuthority: {} };
  add(store, 'A', { debit: 200, credit: 0, date: originalDate }, { debit: 199.5, credit: 0, date: bankDate });
  add(store, 'B', { debit: 300, credit: 0, date: originalDate }, { debit: 300.25, credit: 0, date: bankDate });
  const rows = [{ id: 'BATCH', date: originalDate, debit: 500, credit: 0, sourceIds: ['A', 'B'], linkedEntries: [movement('A', 200), movement('B', 300)] }];
  const projected = authority.project(store, account, 'SAMAST', freeze(rows));
  assert.equal(projected.length, 1);
  assert.equal(projected[0].debit, 499.75);
  assert.equal(projected[0].date, bankDate);
  assert.deepEqual(projected[0].linkedEntries, [{ id: 'A' }, { id: 'B' }]);
  assert.equal(authority.delta(store, account, 'SAMAST', { sourceRows: rows }), .25);
  assert.deepEqual(authority.project(store, account, 'SAMAST', projected), projected);
});

test('a partial aggregate date correction leaves unrelated members grouped and balances each day exactly', () => {
  const store = { bankMovementAuthority: {} };
  add(store, 'A', { debit: 200, credit: 0, date: originalDate }, { debit: 199.5, credit: 0, date: bankDate });
  const rows = [{ id: 'BATCH', date: originalDate, debit: 500, credit: 0, linkedEntryIds: ['A', 'B'] }];
  const projected = authority.project(store, account, 'SAMAST', rows);
  assert.equal(projected[0].debit, 499.5);
  assert.equal(projected[0].date, originalDate);
  assert.equal(projected.bankMovementAuthorityConflicts[0].type, 'bank_movement_aggregate_date_spread');
  assert.equal(authority.delta(store, account, 'SAMAST', { sourceRows: rows, from: originalDate, to: originalDate }), 200);
  assert.equal(authority.delta(store, account, 'SAMAST', { sourceRows: rows, from: bankDate, to: bankDate }), -199.5);
  assert.equal(authority.delta(store, account, 'SAMAST', { sourceRows: rows }), .5);
});

test('overlapping parent and child authority never applies or counts the child delta twice', () => {
  const f = fixture('BATCH', 499.5, 'debit', { sourceIds: ['A', 'B'] });
  add(f.store, 'A', { debit: 200, credit: 0, date: originalDate }, { debit: 150, credit: 0, date: bankDate });
  const rows = [{ id: 'BATCH', date: originalDate, debit: 500, credit: 0, sourceIds: ['A', 'B'] }];
  const projected = authority.project(f.store, account, 'SAMAST', rows);
  assert.equal(projected[0].debit, 499.5);
  assert.equal(projected.length, 1);
  assert.ok(projected.bankMovementAuthorityConflicts.some(conflict => conflict.type === 'overlapping_bank_movement_authority'));
  assert.equal(authority.delta(f.store, account, 'SAMAST', { sourceRows: rows }), .5);
  assert.equal(authority.delta(f.store, account, 'SAMAST'), .5);
});

test('a deleted source retains one bank-confirmed row and warns instead of dropping finalized cash flow', () => {
  const f = fixture('DELETED-SOURCE');
  const result = authority.project(f.store, account, 'SAMAST', [movement('OTHER', 12)]);
  assert.equal(result.length, 2);
  const bank = result.find(row => row.id === f.row.id);
  assert.equal(bank.debit, 499.5);
  assert.equal(bank.bankSourceMissing, true);
  assert.equal(bank.bankMovementAuthority.sourceChanged, true);
  assert.equal(authority.delta(f.store, account, 'SAMAST', { sourceRows: [movement('OTHER', 12)] }), -499.5);
  assert.deepEqual(authority.project(f.store, account, 'SAMAST', result), result);
});

test('partially missing aggregate members remain identifiable without duplicating surviving members', () => {
  const f = fixture('BATCH', 499.5, 'debit', { sourceIds: ['A', 'B'] });
  const result = authority.project(f.store, account, 'SAMAST', [movement('A', 200), movement('UNRELATED', 33)]);
  assert.equal(result.length, 2);
  assert.equal(result.find(row => row.id === 'BATCH').debit, 499.5);
  assert.deepEqual(result.find(row => row.id === 'BATCH').sourceIds, ['A', 'B']);
  assert.equal(result.some(row => row.id === 'A'), false);
  assert.equal(result.find(row => row.id === 'BATCH').bankMovementAuthority.sourceChanged, true);
  assert.equal(result.find(row => row.id === 'UNRELATED').debit, 33);
});

test('duplicate identities cannot be silently consumed by an aggregate authority', () => {
  const f = fixture('BATCH', 499.5, 'debit', { sourceIds: ['A', 'B'] });
  const rows = [movement('A', 200), movement('A', 200), movement('B', 300)];
  const result = authority.project(f.store, account, 'SAMAST', rows);
  assert.deepEqual(result, rows);
  assert.equal(result.bankMovementAuthorityConflicts[0].type, 'ambiguous_bank_movement_members');
  assert.equal(authority.delta(f.store, account, 'SAMAST', { sourceRows: rows }), 0);
});

test('public projection includes no private invoice, provider report or approval snapshots from metadata', () => {
  const f = fixture('PRIVATE', 499.5, 'debit', { invoiceTotal: 123456.78, sourceFingerprint: 'PRIVATE-FINGERPRINT',
    before: { privateInvoice: 123456.78 }, afterSnapshot: { secret: 'provider source' }, approvedBy: 'PRIVATE-OWNER' });
  const result = authority.project(f.store, account, 'SAMAST', [f.row]), serialized = JSON.stringify(result);
  for (const secret of ['123456.78', 'PRIVATE-FINGERPRINT', 'provider source', 'PRIVATE-OWNER', 'invoiceTotal', 'before', 'afterSnapshot']) assert.equal(serialized.includes(secret), false, secret);
  assert.equal(Object.keys(result).includes('bankMovementAuthorityConflicts'), false);
  assert.deepEqual(Object.keys(result[0].bankMovementAuthority).sort(), ['movementId', 'movementIds', 'planId', 'planIds', 'sourceChanged'].sort());
});

test('malformed or direction-reversing persisted authority produces a conflict without inventing money', () => {
  for (const after of [{ debit: -1, credit: 0, date: bankDate }, { debit: 0, credit: 500, date: bankDate },
    { debit: 500.001, credit: 0, date: bankDate }, { debit: 500, credit: 0, date: '2026-02-30' }]) {
    const f = fixture('INVALID', 499.5, 'debit', { after });
    const result = authority.project(f.store, account, 'SAMAST', [f.row]);
    assert.deepEqual(result, [f.row]);
    assert.equal(result.bankMovementAuthorityConflicts[0].type, 'invalid_bank_movement_authority');
    assert.equal(authority.delta(f.store, account, 'SAMAST'), 0);
  }
});

test('unknown stores and empty account rows return independent empty projections', () => {
  assert.deepEqual(authority.project({}, account, 'SAMAST', []), []);
  assert.equal(authority.delta({}, account, 'SAMAST'), 0);
  const rows = [movement('UNCHANGED')];
  const result = authority.project({}, account, 'SAMAST', rows);
  assert.notEqual(result, rows);
  assert.notEqual(result[0], rows[0]);
  assert.deepEqual(result, rows);
});
