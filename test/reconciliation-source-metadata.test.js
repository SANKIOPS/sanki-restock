'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { recoverReversalSourceMetadata } = require('../modules/reconciliation-source-metadata');

function transactions() {
  return [
    { date: '2026-10-07', debit: 150, credit: 0, balance: 450, description: 'Payment', reference: '627945678901', row: 1 },
    { date: '2026-10-07', debit: 0, credit: 150, balance: 600, description: 'Old split narration', reference: '6279456789', row: 2 },
    { date: '2026-10-07', debit: 150, credit: 0, balance: 450.01, description: 'Successful retry', reference: '627945678902', row: 3 }
  ];
}

function input() {
  const currentTransactions = transactions(), parsedTransactions = transactions();
  parsedTransactions[1] = { ...parsedTransactions[1], description: 'REVERSED: UPI/627945678901/Payment', reference: '627945678901', reversal: true };
  return { currentTransactions, parsedTransactions, accountLast4: '8181', parsedSummary: { validated: true, accountLast4: '8181' } };
}

test('recover only verified reversal metadata at its existing draft index', () => {
  const options = input();
  options.currentTransactions[1].decision = { action: 'link_existing', appId: 'TR-00150' };
  const before = JSON.parse(JSON.stringify(options));
  assert.deepEqual(recoverReversalSourceMetadata(options), [{ index: 1, metadata: { description: 'REVERSED: UPI/627945678901/Payment', reference: '627945678901', reversal: true } }]);
  assert.deepEqual(options, before, 'source rows, money, dates, indexes, and decisions are not mutated');
});

test('reverse-chronological mobile rows are matched to existing indexes by exact money and date', () => {
  const options = input();
  options.parsedTransactions.reverse();
  assert.equal(recoverReversalSourceMetadata(options)[0].index, 1);
  const parsedTransactions = [options.parsedTransactions[1], options.parsedTransactions[2], options.parsedTransactions[0]];
  assert.equal(recoverReversalSourceMetadata({ ...options, parsedTransactions })[0].index, 1);
});

test('a REVERSED marker is accepted without a parser flag; flag-only descriptions are also accepted', () => {
  const marker = input();
  delete marker.parsedTransactions[1].reversal;
  assert.equal(recoverReversalSourceMetadata(marker)[0].metadata.reversal, true);
  const flag = input();
  flag.parsedTransactions[1].description = 'Bank refund of failed payment';
  assert.equal(recoverReversalSourceMetadata(flag)[0].metadata.description, 'Bank refund of failed payment');
});

test('ordinary reparsed descriptions and references cannot overwrite draft metadata', () => {
  const options = input();
  options.parsedTransactions[0].description = 'Changed payee';
  options.parsedTransactions[0].reference = '111111111111';
  const patches = recoverReversalSourceMetadata(options);
  assert.equal(patches.length, 1);
  assert.equal(patches[0].index, 1);
  assert.deepEqual(Object.keys(patches[0].metadata).sort(), ['description', 'reference', 'reversal']);
});

test('already recovered metadata needs no patch', () => {
  const options = input();
  options.currentTransactions[1] = { ...options.parsedTransactions[1] };
  assert.deepEqual(recoverReversalSourceMetadata(options), []);
});

test('fail closed for unvalidated source, unknown or wrong account, or unequal row counts', () => {
  for (const change of [
    options => { options.parsedSummary.validated = false; },
    options => { options.parsedSummary.validated = 'true'; },
    options => { options.parsedSummary.accountLast4 = '3645'; },
    options => { delete options.parsedSummary.accountLast4; },
    options => { delete options.accountLast4; },
    options => { options.accountLast4 = 'Bank 8181'; },
    options => { options.parsedTransactions.pop(); },
    options => { options.currentTransactions.pop(); },
    options => { options.currentTransactions = []; options.parsedTransactions = []; }
  ]) {
    const options = input();
    change(options);
    assert.deepEqual(recoverReversalSourceMetadata(options), []);
  }
});

test('any changed money, balance, or date anywhere in the source rejects all proposed patches', () => {
  for (const field of ['debit', 'credit', 'balance', 'date']) {
    const options = input();
    options.parsedTransactions[0][field] = field === 'date' ? '2026-10-08' : options.parsedTransactions[0][field] + .01;
    assert.deepEqual(recoverReversalSourceMetadata(options), [], field);
  }
});

test('duplicate monetary identity in either source or draft is ambiguous and cannot be recovered', () => {
  for (const field of ['currentTransactions', 'parsedTransactions']) {
    const options = input();
    options[field][2] = { ...options[field][0] };
    assert.deepEqual(recoverReversalSourceMetadata(options), []);
  }
});

test('a reversal needs an exact twelve-digit canonical reference and readable source description', () => {
  for (const reference of ['', '6279456789', '6279456789012', '627945\n678901', 'REF-627945678901']) {
    const options = input();
    options.parsedTransactions[1].reference = reference;
    assert.deepEqual(recoverReversalSourceMetadata(options), [], reference);
  }
  const options = input();
  options.parsedTransactions[1].description = '';
  assert.deepEqual(recoverReversalSourceMetadata(options), []);
});

test('a malformed marked reversal prevents partial recovery of another reversal', () => {
  const options = input();
  options.parsedTransactions[2] = { ...options.parsedTransactions[2], reversal: true, reference: 'BAD' };
  assert.deepEqual(recoverReversalSourceMetadata(options), []);
});

test('missing or unsafe monetary values and impossible dates fail closed', () => {
  for (const change of [
    row => { delete row.balance; }, row => { row.balance = null; },
    row => { row.debit = NaN; }, row => { row.credit = Infinity; },
    row => { row.debit = -.01; }, row => { row.debit = 150.001; },
    row => { row.date = '2026-02-30'; }, row => { row.date = '07/10/2026'; }
  ]) {
    const options = input();
    change(options.parsedTransactions[0]);
    assert.deepEqual(recoverReversalSourceMetadata(options), []);
  }
});

test('equivalent decimal strings and parser floating-point cents preserve the exact row identity', () => {
  const options = input();
  options.parsedTransactions[0].debit = '150.00';
  options.parsedTransactions[1].credit = '150.00';
  options.parsedTransactions[2].balance = 450.01000000000005;
  assert.equal(recoverReversalSourceMetadata(options).length, 1);
});

test('the parsed array statementSummary is usable when summary is not separately supplied', () => {
  const options = input();
  options.parsedTransactions.statementSummary = options.parsedSummary;
  delete options.parsedSummary;
  assert.equal(recoverReversalSourceMetadata(options).length, 1);
  assert.deepEqual(recoverReversalSourceMetadata(null), []);
});
