'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');

test('finalized ledger status retains chronological statement order', () => {
  assert.match(source, /chronological\.forEach\(\(row,statementOrder\)/);
  assert.match(source, /reconciliationId:record\.id,bankDate:[\s\S]*statementOrder/);
});

test('running balances and visible rows prefer bank order over internal IDs', () => {
  assert.match(source, /movementEntries\.sort\(\(a,b\) => String\(a\.date\)[\s\S]*statementOrder\(a,b\)[\s\S]*String\(a\.id\)/);
  assert.match(source, /String\(b\.date\)[\s\S]*-statementOrder\(a,b\)[\s\S]*String\(b\.id\)/);
});

test('newest-first statements are normalized before assigning sequence', () => {
  assert.match(source, /first>last\?rows\.slice\(\)\.reverse\(\):rows/);
});
