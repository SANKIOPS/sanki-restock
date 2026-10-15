'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { vendorSearchRank, compareVendorSearch } = require('../modules/vendor-ledger-search');
const vendor = (name, particulars = '') => ({ name, entities: ['SANKI'], ledgerRows: [{ particulars, type: 'Expense', reference: 'EX-00112', in: 0, out: 20000 }] });

test('exact UJ vendor outranks alphabetically earlier particulars and fuzzy matches', () => {
  const rows = [vendor('Bikanervala', 'UJ bill'), vendor('Geeta Pujan Bhandar'), vendor('U J'), vendor('UJ House'), vendor('UJ')];
  rows.sort((a, b) => compareVendorSearch(a, b, 'UJ'));
  assert.deepEqual(rows.map(row => row.name), ['UJ', 'U J', 'UJ House', 'Geeta Pujan Bhandar', 'Bikanervala']);
});
test('vendor ranking normalizes case, hidden Unicode and spacing without merging identities', () => {
  assert.equal(vendorSearchRank(vendor(' U\u200bJ '), 'uj'), 0);
  assert.equal(vendorSearchRank(vendor('U-J'), 'u j'), 1);
  assert.equal(vendorSearchRank(vendor('UJ Collections'), 'uj'), 2);
  assert.equal(vendorSearchRank(vendor('AB UJ'), 'uj'), 4);
});
test('particulars, reference, transaction type, amount and entity search remain available', () => {
  const row = vendor('Different Vendor', 'Flower purchase');
  assert.equal(vendorSearchRank(row, 'flower'), 8);
  assert.equal(vendorSearchRank(row, '00112'), 8);
  assert.equal(vendorSearchRank(row, 'expense'), 8);
  assert.equal(vendorSearchRank(row, '20000'), 8);
  assert.equal(vendorSearchRank(row, 'sanki'), 7);
  assert.equal(vendorSearchRank(row, 'no such content'), Infinity);
});
test('blank vendor search retains alphabetical default ordering', () => {
  const rows = [vendor('Zed'), vendor('Alpha')].sort((a, b) => compareVendorSearch(a, b, ''));
  assert.deepEqual(rows.map(row => row.name), ['Alpha', 'Zed']);
});
