const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');

test('reconciliation refreshes retain stable transaction anchors', () => {
  assert.match(html, /bankMarkReviewRows\(d\)/);
  assert.match(html, /data-bank-reconciliation-row/);
  assert.match(html, /same\.getBoundingClientRect\(\)\.top-anchor\.top/);
});

test('a saved resolution selects the next outstanding transaction', () => {
  const start = html.indexOf('    function bankRowNeedsReview(');
  const end = html.indexOf('    function bankRestoreReviewPosition(', start);
  const context = {};
  vm.createContext(context);
  vm.runInContext(html.slice(start, end), context);
  const previous = { draftId: 'D1', rows: [
    { id: 'one', status: 'missing_in_app' },
    { id: 'two', status: 'missing_in_app' },
    { id: 'three', status: 'possible_match' }
  ] };
  const current = { draftId: 'D1', rows: [
    { id: 'one', status: 'resolved' },
    { id: 'two', status: 'missing_in_app' },
    { id: 'three', status: 'possible_match' }
  ] };
  assert.equal(context.bankNextReviewAfterUpdate(previous, current), 'two');
});

test('bulk resolutions skip every newly completed transaction', () => {
  const start = html.indexOf('    function bankRowNeedsReview(');
  const end = html.indexOf('    function bankRestoreReviewPosition(', start);
  const context = {};
  vm.createContext(context);
  vm.runInContext(html.slice(start, end), context);
  const previous = { draftId: 'D1', rows: [
    { id: 'one', status: 'missing_in_app' },
    { id: 'two', status: 'missing_in_app' },
    { id: 'three', status: 'amount_mismatch' }
  ] };
  const current = { draftId: 'D1', rows: [
    { id: 'one', status: 'resolved' },
    { id: 'two', status: 'resolved' },
    { id: 'three', status: 'amount_mismatch' }
  ] };
  assert.equal(context.bankNextReviewAfterUpdate(previous, current), 'three');
});
