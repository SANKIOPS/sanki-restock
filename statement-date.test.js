const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, 'modules/expenses.js'), 'utf8');
const start = source.indexOf('function statementDate(');
const end = source.indexOf('function statementNum(', start);
const statementDate = vm.runInNewContext(source.slice(start, end) + '\nstatementDate;');

test('ISO statement dates do not get reversed into historical years', () => {
  for (const day of ['01', '02', '04', '08', '09', '26', '31']) {
    assert.equal(statementDate(`2026-08-${day}`), `2026-08-${day}`);
  }
  assert.equal(statementDate('2026-09-01'), '2026-09-01');
  assert.equal(statementDate('2026-09-01T12:30:00Z'), '2026-09-01');
  assert.equal(statementDate('2026/9/1'), '2026-09-01');
});

test('Indian DMY dates retain their day and month', () => {
  assert.equal(statementDate('01/09/2026'), '2026-09-01');
  assert.equal(statementDate('28.08.2026'), '2026-08-28');
  assert.equal(statementDate('1-9-26'), '2026-09-01');
});

test('invalid dates and embedded numeric fragments are rejected', () => {
  for (const value of ['2026-02-30', '31/09/2026', '2026-13-01', 'not a date', 'REF2026-09-01', '2026-09-011']) {
    assert.equal(statementDate(value), '', value);
  }
});
