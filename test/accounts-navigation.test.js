const test = require('node:test');
const assert = require('node:assert/strict');

const { visibleFor } = require('../modules/module-registry');

test('Accounts navigation exposes only Ledgers and Salary', () => {
  const modules = visibleFor({ username: 'owner', role: 'owner', roles: ['owner'] });
  const accounts = modules.filter((module) => module.section === 'Accounts');
  assert.deepEqual(accounts.map((module) => module.title), ['Ledgers', 'Salary']);
  assert.deepEqual(accounts.map((module) => module.href), ['/expenses.html', '/salary.html']);
});
