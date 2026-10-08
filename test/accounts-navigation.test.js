const test = require('node:test');
const assert = require('node:assert/strict');

const { visibleFor } = require('../modules/module-registry');

test('Owner Accounts navigation exposes P&L and preserves incentives below Salary', () => {
  const modules = visibleFor({ username: 'owner', role: 'owner', roles: ['owner'] });
  const accounts = modules.filter((module) => module.section === 'Accounts');
  assert.deepEqual(accounts.map((module) => module.title), ['P&L', 'Rental income', 'Ledgers', 'Salary', 'Sales incentives']);
  assert.deepEqual(accounts.map((module) => module.href), ['/pnl.html', '/rentals.html', '/expenses.html', '/salary.html', '/incentives.html']);
});

test('Prashant sees Salary without granting every claimant salary access',()=>{
  assert.ok(visibleFor({username:'prashant',role:'claimant',roles:['claimant']}).some(m=>m.href==='/salary.html'));
  assert.equal(visibleFor({username:'another-claimant',role:'claimant',roles:['claimant']}).some(m=>m.href==='/salary.html'),false);
});

test('Prashant sees Sales incentives without granting every claimant access',()=>{
  assert.ok(visibleFor({username:'prashant',role:'claimant',roles:['claimant']}).some(m=>m.href==='/incentives.html'));
  assert.equal(visibleFor({username:'another-claimant',role:'claimant',roles:['claimant']}).some(m=>m.href==='/incentives.html'),false);
});
