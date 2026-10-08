'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const tempDir = fs.mkdtempSync(path.join(__dirname, '.tmp-incentive-accounting-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
fs.writeFileSync(path.join(tempDir, 'expenses.json'), '{}');

const { router } = require('../modules/expenses');

function invoke(method, routePath, query = {}) {
  const layer = router.stack.find(item => item.route && item.route.path === routePath && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, `route exists: ${method} ${routePath}`);
  const req = {
    body: {}, params: {}, query,
    headers: { 'user-agent': 'SANKI incentive integration test', 'x-forwarded-for': '203.0.113.20' },
    get(name) { return this.headers[String(name).toLowerCase()] || ''; },
    ip: '203.0.113.20',
    user: { username: 'owner', role: 'owner', roles: ['owner'] }
  };
  let status = 200, body;
  const res = {
    status(code) { status = code; return this; },
    json(value) { body = value; return this; },
    end() { return this; },
    sendFile() { return this; }
  };
  layer.route.stack[0].handle(req, res, error => { throw error; });
  return { status, body };
}

test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

test('recorded incentive payments debit the chosen account and appear for reconciliation', () => {
  const account = 'Counter Cash';
  const before = invoke('GET', '/api/expenses/balances', { nature: 'SANKI' });
  assert.equal(before.status, 200);
  const opening = before.body.accounts.find(item => item.name === account).balance;

  fs.writeFileSync(path.join(tempDir, 'incentives.json'), JSON.stringify({
    payments: [{
      id: 'INCP-00001', salesperson: 'Shivam', amount: 240, date: '2026-10-08',
      account, reference: 'INCENTIVE-TEST', proofs: ['/proof.jpg'], note: 'Approved weekly incentive',
      createdAt: '2026-10-08T10:00:00.000Z', createdBy: 'owner', active: true
    }]
  }));

  const after = invoke('GET', '/api/expenses/balances', { nature: 'SANKI' });
  assert.equal(after.body.accounts.find(item => item.name === account).balance, opening - 240);

  const ledger = invoke('GET', '/api/expenses/account-ledger', { nature: 'SANKI', account });
  const entry = ledger.body.entries.find(item => item.id === 'INCP-00001');
  assert.equal(entry.kind, 'incentive_payment');
  assert.equal(entry.debit, 240);
  assert.match(entry.description, /Shivam/);
});
