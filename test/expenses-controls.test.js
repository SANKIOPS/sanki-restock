'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-expenses-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const { router } = require('../modules/expenses');

test.after(() => {
  fs.rmSync(tempDir, { recursive: true, force: true });
});

function invoke(method, routePath, { body = {}, params = {}, query = {}, role = 'claimant' } = {}) {
  const layer = router.stack.find(item => item.route && item.route.path === routePath && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, `route exists: ${method} ${routePath}`);
  const req = {
    body, params, query,
    user: { username: role + '-user', role, roles: [role] }
  };
  let status = 200;
  let result;
  const res = {
    status(code) { status = code; return this; },
    json(value) { result = value; return this; },
    end() { return this; },
    sendFile() { return this; }
  };
  layer.route.stack[0].handle(req, res, () => {
    throw new Error(`unexpected next() for ${method} ${routePath}`);
  });
  return { status, body: result };
}

test('claimant cannot submit an expense without a bill photo', async () => {
  const result = invoke('POST', '/api/expenses', { body: { ledger: 'FOOD EXPENSE', amount: 100, bill: 'handwritten' } });
  assert.equal(result.status, 400);
  assert.match(result.body.error, /Bill photo is required/);
});

test('new vendor is reviewed and can be corrected by admin before approval', async () => {
  const requested = invoke('POST', '/api/expenses/requests', { body: { kind: 'vendor', name: 'Vender Typo', details: 'Delhi supplier' } });
  assert.equal(requested.status, 200);
  const decided = invoke('POST', '/api/expenses/requests/:id/decide', { params: { id: requested.body.request.id }, body: { approve: true, name: 'Vendor Corrected' }, role: 'admin' });
  assert.equal(decided.status, 200);
  assert.equal(decided.body.request.name, 'Vendor Corrected');

  const config = invoke('GET', '/api/expenses/config').body;
  assert.ok(config.vendors.includes('Vendor Corrected'));
});

test('bill remains mandatory at approval and payment proof is mandatory for cash', async () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', amount: 250, bill: 'printed', billPhoto: '/api/expenses/photo/bill.jpg',
    vendor: 'Vendor Corrected', account: 'Cash'
  } });
  assert.equal(created.status, 200);
  const id = created.body.expense.id;

  const expenseFile = path.join(tempDir, 'expenses.json');
  const stored = JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
  stored.expenses[id].billPhoto = '';
  fs.writeFileSync(expenseFile, JSON.stringify(stored));
  const blockedApproval = invoke('POST', '/api/expenses/:id/approve', { params: { id }, role: 'admin' });
  assert.equal(blockedApproval.status, 400);
  assert.match(blockedApproval.body.error, /Bill photo required/);

  invoke('POST', '/api/expenses/:id', { params: { id }, body: { billPhoto: '/api/expenses/photo/bill.jpg' }, role: 'admin' });
  const approved = invoke('POST', '/api/expenses/:id/approve', { params: { id }, role: 'admin' });
  assert.equal(approved.status, 200);

  const blockedPayment = invoke('POST', '/api/expenses/:id/pay', { params: { id }, body: { account: 'Cash' }, role: 'admin' });
  assert.equal(blockedPayment.status, 400);
  assert.match(blockedPayment.body.error, /Payment screenshot required/);

  const paid = invoke('POST', '/api/expenses/:id/pay', { params: { id }, body: { account: 'Cash', paymentProof: '/api/expenses/photo/cash.jpg' }, role: 'admin' });
  assert.equal(paid.status, 200);
  assert.equal(paid.body.expense.status, 'paid');
});
