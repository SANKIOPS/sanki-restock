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

test('new vendor submits immediately and approver correction becomes reusable', async () => {
  const created = invoke('POST', '/api/expenses', { body: { ledger: 'FOOD EXPENSE', vendor: 'Vender Typo', amount: 100, billPhoto: '/api/expenses/photo/bill.jpg', paymentType: 'Cash' } });
  assert.equal(created.status, 200);
  const corrected = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { vendor: 'Vendor Corrected', ledger: 'FOOD EXPENSE' }, role: 'owner' });
  assert.equal(corrected.status, 200);
  const approved = invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'accounting' });
  assert.equal(approved.status, 200);
  const config = invoke('GET', '/api/expenses/config').body;
  assert.ok(config.vendors.includes('Vendor Corrected'));
});

test('bill remains mandatory at approval and payment proof is mandatory for cash', async () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', amount: 250, bill: 'printed', billPhoto: '/api/expenses/photo/bill.jpg',
    vendor: 'Vendor Corrected', paymentType: 'Cash'
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

  invoke('POST', '/api/expenses/:id', { params: { id }, body: { billPhoto: '/api/expenses/photo/bill.jpg', ledger: 'FOOD EXPENSE' }, role: 'admin' });
  const approved = invoke('POST', '/api/expenses/:id/approve', { params: { id }, role: 'admin' });
  assert.equal(approved.status, 200);

  const blockedPayment = invoke('POST', '/api/expenses/:id/pay', { params: { id }, body: { account: 'Cash' }, role: 'admin' });
  assert.equal(blockedPayment.status, 400);
  assert.match(blockedPayment.body.error, /Payment screenshot required/);

  const paid = invoke('POST', '/api/expenses/:id/pay', { params: { id }, body: { account: 'Cash', paymentProof: '/api/expenses/photo/cash.jpg' }, role: 'admin' });
  assert.equal(paid.status, 200);
  assert.equal(paid.body.expense.status, 'paid');
});

test('claimant identity is automatic and claimant-only fields are enforced server-side', () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', amount: 175, bill: 'printed', billPhoto: '/api/expenses/photo/bill.jpg',
    vendor: 'Vendor Corrected', type: 'fixed',
    claimant: 'spoofed-name', account: 'Private account', channel: 'POS', paymentType: 'UPI',
    qrPhoto: '/api/expenses/photo/vendor-qr.jpg'
  } });
  assert.equal(created.status, 200);
  assert.equal(created.body.expense.claimant, 'claimant-user');
  assert.equal(created.body.expense.account, '');
  assert.equal(created.body.expense.channel, 'Shared');
  assert.equal(created.body.expense.paymentType, 'UPI');
  assert.equal(created.body.expense.qrPhoto, '/api/expenses/photo/vendor-qr.jpg');
  assert.equal(created.body.expense.type, 'variable');
  assert.equal(created.body.expense.ledger, '');

  const edited = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: {
    claimant: 'another-spoof', channel: 'POS'
  }, role: 'claimant' });
  assert.equal(edited.body.expense.claimant, 'claimant-user');
  assert.equal(edited.body.expense.channel, 'Shared');

  const approverEdit = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { channel: 'POS' }, role: 'accounting' });
  assert.equal(approverEdit.body.expense.channel, 'POS');
});

test('UPI requires a vendor QR photo while Cash and Credit do not', () => {
  const upiWithoutQr = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', vendor: 'Vendor Corrected', amount: 100, bill: 'printed', billPhoto: '/api/expenses/photo/bill.jpg', paymentType: 'UPI'
  } });
  assert.equal(upiWithoutQr.status, 400);
  assert.match(upiWithoutQr.body.error, /QR-code photo is required/);

  for (const paymentType of ['Cash', 'Credit']) {
    const result = invoke('POST', '/api/expenses', { body: {
      ledger: 'FOOD EXPENSE', vendor: 'Vendor Corrected', amount: 100, bill: 'printed', billPhoto: '/api/expenses/photo/bill.jpg', paymentType
    } });
    assert.equal(result.status, 200);
    assert.equal(result.body.expense.paymentType, paymentType);
    assert.equal(result.body.expense.qrPhoto, '');
  }
});

test('claimant form supports searchable direct vendor entry and phone gallery uploads', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /id="f_vendor"/);
  assert.match(html, /id="vendorSuggestions" class="combo-menu"/);
  assert.match(html, /No existing match/);
  assert.match(html, /id="f_totalamount"/);
  assert.doesNotMatch(html, /id="reqLedBtn"/);
  assert.doesNotMatch(html, /function requestLedger/);
  assert.match(html, /Have you already paid for this expense/);
  assert.match(html, /id="f_personalproof"/);
  assert.match(html, /id="f_installment"/);
  assert.match(html, /id="f_paymenttype"/);
  assert.match(html, /id="f_qrphoto"/);
  assert.doesNotMatch(html, /capture="environment"/);
  assert.doesNotMatch(html, /id="f_runner"/);
  assert.doesNotMatch(html, /id="f_funded"/);
  assert.doesNotMatch(html, /id="f_account"/);
});

test('Telegram setup is Owner-only and supports per-user notification management', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  const telegram = fs.readFileSync(path.join(__dirname, '..', 'modules', 'telegram.js'), 'utf8');
  assert.match(html, /cfg\.isOwner\?'<div class="card"><b>Telegram notifications \(Owner only\)/);
  assert.match(html, /id="telegramUser"/);
  assert.match(html, /Test selected user/);
  assert.match(html, /Disconnect selected user/);
  assert.doesNotMatch(html, /Link your own Telegram/);
  assert.match(telegram, /router\.post\('\/api\/telegram\/link'.*if\(!owner\(req\)\)/);
  assert.match(telegram, /router\.post\('\/api\/telegram\/test'.*if\(!owner\(req\)\)/);
  assert.match(telegram, /router\.post\('\/api\/telegram\/unlink'.*if\(!owner\(req\)\)/);
});

test('only Admin or Owner can add a missing category during review', () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', vendor: 'Vendor Corrected', amount: 90,
    billPhoto: '/api/expenses/photo/bill.jpg', paymentType: 'Cash'
  } });
  const denied = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'New Review Category' }, role: 'accounting' });
  assert.equal(denied.status, 403);
  const added = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'New Review Category', type: 'running' }, role: 'owner' });
  assert.equal(added.status, 200);
  assert.equal(added.body.expense.ledger, 'New Review Category');
  const config = invoke('GET', '/api/expenses/config', { role: 'owner' }).body;
  assert.ok(config.ledgers.some(l => l.name === 'New Review Category' && l.type === 'running'));
});

test('personally paid expense becomes reimbursement pending only after approval', () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', vendor: 'Personal Vendor', amount: 500, billPhoto: '/api/expenses/photo/bill.jpg',
    paidAlready: true, personalPaymentProof: '/api/expenses/photo/personal.jpg'
  } });
  assert.equal(created.status, 200);
  assert.equal(created.body.expense.reimbursementStatus, 'awaiting_approval');
  invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'FOOD EXPENSE' }, role: 'owner' });
  const approved = invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'accounting' });
  assert.equal(approved.body.expense.reimbursementStatus, 'pending');
  const blocked = invoke('POST', '/api/expenses/:id/reimburse', { params: { id: created.body.expense.id }, body: { amount: 500 }, role: 'accounting' });
  assert.equal(blocked.status, 400);
  const reimbursed = invoke('POST', '/api/expenses/:id/reimburse', { params: { id: created.body.expense.id }, body: { amount: 500, account: 'Cash', paymentProof: '/api/expenses/photo/reimburse.jpg' }, role: 'accounting' });
  assert.equal(reimbursed.body.expense.reimbursementStatus, 'reimbursed');
  assert.equal(reimbursed.body.expense.paidAmount, 500);
});

test('installments support partial payments and prevent overpayment', () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'Furniture Expense-A3', vendor: 'Carpenter', amount: 10000, isInstallment: true, requestedAmount: 1000,
    billPhoto: '/api/expenses/photo/bill.jpg', paymentType: 'Cash'
  } });
  invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'Furniture Expense-A3' }, role: 'owner' });
  invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'accounting' });
  const partial = invoke('POST', '/api/expenses/:id/pay', { params: { id: created.body.expense.id }, body: { amount: 1000, account: 'Cash', paymentProof: '/api/expenses/photo/pay.jpg' }, role: 'accounting' });
  assert.equal(partial.body.expense.status, 'partially_paid');
  const over = invoke('POST', '/api/expenses/:id/pay', { params: { id: created.body.expense.id }, body: { amount: 10000, account: 'Cash', paymentProof: '/api/expenses/photo/pay2.jpg' }, role: 'accounting' });
  assert.equal(over.status, 400);
  assert.match(over.body.error, /cannot exceed/i);
});
