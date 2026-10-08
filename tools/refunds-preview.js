'use strict';
// Loopback-only disposable UI harness. Never points at a production data file.
const fs = require('fs'), path = require('path');
const root = path.resolve(__dirname, '..');
fs.mkdirSync(path.join(root,'.test-tmp'),{recursive:true});
const dataDir = fs.mkdtempSync(path.join(root, '.test-tmp', 'refund-preview-'));
process.env.DATA_PATH = path.join(dataDir, 'data.json');
fs.writeFileSync(path.join(dataDir, 'expenses.json'), '{}');
const express = require('express'), { router } = require('../modules/expenses'), refunds = require('../modules/expense-refunds');
const s = JSON.parse(fs.readFileSync(path.join(dataDir, 'expenses.json'), 'utf8'));
const expense = (id, vendor, value, paid = value) => ({ id, nature: 'SANKI', vendor, particulars: 'Demo office supplies — test data only', amount: value, paidAmount: paid, date: '2026-10-01', status: paid ? 'paid' : 'approved', approvedAt: '2026-10-01T12:00:00Z', createdBy: 'demo', claimant: 'demo', ledger: 'Flowers', type: 'variable', channel: 'Shared', personalPaidAmount: 0, reimbursementAmount: 0, payments: paid ? [{ id: 'PAY-001', amount: paid, date: '2026-10-01', account: 'IndusInd Bank 8181' }] : [] });
Object.assign(s, { expenses: { 'EX-DEMO-001': expense('EX-DEMO-001', 'Demo Supplier', 500), 'EX-DEMO-002': expense('EX-DEMO-002', 'Demo Supplier', 250, 0), 'EX-DEMO-003': expense('EX-DEMO-003', 'Demo Stationery', 200) },
  receipts: [], expenseRefunds: [], expenseRefundSeq: 0, vendorAdvances: [], transfers: [], adjustments: [], bankStatements: {}, bankReconciliationDrafts: {}, bankDateOverrides: {}, cashReconciliations: [], reconciliationExpenses: [],
  vendors: { 'demo supplier': { name: 'Demo Supplier' }, 'demo stationery': { name: 'Demo Stationery' } }, auditLog: [] });
const ctx = { today: '2026-10-08', username: 'demo-owner', expenses: Object.values(s.expenses), checkSource() {}, checkAccount: (sources, c) => c.account, checkRecord() {}, checkExistingMovement() {} };
refunds.post(s, refunds.prepare(s, { requestId: 'preview-refund-example', sources: [{ key: 'expense:EX-DEMO-003', amount: 80 }], components: [{ mode: 'bank', amount: 50, account: 'IndusInd Bank 8181', reference: 'DEMO-REF-50' }, { mode: 'voucher', amount: 30, issuer: 'Demo Stationery', reference: 'DEMO-VOUCHER', expiryDate: '2026-12-31' }], date: '2026-10-07', status: 'received', reasonType: 'return', reason: 'Demo partial return — mixed bank and voucher refund' }, ctx), ctx);
fs.writeFileSync(path.join(dataDir, 'expenses.json'), JSON.stringify(s));
const app = express(); app.use(express.json());
app.use((req, res, next) => { req.user = { username: 'demo-owner', role: 'owner', roles: ['owner'] }; next(); });
app.get('/api/auth/me', (req, res) => res.json({ success: true, username: 'demo-owner', roles: ['owner'], user: req.user }));
app.get('/api/modules', (req, res) => res.json({ success: true, modules: [] }));
app.get('/api/expenses/reconciliation-reminders', (req, res) => res.json({ success: true, reminders: [] }));
app.use(router); app.use(require('../modules/credit-cards').router);
app.use(express.static(path.join(root, 'public')));
const server = app.listen(8787, '127.0.0.1', () => console.log('Disposable refunds preview: http://127.0.0.1:8787/expenses.html?tab=refunds'));
function stop() { server.close(() => process.exit()); }
process.on('SIGTERM', stop); process.on('SIGINT', stop);
