'use strict';
// Local-only synthetic fixture. Never loads or saves real accounting stores.
const express = require('express');
const path = require('path');
const { createRouter } = require('../modules/pnl-routes');
const sources = {
  orders: { orders: { DEMO1: { id: 'DEMO1', name: 'DEMO-ORDER', channel: 'POS', financialStatus: 'paid', processedAt: '2026-10-01', total: 2999, discount: 0, shipping: 0, taxesIncluded: true, lineItems: [{ id: 'L1', sku: 'DEMO-SKU', qty: 1, price: 2999 }], paymentTransactions: [{ id: 'DEMO-PAY', kind: 'sale', status: 'success', amount: 2999, gateway: 'UPI', processedAt: '2026-10-01' }] } } },
  opening: { lots: [{ id: 'DEMO-OPEN', sku: 'DEMO-SKU', qty: 10, unitCost: 999, verified: true }] },
  purchases: {}, salary: {},
  expenses: { expenses: { DEMOEXP: { id: 'DEMOEXP', nature: 'SANKI', status: 'paid', amount: 150, ledger: 'Example marketing', type: 'marketing', particulars: 'Synthetic preview only', payments: [{ id: 'PAY-1', amount: 150, date: '2026-10-02', account: 'Example bank', paymentType: 'UPI' }] } } },
  feeds: [{ name: 'Synthetic preview data — not business figures', status: 'ready' }]
};
const app = express();
app.get('/api/auth/me', (req, res) => res.json({ success: true, user: { username: 'Preview only', roles: ['owner'] } }));
app.get('/api/modules', (req, res) => res.json({ modules: [{ title: 'P&L', section: 'Accounts', href: '/pnl.html', icon: '📊' }], sectionOrder: ['Accounts'] }));
app.use(createRouter({ read: () => sources, clock: () => new Date('2026-10-08T06:00:00Z') }));
app.use(express.static(path.join(__dirname, '..', 'public')));
const server = app.listen(32038, '127.0.0.1', () => console.log('Synthetic preview: http://127.0.0.1:32038/pnl.html'));
server.on('error', error => { console.error(error.message); process.exitCode = 1; });
