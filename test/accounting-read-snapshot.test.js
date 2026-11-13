'use strict';

// Real Express routes, isolated fictional accounting records, no live services.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const directory = fs.mkdtempSync(path.join(__dirname, '.tmp-accounting-read-snapshot-'));
process.env.DATA_PATH = path.join(directory, 'data.json');
process.env.PROCUREMENT_PATH = path.join(directory, 'procurement.json');
process.env.ORDERS_PATH = path.join(directory, 'orders.json');
process.env.VELOCITY_REMITTANCE_PATH = path.join(directory, 'velocity-remittance.json');
process.env.SALES_PATH = path.join(directory, 'sales.json');
process.env.TEMP = directory;
process.env.TMP = directory;
process.env.TELEGRAM_BOT_TOKEN = '';

const file = path.join(directory, 'expenses.json');
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-READ-SNAPSHOT', asOf: '2098-01-01', rows: [] } };
fs.writeFileSync(file, '{}');
const { router } = require('../modules/expenses');

function invoke(route, query = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, complete = false;
    const timer = setTimeout(() => reject(new Error('Route did not complete: ' + route)), 10000);
    const finish = value => { if (complete) return; complete = true; clearTimeout(timer); resolve({ status, body: value }); };
    const req = { method: 'GET', path: route, url: route, originalUrl: route, baseUrl: '', query,
      ip: '203.0.113.71', headers: {}, get: () => '', user: { role: 'owner', roles: ['owner'], username: 'synthetic-owner', permissions: {} } };
    const res = { statusCode: 200, headersSent: false, status(value) { status = value; this.statusCode = value; return this; },
      setHeader() {}, getHeader() {}, removeHeader() {}, json(value) { this.headersSent = true; finish(value); return this; },
      send(value) { this.headersSent = true; finish(value); return this; }, end: finish };
    try { router.handle(req, res, error => { if (error) { clearTimeout(timer); reject(error); } else finish(undefined); }); }
    catch (error) { clearTimeout(timer); reject(error); }
  });
}

const account = 'ICICI Bank 0993', nature = 'PERSONAL';
const dateFor = index => new Date(Date.UTC(2098, 0, 1 + index)).toISOString().slice(0, 10);

test.before(async () => {
  const initialized = await invoke('/api/expenses/config');
  assert.equal(initialized.status, 200);
  const store = JSON.parse(fs.readFileSync(file, 'utf8'));
  const transactions = {};
  for (let index = 0; index < 205; index += 1) {
    const id = 'BANK-' + String(index).padStart(4, '0');
    transactions[id] = { id, date: dateFor(index), description: 'Synthetic finalized bank row ' + index,
      reference: 'SYNTHETIC-' + index, debit: index + 1, credit: 0, balance: 100000 - index };
  }
  store.bankStatements = { ['PERSONAL|' + account]: { transactions, imports: [], reconciledThrough: dateFor(204) } };
  store.bankReconciliationDrafts = {};
  fs.writeFileSync(file, JSON.stringify(store));
});

test.after(() => fs.rmSync(directory, { recursive: true, force: true }));

test('bank read is pure, bounded and refreshes only for its relevant accounting scope', async () => {
  const before = fs.readFileSync(file, 'utf8');
  const originalReadFileSync = fs.readFileSync;
  let storeReads = 0;
  fs.readFileSync = function(target, ...args) {
    if (path.resolve(String(target)) === path.resolve(file)) storeReads += 1;
    return originalReadFileSync.call(this, target, ...args);
  };
  try {
    const first = await invoke('/api/expenses/bank-statements', { nature, account });
    assert.equal(first.status, 200, JSON.stringify(first.body));
    assert.equal(first.body.transactionCount, 205);
    assert.equal(first.body.transactions.length, 200);
    assert.equal(first.body.transactions[0].id, 'BANK-0204');
    assert.equal(first.body.transactions.at(-1).id, 'BANK-0005');
    assert.equal(storeReads, 1, 'the first accounting GET builds one shared read snapshot');
    assert.equal(originalReadFileSync(file, 'utf8'), before, 'a bank-statement GET must not repair or save accounting data');

    const repeated = await invoke('/api/expenses/bank-statements', { nature, account });
    assert.equal(repeated.body.transactionCount, 205);
    assert.equal(storeReads, 1, 'an unchanged accounting GET reuses the existing snapshot');

    const ledger = await invoke('/api/expenses/account-ledger', { nature, account, from: dateFor(0), to: dateFor(204) });
    assert.equal(ledger.status, 200, JSON.stringify(ledger.body));
    assert.equal(ledger.body.success, true);
    assert.equal(storeReads, 1, 'ledger edit eligibility reuses the rendered accounting snapshot instead of rereading expenses.json');

    const repeatedLedger = await invoke('/api/expenses/account-ledger', { nature, account, from: dateFor(0), to: dateFor(204) });
    assert.equal(repeatedLedger.status, 200, JSON.stringify(repeatedLedger.body));
    assert.equal(storeReads, 1, 'an identical ledger GET reuses the completed projection and edit evidence');

    fs.writeFileSync(process.env.ORDERS_PATH, JSON.stringify({ orders: { SYNTHETIC: { id:'SYNTHETIC', createdAt:dateFor(204), total:1 } } }));
    fs.writeFileSync(process.env.VELOCITY_REMITTANCE_PATH, JSON.stringify({ entries: [{ date:dateFor(204), amount:1, reference:'SYNTHETIC-VELOCITY' }] }));
    const personalAfterSalesChange = await invoke('/api/expenses/bank-statements', { nature, account });
    assert.equal(personalAfterSalesChange.status, 200, JSON.stringify(personalAfterSalesChange.body));
    assert.equal(personalAfterSalesChange.body.transactionCount, 205);
    assert.equal(storeReads, 1, 'Shopify and Velocity changes do not reload a PERSONAL snapshot');

    const sankiAfterSalesChange = await invoke('/api/expenses/bank-statements', { nature:'SANKI', account:'Axis Bank 3448' });
    assert.equal(sankiAfterSalesChange.status, 200, JSON.stringify(sankiAfterSalesChange.body));
    assert.equal(storeReads, 2, 'the same Shopify and Velocity changes reload the SANKI scope');

    const changed = JSON.parse(before);
    changed.bankStatements['PERSONAL|' + account].transactions['BANK-0205'] = {
      id: 'BANK-0205', date: dateFor(205), description: 'Externally added finalized bank row',
      reference: 'SYNTHETIC-205', debit: 206, credit: 0, balance: 99794
    };
    fs.writeFileSync(file, JSON.stringify(changed));
    const refreshed = await invoke('/api/expenses/bank-statements', { nature, account });
    assert.equal(refreshed.status, 200, JSON.stringify(refreshed.body));
    assert.equal(refreshed.body.transactionCount, 206);
    assert.equal(refreshed.body.transactions.length, 200);
    assert.equal(refreshed.body.transactions[0].id, 'BANK-0205');
    assert.equal(storeReads, 3, 'an expenses.json change reloads the PERSONAL scope');
  } finally {
    fs.readFileSync = originalReadFileSync;
  }
});
