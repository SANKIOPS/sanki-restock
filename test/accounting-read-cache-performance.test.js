'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const expensesSource = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');

function cacheHarness() {
  const start = expensesSource.indexOf('const SANKI_ONLY_ACCOUNTING_READ_FILES =');
  const end = expensesSource.indexOf("try { fs.mkdirSync(STATEMENT_DIR", start);
  assert.ok(start >= 0 && end > start, 'extract the real accounting read cache implementation');
  const paths = {
    EXP_PATH: 'expenses.json', CREDIT_CARD_PATH: 'credit-cards.json', PROC_PATH: 'procurement.json',
    SALES_PATH: 'sales.json', SALARY_PATH: 'salary.json', SAMAST_SALARY_PATH: 'salary-samast.json',
    INCENTIVES_PATH: 'incentives.json', ORDERS_PATH: 'orders.json', VELOCITY_REMITTANCE_PATH: 'velocity_remittance.json',
    GOKWIK_API_EVIDENCE_PATH: 'gokwik_api_evidence.json', VELOCITY_PAYOUT_EVIDENCE_PATH: 'velocity_payout_evidence.json',
    VELOCITY_ORDER_INDEX_PATH: 'velocity_order_index.json', VELOCITY_TRACKING_EVIDENCE_PATH: 'velocity_tracking_evidence.json'
  };
  const versions = new Map(Object.values(paths).map(file => [file, 1n]));
  let middleware, loadCount = 0, nextStore = { id: 'cold-store' }, statCount = 0;
  const context = {
    ...paths, Map, Object, String, JSON, Promise, Date,
    fs: { statSync(file) { statCount += 1; const version = versions.get(file); if (version == null) throw Object.assign(new Error('missing'), { code: 'ENOENT' }); return { size: version, mtimeNs: version, ctimeNs: version }; } },
    loadStore() { loadCount += 1; return nextStore; },
    router: { use(handler) { middleware = handler; } }
  };
  const exposed = `
    globalThis.__cacheApi={
      middleware:accountingReadCacheMiddleware,
      snapshot:currentAccountingReadSnapshot,
      publish:publishAccountingReadSnapshot,
      invalidate:invalidateAccountingReadCache,
      cache:accountingReadCache,
      movementCache:accountingBankMovementsCache,
      inflight:accountingReadInflight,
      limits:{responses:ACCOUNTING_READ_CACHE_MAX,movements:ACCOUNTING_BANK_MOVEMENTS_CACHE_MAX,ttl:ACCOUNTING_READ_CACHE_TTL_MS}
    };`;
  vm.runInNewContext(expensesSource.slice(start, end) + exposed, context, { filename: 'expenses-accounting-read-cache.js' });
  return {
    api: context.__cacheApi,
    middleware: (...args) => middleware(...args),
    bump(file) { versions.set(file, (versions.get(file) || 0n) + 1n); },
    setStore(store) { nextStore = store; },
    counts() { return { load: loadCount, stat: statCount }; }
  };
}

function request(account, { username = 'owner', endpoint = '/api/expenses/bank-statements', nature = 'PERSONAL' } = {}) {
  return { method: 'GET', path: endpoint, query: { account, nature }, user: { username, role: 'Admin', roles: ['Admin'], permissions: { personalAccounting: true } } };
}

function response() {
  const listeners = new Map();
  return {
    statusCode: 200, output: [],
    status(code) { this.statusCode = code; return this; },
    json(payload) { this.output.push({ statusCode: this.statusCode, payload }); return payload; },
    once(event, listener) { listeners.set(event, listener); return this; },
    emit(event) { const listener = listeners.get(event); if (listener) listener(); }
  };
}

test('hot accounting reads retain more than three accounts and both endpoints without eviction', () => {
  const h = cacheHarness();
  assert.ok(h.api.limits.responses >= 16, 'cache capacity covers several accounts, endpoints and date ranges');
  assert.ok(h.api.limits.movements >= 8, 'movement projections do not thrash across three accounts');
  assert.equal(h.api.limits.ttl, 5 * 60 * 1000);
  const requests = [];
  for (const account of ['ICICI Bank 0992', 'ICICI Bank 0993', 'IndusInd Bank 7883', 'Axis Bank 3448']) {
    for (const endpoint of ['/api/expenses/account-ledger', '/api/expenses/bank-statements']) requests.push(request(account, { endpoint }));
  }
  let executions = 0;
  for (const req of requests) {
    const res = response();
    h.middleware(req, res, () => { executions += 1; res.json({ success: true, account: req.query.account, endpoint: req.path }); });
  }
  assert.equal(executions, 8);
  assert.equal(h.api.cache.size, 8);
  const repeat = response();
  h.middleware(requests[0], repeat, () => { executions += 1; });
  assert.equal(executions, 8, 'the first account remains hot after three additional accounts and both routes');
  assert.equal(repeat.output[0].payload.account, 'ICICI Bank 0992');
});

test('a durable save can publish its normalized store without a following loadStore rebuild', () => {
  const h = cacheHarness(), saved = { id: 'already-normalized-after-save' };
  h.api.publish(saved);
  const snapshot = h.api.snapshot('PERSONAL');
  assert.equal(snapshot.store, saved);
  assert.equal(h.counts().load, 0, 'the next GET reuses the just-saved object');
  const saveStart = expensesSource.indexOf('function saveStore('), saveEnd = expensesSource.indexOf('function audit(', saveStart);
  const saveBody = expensesSource.slice(saveStart, saveEnd);
  assert.equal((saveBody.match(/publishAccountingReadSnapshot\(s\)/g) || []).length, 2, 'both atomic save paths publish the durable normalized store');
  assert.doesNotMatch(saveBody, /invalidateAccountingReadCache\(\)/, 'a successful save does not force the next GET through loadStore');
});

test('both expensive routes reuse the middleware snapshot and movement projection skips a second fingerprint scan', () => {
  const ledgerStart = expensesSource.indexOf("router.get('/api/expenses/account-ledger'");
  const ledgerEnd = expensesSource.indexOf("router.get('/api/expenses/account-ledger/", ledgerStart + 1);
  const ledgerRoute = expensesSource.slice(ledgerStart, ledgerEnd > ledgerStart ? ledgerEnd : ledgerStart + 2500);
  const bankStart = expensesSource.indexOf("router.get('/api/expenses/bank-statements',");
  const bankEnd = expensesSource.indexOf("router.get('/api/expenses/bank-statements/history", bankStart);
  const bankRoute = expensesSource.slice(bankStart, bankEnd);
  const movementStart = expensesSource.indexOf('function accountingReadBankMovements(');
  const movementEnd = expensesSource.indexOf('function applyReviewedBankDates(', movementStart);
  assert.match(ledgerRoute, /req\.accountingReadSnapshot\|\|currentAccountingReadSnapshot\(nature\)/);
  assert.match(bankRoute, /req\.accountingReadSnapshot\|\|currentAccountingReadSnapshot\(nature\)/);
  assert.doesNotMatch(expensesSource.slice(movementStart, movementEnd), /relevantAccountingReadFingerprint/, 'snapshot generation is sufficient for the movement cache key');
});

test('SANKI snapshot invalidates when external GoKwik or Velocity evidence changes', () => {
  const h = cacheHarness(), first = { id: 'first' }, second = { id: 'second' };
  h.api.publish(first);
  assert.equal(h.api.snapshot('SANKI').store, first);
  h.setStore(second);
  h.bump('gokwik_api_evidence.json');
  assert.equal(h.api.snapshot('SANKI').store, second);
  assert.equal(h.counts().load, 1, 'external evidence participates in the generation fingerprint');
});

test('identical concurrent authorized reads execute the expensive route once', async () => {
  const h = cacheHarness(), req = request('ICICI Bank 0993'), first = response(), second = response();
  let firstExecutions = 0, secondExecutions = 0;
  h.middleware(req, first, () => { firstExecutions += 1; });
  const joined = h.middleware(request('ICICI Bank 0993'), second, () => { secondExecutions += 1; });
  assert.equal(firstExecutions, 1); assert.equal(secondExecutions, 0); assert.equal(h.api.inflight.size, 1);
  first.json({ success: true, account: 'ICICI Bank 0993' });
  await joined;
  assert.equal(secondExecutions, 0);
  assert.deepEqual(second.output[0].payload, { success: true, account: 'ICICI Bank 0993' });
  assert.equal(h.api.inflight.size, 0);
});

test('single-flight never crosses users and a failed leader releases its waiter', async () => {
  const h = cacheHarness(), owner = response(), admin = response();
  let ownerExecutions = 0, adminExecutions = 0;
  h.middleware(request('IndusInd Bank 7883', { username: 'owner' }), owner, () => { ownerExecutions += 1; });
  h.middleware(request('IndusInd Bank 7883', { username: 'prashant' }), admin, () => { adminExecutions += 1; });
  assert.equal(ownerExecutions, 1); assert.equal(adminExecutions, 1, 'different authorization identities do not coalesce');
  owner.json({ success: true }); admin.json({ success: true });

  h.api.invalidate();
  const failed = response(), waiter = response(); let retries = 0;
  h.middleware(request('Axis Bank 3448'), failed, () => {});
  const released = h.middleware(request('Axis Bank 3448'), waiter, () => { retries += 1; waiter.json({ success: true, retried: true }); });
  failed.emit('close');
  await released;
  assert.equal(retries, 1, 'a closed/failed leader releases the identical request to retry');
  assert.equal(waiter.output[0].payload.retried, true);
  assert.equal(h.api.inflight.size, 0);
});

