'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
// Use synthetic import metadata only; these tests never inspect or mutate a
// real tenant register, bank snapshot, receipt or source spreadsheet.
const sourcePath = require.resolve('../modules/rental-source.json');
require.cache[sourcePath] = { id: sourcePath, filename: sourcePath, loaded: true, exports: { id: 'SYNTHETIC-RENTAL-IMPORT', asOf: '2026-09-30', file: 'Synthetic source', sheet: 'Synthetic sheet', range: 'A1:B1', rows: [] } };
const rental = require('../modules/rental-register');
const clone = value => structuredClone(value);
function harness({ opening = 1000, openingDate = '2026-09-30', deposit = true, allocations = [], extraDeposits = [] } = {}) {
  let store = { rentalSourceImport: 'SYNTHETIC-RENTAL-IMPORT', rentals: { revision: 7, tenancies: [{ id: 'TEN-SYNTHETIC', tenant: 'Synthetic tenant', floor: 'Synthetic floor', property: 'Synthetic property', baseRent: 100, netRent: 100, startMonth: '2026-09', trackingFrom: '2026-09', securityOpening: opening, securityOpeningDate: openingDate, source: { security: opening, asOf: openingDate, file: 'Synthetic source', retainedField: 'unchanged' }, history: [], autoEnabled: false, confirmedAt: '2026-09-30' }], invoices: [], deposits: [...(deposit ? [{ id: 'SEC-ACTUAL', tenancyId: 'TEN-SYNTHETIC', receiptId: 'REC-ACTUAL', amount: 1000, date: '2026-08-25', reason: 'Synthetic existing bank security source' }] : []), ...extraDeposits], allocations: clone(allocations), credits: [], reminders: [], activity: [], automation: {} }, receipts: [{ id: 'REC-ACTUAL', nature: 'PERSONAL', receiptType: 'rental_security', tenancyId: 'TEN-SYNTHETIC', date: '2026-08-25', amount: 1000, account: 'Synthetic Bank', bankTransactionId: 'BTX-ACTUAL' }], bankStatements: { 'Synthetic Bank': { imports: [{ id: 'FINAL-UNCHANGED' }], transactions: { actual: { id: 'BTX-ACTUAL', credit: 1000 } } } }, expenses: {}, transfers: [], auditLog: [] };
  let writes = 0; const routes = {};
  const jobs = rental.register({ get(route, fn) { routes['GET ' + route] = fn; }, post(route, fn) { routes['POST ' + route] = fn; } }, { loadStore: () => clone(store), saveStore(value) { writes++; store = clone(value); }, isOwner: req => req.user.role === 'owner', audit(s, req, action, subjectType, subjectId, details) { s.auditLog.push({ action, subjectType, subjectId, by: req.user.username, ...details }); }, clock: () => new Date('2026-10-20T10:00:00Z') });
  function call(body, user = { role: 'owner', username: 'synthetic-owner' }) { let status = 200, result; routes['POST /api/expenses/rentals']({ body, user }, { status(value) { status = value; return this; }, json(value) { result = value; return this; } }); return { status, ...result }; }
  const payload = extra => ({ action: 'clear_security_opening', tenancyId: 'TEN-SYNTHETIC', revision: store.rentals.revision, expectedSecurityOpening: store.rentals.tenancies[0].securityOpening, expectedSecurityOpeningDate: store.rentals.tenancies[0].securityOpeningDate, reason: 'Remove duplicated imported security; existing bank evidence retained', ...extra });
  return { call, payload, jobs, store: () => clone(store), writes: () => writes, snapshot: () => rental.snapshot(clone(store), '2026-10-20') };
}
function unchangedBooks(store) { return clone({ receipts: store.receipts, bankStatements: store.bankStatements, expenses: store.expenses, transfers: store.transfers, deposits: store.rentals.deposits, allocations: store.rentals.allocations, credits: store.rentals.credits, importId: store.rentalSourceImport, source: store.rentals.tenancies[0].source }); }

test('clear only the active opening and retain original source, bank receipt and existing security deposit', () => {
  const h = harness(), books = unchangedBooks(h.store()); assert.equal(h.snapshot().tenancies[0].securityBalance, 2000);
  const result = h.call(h.payload()); assert.equal(result.status, 200); assert.equal(result.success, true); assert.equal(h.writes(), 1); assert.equal(result.tenancyId, 'TEN-SYNTHETIC'); assert.equal(result.revision, 8); assert.equal(result.tenancies[0].securityOpening, null); assert.equal(result.tenancies[0].securityBalance, 1000); assert.equal(result.tenancies[0].source.security, 1000);
  const s = h.store(), t = s.rentals.tenancies[0]; assert.equal(t.securityOpening, null); assert.equal(t.securityOpeningDate, ''); assert.equal(h.snapshot().tenancies[0].securityBalance, 1000); assert.deepEqual(unchangedBooks(s), books);
  assert.equal(t.securityOpeningCorrections.length, 1); assert.deepEqual(t.securityOpeningCorrections[0].before, { securityOpening: 1000, securityOpeningDate: '2026-09-30' }); assert.deepEqual(t.securityOpeningCorrections[0].after, { securityOpening: null, securityOpeningDate: '' });
  assert.equal(s.auditLog.length, 1); assert.equal(s.auditLog[0].action, 'RENTAL_CLEAR_SECURITY_OPENING'); assert.deepEqual(s.auditLog[0].before, t.securityOpeningCorrections[0].before); assert.deepEqual(s.auditLog[0].after, t.securityOpeningCorrections[0].after); assert.equal(s.rentals.activity.at(-1).action, 'clear_security_opening');
});
test('clearing without an allocated bank deposit leaves zero security, without fabricating a deposit or refund', () => { const h = harness({ deposit: false }); const before = unchangedBooks(h.store()); assert.equal(h.call(h.payload()).status, 200); assert.equal(h.snapshot().tenancies[0].securityBalance, 0); assert.deepEqual(unchangedBooks(h.store()), before); assert.deepEqual(h.store().rentals.deposits, []); });
test('stale revision, missing revision, changed opening/date and invalid expected types fail without writes', () => {
  const changes = [{ revision: undefined }, { revision: 6 }, { revision: '7' }, { expectedSecurityOpening: 999 }, { expectedSecurityOpening: undefined }, { expectedSecurityOpening: true }, { expectedSecurityOpening: null }, { expectedSecurityOpening: ' ' }, { expectedSecurityOpening: 1000.001 }, { expectedSecurityOpeningDate: '2026-09-29' }, { expectedSecurityOpeningDate: undefined }];
  for (const change of changes) { const h = harness(), before = h.store(); assert.equal(h.call(h.payload(change)).status, 409, JSON.stringify(change)); assert.deepEqual(h.store(), before); assert.equal(h.writes(), 0); }
});
test('a descriptive correction reason is required', () => { for (const reason of ['', 'short', null]) { const h = harness(), before = h.store(); assert.equal(h.call(h.payload({ reason })).status, 400); assert.deepEqual(h.store(), before); assert.equal(h.writes(), 0); } });
test('existing rental permissions remain Owner/Prashant only, not generic Admin', () => {
  for (const user of [{ role: 'admin', username: 'another-admin' }, { role: 'accounting', username: 'accountant' }]) { const h = harness(), before = h.store(); assert.equal(h.call(h.payload(), user).status, 403); assert.deepEqual(h.store(), before); }
  const h = harness(); assert.equal(h.call(h.payload(), { role: 'admin', username: 'prashant' }).status, 200); assert.equal(h.store().rentals.tenancies[0].securityOpeningCorrections[0].by, 'prashant');
});
test('replaying the command cannot append a duplicate correction or audit', () => { const h = harness(), body = h.payload(); assert.equal(h.call(body).status, 200); const before = h.store(); assert.equal(h.call(body).status, 409); assert.equal(h.call({ ...body, revision: before.rentals.revision }).status, 409); assert.deepEqual(h.store(), before); assert.equal(h.writes(), 1); });
test('dependent security allocations cannot become negative now or on an earlier date', () => {
  for (const options of [{ allocations: [{ id: 'ALLOC', tenancyId: 'TEN-SYNTHETIC', kind: 'security', amount: 1500, date: '2026-10-01' }] }, { openingDate: '2026-08-01', allocations: [{ id: 'ALLOC-EARLY', tenancyId: 'TEN-SYNTHETIC', kind: 'security', amount: 500, date: '2026-08-10' }] }, { openingDate: '2026-08-01', extraDeposits: [{ id: 'REFUND-EARLY', tenancyId: 'TEN-SYNTHETIC', amount: -500, date: '2026-08-10' }] }]) {
    const h = harness(options), before = h.store(); assert.equal(h.call(h.payload()).status, 409); assert.deepEqual(h.store(), before); assert.equal(h.writes(), 0);
  }
});
test('valid existing security settlements remain unchanged and an active zero opening can also be removed', () => {
  const h = harness({ allocations: [{ id: 'ALLOC', tenancyId: 'TEN-SYNTHETIC', kind: 'security', amount: 800, date: '2026-10-01' }] }), books = unchangedBooks(h.store()); assert.equal(h.call(h.payload()).status, 200); assert.equal(h.snapshot().tenancies[0].securityBalance, 200); assert.deepEqual(unchangedBooks(h.store()), books);
  const zero = harness({ opening: 0 }); assert.equal(zero.call(zero.payload()).status, 200); assert.equal(zero.store().rentals.tenancies[0].securityOpening, null); assert.equal(zero.snapshot().tenancies[0].securityBalance, 1000);
});
test('same-source scheduler restart preserves the cleared opening and its correction history', () => { const h = harness(); assert.equal(h.call(h.payload()).status, 200); const t = h.store().rentals.tenancies[0], books = unchangedBooks(h.store()); h.jobs.tick(); assert.deepEqual(h.store().rentals.tenancies[0], t); assert.deepEqual(unchangedBooks(h.store()), books); assert.equal(h.store().rentalSourceImport, 'SYNTHETIC-RENTAL-IMPORT'); });

const ui = fs.readFileSync(path.join(__dirname, '../public/rental-register.js'), 'utf8');
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const flush = () => new Promise(resolve => setImmediate(resolve));
async function uiHarness({ reason = 'Duplicate opening <checked>', confirm = true } = {}) {
  const backend = harness({ deposit: false }), listeners = {}, calls = [], warnings = [], message = { textContent: '', scrollIntoView() {} }, host = { innerHTML: '', addEventListener(type, fn) { listeners[type] = fn; }, querySelector(selector) { return selector === '#r-message' ? message : null; }, querySelectorAll() { return []; } };
  const context = { window: { prompt(text) { warnings.push(text); return reason; }, confirm(text) { warnings.push(text); return confirm; }, dispatchEvent() {} }, SankiNavigation: { choice: () => 'history', text: () => 'TEN-SYNTHETIC', month: () => '2026-10', set() {} }, Event: class {}, console };
  vm.createContext(context); vm.runInContext(ui, context);
  const api = async (url, options) => { if (!options) return backend.snapshot(); const payload = JSON.parse(options.body); calls.push(payload); return backend.call(payload); };
  context.window.mountRentalRegister(host, api, esc, value => 'INR ' + value); await flush();
  const click = async () => listeners.click({ target: { closest: () => ({ dataset: { clearSecurityOpening: 'TEN-SYNTHETIC' }, hasAttribute: () => false }) } });
  return { backend, host, calls, warnings, message, click };
}
test('correction UI requires reason and explicit confirmation, sends exact opening snapshot/revision, and retains original sheet', async () => {
  const h = await uiHarness(); assert.match(h.host.innerHTML, /Security per sheet/); assert.match(h.host.innerHTML, /data-clear-security-opening="TEN-SYNTHETIC"/); await h.click();
  assert.equal(h.calls.length, 1); assert.equal(h.calls[0].action, 'clear_security_opening'); assert.equal(h.calls[0].expectedSecurityOpening, 1000); assert.equal(h.calls[0].expectedSecurityOpeningDate, '2026-09-30'); assert.equal(h.calls[0].revision, 7);
  assert.ok(h.warnings.some(value => /No bank receipt, deposit, refund or payment/.test(value))); assert.ok(h.warnings.some(value => /temporarily show zero/.test(value)));
  assert.match(h.host.innerHTML, /Security per sheet/); assert.match(h.host.innerHTML, /Removed/); assert.match(h.host.innerHTML, /Duplicate opening &lt;checked&gt;/); assert.doesNotMatch(h.host.innerHTML, /data-clear-security-opening="TEN-SYNTHETIC"/);
});
test('canceling either confirmation stage or giving a short reason sends no correction', async () => { for (const options of [{ reason: null }, { reason: 'short' }, { confirm: false }]) { const h = await uiHarness(options); await h.click(); assert.equal(h.calls.length, 0); assert.equal(h.backend.store().rentals.tenancies[0].securityOpening, 1000); } });
