'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../public/salary.html'), 'utf8');
const inline = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)]
  .map(match => match[1]).filter(script => script.trim()).join('\n');
const baseAccounts = ['Axis Bank 3448', 'IndusInd Bank 8181', 'Prashant Cash', 'Gagan Sir Cash', 'Counter Cash'];
const sharedAccount = 'IndusInd Bank 7883';
const retiredAccount = 'Prashant Axis 3645';
const flush = () => new Promise(resolve => setImmediate(resolve));
const options = markup => [...markup.matchAll(/<option(?:\s[^>]*)?>([^<]+)<\/option>/g)].map(match => match[1]);

function harness(entity = 'SAMAST') {
  const nodes = new Map(), calls = [], checked = [];
  function node(id) {
    if (!nodes.has(id)) {
      const element = {
        id, value: '', textContent: '', open: false, checked: false, files: [],
        dataset: {}, style: {}, classList: {toggle() {}, add() {}, remove() {}},
        addEventListener() {}, focus() {}, scrollIntoView() {},
        showModal() { this.open = true; }, close() { this.open = false; },
        querySelectorAll(selector) {
          if (id === 'salaryEntities' && selector === '[data-entity]')
            return ['SANKI', 'SAMAST'].map(name => node('entity_' + name));
          return [];
        }
      };
      let markup = '';
      Object.defineProperty(element, 'innerHTML', {
        get() { return markup; },
        set(value) {
          markup = String(value);
          for (const tag of markup.matchAll(/<[^>]+\bid="([^"]+)"[^>]*>/g)) node(tag[1]);
          for (const select of markup.matchAll(/<select\b[^>]*\bid="([^"]+)"[^>]*>([\s\S]*?)<\/select>/g))
            node(select[1]).innerHTML = select[2];
        }
      });
      nodes.set(id, element);
    }
    return nodes.get(id);
  }
  ['SANKI', 'SAMAST'].forEach(name => { node('entity_' + name).dataset.entity = name; });
  const payroll = {
    success: true, rows: [{id: 'E1', name: 'Rajendra Singh', balance: 12000, netPayable: 12000, paid: 0, paidDays: 30}],
    totals: {}, permissions: {canModifyPayroll: true}
  };
  const payments = [
    {id: 'CURRENT', account: sharedAccount, amount: 12000, date: '2026-10-20', proofs: ['/existing.jpg']},
    {id: 'HISTORICAL', account: retiredAccount, amount: 5000, date: '2026-09-12', proofs: ['/historical.jpg']}
  ];
  const document = {
    getElementById: node,
    querySelectorAll(selector) { return selector === '.salpay:checked' ? checked : []; },
    querySelector(selector) {
      const match = selector.match(/^\.salpayamt\[data-emp="([^"]+)"\]$/);
      return match ? node('amount_' + match[1]) : null;
    }
  };
  class MockFormData {
    constructor() { this.values = []; }
    append(key, value) { this.values.push([key, value]); }
  }
  const context = {
    document, console, URL, FormData: MockFormData,
    confirm() { return true; }, prompt() { return 'Test correction'; },
    location: new URL('https://example.test/salary.html?entity=' + entity + '&month=2026-10'),
    fetch(url, request) {
      calls.push({url, request});
      if (url === '/api/auth/me') return new Promise(() => {});
      const parsed = new URL(url, 'https://example.test');
      let response;
      if (url === '/api/expenses/upload') response = {success: true, url: '/uploaded.jpg'};
      else if (request && request.method && request.method !== 'GET')
        response = {success: true, count: 1, batchId: 'TEST-BATCH'};
      else if (parsed.pathname === '/api/salary/employees')
        response = {
          success: true, employees: [],
          salaryPayingAccounts: baseAccounts.concat(parsed.searchParams.get('entity') === 'SAMAST' ? [sharedAccount] : []),
          retiredSalaryPayingAccounts: [retiredAccount],
          advancePayingAccounts: [{name: 'ICICI Bank 0993', nature: 'PERSONAL'}]
        };
      else if (parsed.pathname.startsWith('/api/salary/month/')) response = payroll;
      else if (parsed.pathname.startsWith('/api/salary/payments/')) response = {success: true, payments};
      else throw new Error('Unexpected request: ' + url);
      return Promise.resolve({ok: true, json: () => Promise.resolve(response), text: () => Promise.resolve(JSON.stringify(response))});
    }
  };
  context.window = context;
  context.history = {state: null, replaceState(_state, _title, target) { context.location = new URL(target, context.location); }};
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../public/navigation-state.js'), 'utf8'), context);
  vm.runInContext(inline, context);
  context.load();
  return {context, node, calls, checked};
}

function salaryWrites(calls) {
  return calls.filter(call => call.url.startsWith('/api/salary/') && call.request && call.request.method !== 'GET');
}

test('salary inline scripts remain syntactically valid', () => {
  assert.doesNotThrow(() => new vm.Script(inline));
});

test('SAMAST loads the entity-specific 7883 option into single, batch and correction pickers', async () => {
  const {context, node, calls} = harness();
  await flush();
  assert.ok(calls.some(call => call.url === '/api/salary/employees?entity=SAMAST'));
  // Unrelated advance/personal sources must not be used to populate salary pickers.
  context.state.accounts = ['ICICI Bank 0993', 'ICICI Bank 0992', 'Namita 5464', retiredAccount];
  context.openSalaryPay('E1');
  const expected = baseAccounts.concat(sharedAccount);
  assert.deepEqual(options(node('one_account').innerHTML), expected);
  assert.deepEqual(options(node('sp_account').innerHTML), expected);
  assert.equal(node('salaryPayTitle').textContent, 'Pay salary · Rajendra Singh');
  context.openSalaryPaymentCorrection('CURRENT');
  assert.deepEqual(options(node('edit_account').innerHTML), expected);
  assert.equal(node('edit_account').value, sharedAccount);
  assert.equal(salaryWrites(calls).length, 0, 'viewing payment options never posts money');
});

test('switching SAMAST to SANKI and back refreshes salary accounts without leaking 7883', async () => {
  const {context, node, calls} = harness();
  await flush();
  node('entity_SANKI').onclick.call(node('entity_SANKI'));
  await flush();
  context.openSalaryPay('E1');
  assert.equal(context.state.entity, 'SANKI');
  assert.deepEqual(options(node('one_account').innerHTML), baseAccounts);
  assert.deepEqual(options(node('sp_account').innerHTML), baseAccounts);
  assert.ok(calls.some(call => call.url === '/api/salary/employees?entity=SANKI'));
  node('entity_SAMAST').onclick.call(node('entity_SAMAST'));
  await flush();
  context.openSalaryPay('E1');
  assert.deepEqual(options(node('one_account').innerHTML), baseAccounts.concat(sharedAccount));
  assert.equal(calls.filter(call => call.url === '/api/salary/employees?entity=SAMAST').length, 2);
  assert.equal(salaryWrites(calls).length, 0);
});

test('single SAMAST salary payment submits the chosen 7883 account and SAMAST entity', async () => {
  const {context, node, calls} = harness();
  await flush();
  context.openSalaryPay('E1');
  node('one_account').value = sharedAccount;
  node('one_date').value = '2026-10-20';
  node('one_proof').files = [{name: 'salary.jpg'}];
  context.submitSingleSalaryPayment();
  await flush();
  const writes = salaryWrites(calls);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].url, '/api/salary/payments/batch?entity=SAMAST');
  const body = JSON.parse(writes[0].request.body);
  assert.equal(body.account, sharedAccount);
  assert.equal(body.ym, '2026-10');
  assert.equal(body.date, '2026-10-20');
  assert.deepEqual(body.proofs, ['/uploaded.jpg']);
  assert.deepEqual(body.items, [{empId: 'E1', amount: 12000, modificationReason: ''}]);
  assert.equal(node('salaryPayDlg').open, false);
});

test('batch SAMAST salary payment submits 7883 without replacing it with a SANKI source', async () => {
  const {context, node, calls, checked} = harness();
  await flush();
  checked.push({dataset: {emp: 'E1'}});
  node('amount_E1').value = '12000';
  node('amount_E1').dataset.balance = '12000';
  node('sp_account').value = sharedAccount;
  node('sp_date').value = '2026-10-20';
  node('sp_proof').files = [{name: 'salary.jpg'}];
  context.recordSalaryBatch();
  await flush();
  const writes = salaryWrites(calls);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].url, '/api/salary/payments/batch?entity=SAMAST');
  const body = JSON.parse(writes[0].request.body);
  assert.equal(body.account, sharedAccount);
  assert.deepEqual(body.items, [{empId: 'E1', amount: 12000, modificationReason: ''}]);
});

test('SAMAST correction preserves 7883 and the existing proof in its entity-scoped request', async () => {
  const {context, node, calls} = harness();
  await flush();
  context.openSalaryPaymentCorrection('CURRENT');
  node('edit_reason').value = 'Correct payment reference';
  node('edit_ref').value = 'BANK-REF';
  context.saveSalaryPaymentCorrection();
  await flush();
  const writes = salaryWrites(calls);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].url, '/api/salary/payments/CURRENT?entity=SAMAST');
  assert.equal(writes[0].request.method, 'PATCH');
  const body = JSON.parse(writes[0].request.body);
  assert.equal(body.account, sharedAccount);
  assert.equal(body.reference, 'BANK-REF');
  assert.deepEqual(body.proofs, ['/existing.jpg']);
});

test('retired 3645 is offered only while correcting its own historical salary payment', async () => {
  const {context, node} = harness();
  await flush();
  context.openSalaryPay('E1');
  assert.doesNotMatch(node('one_account').innerHTML, /3645/);
  assert.doesNotMatch(node('sp_account').innerHTML, /3645/);
  context.openSalaryPaymentCorrection('HISTORICAL');
  assert.equal(node('edit_account').value, retiredAccount);
  assert.match(node('edit_account').innerHTML, /Prashant Axis 3645 — historical payment only/);
  assert.match(node('edit_account').innerHTML, /IndusInd Bank 7883/);
  context.openSalaryPaymentCorrection('CURRENT');
  assert.doesNotMatch(node('edit_account').innerHTML, /3645/);
});
