'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const source = html.slice(html.indexOf('    function payingAccountsForNature('), html.indexOf('    function reimbursementAccountOptions('));

function picker(cfg) {
  const elements = { payAcct: { innerHTML: '' }, payMsg: { textContent: '' } };
  const context = vm.createContext({ cfg, Promise, el: id => elements[id], loadConfig: async () => {},
    esc: value => String(value).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;') });
  vm.runInContext(source, context);
  return { context, elements };
}

test('SAMAST picker clearly labels the authorized personal source and retains the exact account value', async () => {
  const { context, elements } = picker({ vendorPaymentAccountsByNature: { SAMAST: ['ICICI Bank 0992', 'Shivam Cash'] }, claimantAccountOwners: { 'Shivam Cash': 'shivam' } });
  await context.populateVendorPayAccounts('SAMAST', false);
  assert.ok(elements.payAcct.innerHTML.includes('<option value="ICICI Bank 0992">ICICI Bank 0992 — Sir\'s personal account; SAMAST expense</option>'));
  assert.equal((elements.payAcct.innerHTML.match(/value="ICICI Bank 0992"/g) || []).length, 1);
  assert.match(elements.payAcct.innerHTML, /Shivam Cash — paid personally by Shivam/);
});

test('0992 authorization is never copied from SAMAST into SANKI or from generic fallback over an empty vendor list', async () => {
  const { context, elements } = picker({ vendorPaymentAccountsByNature: { SANKI: ['Counter Cash'], SAMAST: [] }, payingAccountsByNature: { SAMAST: ['ICICI Bank 0992'] } });
  assert.doesNotMatch(context.vendorPaymentAccountOptions('SANKI'), /0992/);
  await context.populateVendorPayAccounts('SAMAST', true);
  assert.doesNotMatch(elements.payAcct.innerHTML, /0992/);
  assert.match(elements.payMsg.textContent, /No paying account is available/);
});

test('PERSONAL account option does not acquire a SAMAST expense label', () => {
  const { context } = picker({ vendorPaymentAccountsByNature: { PERSONAL: ['ICICI Bank 0992'] } });
  assert.equal(context.vendorPaymentAccountOptions('PERSONAL'), '<option value="ICICI Bank 0992">ICICI Bank 0992</option>');
});
