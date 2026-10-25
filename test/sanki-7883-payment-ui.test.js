const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const source = html.slice(html.indexOf('    function payingAccountsForNature('), html.indexOf('    function reimbursementAccountOptions('));

function picker(cfg) {
  const elements = { payAcct: { innerHTML: '' }, payMsg: { textContent: '' } };
  const context = vm.createContext({
    cfg, Promise,
    esc: value => String(value).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;'),
    el: id => elements[id],
    loadConfig: async () => undefined
  });
  vm.runInContext(source, context);
  return { context, elements };
}

test('authorized SANKI vendor picker labels the shared bank without changing its account value', async () => {
  const { context, elements } = picker({ vendorPaymentAccountsByNature: { SANKI: ['Axis Bank 3448', 'IndusInd Bank 7883'] } });
  await context.populateVendorPayAccounts('SANKI', false);
  assert.match(elements.payAcct.innerHTML, /<option value="IndusInd Bank 7883">IndusInd Bank 7883 — shared bank; SANKI expense<\/option>/);
  assert.equal((elements.payAcct.innerHTML.match(/value="IndusInd Bank 7883"/g) || []).length, 1);
});

test('picker never invents 7883 when server does not authorize it for SANKI', () => {
  const { context } = picker({
    vendorPaymentAccountsByNature: { SANKI: ['Counter Cash'], SAMAST: ['IndusInd Bank 7883'] },
    payingAccountsByNature: { SANKI: ['IndusInd Bank 7883'] }
  });
  assert.doesNotMatch(context.vendorPaymentAccountOptions('SANKI'), /7883/);
  assert.match(context.vendorPaymentAccountOptions('SANKI'), /Counter Cash/);
});

test('empty authorized vendor list does not fall back to another entity or broader paying list', async () => {
  const { context, elements } = picker({
    vendorPaymentAccountsByNature: { SANKI: [], SAMAST: ['IndusInd Bank 7883'] },
    payingAccountsByNature: { SANKI: ['IndusInd Bank 7883'] }
  });
  await context.populateVendorPayAccounts('SANKI', true);
  assert.doesNotMatch(elements.payAcct.innerHTML, /7883/);
  assert.match(elements.payMsg.textContent, /No paying account is available/);
});

test('SAMAST bank labels and claimant-funded payment labels retain their separate meanings', () => {
  const { context } = picker({
    vendorPaymentAccountsByNature: { SANKI: ['Shivam Cash'], SAMAST: ['IndusInd Bank 7883'] },
    claimantAccountOwners: { 'Shivam Cash': 'shivam' }
  });
  assert.match(context.vendorPaymentAccountOptions('SANKI'), /Shivam Cash — paid personally by Shivam/);
  assert.equal(context.vendorPaymentAccountOptions('SAMAST'), '<option value="IndusInd Bank 7883">IndusInd Bank 7883</option>');
});

test('older configuration fallback is entity-specific and escapes account text', () => {
  const { context } = picker({ payingAccountsByNature: { SANKI: ['Cash <test>'], SAMAST: ['IndusInd Bank 7883'] } });
  assert.equal(context.vendorPaymentAccountOptions('SANKI'), '<option value="Cash &lt;test&gt;">Cash &lt;test&gt;</option>');
});

test('shared bank reconciliation displays the linked SANKI expense entity without widening manual target choices', () => {
  const context = vm.createContext({ bankReconView: { account: 'IndusInd Bank 7883' }, bankNature: 'SAMAST', esc: String });
  const start = html.indexOf('    function bankEntityTaggingActive(');
  vm.runInContext(html.slice(start, html.indexOf('    function bankDraftSwitcher(', start)), context);
  vm.runInContext(html.split('\n').find(line => line.includes('function bankRowNature(')), context);
  const row = { entityTag: { nature: 'SANKI', source: 'linked_ledger', confidence: 'exact' } };
  assert.equal(context.bankEntityTagLabel(row.entityTag), 'SANKI');
  assert.match(context.bankEntityTagMarkup(row), /Belongs to: SANKI/);
  assert.equal(context.bankRowNature(row), 'SANKI');
  assert.equal(context.bankEntityTagLabel({ ...row.entityTag, sourceConflict: true }), 'Conflict — review');
  assert.match(html, /function bankEntityAssignmentTargets\(account\).*\(!shared\|\|n!=='SANKI'\)/);
  assert.match(html, /function bankEntityBulkTargets\(\).*n==='SAMAST'\|\|n==='PERSONAL'/);
});
