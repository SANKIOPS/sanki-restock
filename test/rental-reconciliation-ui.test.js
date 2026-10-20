'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const flush = () => new Promise(resolve => setImmediate(resolve));
const line = marker => {
  const result = page.split('\n').find(value => value.includes(marker));
  assert.ok(result, 'source line exists: ' + marker);
  return result;
};
const section = (start, end) => {
  const a = page.indexOf(start), b = page.indexOf(end, a + start.length);
  assert.ok(a >= 0 && b > a, 'source section exists: ' + start);
  return page.slice(a, b);
};
const escapeHtml = value => String(value).replace(/[&<>"']/g, char => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[char]));
const tenancy = { id: 'TEN-00001', property: 'SAMAST', floor: 'First floor', tenant: 'Existing tenant' };

test('bank reconciliation reviews an imported security opening and does not create a duplicate receipt', async () => {
  const opening = { ...tenancy, securityOpening: 200000, securityOpeningDate: '2026-09-30', securityBalance: 200000 };
  const after = { ...opening, securityOpening: null, securityOpeningDate: '', securityBalance: 0 };
  const ui = harness({ responses: [{ success: true, revision: 7, tenancies: [opening] }, { success: true, revision: 8, tenancies: [after] }], row: { bank: { date: '2026-08-25', credit: 200000, debit: 0, reference: 'SOURCE-SECURITY', description: 'Existing source deposit' } } });
  ui.context.confirm = () => true;
  ui.clickRental('security'); await flush();
  ui.nodes.rentalReconTenancy.value = tenancy.id; ui.fire('rentalReconTenancy', 'change');
  assert.equal(ui.nodes.bankRentalOpeningReview.style.display, 'block');
  assert.match(ui.nodes.bankRentalOpeningSummary.textContent, /200000/);
  assert.equal(ui.nodes.bankRentalSave.disabled, true, 'earlier source cannot be double counted while opening exists');
  assert.equal(ui.calls.filter(call => call.options?.method === 'POST').length, 0);
  ui.nodes.bankRentalOpeningReason.value = 'Replace imported opening with exact bank source SOURCE-SECURITY';
  ui.fire('bankRentalClearOpening'); ui.fire('bankRentalClearOpening'); await flush();
  const writes = ui.calls.filter(call => call.options?.method === 'POST'); assert.equal(writes.length, 1);
  assert.equal(writes[0].url, '/api/expenses/rentals');
  assert.deepEqual(JSON.parse(writes[0].options.body), { action: 'clear_security_opening', tenancyId: tenancy.id, revision: 7, expectedSecurityOpening: 200000, expectedSecurityOpeningDate: '2026-09-30', reason: ui.nodes.bankRentalOpeningReason.value });
  assert.equal(ui.nodes.bankRentalOpeningReview.style.display, 'none');
  assert.equal(ui.nodes.bankRentalSave.disabled, false);
  assert.match(ui.nodes.bankRentalMsg.textContent, /no receipt has been added/);
});

test('canceling or failing a bank-form opening correction keeps the source receipt blocked and sends no refund', async () => {
  const opening = { ...tenancy, securityOpening: 200000, securityOpeningDate: '2026-09-30', securityBalance: 200000 };
  const ui = harness({ responses: [{ success: true, revision: 7, tenancies: [opening] }, { success: false, error: 'The register changed. Refresh and try again.' }], row: { bank: { date: '2026-08-25', credit: 200000, debit: 0 } } });
  ui.context.confirm = () => false; ui.clickRental('security'); await flush();
  ui.nodes.rentalReconTenancy.value = tenancy.id; ui.fire('rentalReconTenancy', 'change');
  ui.nodes.bankRentalOpeningReason.value = 'Replace the reviewed opening with exact bank source'; ui.fire('bankRentalClearOpening');
  assert.equal(ui.calls.filter(call => call.options?.method === 'POST').length, 0);
  ui.context.confirm = () => true; ui.fire('bankRentalClearOpening'); await flush();
  assert.equal(ui.nodes.bankRentalSave.disabled, true); assert.equal(ui.nodes.bankRentalClearOpening.disabled, false);
  assert.match(ui.nodes.bankRentalOpeningMsg.textContent, /register changed/);
  assert.ok(ui.calls.filter(call => call.options?.method === 'POST').every(call => JSON.parse(call.options.body).action === 'clear_security_opening'));
});

function harness({ nature = 'SAMAST', account = 'IndusInd Bank 7883', tag = 'SAMAST', canManageRentals = true, row: rowPatch = {}, responses = [] } = {}) {
  const nodes = {}, calls = [], rendered = [], refreshes = [];
  function node(id) {
    if (!nodes[id]) nodes[id] = {
      value: '', textContent: '', innerHTML: '', className: '', disabled: false, open: false, style: {},
      handlers: {},
      addEventListener(event, callback) { this.handlers[event] = callback; },
      showModal() { this.open = true; },
      close() { this.open = false; },
      insertAdjacentHTML(position, html) { this.innerHTML = position === 'afterbegin' ? html + this.innerHTML : this.innerHTML + html; },
      appendChild(child) { this.innerHTML += child.textContent; }
    };
    return nodes[id];
  }
  const row = {
    id: 'bank-1', status: 'missing_in_app',
    bank: { date: '2026-10-01', credit: 25000, debit: 0, reference: 'REF-7883', description: 'Tenant receipt' },
    entityTag: { nature: tag }, ...rowPatch
  };
  const context = {
    cfg: { canManageRentals, isAdmin: true },
    bankNature: nature, bankDraftId: 'BRD-7883', bankEntityTagRowId: '',
    bankReconView: { account, rows: [row] },
    el: node, esc: escapeHtml, fmt: value => '₹' + value,
    transferAccountsForNature: () => [],
    bankDecisionLabel: () => '',
    bankEntityTagMeta: () => '',
    document: { createElement: () => ({}) },
    api: async (url, options) => {
      calls.push({ url, options });
      if (responses.length) {
        const response = responses.shift();
        if (response instanceof Error) throw response;
        if (typeof response === 'function') return response();
        return response;
      }
      if (url === '/api/expenses/rentals') return { success: true, tenancies: [tenancy] };
      if (url === '/api/expenses/bank-statements/create-rental') return {
        success: true, receipt: { id: 'RNT-00001', amount: 25000 }, tenancy
      };
      throw Error('Unexpected API call: ' + url);
    },
    renderBankReconciliation: result => rendered.push(result),
    loadCash: () => refreshes.push('cash'),
    loadAccountLedger: () => refreshes.push('ledger')
  };
  context.window = context;
  node('bs_account').value = account;
  vm.createContext(context);
  vm.runInContext([
    line('function bankEntityTaggingActive('),
    line('function bankEntityTagLabel('),
    line('function bankReconRow('),
    line('function bankReconAmount('),
    section('    function bankAmountAuthorityText(', '    function bankAmountAuthorityHtml('),
    line('function bankAmountAuthorityHtml('),
    line('function bankAmountAuthoritySourceLabel('),
    line('function bankAmountAuthorityEffectText('),
    section('    window.openBankReview=function(rowId)', '    window.reviewBankReversalPair='),
    section('    var openBankReviewBeforeLoans=', "    el('bankEntityTagSave')"),
    section("    var bankRentalRowId='", "    var bankIncomingRowId='")
  ].join('\n'), context);
  return {
    context, nodes, calls, rendered, refreshes, row,
    fire: (id, event = 'click') => node(id).handlers[event].call(node(id)),
    clickRental: kind => {
      context.openBankReview(row.id);
      const buttons = Array.from(node('bankReviewActions').innerHTML.matchAll(/<button\b[^>]*onclick="([^"]*)"[^>]*>(.*?)<\/button>/g));
      const expected = kind === 'security' ? 'Record security deposit' : 'Record rent';
      const button = buttons.find(match => match[2] === expected);
      assert.ok(button, 'actual Review / Resolve button exists: ' + expected);
      vm.runInContext(button[1].replace(/&#39;/g, "'"), context);
    }
  };
}

for (const tag of ['SAMAST', 'PERSONAL', 'UNKNOWN']) test(`7883 Review / Resolve offers rent and security for ${tag} incoming rows`, async () => {
  const ui = harness({ tag });
  ui.context.openBankReview(ui.row.id);
  assert.match(ui.nodes.bankReviewActions.innerHTML, />Record rent</);
  assert.match(ui.nodes.bankReviewActions.innerHTML, />Record security deposit</);
  ui.clickRental('security');
  assert.equal(ui.nodes.bankReviewDlg.open, false);
  assert.equal(ui.nodes.bankRentalDlg.open, true);
  assert.equal(ui.nodes.rentalReconKind.value, 'security');
  assert.equal(ui.nodes.bankRentalSave.textContent, 'Record security & resolve');
  assert.match(ui.nodes.bankRentalEntity.textContent, /IndusInd Bank 7883/);
  assert.match(ui.nodes.bankRentalEntity.textContent, new RegExp(tag === 'UNKNOWN' ? 'Needs review' : tag));
  await flush();
  assert.match(ui.nodes.rentalReconTenancy.innerHTML, /value="TEN-00001">SAMAST · First floor · Existing tenant/);
  assert.equal(ui.nodes.bankRentalSave.disabled, tag === 'UNKNOWN');
  if (tag === 'UNKNOWN') {
    ui.nodes.rentalReconTenancy.value = tenancy.id;
    ui.fire('bankRentalSave');
    assert.match(ui.nodes.bankRentalMsg.textContent, /Tag this transaction as SAMAST or PERSONAL/);
    assert.equal(ui.calls.filter(call => call.options?.method === 'POST').length, 0);
  }
});

test('other PERSONAL banks retain both existing rental receipt options', async () => {
  const ui = harness({ nature: 'PERSONAL', account: 'ICICI Bank 0992', tag: 'UNKNOWN' });
  ui.clickRental('rent');
  await flush();
  assert.equal(ui.nodes.rentalReconKind.value, 'rent');
  assert.equal(ui.nodes.bankRentalSave.textContent, 'Record rent & resolve');
  assert.equal(ui.nodes.bankRentalSave.disabled, false);
});

const disallowed = [
  ['non-shared SAMAST bank', { account: 'ICICI Bank 0993' }],
  ['SANKI business bank', { nature: 'SANKI', account: 'Axis Bank 3448' }],
  ['user without rental management access', { canManageRentals: false }],
  ['outgoing debit', { row: { bank: { debit: 25000, credit: 0 } } }],
  ['zero amount', { row: { bank: { credit: 0 } } }],
  ['negative amount', { row: { bank: { credit: -100 } } }],
  ['non-numeric amount', { row: { bank: { credit: 'invalid' } } }],
  ['non-finite amount', { row: { bank: { credit: Infinity } } }],
  ['mixed debit and credit row', { row: { bank: { debit: 50, credit: 25000 } } }],
  ['matched row', { row: { status: 'matched' } }],
  ['possible match', { row: { status: 'possible_match' } }],
  ['resolved row', { row: { status: 'resolved' } }],
  ['previously reconciled row', { row: { status: 'already_reconciled' } }],
  ['reversal awaiting evidence', { row: { reversalReviewRequired: true } }],
  ['reversal pair candidate', { row: { reversalPairCandidate: { reference: 'REV-1', bankRowIds: ['bank-1', 'bank-2'] } } }],
  ['confirmed reversal pair', { row: { bankReversalPair: true } }]
];
for (const [name, options] of disallowed) test(`${name} cannot open or submit rental receipts`, async () => {
  const ui = harness(options);
  ui.context.openBankReview(ui.row.id);
  assert.doesNotMatch(ui.nodes.bankReviewActions.innerHTML, />Record rent</);
  assert.doesNotMatch(ui.nodes.bankReviewActions.innerHTML, />Record security deposit</);
  ui.context.openBankRental(ui.row.id, 'security');
  assert.notEqual(ui.nodes.bankRentalDlg&&ui.nodes.bankRentalDlg.open, true, 'direct invocation must not open the form');
  ui.fire('bankRentalSave');
  await flush();
  assert.equal(ui.calls.length, 0);
  assert.match(ui.nodes.bankRentalMsg.textContent, /eligible for rent or security/);
});

for (const kind of ['rent', 'security']) test(`${kind} selection posts existing tenancy and original bank-row identity`, async () => {
  const ui = harness();
  ui.clickRental(kind);
  await flush();
  ui.nodes.rentalReconTenancy.value = tenancy.id;
  ui.nodes.rentalReconNote.value = '  Confirmed tenant payment  ';
  ui.fire('bankRentalSave');
  assert.equal(ui.nodes.bankRentalSave.disabled, true);
  await flush();
  const post = ui.calls.find(call => call.options?.method === 'POST');
  assert.equal(post.url, '/api/expenses/bank-statements/create-rental');
  assert.deepEqual(JSON.parse(post.options.body), {
    draftId: 'BRD-7883', rowId: 'bank-1', tenancyId: tenancy.id, kind, note: 'Confirmed tenant payment'
  }, 'amount, entity and paying account must remain server-derived from bank evidence');
  assert.equal(ui.nodes.bankRentalDlg.open, false);
  assert.equal(ui.nodes.bankRentalSave.disabled, false);
  assert.equal(ui.rendered.length, 1);
  assert.deepEqual(ui.refreshes, ['cash', 'ledger']);
  assert.match(ui.nodes.bs_msg.textContent, kind === 'security' ? /Security deposit ₹25000/ : /Rent ₹25000/);
});

test('changing receipt kind updates both save label and submitted kind', async () => {
  const ui = harness();
  ui.clickRental('rent');
  await flush();
  ui.nodes.rentalReconKind.value = 'security';
  ui.fire('rentalReconKind', 'change');
  assert.equal(ui.nodes.bankRentalSave.textContent, 'Record security & resolve');
  ui.nodes.rentalReconTenancy.value = tenancy.id;
  ui.fire('bankRentalSave');
  await flush();
  assert.equal(JSON.parse(ui.calls.at(-1).options.body).kind, 'security');
});

test('unknown or conflicting entity tags block receipt creation without silently assigning SAMAST', async () => {
  for (const entityTag of [{ nature: 'UNKNOWN' }, { nature: 'SAMAST', sourceConflict: true }]) {
    const ui = harness({ row: { entityTag } });
    ui.clickRental('security');
    await flush();
    assert.equal(ui.nodes.bankRentalSave.disabled, true);
    ui.nodes.rentalReconTenancy.value = tenancy.id;
    ui.fire('bankRentalSave');
    assert.match(ui.nodes.bankRentalMsg.textContent, entityTag.sourceConflict ? /conflicting entity tag/ : /Tag this transaction/);
    assert.equal(ui.calls.length, 1, 'only read-only tenancy retrieval is allowed');
    assert.equal(ui.calls[0].url, '/api/expenses/rentals');
  }
});

test('a changed entity or amount is validated again at save time', async () => {
  for (const mutate of [row => { row.entityTag.nature = 'UNKNOWN'; }, row => { row.bank.credit = 0; }, row => { row.status = 'resolved'; }]) {
    const ui = harness();
    ui.clickRental('security');
    await flush();
    ui.nodes.rentalReconTenancy.value = tenancy.id;
    mutate(ui.row);
    ui.fire('bankRentalSave');
    assert.equal(ui.calls.length, 1);
    assert.equal(ui.nodes.bankRentalMsg.className, 'msg err');
  }
});

test('tenancy and reconciliation remark are required before submitting', async () => {
  const ui = harness();
  ui.clickRental('security');
  await flush();
  ui.fire('bankRentalSave');
  assert.match(ui.nodes.bankRentalMsg.textContent, /Choose the floor/);
  ui.nodes.rentalReconTenancy.value = tenancy.id;
  ui.nodes.rentalReconNote.value = '  ';
  ui.fire('bankRentalSave');
  assert.match(ui.nodes.bankRentalMsg.textContent, /Enter a reconciliation remark/);
  assert.equal(ui.calls.length, 1);
});

for (const response of [{ success: false, error: 'Register unavailable' }, new Error('Network unavailable')]) test(`failed rental lookup keeps save disabled and reopening retries (${response.error || response.message})`, async () => {
  const ui = harness({ responses: [response] });
  ui.clickRental('security');
  await flush();
  assert.equal(ui.nodes.bankRentalSave.disabled, true);
  assert.equal(ui.nodes.bankRentalDlg.open, true);
  assert.equal(ui.nodes.bankRentalMsg.className, 'msg err');
  assert.equal(ui.nodes.bankRentalMsg.textContent, response.error || response.message);
  assert.match(ui.nodes.rentalReconTenancy.innerHTML, /Could not load rental floors/);
  ui.fire('bankRentalCancel');
  assert.equal(ui.nodes.bankRentalDlg.open, false);
  ui.clickRental('security');
  await flush();
  assert.equal(ui.nodes.bankRentalSave.disabled, false);
  assert.equal(ui.calls.length, 2);
});

for (const response of [{ success: false, error: 'Receipt could not be saved' }, new Error('Connection failed')]) test(`failed receipt save preserves form and enables retry (${response.error || response.message})`, async () => {
  const ui = harness({ responses: [{ success: true, tenancies: [tenancy] }, response] });
  ui.clickRental('security');
  await flush();
  ui.nodes.rentalReconTenancy.value = tenancy.id;
  ui.nodes.rentalReconNote.value = 'Security payment for existing tenant';
  ui.fire('bankRentalSave');
  await flush();
  assert.equal(ui.nodes.bankRentalDlg.open, true);
  assert.equal(ui.nodes.bankRentalSave.disabled, false);
  assert.equal(ui.nodes.rentalReconTenancy.value, tenancy.id);
  assert.equal(ui.nodes.rentalReconKind.value, 'security');
  assert.equal(ui.nodes.rentalReconNote.value, 'Security payment for existing tenant');
  assert.equal(ui.nodes.bankRentalMsg.textContent, response.error || response.message);
  assert.equal(ui.rendered.length, 0);
  assert.equal(ui.refreshes.length, 0);
  ui.fire('bankRentalSave');
  await flush();
  assert.equal(ui.nodes.bankRentalDlg.open, false);
  assert.equal(ui.calls.filter(call => call.options?.method === 'POST').length, 2);
});

test('stale tenancy request cannot replace a newly opened rental form', async () => {
  let finishFirst;
  const ui = harness({ responses: [() => new Promise(resolve => { finishFirst = resolve; })] });
  ui.clickRental('rent');
  ui.clickRental('security');
  await flush();
  assert.equal(ui.nodes.rentalReconKind.value, 'security');
  assert.match(ui.nodes.rentalReconTenancy.innerHTML, /Existing tenant/);
  finishFirst({ success: false, error: 'Stale failure' });
  await flush();
  assert.equal(ui.nodes.bankRentalSave.disabled, false);
  assert.doesNotMatch(ui.nodes.bankRentalMsg.textContent, /Stale failure/);
});

test('tenancy labels are escaped and security treatment is explained without becoming rent income', async () => {
  const ui = harness({ responses: [{ success: true, tenancies: [{ id: 'TEN-<1>', property: 'A&B', floor: '<script>', tenant: '"Tenant"' }] }] });
  ui.clickRental('security');
  await flush();
  assert.match(ui.nodes.rentalReconTenancy.innerHTML, /value="TEN-&lt;1&gt;"/);
  assert.match(ui.nodes.rentalReconTenancy.innerHTML, /A&amp;B · &lt;script&gt; · &quot;Tenant&quot;/);
  assert.match(page, /Security is held separately and is not treated as rent income/);
  assert.match(page, /<option value="security">Security deposit received<\/option>/);
});

test('all inline JavaScript in the expense page parses', () => {
  const scripts = Array.from(page.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi))
    .filter(match => !/\bsrc\s*=/.test(match[1]) && !/\btype\s*=\s*["']application\/(?:ld\+)?json/.test(match[1]));
  assert.ok(scripts.length > 0);
  for (const [index, script] of scripts.entries()) assert.doesNotThrow(() => new vm.Script(script[2], { filename: `expenses-inline-${index}.js` }));
});

test('rental payment pickers include SAMAST rent and unbound legacy receipts, not security or another tenant receipts', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../public/rental-register.js'), 'utf8');
  assert.doesNotThrow(() => new vm.Script(source, { filename: 'rental-register.js' }));
  const host = { innerHTML: '', handlers: {}, querySelector: () => null, addEventListener(event, handler) { this.handlers[event] = handler; } };
  const calls = [], baseReceipt = { date: '2026-10-01', account: 'IndusInd Bank 7883', source: 'Tenant rent', available: 5000, receiptType: 'rental_income' };
  const data = {
    success: true, today: '2026-10-20',
    tenancies: [{ ...tenancy, currentRent: 5000, creditBalance: 0, securityRecorded: true, securityBalance: 10000 }, { ...tenancy, id: 'TEN-OTHER', tenant: 'Other tenant' }],
    invoices: [], credits: [],
    receipts: [
      { ...baseReceipt, id: 'SAMAST-RENT', nature: 'SAMAST', tenancyId: tenancy.id },
      { ...baseReceipt, id: 'OTHER-TENANT-RENT', nature: 'SAMAST', tenancyId: 'TEN-OTHER' },
      { ...baseReceipt, id: 'SECURITY-RECEIPT', nature: 'SAMAST', tenancyId: tenancy.id, receiptType: 'rental_security' },
      { ...baseReceipt, id: 'USED-RECEIPT', nature: 'SAMAST', tenancyId: tenancy.id, available: 0 },
      { ...baseReceipt, id: 'LEGACY-PERSONAL', nature: 'PERSONAL', receiptType: 'other_income' }
    ]
  };
  const context = {
    SankiNavigation: { choice: () => 'payments', text: () => tenancy.id, month: () => '2026-10', set() {} }
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(source, context);
  context.mountRentalRegister(host, async url => { calls.push(url); return data; }, escapeHtml, value => '₹' + value);
  await flush();
  assert.deepEqual(calls, ['/api/expenses/rentals']);
  const pickers = () => Array.from(host.innerHTML.matchAll(/<select\b[^>]*name="receiptId"[^>]*>([\s\S]*?)<\/select>/g), match => match[1]);
  assert.equal(pickers().length, 3, 'settle, split and reserve-advance forms share the protected receipt list');
  for (const html of pickers()) {
    assert.match(html, />Choose existing rent receipt</);
    assert.match(html, /value="SAMAST-RENT"/);
    assert.match(html, /value="LEGACY-PERSONAL"/);
    assert.doesNotMatch(html, /value="(?:OTHER-TENANT-RENT|SECURITY-RECEIPT|USED-RECEIPT)"/);
  }
  host.handlers.change({ target: { name: 'tenantSelect', value: 'TEN-OTHER' } });
  for (const html of pickers()) {
    assert.match(html, /value="OTHER-TENANT-RENT"/);
    assert.match(html, /value="LEGACY-PERSONAL"/);
    assert.doesNotMatch(html, /value="(?:SAMAST-RENT|SECURITY-RECEIPT|USED-RECEIPT)"/);
  }
});
