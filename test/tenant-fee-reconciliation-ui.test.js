'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const start = page.indexOf("    var bankRentalRowId='"), end = page.indexOf("    var bankIncomingRowId='", start);
assert.ok(start > 0 && end > start);
const source = page.slice(start, end);
const flush = () => new Promise(resolve => setImmediate(resolve));
const tenant = { id: 'TEN-ONLY', tenant: 'Selected tenant', floor: 'First floor', property: 'Rental property', securityOpening: 10000, securityOpeningDate: '2026-10-31', securityBalance: 10000 };

function harness({ nature = 'SAMAST', account = 'IndusInd Bank 7883', direction = 'debit', tag = 'SAMAST', access = true, canApprove = true, approvalNatures = ['SAMAST', 'PERSONAL'], canManagePersonalAccounting = true, rowPatch = {}, response, deferred } = {}) {
  const nodes = {}, calls = [], rendered = [], refreshes = [], options = {};
  const node = id => nodes[id] || (nodes[id] = {
    value: '', textContent: '', innerHTML: '', disabled: false, hidden: false, style: {}, handlers: {}, open: false,
    addEventListener(event, callback) { this.handlers[event] = callback; },
    showModal() { this.open = true; }, close() { this.open = false; },
    querySelector(selector) { return options[selector] || (options[selector] = { hidden: false, disabled: false, textContent: '' }); }
  });
  const row = { id: 'bank-fee', status: 'missing_in_app', entityTag: { nature: tag }, bank: { date: '2026-10-25', credit: direction === 'credit' ? 2950 : 0, debit: direction === 'debit' ? 2950 : 0, description: 'Synthetic dealer or agreement fee', reference: 'TEST-FEE' }, ...rowPatch };
  const context = {
    cfg: { canManageRentals: access, canApprove, approvalNatures, canManagePersonalAccounting }, bankNature: nature, bankDraftId: 'BRD-FEES', bankReconView: { account, rows: [row] }, bankReconForm: null,
    el: node, esc: value => String(value).replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch])), fmt: value => '₹' + value,
    bankReconRow: id => id === row.id ? row : null,
    bankEntityTaggingActive: () => account === 'IndusInd Bank 7883', bankEntityTagLabel: value => value?.nature || 'Needs review',
    api: async (url, request) => {
      calls.push({ url, request });
      if (request?.method !== 'POST') return { success: true, revision: 8, tenancies: [tenant] };
      if (deferred) return deferred;
      return response || { success: true, direction, tenancy: tenant, [direction === 'debit' ? 'expense' : 'receipt']: { id: direction === 'debit' ? 'EX-FEE' : 'RC-FEE', amount: 2950 } };
    },
    renderBankReconciliation: result => rendered.push(result), loadCash: () => refreshes.push('cash'), loadAccountLedger: () => refreshes.push('ledger'),
    showBankReconForm(form) { context.bankReconForm = form; node('bankReconDlg').open = true; },
    clearBankReconForm() { context.bankReconForm = null; node('bankReconDlg').close(); }
  };
  context.window = context;
  vm.createContext(context); vm.runInContext(source, context);
  const fire = (id, event = 'click') => node(id).handlers[event].call(node(id), { preventDefault() {}, stopImmediatePropagation() {} });
  return { context, nodes, calls, rendered, refreshes, options, row, fire, async open(kind) { context.openBankRental(row.id, kind); await flush(); }, writes: () => calls.filter(call => call.request?.method === 'POST') };
}

for (const nature of ['SAMAST', 'PERSONAL']) for (const direction of ['debit', 'credit']) for (const kind of ['dealer_commission', 'documentation_fee']) {
  test(`${nature} 7883 ${kind} ${direction} keeps tenant link and derives amount/account/direction on server`, async () => {
    const ui = harness({ nature, direction, tag: nature });
    const before = JSON.stringify(ui.row);
    const actions = ui.context.bankRentalActions(ui.row);
    assert.match(actions, new RegExp(kind));
    assert.match(actions, new RegExp(direction === 'debit' ? 'paid' : 'received'));
    await ui.open(kind);
    assert.equal(ui.nodes.bankRentalDlg.open, true);
    assert.equal(ui.nodes.bankRentalSave.disabled, false);
    assert.match(ui.nodes.bankRentalTitle.textContent, new RegExp(direction === 'debit' ? 'paid' : 'received'));
    assert.match(ui.nodes.bankRentalSummary.textContent, new RegExp(direction === 'debit' ? 'Money paid ₹2950' : 'Money received ₹2950'));
    assert.match(ui.nodes.rentalReconTenancy.innerHTML, /TEN-ONLY.*First floor.*Selected tenant/);
    assert.equal(ui.nodes.bankRentalOpeningReview.style.display, 'none', 'fee cannot remove security opening or be blocked by security source date');
    assert.match(ui.nodes.bankRentalTreatment.textContent, /separate from rent, tenant advances and security balances/);
    if (direction === 'debit') assert.doesNotMatch(ui.nodes.rentalReconKind.innerHTML, /value="(?:rent|security)"/);
    else assert.match(ui.nodes.rentalReconKind.innerHTML, /value="security"/);
    ui.nodes.rentalReconTenancy.value = tenant.id;
    ui.nodes.rentalReconNote.value = '  Tenant-linked verified fee  ';
    ui.fire('bankRentalSave'); await flush();
    assert.equal(ui.writes().length, 1);
    assert.equal(ui.writes()[0].url, '/api/expenses/bank-statements/create-rental');
    assert.deepEqual(JSON.parse(ui.writes()[0].request.body), { draftId: 'BRD-FEES', rowId: 'bank-fee', tenancyId: tenant.id, kind, note: 'Tenant-linked verified fee' });
    assert.equal(ui.nodes.bankRentalDlg.open, false);
    assert.match(ui.nodes.bs_msg.textContent, /Selected tenant · First floor/);
    assert.match(ui.nodes.bs_msg.textContent, new RegExp(direction === 'debit' ? 'EX-FEE' : 'RC-FEE'));
    assert.equal(JSON.stringify(ui.row), before, 'opening, previewing and simulated save never mutate bank source');
    assert.deepEqual(ui.refreshes, ['cash', 'ledger']);
  });
}

for (const [label, options] of [
  ['other PERSONAL account', { nature: 'PERSONAL', account: 'ICICI Bank 0992' }],
  ['other SAMAST account', { account: 'ICICI Bank 0993' }],
  ['SANKI book even with 7883 suffix', { nature: 'SANKI' }],
  ['missing rental permission', { access: false }],
  ['missing expense approval for payment', { canApprove: false }],
  ['missing entity approval', { approvalNatures: ['SANKI'] }],
  ['PERSONAL expense without personal accounting permission', { tag: 'PERSONAL', canManagePersonalAccounting: false }],
  ['PERSONAL receipt without personal accounting permission', { direction: 'credit', tag: 'PERSONAL', canManagePersonalAccounting: false }],
  ['resolved row', { rowPatch: { status: 'resolved' } }],
  ['already reconciled row', { rowPatch: { status: 'already_reconciled' } }],
  ['saved decision despite stale status', { rowPatch: { resolution: { action: 'exclude' } } }],
  ['row claimed elsewhere', { rowPatch: { bankClaimedElsewhere: true } }],
  ['reversal review', { rowPatch: { reversalReviewRequired: true } }],
  ['reversal pair', { rowPatch: { bankReversalPair: true } }],
  ['reversal candidate', { rowPatch: { reversalPairCandidate: {} } }],
  ['two-sided amount', { rowPatch: { bank: { debit: 2950, credit: 2950 } } }],
  ['negative amount', { rowPatch: { bank: { debit: -1, credit: 0 } } }],
  ['invalid amount', { rowPatch: { bank: { debit: 'not money', credit: 0 } } }]
]) test(`${label} cannot open a fee workflow or submit it`, async () => {
  const ui = harness(options);
  assert.doesNotMatch(ui.context.bankRentalActions(ui.row), /dealer_commission|documentation_fee/);
  for (const kind of ['dealer_commission', 'documentation_fee']) {
    await ui.open(kind); assert.notEqual(ui.nodes.bankRentalDlg?.open, true);
    ui.context.bankRentalRowId = ui.row.id; ui.nodes.rentalReconKind = { value: kind }; ui.fire('bankRentalSave');
  }
  assert.equal(ui.calls.length, 0);
});

for (const direction of ['debit', 'credit']) for (const entityTag of [{ nature: 'UNKNOWN' }, { nature: 'SAMAST', sourceConflict: true }]) {
  test(`${direction} fee preserves unknown/conflicting entity guard (${entityTag.sourceConflict ? 'conflict' : 'unknown'})`, async () => {
    const ui = harness({ direction, rowPatch: { entityTag } });
    await ui.open('dealer_commission');
    assert.equal(ui.nodes.bankRentalSave.disabled, true);
    assert.match(ui.nodes.bankRentalMsg.textContent, entityTag.sourceConflict ? /conflicting entity/ : /Tag this transaction as SAMAST or PERSONAL/);
    ui.nodes.rentalReconTenancy.value = tenant.id; ui.fire('bankRentalSave');
    assert.equal(ui.writes().length, 0);
  });
}

test('fee form requires tenant and remark and blocks duplicate submission while pending', async () => {
  let release;
  const ui = harness({ deferred: new Promise(resolve => { release = resolve; }) });
  await ui.open('dealer_commission'); ui.fire('bankRentalSave');
  assert.match(ui.nodes.bankRentalMsg.textContent, /Choose the floor/);
  ui.nodes.rentalReconTenancy.value = tenant.id; ui.nodes.rentalReconNote.value = ' '; ui.fire('bankRentalSave');
  assert.match(ui.nodes.bankRentalMsg.textContent, /Enter a reconciliation remark/);
  assert.equal(ui.writes().length, 0);
  ui.nodes.rentalReconNote.value = 'Verified dealer payment for selected tenant';
  ui.fire('bankRentalSave'); ui.fire('bankRentalSave');
  assert.equal(ui.writes().length, 1);
  release({ success: false, error: 'Stale row: nothing saved' }); await flush();
  assert.equal(ui.nodes.bankRentalDlg.open, true); assert.equal(ui.nodes.bankRentalSave.disabled, false);
  assert.equal(ui.nodes.rentalReconTenancy.value, tenant.id); assert.match(ui.nodes.bankRentalMsg.textContent, /nothing saved/);
});

test('changing receipt type updates fee treatment and restores security-specific protection', async () => {
  const ui = harness({ direction: 'credit' }); await ui.open('documentation_fee');
  ui.nodes.rentalReconTenancy.value = tenant.id; ui.fire('rentalReconTenancy', 'change');
  assert.equal(ui.nodes.bankRentalOpeningReview.style.display, 'none');
  ui.nodes.rentalReconKind.value = 'security'; ui.fire('rentalReconKind', 'change');
  assert.equal(ui.nodes.bankRentalOpeningReview.style.display, 'block'); assert.equal(ui.nodes.bankRentalSave.disabled, true);
  ui.nodes.rentalReconKind.value = 'dealer_commission'; ui.fire('rentalReconKind', 'change');
  assert.equal(ui.nodes.bankRentalOpeningReview.style.display, 'none'); assert.equal(ui.nodes.bankRentalSave.disabled, false);
  assert.match(ui.nodes.bankRentalTitle.textContent, /Dealer commission received/);
});

for (const kind of ['dealer_commission', 'documentation_fee']) test(`manual assignment routes ${kind} to required tenant form, never generic adjustment`, async () => {
  const ui = harness(); ui.context.showBankReconForm({ mode: 'assign', rowId: ui.row.id });
  const option = ui.options['option[value="tenant_' + kind + '"]'];
  assert.equal(option.hidden, false); assert.equal(option.disabled, false); assert.match(option.textContent, /paid — linked tenant/);
  ui.nodes.brKind.value = 'tenant_' + kind; ui.context.el('brRemark').value = 'Existing reviewed remark';
  ui.fire('brKind', 'change'); await flush();
  assert.equal(ui.context.bankReconForm, null); assert.equal(ui.nodes.bankReconDlg.open, false);
  assert.equal(ui.nodes.bankRentalDlg.open, true); assert.equal(ui.nodes.rentalReconKind.value, kind);
  assert.equal(ui.nodes.rentalReconNote.value, 'Existing reviewed remark'); assert.equal(ui.writes().length, 0);
});

test('manual fee choices are hidden outside 7883 and stale assignments cannot fall through to generic save', async () => {
  const ui = harness({ account: 'ICICI Bank 0992', nature: 'PERSONAL' });
  ui.context.showBankReconForm({ mode: 'assign', rowId: ui.row.id });
  for (const option of Object.values(ui.options)) assert.equal(option.hidden, true);
  ui.nodes.brKind.value = 'tenant_dealer_commission'; ui.context.el('brRemark').value = 'Reviewed note';
  ui.fire('brSave'); await flush();
  assert.match(ui.nodes.brMsg.textContent, /no longer eligible/); assert.equal(ui.calls.length, 0);
});

test('receipt fee uses authorized entity and rental permission without adding an expense-approval requirement', async () => {
  const ui = harness({ direction: 'credit', canApprove: false });
  await ui.open('dealer_commission');
  assert.equal(ui.nodes.bankRentalDlg.open, true);
  assert.equal(ui.nodes.bankRentalSave.disabled, false);
  assert.match(ui.context.bankRentalActions(ui.row), /Dealer commission received/);
});

test('revoked expense approval or selected-entity authority blocks a fee form at save time', async () => {
  for (const revoke of [ui => { ui.context.cfg.canApprove = false; }, ui => { ui.context.cfg.approvalNatures = []; }]) {
    const ui = harness(); await ui.open('dealer_commission'); ui.nodes.rentalReconTenancy.value = tenant.id;
    revoke(ui); ui.fire('bankRentalSave');
    assert.equal(ui.writes().length, 0);
    assert.match(ui.nodes.bankRentalMsg.textContent, /eligible for a tenant-linked fee/);
  }
});

for (const kind of ['dealer_commission', 'documentation_fee']) for (const direction of ['payment', 'receipt']) {
  test(`${kind} success label follows server ${direction} direction`, async () => {
    const paid = direction === 'payment';
    const ui = harness({ direction: paid ? 'debit' : 'credit', response: { success: true, direction, tenancy: tenant, [paid ? 'expense' : 'receipt']: { id: paid ? 'EX-FEE' : 'RC-FEE', amount: 2950 } } });
    await ui.open(kind); ui.nodes.rentalReconTenancy.value = tenant.id;
    ui.fire('bankRentalSave'); await flush();
    assert.match(ui.nodes.bs_msg.textContent, new RegExp((paid ? 'paid' : 'received') + ' ₹2950'));
    assert.doesNotMatch(ui.nodes.bs_msg.textContent, new RegExp((paid ? 'received' : 'paid') + ' ₹2950'));
  });
}
