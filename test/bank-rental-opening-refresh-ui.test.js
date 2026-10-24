'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const start = page.indexOf("    var bankRentalRowId='"), end = page.indexOf("    var bankIncomingRowId='", start);
assert.ok(start >= 0 && end > start, 'the actual bank rental UI section exists');
const source = page.slice(start, end);
const flush = () => new Promise(resolve => setImmediate(resolve));
const opening = {
  id: 'TEN-SELECTED', property: 'SAMAST', floor: 'Upper ground', tenant: 'Synthetic tenant',
  securityOpening: 200000, securityOpeningDate: '2026-09-30', securityBalance: 200000
};
const other = { ...opening, id: 'TEN-OTHER', floor: 'First floor', tenant: 'Other tenant', securityOpening: 50000, securityBalance: 50000 };
const snapshot = (revision, target = opening, extra = []) => ({ success: true, revision, tenancies: [...(target ? [{ ...target }] : []), ...extra] });
const cleared = () => snapshot(10, { ...opening, securityOpening: null, securityOpeningDate: '', securityBalance: 0 });

function harness({ initial = snapshot(7), replies = [], accept = true } = {}) {
  const nodes = {}, calls = [], confirmations = [], refreshes = [];
  const bank = { date: '2026-08-25', credit: 200000, debit: 0, reference: 'EXACT-SECURITY-SOURCE', description: 'Original bank security receipt' };
  const row = { id: 'bank-selected', status: 'missing_in_app', bank, entityTag: { nature: 'SAMAST' } };
  const queue = [initial, ...replies];
  const node = id => nodes[id] || (nodes[id] = {
    value: '', textContent: '', innerHTML: '', disabled: false, className: '', style: {}, open: false, handlers: {},
    addEventListener(event, callback) { this.handlers[event] = callback; },
    showModal() { this.open = true; },
    close() { this.open = false; }
  });
  const context = {
    cfg: { canManageRentals: true }, bankNature: 'SAMAST', bankDraftId: 'BRD-ORIGINAL', bankReconView: { account: 'IndusInd Bank 7883', rows: [row] },
    el: node, bankReconRow: id => id === row.id ? row : null,
    bankEntityTaggingActive: () => true, bankEntityTagLabel: tag => tag.nature,
    esc: value => String(value), fmt: value => '₹' + value,
    api: async (url, options) => {
      calls.push({ url, options });
      assert.equal(url, '/api/expenses/rentals', 'the correction cannot create bank receipts or other ledger entries');
      assert.ok(queue.length, 'every request must be explicitly expected; no automatic retry');
      const response = queue.shift();
      if (response instanceof Error) throw response;
      return typeof response === 'function' ? response(url, options) : response;
    },
    confirm: message => { confirmations.push({ message, callCount: calls.length }); return typeof accept === 'function' ? accept(message) : accept; },
    loadAccountLedger: () => refreshes.push('ledger'), renderBankReconciliation: () => assert.fail('clearing an opening cannot resolve the bank row'), loadCash: () => assert.fail('clearing an opening cannot create a cash movement')
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(source, context);
  const fire = (id, event = 'click', value) => node(id).handlers[event].call(node(id), value);
  return {
    context, nodes, calls, confirmations, refreshes, row, fire,
    async open() {
      context.openBankRental(row.id, 'security'); await flush();
      node('rentalReconTenancy').value = opening.id; fire('rentalReconTenancy', 'change');
      node('bankRentalOpeningReason').value = 'Remove duplicated imported opening; exact bank source already exists';
      node('rentalReconNote').value = 'Keep original receipt remark';
    },
    clear: () => fire('bankRentalClearOpening'),
    writes: () => calls.filter(call => call.options?.method === 'POST')
  };
}

test('refreshes before native confirmation and uses the latest revision only when the reviewed tenant figures are unchanged', async () => {
  const unrelated = { ...other, tenant: 'Unrelated tenant updated', securityBalance: 55000 };
  const ui = harness({ initial: snapshot(7, opening, [other]), replies: [snapshot(9, opening, [unrelated]), cleared()] });
  await ui.open();
  const bankBefore = JSON.stringify(ui.row.bank), noteBefore = ui.nodes.rentalReconNote.value;
  assert.equal(ui.nodes.bankRentalSave.disabled, true, 'the original source is blocked while the opening exists');
  await ui.clear();
  assert.equal(ui.calls.length, 3);
  assert.equal(ui.calls[1].options.cache, 'no-store', 'the pre-confirmation GET must bypass a stale browser cache');
  assert.equal(ui.confirmations.length, 1);
  assert.equal(ui.confirmations[0].callCount, 2, 'confirmation appears after the fresh GET and before the POST');
  assert.match(ui.confirmations[0].message, /200000/);
  assert.match(ui.confirmations[0].message, /2026-09-30/);
  assert.match(ui.confirmations[0].message, /Synthetic tenant/);
  assert.deepEqual(JSON.parse(ui.writes()[0].options.body), {
    action: 'clear_security_opening', tenancyId: opening.id, revision: 9,
    expectedSecurityOpening: 200000, expectedSecurityOpeningDate: '2026-09-30',
    reason: ui.nodes.bankRentalOpeningReason.value
  });
  assert.equal(JSON.stringify(ui.row.bank), bankBefore, 'bank source amount, date, reference and description are unchanged');
  assert.equal(ui.nodes.rentalReconNote.value, noteBefore);
  assert.equal(ui.context.bankRentalSnapshot.revision, 10);
  assert.equal(ui.nodes.bankRentalOpeningReview.style.display, 'none');
  assert.equal(ui.nodes.bankRentalSave.disabled, false, 'recording the exact verified source remains a separate explicit step');
  assert.deepEqual(ui.refreshes, ['ledger']);
});

for (const [label, fresh] of [
  ['opening amount changed', snapshot(8, { ...opening, securityOpening: 199999.99 })],
  ['opening date changed', snapshot(8, { ...opening, securityOpeningDate: '2026-08-24' })],
  ['held balance changed', snapshot(8, { ...opening, securityBalance: 200001 })],
  ['opening removed', snapshot(8, { ...opening, securityOpening: null, securityOpeningDate: '', securityBalance: 0 })],
  ['selected tenant removed', snapshot(8, null, [other])],
  ['duplicate selected ID', snapshot(8, opening, [{ ...opening }])],
  ['different tenant ID', snapshot(8, { ...opening, id: 'TEN-REPLACEMENT' })],
  ['missing revision', { success: true, tenancies: [{ ...opening }] }],
  ['string revision', snapshot('8')],
  ['negative revision', snapshot(-1)],
  ['older revision', snapshot(6)],
  ['fractional revision', snapshot(8.5)],
  ['unsafe revision', snapshot(Number.MAX_SAFE_INTEGER + 1)],
  ['non-finite revision', snapshot(Infinity)],
  ['missing balance', snapshot(8, { ...opening, securityBalance: undefined })],
  ['null balance', snapshot(8, { ...opening, securityBalance: null })],
  ['boolean balance', snapshot(8, { ...opening, securityBalance: false })],
  ['blank balance', snapshot(8, { ...opening, securityBalance: ' ' })],
  ['non-finite balance', snapshot(8, { ...opening, securityBalance: Infinity })],
  ['negative balance', snapshot(8, { ...opening, securityBalance: -1 })],
  ['fractional paise balance', snapshot(8, { ...opening, securityBalance: 200000.001 })],
  ['boolean opening', snapshot(8, { ...opening, securityOpening: true })],
  ['invalid opening date type', snapshot(8, { ...opening, securityOpeningDate: 20260930 })],
  ['unreadable tenancy collection', { success: true, revision: 8, tenancies: {} }],
  ['failed refresh', { success: false, error: 'Owner or Prashant only.' }],
  ['unreadable refresh', null],
  ['network failure', Error('Connection unavailable')]
]) test('does not confirm or post when refreshed ' + label, async () => {
  const ui = harness({ replies: [fresh] });
  await ui.open();
  const bankBefore = JSON.stringify(ui.row.bank);
  await ui.clear();
  assert.equal(ui.calls.length, 2);
  assert.equal(ui.confirmations.length, 0);
  assert.equal(ui.writes().length, 0);
  assert.equal(ui.context.bankRentalSnapshot.revision, 7, 'a changed target is not silently retargeted');
  assert.equal(ui.nodes.bankRentalSave.disabled, true);
  assert.equal(ui.nodes.bankRentalClearOpening.disabled, false);
  assert.equal(ui.nodes.bankRentalOpeningMsg.className, 'msg err');
  assert.ok(ui.nodes.bankRentalOpeningMsg.textContent);
  assert.equal(JSON.stringify(ui.row.bank), bankBefore);
  assert.deepEqual(ui.refreshes, []);
});

test('canceling the native confirmation performs only the refresh and no mutation', async () => {
  const ui = harness({ replies: [snapshot(8)], accept: false });
  await ui.open(); await ui.clear();
  assert.equal(ui.calls.length, 2);
  assert.equal(ui.confirmations.length, 1);
  assert.equal(ui.writes().length, 0);
  assert.equal(ui.nodes.bankRentalSave.disabled, true);
  assert.equal(ui.nodes.rentalReconTenancy.disabled, false);
  assert.equal(ui.nodes.rentalReconNote.disabled, false);
  assert.equal(ui.nodes.bankRentalOpeningMsg.textContent, 'Opening security was not removed.');
  assert.equal(ui.context.bankRentalSnapshot.revision, 8, 'an unchanged refreshed snapshot may be displayed without posting');
});

test('a register change after confirmation shows the CAS error without retrying or creating a receipt', async () => {
  const ui = harness({ replies: [snapshot(8), { success: false, error: 'The register changed. Refresh and try again.' }] });
  await ui.open(); await ui.clear(); await flush();
  assert.equal(ui.calls.length, 3);
  assert.equal(ui.writes().length, 1);
  assert.equal(JSON.parse(ui.writes()[0].options.body).revision, 8);
  assert.equal(ui.confirmations.length, 1);
  assert.match(ui.nodes.bankRentalOpeningMsg.textContent, /register changed/);
  assert.equal(ui.nodes.bankRentalSave.disabled, true);
  assert.equal(ui.nodes.bankRentalClearOpening.disabled, false);
  assert.deepEqual(ui.refreshes, []);
});

test('while refreshing, duplicate clicks and close attempts do not send another request or change the selected receipt', async () => {
  let resolveRefresh;
  const pending = new Promise(resolve => { resolveRefresh = resolve; });
  const ui = harness({ replies: [() => pending, cleared()] });
  await ui.open();
  const bankBefore = JSON.stringify(ui.row.bank), clearing = ui.clear();
  assert.equal(ui.context.bankRentalOpeningBusy, true);
  for (const id of ['bankRentalClearOpening', 'bankRentalSave', 'bankRentalCancel', 'rentalReconTenancy', 'rentalReconKind', 'bankRentalOpeningReason', 'rentalReconNote']) {
    assert.equal(ui.nodes[id].disabled, true, id + ' remains disabled during the refresh');
  }
  ui.clear();
  ui.fire('bankRentalCancel');
  let prevented = false;
  ui.fire('bankRentalDlg', 'cancel', { preventDefault() { prevented = true; } });
  ui.context.openBankRental('bank-other', 'rent');
  assert.equal(prevented, true);
  assert.equal(ui.nodes.bankRentalDlg.open, true);
  assert.equal(ui.calls.length, 2);
  assert.equal(ui.confirmations.length, 0);
  assert.equal(ui.context.bankRentalRowId, 'bank-selected');
  resolveRefresh(snapshot(8)); await clearing;
  assert.equal(ui.calls.length, 3);
  assert.equal(ui.writes().length, 1);
  assert.equal(ui.confirmations.length, 1);
  assert.equal(JSON.stringify(ui.row.bank), bankBefore);
  assert.equal(ui.context.bankRentalOpeningBusy, false);
});

for (const [label, change] of [
  ['tenancy', ui => { ui.nodes.rentalReconTenancy.value = other.id; }],
  ['receipt kind', ui => { ui.nodes.rentalReconKind.value = 'rent'; }],
  ['bank row', ui => { ui.context.bankRentalRowId = 'bank-other'; }],
  ['bank draft', ui => { ui.context.bankDraftId = 'BRD-OTHER'; }],
  ['dialog generation', ui => { ui.context.bankRentalLoadGeneration++; }],
  ['rental permission', ui => { ui.context.cfg.canManageRentals = false; }]
]) test('a changed ' + label + ' during refresh fails closed before confirmation', async () => {
  let resolveRefresh;
  const ui = harness({ replies: [() => new Promise(resolve => { resolveRefresh = resolve; })] });
  await ui.open();
  const clearing = ui.clear();
  change(ui);
  resolveRefresh(snapshot(8)); await clearing;
  assert.equal(ui.calls.length, 2);
  assert.equal(ui.confirmations.length, 0);
  assert.equal(ui.writes().length, 0);
  assert.match(ui.nodes.bankRentalOpeningMsg.textContent, /selected rental transaction changed/);
});

for (const [label, alter] of [
  ['short reason', ui => { ui.nodes.bankRentalOpeningReason.value = 'short'; }],
  ['missing permission', ui => { ui.context.cfg.canManageRentals = false; }],
  ['non-security form', ui => { ui.nodes.rentalReconKind.value = 'rent'; }],
  ['invalid cached revision', ui => { ui.context.bankRentalSnapshot.revision = null; }],
  ['invalid cached balance', ui => { ui.context.bankRentalSnapshot.tenancies[0].securityBalance = NaN; }]
]) test('rejects ' + label + ' without even requesting a refresh', async () => {
  const ui = harness(); await ui.open(); alter(ui); await ui.clear();
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.confirmations.length, 0);
  assert.equal(ui.writes().length, 0);
});

test('zero active opening and precise string amounts are handled without treating them as missing', async () => {
  const zero = { ...opening, securityOpening: '0.00', securityOpeningDate: '', securityBalance: '0.00' };
  const ui = harness({ initial: snapshot(7, zero), replies: [snapshot(8, zero), snapshot(9, { ...zero, securityOpening: null, securityOpeningDate: '', securityBalance: 0 })] });
  await ui.open(); await ui.clear();
  assert.equal(ui.confirmations.length, 1);
  assert.deepEqual(JSON.parse(ui.writes()[0].options.body), {
    action: 'clear_security_opening', tenancyId: opening.id, revision: 8,
    expectedSecurityOpening: '0.00', expectedSecurityOpeningDate: '', reason: ui.nodes.bankRentalOpeningReason.value
  });
});
