'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const clone = value => JSON.parse(JSON.stringify(value));
const endpoint = '/api/expenses/bank-statements/repair-import';
const draftId = 'BRD-SYNTHETIC-REPAIR-UI';

function section(from, to) {
  const start = html.indexOf(from), end = html.indexOf(to, start);
  assert.ok(start >= 0 && end > start, from); return html.slice(start, end);
}
function previewResult(extra = {}) {
  return { success: true, changed: true, fingerprint: 'a'.repeat(64), sourceHash: 'b'.repeat(64),
    changes: [{ rowId: 'bank-9', before: { debit: 90, credit: 0, reference: '123456789012' },
      after: { debit: 490, credit: 0, reference: '123456789012' } }],
    beforeSummary: { openingBalance: 8292.70, totalDebits: 8291, closingBalance: 1.70 },
    afterSummary: { openingBalance: 8692.70, totalDebits: 8691, closingBalance: 1.70 }, ...extra };
}
function harness(options = {}) {
  const nodes = {}, calls = [], inserted = [], loaded = [];
  function addNode(id) {
    return nodes[id] = { id, value: '', disabled: false, innerHTML: '', textContent: '', className: '', open: false,
      showModal() { this.open = true; }, close() { this.open = false; },
      insertAdjacentHTML(position, markup) { assert.equal(position, 'afterbegin'); this.innerHTML = markup + this.innerHTML; } };
  }
  for (const id of ['bs_repair_import', 'bs_msg', 'bs_table']) addNode(id);
  const context = { bankDraftId: draftId,
    el: id => nodes[id] || null, fmt: value => '₹' + Number(value || 0).toFixed(2),
    document: { getElementById: id => nodes[id] || null, body: { insertAdjacentHTML(position, markup) {
      assert.equal(position, 'beforeend'); inserted.push(markup);
      for (const match of markup.matchAll(/\bid="([^"]+)"/g)) addNode(match[1]);
    } } },
    async api(url, request) {
      const call = { url, method: request.method, headers: clone(request.headers), body: JSON.parse(request.body) };
      calls.push(call);
      if (options.respond) return options.respond(call);
      return call.body.mode === 'preview' ? clone(options.preview || previewResult()) : clone(options.apply || { success: true, applied: true });
    },
    loadBankStatement(id) { loaded.push(id); }
  };
  context.window = context; vm.createContext(context);
  const escapeSource = html.split('\n').find(line => line.includes('var esc = function('));
  assert.ok(escapeSource, 'use the real page escaping function');
  vm.runInContext(escapeSource + '\n' + section('    var bankImportRepairReview=', '    function renderBankReconciliation('), context);
  return { context, nodes, calls, inserted, loaded };
}
function onlyRepairCalls(ui) {
  assert.ok(ui.calls.every(call => call.url === endpoint && call.method === 'POST'));
  assert.ok(ui.calls.every(call => !Object.keys(call.body).some(key => /amount|debit|credit|opening/i.test(key))), 'no replacement amount or accounting operation is submitted');
}

test('only an IndusInd mobile PDF offers the original-column check, without automatically issuing requests', () => {
  for (const format of ['IndusInd mobile PDF', 'Axis PDF', 'CSV', undefined]) {
    const ui = harness(); ui.context.renderBankImportRepair({ statementSummary: { format } });
    assert.equal(ui.nodes.bs_table.innerHTML.includes('previewBankImportRepair()'), format === 'IndusInd mobile PDF');
    assert.equal(ui.calls.length, 0);
  }
});

test('preview opens an explicit before/after review with source hash, no amount inputs and no accounting request', async () => {
  const ui = harness(); await ui.context.previewBankImportRepair();
  assert.equal(ui.calls.length, 1); onlyRepairCalls(ui);
  assert.deepEqual(ui.calls[0].body, { draftId, mode: 'preview' });
  assert.equal(ui.nodes.bankImportRepairDlg.open, true);
  assert.match(ui.nodes.bankImportRepairRows.innerHTML, /₹90\.00/); assert.match(ui.nodes.bankImportRepairRows.innerHTML, /₹490\.00/);
  assert.match(ui.nodes.bankImportRepairRows.innerHTML, /₹8292\.70 → ₹8692\.70/);
  assert.match(ui.nodes.bankImportRepairRows.innerHTML, /₹8291\.00 → ₹8691\.00/);
  assert.match(ui.nodes.bankImportRepairRows.innerHTML, /₹1\.70 \(unchanged\)/);
  assert.ok(ui.nodes.bankImportRepairRows.innerHTML.includes('b'.repeat(64)));
  assert.match(ui.inserted[0], /Expenses, payments, adjustments and account opening balances stay unchanged/);
  assert.match(ui.inserted[0], /<textarea[^>]*id="bankImportRepairReason"[^>]*maxlength="2000"/);
  assert.doesNotMatch(ui.inserted[0], /<input\b|type="number"/);
  assert.match(ui.nodes.bankImportRepairMsg.textContent, /No new accounting entry/);
  assert.equal(ui.nodes.bs_repair_import.disabled, false); assert.equal(ui.loaded.length, 0);
});

test('apply sends only the preview fingerprint and trimmed reason, then refreshes that same statement', async () => {
  const ui = harness(); await ui.context.previewBankImportRepair();
  ui.nodes.bankImportRepairReason.value = '  Checked original PDF debit column  ';
  await ui.context.applyBankImportRepair();
  assert.equal(ui.calls.length, 2); onlyRepairCalls(ui);
  assert.deepEqual(ui.calls[1].body, { draftId, mode: 'apply', fingerprint: 'a'.repeat(64), reason: 'Checked original PDF debit column' });
  assert.equal(ui.nodes.bankImportRepairDlg.open, false); assert.deepEqual(ui.loaded, [draftId]);
  assert.equal(ui.context.bankImportRepairReview, null);
  assert.match(ui.nodes.bs_msg.textContent, /audit trail/); assert.match(ui.nodes.bs_msg.textContent, /Expenses, payments and adjustments are unchanged/);
});

test('closing the review without applying never submits a mutation', async () => {
  const ui = harness(); await ui.context.previewBankImportRepair();
  const cancel = ui.inserted[0].match(/onclick="([^"]+)">Cancel<\/button>/);
  assert.ok(cancel, 'there is a dedicated cancel action');
  vm.runInContext(cancel[1], ui.context);
  assert.equal(ui.nodes.bankImportRepairDlg.open, false); assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0].body.mode, 'preview'); assert.equal(ui.loaded.length, 0);
});

test('an empty reason cannot submit apply and a changed selection cannot reuse another statement review', async () => {
  const ui = harness(); await ui.context.previewBankImportRepair();
  ui.nodes.bankImportRepairReason.value = '  '; await ui.context.applyBankImportRepair();
  assert.equal(ui.calls.length, 1); assert.match(ui.nodes.bankImportRepairMsg.textContent, /Enter a reason/);
  ui.nodes.bankImportRepairReason.value = 'Reviewed source'; ui.context.bankDraftId = 'BRD-OTHER';
  await ui.context.applyBankImportRepair();
  assert.equal(ui.calls.length, 1); assert.match(ui.nodes.bankImportRepairMsg.textContent, /selected statement changed/);
  assert.equal(ui.loaded.length, 0);
});

test('a selected-statement change while preview is pending cannot display a stale repair modal', async () => {
  let respond;
  const ui = harness({ respond() { return new Promise(resolve => { respond = resolve; }); } });
  const pending = ui.context.previewBankImportRepair(); assert.equal(ui.nodes.bs_repair_import.disabled, true);
  ui.context.bankDraftId = 'BRD-CHANGED-WHILE-PARSING'; respond(previewResult()); await pending;
  assert.equal(ui.calls.length, 1); assert.equal(ui.nodes.bankImportRepairDlg, undefined);
  assert.equal(ui.context.bankImportRepairReview, null); assert.equal(ui.nodes.bs_repair_import.disabled, false);
});

test('already-correct imports need no modal, reason or apply request', async () => {
  const ui = harness({ preview: previewResult({ changed: false, changes: [] }) });
  await ui.context.previewBankImportRepair();
  assert.equal(ui.calls.length, 1); assert.equal(ui.nodes.bankImportRepairDlg, undefined);
  assert.match(ui.nodes.bs_msg.textContent, /already agree with the original PDF columns/);
  assert.equal(ui.context.bankImportRepairReview, null); assert.equal(ui.nodes.bs_repair_import.disabled, false);
});

test('preview rejection and network failure show text errors, never a repair modal or apply request', async () => {
  for (const respond of [() => ({ success: false, error: '<img src=x> Source is already finalized' }),
    () => Promise.reject(new Error('Source unavailable'))]) {
    const ui = harness({ respond }); await ui.context.previewBankImportRepair();
    assert.equal(ui.calls.length, 1); assert.equal(ui.nodes.bankImportRepairDlg, undefined);
    assert.match(ui.nodes.bs_msg.textContent, /Source/); assert.equal(ui.nodes.bs_msg.innerHTML, '');
    assert.equal(ui.nodes.bs_repair_import.disabled, false); assert.equal(ui.loaded.length, 0);
  }
});

test('stale fingerprint/server rejection keeps the review open and retryable without refreshing accounting', async () => {
  for (const apply of [{ success: false, error: 'Evidence changed; preview the repair again.' }, null]) {
    const ui = harness({ respond(call) {
      if (call.body.mode === 'preview') return previewResult();
      if (!apply) throw new Error('Network unavailable');
      return apply;
    } });
    await ui.context.previewBankImportRepair(); await ui.context.applyBankImportRepair();
    assert.equal(ui.calls.length, 2); onlyRepairCalls(ui);
    assert.equal(ui.nodes.bankImportRepairDlg.open, true); assert.equal(ui.nodes.bankImportRepairApply.disabled, false);
    assert.match(ui.nodes.bankImportRepairMsg.textContent, /Evidence changed|Network unavailable/);
    assert.equal(ui.loaded.length, 0); assert.notEqual(ui.context.bankImportRepairReview, null);
  }
});

test('source row IDs, references and hash are rendered as escaped text', async () => {
  const attack = '<img src=x onerror="alert(1)">', data = previewResult();
  data.changes[0].rowId = attack; data.changes[0].after.reference = attack; data.sourceHash = attack;
  const ui = harness({ preview: data }); await ui.context.previewBankImportRepair();
  assert.doesNotMatch(ui.nodes.bankImportRepairRows.innerHTML, /<img\b|onerror="alert/);
  assert.match(ui.nodes.bankImportRepairRows.innerHTML, /&lt;img src=x onerror=&quot;alert\(1\)&quot;&gt;/);
});

test('no selected draft means no preview request', async () => {
  const ui = harness(); ui.context.bankDraftId = ''; await ui.context.previewBankImportRepair();
  assert.equal(ui.calls.length, 0); assert.equal(ui.context.bankImportRepairReview, null);
});
