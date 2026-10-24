'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const helpers = html.slice(html.indexOf('    var ledgerEditRows='), html.indexOf('    window.correctSaleAllocation='));
const editor = html.slice(html.indexOf('    window.editLedgerEntry='), html.indexOf('    window.deleteLedgerAdjustment='));
assert.ok(helpers && editor);

function harness(options = {}) {
  const row = Object.assign({ id: 'TR-TEST', kind: 'transfer', date: '2098-04-12', debit: 137.42, editable: true, canEditLedgerTransaction: true, source: 'Original source', note: 'Original note', proof: '/original.png', proofs: ['/original.png'] }, options.row);
  const calls = [], messages = [], prompts = [], loads = [];
  const cfg = Object.assign({ isAdmin: true, isOwner: false, claimantOnly: false, canEditLedgerTransactions: true }, options.cfg);
  const preview = Object.assign({ success: true, editable: true, fingerprint: 'server-fingerprint', entry: { id: row.id, kind: row.kind, amount: 137.42, date: '2098-04-12', source: 'Server source', note: 'Server note' }, scope: { nature: 'SANKI', account: 'Synthetic Bank' } }, options.preview);
  const context = {
    cfg, selectedNature: 'SANKI', window: {},
    esc: value => String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]),
    today: () => '2098-04-23',
    location: { search: '?canEditLedgerTransactions=true&isOwner=true&kind=transfer' },
    localStorage: { getItem() { throw Error('Local storage must not grant ledger edit permission'); } },
    setMsg: (message, success) => messages.push({ message, success }),
    prompt: (label, initial) => {
      const index = prompts.length;
      prompts.push({ label, initial });
      if (options.prompt) return options.prompt(label, initial, index, context);
      if (options.answers) return options.answers[index] === undefined ? initial : options.answers[index];
      if (label === 'Amount') return '162.87';
      return initial === undefined ? 'Verified correction against evidence' : initial;
    },
    api: async (url, request = {}) => {
      const call = { url, request, body: request.body ? JSON.parse(request.body) : undefined };
      calls.push(call);
      if (options.api) return options.api(call, preview, context);
      return request.method === 'PATCH' ? { success: true } : preview;
    },
    loadCash: async () => { loads.push('cash'); },
    loadAccountLedger: async () => { loads.push('ledger'); }
  };
  vm.createContext(context);
  vm.runInContext(helpers + '\n' + editor, context);
  const button = () => context.entryEditButton(row, 'Synthetic Bank');
  button();
  return { row, cfg, context, calls, messages, prompts, loads, preview, button, edit: () => context.window.editLedgerEntry(row.id) };
}
const patches = h => h.calls.filter(c => c.request.method === 'PATCH');

test('Admin ledger edit visibility is limited to the explicit server capability and eligible requested kinds', () => {
  for (const kind of ['transfer', 'receipt', 'expense', 'personal_expense']) {
    const h = harness({ row: { kind, id: kind + '-TEST', deletable: true } });
    assert.match(h.button(), /editLedgerEntry/);
    assert.doesNotMatch(h.button(), /Delete|deleteLedgerAdjustment/);
  }
  for (const kind of ['adjustment', 'reimbursement', 'receivable', 'opening', 'loan_received', 'expense_refund', 'refund_recovery', 'provisional_expense_payment', 'expense_payment_batch']) {
    assert.equal(harness({ row: { kind } }).button(), '', kind);
  }
  assert.equal(harness({ row: { classification: 'money_exchange' } }).button(), '');
});

test('missing, false or non-boolean capability and role flags cannot manufacture Edit from navigation state', async () => {
  for (const cfg of [
    { canEditLedgerTransactions: false }, { canEditLedgerTransactions: undefined }, { canEditLedgerTransactions: 'true' },
    { isAdmin: false }, { isAdmin: undefined }, { claimantOnly: true }, { claimantOnly: undefined },
    { isOwner: true, canEditLedgerTransactions: false }
  ]) {
    const h = harness({ cfg });
    assert.equal(h.button(), '', JSON.stringify(cfg));
    await h.edit();
    assert.equal(h.calls.length, 0);
    assert.equal(h.prompts.length, 0);
  }
});

test('protected or unverified rows are hidden and direct editor calls do not bypass row eligibility', async () => {
  for (const row of [
    { canEditLedgerTransaction: false }, { canEditLedgerTransaction: undefined }, { canEditLedgerTransaction: 'true' },
    { editable: false }, { editable: undefined }, { privacyRedacted: true }, { financialFieldsLocked: true },
    { approvalPending: true }, { ledgerEditBlockedReason: 'Reconciled movement is locked' }
  ]) {
    for (const isOwner of [false, true]) {
      const h = harness({ row, cfg: { isOwner } });
      assert.doesNotMatch(h.button(), /<button|editLedgerEntry/);
      await h.edit();
      assert.equal(h.calls.length, 0);
    }
  }
});

test('visible protected rows explain their server-provided lock without exposing private details or HTML', () => {
  const reason = 'Reconciled <payment> is locked & requires review';
  const h = harness({ row: { canEditLedgerTransaction: false, ledgerEditBlockedReason: reason } });
  assert.match(h.button(), /ledger-edit-locked/);
  assert.match(h.button(), /Reconciled &lt;payment&gt; is locked &amp; requires review/);
  assert.doesNotMatch(h.button(), /<payment>|<button/);
  for (const options of [{ row: { privacyRedacted: true } }, { cfg: { isAdmin: false } }, { cfg: { claimantOnly: true } }]) {
    const hidden = harness({ ...options, row: { ...options.row, canEditLedgerTransaction: false, ledgerEditBlockedReason: reason } });
    assert.equal(hidden.button(), '');
  }
});

test('rendering and opening an edit preview never write accounting records', async () => {
  const h = harness({ answers: [null] });
  const original = structuredClone(h.row);
  assert.equal(h.calls.length, 0);
  await h.edit();
  assert.equal(h.calls.length, 1);
  const url = new URL(h.calls[0].url, 'https://synthetic.example');
  assert.equal(url.pathname, '/api/expenses/ledger-entry/edit-preview');
  assert.deepEqual(Object.fromEntries(url.searchParams), { id: 'TR-TEST', kind: 'transfer', nature: 'SANKI', account: 'Synthetic Bank' });
  assert.equal(h.calls[0].request.cache, 'no-store');
  assert.equal(patches(h).length, 0);
  assert.deepEqual(h.row, original);
});

test('amount-only edits use current server evidence and omit untouched source, date, note and proof fields', async () => {
  for (const kind of ['transfer', 'receipt', 'expense', 'personal_expense']) {
    const h = harness({ row: { kind, date: '2098-01-01', source: 'Stale source', note: 'Stale note' } });
    const original = structuredClone(h.row);
    await h.edit();
    assert.equal(patches(h).length, 1);
    assert.deepEqual(patches(h)[0].body, {
      id: 'TR-TEST', kind, nature: 'SANKI', account: 'Synthetic Bank',
      reason: 'Verified correction against evidence', fingerprint: 'server-fingerprint', amount: 162.87
    });
    assert.equal(h.prompts.find(p => p.label.startsWith('Date')).initial, '2098-04-12');
    assert.equal(h.prompts.find(p => p.label === 'Note').initial, 'Server note');
    if (kind === 'receipt') assert.equal(h.prompts.find(p => p.label === 'Source / buyer').initial, 'Server source');
    assert.deepEqual(h.row, original);
    assert.deepEqual(h.loads.sort(), ['cash', 'ledger']);
  }
});

test('amount-only legacy receipt correction preserves an empty source rather than requiring an invented source', async () => {
  const h = harness({ row: { kind: 'receipt' } });
  h.preview.entry.source = '';
  h.preview.entry.note = '';
  await h.edit();
  assert.equal(patches(h).length, 1);
  for (const field of ['source', 'note', 'date', 'proof', 'proofs']) assert.equal(Object.hasOwn(patches(h)[0].body, field), false, field);
});

test('an intentional field edit sends only the verified changed fields and the required reason', async () => {
  const h = harness({ row: { kind: 'receipt' }, answers: ['137.42', '2098-04-13', 'Correct source', 'Correct note', '  Verified metadata correction  '] });
  await h.edit();
  assert.deepEqual(patches(h)[0].body, {
    id: 'TR-TEST', kind: 'receipt', nature: 'SANKI', account: 'Synthetic Bank', fingerprint: 'server-fingerprint',
    date: '2098-04-13', source: 'Correct source', note: 'Correct note', reason: 'Verified metadata correction'
  });
});

test('no-change acceptance, prompt cancellation, invalid amount/date and missing reason never submit a write', async () => {
  const cases = [
    { prompt: (_label, initial) => initial },
    { answers: [null] }, { answers: ['', '2098-04-12'] }, { answers: ['Infinity'] }, { answers: ['-1'] },
    { answers: ['162.87', null] }, { answers: ['162.87', 'wrong date'] },
    { answers: ['162.87', '2098-04-12', null] }, { answers: ['162.87', '2098-04-12', 'Server note', '  '] }
  ];
  for (const options of cases) {
    const h = harness(options);
    await h.edit();
    assert.equal(patches(h).length, 0, JSON.stringify(options));
    assert.equal(h.context.ledgerEditBusy, false);
  }
});

test('failed, mismatched or incomplete previews fail closed before editable prompts', async () => {
  const variants = [
    p => { p.success = false; p.error = 'Permission or lock changed'; }, p => { p.editable = false; }, p => { p.editable = 'true'; },
    p => { p.fingerprint = ''; }, p => { delete p.fingerprint; }, p => { p.entry.id = 'OTHER'; },
    p => { p.entry.kind = 'adjustment'; }, p => { p.scope.nature = 'PERSONAL'; }, p => { p.scope.account = 'Other bank'; },
    p => { delete p.scope; }, p => { delete p.entry; }, p => { p.entry.date = ''; },
    p => { p.entry.amount = Infinity; }, p => { p.entry.amount = 0; }, p => { delete p.entry.note; }, p => { delete p.entry.source; }
  ];
  for (const mutate of variants) {
    const h = harness(); mutate(h.preview);
    await h.edit();
    assert.equal(h.prompts.length, 0);
    assert.equal(patches(h).length, 0);
  }
});

test('view, row and permission changes while preview is pending abort instead of using stale UI authority', async () => {
  for (const mutate of [
    h => { h.context.selectedNature = 'PERSONAL'; },
    h => { h.context.ledgerEditRows[h.row.id] = { row: h.row, account: 'Other bank', nature: 'SANKI' }; },
    h => { h.cfg.canEditLedgerTransactions = false; },
    h => { h.row.canEditLedgerTransaction = false; }
  ]) {
    let finish;
    const h = harness({ api: () => new Promise(resolve => { finish = resolve; }) });
    const pending = h.edit(); mutate(h); finish(h.preview); await pending;
    assert.equal(h.prompts.length, 0);
    assert.equal(patches(h).length, 0);
  }
});

test('a second click while preview is pending cannot create duplicate writes', async () => {
  let finish;
  const h = harness({ api: call => call.request.method === 'PATCH' ? { success: true } : new Promise(resolve => { finish = resolve; }) });
  const pending = h.edit();
  await h.edit();
  assert.equal(h.calls.length, 1);
  finish(h.preview); await pending;
  assert.equal(patches(h).length, 1);
  assert.equal(h.context.ledgerEditBusy, false);
});

test('permission changes after prompts prevent submission, and server stale/locked responses do not retry', async () => {
  const changed = harness({ prompt(label, initial, _index, ctx) { if (label.startsWith('Reason')) { ctx.cfg.canEditLedgerTransactions = false; return 'Reason'; } return label === 'Amount' ? '162.87' : initial; } });
  await changed.edit();
  assert.equal(patches(changed).length, 0);
  const locked = harness({ api: (call, preview) => call.request.method === 'PATCH' ? { success: false, error: 'Stale preview or reconciled entry is locked' } : preview });
  await locked.edit();
  assert.equal(patches(locked).length, 1);
  assert.equal(locked.loads.length, 0);
  assert.match(locked.messages.at(-1).message, /locked/);
});

test('network failure does not silently retry a possible write and releases the busy flag', async () => {
  const h = harness({ api: (call, preview) => call.request.method === 'PATCH' ? Promise.reject(Error('Connection interrupted')) : preview });
  await h.edit();
  assert.equal(patches(h).length, 1);
  assert.match(h.messages.at(-1).message, /Could not confirm the edit.*Refresh/);
  assert.equal(h.context.ledgerEditBusy, false);
});

test('other Owner-only actions retain their original controls without inheriting the managed capability', async () => {
  for (const kind of ['adjustment', 'reimbursement', 'receivable']) {
    const h = harness({ cfg: { isOwner: true, canEditLedgerTransactions: false }, row: { kind, canEditLedgerTransaction: undefined, deletable: kind === 'adjustment' } });
    assert.match(h.button(), /editLedgerEntry/);
    if (kind === 'adjustment') assert.match(h.button(), /deleteLedgerAdjustment/);
    await h.edit();
    assert.equal(h.calls.length, 1);
    assert.equal(h.calls[0].request.method, 'PATCH');
    assert.equal(Object.hasOwn(h.calls[0].body, 'fingerprint'), false);
  }
  const opening = harness({ cfg: { isOwner: true }, row: { kind: 'opening' } });
  assert.match(opening.button(), /openOpeningBalance/);
  await opening.edit(); assert.equal(opening.calls.length, 0);
  const exchange = harness({ cfg: { isOwner: true }, row: { classification: 'money_exchange' } });
  assert.match(exchange.button(), /editExchange/);
  await exchange.edit(); assert.equal(exchange.calls.length, 0);
  const sale = harness({ row: { kind: 'sale', editableSale: true } });
  assert.match(sale.button(), /correctSaleAllocation.*Request correction/);
});
