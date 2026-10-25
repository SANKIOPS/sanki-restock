'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const money = value => '₹' + Number(value).toFixed(2);
function section(from, to) { const start = html.indexOf(from), end = html.indexOf(to, start); assert.ok(start >= 0 && end > start, from); return html.slice(start, end); }
function line(marker) { const result = html.split('\n').find(value => value.includes(marker)); assert.ok(result, marker); return result; }
const flush = () => new Promise(resolve => setImmediate(resolve));
function row(amount = 499.5, original = 500) { return { id: 'bank-1', status: 'amount_mismatch', bank: { date: '2099-10-19', debit: amount, description: 'Exact bank source' }, app: { id: 'EX-001/PAY-001', date: '2099-10-18', debit: original, description: 'Vendor payment' }, bankAmountAuthority: { eligible: true, mode: 'manual', originalAmount: original, bankAmount: amount, amountDifference: amount - original, expenseAmountBefore: original, expenseAmountAfter: amount, fingerprint: 'verified-fingerprint' } }; }
function harness(data = row(), options = {}) {
  const nodes = {}, calls = [], confirms = [], messages = [], rendered = [];
  const preview = options.preview || { success: true, bankAmountAuthority: data.bankAmountAuthority, bankDate: data.bank.date, originalAmount: data.app.debit, bankAmount: data.bank.debit };
  const context = { cfg: { isOwner: false }, bankNature: 'SANKI', bankDraftId: 'TEST-DRAFT', bankReconView: { rows: [data], linkCandidates: [] }, bankReconForm: null,
    el(id) { return nodes[id] ||= { value: '', disabled: false, innerHTML: '', textContent: '', style: {}, dataset: {}, open: false, querySelector() { return {}; }, showModal() { this.open = true; }, insertAdjacentHTML(where, text) { this.innerHTML += text; } }; },
    esc, fmt: money, bankReconRow(id) { return context.bankReconView.rows.find(value => value.id === id); },
    bankReconAmount(value) { return Number(value?.bank?.debit || value?.bank?.credit || value?.app?.debit || value?.app?.credit || 0); },
    confirm(text) { confirms.push(text); return options.confirm !== false; },
    api(url, request) { calls.push({ url, body: JSON.parse(request.body) }); if (options.networkError) return Promise.reject(new Error('Network unavailable')); return Promise.resolve(url.endsWith('amount-authority-preview') ? preview : { success: true, synthetic: true }); },
    renderBankReconciliation(value) { rendered.push(value); }, showBankReconForm() {}, saveBankReconForm() {}, finishBankReconForm(value) { context.el('brSave').disabled = false; rendered.push(value); }, setMsg(text) { messages.push(text); }, closeBankReview() {}, loadAccountLedger() {}, Promise,
    ...options.context
  };
  context.window = context; vm.createContext(context);
  vm.runInContext(section('    function bankAmountAuthorityText(', '    function renderBankReconciliation('), context);
  return { context, nodes, calls, confirms, messages, rendered, run: source => vm.runInContext(source, context) };
}

test('automatic correction metadata is displayed as pending Owner finalization with both payment and expense before/after', () => {
  for (const difference of [.5, 1]) {
    const data = row(500 + difference); data.status = 'matched'; data.bankAmountAuthority.mode = 'auto';
    const ui = harness(data);
    Object.assign(ui.context, { bankStatementLoadGeneration: 0, SankiNavigation: { set() {} }, bankDisplayParts: bank => ({ particulars: bank.description }), bankDraftSwitcher: () => '', bankDecisionLabel: () => '', ReconciliationStatus: { bank: () => '' } });
    ui.run(section('    function renderBankReconciliation(', '    function renderBankContinuityEvidence('));
    ui.context.renderBankReconciliation({ ...ui.context.bankReconView, draftId: 'TEST-DRAFT', summary: {}, statementSummary: {}, balanceResolved: true, canFinalizeTransactions: true });
    assert.match(ui.nodes.bs_table.innerHTML, /Automatic correction pending Owner finalization/);
    assert.ok(ui.nodes.bs_table.innerHTML.includes('Payment ₹500.00 → ' + money(500 + difference)));
    assert.ok(ui.nodes.bs_table.innerHTML.includes('Expense ₹500.00 → ' + money(500 + difference)));
    assert.match(ui.nodes.bs_table.innerHTML, /Original figures remain in the audit history/);
    assert.equal(ui.calls.length, 0, 'rendering does not resolve or post anything');
  }
});

test('eligible amount mismatch offers bank authority while blocked rows explain the lock without offering it', () => {
  const ui = harness(); ui.run(line('window.openBankReview=function(rowId){var x='));
  ui.context.openBankReview('bank-1');
  assert.match(ui.nodes.bankReviewActions.innerHTML, /Use bank amount &amp; date/);
  assert.match(ui.nodes.bankReviewSummary.innerHTML, /Expense ₹500.00 → ₹499.50/);
  delete ui.context.bankReconView.rows[0].bankAmountAuthority;
  ui.context.bankReconView.rows[0].bankAmountAuthorityBlocked = 'Payment already finalized <locked>';
  ui.context.openBankReview('bank-1');
  assert.doesNotMatch(ui.nodes.bankReviewActions.innerHTML, /Use bank amount/);
  assert.match(ui.nodes.bankReviewSummary.innerHTML, /Payment already finalized &lt;locked&gt;/);
});

test('manual unequal match previews and explicitly confirms exact payment/expense figures and bank date before staging', async () => {
  const ui = harness(row(499.5)); ui.run(section('    window.resolveBankRow=function', '    window.resolveBankBalance='));
  ui.context.resolveBankRow('bank-1', 'accept_match'); ui.context.resolveBankRow('bank-1', 'accept_match');
  await flush();
  assert.equal(ui.calls.length, 2, 'one read-only preview and one decision, not duplicate submissions');
  assert.equal(ui.calls[0].url, '/api/expenses/bank-statements/amount-authority-preview');
  assert.match(ui.confirms[0], /Payment: ₹500.00 → ₹499.50\nExpense: ₹500.00 → ₹499.50/);
  assert.match(ui.confirms[0], /Official bank date: 2099-10-19/);
  assert.match(ui.confirms[0], /posted only at Owner finalization/);
  assert.equal(ui.calls[1].body.action, 'accept_match');
  assert.equal(ui.calls[1].body.confirmBankAmount, true);
  assert.equal(ui.calls[1].body.bankAmountAuthorityFingerprint, 'verified-fingerprint');
  assert.match(ui.nodes.bs_msg.textContent, /correction staged pending Owner finalization/);
  assert.ok(ui.calls.every(call => !/finalize|payment/.test(call.url)), 'no financial posting endpoint is called');
});

test('cancellation, blocked preview, missing fingerprints and network errors cannot stage a correction', async () => {
  const scenarios = [
    { confirm: false },
    { preview: { success: true, bankAmountAuthorityBlocked: 'Already finalized: protected entry' } },
    { preview: { success: true, bankAmountAuthority: { eligible: true, originalAmount: 500, bankAmount: 499.5 } } },
    { networkError: true }
  ];
  for (const options of scenarios) {
    const ui = harness(row(), options); ui.run(section('    window.resolveBankRow=function', '    window.resolveBankBalance='));
    ui.context.resolveBankRow('bank-1', 'accept_match'); await flush();
    assert.equal(ui.calls.length, 1);
    assert.equal(ui.rendered.length, 0);
    if (options.confirm !== false) assert.ok(ui.nodes.bs_msg.textContent);
    assert.equal(Object.keys(ui.context.bankMatchReviewPending).length, 0, 'safe retry remains possible');
  }
});

test('equal amounts retain the existing date-authority decision without an amount-correction claim', async () => {
  const ui = harness(row(500)); ui.run(section('    window.resolveBankRow=function', '    window.resolveBankBalance='));
  ui.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0].url, '/api/expenses/bank-statements/resolve');
  assert.equal(ui.calls[0].body.confirmBankAmount, undefined);
  assert.equal(ui.confirms.length, 0);
  assert.match(ui.nodes.bs_msg.textContent, /official transaction date will be 2099-10-19/);
});

test('the exact existing known Axis fee pair uses its dedicated split route, not bank amount authority', async () => {
  const data = row(10005.9, 10000); data.axisTransferCharge = true; delete data.bankAmountAuthority;
  const ui = harness(data); ui.run(section('    window.resolveBankRow=function', '    window.resolveBankBalance='));
  ui.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0].url, '/api/expenses/bank-statements/resolve');
  assert.equal(ui.calls[0].body.action, 'accept_match');
  assert.equal(ui.calls[0].body.confirmBankAmount, undefined);
  assert.equal(ui.calls[0].body.bankAmountAuthorityFingerprint, undefined);
  assert.match(ui.confirms[0], /Bank total ₹10005.90 includes principal ₹10000.00 plus bank charge ₹5.90/);
  assert.match(ui.confirms[0], /Keep the expense\/payment principal unchanged/);
  assert.doesNotMatch(ui.confirms[0], /Expense:.*→|Payment:.*→/);
  const cancelled = harness(data, { confirm: false }); cancelled.run(section('    window.resolveBankRow=function', '    window.resolveBankBalance='));
  cancelled.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  assert.equal(cancelled.calls.length, 0);
});

test('a different manually selected candidate cannot inherit a known Axis fee bypass', async () => {
  const data = row(10005.9, 10000); data.axisTransferCharge = true; delete data.bankAmountAuthority;
  const ui = harness(data, { preview: { success: true, bankAmountAuthorityBlocked: 'Known Axis charge: use the dedicated principal and bank-charge route.' } });
  ui.context.bankReconView.linkCandidates.push({ id: 'OTHER/PAY-001', debit: 9000 });
  const result = await ui.context.prepareBankAmountAuthority({ draftId: 'TEST-DRAFT', rowId: 'bank-1', bankRowId: 'bank-1', appId: 'OTHER/PAY-001' }).catch(error => error);
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0].url, '/api/expenses/bank-statements/amount-authority-preview');
  assert.match(result.message, /dedicated principal/);
  assert.equal(ui.confirms.length, 0);
});

function separateAxisRows(fee, side, options = {}) {
  const principal = 10490, gross = Number((principal + fee).toFixed(2));
  const bank = { id: 'bank-6', status: 'missing_in_app', bank: { date: '2099-10-21', debit: gross, description: 'Axis IMPS transfer' } };
  const app = { id: 'app-6', status: 'missing_in_bank', app: { id: 'TR-00186', date: '2099-10-21', debit: principal, description: 'Transfer to IndusInd Bank 8181' } };
  const split = { bankRowId: bank.id, appId: app.app.id, principalAmount: principal, chargeAmount: fee, chargeCategory: 'BANK CHARGES', grossAmount: gross };
  const ui = harness(bank, { ...options, preview: { success: true, bankAmountAuthority: null, axisTransferChargeSplit: options.split || split } });
  ui.context.bankReconView.rows.push(app);
  ui.context.bankReconForm = { draftId: 'TEST-DRAFT', rowId: side === 'ledger' ? app.id : bank.id, mode: 'link', side };
  ui.context.el('brAppId').value = app.app.id;
  ui.context.el('brBankRow').value = bank.id;
  ui.context.el('brRemark').value = 'Confirmed existing transfer and the separate Axis IMPS fee';
  ui.run(line('function submitBankReconForm()'));
  return { ...ui, bank, app, split };
}

test('separate bank and ledger rows offer recognized ₹5.90 and ₹2.95 fee splits without rewriting the principal', async () => {
  for (const fee of [5.90, 2.95]) for (const side of ['bank', 'ledger']) {
    const ui = separateAxisRows(fee, side);
    ui.context.submitBankReconForm(); await flush();
    assert.equal(ui.calls.length, 2, 'only a preview and a staged split decision');
    assert.equal(ui.calls[0].url, '/api/expenses/bank-statements/amount-authority-preview');
    assert.equal(ui.calls[0].body.rowId, side === 'ledger' ? 'app-6' : 'bank-6');
    assert.equal(ui.calls[0].body.bankRowId, 'bank-6');
    assert.equal(ui.calls[0].body.appId, 'TR-00186');
    assert.match(ui.confirms[0], /TR-00186: principal ₹10490.00 stays unchanged/);
    assert.ok(ui.confirms[0].includes('Bank Charges ' + money(fee)));
    assert.match(ui.confirms[0], /posting still requires Owner finalization/);
    const posted = ui.calls[1].body;
    assert.equal(posted.action, 'split_allocation');
    assert.equal(posted.rowId, 'bank-6');
    assert.equal(posted.appId, 'TR-00186');
    assert.equal(posted.principalAmount, 10490);
    assert.equal(posted.chargeAmount, fee);
    assert.equal(posted.chargeCategory, 'BANK CHARGES');
    assert.equal(posted.confirmBankAmount, undefined);
    assert.equal(posted.bankAmountAuthorityFingerprint, undefined);
    assert.equal(ui.app.app.debit, 10490);
    assert.equal(ui.bank.bank.debit, Number((10490 + fee).toFixed(2)));
    assert.ok(ui.calls.every(call => !/finalize|payments|transfers/.test(call.url)));
  }
});

test('cancelling a recognized fee split does not save any reconciliation decision', async () => {
  for (const side of ['bank', 'ledger']) {
    const ui = separateAxisRows(5.9, side, { confirm: false });
    ui.context.submitBankReconForm(); await flush();
    assert.equal(ui.calls.length, 1);
    assert.equal(ui.rendered.length, 0);
    assert.equal(ui.nodes.brSave.disabled, false);
  }
});

test('malformed or stale recognized fee previews fail closed before confirmation or save', async () => {
  const changes = [
    { bankRowId: 'bank-other' }, { appId: 'TR-other' }, { principalAmount: 10495.9 },
    { grossAmount: 10490 }, { chargeAmount: 2.95 }, { chargeAmount: -5.9 },
    { principalAmount: '10490' }, { chargeCategory: 'PAYTM CHARGES' },
    { chargeAmount: 5.901 }, { grossAmount: null }
  ];
  for (const change of changes) {
    const baseline = separateAxisRows(5.9, 'ledger');
    const ui = separateAxisRows(5.9, 'ledger', { split: { ...baseline.split, ...change } });
    ui.context.submitBankReconForm(); await flush();
    assert.equal(ui.calls.length, 1);
    assert.equal(ui.confirms.length, 0);
    assert.equal(ui.rendered.length, 0);
    assert.match(ui.nodes.brMsg.textContent, /preview no longer matches/);
    assert.equal(ui.nodes.brSave.disabled, false);
  }
  for (const change of [value => { value.bank.resolution = { action: 'exclude' }; }, value => { value.bank.bankClaimedElsewhere = true; }, value => { value.bank.status = 'already_reconciled'; }, value => { value.bank.bank.credit = value.bank.bank.debit; }, value => { value.app.app.debit = 10491; }, value => { value.app.app.credit = 10490; }, value => { value.app.ledgerClaimedElsewhere = true; }, value => { value.app.resolution = { action: 'linked_split' }; }, value => { value.app.status = 'matched'; }]) {
    const ui = separateAxisRows(5.9, 'bank'); change(ui);
    ui.context.submitBankReconForm(); await flush();
    assert.equal(ui.calls.length, 1);
    assert.equal(ui.confirms.length, 0);
    assert.equal(ui.rendered.length, 0);
    assert.match(ui.nodes.brMsg.textContent, /preview no longer matches/);
  }
});

test('unreviewed debit rows expose the manual bank-charge split directly without changing any records', () => {
  for (const values of [{ debit: 10495.9 }, { credit: 10495.9 }, { debit: 10495.9, resolved: true }, { debit: 10495.9, claimed: true }]) {
    const data = row(10495.9, 10490); data.status = 'missing_in_app'; delete data.app;
    data.bank = { date: '2099-10-21', debit: values.debit, credit: values.credit };
    if (values.resolved) data.resolution = { action: 'exclude' };
    if (values.claimed) data.bankClaimedElsewhere = true;
    const ui = harness(data, { preview: {} }); ui.context.openBankReview = () => {};
    ui.run(section('    var openBankReviewWithMatches=', '    window.uploadVelocityRemittanceForBank='));
    ui.context.openBankReview('bank-1');
    assert.equal(/Link existing entry \+ bank charge/.test(ui.nodes.bankReviewActions.innerHTML), !!values.debit && !values.resolved && !values.claimed);
    assert.equal(ui.calls.length, 0);
  }
});

test('manual split selection fills the existing principal and remainder for explicit review without assuming a fee', () => {
  const ui = separateAxisRows(5.9, 'bank');
  ui.context.bankReconForm.mode = 'assign'; ui.context.el('brKind').value = 'split';
  ui.context.bankAppCandidates = () => [{ id: 'TR-00186', amount: 10490 }, { id: 'TR-OTHER', amount: 10000 }, { id: 'TR-TOO-LARGE', amount: 11000 }];
  ui.context.renderBankAppCandidates = () => {};
  ui.run(line('window.toggleBankAppCandidate=function'));
  for (const [id, principal, charge] of [['TR-00186', 10490, 5.9], ['TR-OTHER', 10000, 495.9], ['TR-TOO-LARGE', 11000, ''], ['UNKNOWN', '', '']]) {
    ui.context.toggleBankAppCandidate(id);
    assert.equal(ui.nodes.brPrincipal.value, principal);
    assert.equal(ui.nodes.brCharge.value, charge);
    assert.match(ui.nodes.brMsg.textContent, /confirm the difference is a real bank charge before saving/);
    assert.equal(ui.calls.length, 0);
    assert.equal(ui.confirms.length, 0);
  }
});

test('bank-side one-to-one link supports any selected amount only after reason and explicit server-verified confirmation', async () => {
  const ui = harness(row(800, 500));
  ui.context.bankReconForm = { draftId: 'TEST-DRAFT', rowId: 'bank-1', mode: 'link', side: 'bank' };
  ui.context.el('brAppId').value = 'EX-001/PAY-001';
  ui.run(line('function submitBankReconForm()'));
  ui.context.submitBankReconForm();
  assert.equal(ui.calls.length, 0); assert.match(ui.nodes.brMsg.textContent, /remark/);
  ui.context.el('brRemark').value = 'Verified this exact vendor payment';
  ui.context.submitBankReconForm(); await flush();
  assert.equal(ui.calls.length, 2);
  assert.match(ui.confirms[0], /Expense: ₹500.00 → ₹800.00/);
  assert.equal(ui.calls[1].body.action, 'link_existing');
  assert.equal(ui.calls[1].body.reason, 'Verified this exact vendor payment');
  assert.equal(ui.calls[1].body.confirmBankAmount, true);
});

test('ledger-side bank picker admits different amounts but excludes opposite directions and finalized bank rows', async () => {
  const data = row(999, 500), ui = harness(data);
  const ledger = { id: 'app-1', status: 'missing_in_bank', app: data.app };
  ui.context.bankReconView.rows.push(ledger, { id: 'credit-bank', status: 'missing_in_app', bank: { date: '2099-10-19', credit: 500 } }, { id: 'finalized-bank', status: 'resolved', bank: { date: '2099-10-19', debit: 500 }, resolution: {} });
  Object.assign(ui.context, { bankRowNature: () => 'SANKI', categorySuggestions: () => '', transferCounterpartOptions: () => '', ownTransferOptions: () => '', ownTransferAccount: () => ({ account: '' }), bankTransferOptions: () => '', bankTransferCounterpart: () => ({ account: '' }), bankTransferNature: () => 'SANKI', updateBankReconFields() {}, opts: () => '' });
  ui.run(line('function showBankReconForm(form)'));
  ui.context.showBankReconForm({ draftId: 'TEST-DRAFT', rowId: 'app-1', mode: 'link', side: 'ledger' });
  assert.match(ui.nodes.brBankRow.innerHTML, /bank-1.*₹999.00/);
  assert.doesNotMatch(ui.nodes.brBankRow.innerHTML, /credit-bank|finalized-bank/);
  ui.context.el('brBankRow').value = 'bank-1'; ui.context.el('brRemark').value = 'Verified same payment in original bank evidence';
  ui.run(line('function submitBankReconForm()')); ui.context.submitBankReconForm(); await flush();
  assert.equal(ui.calls[0].body.appId, 'EX-001/PAY-001');
  assert.equal(ui.calls[0].body.bankRowId, 'bank-1');
  assert.equal(ui.calls[1].body.action, 'link_existing');
  assert.equal(ui.calls[1].body.confirmBankAmount, true);
});

test('eligible differences up to one rupee cannot become bank-charge quick matches; larger existing charge actions remain', () => {
  for (const difference of [.5, 1, 2]) {
    const ui = harness(row(500 + difference)); ui.context.openBankReview = () => {};
    ui.run(section('    var openBankReviewWithMatches=', '    window.uploadVelocityRemittanceForBank='));
    ui.context.openBankReview('bank-1');
    assert.equal(/bank charge/.test(ui.nodes.bankReviewActions?.innerHTML || ''), difference > 1);
    ui.run(line('window.matchSmallBankDifference=function'));
    if (difference <= 1) { ui.context.matchSmallBankDifference('bank-1'); assert.match(ui.messages[0], /must not create a BANK CHARGES entry/); assert.equal(ui.calls.length, 0); }
  }
});

test('unsupported positive differences up to one rupee cannot fall back to the old bank-charge shortcut', () => {
  for (const difference of [.5, 1, 2.95]) {
    const data = row(500 + difference); delete data.bankAmountAuthority;
    data.bankAmountAuthorityBlocked = 'This batched payment requires its dedicated allocation workflow.';
    const ui = harness(data); ui.context.openBankReview = () => {};
    ui.run(section('    var openBankReviewWithMatches=', '    window.uploadVelocityRemittanceForBank='));
    ui.context.openBankReview('bank-1');
    assert.equal(/bank charge/.test(ui.nodes.bankReviewActions?.innerHTML || ''), difference > 1);
    if (difference <= 1) {
      ui.run(line('window.matchSmallBankDifference=function'));
      ui.context.matchSmallBankDifference('bank-1');
      assert.match(ui.messages[0], /otherwise review the protected entry or dedicated allocation/);
      assert.equal(ui.calls.length, 0);
    }
  }
});

test('one-to-one form refuses multiple typed entries and all inline scripts still compile', () => {
  const ui = harness(); ui.context.bankReconForm = { draftId: 'TEST-DRAFT', rowId: 'bank-1', mode: 'link', side: 'bank' };
  ui.context.el('brAppId').value = 'EX-001/PAY-001,EX-002/PAY-001'; ui.context.el('brRemark').value = 'Keep the selected allocations separate';
  ui.run(line('function submitBankReconForm()')); ui.context.submitBankReconForm();
  assert.match(ui.nodes.brMsg.textContent, /exactly one existing ledger entry/);
  assert.equal(ui.calls.length, 0);
  for (const [, script] of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) if (script.trim()) assert.doesNotThrow(() => new vm.Script(script));
});
