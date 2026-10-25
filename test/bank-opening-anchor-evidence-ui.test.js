'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
function section(from, to) { const start = html.indexOf(from), end = html.indexOf(to, start); assert.ok(start >= 0 && end > start, from); return html.slice(start, end); }
function line(marker) { const found = html.split('\n').find(value => value.includes(marker)); assert.ok(found, marker); return found; }
const conflict = { type: 'ambiguous_opening_anchor', cutoffDate: '2026-10-18', message: 'Finalized statements contain different closing balances for the same opening-anchor date.', bankRowIds: [] };
function view(extra = {}) {
  return { draftId: 'SYNTHETIC-ANCHOR-DRAFT', account: 'Axis Bank 3448', nature: 'SANKI',
    statementSummary: { from: '2026-10-19', to: '2026-10-21', openingBalance: 165673.07, closingBalance: 249538.20, totalCredits: 100000, totalDebits: 16134.87, validated: true },
    rows: [{ id: 'bank-1', status: 'missing_in_app', bank: { date: '2026-10-21', debit: 1000, description: 'Reviewable bank entry' } }], summary: {}, unresolved: 1,
    openingSource: 'ledger_history', openingAnchorSource: 'unavailable', accountingFrom: '2026-10-19', currentOpening: 499454.53, proposedOpening: 499454.53,
    postedPeriodNet: 85894.63, pendingPostingNet: -11.8, ledgerClosing: 585337.36, bankClosing: 249538.20, balanceDifference: -335799.16,
    balanceResolved: false, reconciliationConflicts: [conflict], canFinalizeTransactions: false, ...extra };
}
function harness() {
  const nodes = {}, calls = [], openedForms = [], confirms = [];
  function node() {
    let markup = '', buttons = [];
    return { dataset: {}, style: {}, disabled: false, textContent: '', value: '',
      get innerHTML() { return markup; },
      set innerHTML(value) { markup = value; buttons = Array.from(value.matchAll(/<button\b([^>]*)>([\s\S]*?)<\/button>/gi), match => ({ textContent: match[2].replace(/<[^>]*>/g, ''), disabled: /\bdisabled(?:\s|=|$)/i.test(match[1]), title: '' })); },
      insertAdjacentHTML(where, value) { assert.equal(where, 'afterbegin'); this.innerHTML = value + markup; },
      querySelectorAll(selector) { assert.equal(selector, 'button'); return buttons; }
    };
  }
  const context = { el: id => nodes[id] ||= node(), fmt: value => '₹' + Number(value || 0).toFixed(2), bankNature: 'SANKI', bankStatementLoadGeneration: 0,
    bankDraftId: '', bankReconView: null, cfg: { isOwner: true }, SankiNavigation: { set() {} }, ReconciliationStatus: { bank: () => '' }, bankDraftSwitcher: () => '',
    bankAmountAuthorityHtml: () => '', bankDisplayParts: bank => ({ particulars: bank.description }),
    bankReconRow: id => context.bankReconView.rows.find(row => row.id === id), bankReconAmount: row => Number(row?.bank?.debit || row?.bank?.credit || 0),
    showBankReconForm: form => openedForms.push(form), saveBankReconForm() {}, confirm: text => { confirms.push(text); return true; },
    api: (url, request) => { calls.push({ url, request }); return Promise.resolve({ success: true }); } };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(line('var esc = function(') + '\n' + section('    function renderBankReconciliation(', '    var personalHistoricalSelection='), context);
  return { context, nodes, calls, openedForms, confirms, run: source => vm.runInContext(source, context), render(data) { context.renderBankReconciliation(data); context.renderBankContinuityEvidence(data); return nodes.bs_table.innerHTML; } };
}

test('ambiguous anchor preserves statement figures and labels full-history fallback as unverified diagnostics', () => {
  const ui = harness(), data = view(), before = JSON.stringify(data), output = ui.render(data);
  assert.match(output, /Bank-anchored balance comparison unavailable/);
  assert.match(output, /Uploaded statement opening <b>₹165673\.07<\/b> · Uploaded statement closing <b>₹249538\.20<\/b> — retained unchanged/);
  assert.match(output, /Unverified diagnostic ledger opening <b>₹499454\.53/);
  assert.match(output, /Unverified diagnostic ledger closing <b>₹585337\.36/);
  assert.match(output, /Diagnostic reported gap <b>₹-335799\.16<\/b> — not a verified reconciliation difference/);
  assert.match(output, /<details class="claim-card"><summary><b>Unverified full-history ledger arithmetic/);
  assert.equal((output.match(/₹499454\.53/g) || []).length, 1, 'fallback opening is shown once, inside collapsed diagnostics');
  assert.equal((output.match(/₹585337\.36/g) || []).length, 1, 'fallback closing is shown once, inside collapsed diagnostics');
  assert.equal((output.match(/₹-335799\.16/g) || []).length, 1, 'unverified full-history gap is not repeated in the headline');
  assert.match(output, /Finalized statements contain different closing balances/);
  assert.doesNotMatch(output, /Ledger closing:|✓ balances agree|this comparison uses the statement-end bank balance|= preview closing/);
  assert.match(output, /openBankReview\('bank-1'\)/, 'individual transaction review remains available');
  assert.equal(JSON.stringify(data), before, 'rendering never replaces any report value');
  assert.equal(ui.calls.length, 0);
});

test('validated full-period statement-opening comparison isolates the period gap without selecting an anchor', () => {
  const ui = harness(), data = view(), output = ui.render(data);
  assert.match(output, /Statement-opening comparison — diagnostic only/);
  assert.match(output, /Uploaded statement opening ₹165673\.07 \+ recorded period net ₹85894\.63 \+ pending net ₹-11\.80 = diagnostic period closing <b>₹251555\.90/);
  assert.match(output, /Diagnostic period gap <b>₹-2017\.70/);
  assert.match(output, /does not select a historical anchor or change the account opening/);
  assert.equal(data.ledgerClosing, 585337.36);
  assert.equal(data.balanceDifference, -335799.16);
  assert.equal(ui.nodes.bs_finalize.disabled, true);
});

test('missing inputs, unvalidated statements and mismatched period or bank basis cannot fabricate a diagnostic zero', () => {
  for (const change of [data => { data.statementSummary.validated = false; }, data => { data.pendingPostingNet = null; }, data => { data.postedPeriodNet = ''; }, data => { data.statementSummary.openingBalance = undefined; }, data => { data.statementSummary.closingBalance = Infinity; }, data => { data.accountingFrom = '2026-10-20'; }, data => { data.companyAdjustedBankTruth = true; }]) {
    const ui = harness(), data = view(); change(data);
    assert.doesNotMatch(ui.render(data), /Statement-opening comparison — diagnostic only/);
    assert.equal(ui.nodes.bs_finalize.disabled, true);
  }
  const ui = harness(), output = ui.render(view({ currentOpening: null, proposedOpening: undefined, ledgerClosing: null, balanceDifference: null }));
  assert.match(output, /Unverified diagnostic ledger opening <b>Unavailable<\/b>/);
  assert.match(output, /Unverified diagnostic ledger closing <b>Unavailable<\/b>/);
  assert.match(output, /Diagnostic reported gap <b>Unavailable<\/b>/);
  assert.doesNotMatch(output, /Diagnostic reported gap <b>₹0\.00/);
});

test('evidence link is encoded read-only GET for the exact current draft and is never requested by rendering', () => {
  const ui = harness(), data = view({ account: 'Axis Bank 3448 & <legacy>', draftId: 'SYNTHETIC & "draft"' }), output = ui.render(data);
  assert.match(output, /target="_blank" rel="noopener" href="\/api\/expenses\/bank-opening-anchor-evidence\?nature=SANKI&amp;account=Axis%20Bank%203448%20%26%20%3Clegacy%3E&amp;draftId=SYNTHETIC%20%26%20%22draft%22"/);
  assert.match(output, /Read-only historical evidence; balances and decisions are not changed/);
  assert.equal(ui.calls.length, 0);
  assert.equal(ui.context.bankOpeningEvidenceLink({ ...data }), '', 'a stale or unrelated report cannot produce an evidence link');
  ui.context.bankDraftId = 'DIFFERENT'; assert.equal(ui.context.bankOpeningEvidenceLink(data), '');
  ui.context.bankDraftId = data.draftId; data.nature = 'UNAUTHORIZED'; assert.equal(ui.context.bankOpeningEvidenceLink(data), '');
});

test('anchor conflicts disable finalization even with accidental balance equality or pending Owner approval', () => {
  for (const marker of ['reconciliationConflicts', 'anchorConflict', 'openingAnchorConflict']) {
    const data = view({ reconciliationConflicts: [], unresolved: 0, balanceResolved: true, canFinalizeTransactions: true, finalizationApproval: { status: 'pending', requestedBy: 'test' } });
    data[marker] = marker === 'reconciliationConflicts' ? [conflict] : conflict;
    const ui = harness(); ui.context.el('bs_finalize').dataset.confirmPending = '1';
    const output = ui.render(data);
    assert.equal(ui.nodes.bs_finalize.disabled, true);
    assert.match(ui.nodes.bs_finalize.textContent, /Historical opening evidence required/);
    assert.equal(ui.nodes.bs_finalize.dataset.confirmPending, undefined);
    assert.doesNotMatch(output, /✓ balances agree|onclick="resolveBankBalance/);
    ui.run(line('function finalizeBankStatement()'));
    ui.context.finalizeBankStatement();
    assert.match(ui.nodes.bs_msg.textContent, /Finalization blocked/);
    assert.equal(ui.confirms.length, 0);
    assert.equal(ui.calls.length, 0);
  }
});

test('opening-adjustment entry and a previously open adjustment form cannot use diagnostic amounts', () => {
  const ui = harness(); ui.render(view());
  ui.run(line('window.resolveBankBalance=function'));
  ui.context.resolveBankBalance(165673.07);
  assert.equal(ui.openedForms.length, 0);
  assert.match(ui.nodes.bs_msg.textContent, /Diagnostic arithmetic cannot be used as a verified opening/);
  ui.context.bankReconForm = { draftId: ui.context.bankDraftId, rowId: 'bank-1', mode: 'balance', amount: 165673.07 };
  ui.context.el('brRemark').value = 'Attempting to clear the unverified diagnostic difference';
  ui.run(line('function submitBankReconForm()'));
  ui.context.submitBankReconForm();
  assert.match(ui.nodes.brMsg.textContent, /Opening adjustment blocked/);
  assert.equal(ui.calls.length, 0);
});

test('normal verified anchor and ordinary transaction-conflict paths keep their original comparisons', () => {
  for (const conflicts of [[], [{ type: 'duplicate_pending_transfer', message: 'Transfer already exists', bankRowIds: ['bank-1'] }]]) {
    const ui = harness(), output = ui.render(view({ reconciliationConflicts: conflicts, openingSource: 'previous_reconciliation', openingBaselineDate: '2026-10-18' }));
    assert.match(output, /Verified closing at 2026-10-18 plus uncovered carry-forward/);
    assert.match(output, /Ledger closing: <b>₹585337\.36/);
    assert.doesNotMatch(output, /Bank-anchored balance comparison unavailable|Statement-opening comparison — diagnostic only/);
  }
});

test('anchor conflict and diagnostic carry-forward evidence are escaped, not suppressed or relabeled as verified', () => {
  const attack = '<img src=x onerror="alert(1)">', ui = harness();
  const output = ui.render(view({ reconciliationConflicts: [{ ...conflict, message: attack, bankRowIds: [attack] }],
    carryForwardMovements: [{ id: attack, date: attack, description: attack, debit: 5.9 }], openingContinuityGap: -333781.46 }));
  assert.doesNotMatch(output, /<img\b|onerror="alert/);
  assert.match(output, /&lt;img src=x onerror=&quot;alert\(1\)&quot;&gt;/);
  assert.match(output, /Diagnostic carry-forward evidence — opening unverified/);
  assert.match(output, /Diagnostic opening gap — unverified ₹-333781\.46/);
  assert.doesNotMatch(output, /Previous saved bank closing ₹0\.00/);
});
