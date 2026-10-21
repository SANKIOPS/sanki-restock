'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');

function section(from, to) {
  const start = html.indexOf(from), end = html.indexOf(to, start);
  assert.ok(start >= 0 && end > start, from);
  return html.slice(start, end);
}

function harness() {
  const nodes = {};
  function node() {
    let markup = '', buttons = [];
    return { dataset: {}, style: {}, disabled: false, textContent: '',
      get innerHTML() { return markup; },
      set innerHTML(value) {
        markup = value;
        buttons = Array.from(value.matchAll(/<button\b([^>]*)>([\s\S]*?)<\/button>/gi), match => ({
          textContent: match[2].replace(/<[^>]*>/g, ''), disabled: /\bdisabled(?:\s|=|$)/i.test(match[1]), title: ''
        }));
      },
      insertAdjacentHTML(where, value) { assert.equal(where, 'afterbegin'); this.innerHTML = value + markup; },
      querySelectorAll(selector) { assert.equal(selector, 'button'); return buttons; }
    };
  }
  const context = { el: id => nodes[id] ||= node(), fmt: value => '₹' + Number(value || 0).toFixed(2),
    bankStatementLoadGeneration: 0, bankDraftId: '', bankReconView: null, cfg: { isOwner: true },
    SankiNavigation: { set() {} }, ReconciliationStatus: { bank: () => '' }, bankDraftSwitcher: () => '' };
  vm.createContext(context);
  const escapeSource = html.split('\n').find(line => line.includes('var esc = function('));
  assert.ok(escapeSource, 'use the page escape function, not a test replacement');
  vm.runInContext(escapeSource + '\n' + section('    function renderBankReconciliation(',
    '    var personalHistoricalSelection='), context);
  return { nodes, render(data) { context.renderBankReconciliation(data); context.renderBankContinuityEvidence(data); return nodes.bs_table; } };
}

function view(extra = {}) {
  return { draftId: 'UI-OPENING', statementSummary: { from: '2026-10-18', to: '2026-10-18' },
    openingSource: 'ledger_history', accountingFrom: '2026-10-18', currentOpening: 234515.82,
    proposedOpening: 234515.82, postedPeriodNet: 3250, pendingPostingNet: -72092.75,
    ledgerClosing: 165673.07, bankClosing: 165673.07, balanceDifference: 0, balanceResolved: true,
    rows: [], summary: {}, unresolved: 0, reconciliationConflicts: [], canFinalizeTransactions: true, ...extra };
}

test('balance UI scripts compile and show statement date, opening, posted net and pending net separately', () => {
  let compiled = 0;
  for (const [index, script] of Array.from(html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)).entries()) {
    if (/\bsrc\s*=|\btype\s*=\s*["'](?:application\/json|importmap)["']/i.test(script[1]) || !script[2].trim()) continue;
    new vm.Script(script[2], { filename: 'expenses-inline-' + index + '.js' });
    compiled++;
  }
  assert.ok(compiled > 0);
  const table = harness().render(view());
  assert.match(table.innerHTML, /Balance calculation — as of 2026-10-18/);
  assert.match(table.innerHTML, /Account opening plus recorded activity before 2026-10-18/);
  assert.match(table.innerHTML, /Opening ₹234515\.82 \+ posted period movement ₹3250\.00 \+ unposted reviewed transfers \/ charges ₹-72092\.75 = preview closing <b>₹165673\.07/);
  assert.match(table.innerHTML, /account card may use a later date and deduct blocked funds/);
  const anchored = harness().render(view({ openingSource: 'previous_reconciliation', openingBaselineDate: '2026-10-17' }));
  assert.match(anchored.innerHTML, /Verified closing at 2026-10-17 plus uncovered carry-forward/);
});

test('balance dates, carried movement details and conflict evidence are escaped as text', () => {
  const attack = '<img src=x onerror="alert(1)">';
  const table = harness().render(view({ statementSummary: { from: attack, to: attack }, accountingFrom: attack,
    carryForwardMovements: [{ id: attack, date: attack, description: attack, debit: 5.9 }],
    reconciliationConflicts: [{ type: 'duplicate_pending_transfer', message: attack, bankRowIds: [attack] }] }));
  assert.doesNotMatch(table.innerHTML, /<img\b|onerror="alert/);
  assert.match(table.innerHTML, /&lt;img src=x onerror=&quot;alert\(1\)&quot;&gt;/);
});

test('a stale opening decision remains reviewable even when the corrected calculation already balances', () => {
  for (const balanceResolved of [false, true]) {
    const table = harness().render(view({ balanceResolved, openingResolutionStale: true,
      openingResolution: { amount: 234515.82, previousAmount: 47844.86 }, unresolved: 1,
      reconciliationConflicts: [{ type: 'stale_opening_resolution', message: 'Review the recalculated opening', bankRowIds: [] }] }));
    const button = table.querySelectorAll('button').find(value => /Resolve opening/.test(value.textContent));
    assert.ok(button, 'the stale decision needs an opening-review action even if balanceResolved=' + balanceResolved);
    assert.equal(button.disabled, false);
    assert.doesNotMatch(table.innerHTML, /Reviewed opening:/);
    assert.match(table.innerHTML, /replace the stale decision/);
  }
});

test('transaction conflicts still disable the opening action even when a stale opening also needs review', () => {
  const table = harness().render(view({ balanceResolved: false, openingResolutionStale: true, unresolved: 2,
    reconciliationConflicts: [{ type: 'stale_opening_resolution', message: 'Stale opening' },
      { type: 'duplicate_pending_transfer', message: 'Transfer exists', bankRowIds: ['bank-1'] }] }));
  const button = table.querySelectorAll('button').find(value => /Resolve opening/.test(value.textContent));
  assert.ok(button);
  assert.equal(button.disabled, true);
  assert.match(button.title, /duplicate transaction decisions first/);
  assert.match(table.innerHTML, /No opening adjustment can clear transaction conflicts/);
});
