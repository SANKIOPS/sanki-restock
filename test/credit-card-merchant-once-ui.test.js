'use strict';
// Synthetic statement DOM only. No stored app data, network, browser, or accounting writes.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const review = require('../public/credit-card-review');
const fields = ['classification', 'type', 'category', 'nature', 'channel'];
const selectors = { classification: '.cc_class', type: '.cc_type', category: '.cc_category', nature: '.cc_nature', channel: '.cc_channel', merchant: '.cc_merchant' };
const clone = value => JSON.parse(JSON.stringify(value));
const fmt = value => 'INR ' + value;
let sequence = 0;
function row(overrides = {}) {
  return Object.assign({ id: 'ROW-' + (++sequence), date: '2026-10-01', postingDate: '2026-10-01', narration: 'FACEBOOK synthetic', merchant: 'Facebook', debit: 100, credit: 0, amount: 100, classification: 'expense', type: '', category: '', nature: '', channel: '', confirmed: false }, overrides);
}
function domRow(original, index, statement) {
  const tr = { dataset: { id: original.id }, isConnected: true, fields: {}, querySelector(selector) { return this.fields[selector] || null; } };
  const decode = value => String(value).replace(/&(amp|lt|gt|quot|#39);/g, (_, key) => ({ amp: '&', lt: '<', gt: '>', quot: '"', '#39': "'" }[key]));
  function classes(attributes) { const match = attributes.match(/class="([^"]+)"/); return match ? match[1].split(/\s+/) : []; }
  let markup = '';
  Object.defineProperty(tr, 'innerHTML', { get() { return markup; }, set(value) {
    markup = value; this.fields = {};
    for (const match of value.matchAll(/<input\b([^>]*)>/g)) {
      const node = { value: decode((match[1].match(/value="([^"]*)"/) || [])[1] || ''), disabled: /\bdisabled\b/.test(match[1]), checked: /\bchecked\b/.test(match[1]), dataset: {} };
      classes(match[1]).forEach(cls => { this.fields['.' + cls] = node; });
    }
    for (const match of value.matchAll(/<select\b([^>]*)>([\s\S]*?)<\/select>/g)) {
      const options = [...match[2].matchAll(/<option\b([^>]*)>/g)], selected = options.find(option => /\bselected\b/.test(option[1])) || options[0];
      const node = { value: selected ? decode((selected[1].match(/value="([^"]*)"/) || [])[1] || '') : '', disabled: /\bdisabled\b/.test(match[1]), dataset: {} };
      classes(match[1]).forEach(cls => { this.fields['.' + cls] = node; });
    }
  } });
  tr.innerHTML = review.rowCells(original, index, statement, ['SANKI', 'SAMAST', 'PERSONAL'], fmt);
  return tr;
}
function setup(rows = [row(), row(), row()], options = {}) {
  const statement = { id: options.id || 'CCS-SYNTHETIC-ONCE', cardId: 'CARD-SYNTHETIC', status: options.status || 'review', reviewRevision: options.revision || 0, rows };
  let dirty = 0;
  const nodes = rows.map((original, index) => {
    const tr = domRow(original, index, statement);
    review.bindRow(tr, original, index, statement, ['SANKI', 'SAMAST', 'PERSONAL'], fmt, () => { dirty++; });
    return tr;
  });
  review.bindStatement(nodes, statement, { markDirty: () => { dirty++; }, isBusy: options.isBusy || (() => false) });
  return { statement, nodes, get dirty() { return dirty; } };
}
function change(tr, field, value) {
  const node = tr.querySelector(selectors[field]); assert.ok(node, field + ' exists'); node.value = value;
  if (node.onchange) node.onchange.call(node, { target: node });
}
function selected(tr, field) { const node = tr.querySelector(selectors[field]); return node && node.value; }
function payload(tr) { return review.collectRow(tr, tr._ccReviewDraft); }

test('first Facebook entry fills each selected expense field once across exact normalized same-statement merchants', () => {
  const ui = setup([row({ merchant: ' Facebook ' }), row({ merchant: 'facebook' }), row({ merchant: 'FACEBOOK' }), row({ merchant: 'Facebook Ads' }), row({ merchant: 'Face book' })]);
  const before = clone(ui.statement);
  const choices = { classification: 'expense', type: 'marketing', category: 'IG Meta', nature: 'SANKI', channel: 'Both' };
  for (const [field, value] of Object.entries(choices)) {
    change(ui.nodes[0], field, value);
    for (const tr of ui.nodes.slice(0, 3)) assert.equal(selected(tr, field), value, field);
  }
  assert.equal(selected(ui.nodes[3], 'category'), ''); assert.equal(selected(ui.nodes[4], 'category'), '');
  for (const tr of ui.nodes.slice(0, 3)) assert.deepEqual(payload(tr).merchantAutofill, { version: 1, touchedFields: fields });
  assert.ok(ui.dirty > 0); assert.deepEqual(ui.statement, before, 'saved server snapshot is not mutated');
});

test('second edits of the first merchant row and all later-row edits stay local', () => {
  const ui = setup();
  for (const [field, value] of Object.entries({ classification: 'fee', type: 'marketing', category: 'IG Meta', nature: 'SANKI', channel: 'Both' })) change(ui.nodes[0], field, value);
  for (const [field, value] of Object.entries({ classification: 'emi_interest', type: 'variable', category: 'Local source change', nature: 'SAMAST', channel: 'POS' })) change(ui.nodes[0], field, value);
  assert.equal(selected(ui.nodes[1], 'classification'), 'fee'); assert.equal(selected(ui.nodes[2], 'category'), 'IG Meta');
  change(ui.nodes[1], 'category', 'Local target change'); change(ui.nodes[1], 'nature', 'PERSONAL');
  assert.equal(selected(ui.nodes[0], 'category'), 'Local source change'); assert.equal(selected(ui.nodes[2], 'category'), 'IG Meta');
  assert.equal(selected(ui.nodes[2], 'nature'), 'SANKI');
});

test('a later-row manual override, including an explicit blank, is not overwritten by the first fill', () => {
  const ui = setup();
  change(ui.nodes[1], 'category', ''); change(ui.nodes[1], 'nature', 'SAMAST');
  change(ui.nodes[0], 'category', 'IG Meta'); change(ui.nodes[0], 'nature', 'SANKI');
  assert.equal(selected(ui.nodes[1], 'category'), ''); assert.equal(selected(ui.nodes[1], 'nature'), 'SAMAST');
  assert.equal(selected(ui.nodes[2], 'category'), 'IG Meta'); assert.equal(selected(ui.nodes[2], 'nature'), 'SANKI');
  assert.deepEqual(payload(ui.nodes[1]).merchantAutofill.touchedFields, ['category', 'nature']);
});

test('partly typed category commits on change, not input, and uncommitted target text is preserved', () => {
  const ui = setup(), source = ui.nodes[0].querySelector('.cc_category');
  source.value = 'I'; assert.equal(source.oninput, undefined); assert.equal(selected(ui.nodes[1], 'category'), '');
  ui.nodes[1].querySelector('.cc_category').value = 'Local unsaved target';
  change(ui.nodes[0], 'category', 'IG Meta');
  assert.equal(selected(ui.nodes[1], 'category'), 'Local unsaved target'); assert.equal(selected(ui.nodes[2], 'category'), 'IG Meta');
  assert.ok(payload(ui.nodes[1]).merchantAutofill.touchedFields.includes('category'));
});

test('saved flags survive save/reload and do not rearm source or target fields', () => {
  const ui = setup(); change(ui.nodes[0], 'category', 'IG Meta'); change(ui.nodes[1], 'category', 'Local target');
  const saved = ui.statement.rows.map((original, index) => ({ ...original, ...payload(ui.nodes[index]), confirmed: true }));
  const reloaded = setup(saved, { revision: 1 });
  change(reloaded.nodes[0], 'category', 'New source value');
  assert.equal(selected(reloaded.nodes[1], 'category'), 'Local target'); assert.equal(selected(reloaded.nodes[2], 'category'), 'IG Meta');
  change(reloaded.nodes[0], 'nature', 'SANKI');
  assert.equal(selected(reloaded.nodes[1], 'nature'), 'SANKI', 'previously untouched blank fields still fill once');
  change(reloaded.nodes[0], 'nature', 'SAMAST'); assert.equal(selected(reloaded.nodes[2], 'nature'), 'SANKI');
});

test('classification redraw preserves all flags and does not rearm a previously seeded field', () => {
  const ui = setup(); change(ui.nodes[0], 'category', 'IG Meta'); change(ui.nodes[0], 'classification', 'fee');
  assert.equal(selected(ui.nodes[1], 'classification'), 'fee'); assert.equal(selected(ui.nodes[1], 'category'), 'IG Meta');
  change(ui.nodes[0], 'category', 'After source redraw'); assert.equal(selected(ui.nodes[1], 'category'), 'IG Meta');
  change(ui.nodes[1], 'classification', 'expense'); change(ui.nodes[1], 'category', 'After target redraw');
  assert.equal(selected(ui.nodes[2], 'classification'), 'fee'); assert.equal(selected(ui.nodes[2], 'category'), 'IG Meta');
  review.bindStatement(ui.nodes, ui.statement);
  change(ui.nodes[0], 'category', 'After coordinator rebind'); assert.equal(selected(ui.nodes[2], 'category'), 'IG Meta');
  review.bindRow(ui.nodes[0], ui.statement.rows[0], 0, ui.statement, ['SANKI', 'SAMAST', 'PERSONAL'], fmt);
  change(ui.nodes[0], 'classification', 'expense'); assert.equal(selected(ui.nodes[2], 'classification'), 'fee', 'rebinding existing controls retains one-use enforcement');
});

test('legacy saved nonblank choices are protected while blank fields can receive one initial fill', () => {
  const ui = setup([row({ category: 'Saved source', nature: 'SANKI', confirmed: true }), row({ category: 'Saved target', nature: 'SAMAST', confirmed: true }), row({ confirmed: true })], { revision: 4 });
  change(ui.nodes[0], 'category', 'Source local correction'); change(ui.nodes[0], 'nature', 'PERSONAL'); change(ui.nodes[0], 'classification', 'fee');
  assert.equal(selected(ui.nodes[1], 'category'), 'Saved target'); assert.equal(selected(ui.nodes[2], 'category'), '');
  assert.equal(selected(ui.nodes[1], 'nature'), 'SAMAST'); assert.equal(selected(ui.nodes[2], 'classification'), 'expense');
  change(ui.nodes[0], 'type', 'marketing'); assert.equal(selected(ui.nodes[1], 'type'), 'marketing'); assert.equal(selected(ui.nodes[2], 'type'), 'marketing');
});

test('pristine import category suggestions remain untouched until the first deliberate selection fills once', () => {
  const ui = setup([row(), row({ category: 'Existing suggestion', suggestedCategory: 'Existing suggestion' }), row()]);
  assert.equal(selected(ui.nodes[1], 'category'), 'Existing suggestion', 'opening review never changes a suggestion');
  change(ui.nodes[0], 'category', 'IG Meta');
  assert.equal(selected(ui.nodes[1], 'category'), 'IG Meta'); assert.equal(selected(ui.nodes[2], 'category'), 'IG Meta');
  const suggestedFirst = setup([row({ category: 'Suggested source', suggestedCategory: 'Suggested source' }), row({ category: 'Other suggestion', suggestedCategory: 'Other suggestion' }), row()]);
  change(suggestedFirst.nodes[0], 'category', 'Explicit source change');
  assert.equal(selected(suggestedFirst.nodes[1], 'category'), 'Explicit source change'); assert.equal(selected(suggestedFirst.nodes[2], 'category'), 'Explicit source change');
  change(suggestedFirst.nodes[0], 'category', 'Second source change'); assert.equal(selected(suggestedFirst.nodes[1], 'category'), 'Explicit source change');
});

test('saved, confirmed and manually changed import categories cannot be mistaken for pristine suggestions', () => {
  for (const options of [{ revision: 1 }, { revision: 0, confirmed: true }]) {
    const ui = setup([row({ category: 'Suggested source', suggestedCategory: 'Suggested source', confirmed: !!options.confirmed }), row({ category: 'Saved target', suggestedCategory: 'Saved target', confirmed: !!options.confirmed }), row()], options);
    change(ui.nodes[0], 'category', 'Source local correction');
    assert.equal(selected(ui.nodes[1], 'category'), 'Saved target'); assert.equal(selected(ui.nodes[2], 'category'), '');
  }
  const manual = setup([row({ category: 'Manual source', suggestedCategory: 'Different suggestion' }), row()]);
  change(manual.nodes[0], 'category', 'Source local correction'); assert.equal(selected(manual.nodes[1], 'category'), '');
  const manualTarget = setup([row(), row({ category: 'Manual target', suggestedCategory: 'Different suggestion' }), row({ category: 'No suggestion metadata' })]);
  change(manualTarget.nodes[0], 'category', 'IG Meta'); assert.equal(selected(manualTarget.nodes[1], 'category'), 'Manual target'); assert.equal(selected(manualTarget.nodes[2], 'category'), 'No suggestion metadata');
});

test('manual override of a pristine suggestion before first-row selection is still protected', () => {
  const ui = setup([row({ category: 'Suggestion', suggestedCategory: 'Suggestion' }), row({ category: 'Suggestion', suggestedCategory: 'Suggestion' }), row({ category: 'Suggestion', suggestedCategory: 'Suggestion' })]);
  change(ui.nodes[1], 'category', 'Local target override'); change(ui.nodes[0], 'category', 'IG Meta');
  assert.equal(selected(ui.nodes[1], 'category'), 'Local target override'); assert.equal(selected(ui.nodes[2], 'category'), 'IG Meta');
});

test('only first displayed merchant row seeds, not a later row or a different statement', () => {
  const one = setup(), two = setup(undefined, { id: 'CCS-SECOND' });
  review.bindStatement(one.nodes.concat(two.nodes), one.statement);
  change(one.nodes[1], 'channel', 'POS'); assert.equal(selected(one.nodes[0], 'channel'), ''); assert.equal(selected(one.nodes[2], 'channel'), '');
  change(one.nodes[0], 'channel', 'Both'); assert.equal(selected(one.nodes[1], 'channel'), 'POS'); assert.equal(selected(one.nodes[2], 'channel'), 'Both');
  for (const tr of two.nodes) assert.equal(selected(tr, 'channel'), '');
});

test('whitespace normalization is limited to case and spacing and never changes merchant strings', () => {
  const ui = setup([row({ merchant: 'Meta  Ads' }), row({ merchant: '\tMETA\nADS  ' }), row({ merchant: 'META-ADS' }), row({ merchant: 'MetaAds' })]);
  change(ui.nodes[0], 'category', 'IG Meta');
  assert.equal(selected(ui.nodes[1], 'category'), 'IG Meta'); assert.equal(selected(ui.nodes[2], 'category'), ''); assert.equal(selected(ui.nodes[3], 'category'), '');
  assert.equal(payload(ui.nodes[1]).merchant, '\tMETA\nADS  ');
});

test('repayments, unclassified credits, refunds and EMI principal never receive or seed expense fills', () => {
  const others = ['card_payment', 'unclassified_credit', 'refund'].map(classification => row({ classification, credit: 100, debit: 0, category: 'Credit category', settlementMode: 'pending', expenseRefundReceiptId: 'RC-SYNTHETIC', refundLinkReason: 'Synthetic evidence' }));
  const ui = setup([row(), ...others, row({ classification: 'emi_principal' }), row()]);
  const before = ui.nodes.slice(1, 5).map(payload);
  change(ui.nodes[0], 'classification', 'fee'); change(ui.nodes[0], 'category', 'IG Meta'); change(ui.nodes[0], 'nature', 'SANKI');
  assert.deepEqual(ui.nodes.slice(1, 5).map(payload), before); assert.equal(selected(ui.nodes[5], 'classification'), 'fee');
  change(ui.nodes[3], 'category', 'Refund local correction'); assert.equal(selected(ui.nodes[5], 'category'), 'IG Meta');
  const creditFirst = setup([row({ credit: 100, debit: 0, classification: 'refund' }), row(), row()]);
  change(creditFirst.nodes[0], 'category', 'Credit only'); assert.equal(selected(creditFirst.nodes[1], 'category'), '');
  change(creditFirst.nodes[1], 'category', 'Debit group'); assert.equal(selected(creditFirst.nodes[2], 'category'), 'Debit group');
});

test('protected, finalized, reconciled, linked, duplicate and replacement rows remain untouched', () => {
  const protections = [{ settlementProtected: true }, { finalized: true }, { reconciled: true }, { linkedExpenseId: 'EX-SYNTHETIC' }, { duplicateOf: { statementId: 'CCS-OLD', rowId: 'ROW-OLD' } }, { replaces: { statementId: 'CCS-OLD', rowId: 'ROW-OLD' } }, { supersededBy: { statementId: 'CCS-NEW', rowId: 'ROW-NEW' } }];
  const ui = setup([row(), ...protections.map(protection => row(protection)), row()]);
  const before = ui.nodes.slice(1, -1).map(payload);
  change(ui.nodes[0], 'category', 'IG Meta'); assert.deepEqual(ui.nodes.slice(1, -1).map(payload), before); assert.equal(selected(ui.nodes.at(-1), 'category'), 'IG Meta');
  const finalized = setup(undefined, { status: 'finalized' });
  change(finalized.nodes[0], 'category', 'Attempt'); assert.equal(selected(finalized.nodes[1], 'category'), '');
  const protectedFirst = setup([row({ settlementProtected: true }), row(), row()]); change(protectedFirst.nodes[1], 'category', 'Local only');
  assert.equal(selected(protectedFirst.nodes[2], 'category'), '', 'a protected first displayed row never promotes a later row to source');
});

test('write-disabled fields cannot change or propagate; enabled refresh-time edits stay local and consumed', () => {
  let busy = true; const ui = setup(undefined, { isBusy: () => busy });
  ui.nodes[0].querySelector('.cc_class').disabled = true;
  const markup = ui.nodes.map(tr => tr.innerHTML); change(ui.nodes[0], 'classification', 'fee');
  assert.deepEqual(ui.nodes.map(tr => tr.innerHTML), markup); assert.equal(selected(ui.nodes[1], 'classification'), 'expense'); assert.equal(ui.dirty, 0);
  busy = false; ui.nodes[2].dataset.ccCandidateRefreshing = 'true'; change(ui.nodes[0], 'category', 'While refreshing');
  assert.equal(selected(ui.nodes[1], 'category'), ''); assert.ok(ui.dirty > 0);
  assert.deepEqual(payload(ui.nodes[0]).merchantAutofill.touchedFields, ['category']);
  change(ui.nodes[1], 'nature', '');
  delete ui.nodes[2].dataset.ccCandidateRefreshing; change(ui.nodes[0], 'category', 'After refresh'); assert.equal(selected(ui.nodes[1], 'category'), '', 'the refresh-time source edit consumed the one-time fill');
  change(ui.nodes[0], 'nature', 'SANKI'); assert.equal(selected(ui.nodes[1], 'nature'), '', 'refresh-time explicit target blank remains protected'); assert.equal(selected(ui.nodes[2], 'nature'), 'SANKI');
});

test('collectRow emits only allowlisted flags and never propagates financial or link identities', () => {
  const ui = setup([row({ reference: 'REF-A', date: '2026-10-01', debit: 100, amount: 100, merchantAutofill: { version: 1, touchedFields: ['id', 'settlementSourceId', 'nature', 'nature'], merchant: 'Do not save rule' } }), row({ reference: 'REF-B', date: '2026-10-02', debit: 255, amount: 255, duplicateResolution: 'separate', claimant: 'Synthetic claimant', bill: 'Synthetic proof' })]);
  const identities = ui.nodes.map(tr => ({ ...tr._ccReviewDraft }));
  change(ui.nodes[0], 'category', 'IG Meta');
  assert.deepEqual(payload(ui.nodes[0]).merchantAutofill, { version: 1, touchedFields: ['category', 'nature'] });
  for (let i = 0; i < ui.nodes.length; i++) for (const field of ['id', 'reference', 'date', 'debit', 'credit', 'amount', 'duplicateResolution', 'claimant', 'bill']) assert.equal(ui.nodes[i]._ccReviewDraft[field], identities[i][field], field);
  const helper = fs.readFileSync(path.join(__dirname, '../public/credit-card-review.js'), 'utf8');
  assert.doesNotMatch(helper, /fetch\(|localStorage|sessionStorage|method\s*:\s*['"]POST|merchantRules\s*=/);
});
