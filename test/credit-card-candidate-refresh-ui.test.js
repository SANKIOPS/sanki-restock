'use strict';
// All records are synthetic. No accounting data files, browser sessions or network calls.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const review = require('../public/credit-card-review');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const clone = value => JSON.parse(JSON.stringify(value));
const fmt = value => 'INR ' + value;
const source = { id: 'payment:SYNTHETIC-ONE', date: '2026-08-14', amount: 500, nature: 'SANKI', account: 'Synthetic bank', reference: 'SYNTHETIC-REF', fingerprint: 'a'.repeat(64), eligible: true, label: 'Synthetic payment' };
function fixture() {
  const rows = Array.from({ length: 24 }, (_, i) => ({ id: 'SYNTHETIC-ROW-' + i, date: '2026-08-14', postingDate: '2026-08-14', narration: 'Synthetic purchase ' + i, reference: 'SYNTHETIC-' + i, amount: i + 10, debit: i + 10, credit: 0, merchant: 'Synthetic merchant ' + i, classification: 'expense', type: 'variable', category: 'Synthetic category ' + i, nature: i % 2 ? 'SAMAST' : 'SANKI', channel: 'Both' }));
  Object.assign(rows[12], { narration: 'PAYMENT RECEIVED THANK YOU', amount: 500, credit: 500, debit: 0, merchant: 'Synthetic issuer', classification: 'card_payment', settlementMode: 'pending', settlementSourceId: '', settlementSourceFingerprint: '', settlementReason: 'Saved synthetic repayment remark', settlementCandidates: [] });
  const statement = { id: 'CCS-SYNTHETIC-REFRESH', cardId: 'CARD-SYNTHETIC', status: 'review', reviewRevision: 2, rows };
  const fresh = clone(statement); fresh.rows[12].settlementCandidates = [clone(source)];
  return { statement, row: rows[12], response: { success: true, statements: [fresh] } };
}
function domRow(row, index, statement) {
  const node = { dataset: { id: row.id }, isConnected: true, fields: {}, querySelector(selector) { return this.fields[selector] || null; } };
  let markup = '';
  const decode = value => String(value).replace(/&(amp|lt|gt|quot|#39);/g, (_, key) => ({ amp: '&', lt: '<', gt: '>', quot: '"', '#39': "'" }[key]));
  function classes(attributes) { const match = attributes.match(/class="([^"]+)"/); return match ? match[1].split(/\s+/) : []; }
  Object.defineProperty(node, 'innerHTML', { get() { return markup; }, set(value) {
    markup = value; this.fields = {};
    for (const match of value.matchAll(/<input\b([^>]*)>/g)) { const field = { value: decode((match[1].match(/value="([^"]*)"/) || [])[1] || ''), disabled: /\bdisabled\b/.test(match[1]), checked: /\bchecked\b/.test(match[1]), dataset: {} }; classes(match[1]).forEach(cls => { this.fields['.' + cls] = field; }); }
    for (const match of value.matchAll(/<select\b([^>]*)>([\s\S]*?)<\/select>/g)) { const options = [...match[2].matchAll(/<option\b([^>]*)>/g)], choice = options.find(option => /\bselected\b/.test(option[1])) || options[0], field = { value: choice ? decode((choice[1].match(/value="([^"]*)"/) || [])[1] || '') : '', disabled: /\bdisabled\b/.test(match[1]), dataset: {} }; classes(match[1]).forEach(cls => { this.fields['.' + cls] = field; }); }
    for (const match of value.matchAll(/<(?:button|div)\b([^>]*)>/g)) { const field = { disabled: /\bdisabled\b/.test(match[1]), textContent: '', innerHTML: '', dataset: {} }; classes(match[1]).filter(cls => /^cc_settlement_/.test(cls)).forEach(cls => { this.fields['.' + cls] = field; }); }
  } });
  node.innerHTML = review.rowCells(row, index, statement, ['SANKI', 'SAMAST'], fmt);
  return node;
}
function rowUI(f, load, onDirty = () => {}) {
  const tr = domRow(f.row, 12, f.statement);
  review.bindRow(tr, f.row, 12, f.statement, ['SANKI', 'SAMAST'], fmt, onDirty, load);
  tr.fields['.cc_settlement_mode'].value = 'link_existing'; tr.fields['.cc_settlement_mode'].onchange();
  return tr;
}
const flush = async () => { for (let i = 0; i < 4; i++) await new Promise(resolve => setImmediate(resolve)); };

test('empty and fully blocked payment choices explain the missing source without treating bank reconciliation as the gate', () => {
  const f = fixture(), row = { ...f.row, settlementMode: 'link_existing' };
  const empty = review.rowCells(row, 12, f.statement, ['SANKI'], fmt);
  assert.match(empty, /No accessible recorded card payment or transfer/);
  assert.match(empty, /saved statement review has not been removed/);
  assert.match(empty, /Bank reconciliation is a separate check/);
  assert.match(empty, /not a reason to create a duplicate/);
  assert.match(empty, /Keep the source pending until verified/);
  assert.match(empty, /Refresh existing payments/);
  assert.doesNotMatch(empty, /class="cc_(?:category|nature|channel|refund_link)"/);
  const blocked = review.rowCells({ ...row, settlementCandidates: [{ ...source, eligible: false, blockedReason: 'Synthetic source is already allocated' }] }, 12, f.statement, ['SANKI'], fmt);
  assert.match(blocked, /No existing payment currently passes all linking checks/);
  assert.match(blocked, /value="payment:SYNTHETIC-ONE" disabled/);
  assert.match(blocked, /Synthetic source is already allocated/);
  assert.doesNotMatch(blocked, /value="payment:SYNTHETIC-ONE" selected/);
});

test('missing or ineligible saved sources keep their exact saved identity and evidence without becoming valid automatically', () => {
  for (const candidates of [[], [{ ...source, eligible: false, blockedReason: 'Changed synthetic source' }]]) {
    const f = fixture(); Object.assign(f.row, { settlementMode: 'link_existing', settlementSourceId: source.id, settlementSourceFingerprint: 'b'.repeat(64), settlementCandidates: candidates });
    const tr = domRow(f.row, 12, f.statement); review.bindRow(tr, f.row, 12, f.statement, ['SANKI'], fmt);
    assert.equal(tr.fields['.cc_settlement_source'].value, source.id);
    const payload = review.collectRow(tr, tr._ccReviewDraft);
    assert.equal(payload.settlementSourceId, source.id); assert.equal(payload.settlementSourceFingerprint, 'b'.repeat(64));
    assert.notEqual(payload.settlementSourceFingerprint, source.fingerprint);
  }
});

test('GET-only candidate refresh retains all 24 rows, current treatment and every unsaved classification field', async () => {
  const f = fixture(), calls = [], before = clone(f.statement), nodes = f.statement.rows.map((row, index) => domRow(row, index, f.statement));
  for (let i = 0; i < nodes.length; i++) {
    if (i === 12) continue;
    nodes[i].fields['.cc_merchant'].value = 'Unsaved synthetic merchant ' + i;
    nodes[i].fields['.cc_category'].value = 'Unsaved synthetic category ' + i;
    nodes[i].fields['.cc_nature'].value = i % 2 ? 'SANKI' : 'SAMAST';
    nodes[i].fields['.cc_channel'].value = 'Website';
  }
  const otherPayloads = nodes.map((node, i) => i === 12 ? null : review.collectRow(node, f.statement.rows[i]));
  const tr = rowUI(f, cardId => { calls.push(cardId); return Promise.resolve(f.response); }); nodes[12] = tr;
  tr.fields['.cc_merchant'].value = 'Unsaved synthetic issuer'; tr.fields['.cc_settlement_reason'].value = 'Unsaved synthetic payment evidence';
  assert.equal(await tr.fields['.cc_settlement_refresh'].onclick(), true);
  assert.deepEqual(calls, [f.statement.cardId]);
  assert.equal(nodes.length, 24); assert.deepEqual(f.statement, before, 'the server snapshot and revision are never replaced by refresh');
  assert.equal(tr.fields['.cc_settlement_mode'].value, 'link_existing'); assert.equal(tr.fields['.cc_settlement_source'].value, '', 'new eligible source is not selected');
  assert.equal(tr.fields['.cc_merchant'].value, 'Unsaved synthetic issuer'); assert.equal(tr.fields['.cc_settlement_reason'].value, 'Unsaved synthetic payment evidence');
  assert.equal(review.collectRow(tr, tr._ccReviewDraft).settlementSourceFingerprint, '');
  for (let i = 0; i < nodes.length; i++) if (i !== 12) assert.deepEqual(review.collectRow(nodes[i], f.statement.rows[i]), otherPayloads[i]);
});

test('network and malformed refresh failures retain the form and restore the retry button', async () => {
  for (const load of [() => Promise.reject(new Error('Synthetic network failure')), () => Promise.resolve({ success: true, statements: [] })]) {
    const f = fixture(), tr = rowUI(f, load); tr.fields['.cc_settlement_reason'].value = 'Unsaved synthetic explanation';
    const markup = tr.innerHTML, promise = tr.fields['.cc_settlement_refresh'].onclick(), draft = clone(tr._ccReviewDraft);
    assert.equal(await promise, false); assert.equal(tr.innerHTML, markup); assert.deepEqual(tr._ccReviewDraft, draft);
    assert.equal(tr.fields['.cc_settlement_reason'].value, 'Unsaved synthetic explanation'); assert.equal(tr.fields['.cc_settlement_refresh'].disabled, false);
    assert.equal(tr.dataset.ccCandidateRefreshing, undefined);
    assert.match(tr.fields['.cc_settlement_refresh_status'].textContent, /failure|preserved/);
  }
});

test('refresh double clicks use one read-only request and preserve edits made while it is pending', async () => {
  const f = fixture(); let resolve, calls = 0;
  const tr = rowUI(f, () => { calls++; return new Promise(done => { resolve = done; }); });
  const running = tr.fields['.cc_settlement_refresh'].onclick(); assert.equal(await tr.fields['.cc_settlement_refresh'].onclick(), false); await flush();
  assert.equal(calls, 1); assert.equal(tr.dataset.ccCandidateRefreshing, 'true');
  tr.fields['.cc_settlement_reason'].value = 'Additional unsaved explanation during read-only refresh';
  resolve(f.response); assert.equal(await running, true);
  assert.equal(tr.fields['.cc_settlement_reason'].value, 'Additional unsaved explanation during read-only refresh');
  assert.equal(tr.dataset.ccCandidateRefreshing, undefined); assert.equal(tr.fields['.cc_settlement_refresh'].disabled, false);
});

function busyHarness() {
  const calls = [], messages = [], rows = [{ dataset: { ccCandidateRefreshing: 'true' } }]; let confirmations = 0;
  const ctx = vm.createContext({ CreditCardReview: review, ccReviewSubmitting: false, ccReviewId: 'SYNTHETIC', ccStatements: [], document: { querySelectorAll: selector => selector === '.ccrow' ? rows : [] }, confirm() { confirmations++; return true; }, encodeURIComponent, setMsg(message) { messages.push(message); }, api(url, options) { calls.push({ url, options }); return Promise.resolve({ success: true }); }, renderCreditCards() {} });
  const start = html.indexOf('    function ccReviewRows('), end = html.indexOf('    window.openCreditCardReview=', start);
  vm.runInContext(html.slice(start, end), ctx);
  return { ctx, calls, messages, get confirmations() { return confirmations; } };
}

test('save and finalize cannot write while existing-payment metadata is refreshing', () => {
  const ui = busyHarness(); ui.ctx.saveCreditCardReview('SYNTHETIC'); ui.ctx.finalizeCreditCardStatement('SYNTHETIC');
  assert.equal(ui.calls.length, 0); assert.equal(ui.confirmations, 0); assert.equal(ui.ctx.ccReviewSubmitting, false);
  assert.equal(ui.messages.length, 2); ui.messages.forEach(message => assert.match(message, /refresh to finish.*review is unchanged/));
});

test('production wiring uses uncached GET metadata only, cache-busts the helper and pauses background redraw during refresh', () => {
  assert.match(html, /credit-card-review\.js\?v=20261025-merchant-once-discard-1/);
  assert.match(html, /api\('\/api\/expenses\/credit-cards\/statements\?cardId='\+encodeURIComponent\(cardId\),\{cache:'no-store'\}\)/);
  assert.match(html, /function reconciliationStatusBusy\(\)\{\s*if\(ccReviewSubmitting\)return true;\s*if\(ccCandidateRefreshBusy\(\)\)return true;/);
  const helper = fs.readFileSync(path.join(__dirname, '../public/credit-card-review.js'), 'utf8');
  assert.doesNotMatch(helper, /fetch\(|method\s*:\s*['"]POST|\/finalize|\/review['"]/);
});

const legacySource = { ...source, id: 'legacy_expense_payment:EX-SYNTHETIC/PAY-SYNTHETIC', sourceKind: 'legacy_expense_payment', legacyConfirmationRequired: true, label: 'Legacy Personal payment capture — synthetic', nature: 'PERSONAL', account: 'Synthetic personal bank' };
function legacyFixture(confirmed = false) {
  const f = fixture(); f.statement.cardName = 'Synthetic card 1234';
  Object.assign(f.row, { settlementMode: 'link_existing', settlementSourceId: legacySource.id, settlementSourceFingerprint: legacySource.fingerprint, settlementLegacyConfirmed: confirmed, settlementCandidates: [clone(legacySource)] });
  f.response = { success: true, statements: [clone(f.statement)] };
  return f;
}

test('legacy capture association requires a separate unchecked confirmation naming the selected card and a full reason', () => {
  const f = legacyFixture(), output = review.rowCells(f.row, 12, f.statement, ['PERSONAL'], fmt), tr = domRow(f.row, 12, f.statement);
  assert.match(output, /I confirm this recorded payment repaid Synthetic card 1234; keep original bank\/payment history unchanged/);
  assert.match(output, /Legacy Personal-capture association only/);
  assert.match(output, /no new withdrawal or expense offset/);
  assert.match(output, /Required: explain this legacy payment association in at least 10 characters/);
  assert.equal(tr.fields['.cc_settlement_legacy_confirmed'].checked, false);
  assert.doesNotMatch(output, /class="cc_(?:type|category|nature|channel|refund_link)"/);
  assert.equal(review.collectRow(tr, f.row).settlementLegacyConfirmed, false);
  tr.fields['.cc_settlement_legacy_confirmed'].checked = true;
  assert.equal(review.collectRow(tr, f.row).settlementLegacyConfirmed, true);
  tr.fields['.cc_settlement_legacy_confirmed'].checked = 'true';
  assert.equal(review.collectRow(tr, f.row).settlementLegacyConfirmed, false, 'confirmation must be an actual checkbox boolean');
  f.statement.cardName = '<script>untrusted</script>';
  const escaped = review.rowCells(f.row, 12, f.statement, ['PERSONAL'], fmt);
  assert.match(escaped, /&lt;script&gt;untrusted&lt;\/script&gt;/); assert.doesNotMatch(escaped, /<script>/);
});

test('explicit source changes reset legacy confirmation but keep the unsaved payment explanation', () => {
  const f = legacyFixture(true), second = { ...legacySource, id: 'legacy_expense_payment:EX-SYNTHETIC-TWO/PAY-SYNTHETIC-TWO', fingerprint: 'c'.repeat(64) };
  f.row.settlementCandidates.push(second);
  const tr = domRow(f.row, 12, f.statement); review.bindRow(tr, f.row, 12, f.statement, ['PERSONAL'], fmt);
  assert.equal(tr.fields['.cc_settlement_legacy_confirmed'].checked, true);
  tr.fields['.cc_settlement_reason'].value = 'Unsaved synthetic explanation of the association';
  tr.fields['.cc_settlement_source'].value = second.id; tr.fields['.cc_settlement_source'].onchange();
  assert.equal(tr.fields['.cc_settlement_legacy_confirmed'].checked, false); assert.equal(tr._ccReviewDraft.settlementLegacyConfirmed, false);
  assert.equal(tr.fields['.cc_settlement_reason'].value, 'Unsaved synthetic explanation of the association');
  assert.equal(review.collectRow(tr, tr._ccReviewDraft).settlementSourceFingerprint, second.fingerprint);
  tr.fields['.cc_settlement_legacy_confirmed'].checked = true;
  tr.fields['.cc_settlement_source'].value = ''; tr.fields['.cc_settlement_source'].onchange();
  assert.equal(tr._ccReviewDraft.settlementLegacyConfirmed, false);
});

test('refresh preserves explicit legacy confirmation and old fingerprint for changed, blocked or missing sources', async () => {
  for (const candidates of [[{ ...legacySource, fingerprint: 'd'.repeat(64) }], [{ ...legacySource, eligible: false, blockedReason: 'Synthetic payment is now reserved' }], []]) {
    const f = legacyFixture(true); f.response.statements[0].rows[12].settlementCandidates = candidates;
    const tr = domRow(f.row, 12, f.statement); review.bindRow(tr, f.row, 12, f.statement, ['PERSONAL'], fmt, () => {}, () => Promise.resolve(f.response));
    tr.fields['.cc_settlement_reason'].value = 'Unsaved synthetic evidence stays with the selected payment';
    assert.equal(await tr.fields['.cc_settlement_refresh'].onclick(), true);
    const payload = review.collectRow(tr, tr._ccReviewDraft);
    assert.equal(payload.settlementSourceId, legacySource.id); assert.equal(payload.settlementSourceFingerprint, legacySource.fingerprint); assert.equal(payload.settlementLegacyConfirmed, true);
    assert.equal(payload.settlementReason, 'Unsaved synthetic evidence stays with the selected payment');
    if (tr.fields['.cc_settlement_legacy_confirmed']) assert.equal(tr.fields['.cc_settlement_legacy_confirmed'].checked, true);
    if (candidates.length && candidates[0].eligible === false) assert.equal(tr.fields['.cc_settlement_legacy_confirmed'].disabled, true);
  }
});

test('candidate refresh never preselects or confirms a newly available legacy capture', async () => {
  const f = fixture(); f.response.statements[0].rows[12].settlementCandidates = [clone(legacySource)];
  const tr = rowUI(f, () => Promise.resolve(f.response)); assert.equal(await tr.fields['.cc_settlement_refresh'].onclick(), true);
  assert.equal(tr.fields['.cc_settlement_source'].value, ''); assert.equal(tr.fields['.cc_settlement_legacy_confirmed'], undefined);
  tr.fields['.cc_settlement_source'].value = legacySource.id; tr.fields['.cc_settlement_source'].onchange();
  assert.equal(tr.fields['.cc_settlement_legacy_confirmed'].checked, false);
  assert.equal(review.collectRow(tr, tr._ccReviewDraft).settlementLegacyConfirmed, false);
});

test('remote legacy-confirmation changes cannot be adopted by a candidate-only refresh', () => {
  const f = legacyFixture(); f.response.statements[0].rows[12].settlementLegacyConfirmed = true;
  assert.throws(() => review.candidateRefreshMetadata(f.statement, f.row, f.response), /transaction details changed/);
  assert.equal(f.row.settlementLegacyConfirmed, false);
});
