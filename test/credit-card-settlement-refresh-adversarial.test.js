'use strict';
// Synthetic evidence only. These tests never load accounting stores or send a request.
const test = require('node:test');
const assert = require('node:assert/strict');
const review = require('../public/credit-card-review');
const clone = value => JSON.parse(JSON.stringify(value));
const fmt = value => 'INR ' + value;
const source = { id: 'payment:CCP-SYNTHETIC', label: 'Synthetic payment', date: '2026-08-27', amount: 500, account: 'Synthetic business bank', nature: 'SANKI', reference: 'SYNTHETIC-REF', fingerprint: 'a'.repeat(64), eligible: true, blockedReason: '', dateReasonRequired: false, bankMovementRecorded: true, status: 'Existing recorded settlement; bank verification is separate' };
function fixture() {
  const row = { id: 'ROW-SYNTHETIC-CREDIT', date: '2026-08-27', postingDate: '2026-08-27', narration: 'PAYMENT RECEIVED THANK YOU', reference: 'CARD-REF', amount: 500, debit: 0, credit: 500, merchant: 'Synthetic issuer', classification: 'card_payment', settlementMode: 'link_existing', settlementSourceId: source.id, settlementSourceFingerprint: 'b'.repeat(64), settlementReason: 'Saved source awaiting revalidation', settlementCandidates: [] };
  const other = { id: 'ROW-SYNTHETIC-DEBIT', date: '2026-08-28', amount: 50, debit: 50, credit: 0, narration: 'Synthetic merchant purchase', merchant: 'Synthetic merchant', classification: 'expense', type: 'variable', category: 'Synthetic category', nature: 'SAMAST', channel: 'Both' };
  const statement = { id: 'CCS-SYNTHETIC', cardId: 'CC-SYNTHETIC', status: 'review', reviewRevision: 7, rows: [row, other] };
  const fresh = clone(statement); fresh.rows[0].settlementCandidates = [clone(source)];
  return { row, other, statement, fresh, response: { success: true, statements: [fresh] } };
}
function metadata(f) { return review.candidateRefreshMetadata(f.statement, f.row, f.response); }

test('candidate refresh is read-only, deep-clones candidates, and never supplies replacement review fields', () => {
  const f = fixture(), before = clone(f), result = metadata(f);
  assert.deepEqual(Object.keys(result), ['settlementCandidates']);
  assert.deepEqual(result.settlementCandidates, [source]);
  assert.deepEqual(f, before, 'metadata extraction cannot rewrite source or saved review');
  result.settlementCandidates[0].account = 'Modified local candidate';
  assert.equal(f.fresh.rows[0].settlementCandidates[0].account, source.account);
  assert.equal(f.row.settlementSourceFingerprint, 'b'.repeat(64));
});

test('candidate refresh rejects stale revisions and foreign, finalized or missing statements', () => {
  const mutations = [
    f => { f.fresh.reviewRevision++; },
    f => { delete f.fresh.reviewRevision; },
    f => { f.fresh.reviewRevision = String(f.statement.reviewRevision); },
    f => { f.fresh.id = 'CCS-OTHER'; },
    f => { f.fresh.cardId = 'CC-OTHER'; },
    f => { f.fresh.status = 'finalized'; },
    f => { f.response.statements = []; },
    f => { f.response.success = false; f.response.error = 'Rejected'; }
  ];
  for (const mutate of mutations) {
    const f = fixture(); mutate(f); const before = clone(f);
    assert.throws(() => metadata(f)); assert.deepEqual(f, before);
  }
});

test('candidate refresh rejects changed raw credit evidence, identity, privacy protection and source selections', () => {
  const mutations = [
    f => { f.fresh.rows[0].id = 'ROW-OTHER'; },
    f => { f.fresh.rows.push(clone(f.fresh.rows[0])); },
    f => { f.fresh.rows.pop(); },
    f => { f.fresh.rows[0].date = '2026-08-28'; },
    f => { f.fresh.rows[0].amount = 501; },
    f => { f.fresh.rows[0].credit = 501; },
    f => { f.fresh.rows[0].debit = 1; },
    f => { f.fresh.rows[0].narration = 'A different credit'; },
    f => { f.fresh.rows[0].reference = 'DIFFERENT-REFERENCE'; },
    f => { f.fresh.rows[0].classification = 'refund'; },
    f => { f.fresh.rows[0].settlementSourceId = 'payment:OTHER'; },
    f => { f.fresh.rows[0].settlementSourceFingerprint = 'c'.repeat(64); },
    f => { f.fresh.rows[0].settlementProtected = true; }
  ];
  for (const mutate of mutations) {
    const f = fixture(); mutate(f); const before = clone(f);
    assert.throws(() => metadata(f)); assert.deepEqual(f, before);
  }
});

function domRow(row, statement) {
  const node = { dataset: { id: row.id }, isConnected: true, fields: {}, querySelector(selector) { return this.fields[selector] || null; } };
  let markup = '';
  const decode = value => String(value).replace(/&(amp|lt|gt|quot|#39);/g, (_, key) => ({ amp: '&', lt: '<', gt: '>', quot: '"', '#39': "'" }[key]));
  Object.defineProperty(node, 'innerHTML', { get() { return markup; }, set(value) {
    markup = value; this.fields = {};
    for (const match of value.matchAll(/<input\b([^>]*)>/g)) { const cls = match[1].match(/class="([^"]+)"/); if (cls) this.fields['.' + cls[1]] = { value: decode((match[1].match(/value="([^"]*)"/) || [])[1] || '') }; }
    for (const match of value.matchAll(/<select\b([^>]*)>([\s\S]*?)<\/select>/g)) { const cls = match[1].match(/class="([^"]+)"/); if (cls) { const choices = [...match[2].matchAll(/<option\b([^>]*)>/g)], choice = choices.find(item => /\bselected\b/.test(item[1])) || choices[0]; this.fields['.' + cls[1]] = { value: choice ? decode((choice[1].match(/value="([^"]*)"/) || [])[1] || '') : '' }; } }
    const classes = [...value.matchAll(/class="([^"]+)"/g)].map(match => match[1].split(/\s+/));
    for (const key of ['cc_settlement_refresh', 'cc_settlement_refresh_status', 'cc_settlement_evidence']) if (classes.some(list => list.includes(key))) this.fields['.' + key] = { disabled: false, textContent: '', innerHTML: '', dataset: {} };
  } });
  node.innerHTML = review.rowCells(row, 0, statement, ['SANKI', 'SAMAST'], fmt); return node;
}
const flush = async () => { for (let i = 0; i < 5; i++) await new Promise(resolve => setImmediate(resolve)); };

test('refreshing candidate metadata keeps saved unavailable source and unsaved merchant/remark without auto-confirmation', async () => {
  const f = fixture(), tr = domRow(f.row, f.statement), calls = [], dirty = [];
  review.bindRow(tr, f.row, 0, f.statement, ['SANKI', 'SAMAST'], fmt, () => dirty.push(true), () => { calls.push('read-only'); return Promise.resolve(f.response); });
  assert.equal(tr.fields['.cc_settlement_source'].value, source.id, 'missing saved choice must not become blank');
  tr.fields['.cc_merchant'].value = 'Unsaved merchant correction'; tr.fields['.cc_settlement_reason'].value = 'Unsaved settlement explanation';
  const otherBefore = clone(f.other); tr.fields['.cc_settlement_refresh'].onclick(); await flush();
  assert.equal(calls.length, 1);
  assert.equal(tr.fields['.cc_merchant'].value, 'Unsaved merchant correction');
  assert.equal(tr.fields['.cc_settlement_reason'].value, 'Unsaved settlement explanation');
  const collected = review.collectRow(tr, tr._ccReviewDraft);
  assert.equal(collected.settlementSourceId, source.id);
  assert.equal(collected.settlementSourceFingerprint, 'b'.repeat(64), 'a GET must not silently endorse changed source fingerprint');
  assert.deepEqual(f.other, otherBefore); assert.equal(f.statement.reviewRevision, 7);
  assert.ok(dirty.length, 'remembering unsaved row fields must retain the existing dirty protection');
});

test('a late candidate response cannot rewrite a detached row or adopt a newer review revision', async () => {
  for (const changed of ['detached', 'stale']) {
    const f = fixture(), tr = domRow(f.row, f.statement); let resolve;
    review.bindRow(tr, f.row, 0, f.statement, ['SANKI', 'SAMAST'], fmt, () => {}, () => new Promise(done => { resolve = done; }));
    tr.fields['.cc_settlement_refresh'].onclick(); await Promise.resolve();
    if (changed === 'detached') tr.isConnected = false; else f.fresh.reviewRevision++;
    const draftBefore = clone(tr._ccReviewDraft); resolve(f.response); await flush();
    assert.deepEqual(tr._ccReviewDraft, draftBefore, changed);
    assert.equal(f.statement.reviewRevision, 7);
  }
});
