'use strict';
// Synthetic UI harness only: no production data, browsers or network.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const start = html.indexOf('    function discardCreditCardReview(');
const end = html.indexOf('    function saveCreditCardReview(', start);
assert.ok(start >= 0 && end > start);
function harness(options = {}) {
  const calls = [], messages = [], busy = [], prompts = [], confirmations = [];
  const statement = { id: 'CCS-SYNTHETIC', cardId: 'CARD-SYNTHETIC', status: 'review', reviewRevision: 4, canDiscardReview: true, ...options.statement };
  let rendered = 0, resolveRequest;
  const ctx = vm.createContext({
    ccReviewSubmitting: !!options.busy, ccReviewId: statement.id, ccStatements: [statement],
    ccCandidateRefreshBusy: () => !!options.refreshing,
    ccReviewRevision: () => statement.reviewRevision,
    prompt(message) { prompts.push(message); return Object.hasOwn(options, 'reason') ? options.reason : 'Restart this synthetic draft review'; },
    confirm(message) { confirmations.push(message); return options.confirm !== false; },
    setMsg(message, ok) { messages.push({ message, ok }); },
    ccSetReviewBusy(value) { ctx.ccReviewSubmitting = value; busy.push(value); },
    api(url, request) {
      calls.push({ url, request: JSON.parse(JSON.stringify(request)) });
      if(options.pending) return new Promise(resolve => { resolveRequest = resolve; });
      if(options.reject) return Promise.reject(new Error('Synthetic network error'));
      return Promise.resolve(options.response || { success: true, statement: { ...statement, reviewRevision: 5 } });
    },
    renderCreditCards() { rendered++; }, encodeURIComponent
  });
  vm.runInContext(html.slice(start, end), ctx);
  return { ctx, calls, messages, busy, prompts, confirmations, statement, get rendered() { return rendered; }, finish(value) { resolveRequest(value); } };
}
test('discard requires explicit reason and confirmation and posts only revision and reason', async () => {
  const h = harness(); await h.ctx.discardCreditCardReview(h.statement.id);
  assert.equal(h.prompts.length, 1); assert.equal(h.confirmations.length, 1);
  assert.match(h.confirmations[0], /saved and unsaved review choices/);
  assert.match(h.confirmations[0], /original statement, posted records and existing reconciliations will not be changed/);
  assert.equal(h.calls.length, 1); assert.match(h.calls[0].url, /\/CCS-SYNTHETIC\/discard-review$/);
  assert.equal(h.calls[0].request.method, 'POST');
  assert.deepEqual(JSON.parse(h.calls[0].request.body), { reviewRevision: 4, reason: 'Restart this synthetic draft review' });
  assert.deepEqual(h.busy, [true, false]); assert.equal(h.rendered, 1);
  assert.match(h.messages.at(-1).message, /no accounting records or reconciliations were changed/);
});
test('cancelled or invalid discard never submits or redraws the current review', async () => {
  for(const options of [{ reason: null }, { reason: 'short' }, { reason: 'x'.repeat(2001) }, { confirm: false }]) {
    const h = harness(options); await h.ctx.discardCreditCardReview(h.statement.id);
    assert.equal(h.calls.length, 0); assert.equal(h.rendered, 0); assert.deepEqual(h.busy, []);
  }
});
test('write, candidate refresh, finalized and protected guards precede confirmation', async () => {
  for(const options of [{ busy: true }, { refreshing: true }, { statement: { status: 'finalized' } }, { statement: { canDiscardReview: false, discardReviewBlockedReason: 'Synthetic reconciliation lock' } }]) {
    const h = harness(options); await h.ctx.discardCreditCardReview(h.statement.id);
    assert.equal(h.calls.length, 0); assert.equal(h.prompts.length, 0); assert.equal(h.confirmations.length, 0);
    assert.equal(h.rendered, 0);
  }
});
test('request failures and stale revisions preserve the local review and restore controls', async () => {
  for(const options of [{ reject: true }, { response: { success: false, error: 'Synthetic stale revision' } }]) {
    const h = harness(options); await h.ctx.discardCreditCardReview(h.statement.id);
    assert.equal(h.calls.length, 1); assert.equal(h.rendered, 0); assert.deepEqual(h.busy, [true, false]);
    assert.equal(h.messages.at(-1).ok, false); assert.equal(h.statement.reviewRevision, 4);
  }
});
test('unverified success identity or revision never silently replaces the open review', async () => {
  for(const statement of [{ id: 'WRONG', cardId: 'CARD-SYNTHETIC', status: 'review', reviewRevision: 5 }, { id: 'CCS-SYNTHETIC', cardId: 'WRONG', status: 'review', reviewRevision: 5 }, { id: 'CCS-SYNTHETIC', cardId: 'CARD-SYNTHETIC', status: 'finalized', reviewRevision: 5 }, { id: 'CCS-SYNTHETIC', cardId: 'CARD-SYNTHETIC', status: 'review', reviewRevision: 4 }]) {
    const h = harness({ response: { success: true, statement } }); await h.ctx.discardCreditCardReview(h.statement.id);
    assert.equal(h.rendered, 0); assert.equal(h.messages.at(-1).ok, false); assert.match(h.messages.at(-1).message, /do not repeat/);
  }
});
test('double clicks during discard submit only one mutation', async () => {
  const h = harness({ pending: true }); const first = h.ctx.discardCreditCardReview(h.statement.id);
  await h.ctx.discardCreditCardReview(h.statement.id);
  assert.equal(h.calls.length, 1); assert.equal(h.prompts.length, 1);
  h.finish({ success: true, statement: { ...h.statement, reviewRevision: 5 } }); await first;
  assert.equal(h.rendered, 1); assert.equal(h.ctx.ccReviewSubmitting, false);
});
test('review actions preserve manual save and explicit finalization without autosave or apply-matching button', () => {
  const render = html.slice(html.indexOf('    function renderCreditCardReview('), html.indexOf('    window.editCreditCard='));
  assert.match(render, /Discard saved review/); assert.match(render, /Save review/); assert.match(render, /Finalize and post to ledgers/);
  assert.match(render, /st\.canDiscardReview===false\?' disabled'/);
  assert.match(render, /esc\(st\.discardReviewBlockedReason/);
  assert.match(render, /fills untouched matching entries once in this statement/);
  assert.match(render, /Later edits affect only that row/);
  assert.doesNotMatch(render, /Apply.*matching|Autosave|auto-save/i);
  assert.match(html, /CreditCardReview\.bindStatement\(reviewNodes,review,\{isBusy:function\(\)\{return ccReviewSubmitting\|\|ccCandidateRefreshBusy\(\);\},markDirty:markReviewDirty\}\)/);
  assert.match(html, /el\('cc_discard_review'\)\.onclick=function\(\)\{discardCreditCardReview\(review\.id\);\}/);
});
