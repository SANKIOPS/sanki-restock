'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const segment = html.slice(html.indexOf('    function renderCreditCardStatementLog('), html.indexOf('    function saveCreditCardReview('));
const esc = value => String(value == null ? '' : value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
function harness(options = {}) {
  const restore = !!options.restore, statement = { id: 'CCS-DUPLICATE', cardId: 'CC-SYNTHETIC', rows: [], status: restore ? 'archived' : 'review', reviewRevision: 4, canArchiveReview: !restore, canRestoreReview: restore, ...options.statement };
  const calls = [], prompts = [], confirmations = [], messages = [], busy = []; let rendered = 0, resolveRequest;
  const ctx = vm.createContext({ ccReviewSubmitting: !!options.busy, ccReviewId: statement.id, ccArchivedViewId: restore ? statement.id : '', ccStatements: [statement],
    ccCandidateRefreshBusy: () => !!options.refreshing, el: () => ({ dataset: { reconciliationDirty: options.dirty ? 'true' : '' } }), ccReviewRevision: () => statement.reviewRevision,
    esc, fmt: x => 'INR' + x, ReconciliationStatus: { card: () => 'Archived status' },
    prompt(message) { prompts.push(message); return Object.hasOwn(options, 'reason') ? options.reason : 'Retain this verified duplicate review as evidence'; },
    confirm(message) { confirmations.push(message); return options.confirm !== false; }, setMsg(message, ok) { messages.push({ message, ok }); },
    ccSetReviewBusy(value) { ctx.ccReviewSubmitting = value; busy.push(value); }, renderCreditCards() { rendered++; },
    api(url, request) { calls.push({ url, request: JSON.parse(JSON.stringify(request)) }); if (options.pending) return new Promise(resolve => { resolveRequest = resolve; }); if (options.reject) return Promise.reject(new Error('Synthetic network failure')); return Promise.resolve(options.response || { success: true, statement: { ...statement, status: restore ? 'review' : 'archived', reviewRevision: 5 } }); }
  });
  vm.runInContext(segment, ctx);
  return { ctx, statement, calls, prompts, confirmations, messages, busy, get rendered() { return rendered; }, finish(value) { resolveRequest(value); } };
}
for (const restore of [false, true]) test((restore ? 'restore' : 'archive') + ' asks explicit confirmation, sends only exact revision/reason and verifies returned identity', async () => {
  const h = harness({ restore }); await h.ctx.archiveCreditCardReview(h.statement.id, restore);
  assert.equal(h.calls.length, 1); assert.equal(h.prompts.length, 1); assert.equal(h.confirmations.length, 1);
  assert.match(h.confirmations[0], /No saved choices, original statement, accounting entry or reconciliation will be deleted or reversed/);
  assert.equal(h.calls[0].url, '/api/expenses/credit-cards/statements/CCS-DUPLICATE/' + (restore ? 'restore' : 'archive') + '-review');
  assert.deepEqual(JSON.parse(h.calls[0].request.body), { reviewRevision: 4, reason: 'Retain this verified duplicate review as evidence' });
  assert.deepEqual(h.busy, [true, false]); assert.equal(h.rendered, 1); assert.equal(h.ctx.ccArchivedViewId, '');
});
test('unsaved choices, candidate refresh, forbidden status and denied server eligibility block archive before prompt', async () => {
  for (const options of [{ dirty: true }, { refreshing: true }, { busy: true }, { statement: { status: 'finalized' } }, { statement: { canArchiveReview: false } }]) {
    const h = harness(options); await h.ctx.archiveCreditCardReview(h.statement.id, false); assert.equal(h.calls.length, 0); assert.equal(h.prompts.length, 0); assert.equal(h.rendered, 0);
  }
});
test('cancelled and invalid reasons leave the local review untouched', async () => {
  for (const options of [{ reason: null }, { reason: 'no' }, { reason: 'x'.repeat(2001) }, { confirm: false }]) {
    const h = harness(options); await h.ctx.archiveCreditCardReview(h.statement.id, false); assert.equal(h.calls.length, 0); assert.equal(h.rendered, 0); assert.deepEqual(h.busy, []);
  }
});
test('stale, failed and unverifiable results never redraw or repeat an archive', async () => {
  for (const options of [{ reject: true }, { response: { success: false, error: 'Synthetic revision conflict' } }, { response: { success: true, statement: { id: 'WRONG', cardId: 'CC-SYNTHETIC', status: 'archived', reviewRevision: 5 } } }, { response: { success: true, statement: { id: 'CCS-DUPLICATE', cardId: 'CC-SYNTHETIC', status: 'review', reviewRevision: 5 } } }]) {
    const h = harness(options); await h.ctx.archiveCreditCardReview(h.statement.id, false); assert.equal(h.calls.length, 1); assert.equal(h.rendered, 0); assert.equal(h.messages.at(-1).ok, false); assert.equal(h.statement.reviewRevision, 4); assert.deepEqual(h.busy, [true, false]);
  }
});
test('double archive clicks submit once', async () => {
  const h = harness({ pending: true }), first = h.ctx.archiveCreditCardReview(h.statement.id, false); await h.ctx.archiveCreditCardReview(h.statement.id, false); assert.equal(h.calls.length, 1);
  h.finish({ success: true, statement: { ...h.statement, status: 'archived', reviewRevision: 5 } }); await first; assert.equal(h.rendered, 1);
});
test('archived view is readonly, escaped and never offers finalization or source reopening', () => {
  const h = harness({ restore: true }), st = { ...h.statement, cardName: '<script>card</script>', reviewArchive: { archivedBy: '<owner>', archivedAt: '2026-06-01', reason: '<img src=x onerror=x>' }, rows: [{ date: '2026-06-01', narration: '<script>purchase</script>', amount: 10, category: '<category>', duplicateOf: { statementId: 'CCS-SOURCE' } }] };
  const view = h.ctx.renderArchivedCreditCardReview(st), log = h.ctx.renderCreditCardStatementLog(st);
  assert.match(view, /Restore duplicate review/); assert.match(view, /Saved choices and uploaded evidence are retained/); assert.match(view, /&lt;script&gt;purchase/); assert.doesNotMatch(view, /<input|<select|cc_finalize|Finalize and post|Save review|discardCreditCardReview/);
  assert.match(log, /View retained duplicate review/); assert.doesNotMatch(log, /Reopen with reason|Classify &amp; confirm/); assert.doesNotMatch(log, /<script>card/);
});
test('default selection and active pending lists exclude archived reviews; archive logs remain collapsed', () => {
  const draw = html.slice(html.indexOf('    function drawCreditCards('), html.indexOf('    function attachCardCorrectionButtons('));
  assert.match(draw, /s\.status!=='archived'\|\|ccArchivedViewId===s\.id/); assert.match(draw, /ccStatements\.filter\(function\(s\)\{return s\.status==='review';\}\)\.length/);
  assert.match(draw, /<details class="cc-archived-logs"><summary>Archived duplicate reviews/); assert.doesNotMatch(draw, /cc-archived-logs" open/);
});
