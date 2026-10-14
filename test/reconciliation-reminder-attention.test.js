'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { reconciliationReminderAttention } = require('../modules/reconciliation-reminder-attention');

const reminder = { type: 'bank', account: 'Axis Bank 3448', nature: 'SANKI',
  status: 'review_pending', pendingDraft: true, reconciledThrough: '2026-10-01',
  cadenceDays: 2, needsStatement: true, draftId: 'D1' };
function pending(id = 'D1', to = '2026-10-13') {
  return {
    draft: { id, nature: 'SANKI', account: reminder.account,
      summary: { from: '2026-10-02', to, validated: true } },
    approval: { draftId: id, account: reminder.account, status: 'pending', from: '2026-10-02', to },
    view: { canFinalize: true, unresolved: 0, balanceResolved: true, balanceDifference: 0, reconciliationConflicts: [] }
  };
}
const due = (date, every) => !date || (Date.parse('2026-10-13T00:00:00Z') - Date.parse(date + 'T00:00:00Z')) / 86400000 >= every;
const policy = (overrides = {}) => reconciliationReminderAttention({ reminder, drafts: [pending()], owner: false, isDue: due, ...overrides });

test('a clean submitted review stops admin attention but remains pending for Owner approval', () => {
  const admin = policy(), owner = policy({ owner: true });
  assert.equal(admin.attentionRequired, false);
  assert.equal(owner.attentionRequired, true);
  for (const result of [admin, owner]) {
    assert.equal(result.status, 'approval_pending');
    assert.equal(result.pendingDraft, true);
    assert.equal(result.approvalPending, true);
    assert.equal(result.awaitingOwnerApproval, true);
    assert.equal(result.reconciledThrough, '2026-10-01', 'submission never advances the finalized cutoff');
    assert.equal(result.submittedThrough, '2026-10-13');
    assert.equal(result.needsStatement, false);
  }
  assert.match(owner.actionLabel, /Approve \/ finalize/);
  assert.match(admin.actionLabel, /awaiting Owner/);
});

test('a submitted review does not hide genuinely overdue newer statements', () => {
  for (const owner of [false, true]) {
    const result = policy({ owner, drafts: [pending('D1', '2026-10-11')] });
    assert.equal(result.attentionRequired, true);
    assert.equal(result.status, 'statement_required');
    assert.equal(result.needsStatement, true);
    assert.equal(result.approvalPending, true);
    assert.match(result.actionLabel, /[Nn]ewer statement required/);
  }
  assert.equal(policy({ drafts: [pending('D1', '2026-10-12')] }).attentionRequired, false,
    'alternate-day cadence is retained, not replaced with a daily warning');
});

test('unresolved, balance-invalid, conflicting and recovery-locked submissions remain warnings', () => {
  const cases = [
    entry => { entry.view.unresolved = 1; },
    entry => { entry.view.canFinalize = false; },
    entry => { entry.view.balanceResolved = false; },
    entry => { entry.view.balanceDifference = 10; },
    entry => { entry.view.balanceDifference = NaN; },
    entry => { entry.view.statementSummary = { ...entry.draft.summary, validated: false }; },
    entry => { entry.view.statementSummary = { ...entry.draft.summary, from: '2026-10-01' }; },
    entry => { entry.view.reconciliationConflicts = [{ type: 'duplicate_ledger_link' }]; },
    entry => { entry.draft.originalStatementPeriod = { from: '2026-10-01' }; },
    entry => { entry.draft.summary.validated = false; },
    entry => { delete entry.draft.summary.validated; },
    entry => { entry.draft.summary.to = '2026-02-30'; },
    entry => { entry.approval.to = '2026-10-12'; },
    entry => { entry.approval.account = 'Another bank'; },
    entry => { entry.approval.draftId = 'Another draft'; },
    entry => { entry.approval.status = 'rejected'; },
    entry => { entry.view = null; }
  ];
  cases.forEach((change, index) => {
    const entry = pending(); change(entry);
    const result = policy({ drafts: [entry] });
    assert.equal(result.attentionRequired, true, 'scenario ' + index);
    assert.equal(result.status, 'review_pending', 'scenario ' + index);
  });
});

test('an unsubmitted sibling period cannot be hidden by a clean awaiting-Owner period', () => {
  const sibling = pending('D2'); sibling.approval = null;
  const result = policy({ drafts: [pending(), sibling] });
  assert.equal(result.attentionRequired, true);
  assert.equal(result.status, 'review_pending');
  assert.match(result.detail, /another saved period/);
});

test('legacy card review remains actionable beside a submitted bank-style card reconciliation', () => {
  const result = policy({ reminder: { ...reminder, type: 'credit_card', account: 'HDFC 7376' }, additionalReviewPending: true });
  assert.equal(result.attentionRequired, true);
  assert.equal(result.status, 'review_pending');
});

test('multiple clean submitted periods remain awaiting approval without modifying source objects', () => {
  const drafts = [pending(), pending('D2', '2026-10-12')], inputs = { reminder, drafts, owner: false, isDue: due };
  const before = JSON.stringify(inputs);
  const result = reconciliationReminderAttention(inputs);
  assert.equal(result.attentionRequired, false);
  assert.equal(result.status, 'approval_pending');
  assert.match(result.detail, /2 submitted periods/);
  assert.equal(JSON.stringify(inputs), before);
});

test('ordinary reminders retain attention and finalized up-to-date accounts do not gain warnings', () => {
  for (const status of ['statement_required', 'review_pending', 'report_required', 'verification_required']) {
    assert.equal(policy({ reminder: { ...reminder, status }, drafts: [] }).attentionRequired, true);
  }
  assert.equal(policy({ reminder: { ...reminder, status: 'up_to_date' }, drafts: [] }).attentionRequired, false);
});
