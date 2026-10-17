'use strict';

function validDate(value) {
  const text = String(value || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return '';
  const parsed = new Date(text + 'T00:00:00Z');
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === text ? text : '';
}

// Workflow completion and final approval are different facts. A submitted
// period can stop prompting its reconciler without becoming a finalized book.
// This policy is pure: views must be computed on detached, read-only snapshots.
function reconciliationReminderAttention({ reminder, owner = false, drafts = [], additionalReviewPending = false, isDue }) {
  const result = { ...reminder, attentionRequired: reminder.status !== 'up_to_date' };
  if (!drafts.length) return result;

  const approvalStates = drafts.filter(({ approval }) => approval && approval.status === 'pending');
  const ready = ({ draft, approval, view }) => {
    const summary = draft && draft.summary || {};
    return !!(draft && !draft.originalStatementPeriod && summary.validated === true &&
      validDate(summary.from) && validDate(summary.to) && summary.from <= summary.to &&
      approval && approval.status === 'pending' && approval.draftId === draft.id &&
      String(approval.from || '') === summary.from && String(approval.to || '') === summary.to &&
      (!approval.account || approval.account === draft.account) &&
      view && view.canFinalize === true && view.unresolved === 0 && view.balanceResolved === true &&
      (!view.statementSummary || view.statementSummary.validated === true &&
        view.statementSummary.from === summary.from && view.statementSummary.to === summary.to) &&
      Number.isFinite(view.balanceDifference) && Math.abs(view.balanceDifference) < .01 &&
      !(view.reconciliationConflicts || []).length);
  };
  if (additionalReviewPending || !drafts.every(ready)) {
    result.status = 'review_pending';
    result.attentionRequired = true;
    if (approvalStates.length) {
      result.approvalPending = true;
      result.actionLabel = 'Review reconciliation before Owner approval';
      result.detail = 'A submitted or another saved period still needs review; Owner approval does not clear unresolved entries or balance differences.';
    }
    return result;
  }

  const submittedThrough = drafts.map(({ draft }) => draft.summary.to).sort().at(-1);
  const effectiveThrough = [validDate(reminder.reconciledThrough), submittedThrough].filter(Boolean).sort().at(-1);
  const needsStatement = isDue(effectiveThrough, reminder.cadenceDays);
  const periodLabel = drafts.length === 1
    ? drafts[0].draft.summary.from + ' to ' + submittedThrough
    : drafts.length + ' submitted periods through ' + submittedThrough;
  const cash = reminder.type === 'cash';
  return {
    ...result,
    // Keep the actual finalized cutoff and pending workflow visible to callers.
    status: needsStatement ? (cash ? 'verification_required' : 'statement_required') : 'approval_pending',
    pendingDraft: true,
    approvalPending: true,
    awaitingOwnerApproval: true,
    submittedThrough,
    needsStatement,
    attentionRequired: owner || needsStatement,
    actionLabel: needsStatement
      ? (cash ? (owner ? 'Approve cash verification; newer count required' : 'Newer cash verification required')
        : owner ? 'Approve reconciliation; newer statement required' : 'Newer statement required')
      : (owner ? (cash ? 'Approve cash verification' : 'Approve / finalize reconciliation') : 'Submitted — awaiting Owner approval'),
    detail: 'Completed review: ' + periodLabel + '. ' +
      (needsStatement ? (cash ? 'A newer physical cash count is due after the submitted verification.' : 'New statement review is due after the submitted period.') :
        owner ? 'The reconciler has finished; final Owner approval is required.' : 'No further review is required from the admin while Owner approval is pending.')
  };
}

module.exports = { reconciliationReminderAttention };
