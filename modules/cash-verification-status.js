'use strict';

const finite = value => value !== null && value !== undefined && String(value).trim() !== '' && Number.isFinite(Number(value));
const money = value => '₹' + Number(value).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

// Preserve the submitted snapshot. Current validation is a separate, read-only
// fact shared by the reminder and both roles' cash-verification screens.
function cashVerificationStatus(row, balanceAt) {
  const current = balanceAt(row.verifiedThrough);
  const valid = [row.countedBalance, row.ledgerBalance, row.difference, current].every(finite);
  const currentLedgerBalance = finite(current) ? Math.round(Number(current)) : null;
  const currentDifference = valid ? Math.round(Number(row.countedBalance) - currentLedgerBalance) : null;
  const ledgerChanged = valid && Math.round(Number(row.ledgerBalance)) !== currentLedgerBalance;
  const reviewReasons = [];
  const date = String(row.verifiedThrough || ''), parsed = new Date(date + 'T00:00:00Z');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== date) reviewReasons.push('The verification date is missing or invalid.');
  if (!valid) reviewReasons.push('The submitted count or ledger balance is missing or invalid; review the original verification.');
  if (finite(row.countedBalance) && Number(row.countedBalance) < 0) reviewReasons.push('A physical cash count cannot be negative.');
  if (ledgerChanged) reviewReasons.push('Ledger changed after submission: submitted ledger ' + money(row.ledgerBalance) + '; current ledger ' + money(currentLedgerBalance) + '.');
  if (currentDifference) reviewReasons.push('Submitted physical count differs from the current ledger by ' + money(currentDifference) + '.');
  if (finite(row.difference) && Math.round(Number(row.difference)) !== 0) reviewReasons.push('The original submission recorded a difference of ' + money(row.difference) + '.');
  return { ...row, currentLedgerBalance, currentDifference, ledgerChanged, reviewRequired: reviewReasons.length > 0, reviewReasons };
}

function cashVerificationView(rows, balanceAt) {
  const history = rows.slice().sort((a, b) => String(b.verifiedThrough + b.requestedAt).localeCompare(String(a.verifiedThrough + a.requestedAt)));
  const latest = history.find(row => row.status === 'approved') || null;
  const pending = history.filter(row => row.status === 'pending').map(row => cashVerificationStatus(row, balanceAt));
  const review = pending.filter(row => row.reviewRequired);
  const counts = { pending: pending.length, reviewRequired: review.length, awaitingOwnerApproval: pending.length - review.length };
  const detail = pending.map(row => row.id + ' (' + row.verifiedThrough + '): ' +
    (row.reviewRequired ? row.reviewReasons.join(' ') : 'Count agrees with the current ledger; awaiting Owner approval.')).join(' ');
  return { account: 'Counter Cash', reconciledThrough: latest && latest.verifiedThrough || '', latest, pending, history: history.slice(0, 20),
    reconciliationStatus: { code: review.length ? 'review_required' : pending.length ? 'approval_pending' : latest ? 'approved' : 'verification_required', counts, detail } };
}

module.exports = { cashVerificationStatus, cashVerificationView };
