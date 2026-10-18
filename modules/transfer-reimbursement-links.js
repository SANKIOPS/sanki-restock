'use strict';

// A settlement allocation is not another bank movement. The original transfer
// remains the sole cash entry, with its original proof and finalized bank link.
const connections = require('./expense-ledger-connections');
const cents = value => Math.round(Number(value || 0) * 100);
const postsMovement = payment => !payment.accountingExcluded && !payment.settledByTransferId;
const allocations = (store, id) => Object.values(store.expenses || {}).flatMap(expense =>
  (expense.reimbursementPayments || []).filter(payment => payment.settledByTransferId === id)
    .map(payment => ({ expenseId: expense.id, paymentId: payment.id, amount: Number(payment.amount) })));
const protectsTransfer = (store, id) => allocations(store, id).length > 0;
const protectsMovement = (store, id) => protectsTransfer(store,id) || Object.values(store.expenses || {}).some(e =>
  (e.reimbursementPayments || []).some(p=>p.settledByTransferId) && (id===e.id || [...(e.payments||[]),...(e.reimbursementPayments||[])].some(p=>e.id+'/'+p.id===id)));

function validate(store, expense, transfer, pending) {
  if (!expense || !transfer) return { error: 'Expense or transfer not found.' };
  if (!expense.approvedAt || expense.status === 'rejected' || !['pending', 'partially_reimbursed'].includes(expense.reimbursementStatus)) return { error: 'An approved pending reimbursement is required.' };
  const amount = cents(pending);
  if (!Number.isSafeInteger(amount) || amount <= 0 || amount !== cents(transfer.amount)) return { error: 'The existing transfer principal must exactly equal the pending reimbursement. Use a reviewed allocation for other amounts.' };
  if (transfer.accountingExcluded || transfer.classification !== 'internal_transfer' || transfer.salaryAdvanceId || transfer.confirmedBankSplit && cents(transfer.confirmedBankSplit.principal) !== amount || transfer.routedThroughIntermediary || transfer.settledThroughPaytm) return { error: 'Only an active direct internal transfer of the verified principal can be linked.' };
  const recipient = connections.claimantAccount(expense);
  if (!recipient || recipient !== transfer.toAccount || transfer.fromAccount === recipient) return { error: 'The transfer recipient must be the account that personally funded this expense.' };
  if (protectsTransfer(store, transfer.id) || (transfer.reimbursementAllocations || []).length) return { error: 'This transfer is already allocated to a reimbursement.' };
  const evidence = connections.uniqueFinalizedLink(store, transfer.id);
  if (!evidence || !evidence.record.finalizedAt || evidence.row.corrected || !evidence.row.ledger || evidence.row.ledger.id !== transfer.id) return { error: 'A unique finalized source-bank link is required; amount-only matches are not accepted.' };
  const { row, record, key } = evidence;
  const sourceKey = (transfer.fromNature || transfer.nature) === 'PERSONAL' ? 'PERSONAL|' + transfer.fromAccount : transfer.fromAccount;
  if (key !== sourceKey || !row.bank || row.bank.reversal || !(cents(row.bank.debit) >= amount) || cents(row.bank.credit) || cents(row.ledger.debit) !== amount || cents(row.ledger.credit) || row.bank.date !== transfer.date || /excluded|failed payment|timing difference/i.test(String(row.decision || ''))) return { error: 'The finalized bank evidence must agree with this transfer’s source, direction, date and principal.' };
  if (!transfer.proof && !(transfer.proofs || []).length) return { error: 'The existing transfer must retain payment proof.' };
  return { transferId: transfer.id, amount: amount / 100, date: row.bank.date, account: transfer.fromAccount, recipient, sourceNature: transfer.fromNature || transfer.nature, recordId: record.id, bankRowId: row.id, bankReference: row.bank.reference || transfer.bankReference || '' };
}

function apply(store, expense, transfer, plan, reason, by, at = new Date().toISOString()) {
  const payments = expense.reimbursementPayments || [];
  const history=payments.concat((expense.reimbursementReversals||[]).map(r=>r.payment).filter(Boolean));
  const seq = history.reduce((max, p) => Math.max(max, Number(String(p.id).replace(/^REIM-/, '')) || 0), 0) + 1;
  const id = 'REIM-' + String(seq).padStart(3, '0');
  const proofs = [...new Set([...(transfer.proofs || []), transfer.proof].filter(Boolean))];
  const payment = { id, amount: plan.amount, date: plan.date, account: plan.account, accountNatures: [plan.sourceNature], paymentType: 'Existing transfer', proof: proofs[0], proofs, note: reason, paidBy: by, paidAt: at, settledByTransferId: transfer.id, transferAllocationEvidence: { recordId: plan.recordId, bankRowId: plan.bankRowId, bankReference: plan.bankReference, recipient: plan.recipient } };
  expense.reimbursementPayments = payments.concat(payment);
  expense.reimbursementAmount = expense.reimbursementPayments.reduce((sum, p) => sum + cents(p.amount), 0) / 100;
  transfer.reimbursementAllocations = [{ expenseId: expense.id, paymentId: id, amount: plan.amount, reason, linkedBy: by, linkedAt: at }];
  return payment;
}

module.exports = { postsMovement, allocations, protectsTransfer, protectsMovement, validate, apply };
