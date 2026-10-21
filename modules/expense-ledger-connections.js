'use strict';

// Shared identities for the paying and receiving views of ONE movement.
// These helpers never create a payment or certify an unreconciled statement.
const money = value => Math.round(Number(value || 0) * 100);
function claimantAccount(expense) {
  return (expense.payments || []).find(payment => payment.personalFunds && payment.account)?.account || expense.claimantFundingAccount || '';
}
function canonicalMovementId(id) { return String(id || '').replace(/\/RECEIVED$/, ''); }
function linkedIds(row) {
  return [...new Set((row.linkedRecordIds || []).flatMap(id => String(id).split(/\s*,\s*/)).filter(Boolean))];
}
function records(store) {
  return Object.entries(store.bankStatements || {}).flatMap(([key, book]) => (book.imports || []).flatMap(record =>
    [...(record.reconciliationRows || []), ...(record.carriedReconciliationRows || [])].map(row => ({ key, book, record, row }))));
}
function finalizedLinks(store) {
  const result = new Map();
  for (const evidence of records(store)) {
    if (!evidence.record.finalizedAt || !evidence.row.bank || evidence.row.resolutionAction === 'bank_reversal_pair') continue;
    for (const id of linkedIds(evidence.row)) {
      const existing = result.get(id) || [];
      // A carried copy of the SAME bank transaction is evidence, not another payment.
      if (!existing.some(x => x.key === evidence.key && x.row.bank.transactionId && x.row.bank.transactionId === evidence.row.bank.transactionId)) existing.push(evidence);
      result.set(id, existing);
    }
  }
  return result;
}
function uniqueFinalizedLink(store, id, links = finalizedLinks(store)) {
  const candidates = links.get(canonicalMovementId(id)) || [];
  return candidates.length === 1 ? candidates[0] : null;
}
function movementOverride(store, id) {
  const overrides = store.bankDateOverrides || {};
  return overrides[id] || overrides[canonicalMovementId(id)] || null;
}
function evidenceStatus(evidence) {
  return { status: 'reconciled', reconciliationId: evidence.record.id, bankDate: evidence.row.bank.date || '', bankReference: evidence.row.bank.reference || '', finalizedAt: evidence.record.finalizedAt || '', finalizedBy: evidence.record.finalizedBy || '', sourceAccount: evidence.key.replace(/^PERSONAL\|/, '') };
}
function ownerPersonalTransfer(transfer) {
  // Only the explicitly selectable Owner bank counterparts qualify. This
  // recognition never grants access to a PERSONAL book or creates a movement.
  const fromNature=transfer.fromNature||transfer.nature,toNature=transfer.toNature||transfer.nature;
  const contribution=fromNature==='PERSONAL'&&toNature==='SANKI'&&transfer.classification==='owner_contribution';
  const withdrawal=fromNature==='SANKI'&&toNature==='PERSONAL'&&transfer.classification==='owner_withdrawal';
  const account=contribution?transfer.fromAccount:withdrawal?transfer.toAccount:'';
  return ['IndusInd Bank 7883','ICICI Bank 0992','ICICI Bank 0993'].includes(account);
}
function transferEvidence(store, transfer) {
  if (!transfer.bankStatementEvidence || (transfer.classification!=='internal_transfer'&&!ownerPersonalTransfer(transfer)) || !transfer.reconciliationDraft || !transfer.bankRowId || transfer.accountingExcluded) return null;
  if ((store.transfers || []).some(other => other !== transfer && !other.accountingExcluded && other.reconciliationDraft === transfer.reconciliationDraft && other.bankRowId === transfer.bankRowId)) return null;
  const candidates = records(store).filter(({ key, record, row }) => {
    const bank = row.bank;
    if (!record.finalizedAt || record.draftId !== transfer.reconciliationDraft || row.id !== transfer.bankRowId || !bank || bank.reversal || row.resolutionAction === 'bank_reversal_pair') return false;
    if(row.corrected||(/excluded|timing difference|failed payment/i).test(String(row.decision||'')))return false;
    const override=store.bankDateOverrides?.[transfer.id];
    if(override?.reconciliationDraft&&![record.id,record.draftId].includes(override.reconciliationDraft))return false;
    const incoming = money(bank.credit) > 0 && money(bank.debit) === 0, outgoing = money(bank.debit) > 0 && money(bank.credit) === 0;
    const nature = incoming ? transfer.toNature || transfer.nature : transfer.fromNature || transfer.nature;
    const account = incoming ? transfer.toAccount : transfer.fromAccount;
    const expectedKey = nature === 'PERSONAL' ? 'PERSONAL|' + account : account;
    const charge = confirmedCharge(store, transfer);
    if(ownerPersonalTransfer(transfer)&&charge&&(nature!=='SANKI'||charge.nature!=='SANKI'))return false;
    if ((!incoming && !outgoing) || key !== expectedKey || transfer.date !== bank.date || money(transfer.amount) + (outgoing && charge ? -money(charge.amount) : 0) !== money(bank.debit) + money(bank.credit)) return false;
    if (transfer.bankReference && transfer.bankReference !== bank.reference) return false;
    const ids = linkedIds(row);
    const expectedIds=[transfer.id,...(charge?[charge.id]:[])];
    return (!ids.length && !row.ledger?.id) || (ids.length===expectedIds.length&&expectedIds.every(id=>ids.includes(id)));
  });
  if (candidates.length !== 1) return null;
  const evidence = candidates[0];
  if (records(store).some(x => x.row !== evidence.row && linkedIds(x.row).includes(transfer.id) && x.key === evidence.key && x.row.bank?.transactionId !== evidence.row.bank.transactionId)) return null;
  return evidence;
}
function confirmedCharge(store, transfer) {
  const split=transfer.confirmedBankSplit, charge=split&&(store.adjustments||[]).find(x=>x.id===split.chargeId);
  return charge&&!charge.accountingExcluded&&charge.transferChargeFor===transfer.id&&charge.account===transfer.fromAccount&&charge.date===transfer.date&&charge.reconciliationDraft===transfer.reconciliationDraft&&charge.bankRowId===transfer.bankRowId&&money(charge.amount)===-money(split.charge)&&money(split.principal)===money(transfer.amount)&&money(split.gross)===money(split.principal)+money(split.charge)?charge:null;
}
function repairCreatedTransferLinks(store, audit) {
  let changed = false;
  for (const transfer of store.transfers || []) {
    const evidence = transferEvidence(store, transfer);
    if (!evidence || linkedIds(evidence.row).includes(transfer.id)) continue;
    const { row, record, key } = evidence, before = { linkedRecordIds: row.linkedRecordIds || [], ledger: row.ledger || null };
    const charge=confirmedCharge(store,transfer);
    row.linkedRecordIds = [transfer.id,...(charge?[charge.id]:[])];
    row.ledger = { id: transfer.id, date: transfer.date, description: ownerPersonalTransfer(transfer)?'Bank-confirmed '+transfer.classification.replaceAll('_',' '):'Bank-confirmed internal transfer', reference: transfer.bankReference || transfer.id, debit: money(row.bank.debit)>0?Number(transfer.amount):0, credit: money(row.bank.credit)>0?Number(transfer.amount):0 };
    if(charge){row.chargeAmount=-Number(charge.amount);row.chargeAdjustmentId=charge.id;}
    row.linkRepair = { rule: 'exact-created-transfer-provenance-v1', at: new Date().toISOString(), transferId: transfer.id, draftId: record.draftId, bankRowId: row.id };
    row.correctionHistory = row.correctionHistory || [];
    row.correctionHistory.push({ before, after: { linkedRecordIds: row.linkedRecordIds, ledger: row.ledger }, reason: 'Restore the existing transfer created from this exact finalized bank row; no money or decision changed.', by: 'system', at: row.linkRepair.at });
    store.bankDateOverrides = store.bankDateOverrides || {};
    if (!store.bankDateOverrides[transfer.id]) store.bankDateOverrides[transfer.id] = { bankDate: row.bank.date, originalDate: transfer.date, bankReference: row.bank.reference || '', reconciliationDraft: record.id, by: record.finalizedBy || 'system', at: record.finalizedAt, remark: 'Recovered exact finalized transfer link; no accounting movement created.' };
    if (audit) audit({ transfer, record, row, key, before });
    changed = true;
  }
  return changed;
}
function transferSplit(bank, principal, charge) {
  const gross = money(bank.debit) + money(bank.credit), p = principal == null ? gross : money(principal), c = money(charge);
  if (![gross,p,c].every(Number.isSafeInteger) || gross <= 0 || p <= 0 || c < 0 || p + c !== gross || ((money(bank.debit)>0)===(money(bank.credit)>0)) || (c > 0 && !(money(bank.debit) > 0 && money(bank.credit) === 0))) return null;
  return { principal: p / 100, charge: c / 100, gross: gross / 100 };
}

module.exports = { claimantAccount, canonicalMovementId, linkedIds, finalizedLinks, uniqueFinalizedLink, movementOverride, evidenceStatus, transferEvidence, repairCreatedTransferLinks, transferSplit };
