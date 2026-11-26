'use strict';

const list=value=>Array.isArray(value)?value:value?[value]:[];
const money=value=>Math.round(Number(value||0)*100)/100;
const text=value=>String(value==null?'':value).trim();

function proofs(...values){
  const out=[];
  const add=value=>{if(Array.isArray(value))return value.forEach(add);const item=text(value);if(item&&!out.includes(item))out.push(item);};
  values.forEach(add);return out;
}

function bankEvidence(expense,payment,allowExpenseEvidence=true){
  const p=payment||{};
  const exact=!!(p.bankReconciliationDraft&&p.bankReconciliationRow||p.bankReconciliationEvidence&&p.bankReconciliationEvidence.draftId);
  if(exact)return true;
  const activePayments=list(expense&&expense.payments).filter(item=>item&&!item.accountingExcluded&&money(item.amount)>0);
  // Old reconciled expenses sometimes keep the statement link on the expense
  // instead of its only payment. Never let that legacy shortcut hide a second,
  // unrelated payment that still needs its own proof.
  const mayUseExpenseEvidence=allowExpenseEvidence&&(!payment||activePayments.length<=1);
  return !!(mayUseExpenseEvidence&&(expense.statementBacked||expense.reconciliationSource&&expense.reconciliationSource.draftId||expense.bankReconciliationEvidence&&expense.bankReconciliationEvidence.draftId));
}

function noBillException(expense){
  if(expense.bill!=='none')return false;
  return !!((expense.exceptionReason||expense.billNote)&&(expense.exceptionEvidence||expense.purchasePaymentProof||list(expense.purchasePaymentProofs).length));
}

function proofGaps(expense){
  if(!expense||expense.status==='rejected'||expense.deletedAt)return[];
  const gaps=[];
  if(!proofs(expense.billPhotos,expense.billPhoto).length&&!noBillException(expense))gaps.push({kind:'bill',label:'Bill / invoice missing',paymentId:'',date:expense.date||'',account:'',amount:money(expense.amount)});
  const payments=list(expense.payments).filter(payment=>payment&&!payment.accountingExcluded&&money(payment.amount)>0);
  const expensePaymentProofs=proofs(expense.paymentProofs,expense.paymentProof),personalProofs=proofs(expense.purchasePaymentProofs,expense.purchasePaymentProof);
  payments.forEach((payment,index)=>{
    const direct=proofs(payment.proofs,payment.proof),legacy=index===payments.length-1?expensePaymentProofs:[],personal=payment.personalFunds&&payments.length===1?personalProofs:[];
    if(!direct.length&&!legacy.length&&!personal.length&&!bankEvidence(expense,payment))gaps.push({kind:'payment',label:'Payment proof missing',paymentId:text(payment.id),date:payment.date||expense.date||'',account:payment.account||expense.account||'',amount:money(payment.amount)});
  });
  if(!payments.length&&money(expense.paidAmount)>0&&!expensePaymentProofs.length&&!personalProofs.length&&!bankEvidence(expense,null))gaps.push({kind:'payment',label:'Payment proof missing',paymentId:'',date:expense.paidAt?String(expense.paidAt).slice(0,10):expense.date||'',account:expense.account||'',amount:money(expense.paidAmount)});
  list(expense.reimbursementPayments).filter(payment=>payment&&!payment.accountingExcluded&&money(payment.amount)>0).forEach(payment=>{
    if(!proofs(payment.proofs,payment.proof).length&&!bankEvidence(expense,payment,false))gaps.push({kind:'reimbursement',label:'Reimbursement proof missing',paymentId:text(payment.id),date:payment.date||'',account:payment.account||'',amount:money(payment.amount)});
  });
  return gaps;
}

function missingProofRow(expense){
  const gaps=proofGaps(expense);if(!gaps.length)return null;
  return{id:expense.id,nature:expense.nature||'SANKI',date:expense.date||'',vendor:expense.vendor||'',particulars:expense.particulars||'',category:expense.ledger||'',status:expense.status||'',amount:money(expense.amount),billPhotos:proofs(expense.billPhotos,expense.billPhoto),paymentProofs:proofs(expense.paymentProofs,expense.paymentProof,...list(expense.payments).map(payment=>proofs(payment&&payment.proofs,payment&&payment.proof))),gaps};
}

function attachProofs(expense,input,actor,at){
  const kind=text(input&&input.kind).toLowerCase(),paymentId=text(input&&input.paymentId),incoming=proofs(input&&input.proofs);
  if(!['bill','payment','reimbursement'].includes(kind))throw Error('Choose bill, payment or reimbursement proof.');
  if(!incoming.length||incoming.length>10||incoming.some(url=>!/^\/api\/expenses\/photo\/[A-Za-z0-9._-]+$/.test(url)))throw Error('Upload one or more valid proof files first.');
  const before=proofGaps(expense),append=(current)=>proofs(current,incoming);
  if(kind==='bill'){
    expense.billPhotos=append(proofs(expense.billPhotos,expense.billPhoto));expense.billPhoto=expense.billPhotos[0]||'';
  }else{
    const collection=kind==='reimbursement'?list(expense.reimbursementPayments):list(expense.payments);
    const target=paymentId?collection.find(payment=>text(payment&&payment.id)===paymentId):(collection.length===1?collection[0]:null);
    if(paymentId&&!target)throw Error('The selected payment record no longer exists. Refresh and try again.');
    if(target){target.proofs=append(proofs(target.proofs,target.proof));target.proof=target.proofs[0]||'';}
    else if(kind==='reimbursement')throw Error('Choose the reimbursement that needs proof.');
    if(kind==='payment'){
      if(!target||collection.length===1){expense.paymentProofs=append(proofs(expense.paymentProofs,expense.paymentProof));expense.paymentProof=expense.paymentProofs[0]||'';}
      if(target&&target.personalFunds){expense.purchasePaymentProofs=append(proofs(expense.purchasePaymentProofs,expense.purchasePaymentProof));expense.purchasePaymentProof=expense.purchasePaymentProofs[0]||'';}
    }
  }
  expense.proofHistory=Array.isArray(expense.proofHistory)?expense.proofHistory:[];
  expense.proofHistory.push({id:'PROOF-'+String(expense.proofHistory.length+1).padStart(3,'0'),kind,paymentId,proofs:incoming.slice(),note:text(input&&input.note)||'Proof uploaded later',by:actor,at});
  return{before,after:proofGaps(expense),added:incoming};
}

module.exports={proofs,bankEvidence,noBillException,proofGaps,missingProofRow,attachProofs};
