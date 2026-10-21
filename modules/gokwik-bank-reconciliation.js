'use strict';
const g = require('./gokwik-accounting');
const ref = value => String(value || '').toUpperCase().replace(/[^A-Z0-9]/g, '');

// Pure, draft-scoped plan. Never saves, approves, or creates accounting rows.
function plan(store, draft, orders) {
  const items=[], errors=[];
  if (!draft || draft.account!==g.BANK || (draft.nature || 'SANKI')!=='SANKI') return {items,errors,fingerprint:g.hash([])};
  for (const batch of g.batches(Object.values(store.gokwikTransactions || {}))) {
    if ((store.gokwikPostings || {})[batch.id]) continue;
    const rows=(draft.transactions || []).map((bank,index)=>({bank,rowId:'bank-'+index})).filter(({bank})=>ref([bank.reference,bank.description].join(' ')).includes(ref(batch.utr)));
    if (!rows.length) continue;
    try {
      if (rows.length!==1) throw Error('Settlement UTR occurs more than once in this statement.');
      const {bank,rowId}=rows[0];
      if ((draft.matchingExclusions || {})[rowId]) throw Error('This bank match was manually unmatched. Review the source or remove the exclusion before finalizing.');
      const checked=g.review(store,batch.id,orders);
      if (g.bankSignature(bank)!==checked.bank.signature) throw Error('The report does not match this bank row by exact date, UTR and net credit.');
      const official=Object.values(((store.bankStatements || {})[g.BANK] || {}).transactions || {});
      if (official.some(row=>g.bankSignature(row)===checked.bank.signature)) throw Error('This bank credit is already finalized. Review its existing accounting evidence; no settlement will be back-posted automatically.');
      const app={id:'GKW-PENDING/'+batch.id,date:bank.date,description:'GoKwik net settlement · '+batch.transactions.map(tx=>'#'+tx.orderNumber).join(', ')+' · awaits 0437 finalization',reference:batch.utr,proof:'/api/expenses/gokwik-reports/proof/'+encodeURIComponent(batch.id),credit:batch.net,debit:0,gokwikSettlementId:batch.id,pendingPosting:true};
      items.push({...checked,bank:{...checked.bank,locator:{kind:'draft',draftId:draft.id,rowId}},rowId,app});
    } catch(error) {
      errors.push({settlementId:batch.id,utr:batch.utr,bankRowIds:rows.map(row=>row.rowId),message:'GoKwik '+batch.utr+': '+error.message});
    }
  }
  return {items,errors,fingerprint:g.hash({items:items.map(x=>({id:x.batch.id,rowId:x.rowId,fingerprint:x.fingerprint})),errors})};
}

// Called only inside successful Owner bank finalization, before its single
// saveStore. The caller works on a private store snapshot; failure saves none.
function post(store, checked, {username, now, draftId, importId, registerVendor, event}) {
  const {batch,bank}=checked,id=batch.id;
  if ((store.gokwikPostings || {})[id]) throw Error('GoKwik settlement already posted. Reload the bank statement.');
  const proof='/api/expenses/gokwik-reports/proof/'+encodeURIComponent(id),sourceIds=[...new Set(batch.transactions.map(tx=>tx.importId))];
  const approval={requestedBy:username,requestedAt:now,approvedBy:username,approvedAt:now,reason:'Included in Owner-approved 0437 bank reconciliation',fingerprint:checked.fingerprint,deductionCategory:g.SUBSCRIPTION,deductionVendor:'GoKwik',deductionTreatment:'User-confirmed GoKwik service charge; final classification: Subscription Expense, vendor GoKwik',bankReconciliationDraft:draftId,bankImportId:importId};
  store.expenses=store.expenses || {};store.transfers=store.transfers || [];store.gokwikPostings=store.gokwikPostings || {};store.customLedgers=store.customLedgers || {};
  const vendor=registerVendor(store,'SANKI','GoKwik',{}),expenseIds=[];
  if (store.sankiCategoryCatalog?.categories&&!store.sankiCategoryCatalog.categories.some(c=>String(c.name).toLowerCase()===g.SUBSCRIPTION.toLowerCase())) store.sankiCategoryCatalog.categories.push({name:g.SUBSCRIPTION,group:'Subscriptions',type:'fixed',createdBy:username,createdAt:now});
  function expense(total,ledger,particulars,type) {
    if (g.money(total)<=0) return;
    do {store.seq=Number(store.seq || 0)+1;} while(store.expenses['EX-'+String(store.seq).padStart(5,'0')]);
    const expenseId='EX-'+String(store.seq).padStart(5,'0');
    const payment={id:'PAY-001',date:bank.date,amount:total,account:g.CLEARING,paymentType:'Settlement deduction',proof,proofs:[proof],bankReference:batch.utr,paidBy:username,paidAt:now,personalFunds:false,gokwikPostingId:id};
    const record={id:expenseId,nature:'SANKI',channel:'Website',date:bank.date,amount:total,requestedAmount:total,paidAmount:total,status:'paid',approvedAt:now,approvedBy:username,createdAt:now,createdBy:username,claimant:username,vendor,ledger,type,particulars,account:g.CLEARING,fundedBy:'company',paidAlready:false,paymentType:payment.paymentType,payments:[payment],paymentProof:proof,billPhoto:proof,billPhotos:[proof],bill:'statement',exceptionEvidence:'Original GoKwik settlement report and exact 0437 bank credit',exceptionReason:'Owner approved report-backed deduction with 0437 reconciliation',gokwikPostingId:id,gokwikSourceIds:sourceIds,paidAt:now,paidBy:username};
    store.expenses[expenseId]=record;store.customLedgers[ledger]=store.customLedgers[ledger] || {name:ledger,type};expenseIds.push(expenseId);event('GOKWIK_EXPENSE_POSTED',expenseId,{after:record});
  }
  expense((g.money(batch.fee)+g.money(batch.tax)+g.money(batch.additionalFee)+g.money(batch.additionalTax))/100,'BANK CHARGES','GoKwik processing fee and tax · UTR '+batch.utr,'running');
  expense(batch.deduction,g.SUBSCRIPTION,'GoKwik service charge · Subscription Expense · UTR '+batch.utr,'fixed');
  do {store.transferSeq=Number(store.transferSeq || 0)+1;} while(store.transfers.some(tx=>tx.id==='TR-'+String(store.transferSeq).padStart(5,'0')));
  const transfer={id:'TR-'+String(store.transferSeq).padStart(5,'0'),nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',classification:'internal_transfer',fromAccount:g.CLEARING,toAccount:g.BANK,amount:batch.net,date:bank.date,bankReference:batch.utr,reference:batch.utr,proof,proofs:[proof],note:'GoKwik net settlement · '+batch.transactions.map(tx=>'#'+tx.orderNumber).join(', '),gokwikPostingId:id,createdAt:now,createdBy:username};
  store.transfers.push(transfer);
  const posting={id,utr:batch.utr,gross:batch.gross,net:batch.net,fee:batch.fee,tax:batch.tax,additionalFee:batch.additionalFee,additionalTax:batch.additionalTax,deduction:batch.deduction,date:bank.date,bankSignature:bank.signature,bankLocator:bank.locator,transferId:transfer.id,expenseIds,sourceIds,approval,postedBy:username,postedAt:now,bankReconciliationDraft:draftId,bankImportId:importId};
  store.gokwikPostings[id]=posting;if(store.gokwikApprovals)delete store.gokwikApprovals[id];
  event('GOKWIK_SETTLEMENT_POSTED',id,{after:posting,note:'Posted atomically with Owner finalization of 0437; no separate settlement approval.'});
  return posting;
}
module.exports={plan,post};
