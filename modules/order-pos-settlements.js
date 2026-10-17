'use strict';
// Consume the accounting screen's canonical payout projection, including merged
// payout IDs and audited historical attachments. Never create another posting.
function attach(orders,accounting,payouts){
 const latestReportDate=Object.values(accounting.paytmReportTransactions||{}).filter(t=>t.isCustomerPayment!==false).map(t=>t.date).filter(Boolean).sort().at(-1)||'';
 const byTransaction=new Map();
 for(const p of payouts)for(const id of p.transactionIds||[]){const list=byTransaction.get(String(id))||[];list.push(p);byTransaction.set(String(id),list);}
 for(const o of orders){if(o.channel!=='POS')continue;
  o.posClearance=Object.entries(accounting.paytmOrderLinks||{}).filter(([,l])=>String(l.orderId)===String(o.id)).map(([transactionId,link])=>{
   const tx=(accounting.paytmReportTransactions||{})[transactionId]||{},matches=byTransaction.get(transactionId)||[],p=matches.length===1?matches[0]:null,s=p?.reconciliationStatus;
   const reconciled=s?.code==='posted',proof=reconciled&&p.bankMatched&&(p.bankCandidates||[]).length===1?p.bankCandidates[0]:null;
   return {transactionId,payoutId:p?.payoutId||tx.payoutId||'',settlementId:p?.settlementId||'',statusCode:s?.code||'unlinked',status:proof?'Received in bank · reconciled':reconciled?'Reconciled · saved bank posting':s?.code==='finalized'?'Paytm review finalized · bank link pending':s?.code==='ready'?'Ready for reconciliation':matches.length>1?'Conflicting payout allocations · review required':'Linked · reconciliation pending',reference:p?.utr||'',amount:Number(tx.amount??link.amount)||null,batchNet:p?.net??null,bankVerified:!!proof,bankEvidence:proof?{account:'Axis Bank 3448',date:proof.date,amount:proof.credit,reference:p.utr,transactionId:proof.id,reconciliationImportId:s.historicalEvidenceLink?.reconciliationImportId||''}:null,reconciledAt:s?.finalizedAt||null};
  });
  const links=o.posClearance;
  const cashPayments=(o.payments||[]).filter(p=>/^cash$/i.test(p.gateway||'')),cashAmount=cashPayments.reduce((n,p)=>n+Math.round(Number(p.amount||0)*100),0)/100;
  const orderDay=String(o.createdAt||'').slice(0,10),closing=(accounting.cashReconciliations||[]).filter(r=>r.status==='approved'&&r.account==='Counter Cash'&&r.verifiedThrough>=orderDay&&orderDay).sort((a,b)=>String(a.verifiedThrough).localeCompare(String(b.verifiedThrough)))[0];
  const cashLeg=cashAmount>0?{provider:'Counter cash',amount:cashAmount,status:closing?'Cash closing verified':'Cash received · closing pending',bankVerified:false,references:closing?[closing.id||closing.verifiedThrough]:[],bankEvidence:[]}:null;
  if(!links.length){if(cashLeg&&(o.payments||[]).every(p=>/^cash$/i.test(p.gateway||''))){o.bankReceiptStatus=Number(o.refunded||0)>=cashAmount?'Cash payment refunded':closing?'Cash received · closing verified':'Cash received · closing pending';o.settlementStatus=o.bankReceiptStatus;o.moneyJourney=[cashLeg];}else if((o.payments||[]).some(p=>Number(p.amount)>0)){o.bankReceiptStatus='Paytm transaction link pending';o.settlementStatus=o.bankReceiptStatus;o.paymentEvidenceNote=latestReportDate&&orderDay>latestReportDate?'The imported Paytm payment report ends '+latestReportDate+'. A later Paytm report is needed to identify this payment and its payout.':'No confirmed Paytm transaction link is available for this order. Bank reconciliation coverage alone does not establish the payment-to-order link.';}continue;}
  const verified=links.every(l=>l.bankVerified),allReconciled=links.every(l=>l.statusCode==='posted');
  // A linked component does not establish that every tender in a mixed sale cleared.
  const linkedAmount=links.reduce((n,l)=>n+Math.round(Number(l.amount||0)*100),0),total=Math.round(Number(o.total||0)*100);
  o.bankReceiptStatus=verified?(linkedAmount>=total?'Received in bank · reconciled':'Part received in bank'):allReconciled?'Reconciled · saved bank posting':links.some(l=>l.bankVerified)?'Part received in bank':links.some(l=>l.statusCode==='finalized')?'Paytm review finalized · bank link pending':'POS reconciliation pending';
  if(cashLeg&&verified&&Math.round((cashAmount+linkedAmount/100)*100)>=total)o.bankReceiptStatus=closing?'Paytm in bank · cash closing verified':'Paytm in bank · cash closing pending';
  o.settlementStatus=o.bankReceiptStatus;
  o.moneyJourney=links.map(l=>({provider:'Paytm',amount:l.amount,paymentId:l.transactionId,batchId:l.payoutId,batchNet:l.batchNet,status:l.status,bankVerified:l.bankVerified,references:l.reference?[l.reference]:[],bankEvidence:l.bankEvidence?[l.bankEvidence]:[]}));
  if(cashLeg)o.moneyJourney.push(cashLeg);
  o.bankEvidence=[...new Map([...(o.bankEvidence||[]),...links.map(l=>l.bankEvidence).filter(Boolean)].map(b=>[b.account+'|'+b.transactionId,b])).values()];
 }
 return orders;
}
module.exports={attach};
