'use strict';
function paytmBatches(orders,accounting,payouts){
 const byId=new Map(orders.map(o=>[String(o.id),o]));
 return payouts.map(p=>({id:p.payoutId,provider:'Paytm',reference:p.utr,settlementDate:p.settledDate,status:p.reconciliationStatus?.label||'Not reconciled',statusCode:p.reconciliationStatus?.code||'',gross:p.gross,net:p.net,fees:Number(p.commission||0)+Number(p.platformFee||0)+Number(p.gst||0),otherDeductions:p.nonCustomerAmount||0,sourcePayoutIds:p.sourcePayoutIds||[],bankEvidence:p.bankMatched?(p.bankCandidates||[]).map(b=>({account:'Axis Bank 3448',date:b.date,amount:b.credit,reference:p.utr,transactionId:b.id})):[],payments:(p.transactionIds||[]).map(id=>{
  const t=(accounting.paytmReportTransactions||{})[id]||{},link=(accounting.paytmOrderLinks||{})[id],o=link&&byId.get(String(link.orderId)),manual=(accounting.paytmManualResolutions||{})[id],exclusion=(accounting.paytmExcludedTransactions||{})[id];
  return {transactionId:id,date:t.date,amount:t.amount,net:t.isCustomerPayment===false?-Number(t.amount||0):t.settledAmount,fees:Number(t.commission||0)+Number(t.platformFee||0)+Number(t.gst||0),orderId:o?String(o.id):'',orderNumber:o?.name||link?.orderNumber||'',orderTotal:o?.total??null,type:t.isCustomerPayment===false?'Adjustment':exclusion?'Excluded from order matching':manual?'Manually classified payment':'Customer payment',linkStatus:o?'Linked':exclusion?exclusion.reason||'Excluded':manual?manual.reason||'Manual classification':'Order link pending'};
 })}));
}
module.exports={paytmBatches};
