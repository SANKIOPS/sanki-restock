'use strict';
const money=v=>Math.round(Number(v||0)*100);
const ref=v=>String(v||'').toUpperCase().replace(/[^A-Z0-9]/g,'');
function bankEvidence(accounting, reference, expected){
 const target=ref(reference);if(target.length<8)return null;
 const found=[];
 for(const [bookKey,book] of Object.entries(accounting.bankStatements||{}))for(const row of Object.values(book.transactions||{})){
  const text=ref([row.reference,row.description,row.narration,row.utr].filter(Boolean).join(' '));
  if(text.includes(target)&&money(row.credit)===money(expected)&&Number(row.credit)>0)found.push({account:book.account||bookKey,date:row.date,reference,amount:Number(row.credit),transactionId:row.id});
 }
 return found.length===1?found[0]:null;
}
function attachSettlementLinks(orders,accounting,online,finance){
 const groups=new Map();
 for(const row of online){
  if(!row.utr)continue;const key='online|'+row.utr,g=groups.get(key)||{reference:row.utr,net:0,invalid:false};
  let net;
  if(row.credit!=null&&row.reportedGokwikDeduction!=null){
   net=(money(row.credit)-money(row.reportedGokwikDeduction))/100;
   if(net<0||(money(row.debit)!==0&&money(row.debit)!==money(net)))g.invalid=true;
  }else{net=Number(row.net);if(!Number.isFinite(net)||net<0)g.invalid=true;}
  g.net+=Number.isFinite(net)?net:0;groups.set(key,g);
 }
 // COD API provides gross collection, not net batch deductions. Do not infer
 // the net bank payout by summing collections. Only imported payout data can
 // establish an expected COD batch total.
 for(const order of orders){
  order.bankEvidence=[];
  for(const row of order.onlineEvidence||[]){const group=groups.get('online|'+row.utr);const match=group&&!group.invalid&&bankEvidence(accounting,group.reference,group.net);if(match&&!order.bankEvidence.some(x=>x.reference===match.reference))order.bankEvidence.push(match);}
  const links=Object.entries(accounting.paytmOrderLinks||{}).filter(([,v])=>String(v.orderId)===String(order.id));
  order.posClearance=links.map(([transactionId,link])=>{
   const tx=(accounting.paytmReportTransactions||{})[transactionId]||{};
   const posting=(accounting.paytmPayoutPostings||[]).find(p=>String(p.payoutId)===String(tx.payoutId));
   return {transactionId,payoutId:tx.payoutId||'',status:posting?'Posted through existing POS reconciliation':'Linked · bank clearance pending',reference:posting&&posting.utr||'',amount:Number(tx.amount)||null};
  });
  if(order.channel==='POS'&&order.posClearance.length)order.settlementStatus='Existing POS reconciliation linked';
  if(order.bankEvidence.length)order.settlementStatus='Bank credit matched · batch allocation review pending';
 }
 return orders;
}
module.exports={attachSettlementLinks,bankEvidence};
