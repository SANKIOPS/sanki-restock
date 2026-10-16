'use strict';
const schema=require('./gokwik-report-schema');
function normalizeRows(rows){
 return rows.map(r=>{
  if(!r['Payment Id']||!r['Merchant Order Id'])throw new Error('GoKwik report requires payment and merchant order IDs');
  if(r['Transaction Type']&&String(r['Transaction Type']).toLowerCase()!=='payment')throw Error('Unsupported GoKwik transaction type; refunds and reversals require separate review.');
  const v=schema.amounts(r),gross=v.Amount,fees=v.Fee+v.Tax+v['Additional Fees']+v['Additional Tax'],credit=v.Credit,debit=v.Debit,deduction=v['gokwik Deduction'];
  return {paymentId:String(r['Payment Id']),orderId:String(r['Merchant Order Id']),orderNumber:String(r['Platform Order Id']||''),type:String(r['Transaction Type']),gross:gross/100,fees:fees/100,credit:credit/100,debit:debit/100,net:v.net/100,reportedGokwikDeduction:deduction/100,utr:String(r['Settlement UTR']||''),date:schema.date(r['Settlement Date']),provider:String(r['Settled By']||''),method:String(r['Payment Method']||''),bankVerified:false,requiresDeductionReview:deduction!==0,amountDifference:(gross-fees-credit)/100};
 });
}
function batches(rows){const groups=new Map();for(const row of rows){if(!row.utr)continue;const key=row.provider+'|'+row.utr+'|'+row.date;const b=groups.get(key)||{reference:row.utr,date:row.date,provider:row.provider,transactions:[],reportedNet:0,bankVerified:false};b.transactions.push(row);b.reportedNet=Math.round((b.reportedNet+row.net)*100)/100;groups.set(key,b);}return [...groups.values()];}
module.exports={normalizeRows,batches};
