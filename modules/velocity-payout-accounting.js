'use strict';
const crypto=require('crypto'),{bankEvidence}=require('./order-settlement-links');
const money=v=>Math.round(Number(v||0)*100),ref=v=>String(v||'').toUpperCase().replace(/[^A-Z0-9]/g,'');
const BANK='Axis Bank 3448',CLEARING='Velocity',WALLET='Velocity Shipping Wallet';
// Project only complete, bank-proven payouts; never post a new sale or expense.
function batches(accounting,orders,sales,payouts,from='2026-08-21'){
 const byAwb=new Map(),byOrder=new Map();
 for(const o of orders){for(const s of [...(o.shipments||[]),...(o.fulfillments||[])]){const awb=String(s.awb||s.tracking_number||'');if(!awb)continue;const ids=byAwb.get(awb)||new Set();ids.add(String(o.id));byAwb.set(awb,ids);}}
 for(const sale of sales){if(sale.account!==CLEARING)continue;const rows=byOrder.get(String(sale.orderId))||[];rows.push(sale);byOrder.set(String(sale.orderId),rows);}
 const result=[];
 for(const b of payouts){
  if(b.date<from||b.status!=='settled'||!b.allocationsComplete||b.credits||b.otherDeduction)continue;
  const proof=bankEvidence(accounting,b.utr,b.net,{account:BANK,date:b.date});if(!proof)continue;
  const linked=new Map();let valid=true;
  for(const e of b.entries||[]){const ids=byAwb.get(e.awb);if(e.type!=='credit'||ids?.size!==1){valid=false;break;}const id=[...ids][0];linked.set(id,(linked.get(id)||0)+money(e.amount));}
  if(!valid||!linked.size)continue;
  const linkedSales=[];for(const [id,amount] of linked){const rows=byOrder.get(id)||[];if(rows.reduce((n,r)=>n+money(r.amount),0)!==amount){valid=false;break;}linkedSales.push(...rows);}
  if(!valid)continue;
  const key='UTR:'+ref(b.utr).toLowerCase(),id='VELOCITY-'+crypto.createHash('sha1').update(key).digest('hex').slice(0,12);
  const existing=(accounting.transfers||[]).filter(t=>!t.accountingExcluded&&t.fromAccount===CLEARING&&t.toAccount===BANK&&ref(t.bankReference||t.reference)===ref(b.utr));
  if(existing.length>1||existing.some(t=>money(t.amount)!==money(b.net)))continue;
  // A previously recorded receipt or bank-truth entry must be reviewed in place,
  // never supplemented with a second bank credit.
  const other=[...(accounting.receipts||[]),...(accounting.bankTruthMovements||[]),...(accounting.adjustments||[])].some(r=>r.account===BANK&&(ref(r.bankReference||r.reference)===ref(b.utr)||r.bankTransactionId===proof.transactionId&&proof.transactionId));
  if(other)continue;

  result.push({id,date:proof.date,reference:b.utr,batchId:b.batchId,netAmount:b.net,grossAmount:b.gross,chargeAmount:0,walletOffset:b.walletOffset,orderIds:[...linked.keys()],linkedSales,entries:b.entries,complete:true,ordersLinked:true,source:'velocity_mcp',bankEvidence:proof,manualTransferId:existing[0]?.id||''});
 }
 return result;
}
module.exports={batches,BANK,CLEARING,WALLET};
