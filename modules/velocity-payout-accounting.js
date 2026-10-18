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
function projectCollections(rows,orders,payouts,from='2026-08-21'){
 const result=rows.map(r=>({...r})),byAwb=new Map(),collections=new Map(),seen=new Set();
 for(const o of orders)for(const ship of [...(o.shipments||[]),...(o.fulfillments||[])]){const awb=String(ship.awb||ship.tracking_number||'');if(!awb)continue;const ids=byAwb.get(awb)||new Set();ids.add(String(o.id));byAwb.set(awb,ids);}
 for(const b of payouts){if(b.status!=='settled'||!b.allocationsComplete)continue;for(const e of b.entries||[]){if(e.type!=='credit'||seen.has(e.awb))continue;const ids=byAwb.get(e.awb);if(ids?.size!==1)continue;seen.add(e.awb);const id=[...ids][0],g=collections.get(id)||{amount:0,date:'',awbs:[]};g.amount+=money(e.amount);const date=String(e.deliveryDate||b.date).slice(0,10);if(!g.date||date<g.date)g.date=date;g.awbs.push(e.awb);collections.set(id,g);}}
 for(const o of orders){const c=collections.get(String(o.id));if(!c||c.date<from||o.cancelledAt||['refunded','partially_refunded'].includes(o.financialStatus)||Number(o.moneyRefunded||o.refundAmount)>0||o.channel==='POS'||['2720','2717'].includes(String(o.orderNumber||o.name||'').replace(/^#/,'')))continue;const existing=result.filter(r=>String(r.orderId)===String(o.id));if(existing.some(r=>r.account===CLEARING||r.manualCashCollected)||Object.keys(o.saleAllocationOverrides||{}).length)continue;
  // An explicit account allocation or another verified receipt must never be
  // silently overwritten. Only fill an entirely absent courier receipt.
  if(existing.some(r=>r.saleAllocation||r.componentAmountOverride))continue;
  if(c.amount+existing.reduce((n,r)=>n+money(r.amount),0)>money(o.total))continue;
  result.push({id:'VELOCITY-COLLECTION/'+o.id,orderId:String(o.id),orderNumber:String(o.orderNumber||o.name||'').replace(/^#/,''),date:c.date,account:CLEARING,amount:c.amount/100,originalAmount:c.amount/100,gross:Number(o.total),allocationPart:'cod',reference:c.awbs.join(', '),source:'velocity_mcp',description:'Velocity verified COD collection · '+(o.name||o.id)});
 }
 return result;
}
module.exports={batches,projectCollections,BANK,CLEARING,WALLET};
