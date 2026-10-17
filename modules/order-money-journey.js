'use strict';
const {bankEvidence}=require('./order-settlement-links');
const cents=v=>Math.round(Number(v||0)*100),ref=v=>String(v||'').toUpperCase().replace(/[^A-Z0-9]/g,'');
function attach(orders,accounting,online,payouts){
 const onlineGroups=new Map();
 for(const r of online){if(!r.utr)continue;const key=ref(r.utr),g=onlineGroups.get(key)||{net:0,valid:true,utr:r.utr,date:r.date};if(!Number.isFinite(Number(r.net)))g.valid=false;else g.net+=Math.round(Number(r.net)*10000);onlineGroups.set(key,g);}
 const ownership=new Map();for(const o of orders)for(const s of o.shipments||[]){const ids=ownership.get(s.awb)||new Set();ids.add(String(o.id));ownership.set(s.awb,ids);}
 for(const o of orders){
  const legs=[],payments=new Map();
  for(const r of o.onlineEvidence||[]){if(r.type!=='Payment')continue;const key=r.paymentId||r.referenceId;if(!key)continue;const p=payments.get(key)||{amount:0,rows:[]};p.amount=Math.max(p.amount,cents(r.gross));p.rows.push(r);payments.set(key,p);}
  for(const [paymentId,p] of payments){const matches=p.rows.map(r=>{const g=onlineGroups.get(ref(r.utr));return g?.valid?bankEvidence(accounting,g.utr,g.net/10000,{account:'Tiana Traders IndusInd 0437',date:g.date}):null;});const verified=matches.length>0&&matches.every(Boolean);legs.push({provider:'GoKwik',paymentId,amount:p.amount/100,status:verified?'Received in bank':p.rows.some(r=>r.utr)?'Payout sent · bank confirmation pending':'With GoKwik',bankVerified:verified,bankEvidence:verified?matches:[],references:[...new Set(p.rows.map(r=>r.utr).filter(Boolean))]});}
  o.velocityPayouts=[];
  for(const b of payouts){const entries=(b.entries||[]).filter(e=>(o.shipments||[]).some(s=>s.awb===e.awb)&&ownership.get(e.awb)?.size===1);if(!entries.length)continue;
   const gross=entries.reduce((n,e)=>n+(e.type==='credit'?1:-1)*cents(e.amount),0);const valid=b.allocationsComplete&&b.status==='settled'&&entries.every(e=>e.type==='credit')&&gross>0;
   const match=valid?bankEvidence(accounting,b.utr,b.net,{account:'Axis Bank 3448',date:b.date}):null;
   const {fingerprint,...safe}=b;o.velocityPayouts.push({...safe,entries,orderCodAmount:gross/100,bankVerified:!!match,bankEvidence:match});
   legs.push({provider:'Velocity',amount:gross/100,batchId:b.batchId,status:match?'Received in bank':b.status==='settled'?'Payout sent · bank confirmation pending':'With Velocity',bankVerified:!!match,references:[b.utr],bankEvidence:match?[match]:[],batchNet:b.net,batchDeduction:b.deduction,deductionLabel:b.deductionLabel});
  }
  const linkedAwbs=new Set(o.velocityPayouts.flatMap(b=>b.entries.map(e=>e.awb)));
  for(const r of o.codEvidence||[]){if(r.error||linkedAwbs.has(r.awb))continue;legs.push({provider:'Velocity',amount:r.amount,awb:r.awb,status:r.utr?'Payout reported · batch breakdown pending':r.deliveryDate?'With Velocity':'Customer COD pending',bankVerified:false,references:r.utr?[r.utr]:[],bankEvidence:[]});}
  o.moneyJourney=legs;
  const covered=legs.reduce((n,l)=>n+cents(l.amount),0),total=cents(o.total),allVerified=legs.length>0&&legs.every(l=>l.bankVerified)&&covered===total&&total>0;
  o.bankReceiptStatus=allVerified?'Received in bank':legs.some(l=>l.bankVerified)?'Part received in bank':legs.some(l=>l.references.length)?'Payout sent · bank confirmation pending':legs.length?'With payment provider':'Payment evidence pending';
  if(o.channel==='POS')o.bankReceiptStatus=(o.posClearance||[]).some(p=>/Posted/.test(p.status))?'Existing POS reconciliation linked':'POS clearance pending';
  if(o.channel!=='POS'&&legs.length)o.settlementStatus=o.bankReceiptStatus;
  o.bankEvidence=[...new Map([...(o.bankEvidence||[]),...legs.flatMap(l=>l.bankEvidence)].map(b=>[b.account+'|'+b.transactionId,b])).values()];
 }
 return orders;
}
module.exports={attach};
