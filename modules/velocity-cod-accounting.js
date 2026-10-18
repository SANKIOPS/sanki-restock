'use strict';

const number=value=>{const parsed=Number(value);return Number.isFinite(parsed)?parsed:0;};
const round=value=>Math.round((number(value)+Number.EPSILON)*100)/100;
const isCodGateway=value=>/cash\s*on\s*delivery|\bcod\b/i.test(String(value||''));
const terminalShipmentStatus=value=>/\brto\b|return(?:ed)?(?:\s+to\s+origin)?|cancel/i.test(String(value||''));

function shipmentState(shop,order,evidence={}){
  const dispatch=(shop.dispatch||{})[String(order.id)]||{},candidates=[];
  (order.shipments||[]).forEach(row=>candidates.push(row));
  const index=evidence.index||{byOrder:{},shipments:{}},tracking=evidence.tracking||{shipments:{}};
  (index.byOrder&&index.byOrder[String(order.id)]||[]).forEach(row=>candidates.push(row));
  if(dispatch.velocityAwb)candidates.push({awb:dispatch.velocityAwb,status:dispatch.packingStatus||''});
  const active=candidates.some(row=>{
    const awb=String(row.awb||row.tracking_number||'').trim();
    if(!awb)return false;
    const latest=(tracking.shipments||{})[awb]||(index.shipments||{})[awb]||{};
    return !terminalShipmentStatus(latest.status||latest.rawStatus||row.status||row.rawStatus);
  });
  const terminal=candidates.length>0&&!active;
  const dispatchConfirmed=!terminal&&['booked','dispatched','delivered'].includes(String(dispatch.packingStatus||'').toLowerCase());
  return {active:active||dispatchConfirmed,terminal};
}

function isPendingReceivableOrder(shop,order,evidence={}){
  if(!order||order.cancelledAt||!/website|online/i.test(String(order.channel||'Website'))||!isCodGateway((order.paymentGateways||[]).join(' ')))return false;
  return shipmentState(shop,order,evidence).active;
}

function pendingReceivable(order,eligible){
  const transactions=Array.isArray(order.paymentTransactions)?order.paymentTransactions:[];
  const successfulTransactions=transactions.filter(tx=>['sale','capture'].includes(String(tx.kind||'').toLowerCase())&&(!tx.status||String(tx.status).toLowerCase()==='success')&&number(tx.amount)>0);
  const pendingCodTransactions=transactions.filter(tx=>['sale','capture','authorization'].includes(String(tx.kind||'').toLowerCase())&&String(tx.status||'').toLowerCase()!=='success'&&isCodGateway(tx.gateway||tx.payment_gateway)&&number(tx.amount)>0);
  const financialStatus=String(order.financialStatus||'').toLowerCase();
  const pendingEligible=eligible&&(pendingCodTransactions.length>0||['pending','partially_paid','authorized'].includes(financialStatus));
  const successfulTotal=round(successfulTransactions.reduce((sum,tx)=>sum+number(tx.amount),0));
  const capacity=round(Math.max(0,number(order.total)-successfulTotal));
  const reported=round(pendingCodTransactions.reduce((sum,tx)=>sum+number(tx.amount),0));
  return {amount:pendingEligible?Math.min(capacity,reported||capacity):0,successfulTransactions,pendingCodTransactions};
}

module.exports={isCodGateway,terminalShipmentStatus,shipmentState,isPendingReceivableOrder,pendingReceivable};
