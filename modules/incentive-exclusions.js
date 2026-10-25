'use strict';

const {createHash}=require('node:crypto');
const money=value=>Math.round((Number(value)||0)*100)/100;
const copy=value=>JSON.parse(JSON.stringify(value));
const adjustmentTotal=(state,key)=>money((state.exclusionAdjustments||[]).filter(row=>row.approvalKey===key).reduce((sum,row)=>sum+money(row.amount),0));
const approvedAmount=(state,key)=>money(Number((state.approvals||{})[key]?.incentive||0)+adjustmentTotal(state,key));
const snapshot=day=>({eligibleAmount:day?.eligibleAmount||0,incentiveBaseAmount:day?.incentiveBaseAmount||0,carriedQualifiedAmount:day?.carriedQualifiedAmount||0,thresholdMet:!!day?.thresholdMet,incentive:day?.incentive||0,qualificationSources:day?.qualificationSources||[],unresolved:!!day?.unresolved});
function fail(status,message){const error=new Error(message);error.status=status;throw error;}

// This operation changes only incentive policy and approved incentive earnings.
// Shopify orders, review classifications, salary and bank payments are untouched.
function preview(orders,state,orderId,excluded,calculate,ledgerView,parseSalespeople){
  if(typeof excluded!=='boolean')fail(400,'Choose whether to exclude or restore the order.');
  const order=orders.find(row=>String(row.id)===String(orderId));
  if(!order)fail(404,'Shopify order not found.');
  if(String(order.channel||'').toUpperCase()!=='POS')fail(400,'Only POS orders can be managed in this incentive ledger.');
  const alreadyApplied=Boolean((state.exclusions||{})[orderId]?.excluded)===excluded;
  const next=copy(state);next.exclusions=next.exclusions||{};next.exclusions[orderId]={excluded};
  const beforeCalculation=calculate(orders,state),beforeDays=new Map(beforeCalculation.daily.map(day=>[day.key,day]));
  const afterDays=new Map(calculate(orders,next).daily.map(day=>[day.key,day]));
  // Older reviews could mark no salesperson without correcting the ledger.
  // Include that order's original names/dates and approved dependent sources
  // even when the current calculated rows are already excluded by that review.
  const relatedKeys=new Set(),names=new Set(parseSalespeople(order.note).salespersons);
  beforeCalculation.records.filter(row=>row.orderId===String(orderId)).forEach(row=>row.salespersons.forEach(name=>names.add(name)));
  beforeCalculation.records.filter(row=>row.orderId===String(orderId)).forEach(row=>names.forEach(name=>relatedKeys.add(row.receiptDate+'|'+name)));
  Object.entries(state.approvals||{}).forEach(([key,approval])=>{
    if((approval.qualificationSources||[]).some(source=>relatedKeys.has(source.qualificationDate+'|'+approval.salesperson)))relatedKeys.add(key);
  });
  const affectedDays=[];
  if(!alreadyApplied)for(const key of new Set([...beforeDays.keys(),...afterDays.keys(),...relatedKeys])){
    const before=beforeDays.get(key),after=afterDays.get(key),oldSnapshot=snapshot(before),newSnapshot=snapshot(after);
    const approval=(state.approvals||{})[key],posted=approval?approvedAmount(state,key):0;
    // Restoring cannot grant more than the last explicit approval. Pending
    // earnings still require Approve day; exclusions can only reduce them.
    const revised=approval?money(Math.max(0,Math.min(Number(approval.incentive)||0,newSnapshot.incentive,excluded||newSnapshot.unresolved?posted:Infinity))):0;
    if(JSON.stringify(oldSnapshot)===JSON.stringify(newSnapshot)&&!(excluded&&relatedKeys.has(key)&&posted!==revised))continue;
    const day=after||before||approval;if(!day)continue;
    affectedDays.push({key,date:day.date,salesperson:day.salesperson,previousEligible:oldSnapshot.eligibleAmount,eligible:newSnapshot.eligibleAmount,previousIncentive:oldSnapshot.incentive,incentive:newSnapshot.incentive,hasApproval:!!approval,approvedBefore:posted,approvedAfter:revised,adjustment:money(revised-posted),snapshot:newSnapshot});
  }
  next.exclusionAdjustments=next.exclusionAdjustments||[];
  affectedDays.filter(day=>day.adjustment).forEach((day,index)=>next.exclusionAdjustments.push({id:'preview-'+index,approvalKey:day.key,salesperson:day.salesperson,date:day.date,qualifyingDate:day.date,amount:day.adjustment,orderNumber:order.name||order.id}));
  const oldLedgers=ledgerView(state),newLedgers=ledgerView(next),affectedNames=new Set(affectedDays.map(day=>day.salesperson));
  const balances=newLedgers.filter(row=>affectedNames.has(row.salesperson)).map(row=>({salesperson:row.salesperson,paid:row.paid,previousBalance:oldLedgers.find(old=>old.salesperson===row.salesperson).balance,balance:row.balance,payable:row.payableBalance,overpaid:row.overpaidAmount}));
  return{success:true,orderId:String(orderId),orderNumber:String(order.name||order.number||order.id),excluded,alreadyApplied,previewToken:createHash('sha256').update(JSON.stringify({orderId:String(orderId),excluded,orders,state})).digest('hex'),affectedDays,balances,totalAdjustment:money(affectedDays.reduce((sum,day)=>sum+day.adjustment,0))};
}

function appendAdjustment(state,details){
  state.exclusionAdjustments=state.exclusionAdjustments||[];
  const at=new Date().toISOString(),date=new Date(Date.now()+19800000).toISOString().slice(0,10);
  state.exclusionAdjustments.push({id:'INCX-'+String(state.exclusionAdjustments.length+1).padStart(6,'0'),date,at,...details});
}
function apply(state,plan,reason,by){
  state.exclusions=state.exclusions||{};state.approvalRevisions=state.approvalRevisions||{};
  const before=state.exclusions[plan.orderId]||null;
  state.exclusions[plan.orderId]={excluded:plan.excluded,reason,by,at:new Date().toISOString()};
  for(const day of plan.affectedDays.filter(row=>row.hasApproval)){
    state.approvalRevisions[day.key]={...day.snapshot,incentive:day.approvedAfter,by,at:new Date().toISOString()};
    if(day.adjustment)appendAdjustment(state,{approvalKey:day.key,salesperson:day.salesperson,qualifyingDate:day.date,amount:day.adjustment,orderId:plan.orderId,orderNumber:plan.orderNumber,excluded:plan.excluded,reason,by});
  }
  return before;
}

// A later explicit reapproval replaces the base approval. Neutralize only the
// old exclusion adjustments (with a visible counter-entry), never bank payments.
function resetForApproval(state,key,by){
  const total=adjustmentTotal(state,key),approval=(state.approvals||{})[key];
  if(total&&approval)appendAdjustment(state,{approvalKey:key,salesperson:approval.salesperson,qualifyingDate:approval.date,amount:money(-total),reason:'Superseded by explicit day reapproval',by,reapproval:true});
  if(state.approvalRevisions)delete state.approvalRevisions[key];
}
module.exports={preview,apply,approvedAmount,resetForApproval};
