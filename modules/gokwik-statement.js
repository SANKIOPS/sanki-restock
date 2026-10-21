'use strict';
const g = require('./gokwik-accounting');

// Read-only statement projection. Pending provider outflows are NOT ledger
// postings, bank credits, paid expenses, reconciliation matches or approvals.
function buildStatement(entries, store, {from='',to='',expenseNature='',recordedBalance=0}={}) {
  const opening=entries.find(row=>row.kind==='opening');
  const captures=Object.values(store.gokwikTransactions||{}),captureIds=new Set(captures.map(tx=>'GKW-PAYMENT/'+tx.paymentId));
  // The primary statement follows report collections, not estimates of website
  // receipts awaiting a report or an order identity review. Recorded balance is
  // retained separately and is never changed by this projection.
  const rows=entries.filter(row=>row.kind!=='opening'&&(!expenseNature||!row.entity||row.entity===expenseNature)&&(!captures.length||row.kind!=='sale'||captureIds.has(row.id))).map(row=>({...row,pendingPosting:false,status:'Recorded'}));
  if(!expenseNature||expenseNature==='SANKI') {
    for(const tx of captures)if(!rows.some(row=>row.id==='GKW-PAYMENT/'+tx.paymentId))rows.push({id:'GKW-REPORT-COLLECTION/'+tx.paymentId,kind:'gokwik_report_collection',date:tx.date,entity:'SANKI',description:'GoKwik reported collection · #'+tx.orderNumber,reference:tx.paymentId,credit:tx.gross,debit:0,pendingPosting:true,status:'Report collection · order link awaiting review',editable:false});
    for(const batch of g.batches(Object.values(store.gokwikTransactions||{}))) {
      if((store.gokwikPostings||{})[batch.id])continue; // Real expense/transfer rows already exist.
      const status='Report figures · awaiting 0437 reconciliation';
      const common={date:batch.settledDate,entity:'SANKI',reference:batch.utr,settlementId:batch.id,pendingPosting:true,status,editable:false,credit:0};
      const add=(suffix,kind,description,value,sequence)=>{if(g.money(value)>0)rows.push({...common,id:'GKW-REPORT/'+batch.id+'/'+suffix,kind,description,debit:value,sequence});};
      add('FEES','gokwik_report_fee','GoKwik processing fees and tax',Math.round((batch.fee+batch.tax+batch.additionalFee+batch.additionalTax)*100)/100,10);
      add('DEDUCTION','gokwik_report_deduction','GoKwik service charge · Subscription Expense',batch.deduction,20);
      add('PAYOUT','gokwik_report_payout','GoKwik payout to '+g.BANK+' · reported, not posted',batch.net,30);
    }
  }
  // Exclude future events before calculating either the closing or the carry-forward.
  const ordered=rows.filter(row=>!to||row.date<=to).sort((a,b)=>String(a.date).localeCompare(String(b.date))||(a.sequence||0)-(b.sequence||0)||String(a.id).localeCompare(String(b.id)));
  let running=opening?g.money(opening.credit)-g.money(opening.debit):0,carried=running,pendingOut=0,totalIn=0,totalOut=0;
  const visible=[];
  for(const row of ordered) {
    const credit=g.money(row.credit),debit=g.money(row.debit);
    running+=credit-debit;
    if(row.pendingPosting)pendingOut+=debit;
    if(from&&row.date<from){carried=running;continue;}
    totalIn+=credit;totalOut+=debit;
    visible.push({...row,balance:running/100});
  }
  return {account:g.CLEARING,from,to,recordedBalance,openingBalance:carried/100,closingBalance:running/100,pendingOut:pendingOut/100,totalIn:totalIn/100,totalOut:totalOut/100,entries:visible,
    notice:'Collections, charges and payouts follow the GoKwik report; website receipt estimates awaiting a report are excluded from this statement. Report-only rows are not accounting postings. Matched settlements, charges and transfers are recorded together only when the Owner finalizes the 0437 bank reconciliation. No opening balance is inferred.'};
}
module.exports={buildStatement};
