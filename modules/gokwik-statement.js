'use strict';
const g = require('./gokwik-accounting');

// Read-only statement projection. Pending provider outflows are NOT ledger
// postings, bank credits, paid expenses, reconciliation matches or approvals.
function buildStatement(entries, store, {from='',to='',expenseNature='',recordedBalance=0}={}) {
  const opening=entries.find(row=>row.kind==='opening');
  const rows=entries.filter(row=>row.kind!=='opening'&&(!expenseNature||!row.entity||row.entity===expenseNature)).map(row=>({...row,pendingPosting:false,status:'Recorded'}));
  if(!expenseNature||expenseNature==='SANKI') {
    for(const batch of g.batches(Object.values(store.gokwikTransactions||{}))) {
      if((store.gokwikPostings||{})[batch.id])continue; // Real expense/transfer rows already exist.
      const approval=(store.gokwikApprovals||{})[batch.id];
      const status=approval?'Unposted · awaiting Owner approval':'Unposted · review required';
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
    notice:'Includes unposted provider-reported fees, deductions and payouts for review only. Recorded account balances, bank balances, paid expenses and P&L do not change until Owner posting. No opening balance is inferred.'};
}
module.exports={buildStatement};
