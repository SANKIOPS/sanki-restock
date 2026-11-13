'use strict';
const {buildReport,validDate}=require('./pnl-report');
const loans=require('./expense-loans');
const {money}=require('./physical-bank-balance');
const START='2026-08-22', list=v=>Array.isArray(v)?v:Object.values(v||{}), nature=v=>String(v||'SANKI').toUpperCase();
const active=r=>r&&!r.accountingExcluded&&!r.voided&&!r.voidedAt&&!r.reversedAt&&r.status!=='voided';
const inRange=(r,from,to)=>r.date>=from&&r.date<=to;
const incomeTypes=new Set(['rental_income','hotel_income','sewak_park_hotel_income','other_income','bank_interest','bank_interest_received','asset_sale','product_sale','rental_dealer_commission','rental_documentation_fee','other']);
const balanceCost=/security|deposit|loan principal|salary advance|gst payment|gst paid|capital|asset purchase|credit[ -]?card|card repayment|\bemi\b|loan repayment|owner withdrawal|owner contribution|internal transfer/i;
function build(sources,{entity,from,to,today,accounts=[]}){
  if(!['SANKI','SAMAST','PERSONAL'].includes(entity)||!validDate(from)||!validDate(to)||from<START||from>to||to>today)throw Object.assign(Error('Select an entity and valid dates from 22 August 2026 through today.'),{status:400});
  const s=sources.expenses, warnings=[], rows=[], match=r=>nature(r.reportingNature||r.nature)===entity;
  if(!s||typeof s.expenses!=='object')throw Object.assign(Error('Accounting records are unavailable. No totals were assumed.'),{status:409});
  const add=(kind,r,amount)=>rows.push({id:r.id,reference:r.reference||r.id,date:r.date,kind,entity,account:r.account||'',category:r.category||r.ledger||kind,description:r.description||r.particulars||r.source||r.vendor||'',amount:money(amount)});
  const otherMovements=[];for(const e of list(s.expenses).filter(r=>active(r)&&match(r)&&balanceCost.test(r.ledger||r.category||''))){for(const p of list(e.payments).filter(p=>active(p)&&inRange(p,from,to))){otherMovements.push({id:e.id+'/'+p.id,date:p.date,category:e.ledger||e.category,description:e.particulars||e.vendor||'',account:p.account||e.account||'',amount:money(p.amount)});}}
  const reviewCosts=otherMovements.filter(r=>/credit[ -]?card|card repayment|\bemi\b|loan repayment/i.test(r.category));for(const r of reviewCosts)warnings.push(r.id+': '+r.category+' needs repayment/interest review. Excluded from costs; original bank payment remains unchanged.');
  let income=0,expenses=0,cogs=0,result=null,basis,resultLabel;
  if(entity==='SANKI'){
    const report=buildReport({...sources,expenses:{...s,expenses:Object.fromEntries(Object.entries(s.expenses||{}).filter(([,r])=>!balanceCost.test(r.ledger||r.category||'')))}},{from,to,start:START,channel:'All'});
    income=report.totals.netSales==null?null:money(report.totals.netSales+report.totals.otherIncome);
    expenses=report.totals.expenses;cogs=report.totals.cogs;result=report.totals.netProfit;
    warnings.push(...report.warnings.map(w=>w.reference+': '+w.message));
    basis=report.basis;resultLabel='Management profit';
    for(const r of report.transactions){if(r.netSales||r.otherIncome||r.grossSales)add('income',r,r.netSales==null?0:r.netSales+r.otherIncome);if(r.expenses)add('expense',r,r.expenses);if(r.cogs)add('cost_of_sales',r,r.cogs);}
  }else{
    // Reuse tested paid-cost/refund allocation rules, with a report-local copy.
    // No source nature, payment, date, evidence, or ledger is saved or retagged.
    const virtual={...s,expenses:Object.fromEntries(Object.entries(s.expenses||{}).filter(([,r])=>match(r)&&!balanceCost.test(r.ledger||r.category||'')).map(([id,r])=>[id,{...r,nature:'SANKI'}])),receipts:[],salesRefunds:[],receivables:{},
      reconciliationExpenses:list(s.reconciliationExpenses).filter(r=>match(r)&&!balanceCost.test(r.category||r.ledger||'')).map(r=>({...r,nature:'SANKI'})),
      vendorAdvances:list(s.vendorAdvances).filter(match).map(r=>({...r,nature:'SANKI'})),expenseRefunds:list(s.expenseRefunds).filter(match).map(r=>({...r,nature:'SANKI'})),loanRepayments:list(s.loanRepayments).filter(match).map(r=>({...r,nature:'SANKI'}))};
    const salary=entity==='SAMAST'?sources.samastSalary:{};
    const report=buildReport({...sources,expenses:virtual,orders:{orders:{},dispatch:{}},manualSales:{sales:[]},purchases:{pos:{}},salary:salary||{},incentives:{payments:[]},lineCorrections:{},shopifyCosts:{items:[]}},{from,to,start:START,channel:'All'});
    for(const r of report.transactions.filter(r=>r.expenses)){expenses+=r.expenses;add('expense',r,r.expenses);}
    warnings.push(...report.warnings.filter(w=>['expense','salary','source'].includes(w.scope)).map(w=>w.reference+': '+w.message));
    for(const r of list(s.receipts).filter(r=>active(r)&&!r.historicalOpening&&match(r)&&inRange(r,from,to))){
      if(r.bankReconciliationDraft&&s.bankReconciliationDrafts?.[r.bankReconciliationDraft]){warnings.push(r.id+': Pending reconciliation receipt excluded.');continue;}
      if(loans.isLoan(r)||r.expenseRefundId||r.refundRecoveryExpenseId||/security|deposit|contribution|exchange|refund/i.test(r.receiptType||''))continue;
      if(!incomeTypes.has(r.receiptType)){warnings.push(r.id+': Receipt type needs review before inclusion in income.');continue;}
      income+=Number(r.amount)||0;add('income',{...r,category:r.category||r.receiptType},r.amount);
    }
    result=money(income-expenses);resultLabel='Cash surplus / deficit';
    basis='Recorded income received minus approved paid costs. Transfers, loan principal and refundable security are excluded. This is a cash management view, not earned or tax-adjusted profit.';
    if(entity==='SAMAST')warnings.push('Rental receipts are bank amounts and may include GST, TDS effects or advance rent. Cash surplus is not net rental profit.');
  }
  const transfers=[];
  for(const r of list(s.transfers).filter(r=>active(r)&&inRange(r,from,to))){
    const outgoing=nature(r.fromNature||r.nature)===entity,incoming=nature(r.toNature||r.nature)===entity;if(!outgoing&&!incoming)continue;
    transfers.push({id:r.id,date:r.date,from:r.fromAccount,to:r.toAccount,fromEntity:nature(r.fromNature||r.nature),toEntity:nature(r.toNature||r.nature),classification:r.classification||'internal_transfer',amount:money(r.amount)});
  }
  const loanRows=loans.catalog(s,to).filter(match).map(r=>({id:r.id,description:r.source,account:r.account,borrowed:r.borrowed,repaid:r.repaid,outstanding:r.outstanding,interestPaid:r.interestPaid}));
  const security=[];
  if(entity==='SAMAST'){
    const r=s.rentals||{};
    for(const t of list(r.tenancies)){
      const opening=t.securityOpeningDate&&t.securityOpeningDate<=to?Number(t.securityOpening)||0:0;
      const deposits=list(r.deposits).filter(x=>active(x)&&x.tenancyId===t.id&&x.date<=to).reduce((n,x)=>n+Number(x.amount||0),0);
      const used=list(r.allocations).filter(x=>active(x)&&x.tenancyId===t.id&&x.kind==='security'&&x.date<=to).reduce((n,x)=>n+Number(x.amount||0),0);
      const amount=money(opening+deposits-used);if(amount)security.push({id:t.id,description:t.tenant||t.name||t.unit||t.id,amount});
    }
    if(!s.rentals)warnings.push('Rental security register is unavailable; security owed is not assumed to be zero.');
  }
  const usage={};
  for(const e of list(s.expenses).filter(r=>active(r)&&match(r))){for(const p of list(e.payments).filter(p=>active(p)&&!p.provisional&&inRange(p,from,to))){const account=p.account||e.account;if(!account)continue;usage[account]=money((usage[account]||0)+Number(p.amount||0));}}
  for(const feed of sources.feeds||[])if(feed.required&&feed.status!=='ready')warnings.push(feed.name+' is '+feed.status+'. Report may be incomplete.');
  if(reviewCosts.length)result=null;
  return {success:true,entity,from,to,basis,resultLabel,otherMovements,totals:{income:income==null?null:money(income),expenses:money(expenses),costOfSales:money(cogs),result:result==null?null:money(result)},accounts,transactions:rows.sort((a,b)=>b.date.localeCompare(a.date)),transfers,paidFrom:Object.entries(usage).map(([account,amount])=>({account,amount})),obligations:{loans:loanRows,security,securityKnown:entity!=='SAMAST'||!!s.rentals,loanTotal:money(loanRows.reduce((n,r)=>n+r.outstanding,0)),securityTotal:money(security.reduce((n,r)=>n+r.amount,0))},warnings:[...new Set(warnings)],note:'Account balances cover all money held in those accounts, including borrowed money and deposits. Shared bank money is not allocated to an entity by this report. Personal spending by paying account does not prove that rental income funded it. Transfers are shown once and are not additional expenses. No automatic debts are created.'};
}
module.exports={build,START};
