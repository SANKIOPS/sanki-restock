'use strict';

// Personal reporting is deliberately a management layer. It never retags a
// bank movement or changes an expense, transfer, loan, amount or date.
const loans=require('./expense-loans');
const refunds=require('./expense-refunds');
const hotel=require('./sewak-park-reporting');
const groupManagement=require('./expense-category-groups');

const KNOWN_SANKI_LOANS=new Set(['REC-00200','REC-00201']);
const REPORT_START='2026-08-22';
const SANKI_ACCOUNTS=new Set(['Axis Bank 3448','Tiana 0425','Tiana Traders IndusInd 0437','Prashant Axis 3645','IndusInd Bank 8181','Counter Cash','Gagan Sir Cash','Prashant Cash','Services']);
const INBOUND_TRANSFER_TREATMENTS=new Set(['permanent_withdrawal','business_repayment','temporary_due_to_business']);
const OUTBOUND_TRANSFER_TREATMENTS=new Set(['permanent_contribution','personal_repayment','temporary_due_from_business']);
const NON_INCOME_RECEIPT=/security|deposit|loan|contribution|exchange|refund|transfer/i;
const list=value=>Array.isArray(value)?value:Object.values(value||{});
const nature=value=>String(value||'').trim().toUpperCase();
const money=value=>Math.round(Number(value||0)*100)/100;
const active=row=>row&&!row.accountingExcluded&&!row.deletedAt&&!row.voided&&!row.voidedAt&&!row.reversedAt&&row.active!==false&&!['void','voided','deleted','reversed','rejected','cancelled','canceled'].includes(String(row.status||'').toLowerCase());
const inRange=(date,from,to)=>String(date||'')>=from&&String(date||'')<=to;
const claimant=row=>String(row.reimbursementClaimant||row.claimant||row.createdBy||'').trim().toLowerCase();
const movementKey=(expense,payment)=>expense.id+'/'+(payment.id||'PAYMENT');
const state=store=>store.personalReporting&&typeof store.personalReporting==='object'?store.personalReporting:{};
const draftId=row=>String(row&&(
  row.bankReconciliationDraft||row.reconciliationDraft||row.bankReconciliationEvidence&&row.bankReconciliationEvidence.draftId
)||'');
const finalizedMovement=(store,row)=>active(row)&&(!draftId(row)||!store.bankReconciliationDrafts?.[draftId(row)]);
const finalizedReceipt=(store,row)=>{
  if(!active(row)||row.historicalOpening||row.balanceSheetOnly)return false;
  return finalizedMovement(store,row);
};

function sourceReceipts(store,from,to){
  const rows=[],seen=new Set();
  for(const receipt of list(store.receipts).filter(row=>finalizedReceipt(store,row)&&inRange(row.date,from,to))){
    let source='';
    if(nature(receipt.nature)==='SAMAST'&&String(receipt.receiptType||'').toLowerCase()==='rental_income')source='SAMAST';
    else if(nature(receipt.nature)==='PERSONAL'&&hotel.isHotelReceipt(receipt))source=hotel.UNIT;
    if(!source||loans.isLoan(receipt)||receipt.expenseRefundId||receipt.refundRecoveryExpenseId||NON_INCOME_RECEIPT.test(receipt.receiptType||receipt.category||''))continue;
    if(seen.has(receipt.id))throw Object.assign(Error('Duplicate source receipt '+receipt.id+' prevents a reliable Personal report. No total was assumed.'),{status:409});
    seen.add(receipt.id);
    if(!Number.isFinite(Number(receipt.amount))||Number(receipt.amount)<0)throw Object.assign(Error('Invalid source receipt amount on '+receipt.id+' prevents a reliable Personal report. No total was assumed.'),{status:409});
    if(!Number(receipt.amount))continue;
    rows.push({
      id:receipt.id,date:receipt.date,source,
      label:source===hotel.UNIT?'Sewak Park income received':'SAMAST rent received',
      account:receipt.account||'',description:receipt.particulars||receipt.source||receipt.payer||receipt.category||'',
      amount:money(receipt.amount),treatment:'Finalized receipt recorded from bank / cash evidence'
    });
  }
  return rows.sort((a,b)=>String(b.date).localeCompare(String(a.date))||Number(b.amount)-Number(a.amount)||String(a.id).localeCompare(String(b.id)));
}

function loanOwner(store,loan){
  // The borrower belongs to the loan record itself. Historical Personal-report
  // overrides were management notes, not transaction evidence, and must never
  // supersede the borrower saved in the loan setup.
  return nature(loan.loanTerms&&loan.loanTerms.borrowerNature||(KNOWN_SANKI_LOANS.has(loan.id)?'SANKI':loan.nature));
}
function transferTreatment(store,transfer){
  const record=state(store).transferTreatments?.[transfer.id];
  const allowed=transfer.toEntity==='PERSONAL'?INBOUND_TRANSFER_TREATMENTS:transfer.fromEntity==='PERSONAL'?OUTBOUND_TRANSFER_TREATMENTS:new Set();
  if(record&&allowed.has(record.treatment))return record;
  if(transfer.classification==='inter_entity_loan')return{treatment:transfer.toEntity==='PERSONAL'?'temporary_due_to_business':'temporary_due_from_business',reason:'Saved as an inter-entity loan',source:'accounting_record'};
  return null;
}
function paymentRows(store,to){
  const rows=[];
  for(const expense of list(store.expenses).filter(row=>active(row)&&nature(row.nature)==='PERSONAL')){
    for(const payment of list(expense.payments).filter(row=>finalizedMovement(store,row)&&!row.provisional&&String(row.date||expense.date||'')<=to)){
      rows.push({key:movementKey(expense,payment),id:expense.id,date:String(payment.date||expense.date||''),amount:money(payment.amount),account:payment.account||expense.account||'',expense,payment,category:expense.ledger||expense.category||'Unclassified',description:expense.particulars||expense.vendor||''});
    }
  }
  return rows;
}
function receiptOrder(loan){
  const row=String(loan.bankReconciliationEvidence&&loan.bankReconciliationEvidence.rowId||''),matched=row.match(/(\d+)$/);
  return matched?Number(matched[1]):Number.MAX_SAFE_INTEGER;
}
function allocateLoanUsage(store,{from,to,accountChecks={},accountOwners={}}){
  // The management split is deliberately simple: principal sent from the
  // receiving Personal account to the borrowing business is business
  // allocation. Principal repaid to the lender is separate. The remainder is
  // the Personal portion. Individual expenses are not assigned to a loan.
  // A staged reconciliation entry is not accounting evidence yet. Build the
  // loan position from finalized receipts and repayments only.
  const finalizedStore={...store,
    receipts:list(store.receipts).filter(row=>finalizedReceipt(store,row)),
    loanRepayments:list(store.loanRepayments).filter(row=>finalizedMovement(store,row))
  };
  // The Personal report follows only principal that actually entered a
  // Personal-owned account (or was explicitly recorded in the Personal book).
  // A SANKI/SAMAST loan received directly by that business belongs only in the
  // business loan register and can never create a Personal settlement.
  const catalog=loans.catalog(finalizedStore,to).filter(loan=>nature(loan.nature)==='PERSONAL'||nature(accountOwners[loan.account])==='PERSONAL'||KNOWN_SANKI_LOANS.has(loan.id)).sort((a,b)=>String(a.date).localeCompare(String(b.date))||receiptOrder(a)-receiptOrder(b)||String(a.createdAt||'').localeCompare(String(b.createdAt||''))||String(a.id).localeCompare(String(b.id)));
  for(const loan of catalog)if(!['SANKI','SAMAST','PERSONAL'].includes(loanOwner(store,loan)))throw Object.assign(Error('Loan '+loan.id+' has no valid borrower entity. Personal settlement was not calculated.'),{status:409});
  const reports=new Map(catalog.map(loan=>[loan.id,{id:loan.id,date:loan.date,lender:loan.source||'Lender',reference:loan.bankReference||loan.bankReconciliationEvidence&&loan.bankReconciliationEvidence.reference||loan.id,account:loan.account,owner:loanOwner(store,loan),borrowed:money(loan.borrowed),repaid:money(loan.repaid),outstanding:money(loan.outstanding),businessUse:0,personalUse:0,personalPortion:0,personalPrincipalRepaid:0,businessPrincipalRepaid:0,unresolvedPrincipalRepaid:0,personalPrincipalApplied:0,personalPaidForBusiness:0,periodBusinessUse:0,events:[],accountCheck:accountChecks[loan.id]||accountChecks[loan.account]||null} ]));
  // A transfer from the receiving Personal account to the borrowing business
  // is the strongest evidence of business allocation. Do not reduce that
  // capacity using a later principal repayment before matching the transfer.
  const available=new Map(catalog.map(loan=>[loan.id,money(loan.borrowed)])),businessTransferAllocations={};
  const outbound=list(store.transfers).filter(row=>finalizedMovement(store,row)&&String(row.date||'')<=to&&nature(row.fromNature||row.nature)==='PERSONAL'&&nature(row.toNature||row.nature)!=='PERSONAL').sort((a,b)=>String(a.date).localeCompare(String(b.date))||String(a.createdAt||'').localeCompare(String(b.createdAt||''))||String(a.id).localeCompare(String(b.id)));
  for(const transfer of outbound){
    let left=money(transfer.amount),allocated=0;
    for(const loan of catalog){
      if(left<=0)break;
      const report=reports.get(loan.id),room=available.get(loan.id)||0;
      if(!room||String(loan.date)>String(transfer.date)||loan.account!==transfer.fromAccount||report.owner!==nature(transfer.toNature||transfer.nature))continue;
      const amount=money(Math.min(left,room));available.set(loan.id,money(room-amount));left=money(left-amount);allocated=money(allocated+amount);report.businessUse=money(report.businessUse+amount);if(inRange(transfer.date,from,to))report.periodBusinessUse=money(report.periodBusinessUse+amount);report.events.push({id:transfer.id,date:transfer.date,kind:'business_transfer',description:(transfer.fromAccount||'')+' → '+(transfer.toAccount||''),account:transfer.fromAccount,amount,use:'Allocated to '+report.owner,reference:transfer.bankReference||transfer.bankReconciliationEvidence&&transfer.bankReconciliationEvidence.reference||''});
    }
    if(allocated)businessTransferAllocations[transfer.id]=allocated;
  }
  const repaymentOwner=account=>{
    const configured=String(accountOwners[account]||'').toUpperCase();
    if(configured)return configured;
    if(SANKI_ACCOUNTS.has(account))return 'SANKI';
    return '';
  };
  for(const report of reports.values()){
    const sourceLoan=catalog.find(row=>row.id===report.id);
    for(const payment of sourceLoan?.payments||[]){
      const principal=money(payment.principalAmount==null?payment.amount:payment.principalAmount),owner=repaymentOwner(payment.account);
      if(owner==='PERSONAL'||!owner&&payment.account===report.account)report.personalPrincipalRepaid=money(report.personalPrincipalRepaid+principal);
      else if(owner===report.owner)report.businessPrincipalRepaid=money(report.businessPrincipalRepaid+principal);
      else report.unresolvedPrincipalRepaid=money(report.unresolvedPrincipalRepaid+principal);
      report.events.push({id:payment.id,date:payment.date||'',kind:'principal_repaid',description:'Principal repaid to lender',account:payment.account||'',amount:principal,use:owner==='PERSONAL'||!owner&&payment.account===report.account?'Paid from Personal':owner===report.owner?'Paid by '+report.owner:'Repayment account needs ownership review'});
    }
    const room=money(Math.max(0,report.borrowed-report.businessUse));
    report.personalPrincipalApplied=money(Math.min(room,report.personalPrincipalRepaid));
    report.personalPaidForBusiness=money(Math.max(0,report.personalPrincipalRepaid-report.personalPrincipalApplied));
    report.personalPortion=money(Math.max(0,room-report.personalPrincipalApplied));report.personalUse=report.personalPortion;report.remaining=report.personalPortion;report.allocationStatus=report.accountCheck&&report.accountCheck.matched?'Account matched':'Calculated from account transfers';report.allocationReason='Loan received minus transfers from '+report.account+' to '+report.owner+' minus principal actually repaid from Personal. Principal repaid by the business does not reduce the Personal portion.';
    if(report.personalPortion)report.events.push({id:report.id+'/PERSONAL',date:'',kind:'personal_portion',description:'Business-owned loan principal remaining with Personal',account:report.account,amount:report.personalPortion,use:'Personal portion'});
    if(report.personalPaidForBusiness)report.events.push({id:report.id+'/PERSONAL-EXCESS-REPAYMENT',date:'',kind:'personal_paid_for_business',description:'Personal principal repayment exceeded the loan principal remaining with Personal',account:report.account,amount:report.personalPaidForBusiness,use:report.owner+' must return this to Personal'});
  }
  const poolMap=new Map();
  for(const report of reports.values()){
    const key=report.owner+'\u0000'+report.account,pool=poolMap.get(key)||{id:'POOL-'+report.owner+'-'+report.account,owner:report.owner,account:report.account,lenders:[],loanIds:[],borrowed:0,repaid:0,outstanding:0,businessUse:0,personalPortion:0,personalPrincipalRepaid:0,businessPrincipalRepaid:0,unresolvedPrincipalRepaid:0,personalPrincipalApplied:0,personalPaidForBusiness:0,events:[],accountCheck:report.accountCheck||null};
    pool.lenders.push(report.lender);pool.loanIds.push(report.id);for(const field of ['borrowed','repaid','outstanding','businessUse','personalPrincipalRepaid','businessPrincipalRepaid','unresolvedPrincipalRepaid'])pool[field]=money(pool[field]+Number(report[field]||0));
    for(const event of report.events.filter(row=>!['personal_portion','personal_paid_for_business'].includes(row.kind))){const existing=pool.events.find(row=>row.id===event.id&&row.kind===event.kind);if(existing)existing.amount=money(existing.amount+Number(event.amount||0));else pool.events.push({...event});}
    poolMap.set(key,pool);
  }
  const accountChecksConfigured=Object.keys(accountChecks||{}).length>0,pools=[...poolMap.values()].map(pool=>{
    const room=money(Math.max(0,pool.borrowed-pool.businessUse));pool.personalPrincipalApplied=money(Math.min(room,pool.personalPrincipalRepaid));pool.personalPaidForBusiness=money(Math.max(0,pool.personalPrincipalRepaid-pool.personalPrincipalApplied));pool.personalPortion=money(Math.max(0,room-pool.personalPrincipalApplied));
    if(pool.personalPortion)pool.events.push({id:pool.id+'/PERSONAL',date:'',kind:'personal_portion',description:'Business-owned loan principal remaining with Personal',account:pool.account,amount:pool.personalPortion,use:'Personal portion'});
    if(pool.personalPaidForBusiness)pool.events.push({id:pool.id+'/PERSONAL-EXCESS-REPAYMENT',date:'',kind:'personal_paid_for_business',description:'Personal principal repayment exceeded the loan principal remaining with Personal',account:pool.account,amount:pool.personalPaidForBusiness,use:pool.owner+' must return this to Personal'});
    pool.lender=[...new Set(pool.lenders)].join(' + ');pool.reference=pool.loanIds.join(' + ');pool.loanSpecificAllocationProven=pool.loanIds.length===1;const accountMatched=!accountChecksConfigured||pool.accountCheck?.matched===true;pool.calculationProven=accountMatched&&!pool.unresolvedPrincipalRepaid;pool.blockers=[];if(!accountMatched)pool.blockers.push('The receiving account is not bank-verified through '+to+' without reconciliation warnings. The calculated split is visible, but no amount is added to Money to Settle.');if(pool.unresolvedPrincipalRepaid)pool.blockers.push('₹'+pool.unresolvedPrincipalRepaid.toFixed(2)+' of principal repayment uses an account whose owner is not proven. It was not used to reduce the Personal portion.');pool.allocationStatus=pool.calculationProven?'Account matched':pool.loanIds.length>1?'Account pool calculated; verification pending':'Calculated split; verification pending';pool.allocationReason='All business-owned loans received in '+pool.account+' for '+pool.owner+' are pooled. Actual transfers to the borrower are business allocation. Only principal repaid from a proven Personal account reduces the Personal portion; business-paid principal does not. No arbitrary lender-by-lender split is presented.';return pool;
  });
  return{loans:[...reports.values()],pools,businessTransferAllocations};
}

function build(store,{from,to,viewer='',spendingRows=[],monthSpendingRows=spendingRows,accounts=[],transfers=[],allTransfers=transfers,loanAccountChecks={},sharedAccountPositions={},accountOwners={}}){
  const settings=state(store),loanUsage=allocateLoanUsage(store,{from,to,accountChecks:loanAccountChecks,accountOwners});
  const configuredOwners=Object.keys(accountOwners||{}).length>0,exclusiveBusinessOwner=account=>{
    const owner=configuredOwners?accountOwners[account]:(SANKI_ACCOUNTS.has(account)?'SANKI':'');
    return ['SANKI','SAMAST'].includes(owner)?owner:'';
  };
  const paymentByKey=new Map(paymentRows(store,to).map(row=>[row.key,row]));
  const normalizeSpending=rows=>rows.filter(row=>{
    if(row.kind!=='expense')return false;
    const payment=paymentByKey.get(row.id),expense=row.appExpense||row.appRecord||payment&&payment.expense;
    // Sewak Park operating costs belong to the separate hotel report. Only an
    // explicitly tagged personal-use expense belongs in Personal spending.
    return !hotel.isHotelExpense(expense);
  }).map(row=>{const category=row.appExpense&&row.appExpense.category||row.appRecord&&row.appRecord.category||row.category||'Unclassified',group=groupManagement.resolveGroupName(store,row.appExpense&&row.appExpense.categoryGroup||row.appRecord&&row.appRecord.categoryGroup||row.categoryGroup);return{...row,category,categoryGroup:group||'Ungrouped'};});
  const spending=normalizeSpending(spendingRows),monthSpending=monthSpendingRows===spendingRows?spending:normalizeSpending(monthSpendingRows);
  const budgetMonth=to.slice(0,7),monthStart=budgetMonth+'-01',monthFrom=monthStart<REPORT_START?REPORT_START:monthStart;
  const monthActual=new Map(),periodActual=new Map(),groupByCategory=new Map();
  for(const expense of list(store.expenses)){const category=String(expense.ledger||expense.category||'').trim(),group=groupManagement.resolveGroupName(store,expense.categoryGroup||expense.group);if(category&&group&&!groupByCategory.has(category))groupByCategory.set(category,group);}
  for(const row of spending){const category=row.category||'Unclassified',group=groupManagement.resolveGroupName(store,row.categoryGroup||row.appExpense&&row.appExpense.categoryGroup||row.appRecord&&row.appRecord.categoryGroup);if(group)groupByCategory.set(category,group);periodActual.set(category,money((periodActual.get(category)||0)+Number(row.amount||0)));}
  for(const row of monthSpending){const category=row.category||'Unclassified',group=groupManagement.resolveGroupName(store,row.categoryGroup||row.appExpense&&row.appExpense.categoryGroup||row.appRecord&&row.appRecord.categoryGroup);if(group)groupByCategory.set(category,group);if(String(row.date||'').startsWith(budgetMonth))monthActual.set(category,money((monthActual.get(category)||0)+Number(row.amount||0)));}
  const savedBudgets=settings.budgets?.[budgetMonth]||{},groupOf=category=>groupByCategory.get(category)||'Ungrouped',groupTotals=new Map();for(const [category,amount] of periodActual)groupTotals.set(groupOf(category),money((groupTotals.get(groupOf(category))||0)+amount));
  const categories=[...new Set([...periodActual.keys(),...monthActual.keys(),...Object.keys(savedBudgets)])].sort((a,b)=>(groupTotals.get(groupOf(b))||0)-(groupTotals.get(groupOf(a))||0)||groupOf(a).localeCompare(groupOf(b))||(periodActual.get(b)||0)-(periodActual.get(a)||0)||a.localeCompare(b));
  const displayDate=row=>String(row.appExpense&&row.appExpense.date||row.appRecord&&row.appRecord.date||row.date||'');
  spending.sort((a,b)=>displayDate(b).localeCompare(displayDate(a))||Number(b.amount||0)-Number(a.amount||0)||String(a.id||'').localeCompare(String(b.id||'')));
  const budgets=categories.map(category=>{const actual=money(monthActual.get(category)||0),budget=Object.prototype.hasOwnProperty.call(savedBudgets,category)?money(savedBudgets[category]):null,remaining=budget==null?null:money(budget-actual);return{group:groupOf(category),category,periodActual:money(periodActual.get(category)||0),actual,budget,remaining,status:budget==null?'Set budget':remaining<0?'Over budget':'Within budget'};});

  const sourceReceiptRows=sourceReceipts(store,from,to),receivedRows=[],permanentRows=[],personalUseRows=[],unlinkedRows=[],review=[],transferReview=[],transferSettlements=[],transferDueToBusiness={},transferDueFromBusiness={};
  for(const original of allTransfers.filter(row=>finalizedMovement(store,row)&&(row.toEntity==='PERSONAL')!==(row.fromEntity==='PERSONAL')).slice().sort((a,b)=>String(a.date||'').localeCompare(String(b.date||''))||String(a.id||'').localeCompare(String(b.id||'')))){
    const allocated=money(loanUsage.businessTransferAllocations[original.id]||0),transfer=allocated?{...original,amount:money(original.amount-allocated),loanAllocated:allocated}:original;if(!(transfer.amount>0))continue;
    const inbound=transfer.toEntity==='PERSONAL',business=inbound?transfer.fromEntity:transfer.toEntity,decision=transferTreatment(store,transfer);
    if(!decision)transferReview.push({...transfer,direction:inbound?'Business to personal':'Personal to business',business,note:transfer.note||'',options:inbound?[{value:'permanent_withdrawal',label:'Permanent personal withdrawal — nothing to return'},{value:'business_repayment',label:'Business repaid money it owed you'},{value:'temporary_due_to_business',label:'Temporary business money — you must return it'}]:[{value:'permanent_contribution',label:'Permanent contribution to business — nothing to return'},{value:'personal_repayment',label:'You repaid money owed to business'},{value:'temporary_due_from_business',label:'Temporary personal money — business must return it'}]});
    if(inbound&&inRange(transfer.date,from,to)){
      const treatment=decision?.treatment==='permanent_withdrawal'?'Permanent withdrawal — nothing to return':decision?.treatment==='business_repayment'?'Business repayment':decision?.treatment==='temporary_due_to_business'?'Temporary business money — returnable':'Movement recorded; purpose still needs review';
      const row={id:transfer.id,date:transfer.date,source:business,label:'Moved from '+business+' into Personal',account:transfer.from+' → '+transfer.to,description:transfer.note||'',amount:money(transfer.amount),treatment};
      receivedRows.push(row);if(decision?.treatment==='permanent_withdrawal')permanentRows.push(row);
    }
    if(!decision)continue;
    if(decision.treatment==='temporary_due_to_business')transferDueToBusiness[business]=money((transferDueToBusiness[business]||0)+transfer.amount);
    if(decision.treatment==='temporary_due_from_business')transferDueFromBusiness[business]=money((transferDueFromBusiness[business]||0)+transfer.amount);
    if(['business_repayment','personal_repayment'].includes(decision.treatment)){
      const due=decision.treatment==='business_repayment'?transferDueFromBusiness:transferDueToBusiness,current=money(due[business]||0),applied=money(Math.min(current,transfer.amount)),unapplied=money(transfer.amount-applied);
      due[business]=money(current-applied);
      transferSettlements.push({id:transfer.id,date:transfer.date,entity:business,direction:decision.treatment==='business_repayment'?'Business repaid you':'You repaid business',amount:money(transfer.amount),applied,unapplied,note:transfer.note||'',reason:decision.reason||''});
    }
  }
  for(const transfer of transfers.filter(row=>finalizedMovement(store,row)&&row.fromEntity==='PERSONAL'&&row.toEntity==='PERSONAL'&&row.fromReportingUnit===hotel.UNIT&&row.toReportingUnit!==hotel.UNIT)){
    receivedRows.push({id:transfer.id,date:transfer.date,source:hotel.UNIT,label:'Moved from Sewak Park into Personal',account:transfer.from+' → '+transfer.to,description:transfer.note||'',amount:money(transfer.amount),treatment:'Reporting-unit withdrawal recorded by the bank transfer'});
  }
  for(const row of spending){
    const payment=paymentByKey.get(row.id),expense=payment&&payment.expense;
    const account=payment?.account||row.account||'',hotelPersonalTag=!!(expense&&hotel.isHotelPersonalUse(expense)),paidFromHotelAccount=hotel.isHotelAccount(account);
    let source='';
    // A saved Sewak Park personal-use tag proves the treatment, but the bank
    // account still has to prove the funding source. A tag on 0993 (or any
    // other Personal account) cannot turn that account's money into 0992
    // hotel money without a recorded transfer trail.
    if(hotelPersonalTag&&paidFromHotelAccount)source=hotel.UNIT;
    // Only the saved paying account and configured ownership can attribute a
    // direct business-funded personal expense. Historical owner-entered
    // funding labels are intentionally ignored: they are not bank evidence.
    // Shared accounts remain subject to their separate fund equations.
    if(!source&&expense)source=exclusiveBusinessOwner(payment?.account);
    if(source&&source!=='PERSONAL'&&!source.startsWith('LOAN:'))personalUseRows.push({...row,source,label:source===hotel.UNIT?'Personal expense using Sewak Park money':'Personal expense using '+source+' money',account,category:row.category,description:row.description,treatment:'Used for a personal expense'});
    if(expense&&!hotel.isHotelExpense(expense)){
      if(hotelPersonalTag&&!paidFromHotelAccount)unlinkedRows.push({...row,source:hotel.UNIT,label:'Needs transfer evidence',account,category:row.category,description:row.description,treatment:'The saved Sewak Park tag is not backed by a payment from 0992 or a recorded Sewak Park transfer into this account'});
      else if(!source&&paidFromHotelAccount)unlinkedRows.push({...row,source:hotel.UNIT,label:'Needs account evidence',account,category:row.category,description:row.description,treatment:'The payment came from 0992, but no saved transaction treatment proves whether it was hotel operating cost or personal use'});
    }
    // A posted PERSONAL expense is already personal spending. The paying
    // account does not require a second funding-source decision here.
  }
  const accountPositions=Object.values(sharedAccountPositions||{}),provenAccountPositions=accountPositions.filter(row=>row&&row.proven),pendingAccountPositions=accountPositions.filter(row=>row&&!row.proven);
  const accountProvenUseBySource={};
  for(const position of provenAccountPositions)if(position.periodIncrease>0)accountProvenUseBySource.SAMAST=money((accountProvenUseBySource.SAMAST||0)+position.periodIncrease);
  const bySource=rows=>rows.reduce((out,row)=>(out[row.source]=money((out[row.source]||0)+Number(row.amount||0)),out),{}),sourceReceiptsBySource=bySource(sourceReceiptRows),receivedBySource=bySource(receivedRows),personalUseBySource=bySource(personalUseRows),unlinkedBySource=bySource(unlinkedRows);

  const owesYou=[];const you=String(viewer||'').trim().toLowerCase();
  for(const expense of list(store.expenses).filter(row=>active(row)&&['SANKI','SAMAST'].includes(nature(row.nature))&&String(row.date||'')<=to&&claimant(row)===you)){
    const position=refunds.reimbursementPosition(store,expense);if(position.pending>0)owesYou.push({id:expense.id,date:expense.date,entity:nature(expense.nature),category:expense.ledger||expense.category||'',description:expense.particulars||expense.vendor||'',amount:money(position.pending)});
  }
  for(const [entity,amount] of Object.entries(transferDueFromBusiness))if(amount>0)owesYou.push({id:'TRANSFER-DUE-'+entity,date:'',entity,category:'Temporary transfer',description:'Temporary personal money sent to business',amount});
  for(const loan of loanUsage.pools.filter(row=>row.calculationProven&&row.owner!=='PERSONAL'&&row.personalPaidForBusiness>0))owesYou.push({id:loan.id+'/PERSONAL-PRINCIPAL',date:'',entity:loan.owner,category:'Loan principal',description:'Principal paid personally beyond the business-loan amount remaining with Personal',amount:money(loan.personalPaidForBusiness)});
  const oweRows=Object.entries(transferDueToBusiness).filter(([,amount])=>amount>0).map(([entity,amount])=>({entity,reason:'Temporary business money received',amount:money(amount)}));
  const loanPersonalByOwner={};for(const loan of loanUsage.pools.filter(row=>row.calculationProven&&row.owner!=='PERSONAL'))loanPersonalByOwner[loan.owner]=money((loanPersonalByOwner[loan.owner]||0)+Number(loan.personalPortion||0));
  for(const [entity,amount] of Object.entries(loanPersonalByOwner))if(amount>0)oweRows.push({entity,reason:'Business loan personal portion',amount});
  for(const position of provenAccountPositions)if(position.personalDeficitClosing>0)oweRows.push({entity:'SAMAST',reason:'Personal deficit in shared account '+position.account,amount:money(position.personalDeficitClosing),account:position.account,basis:position.basis});

  const total=array=>money(array.reduce((sum,row)=>sum+Number(row.amount||0),0));
  const integrityBlockers=[
    ...pendingAccountPositions.flatMap(row=>(row.blockers||[]).map(message=>row.account+': '+message)),
    ...loanUsage.pools.filter(row=>!row.calculationProven).flatMap(row=>row.blockers.map(message=>row.account+': '+message)),
    ...(transferReview.length?[transferReview.length+' cross-entity transfer purpose(s) still need accounting treatment.']:[]),
    ...(unlinkedRows.length?[unlinkedRows.length+' potential Sewak Park funding payment(s) still need transaction evidence.']:[])
  ];
  const integrityChecks=[
    {name:'Personal spending entries',amount:total(spending),expected:money(spending.reduce((sum,row)=>sum+Number(row.amount||0),0))},
    {name:'Finalized source receipts',amount:total(sourceReceiptRows),expected:money(Object.values(sourceReceiptsBySource).reduce((sum,amount)=>sum+Number(amount||0),0))},
    {name:'Recorded inward movements',amount:total(receivedRows),expected:money(Object.values(receivedBySource).reduce((sum,amount)=>sum+Number(amount||0),0))},
    {name:'Direct proven personal use',amount:total(personalUseRows),expected:money(Object.values(personalUseBySource).reduce((sum,amount)=>sum+Number(amount||0),0))}
  ].map(row=>({...row,matched:Math.abs(row.amount-row.expected)<.005}));
  if(integrityChecks.some(row=>!row.matched))throw Object.assign(Error('Personal report arithmetic did not reconcile. No totals were returned.'),{status:409});
  return{
    basis:'Personal spending is counted once. For a business loan received in a Personal account: amount transferred from that receiving account to the borrowing business is the business allocation; principal repaid is separate; the remainder is the Personal portion. The account closing check shows whether the bank ledger matches.',
    spending:{total:money(spending.reduce((sum,row)=>sum+Number(row.amount||0),0)),entries:spending,budgetMonth,budgetFrom:monthFrom,budgetTo:to,budgets,budgetTotal:money(budgets.reduce((sum,row)=>sum+Number(row.budget||0),0)),budgetActual:money(budgets.reduce((sum,row)=>sum+Number(row.actual||0),0)),overBudget:money(budgets.reduce((sum,row)=>sum+Math.max(0,-Number(row.remaining||0)),0))},
    moneyTaken:{
      sourceReceiptRows,sourceReceiptsBySource,sourceReceiptTotal:total(sourceReceiptRows),
      receivedRows,receivedBySource,receivedTotal:total(receivedRows),
      personalUseRows,personalUseBySource,personalUseTotal:total(personalUseRows),
      unlinkedRows,unlinkedBySource,unlinkedTotal:total(unlinkedRows),
      accountPositions,accountProvenUseBySource,accountProvenUseTotal:money(Object.values(accountProvenUseBySource).reduce((sum,amount)=>sum+Number(amount||0),0)),pendingAccountPositions,
      // Compatibility names now mean permanent money received only. Personal
      // expenses are deliberately separate so the two figures cannot be
      // accidentally added together or presented as the same thing.
      permanentRows,bySource:bySource(permanentRows),permanentTotal:total(permanentRows),
      loanPersonalTotal:money(loanUsage.pools.reduce((sum,row)=>sum+Number(row.personalPortion||0),0)),
      loanPersonalProvenTotal:money(loanUsage.pools.filter(row=>row.calculationProven).reduce((sum,row)=>sum+Number(row.personalPortion||0),0))
    },
    settlements:{businessOwesYou:owesYou,businessOwesYouTotal:total(owesYou),youOweBusiness:oweRows,youOweBusinessTotal:total(oweRows),recordedSettlements:transferSettlements,unappliedSettlements:transferSettlements.filter(row=>row.unapplied>0),unappliedSettlementTotal:money(transferSettlements.reduce((sum,row)=>sum+Number(row.unapplied||0),0))},
    loanUsage:loanUsage.loans,loanPools:loanUsage.pools,
    accounts,transfers,
    integrity:{status:integrityBlockers.length?'needs_review':'ready',blockers:integrityBlockers,checks:integrityChecks},
    review:{rows:review,sources:[],total:0,transferRows:transferReview,loanRows:loanUsage.pools.filter(row=>!row.calculationProven)}
  };
}

function ensureState(store){
  store.personalReporting=store.personalReporting&&typeof store.personalReporting==='object'?store.personalReporting:{};
  store.personalReporting.budgets=store.personalReporting.budgets&&typeof store.personalReporting.budgets==='object'?store.personalReporting.budgets:{};
  store.personalReporting.transferTreatments=store.personalReporting.transferTreatments&&typeof store.personalReporting.transferTreatments==='object'?store.personalReporting.transferTreatments:{};
  return store.personalReporting;
}

module.exports={build,ensureState,loanOwner,transferTreatment,movementKey,KNOWN_SANKI_LOANS,INBOUND_TRANSFER_TREATMENTS,OUTBOUND_TRANSFER_TREATMENTS};
