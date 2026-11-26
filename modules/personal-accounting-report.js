'use strict';

// Personal reporting is deliberately a management layer. It never retags a
// bank movement or changes an expense, transfer, loan, amount or date.
const loans=require('./expense-loans');
const refunds=require('./expense-refunds');
const hotel=require('./sewak-park-reporting');

const KNOWN_SANKI_LOANS=new Set(['REC-00200','REC-00201']);
const SANKI_ACCOUNTS=new Set(['Axis Bank 3448','Tiana 0425','Tiana Traders IndusInd 0437','Prashant Axis 3645','IndusInd Bank 8181','Counter Cash','Gagan Sir Cash','Prashant Cash','Services']);
const VALID_SOURCES=new Set(['PERSONAL','SANKI','SAMAST',hotel.UNIT]);
const INBOUND_TRANSFER_TREATMENTS=new Set(['permanent_withdrawal','business_repayment','temporary_due_to_business']);
const OUTBOUND_TRANSFER_TREATMENTS=new Set(['permanent_contribution','personal_repayment','temporary_due_from_business']);
const list=value=>Array.isArray(value)?value:Object.values(value||{});
const nature=value=>String(value||'').trim().toUpperCase();
const money=value=>Math.round(Number(value||0)*100)/100;
const active=row=>row&&!row.accountingExcluded&&!row.voided&&!row.voidedAt&&!row.reversedAt&&row.status!=='voided';
const inRange=(date,from,to)=>String(date||'')>=from&&String(date||'')<=to;
const claimant=row=>String(row.reimbursementClaimant||row.claimant||row.createdBy||'').trim().toLowerCase();
const movementKey=(expense,payment)=>expense.id+'/'+(payment.id||'PAYMENT');
const state=store=>store.personalReporting&&typeof store.personalReporting==='object'?store.personalReporting:{};

function loanOwner(store,loan){
  const saved=state(store).loanOwners||{};
  return nature(saved[loan.id]|| (KNOWN_SANKI_LOANS.has(loan.id)?'SANKI':loan.nature));
}
function sourceChoice(store,value){
  const raw=String(value||'').trim();
  if(VALID_SOURCES.has(raw))return raw;
  if(raw.startsWith('LOAN:')){
    const id=raw.slice(5),loan=loans.loans(store).find(row=>row.id===id);
    if(loan&&loanOwner(store,loan)!=='PERSONAL')return raw;
  }
  return '';
}
function transferTreatment(store,transfer){
  const record=state(store).transferTreatments?.[transfer.id];
  if(record&&[...INBOUND_TRANSFER_TREATMENTS,...OUTBOUND_TRANSFER_TREATMENTS].includes(record.treatment))return record;
  if(transfer.classification==='inter_entity_loan')return{treatment:transfer.toEntity==='PERSONAL'?'temporary_due_to_business':'temporary_due_from_business',reason:'Saved as an inter-entity loan',source:'accounting_record'};
  if(transfer.toEntity==='PERSONAL'&&/pay\w*\s+back|repaid|repayment/i.test(String(transfer.note||'')))return{treatment:'business_repayment',reason:'Saved transfer note identifies a repayment',source:'transfer_note'};
  return null;
}
function paymentRows(store,to){
  const rows=[];
  for(const expense of list(store.expenses).filter(row=>active(row)&&nature(row.nature)==='PERSONAL')){
    for(const payment of list(expense.payments).filter(row=>active(row)&&!row.provisional&&String(row.date||expense.date||'')<=to)){
      rows.push({key:movementKey(expense,payment),id:expense.id,date:String(payment.date||expense.date||''),amount:money(payment.amount),account:payment.account||expense.account||'',expense,payment,category:expense.ledger||expense.category||'Unclassified',description:expense.particulars||expense.vendor||''});
    }
  }
  return rows;
}
function receiptOrder(loan){
  const row=String(loan.bankReconciliationEvidence&&loan.bankReconciliationEvidence.rowId||''),matched=row.match(/(\d+)$/);
  return matched?Number(matched[1]):Number.MAX_SAFE_INTEGER;
}
function allocateLoanUsage(store,{from,to,accountChecks={}}){
  // The management split is deliberately simple: principal sent from the
  // receiving Personal account to the borrowing business is business
  // allocation. Principal repaid to the lender is separate. The remainder is
  // the Personal portion. Individual expenses are not assigned to a loan.
  const catalog=loans.catalog(store,to).filter(loan=>loanOwner(store,loan)!=='PERSONAL').sort((a,b)=>String(a.date).localeCompare(String(b.date))||receiptOrder(a)-receiptOrder(b)||String(a.createdAt||'').localeCompare(String(b.createdAt||''))||String(a.id).localeCompare(String(b.id)));
  const reports=new Map(catalog.map(loan=>[loan.id,{id:loan.id,date:loan.date,lender:loan.source||'Lender',reference:loan.bankReference||loan.bankReconciliationEvidence&&loan.bankReconciliationEvidence.reference||loan.id,account:loan.account,owner:loanOwner(store,loan),borrowed:money(loan.borrowed),repaid:money(loan.repaid),outstanding:money(loan.outstanding),businessUse:0,personalUse:0,personalPortion:0,periodBusinessUse:0,events:[],accountCheck:accountChecks[loan.id]||accountChecks[loan.account]||null} ]));
  const available=new Map(catalog.map(loan=>[loan.id,money(Math.max(0,loan.borrowed-loan.repaid))])),businessTransferAllocations={};
  const outbound=list(store.transfers).filter(row=>active(row)&&String(row.date||'')<=to&&nature(row.fromNature||row.nature)==='PERSONAL'&&nature(row.toNature||row.nature)!=='PERSONAL').sort((a,b)=>String(a.date).localeCompare(String(b.date))||String(a.createdAt||'').localeCompare(String(b.createdAt||''))||String(a.id).localeCompare(String(b.id)));
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
  for(const report of reports.values()){
    report.personalPortion=money(Math.max(0,report.borrowed-report.repaid-report.businessUse));report.personalUse=report.personalPortion;report.remaining=report.personalPortion;report.allocationStatus=report.accountCheck&&report.accountCheck.matched?'Account matched':'Calculated from account transfers';report.allocationReason='Loan received minus principal repaid minus transfers from '+report.account+' to '+report.owner+'.';
    if(report.repaid)report.events.unshift({id:report.id+'/REPAID',date:'',kind:'principal_repaid',description:'Principal repaid to lender',account:'',amount:report.repaid,use:'Repaid'});
    if(report.personalPortion)report.events.push({id:report.id+'/PERSONAL',date:'',kind:'personal_portion',description:'Loan amount remaining in Personal after business transfer and principal repayment',account:report.account,amount:report.personalPortion,use:'Personal portion'});
  }
  return{loans:[...reports.values()],businessTransferAllocations};
}

function build(store,{from,to,viewer='',spendingRows=[],accounts=[],transfers=[],loanAccountChecks={}}){
  const settings=state(store),movementSources=settings.movementSources||{},loanUsage=allocateLoanUsage(store,{from,to,accountChecks:loanAccountChecks});
  const paymentByKey=new Map(paymentRows(store,to).map(row=>[row.key,row]));
  const spending=spendingRows.filter(row=>row.kind==='expense');
  const budgetMonth=to.slice(0,7),monthStart=budgetMonth+'-01',monthFrom=from>monthStart?from:monthStart;
  const monthActual=new Map(),periodActual=new Map();
  for(const row of spending){const category=row.category||'Unclassified';periodActual.set(category,money((periodActual.get(category)||0)+Number(row.amount||0)));if(String(row.date||'').startsWith(budgetMonth))monthActual.set(category,money((monthActual.get(category)||0)+Number(row.amount||0)));}
  const savedBudgets=settings.budgets?.[budgetMonth]||{},categories=[...new Set([...periodActual.keys(),...Object.keys(savedBudgets)])].sort((a,b)=>a.localeCompare(b));
  const budgets=categories.map(category=>{const actual=money(monthActual.get(category)||0),budget=Object.prototype.hasOwnProperty.call(savedBudgets,category)?money(savedBudgets[category]):null,remaining=budget==null?null:money(budget-actual);return{category,periodActual:money(periodActual.get(category)||0),actual,budget,remaining,status:budget==null?'Set budget':remaining<0?'Over budget':'Within budget'};});

  const permanentRows=[],review=[],transferReview=[],transferSettlements=[],transferDueToBusiness={},transferDueFromBusiness={};
  for(const original of transfers.filter(row=>(row.toEntity==='PERSONAL')!==(row.fromEntity==='PERSONAL'))){
    const allocated=money(loanUsage.businessTransferAllocations[original.id]||0),transfer=allocated?{...original,amount:money(original.amount-allocated),loanAllocated:allocated}:original;if(!(transfer.amount>0))continue;
    const inbound=transfer.toEntity==='PERSONAL',business=inbound?transfer.fromEntity:transfer.toEntity,decision=transferTreatment(store,transfer);
    if(!decision){transferReview.push({...transfer,direction:inbound?'Business to personal':'Personal to business',business,note:transfer.note||'',options:inbound?[{value:'permanent_withdrawal',label:'Permanent personal withdrawal — nothing to return'},{value:'business_repayment',label:'Business repaid money it owed you'},{value:'temporary_due_to_business',label:'Temporary business money — you must return it'}]:[{value:'permanent_contribution',label:'Permanent contribution to business — nothing to return'},{value:'personal_repayment',label:'You repaid money owed to business'},{value:'temporary_due_from_business',label:'Temporary personal money — business must return it'}]});continue;}
    if(decision.treatment==='permanent_withdrawal')permanentRows.push({id:transfer.id,date:transfer.date,source:business,label:'Taken permanently from '+business,account:transfer.from+' → '+transfer.to,description:transfer.note||'',amount:money(transfer.amount),treatment:'Confirmed permanent — nothing to return'});
    if(decision.treatment==='temporary_due_to_business')transferDueToBusiness[business]=money((transferDueToBusiness[business]||0)+transfer.amount);
    if(decision.treatment==='temporary_due_from_business')transferDueFromBusiness[business]=money((transferDueFromBusiness[business]||0)+transfer.amount);
    if(['business_repayment','personal_repayment'].includes(decision.treatment))transferSettlements.push({id:transfer.id,date:transfer.date,entity:business,direction:decision.treatment==='business_repayment'?'Business repaid you':'You repaid business',amount:money(transfer.amount),note:transfer.note||'',reason:decision.reason||''});
  }
  for(const row of spending){
    const payment=paymentByKey.get(row.id),expense=payment&&payment.expense,saved=sourceChoice(store,movementSources[row.id]?.source);
    let source=saved;
    if(!source&&expense&&hotel.isHotelPersonalUse(expense))source=hotel.UNIT;
    if(!source&&payment?.payment?.crossEntityCompanyPayment&&SANKI_ACCOUNTS.has(payment.account))source='SANKI';
    if(source&&source!=='PERSONAL'&&!source.startsWith('LOAN:'))permanentRows.push({id:row.id,date:row.date,source,label:source===hotel.UNIT?'Personal use of Sewak Park money':'Taken permanently from '+source,account:row.account,category:row.category,description:row.description,amount:money(row.amount),treatment:'Permanent — nothing to return'});
    // A posted PERSONAL expense is already personal spending. The paying
    // account does not require a second funding-source decision here.
  }
  const takenBySource=permanentRows.reduce((out,row)=>(out[row.source]=money((out[row.source]||0)+row.amount),out),{});

  const owesYou=[];const you=String(viewer||'').trim().toLowerCase();
  for(const expense of list(store.expenses).filter(row=>active(row)&&['SANKI','SAMAST'].includes(nature(row.nature))&&String(row.date||'')<=to&&claimant(row)===you)){
    const position=refunds.reimbursementPosition(store,expense);if(position.pending>0)owesYou.push({id:expense.id,date:expense.date,entity:nature(expense.nature),category:expense.ledger||expense.category||'',description:expense.particulars||expense.vendor||'',amount:money(position.pending)});
  }
  for(const [entity,amount] of Object.entries(transferDueFromBusiness))if(amount>0)owesYou.push({id:'TRANSFER-DUE-'+entity,date:'',entity,category:'Temporary transfer',description:'Temporary personal money sent to business',amount});
  const oweRows=Object.entries(transferDueToBusiness).filter(([,amount])=>amount>0).map(([entity,amount])=>({entity,reason:'Temporary business money received',amount:money(amount)}));
  const loanPersonalByOwner={};for(const loan of loanUsage.loans)loanPersonalByOwner[loan.owner]=money((loanPersonalByOwner[loan.owner]||0)+Number(loan.personalPortion||0));
  for(const [entity,amount] of Object.entries(loanPersonalByOwner))if(amount>0)oweRows.push({entity,reason:'Business loan personal portion',amount});

  const reviewSources=[{value:'PERSONAL',label:'Own personal money'},{value:'SANKI',label:'Taken permanently from SANKI'},{value:'SAMAST',label:'Taken permanently from SAMAST'},{value:hotel.UNIT,label:'Personal use of Sewak Park money'}];
  const total=array=>money(array.reduce((sum,row)=>sum+Number(row.amount||0),0));
  return{
    basis:'Personal spending is counted once. For a business loan received in a Personal account: amount transferred from that receiving account to the borrowing business is the business allocation; principal repaid is separate; the remainder is the Personal portion. The account closing check shows whether the bank ledger matches.',
    spending:{total:money(spending.reduce((sum,row)=>sum+Number(row.amount||0),0)),entries:spending,budgetMonth,budgetFrom:monthFrom,budgetTo:to,budgets,budgetTotal:money(budgets.reduce((sum,row)=>sum+Number(row.budget||0),0)),budgetActual:money(budgets.reduce((sum,row)=>sum+Number(row.actual||0),0)),overBudget:money(budgets.reduce((sum,row)=>sum+Math.max(0,-Number(row.remaining||0)),0))},
    moneyTaken:{permanentRows,bySource:takenBySource,permanentTotal:total(permanentRows),loanPersonalTotal:money(loanUsage.loans.reduce((sum,row)=>sum+Number(row.personalPortion||0),0))},
    settlements:{businessOwesYou:owesYou,businessOwesYouTotal:total(owesYou),youOweBusiness:oweRows,youOweBusinessTotal:total(oweRows),recordedSettlements:transferSettlements},
    loanUsage:loanUsage.loans,
    accounts,transfers,
    review:{rows:review,sources:reviewSources,total:money(review.reduce((sum,row)=>sum+row.amount,0)),transferRows:transferReview,loanRows:[]}
  };
}

function ensureState(store){
  store.personalReporting=store.personalReporting&&typeof store.personalReporting==='object'?store.personalReporting:{};
  store.personalReporting.budgets=store.personalReporting.budgets&&typeof store.personalReporting.budgets==='object'?store.personalReporting.budgets:{};
  store.personalReporting.movementSources=store.personalReporting.movementSources&&typeof store.personalReporting.movementSources==='object'?store.personalReporting.movementSources:{};
  store.personalReporting.loanOwners=store.personalReporting.loanOwners&&typeof store.personalReporting.loanOwners==='object'?store.personalReporting.loanOwners:{};
  store.personalReporting.loanAllocations=store.personalReporting.loanAllocations&&typeof store.personalReporting.loanAllocations==='object'?store.personalReporting.loanAllocations:{};
  store.personalReporting.transferTreatments=store.personalReporting.transferTreatments&&typeof store.personalReporting.transferTreatments==='object'?store.personalReporting.transferTreatments:{};
  return store.personalReporting;
}

module.exports={build,ensureState,loanOwner,sourceChoice,transferTreatment,movementKey,KNOWN_SANKI_LOANS,VALID_SOURCES,INBOUND_TRANSFER_TREATMENTS,OUTBOUND_TRANSFER_TREATMENTS};
