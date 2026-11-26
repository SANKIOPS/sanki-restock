'use strict';

// Personal reporting is deliberately a management layer. It never retags a
// bank movement or changes an expense, transfer, loan, amount or date.
const loans=require('./expense-loans');
const refunds=require('./expense-refunds');
const hotel=require('./sewak-park-reporting');

const KNOWN_SANKI_LOANS=new Set(['REC-00200','REC-00201']);
const SHARED_SOURCE_ACCOUNTS=new Set(['IndusInd Bank 7883','ICICI Bank 0993','ICICI Bank 0992']);
const SANKI_ACCOUNTS=new Set(['Axis Bank 3448','Tiana 0425','Tiana Traders IndusInd 0437','Prashant Axis 3645','IndusInd Bank 8181','Counter Cash','Gagan Sir Cash','Prashant Cash','Services']);
const VALID_SOURCES=new Set(['PERSONAL','SANKI','SAMAST',hotel.UNIT]);
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
function paymentRows(store,to){
  const rows=[];
  for(const expense of list(store.expenses).filter(row=>active(row)&&nature(row.nature)==='PERSONAL')){
    for(const payment of list(expense.payments).filter(row=>active(row)&&!row.provisional&&String(row.date||expense.date||'')<=to)){
      rows.push({key:movementKey(expense,payment),id:expense.id,date:String(payment.date||expense.date||''),amount:money(payment.amount),account:payment.account||expense.account||'',expense,payment,category:expense.ledger||expense.category||'Unclassified',description:expense.particulars||expense.vendor||''});
    }
  }
  return rows;
}
function allocateLoanUsage(store,{from,to}){
  const catalog=loans.catalog(store,to).filter(loan=>loanOwner(store,loan)!=='PERSONAL');
  const reports=new Map(catalog.map(loan=>[loan.id,{id:loan.id,date:loan.date,lender:loan.source||'Lender',account:loan.account,owner:loanOwner(store,loan),borrowed:money(loan.borrowed),outstanding:money(loan.outstanding),businessUse:0,personalUse:0,periodBusinessUse:0,periodPersonalUse:0,remaining:0,events:[]} ]));
  const lots=new Map(),allocations=new Map(),overrides=state(store).movementSources||{};
  const addLot=(account,loanId,amount)=>{if(!(amount>0))return;if(!lots.has(account))lots.set(account,[]);lots.get(account).push({loanId,amount:money(amount)});};
  const record=(lot,event,amount,bucket)=>{
    const report=reports.get(lot.loanId);if(!report)return;
    report[bucket]=money(report[bucket]+amount);
    if(inRange(event.date,from,to))report['period'+bucket[0].toUpperCase()+bucket.slice(1)]=money(report['period'+bucket[0].toUpperCase()+bucket.slice(1)]+amount);
    report.events.push({id:event.id,date:event.date,kind:event.kind,description:event.description,account:event.account,amount:money(amount),use:bucket==='personalUse'?'Used personally':'Used for business'});
    if(bucket==='personalUse')allocations.set(event.id,{loanId:lot.loanId,amount:money((allocations.get(event.id)?.amount||0)+amount)});
  };
  const take=(account,amount,event,bucket,destination='',specificLoan='')=>{
    let left=money(amount),source=lots.get(account)||[];
    for(const lot of source){
      if(left<=0)break;if(specificLoan&&lot.loanId!==specificLoan)continue;
      const used=money(Math.min(left,lot.amount));if(!(used>0))continue;
      lot.amount=money(lot.amount-used);left=money(left-used);
      if(bucket==='move')addLot(destination,lot.loanId,used);else record(lot,event,used,bucket);
    }
    lots.set(account,source.filter(lot=>lot.amount>0));return money(amount-left);
  };
  const events=[];
  for(const loan of catalog)events.push({priority:0,id:loan.id,date:loan.date,kind:'loan',account:loan.account,amount:loan.borrowed,loanId:loan.id,description:loan.source||loan.id});
  for(const transfer of list(store.transfers).filter(row=>active(row)&&String(row.date||'')<=to))events.push({priority:1,id:transfer.id,date:transfer.date,kind:'transfer',account:transfer.fromAccount,toAccount:transfer.toAccount,fromNature:nature(transfer.fromNature||transfer.nature),toNature:nature(transfer.toNature||transfer.nature),classification:transfer.classification||'',amount:money(transfer.amount),description:(transfer.fromAccount||'')+' → '+(transfer.toAccount||'')});
  for(const row of paymentRows(store,to))events.push({...row,id:row.key,priority:2,kind:'personal_expense',description:row.description||row.category});
  events.sort((a,b)=>String(a.date).localeCompare(String(b.date))||a.priority-b.priority||String(a.id).localeCompare(String(b.id)));
  for(const event of events){
    if(event.kind==='loan'){addLot(event.account,event.loanId,event.amount);continue;}
    if(event.kind==='transfer'){
      if(event.toNature==='PERSONAL')take(event.account,event.amount,event,'move',event.toAccount);
      else take(event.account,event.amount,event,'businessUse');
      continue;
    }
    const saved=sourceChoice(store,overrides[event.id]?.source);
    if(saved&&!saved.startsWith('LOAN:'))continue;
    take(event.account,event.amount,event,'personalUse','',saved.startsWith('LOAN:')?saved.slice(5):'');
  }
  for(const accountLots of lots.values())for(const lot of accountLots){const report=reports.get(lot.loanId);if(report)report.remaining=money(report.remaining+lot.amount);}
  return{loans:[...reports.values()].sort((a,b)=>a.date.localeCompare(b.date)||a.id.localeCompare(b.id)),allocations};
}

function build(store,{from,to,viewer='',spendingRows=[],accounts=[],transfers=[]}){
  const settings=state(store),movementSources=settings.movementSources||{},loanUsage=allocateLoanUsage(store,{from,to});
  const paymentByKey=new Map(paymentRows(store,to).map(row=>[row.key,row]));
  const spending=spendingRows.filter(row=>row.kind==='expense');
  const budgetMonth=to.slice(0,7),monthStart=budgetMonth+'-01',monthFrom=from>monthStart?from:monthStart;
  const monthActual=new Map(),periodActual=new Map();
  for(const row of spending){const category=row.category||'Unclassified';periodActual.set(category,money((periodActual.get(category)||0)+Number(row.amount||0)));if(String(row.date||'').startsWith(budgetMonth))monthActual.set(category,money((monthActual.get(category)||0)+Number(row.amount||0)));}
  const savedBudgets=settings.budgets?.[budgetMonth]||{},categories=[...new Set([...periodActual.keys(),...Object.keys(savedBudgets)])].sort((a,b)=>a.localeCompare(b));
  const budgets=categories.map(category=>{const actual=money(monthActual.get(category)||0),budget=Object.prototype.hasOwnProperty.call(savedBudgets,category)?money(savedBudgets[category]):null,remaining=budget==null?null:money(budget-actual);return{category,periodActual:money(periodActual.get(category)||0),actual,budget,remaining,status:budget==null?'Set budget':remaining<0?'Over budget':'Within budget'};});

  const permanentRows=[],review=[];
  for(const transfer of transfers.filter(row=>row.classification==='owner_withdrawal'&&row.toEntity==='PERSONAL'))permanentRows.push({id:transfer.id,date:transfer.date,source:transfer.fromEntity,label:'Taken permanently from '+transfer.fromEntity,account:transfer.from+' → '+transfer.to,amount:money(transfer.amount),treatment:'Permanent — nothing to return'});
  for(const row of spending){
    const payment=paymentByKey.get(row.id),expense=payment&&payment.expense,saved=sourceChoice(store,movementSources[row.id]?.source),loanAllocation=loanUsage.allocations.get(row.id);
    if(loanAllocation)continue;
    let source=saved;
    if(!source&&expense&&hotel.isHotelPersonalUse(expense))source=hotel.UNIT;
    if(!source&&payment?.payment?.crossEntityCompanyPayment&&SANKI_ACCOUNTS.has(payment.account))source='SANKI';
    if(source&&source!=='PERSONAL'&&!source.startsWith('LOAN:'))permanentRows.push({id:row.id,date:row.date,source,label:source===hotel.UNIT?'Personal use of Sewak Park money':'Taken permanently from '+source,account:row.account,category:row.category,description:row.description,amount:money(row.amount),treatment:'Permanent — nothing to return'});
    else if(!source&&SHARED_SOURCE_ACCOUNTS.has(row.account))review.push({id:row.id,date:row.date,account:row.account,category:row.category,description:row.description,amount:money(row.amount),suggestion:row.account==='ICICI Bank 0992'?'Check: Sewak Park or personal':row.account==='IndusInd Bank 7883'?'Check: SAMAST or personal':'Check: personal, SANKI loan or SAMAST'});
  }
  const takenBySource=permanentRows.reduce((out,row)=>(out[row.source]=money((out[row.source]||0)+row.amount),out),{});

  const owesYou=[];const you=String(viewer||'').trim().toLowerCase();
  for(const expense of list(store.expenses).filter(row=>active(row)&&['SANKI','SAMAST'].includes(nature(row.nature))&&String(row.date||'')<=to&&claimant(row)===you)){
    const position=refunds.reimbursementPosition(store,expense);if(position.pending>0)owesYou.push({id:expense.id,date:expense.date,entity:nature(expense.nature),category:expense.ledger||expense.category||'',description:expense.particulars||expense.vendor||'',amount:money(position.pending)});
  }
  const interEntity={};
  for(const row of transfers.filter(item=>item.classification==='inter_entity_loan')){
    if(row.toEntity==='PERSONAL'&&row.fromEntity!=='PERSONAL')interEntity[row.fromEntity]=money((interEntity[row.fromEntity]||0)+row.amount);
    if(row.fromEntity==='PERSONAL'&&row.toEntity!=='PERSONAL')interEntity[row.toEntity]=money((interEntity[row.toEntity]||0)-row.amount);
  }
  const oweRows=Object.entries(interEntity).filter(([,amount])=>amount>0).map(([entity,amount])=>({entity,reason:'Temporary money received',amount:money(amount)}));
  const loanPersonalByOwner={};for(const loan of loanUsage.loans)loanPersonalByOwner[loan.owner]=money((loanPersonalByOwner[loan.owner]||0)+loan.personalUse);
  for(const [entity,amount] of Object.entries(loanPersonalByOwner))if(amount>0)oweRows.push({entity,reason:'Business loan used personally',amount});

  const reviewSources=[{value:'PERSONAL',label:'Own personal money'},{value:'SANKI',label:'Taken permanently from SANKI'},{value:'SAMAST',label:'Taken permanently from SAMAST'},{value:hotel.UNIT,label:'Personal use of Sewak Park money'},...loanUsage.loans.map(loan=>({value:'LOAN:'+loan.id,label:loan.owner+' loan · '+loan.lender+' · '+loan.id}))];
  const total=array=>money(array.reduce((sum,row)=>sum+Number(row.amount||0),0));
  return{
    basis:'Personal spending is counted once. Permanent money taken from an entity is shown separately. Business-loan money used personally is repayable to that business, while ordinary owner withdrawals are not.',
    spending:{total:money(spending.reduce((sum,row)=>sum+Number(row.amount||0),0)),entries:spending,budgetMonth,budgetFrom:monthFrom,budgetTo:to,budgets,budgetTotal:money(budgets.reduce((sum,row)=>sum+Number(row.budget||0),0)),budgetActual:money(budgets.reduce((sum,row)=>sum+Number(row.actual||0),0)),overBudget:money(budgets.reduce((sum,row)=>sum+Math.max(0,-Number(row.remaining||0)),0))},
    moneyTaken:{permanentRows,bySource:takenBySource,permanentTotal:total(permanentRows),loanPersonalTotal:money(loanUsage.loans.reduce((sum,row)=>sum+row.personalUse,0))},
    settlements:{businessOwesYou:owesYou,businessOwesYouTotal:total(owesYou),youOweBusiness:oweRows,youOweBusinessTotal:total(oweRows)},
    loanUsage:loanUsage.loans,
    accounts,transfers,
    review:{rows:review,sources:reviewSources,total:money(review.reduce((sum,row)=>sum+row.amount,0))}
  };
}

function ensureState(store){
  store.personalReporting=store.personalReporting&&typeof store.personalReporting==='object'?store.personalReporting:{};
  store.personalReporting.budgets=store.personalReporting.budgets&&typeof store.personalReporting.budgets==='object'?store.personalReporting.budgets:{};
  store.personalReporting.movementSources=store.personalReporting.movementSources&&typeof store.personalReporting.movementSources==='object'?store.personalReporting.movementSources:{};
  store.personalReporting.loanOwners=store.personalReporting.loanOwners&&typeof store.personalReporting.loanOwners==='object'?store.personalReporting.loanOwners:{};
  return store.personalReporting;
}

module.exports={build,ensureState,loanOwner,sourceChoice,movementKey,KNOWN_SANKI_LOANS,VALID_SOURCES};
