'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const ORDERS_PATH = process.env.ORDERS_PATH || path.join(DATA_DIR, 'orders.json');
const INCENTIVES_PATH = process.env.INCENTIVES_PATH || path.join(DATA_DIR, 'incentives.json');
const RATE = 0.02;
const DAILY_THRESHOLD = 10000;
const SALESPERSONS = [
  { name:'Shivam', aliases:['shivam'], incentiveEligible:true },
  { name:'Krishnakant', aliases:['krishnakant', 'krishna kant', 'krishna'], incentiveEligible:true },
  { name:'Isha', aliases:['isha'], incentiveEligible:false },
  { name:'Nandini', aliases:['nandini'], incentiveEligible:false },
  { name:'Simran', aliases:['simran'], incentiveEligible:false }
];
const PAYING_ACCOUNTS = ['Axis Bank 3448', 'Prashant Axis 3645', 'IndusInd Bank 8181', 'Counter Cash', 'Prashant Cash', 'Gagan Sir Cash'];

const money = value => Math.round((Number(value) || 0) * 100) / 100;
const text = (value, max=2000) => String(value || '').trim().slice(0, max);
const validDate = value => /^\d{4}-\d{2}-\d{2}$/.test(String(value || '')) && !Number.isNaN(Date.parse(String(value) + 'T00:00:00Z'));
const roles = req => (req.user && (req.user.roles || [req.user.role])) || [];
const allowed = req => roles(req).some(role => ['owner', 'admin', 'accounting'].includes(role)) || String(req.user && req.user.username || '').trim().toLowerCase() === 'prashant';

function blankState(){ return { revision:0, reviews:{}, approvals:{}, payments:[], paymentSeq:0, audit:[] }; }
function readJson(file, fallback){ try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; } }
function writeJson(file, value){ const tmp=file+'.tmp-'+process.pid+'-'+Date.now();fs.writeFileSync(tmp,JSON.stringify(value,null,2));fs.renameSync(tmp,file); }
function loadOrders(){ const store=readJson(ORDERS_PATH,{orders:{}});return Object.values(store.orders || {}); }
function loadState(){ return Object.assign(blankState(), readJson(INCENTIVES_PATH, blankState())); }
function saveState(state){ writeJson(INCENTIVES_PATH, state); }

function normalizedWords(value){
  return String(value || '').normalize('NFKD').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().replace(/\s+/g, ' ');
}
function compact(value){ return normalizedWords(value).replace(/\s/g, ''); }
function incentiveEligible(name){ return SALESPERSONS.some(person=>person.name===name&&person.incentiveEligible); }
function escapeRegExp(value){ return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
function levenshtein(a,b){
  a=compact(a);b=compact(b);const row=Array.from({length:b.length+1},(_,i)=>i);
  for(let i=1;i<=a.length;i++){let prev=row[0];row[0]=i;for(let j=1;j<=b.length;j++){const old=row[j];row[j]=Math.min(row[j]+1,row[j-1]+1,prev+(a[i-1]===b[j-1]?0:1));prev=old;}}
  return row[b.length];
}
function parseSalespeople(note){
  const original=String(note || ''),lines=original.split(/\r?\n/).map(x=>x.trim()).filter(Boolean),first=lines[0] || '';
  const reference=((first.match(/^\D*(\d{6})(?!\d)/) || original.match(/(?:^|\s)(\d{6})(?!\d)/)) || [])[1] || '';
  const nameSource=lines.length ? [first.replace(/^\D*\d{6}(?!\d)[\s:;|,/-]*/, ''), ...lines.slice(1)].join(' ') : original;
  const normalized=normalizedWords(nameSource),matched=[];
  SALESPERSONS.forEach(person=>{
    if(person.aliases.some(alias=>new RegExp('(?:^|\\s)'+escapeRegExp(normalizedWords(alias)).replace(/\\ /g,'\\s+')+'(?:$|\\s)').test(normalized)))matched.push(person.name);
  });
  if(matched.length)return {reference,salespersons:matched,suggestions:[],confidence:'confirmed',source:nameSource};
  const tokens=normalized.split(' ').filter(Boolean),phrases=tokens.concat(tokens.slice(0,-1).map((token,index)=>token+' '+tokens[index+1])),suggestions=[];
  SALESPERSONS.forEach(person=>{
    let best=99,bestText='';
    person.aliases.forEach(alias=>phrases.forEach(phrase=>{const distance=levenshtein(alias,phrase);if(distance<best){best=distance;bestText=phrase;}}));
    const minimumLength=Math.min(...person.aliases.map(alias=>compact(alias).length));
    if(best<=2&&minimumLength>=6)suggestions.push({name:person.name,matchedText:bestText,distance:best});
  });
  suggestions.sort((a,b)=>a.distance-b.distance||a.name.localeCompare(b.name));
  return {reference,salespersons:[],suggestions,confidence:suggestions.length?'suggested':'unmatched',source:nameSource};
}

function classifyGateway(gateway){
  const value=normalizedWords(gateway);
  if(!value)return 'unknown';
  if(/store credit|storecredit|gift card|giftcard/.test(value))return 'store_credit';
  if(/cash on delivery|\bcod\b/.test(value))return 'excluded';
  if(/\bcash\b/.test(value))return 'cash';
  if(/\bupi\b|paytm|phonepe|phone pe|google pay|gpay|bharatpe|bhim|card|visa|mastercard|master card|rupay|amex|pinelabs|pine labs|shopify payments/.test(value))return 'upi_card';
  return 'unknown';
}
function successfulReceipt(tx){ return ['sale','capture'].includes(String(tx.kind || '').toLowerCase()) && String(tx.status || '').toLowerCase() === 'success' && money(tx.amount)>0; }
function receiptDate(tx, order){ return String(tx.processedAt || tx.processed_at || order.processedAt || order.createdAt || '').slice(0,10); }
function splitMoney(amount, count){
  if(count<=1)return [money(amount)];const cents=Math.round(money(amount)*100),base=Math.floor(cents/count),remainder=cents-base*count;
  return Array.from({length:count},(_,index)=>(base+(index<remainder?1:0))/100);
}

function buildRecords(orders, state){
  const records=[];
  // A return, refund or later cancellation does not reverse an incentive once
  // a successful in-store receipt exists. Receipt transactions, not the
  // order's current lifecycle label, decide whether value was collected.
  (orders || []).filter(order=>String(order.channel || '').toUpperCase()==='POS').forEach(order=>{
    const review=(state.reviews || {})[String(order.id)] || {},parsed=parseSalespeople(order.note),salespersons=Array.isArray(review.salespersons)?review.salespersons.filter(name=>SALESPERSONS.some(person=>person.name===name)):parsed.salespersons;
    const noSalesperson=review.noSalesperson===true,transactions=(order.paymentTransactions || []).filter(successfulReceipt),groups=new Map();
    if(!transactions.length){
      const date=String(order.processedAt || order.createdAt || '').slice(0,10);groups.set(date,[]);
    } else transactions.forEach(tx=>{const date=receiptDate(tx,order);if(!groups.has(date))groups.set(date,[]);groups.get(date).push(tx);});
    groups.forEach((rows,date)=>{
      if(!validDate(date))return;
      const mapped=rows.map(tx=>{const automaticMode=classifyGateway(tx.gateway),effectiveMode=(review.gatewayModes || {})[String(tx.id)] || automaticMode;return {id:String(tx.id || ''),gateway:String(tx.gateway || ''),amount:money(tx.amount),automaticMode,effectiveMode};});
      const sum=mode=>money(mapped.filter(tx=>tx.effectiveMode===mode).reduce((total,tx)=>total+tx.amount,0)),cash=sum('cash'),upiCard=sum('upi_card'),storeCredit=sum('store_credit'),otherExcluded=sum('excluded'),unknown=sum('unknown'),eligible=money(cash+upiCard),totalReceived=money(mapped.reduce((total,tx)=>total+tx.amount,0));
      const recordOnlySalespersons=salespersons.filter(name=>!incentiveEligible(name)),recordOnly=!noSalesperson&&salespersons.length>0&&recordOnlySalespersons.length===salespersons.length;
      // Record-only orders cannot create a payable, so tender review is not
      // required for them. Mixed orders retain every normal receipt check.
      const issues=[];if(!recordOnly&&!rows.length)issues.push('Payment transactions have not been synced from Shopify.');if(!noSalesperson&&!salespersons.length)issues.push(parsed.suggestions.length?'Confirm the suggested salesperson spelling.':'Salesperson was not found in the order note.');if(!recordOnly&&unknown>0)issues.push('Classify '+mapped.filter(tx=>tx.effectiveMode==='unknown').map(tx=>tx.gateway||'unnamed gateway').join(', ')+'.');
      const portions=splitMoney(eligible,salespersons.length),recordOnlyAmount=noSalesperson?0:money(salespersons.reduce((total,name,index)=>total+(!incentiveEligible(name)?portions[index]:0),0)),incentiveEligibleAmount=noSalesperson?0:money(eligible-recordOnlyAmount);
      let exclusionReason='';if(noSalesperson)exclusionReason='Confirmed: no eligible salesperson';else if(recordOnlySalespersons.length)exclusionReason=recordOnlySalespersons.join(' + ')+': former employee — record only; no incentive payable on their share.';else if(!issues.length&&eligible<=0)exclusionReason=storeCredit>0&&storeCredit===totalReceived?'Store credit only':'No eligible cash, UPI or card-machine receipt';
      const status=recordOnly?'record_only':issues.length?'needs_review':(noSalesperson||eligible<=0?'excluded':'ready');
      records.push({
        id:String(order.id)+'|'+date,orderId:String(order.id),orderNumber:String(order.name || order.number || order.id),customerName:String(order.customer&&order.customer.name || ''),receiptDate:date,
        salespersons,noSalesperson,recordOnly,recordOnlySalespersons,salespersonSuggestions:parsed.suggestions,matchConfidence:Array.isArray(review.salespersons)?'reviewed':parsed.confidence,transactionReference:parsed.reference,
        billAmount:money(order.total),totalReceived,cashAmount:cash,upiCardAmount:upiCard,storeCreditAmount:storeCredit,otherExcludedAmount:otherExcluded,unknownAmount:unknown,eligibleTenderAmount:eligible,eligibleAmount:incentiveEligibleAmount,recordOnlyAmount,
        refundAmount:money(order.refundAmount),orderNote:String(order.note || ''),transactions:mapped,reviewStatus:status,reviewIssues:issues,exclusionReason,reviewedAt:review.reviewedAt || '',reviewedBy:review.reviewedBy || ''
      });
    });
  });
  return records.sort((a,b)=>a.receiptDate.localeCompare(b.receiptDate)||a.orderNumber.localeCompare(b.orderNumber,undefined,{numeric:true}));
}

function calculate(orders,state){
  const records=buildRecords(orders,state),dailyMap=new Map();
  records.forEach(record=>{
    record.shares=[];if(!['ready','record_only'].includes(record.reviewStatus)||!record.salespersons.length)return;
    const amounts=splitMoney(record.eligibleTenderAmount,record.salespersons.length);
    record.salespersons.forEach((salesperson,index)=>{
      const recordOnly=!incentiveEligible(salesperson),key=record.receiptDate+'|'+salesperson,share={salesperson,recordOnly,receiptAmount:amounts[index],eligibleAmount:recordOnly?0:amounts[index],incentive:0,qualifies:false,qualificationDate:'',inheritedQualification:false,approvalStatus:recordOnly?'record_only':'not_eligible'};record.shares.push(share);
      if(!dailyMap.has(key))dailyMap.set(key,{key,date:record.receiptDate,salesperson,recordOnly,eligibleAmount:0,recordOnlyAmount:0,incentiveBaseAmount:0,carriedQualifiedAmount:0,thresholdMet:false,qualificationSources:[],qualificationReviewDates:[],incentive:0,qualifies:false,records:[],unresolved:false});
      const day=dailyMap.get(key);day.eligibleAmount=money(day.eligibleAmount+share.eligibleAmount);day.recordOnlyAmount=money(day.recordOnlyAmount+(recordOnly?share.receiptAmount:0));day.records.push({record,share});
    });
  });
  const unresolvedDates=new Set(records.filter(row=>row.reviewStatus==='needs_review').map(row=>row.receiptDate)),qualifiedOrders=new Map();
  const daily=Array.from(dailyMap.values()).sort((a,b)=>a.date.localeCompare(b.date)||a.salesperson.localeCompare(b.salesperson));
  // Policy (2026-10-10): once this order/person participates in a qualifying
  // day, later cash/UPI/card receipts retain that qualification. Only money
  // actually received earns 2%, on its receipt date (50,000 now + 7,500 later
  // means 1,000 now + 150 later). Never transfer qualification to other orders,
  // backdate later collections, or bypass review of the qualifying source day.
  daily.forEach(day=>{
    if(day.recordOnly){day.approval=null;day.approvalStatus='record_only';return;}
    day.thresholdMet=day.eligibleAmount>=DAILY_THRESHOLD;day.unresolved=unresolvedDates.has(day.date);
    day.records.forEach(({record,share})=>{
      const orderKey=JSON.stringify([record.orderId,share.salesperson]);let prior=qualifiedOrders.get(orderKey);
      // A later independently qualifying, reviewed day can become the source
      // instead of keeping future collections blocked on a provisional day.
      if(prior&&day.thresholdMet&&!unresolvedDates.has(day.date)&&unresolvedDates.has(prior.date))prior=null;
      share.qualifies=share.eligibleAmount>0&&(day.thresholdMet||Boolean(prior));
      if(!share.qualifies)return;
      share.qualificationDate=prior?prior.date:day.date;share.inheritedQualification=Boolean(prior);
      share.incentive=money(share.eligibleAmount*RATE);day.incentiveBaseAmount=money(day.incentiveBaseAmount+share.eligibleAmount);
      if(prior){
        day.carriedQualifiedAmount=money(day.carriedQualifiedAmount+share.eligibleAmount);
        day.qualificationSources.push({orderId:record.orderId,orderNumber:record.orderNumber,qualificationDate:prior.date,qualifyingDailyAmount:prior.eligibleAmount,receivedAmount:share.eligibleAmount});
        if(!day.thresholdMet&&unresolvedDates.has(prior.date)&&!day.qualificationReviewDates.includes(prior.date))day.qualificationReviewDates.push(prior.date);
      }
      if(day.thresholdMet&&!prior)qualifiedOrders.set(orderKey,{date:day.date,eligibleAmount:day.eligibleAmount});
    });
    day.qualifies=day.incentiveBaseAmount>0;day.incentive=money(day.incentiveBaseAmount*RATE);day.unresolved=day.unresolved||day.qualificationReviewDates.length>0;
    const approval=(state.approvals || {})[day.key];
    const sameApproval=approval&&money(approval.eligibleAmount)===day.eligibleAmount&&money(approval.incentiveBaseAmount==null?approval.eligibleAmount:approval.incentiveBaseAmount)===day.incentiveBaseAmount&&money(approval.incentive)===day.incentive&&(!approval.qualificationSources||JSON.stringify(approval.qualificationSources)===JSON.stringify(day.qualificationSources));
    day.approval=approval || null;day.approvalStatus=!day.qualifies?'not_eligible':!approval?'pending':sameApproval?'approved':'needs_reapproval';
    day.records.forEach(({share})=>{share.approvalStatus=share.qualifies?day.approvalStatus:'not_eligible';});
  });
  return {records,daily};
}

function ledgerView(state){
  return SALESPERSONS.map(person=>{
    const entries=[];
    Object.values(state.approvals || {}).filter(item=>item.salesperson===person.name).forEach(item=>entries.push({id:item.id,date:item.date,type:'earned',description:'Approved incentive · qualifying receipts ₹'+money(item.incentiveBaseAmount==null?item.eligibleAmount:item.incentiveBaseAmount).toFixed(2)+(item.carriedQualifiedAmount?' · previously qualified orders ₹'+money(item.carriedQualifiedAmount).toFixed(2):''),reference:item.id,credit:money(item.incentive),debit:0,proofs:[],by:item.approvedBy,at:item.approvedAt,qualificationSources:item.qualificationSources||[]}));
    (state.payments || []).filter(item=>item.active!==false&&item.salesperson===person.name).forEach(item=>entries.push({id:item.id,date:item.date,type:'payment',description:'Incentive payment from '+item.account,reference:item.reference||item.id,credit:0,debit:money(item.amount),proofs:item.proofs||[],by:item.createdBy,at:item.createdAt,note:item.note||''}));
    entries.sort((a,b)=>a.date.localeCompare(b.date)||String(a.at||'').localeCompare(String(b.at||''))||a.id.localeCompare(b.id));let balance=0;entries.forEach(entry=>{balance=money(balance+entry.credit-entry.debit);entry.balance=balance;});
    // Preserve any historical entries, but former employees have no current
    // payable and cannot receive a new approval or payment through this book.
    return {salesperson:person.name,recordOnly:!person.incentiveEligible,earned:money(entries.reduce((n,x)=>n+x.credit,0)),paid:money(entries.reduce((n,x)=>n+x.debit,0)),balance,payableBalance:person.incentiveEligible?balance:0,entries};
  });
}

function buildView(orders,state,filters={}){
  const calculated=calculate(orders,state),from=String(filters.from || ''),to=String(filters.to || ''),salesperson=String(filters.salesperson || ''),status=String(filters.status || '');
  let records=calculated.records.filter(row=>(!from||row.receiptDate>=from)&&(!to||row.receiptDate<=to));
  if(salesperson)records=records.filter(row=>row.salespersons.includes(salesperson)||row.salespersonSuggestions.some(item=>item.name===salesperson));
  if(status)records=records.filter(row=>status==='approved'?row.shares.some(share=>share.approvalStatus==='approved'):row.reviewStatus===status);
  const days=calculated.daily.filter(day=>(!from||day.date>=from)&&(!to||day.date<=to)&&(!salesperson||day.salesperson===salesperson));
  const orderIds=new Set(records.map(row=>row.orderId)),uniqueOrders=calculated.records.filter(row=>orderIds.has(row.orderId)).filter((row,index,list)=>list.findIndex(item=>item.orderId===row.orderId)===index),ledgers=ledgerView(state);
  const periodApprovals=Object.values(state.approvals || {}).filter(item=>incentiveEligible(item.salesperson)&&(!from||item.date>=from)&&(!to||item.date<=to)&&(!salesperson||item.salesperson===salesperson));
  const periodPayments=(state.payments || []).filter(item=>item.active!==false&&(!from||item.date>=from)&&(!to||item.date<=to)&&(!salesperson||item.salesperson===salesperson));
  return {
    success:true,configuration:{rate:RATE,dailyThreshold:DAILY_THRESHOLD,salespersons:SALESPERSONS.map(person=>person.name),salespersonDetails:SALESPERSONS.map(person=>({name:person.name,incentiveEligible:person.incentiveEligible,recordOnly:!person.incentiveEligible})),payingAccounts:PAYING_ACCOUNTS},filters:{from,to,salesperson,status},records:records.slice().sort((a,b)=>b.receiptDate.localeCompare(a.receiptDate)||b.orderNumber.localeCompare(a.orderNumber,undefined,{numeric:true})),days:days.slice().sort((a,b)=>b.date.localeCompare(a.date)||a.salesperson.localeCompare(b.salesperson)),ledgers,
    summary:{orders:orderIds.size,totalBilling:money(uniqueOrders.reduce((n,row)=>n+row.billAmount,0)),totalReceived:money(records.reduce((n,row)=>n+row.totalReceived,0)),eligibleReceived:money(records.filter(row=>row.reviewStatus==='ready').reduce((n,row)=>n+row.eligibleAmount,0)),recordOnlyReceipts:money(records.reduce((n,row)=>n+row.recordOnlyAmount,0)),storeCreditExcluded:money(records.reduce((n,row)=>n+row.storeCreditAmount,0)),refundsReturns:money(uniqueOrders.reduce((n,row)=>n+row.refundAmount,0)),incentiveEarned:money(days.reduce((n,day)=>n+day.incentive,0)),approvedIncentive:money(periodApprovals.reduce((n,item)=>n+money(item.incentive),0)),paidInPeriod:money(periodPayments.reduce((n,item)=>n+money(item.amount),0)),outstanding:money(ledgers.reduce((n,ledger)=>n+ledger.payableBalance,0)),needsReview:records.filter(row=>row.reviewStatus==='needs_review').length}
  };
}

function audit(state,req,action,details){ state.audit.push({at:new Date().toISOString(),by:req.user&&req.user.username||'system',action,details}); }
function createRouter(deps={}){
  const router=express.Router(),ordersLoader=deps.loadOrders||loadOrders,stateLoader=deps.loadState||loadState,stateSaver=deps.saveState||saveState;
  router.use('/api/incentives',(req,res,next)=>allowed(req)?next():res.status(403).json({success:false,error:'Salary incentive access is restricted to Accounting, Admin, Owner and Prashant.'}));
  const route=(method,url,handler)=>router[method](url,(req,res,next)=>{try{handler(req,res);}catch(error){if(error.status)return res.status(error.status).json({success:false,error:error.message});next(error);}});
  route('get','/api/incentives',(req,res)=>res.json(buildView(ordersLoader(),stateLoader(),req.query||{})));
  route('post','/api/incentives/reviews/:orderId',(req,res)=>{
    const state=stateLoader(),body=req.body||{},orderId=String(req.params.orderId),orders=ordersLoader(),order=orders.find(item=>String(item.id)===orderId);if(!order){const error=new Error('Shopify order not found.');error.status=404;throw error;}
    const salespersons=Array.isArray(body.salespersons)?Array.from(new Set(body.salespersons.map(String))).filter(name=>SALESPERSONS.some(person=>person.name===name)):[];
    if(body.noSalesperson!==true&&!salespersons.length){const error=new Error('Choose at least one salesperson or confirm that the order has no eligible salesperson.');error.status=400;throw error;}
    const ids=new Set((order.paymentTransactions||[]).map(tx=>String(tx.id))),gatewayModes={};Object.entries(body.gatewayModes||{}).forEach(([id,mode])=>{if(ids.has(String(id))&&['cash','upi_card','store_credit','excluded'].includes(mode))gatewayModes[String(id)]=mode;});
    const before=state.reviews[orderId]||null;state.reviews[orderId]={salespersons,noSalesperson:body.noSalesperson===true,gatewayModes,reason:text(body.reason,500),reviewedAt:new Date().toISOString(),reviewedBy:req.user.username};state.revision++;audit(state,req,'INCENTIVE_ORDER_REVIEWED',{orderId,before,after:state.reviews[orderId]});stateSaver(state);res.json(buildView(orders,state,body.filters||{}));
  });
  route('post','/api/incentives/approve-day',(req,res)=>{
    const body=req.body||{},date=String(body.date||''),salesperson=String(body.salesperson||'');if(!validDate(date)||!SALESPERSONS.some(person=>person.name===salesperson)){const error=new Error('Choose a valid incentive date and salesperson.');error.status=400;throw error;}
    if(!incentiveEligible(salesperson)){const error=new Error(salesperson+' is a former employee — record only. Incentives cannot be approved.');error.status=409;throw error;}
    const state=stateLoader(),orders=ordersLoader(),calculated=calculate(orders,state),day=calculated.daily.find(item=>item.date===date&&item.salesperson===salesperson);if(calculated.records.some(row=>row.receiptDate===date&&row.reviewStatus==='needs_review')){const error=new Error('Review every uncertain POS order on '+date+' before approving incentives.');error.status=409;throw error;}if(!day||!day.qualifies){const error=new Error(salesperson+' has neither reached ₹'+DAILY_THRESHOLD.toLocaleString('en-IN')+' in eligible receipts on '+date+' nor received a balance for an already-qualified order.');error.status=409;throw error;}if(day.unresolved){const error=new Error('Review uncertain POS orders on the qualifying date(s) '+day.qualificationReviewDates.join(', ')+' before approving this later collection.');error.status=409;throw error;}
    const id=date+'|'+salesperson,before=state.approvals[id]||null;state.approvals[id]={id,date,salesperson,eligibleAmount:day.eligibleAmount,incentiveBaseAmount:day.incentiveBaseAmount,carriedQualifiedAmount:day.carriedQualifiedAmount,thresholdMet:day.thresholdMet,qualificationSources:day.qualificationSources,incentive:day.incentive,rate:RATE,threshold:DAILY_THRESHOLD,note:text(body.note,500),approvedAt:new Date().toISOString(),approvedBy:req.user.username};state.revision++;audit(state,req,'INCENTIVE_DAY_APPROVED',{id,before,after:state.approvals[id]});stateSaver(state);res.json(buildView(orders,state,body.filters||{}));
  });
  route('post','/api/incentives/payments',(req,res)=>{
    const state=stateLoader(),body=req.body||{},salesperson=String(body.salesperson||''),amount=money(body.amount),date=String(body.date||''),account=String(body.account||''),proofs=Array.isArray(body.proofs)?body.proofs.map(x=>text(x,500)).filter(Boolean):[];
    const ledger=ledgerView(state).find(item=>item.salesperson===salesperson);if(!ledger){const error=new Error('Choose a valid salesperson.');error.status=400;throw error;}if(ledger.recordOnly){const error=new Error(salesperson+' is a former employee — record only. Incentive payments are not allowed.');error.status=409;throw error;}if(!validDate(date)||!(amount>0)){const error=new Error('Enter a valid payment date and amount.');error.status=400;throw error;}if(!PAYING_ACCOUNTS.includes(account)){const error=new Error('Choose an approved paying account.');error.status=400;throw error;}if(!proofs.length){const error=new Error('Attach at least one payment proof.');error.status=400;throw error;}if(amount>ledger.payableBalance+.005){const error=new Error('Payment exceeds the approved outstanding incentive of ₹'+ledger.payableBalance.toFixed(2)+'.');error.status=409;throw error;}
    state.paymentSeq=Number(state.paymentSeq||0)+1;const payment={id:'INCP-'+String(state.paymentSeq).padStart(5,'0'),salesperson,amount,date,account,reference:text(body.reference,100),proofs,note:text(body.note,500),createdAt:new Date().toISOString(),createdBy:req.user.username,active:true};state.payments.push(payment);state.revision++;audit(state,req,'INCENTIVE_PAYMENT_RECORDED',{payment});stateSaver(state);res.json(buildView(ordersLoader(),state,body.filters||{}));
  });
  return router;
}

const router=createRouter();
module.exports={router,createRouter,parseSalespeople,classifyGateway,buildRecords,calculate,buildView,ledgerView,constants:{RATE,DAILY_THRESHOLD,SALESPERSONS,PAYING_ACCOUNTS}};
