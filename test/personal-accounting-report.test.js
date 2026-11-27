'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path');
const report=require('../modules/personal-accounting-report');

function fixture(){
  const expense=(id,date,amount,account,ledger='Food')=>({id,date,nature:'PERSONAL',status:'paid',approvedAt:date,ledger,amount,particulars:id,payments:[{id:'PAY-001',date,amount,account}]});
  return{
    receipts:[
      {id:'REC-00200',date:'2026-10-15',nature:'PERSONAL',account:'ICICI Bank 0993',amount:1000000,source:'Amit Buch',receiptType:'loan_received'},
      {id:'REC-00201',date:'2026-10-15',nature:'PERSONAL',account:'ICICI Bank 0993',amount:1000000,source:'15% loan',receiptType:'loan_received'},
      {id:'REC-SAMAST',date:'2026-10-14',nature:'SAMAST',account:'IndusInd Bank 7883',amount:1000,source:'Tenant',receiptType:'rental_income'},
      {id:'REC-HOTEL',date:'2026-10-14',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',account:'ICICI Bank 0992',amount:2000,source:'Guest',receiptType:'hotel_income'}
    ],loanRepayments:[],expenseRefunds:[],
    transfers:[
      {id:'TR-BUSINESS',date:'2026-10-16',fromNature:'PERSONAL',toNature:'SANKI',fromAccount:'ICICI Bank 0993',toAccount:'Axis Bank 3448',classification:'owner_contribution',amount:500000},
      {id:'TR-MOVE',date:'2026-10-18',fromNature:'PERSONAL',toNature:'PERSONAL',fromAccount:'ICICI Bank 0993',toAccount:'IndusInd Bank 7883',classification:'internal_transfer',amount:200000},
      {id:'TR-TAKEN',date:'2026-10-20',fromNature:'SANKI',toNature:'PERSONAL',fromAccount:'Axis Bank 3448',toAccount:'ICICI Bank 0993',classification:'owner_withdrawal',amount:100000}
    ],
    expenses:{
      P1:expense('P1','2026-10-17',50000,'ICICI Bank 0993'),
      P2:expense('P2','2026-10-19',100000,'IndusInd Bank 7883'),
      P3:expense('P3','2026-10-20',10000,'ICICI Bank 0992','Food & Drinks'),
      B1:{id:'B1',date:'2026-10-10',nature:'SANKI',status:'paid',approvedAt:'2026-10-10',ledger:'Subscriptions',particulars:'Owner paid',claimant:'owner',paidAlready:true,personalPaidAmount:5000,reimbursementAmount:2000,reimbursementPayments:[{id:'R1',date:'2026-10-12',amount:2000}],payments:[]}
    },
    personalReporting:{budgets:{'2026-10':{Food:160000}},movementSources:{'P1/PAY-001':{source:'LOAN:REC-00200'},'P2/PAY-001':{source:'LOAN:REC-00200'}},loanOwners:{},loanAllocations:{},transferTreatments:{}}
  };
}
const spending=()=>[
  {id:'P1/PAY-001',date:'2026-10-17',kind:'expense',account:'ICICI Bank 0993',categoryGroup:'Living & Lifestyle',category:'Food',description:'P1',amount:50000},
  {id:'P2/PAY-001',date:'2026-10-19',kind:'expense',account:'IndusInd Bank 7883',categoryGroup:'Living & Lifestyle',category:'Food',description:'P2',amount:100000},
  {id:'P3/PAY-001',date:'2026-10-20',kind:'expense',account:'ICICI Bank 0992',categoryGroup:'Living & Lifestyle',category:'Food & Drinks',description:'P3',amount:10000}
];
const transfers=s=>s.transfers.map(row=>({id:row.id,date:row.date,from:row.fromAccount,to:row.toAccount,fromEntity:row.fromNature,toEntity:row.toNature,classification:row.classification,amount:row.amount,note:row.note||''}));

test('SANKI loans use actual transfers from the receiving account',()=>{
  const store=fixture(),result=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store),loanAccountChecks:{'ICICI Bank 0993':{matched:true,reconciledThrough:'2026-10-31'}}});
  assert.deepEqual(result.loanUsage.map(row=>row.owner),['SANKI','SANKI']);
  assert.deepEqual(result.loanUsage.map(row=>[row.id,row.businessUse,row.personalPortion,row.allocationStatus]),[
    ['REC-00200',500000,500000,'Account matched'],['REC-00201',0,1000000,'Account matched']
  ]);
  assert.deepEqual(result.loanUsage[0].events.map(row=>[row.id,row.amount,row.use]),[
    ['TR-BUSINESS',500000,'Allocated to SANKI'],['REC-00200/PERSONAL',500000,'Personal portion']
  ]);
  assert.deepEqual(result.loanPools.map(row=>[row.owner,row.account,row.borrowed,row.businessUse,row.personalPortion,row.loanSpecificAllocationProven]),[['SANKI','ICICI Bank 0993',2000000,500000,1500000,false]]);
  assert.equal(result.spending.total,160000,'spending is counted once, not again as loan use');
  assert.equal(result.spending.budgets.find(row=>row.category==='Food').group,'Living & Lifestyle');
  assert.deepEqual(result.spending.budgets.map(row=>row.category),['Food','Food & Drinks'],'categories are highest-spending first inside the group');
  assert.deepEqual(result.spending.entries.map(row=>row.id),['P3/PAY-001','P2/PAY-001','P1/PAY-001'],'individual entries are newest-date first');
  assert.equal(result.settlements.youOweBusinessTotal,1500000);
  assert.deepEqual(result.settlements.youOweBusiness,[{entity:'SANKI',reason:'Business loan personal portion',amount:1500000}]);
});

test('owner withdrawal label alone is not proof that a transfer is permanent',()=>{
  const store=fixture(),result=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store)});
  assert.equal(result.moneyTaken.receivedTotal,100000,'the actual bank movement is shown even while its purpose is pending');
  assert.equal(result.moneyTaken.permanentTotal,0);
  assert.equal(result.moneyTaken.sourceReceiptTotal,3000);assert.deepEqual(result.moneyTaken.sourceReceiptsBySource,{SAMAST:1000,SEWAK_PARK_HOTEL:2000});
  assert.deepEqual(result.review.transferRows.map(row=>row.id),['TR-TAKEN']);
  assert.equal(result.settlements.businessOwesYouTotal,3000);
  assert.equal(result.spending.budgets.find(row=>row.category==='Food').remaining,10000);
  assert.deepEqual(result.review.rows,[],'personal expenses do not need a second funding-source classification');
  store.personalReporting.transferTreatments['TR-TAKEN']={treatment:'permanent_withdrawal',reason:'Owner confirmed permanent'};
  const confirmed=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store)});
  assert.equal(confirmed.moneyTaken.receivedTotal,100000);assert.equal(confirmed.moneyTaken.receivedBySource.SANKI,100000);assert.equal(confirmed.moneyTaken.permanentTotal,100000);assert.equal(confirmed.moneyTaken.personalUseTotal,0);assert.deepEqual(confirmed.review.transferRows,[]);
});

test('source receipts include only finalized operating income inside the selected dates',()=>{
  const store=fixture();
  store.bankReconciliationDrafts={OPEN:{id:'OPEN'}};
  store.receipts.push(
    {id:'SAMAST-FROM',date:'2026-10-01',nature:'SAMAST',receiptType:'rental_income',amount:.10},
    {id:'SAMAST-TO',date:'2026-10-31',nature:'SAMAST',receiptType:'rental_income',amount:.20},
    {id:'SAMAST-BEFORE',date:'2026-09-30',nature:'SAMAST',receiptType:'rental_income',amount:900},
    {id:'SAMAST-AFTER',date:'2026-11-01',nature:'SAMAST',receiptType:'rental_income',amount:900},
    {id:'SAMAST-PENDING',date:'2026-10-10',nature:'SAMAST',receiptType:'rental_income',amount:500,bankReconciliationDraft:'OPEN'},
    {id:'SAMAST-SECURITY',date:'2026-10-10',nature:'SAMAST',receiptType:'security_deposit',amount:500,balanceSheetOnly:true},
    {id:'HOTEL-TRANSFER',date:'2026-10-10',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',receiptType:'internal_transfer',amount:500},
    {id:'HOTEL-REFUND',date:'2026-10-10',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',receiptType:'hotel_income',expenseRefundId:'EX-1',amount:500},
    {id:'HOTEL-ZERO',date:'2026-10-10',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',receiptType:'hotel_income',amount:0}
  );
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[]});
  assert.equal(result.moneyTaken.sourceReceiptTotal,3000.3);
  assert.deepEqual(result.moneyTaken.sourceReceiptsBySource,{SAMAST:1000.3,SEWAK_PARK_HOTEL:2000});
  assert.ok(!result.moneyTaken.sourceReceiptRows.some(row=>/PENDING|SECURITY|TRANSFER|REFUND|BEFORE|AFTER|ZERO/.test(row.id)));
});

test('duplicate or invalid source receipt evidence blocks the total instead of guessing',()=>{
  const duplicate=fixture();duplicate.receipts.push({...duplicate.receipts.find(row=>row.id==='REC-SAMAST')});
  assert.throws(()=>report.build(duplicate,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[]}),/Duplicate source receipt REC-SAMAST/);
  const invalid=fixture();invalid.receipts.find(row=>row.id==='REC-SAMAST').amount=-1;
  assert.throws(()=>report.build(invalid,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[]}),/Invalid source receipt amount/);
});

test('Sewak Park operating costs stay out of Personal spending while personal use remains once',()=>{
  const store=fixture(),hotelExpense={id:'HOTEL-OP',date:'2026-10-22',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',status:'paid',ledger:'Hotel supplies',amount:2500,payments:[{id:'PAY-001',date:'2026-10-22',amount:2500,account:'ICICI Bank 0992'}]},personalUse={id:'HOTEL-PERSONAL',date:'2026-10-23',nature:'PERSONAL',reportingTreatment:'personal_use',status:'paid',ledger:'Food',amount:600,payments:[{id:'PAY-001',date:'2026-10-23',amount:600,account:'ICICI Bank 0992'}]};
  store.expenses[hotelExpense.id]=hotelExpense;store.expenses[personalUse.id]=personalUse;
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[
    {id:'HOTEL-OP/PAY-001',date:'2026-10-22',kind:'expense',amount:2500,category:'Hotel supplies',appExpense:hotelExpense},
    {id:'HOTEL-PERSONAL/PAY-001',date:'2026-10-23',kind:'expense',amount:600,category:'Food',appExpense:personalUse}
  ],transfers:[]});
  assert.equal(result.spending.total,600);
  assert.deepEqual(result.spending.entries.map(row=>row.id),['HOTEL-PERSONAL/PAY-001']);
  assert.equal(result.moneyTaken.personalUseBySource.SEWAK_PARK_HOTEL,600);
});

test('a reconciled shared-account deficit is the SAMAST amount outstanding, while an unreconciled equation stays unassumed',()=>{
  const store=fixture(),position={account:'IndusInd Bank 7883',proven:true,personalDeficitClosing:275000,periodIncrease:25000,periodRestored:0,basis:'Entity sub-ledgers reconcile'};
  const proven=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:spending(),transfers:[],sharedAccountPositions:{'IndusInd Bank 7883':position}});
  assert.equal(proven.moneyTaken.accountProvenUseBySource.SAMAST,25000);assert.equal(proven.moneyTaken.accountProvenUseTotal,25000);
  assert.ok(proven.settlements.youOweBusiness.some(row=>row.entity==='SAMAST'&&row.amount===275000&&/shared account/.test(row.reason)));
  const pending=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:spending(),transfers:[],sharedAccountPositions:{'IndusInd Bank 7883':{...position,proven:false,blockers:['₹1 difference']}}});
  assert.deepEqual(pending.moneyTaken.accountProvenUseBySource,{});assert.equal(pending.moneyTaken.pendingAccountPositions.length,1);assert.ok(!pending.settlements.youOweBusiness.some(row=>row.entity==='SAMAST'));
});

test('a posted Personal expense paid from an exclusively SANKI account is objective SANKI use',()=>{
  const store=fixture(),expense={id:'SANKI-PAID-PERSONAL',date:'2026-10-24',nature:'PERSONAL',status:'paid',ledger:'Personal food',amount:750,payments:[{id:'PAY-001',date:'2026-10-24',amount:750,account:'Axis Bank 3448'}]};store.expenses[expense.id]=expense;
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[{id:'SANKI-PAID-PERSONAL/PAY-001',date:'2026-10-24',kind:'expense',amount:750,account:'Axis Bank 3448',category:'Personal food',appExpense:expense}],transfers:[]});
  assert.equal(result.moneyTaken.personalUseBySource.SANKI,750);assert.equal(result.moneyTaken.unlinkedTotal,0);
});

test('a payment still inside reconciliation cannot prove its business funding source',()=>{
  const store=fixture(),expense={id:'PENDING-SANKI-PAID',date:'2026-10-24',nature:'PERSONAL',status:'paid',ledger:'Personal food',amount:750,payments:[{id:'PAY-001',date:'2026-10-24',amount:750,account:'Axis Bank 3448',bankReconciliationDraft:'OPEN'}]};store.expenses[expense.id]=expense;store.bankReconciliationDrafts={OPEN:{id:'OPEN'}};
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[{id:'PENDING-SANKI-PAID/PAY-001',date:'2026-10-24',kind:'expense',amount:750,account:'Axis Bank 3448',category:'Personal food',appExpense:expense}],transfers:[]});
  assert.equal(result.moneyTaken.personalUseBySource.SANKI,undefined);assert.equal(result.moneyTaken.personalUseTotal,0);
});

test('configured account ownership automatically covers future SANKI accounts and never treats a shared account as exclusive',()=>{
  const store=fixture(),newAccount={id:'NEW-ACCOUNT',date:'2026-10-24',nature:'PERSONAL',status:'paid',ledger:'Personal',amount:900,payments:[{id:'PAY-001',date:'2026-10-24',amount:900,account:'New Business Bank 9999'}]},shared={id:'SHARED-ACCOUNT',date:'2026-10-24',nature:'PERSONAL',status:'paid',ledger:'Personal',amount:100,payments:[{id:'PAY-001',date:'2026-10-24',amount:100,account:'IndusInd Bank 7883'}]};store.expenses[newAccount.id]=newAccount;store.expenses[shared.id]=shared;
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[
    {id:'NEW-ACCOUNT/PAY-001',date:'2026-10-24',kind:'expense',amount:900,account:'New Business Bank 9999',appExpense:newAccount},
    {id:'SHARED-ACCOUNT/PAY-001',date:'2026-10-24',kind:'expense',amount:100,account:'IndusInd Bank 7883',appExpense:shared}
  ],transfers:[],accountOwners:{'New Business Bank 9999':'SANKI','IndusInd Bank 7883':'SHARED'}});
  assert.equal(result.moneyTaken.personalUseBySource.SANKI,900);assert.deepEqual(result.moneyTaken.unlinkedRows,[],'a shared-account debit is resolved only by the shared-account equation, never a manual source link');
});

test('legacy verbal funding-source labels are ignored and do not change bank-backed totals',()=>{
  const store=fixture(),before=JSON.stringify({expenses:store.expenses,transfers:store.transfers,receipts:store.receipts});
  store.personalReporting.movementSources['P3/PAY-001']={source:'SEWAK_PARK_HOTEL',reason:'Owner confirmed'};
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store)});
  assert.equal(result.review.rows.length,0);
  assert.equal(result.moneyTaken.receivedTotal,100000);
  assert.equal(result.moneyTaken.personalUseBySource.SEWAK_PARK_HOTEL,undefined);
  assert.deepEqual(result.moneyTaken.personalUseRows,[]);
  assert.deepEqual(result.moneyTaken.unlinkedRows.map(row=>row.id),['P3/PAY-001']);
  assert.deepEqual(result.loanUsage.map(row=>row.businessUse),[500000,0]);
  assert.equal(JSON.stringify({expenses:store.expenses,transfers:store.transfers,receipts:store.receipts}),before);
});

test('unfinished reconciliation loans and transfers never enter personal totals',()=>{
  const store=fixture();store.bankReconciliationDrafts={OPEN:{id:'OPEN'}};
  store.receipts[0].bankReconciliationDraft='OPEN';store.transfers[0].bankReconciliationDraft='OPEN';
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[]});
  assert.deepEqual(result.loanUsage.map(row=>row.id),['REC-00201']);
  assert.equal(result.loanUsage[0].businessUse,0);
});

test('an unverified loan receiving account stays visible but creates no amount to settle',()=>{
  const store=fixture(),result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:transfers(store),loanAccountChecks:{'ICICI Bank 0993':{matched:false,reconciledThrough:'2026-10-24',warnings:['Bank check pending']}}});
  assert.equal(result.loanPools[0].personalPortion,1500000);assert.equal(result.loanPools[0].calculationProven,false);
  assert.equal(result.settlements.youOweBusinessTotal,0);assert.equal(result.review.loanRows.length,1);assert.equal(result.integrity.status,'needs_review');
});

test('a future Personal-account loan uses its saved borrower entity instead of a hard-coded receipt id',()=>{
  const store={receipts:[{id:'REC-FUTURE',date:'2026-10-01',nature:'PERSONAL',account:'ICICI Bank 0993',amount:100000,source:'Future lender',receiptType:'loan_received',loanTerms:{borrowerNature:'SAMAST'}}],loanRepayments:[],expenses:{},transfers:[{id:'TR-FUTURE',date:'2026-10-02',fromNature:'PERSONAL',toNature:'SAMAST',fromAccount:'ICICI Bank 0993',toAccount:'IndusInd Bank 7883',amount:40000}],personalReporting:{}};
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[],loanAccountChecks:{'ICICI Bank 0993':{matched:true,reconciledThrough:'2026-10-31'}}});
  assert.equal(result.loanPools[0].owner,'SAMAST');assert.equal(result.loanPools[0].businessUse,40000);assert.equal(result.loanPools[0].personalPortion,60000);assert.deepEqual(result.settlements.youOweBusiness,[{entity:'SAMAST',reason:'Business loan personal portion',amount:60000}]);
});

test('business-paid principal never reduces the business-loan amount still with Personal',()=>{
  const store={receipts:[{id:'REC-BUSINESS-LOAN',date:'2026-10-01',nature:'PERSONAL',account:'ICICI Bank 0993',amount:100000,source:'Lender',receiptType:'loan_received',loanTerms:{borrowerNature:'SANKI'}}],loanRepayments:[{id:'LOANPAY-BUSINESS',loanId:'REC-BUSINESS-LOAN',date:'2026-10-10',nature:'PERSONAL',account:'Axis Bank 3448',amount:50000,principalAmount:50000}],expenses:{},transfers:[{id:'TR-LOAN-BUSINESS',date:'2026-10-02',fromNature:'PERSONAL',toNature:'SANKI',fromAccount:'ICICI Bank 0993',toAccount:'Axis Bank 3448',amount:60000}],personalReporting:{}};
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[],loanAccountChecks:{'ICICI Bank 0993':{matched:true,reconciledThrough:'2026-10-31'}},accountOwners:{'ICICI Bank 0993':'PERSONAL','Axis Bank 3448':'SANKI'}}),pool=result.loanPools[0];
  assert.equal(pool.repaid,50000);assert.equal(pool.businessPrincipalRepaid,50000);assert.equal(pool.personalPrincipalRepaid,0);assert.equal(pool.businessUse,60000);assert.equal(pool.personalPortion,40000);assert.equal(result.settlements.youOweBusinessTotal,40000);
});

test('Personal-paid principal reduces the Personal portion and any excess becomes business money owed back',()=>{
  const store={receipts:[{id:'REC-BUSINESS-LOAN',date:'2026-10-01',nature:'PERSONAL',account:'ICICI Bank 0993',amount:100000,source:'Lender',receiptType:'loan_received',loanTerms:{borrowerNature:'SANKI'}}],loanRepayments:[{id:'LOANPAY-PERSONAL',loanId:'REC-BUSINESS-LOAN',date:'2026-10-10',nature:'PERSONAL',account:'ICICI Bank 0993',amount:50000,principalAmount:50000}],expenses:{},transfers:[{id:'TR-LOAN-BUSINESS',date:'2026-10-02',fromNature:'PERSONAL',toNature:'SANKI',fromAccount:'ICICI Bank 0993',toAccount:'Axis Bank 3448',amount:60000}],personalReporting:{}};
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[],loanAccountChecks:{'ICICI Bank 0993':{matched:true,reconciledThrough:'2026-10-31'}},accountOwners:{'ICICI Bank 0993':'PERSONAL','Axis Bank 3448':'SANKI'}}),pool=result.loanPools[0];
  assert.equal(pool.personalPrincipalApplied,40000);assert.equal(pool.personalPaidForBusiness,10000);assert.equal(pool.personalPortion,0);assert.equal(result.settlements.youOweBusinessTotal,0);assert.equal(result.settlements.businessOwesYouTotal,10000);
});

test('loan allocation equations stay balanced across transfer and repayment combinations',()=>{
  for(const businessUse of [0,25000,60000,100000])for(const personalPaid of [0,10000,40000,100000])for(const businessPaid of [0,15000]){
    if(personalPaid+businessPaid>100000)continue;
    const repayments=[];if(personalPaid)repayments.push({id:'PAY-PERSONAL',loanId:'LOAN',date:'2026-10-03',nature:'PERSONAL',account:'ICICI Bank 0993',amount:personalPaid,principalAmount:personalPaid});if(businessPaid)repayments.push({id:'PAY-BUSINESS',loanId:'LOAN',date:'2026-10-04',nature:'SANKI',account:'Axis Bank 3448',amount:businessPaid,principalAmount:businessPaid});
    const transferRows=businessUse?[{id:'TO-BUSINESS',date:'2026-10-02',fromNature:'PERSONAL',toNature:'SANKI',fromAccount:'ICICI Bank 0993',toAccount:'Axis Bank 3448',amount:businessUse}]:[];
    const store={receipts:[{id:'LOAN',date:'2026-10-01',nature:'PERSONAL',account:'ICICI Bank 0993',amount:100000,source:'Lender',receiptType:'loan_received',loanTerms:{borrowerNature:'SANKI'}}],loanRepayments:repayments,expenses:{},transfers:transferRows,personalReporting:{}};
    const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[],loanAccountChecks:{'ICICI Bank 0993':{matched:true,reconciledThrough:'2026-10-31'}},accountOwners:{'ICICI Bank 0993':'PERSONAL','Axis Bank 3448':'SANKI'}}),pool=result.loanPools[0];
    assert.equal(pool.borrowed,pool.businessUse+pool.personalPrincipalApplied+pool.personalPortion,`borrowed split ${businessUse}/${personalPaid}/${businessPaid}`);
    assert.equal(pool.personalPrincipalRepaid,pool.personalPrincipalApplied+pool.personalPaidForBusiness,`personal repayment split ${businessUse}/${personalPaid}/${businessPaid}`);
    assert.ok(pool.personalPortion>=0);assert.ok(pool.personalPaidForBusiness>=0);
    assert.equal(result.settlements.youOweBusinessTotal,pool.personalPortion);
    assert.equal(result.settlements.businessOwesYouTotal,pool.personalPaidForBusiness);
  }
});

test('an unresolved principal-paying account blocks the loan settlement instead of guessing',()=>{
  const store={receipts:[{id:'REC-BUSINESS-LOAN',date:'2026-10-01',nature:'PERSONAL',account:'ICICI Bank 0993',amount:100000,source:'Lender',receiptType:'loan_received',loanTerms:{borrowerNature:'SANKI'}}],loanRepayments:[{id:'LOANPAY-SHARED',loanId:'REC-BUSINESS-LOAN',date:'2026-10-10',nature:'PERSONAL',account:'IndusInd Bank 7883',amount:10000,principalAmount:10000}],expenses:{},transfers:[],personalReporting:{}};
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[],loanAccountChecks:{'ICICI Bank 0993':{matched:true,reconciledThrough:'2026-10-31'}},accountOwners:{'ICICI Bank 0993':'PERSONAL','IndusInd Bank 7883':'SHARED'}}),pool=result.loanPools[0];
  assert.equal(pool.unresolvedPrincipalRepaid,10000);assert.equal(pool.calculationProven,false);assert.equal(result.settlements.youOweBusinessTotal,0);assert.match(pool.blockers.join(' '),/owner is not proven/);
});

test('a business loan received directly in a business bank never enters Personal loan use or settlement',()=>{
  const store={receipts:[{id:'REC-DIRECT-SANKI',date:'2026-10-01',nature:'SANKI',account:'Axis Bank 3448',amount:1000000,source:'Business lender',receiptType:'loan_received',loanTerms:{borrowerNature:'SANKI'}}],loanRepayments:[],expenses:{},transfers:[],personalReporting:{}};
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[],accountOwners:{'Axis Bank 3448':'SANKI'}});
  assert.deepEqual(result.loanPools,[]);assert.equal(result.moneyTaken.loanPersonalTotal,0);assert.equal(result.settlements.youOweBusinessTotal,0);
});

test('an invalid borrower on the loan record blocks the Personal settlement instead of inventing an owner',()=>{
  const store={receipts:[{id:'REC-INVALID-OWNER',date:'2026-10-01',nature:'PERSONAL',account:'ICICI Bank 0993',amount:100000,source:'Lender',receiptType:'loan_received',loanTerms:{borrowerNature:'UNKNOWN'}}],loanRepayments:[],expenses:{},transfers:[],personalReporting:{loanOwners:{'REC-INVALID-OWNER':'SANKI'}}};
  assert.throws(()=>report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[]}),/no valid borrower entity/);
});

test('a stale Personal-report loan-owner override cannot replace the borrower stored on the loan',()=>{
  const store={receipts:[{id:'REC-RECORDED-OWNER',date:'2026-10-01',nature:'PERSONAL',account:'ICICI Bank 0993',amount:100000,source:'Lender',receiptType:'loan_received',loanTerms:{borrowerNature:'SANKI'}}],loanRepayments:[],expenses:{},transfers:[],personalReporting:{loanOwners:{'REC-RECORDED-OWNER':'SAMAST'}}};
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:[],loanAccountChecks:{'ICICI Bank 0993':{matched:true,reconciledThrough:'2026-10-31'}},accountOwners:{'ICICI Bank 0993':'PERSONAL'}});
  assert.equal(result.loanPools[0].owner,'SANKI');assert.equal(result.settlements.youOweBusiness[0].entity,'SANKI');
});

test('rejected, cancelled and unfinished movements are never treated as accounting evidence',()=>{
  const store=fixture();store.bankReconciliationDrafts={OPEN:{id:'OPEN'}};store.receipts.push({id:'REJECTED-RENT',date:'2026-10-20',nature:'SAMAST',receiptType:'rental_income',amount:900,status:'rejected'});
  const raw=transfers(store).concat([{id:'CANCELLED-IN',date:'2026-10-20',fromEntity:'SANKI',toEntity:'PERSONAL',amount:900,status:'cancelled'},{id:'PENDING-IN',date:'2026-10-20',fromEntity:'SANKI',toEntity:'PERSONAL',amount:900,bankReconciliationDraft:'OPEN'}]),result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers:raw,allTransfers:raw});
  assert.equal(result.moneyTaken.sourceReceiptTotal,3000);assert.ok(!result.moneyTaken.receivedRows.some(row=>['CANCELLED-IN','PENDING-IN'].includes(row.id)));
});

test('a repayment-like note alone cannot settle money without a saved accounting treatment',()=>{
  const store=fixture();store.transfers.push({id:'TR-REPAID',date:'2026-10-25',fromNature:'SANKI',toNature:'PERSONAL',fromAccount:'Axis Bank 3448',toAccount:'ICICI Bank 0993',classification:'owner_withdrawal',amount:25000,note:'Payng Back amount used for business'});
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store)});
  assert.ok(!result.settlements.recordedSettlements.some(row=>row.id==='TR-REPAID'));
  assert.ok(result.review.transferRows.some(row=>row.id==='TR-REPAID'));
  store.personalReporting.transferTreatments['TR-REPAID']={treatment:'business_repayment',reason:'Matched to the earlier temporary transfer'};
  const confirmed=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store)});
  assert.ok(confirmed.settlements.recordedSettlements.some(row=>row.id==='TR-REPAID'&&row.direction==='Business repaid you'));
});

test('a corrupted treatment saved for the wrong transfer direction is reviewed, never applied',()=>{
  const store={receipts:[],loanRepayments:[],expenses:{},personalReporting:{transferTreatments:{IN:{treatment:'personal_repayment',reason:'Invalid direction'}}}},transfers=[{id:'IN',date:'2026-10-01',fromEntity:'SANKI',toEntity:'PERSONAL',from:'3448',to:'0993',amount:100}];
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers});
  assert.equal(result.settlements.youOweBusinessTotal,0);assert.deepEqual(result.review.transferRows.map(row=>row.id),['IN']);
});

test('temporary transfers and repayments settle chronologically without creating negative balances',()=>{
  const store={receipts:[],loanRepayments:[],expenses:{},transfers:[],personalReporting:{budgets:{},movementSources:{},loanOwners:{},transferTreatments:{
    IN:{treatment:'temporary_due_to_business',reason:'Temporary SANKI money'},PAY1:{treatment:'personal_repayment',reason:'Part repayment'},PAY2:{treatment:'personal_repayment',reason:'Excess repayment'}
  }}},transfers=[
    {id:'PAY2',date:'2026-10-03',fromEntity:'PERSONAL',toEntity:'SANKI',from:'0993',to:'3448',amount:80},
    {id:'IN',date:'2026-10-01',fromEntity:'SANKI',toEntity:'PERSONAL',from:'3448',to:'0993',amount:100},
    {id:'PAY1',date:'2026-10-02',fromEntity:'PERSONAL',toEntity:'SANKI',from:'0993',to:'3448',amount:40}
  ];
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers});
  assert.equal(result.settlements.youOweBusinessTotal,0);assert.deepEqual(result.settlements.recordedSettlements.map(row=>[row.id,row.applied,row.unapplied]),[['PAY1',40,0],['PAY2',60,20]]);assert.equal(result.settlements.unappliedSettlementTotal,20);
});

test('a repayment dated before a temporary advance remains unmatched and does not settle future money',()=>{
  const store={receipts:[],loanRepayments:[],expenses:{},personalReporting:{transferTreatments:{EARLY:{treatment:'personal_repayment',reason:'Earlier payment'},LATER:{treatment:'temporary_due_to_business',reason:'Later advance'}}}},transfers=[
    {id:'LATER',date:'2026-10-05',fromEntity:'SANKI',toEntity:'PERSONAL',from:'3448',to:'0993',amount:100},
    {id:'EARLY',date:'2026-10-01',fromEntity:'PERSONAL',toEntity:'SANKI',from:'0993',to:'3448',amount:80}
  ];
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',spendingRows:[],transfers});
  assert.equal(result.settlements.youOweBusinessTotal,100);assert.equal(result.settlements.recordedSettlements[0].applied,0);assert.equal(result.settlements.recordedSettlements[0].unapplied,80);
});

test('money to settle carries earlier transfers forward while selected-period movement stays period-only',()=>{
  const store={receipts:[],loanRepayments:[],expenses:{},personalReporting:{transferTreatments:{OLD:{treatment:'temporary_due_to_business',reason:'October advance'},NOV:{treatment:'permanent_withdrawal',reason:'November withdrawal'}}}},old={id:'OLD',date:'2026-10-20',fromEntity:'SANKI',toEntity:'PERSONAL',from:'3448',to:'0993',amount:100},nov={id:'NOV',date:'2026-11-05',fromEntity:'SANKI',toEntity:'PERSONAL',from:'3448',to:'0993',amount:50};
  const result=report.build(store,{from:'2026-11-01',to:'2026-11-30',spendingRows:[],transfers:[nov],allTransfers:[old,nov]});
  assert.equal(result.moneyTaken.receivedTotal,50);assert.deepEqual(result.moneyTaken.receivedRows.map(row=>row.id),['NOV']);assert.equal(result.settlements.youOweBusinessTotal,100);
});

test('a mid-month report keeps selected-period spending separate from full month-to-date budget use',()=>{
  const store={receipts:[],loanRepayments:[],expenses:{},personalReporting:{budgets:{'2026-11':{Food:350}}}},selected=[{id:'SELECTED',date:'2026-11-20',kind:'expense',category:'Food',amount:100}],month=[{id:'EARLIER',date:'2026-11-05',kind:'expense',category:'Food',amount:300},...selected];
  const result=report.build(store,{from:'2026-11-15',to:'2026-11-30',spendingRows:selected,monthSpendingRows:month,transfers:[]}),food=result.spending.budgets.find(row=>row.category==='Food');
  assert.equal(result.spending.total,100);assert.equal(food.periodActual,100);assert.equal(food.actual,400);assert.equal(food.remaining,-50);assert.equal(food.status,'Over budget');assert.equal(result.spending.budgetFrom,'2026-11-01');
});

test('Personal report UI keeps the agreed simple labels and inline corrections',()=>{
  const read=file=>fs.readFileSync(path.join(__dirname,'..',file),'utf8'),html=read('public/compact-reports.html'),js=read('public/compact-reports.js'),css=read('public/compact-reports.css');
  for(const label of ['Spending &amp; Budget','Money Taken from Entities','Money to Settle','Loans and Their Use','Personal Accounts and Transfers','Review Decisions','Business has to return to you','You have to return to business'])assert.match(html,new RegExp(label.replace(/[&]/g,'&')));
  assert.match(js,/Personal Overview/);assert.match(js,/personal-settle/);assert.match(js,/personal-accounts/);
  assert.match(js,/personal-report\/budget/);assert.doesNotMatch(js,/personal-report\/source/);assert.doesNotMatch(js,/personal-report\/loan-allocation/);assert.match(js,/personal-report\/transfer-treatment/);assert.match(js,/Personal portion/);assert.match(js,/Sent to business/);assert.match(js,/Principal repaid \(who paid\)/);
  assert.doesNotMatch(js,/\['Business-loan personal portion'/,'the loan personal portion must not be shown twice in the overview');assert.match(js,/\['You have to return to business'.*View breakdown/);
  assert.match(js,/\['Expense group','Selected-period spending','This month'/);assert.match(js,/\['Category','Selected-period spending','This month'/);assert.match(js,/showGroup:true/);
  assert.match(html,/Spending by group and category/);assert.match(html,/Individual spending entries/);
  assert.match(js,/newest date first/);assert.match(js,/renderPersonalBudgetGroups/);assert.match(js,/View categories ↓/);assert.match(js,/personal-group-categories/);assert.match(js,/personal-category-expenses/);assert.match(js,/View expenses ↓/);assert.doesNotMatch(js,/function personalSpendingDetails/);
  assert.match(js,/Bill \/ invoice and saved notes/);assert.match(js,/Expense-level payment proofs/);assert.match(js,/Bank \/ payment reference/);assert.match(css,/\.app-entry-table td:last-child/);assert.match(css,/white-space:nowrap/);
  assert.match(html,/Entity receipts, movement and personal use/);assert.match(html,/Moved to Personal/);assert.match(html,/No verbal funding-source choice/);assert.match(html,/sub-ledgers reconcile exactly/);assert.match(js,/Actual source receipts/);assert.match(js,/Needs account evidence/);assert.match(js,/accountProvenUseBySource/);assert.match(js,/Shared-account equation/);assert.match(js,/for\(const sourceMap of/);assert.match(js,/renderPersonalMoneyTaken/);for(const source of ['SANKI','SAMAST','Sewak Park'])assert.match(js,new RegExp(source));assert.match(js,/personal-money-source-detail-row/);assert.match(js,/Business money used personally/);
  assert.match(html,/Sent to business is calculated only from actual transfers/);assert.match(html,/pooled so the total is correct/);assert.match(html,/earlier temporary balance/);assert.doesNotMatch(html,/Oldest available loan money|Unallocated/);
  assert.match(css,/--ivory:#faf7ee/);assert.match(css,/\.personal-source-edit/);
});
