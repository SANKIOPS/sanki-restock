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
      {id:'REC-00201',date:'2026-10-15',nature:'PERSONAL',account:'ICICI Bank 0993',amount:1000000,source:'15% loan',receiptType:'loan_received'}
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
  {id:'P1/PAY-001',date:'2026-10-17',kind:'expense',account:'ICICI Bank 0993',category:'Food',description:'P1',amount:50000},
  {id:'P2/PAY-001',date:'2026-10-19',kind:'expense',account:'IndusInd Bank 7883',category:'Food',description:'P2',amount:100000},
  {id:'P3/PAY-001',date:'2026-10-20',kind:'expense',account:'ICICI Bank 0992',category:'Food & Drinks',description:'P3',amount:10000}
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
  assert.equal(result.spending.total,160000,'spending is counted once, not again as loan use');
  assert.equal(result.settlements.youOweBusinessTotal,1500000);
  assert.deepEqual(result.settlements.youOweBusiness,[{entity:'SANKI',reason:'Business loan personal portion',amount:1500000}]);
});

test('owner withdrawal label alone is not proof that a transfer is permanent',()=>{
  const store=fixture(),result=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store)});
  assert.equal(result.moneyTaken.permanentTotal,0);
  assert.deepEqual(result.review.transferRows.map(row=>row.id),['TR-TAKEN']);
  assert.equal(result.settlements.businessOwesYouTotal,3000);
  assert.equal(result.spending.budgets.find(row=>row.category==='Food').remaining,10000);
  assert.deepEqual(result.review.rows,[],'personal expenses do not need a second funding-source classification');
  store.personalReporting.transferTreatments['TR-TAKEN']={treatment:'permanent_withdrawal',reason:'Owner confirmed permanent'};
  const confirmed=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store)});
  assert.equal(confirmed.moneyTaken.permanentTotal,100000);assert.equal(confirmed.moneyTaken.bySource.SANKI,100000);assert.deepEqual(confirmed.review.transferRows,[]);
});

test('legacy funding-source labels do not change bank-backed loan allocation',()=>{
  const store=fixture(),before=JSON.stringify({expenses:store.expenses,transfers:store.transfers,receipts:store.receipts});
  store.personalReporting.movementSources['P3/PAY-001']={source:'SEWAK_PARK_HOTEL',reason:'Owner confirmed'};
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store)});
  assert.equal(result.review.rows.length,0);
  assert.equal(result.moneyTaken.bySource.SEWAK_PARK_HOTEL,10000);
  assert.deepEqual(result.loanUsage.map(row=>row.businessUse),[500000,0]);
  assert.equal(JSON.stringify({expenses:store.expenses,transfers:store.transfers,receipts:store.receipts}),before);
});

test('a saved repayment note is recognized without a second owner decision',()=>{
  const store=fixture();store.transfers.push({id:'TR-REPAID',date:'2026-10-25',fromNature:'SANKI',toNature:'PERSONAL',fromAccount:'Axis Bank 3448',toAccount:'ICICI Bank 0993',classification:'owner_withdrawal',amount:25000,note:'Payng Back amount used for business'});
  const result=report.build(store,{from:'2026-10-01',to:'2026-10-31',viewer:'owner',spendingRows:spending(),transfers:transfers(store)});
  assert.ok(result.settlements.recordedSettlements.some(row=>row.id==='TR-REPAID'&&row.direction==='Business repaid you'));
  assert.ok(!result.review.transferRows.some(row=>row.id==='TR-REPAID'));
});

test('Personal report UI keeps the agreed simple labels and inline corrections',()=>{
  const read=file=>fs.readFileSync(path.join(__dirname,'..',file),'utf8'),html=read('public/compact-reports.html'),js=read('public/compact-reports.js'),css=read('public/compact-reports.css');
  for(const label of ['Spending &amp; Budget','Money Taken &amp; Settle','Loans &amp; Transfers','Review Decisions','Business has to return to you','You have to return to business'])assert.match(html,new RegExp(label.replace(/[&]/g,'&')));
  assert.match(js,/personal-report\/budget/);assert.match(js,/personal-report\/source/);assert.doesNotMatch(js,/personal-report\/loan-allocation/);assert.match(js,/personal-report\/transfer-treatment/);assert.match(js,/Personal portion/);assert.match(js,/Sent to business/);
  assert.doesNotMatch(js,/\['Business-loan personal portion'/,'the loan personal portion must not be shown twice in the overview');assert.match(js,/\['You have to return to business'.*View breakdown/);
  assert.match(html,/Sent to business is calculated only from actual transfers/);assert.doesNotMatch(html,/Oldest available loan money|Unallocated/);
  assert.match(css,/--ivory:#faf7ee/);assert.match(css,/\.personal-source-edit/);
});
