'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {build}=require('../modules/compact-accounting-report');
const sources=()=>({expenses:{expenses:{},receipts:[],transfers:[],rentals:{tenancies:[],deposits:[],allocations:[]}},orders:{orders:{}},purchases:{pos:{}},salary:{employees:{},salaryPayments:[]},samastSalary:{employees:{},salaryPayments:[]},incentives:{payments:[]},manualSales:{sales:[]},shopifyCosts:{items:[]},feeds:[]});
const options={entity:'PERSONAL',from:'2026-08-22',to:'2026-11-13',today:'2026-11-13'};
test('20 lakh borrowing, transfers and principal do not become profit; personal costs follow expense tag not bank',()=>{
 const src=sources(),s=src.expenses;s.receipts=[{id:'L1',nature:'PERSONAL',receiptType:'loan_received',date:'2026-10-15',amount:1000000,account:'ICICI Bank 0993',source:'Lender 1'},{id:'L2',nature:'PERSONAL',receiptType:'loan_received',date:'2026-10-15',amount:1000000,account:'ICICI Bank 0993',source:'Lender 2'},{id:'I1',nature:'PERSONAL',receiptType:'other_income',date:'2026-10-15',amount:10000}];
 s.expenses.E1={id:'E1',nature:'PERSONAL',status:'paid',date:'2026-10-15',amount:5000,ledger:'Food',payments:[{id:'P1',date:'2026-10-15',amount:5000,account:'IndusInd Bank 7883'}]};
 s.transfers=[{id:'TR1',fromNature:'PERSONAL',toNature:'SANKI',fromAccount:'ICICI Bank 0993',toAccount:'Axis Bank 3448',date:'2026-10-16',amount:500000,classification:'owner_contribution'}];
 const before=JSON.stringify(src),r=build(src,options);assert.equal(r.totals.income,10000);assert.equal(r.totals.expenses,5000);assert.equal(r.totals.result,5000);assert.equal(r.obligations.loanTotal,2000000);assert.equal(r.transfers[0].classification,'owner_contribution');assert.equal(r.paidFrom[0].account,'IndusInd Bank 7883');assert.equal(JSON.stringify(src),before);
});
test('security remains outstanding after spending, counts opening once and ignores future/refunded amounts',()=>{
 const src=sources(),s=src.expenses;s.receipts=[{id:'RENT',nature:'SAMAST',receiptType:'rental_income',amount:100000,date:'2026-10-01'},{id:'SEC',nature:'SAMAST',receiptType:'rental_security_deposit',amount:50000,date:'2026-10-01'}];
 s.rentals={tenancies:[{id:'T1',securityOpening:10000,securityOpeningDate:'2026-08-22'}],deposits:[{id:'SEC1',tenancyId:'T1',amount:50000,date:'2026-10-01'},{id:'REF',tenancyId:'T1',amount:-5000,date:'2026-10-02'},{tenancyId:'T1',amount:90000,date:'2026-12-01'}],allocations:[{tenancyId:'T1',kind:'security',amount:2000,date:'2026-10-03'}]};
 const r=build(src,{...options,entity:'SAMAST'});assert.equal(r.totals.income,100000);assert.equal(r.obligations.securityTotal,53000);assert.equal(r.resultLabel,'Cash surplus / deficit');
});
test('pending payments and loan principal are excluded; interest alone is expense',()=>{
 const src=sources(),s=src.expenses;s.expenses.P={id:'P',nature:'PERSONAL',status:'paid',amount:3000,date:'2026-10-01',payments:[{id:'PAY',amount:3000,date:'2026-10-01',bankReconciliationDraft:'DRAFT'}]};s.bankReconciliationDrafts={DRAFT:{}};
 s.loanRepayments=[{id:'LP',nature:'PERSONAL',date:'2026-10-01',amount:1100,principalAmount:1000,interestAmount:100,source:'Lender'}];
 const r=build(src,options);assert.equal(r.totals.expenses,100);
});
test('date boundaries and entity names are validated',()=>{for(const change of [{entity:'unknown'},{from:'2026-08-21'},{to:'2026-11-14'},{from:'2026-11-13',to:'2026-11-12'}])assert.throws(()=>build(sources(),{...options,...change}),/valid dates/);});

test('legacy card/EMI categories are disclosed for review, never counted as new costs or silently treated as profit',()=>{
 const src=sources();for(const [id,ledger,amount]of [['CARD','Credit Card',5000],['EMI','Car EMI',2000]])src.expenses.expenses[id]={id,nature:'PERSONAL',status:'paid',date:'2026-10-01',amount,ledger,payments:[{id:'PAY1',date:'2026-10-01',amount,account:'IndusInd Bank 7883'}]};
 const r=build(src,options);assert.equal(r.totals.expenses,0);assert.equal(r.totals.result,null);assert.equal(r.otherMovements.length,2);assert.equal(r.warnings.length,2);assert.equal(src.expenses.expenses.CARD.ledger,'Credit Card');
});

test('Sewak Park Hotel report separates operating result, personal use, funding and unclassified 0992 money without mutating PERSONAL records',()=>{
 const src=sources(),s=src.expenses;
 s.receipts=[
  {id:'HOTEL-LEGACY-TYPE',nature:'PERSONAL',receiptType:'hotel_income',category:'Sewak Park Hotel Income',date:'2026-10-01',amount:5000,account:'ICICI Bank 0992'},
  {id:'HOTEL-LEGACY-SOURCE',nature:'PERSONAL',receiptType:'other_income',incomeSource:'hotel_business',date:'2026-10-02',amount:3000,account:'ICICI Bank 0992'},
  {id:'HOTEL-EXPLICIT',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',receiptType:'other_income',date:'2026-10-03',amount:2000,account:'ICICI Bank 0992'},
  {id:'UNCLASSIFIED-IN',nature:'PERSONAL',receiptType:'other_income',date:'2026-10-04',amount:900,account:'ICICI Bank 0992'},
  {id:'OTHER-PERSONAL-IN',nature:'PERSONAL',receiptType:'other_income',date:'2026-10-04',amount:700,account:'ICICI Bank 0993'},
  {id:'HOTEL-LOAN',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',receiptType:'loan_received',date:'2026-10-04',amount:100000,account:'ICICI Bank 0992'},
  {id:'HOTEL-PENDING',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',receiptType:'hotel_income',date:'2026-10-04',amount:1111,account:'ICICI Bank 0992',bankReconciliationEvidence:{draftId:'PENDING'}}
 ];
 s.expenses={
  HOTEL:{id:'HOTEL',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',status:'paid',date:'2026-10-05',amount:3000,ledger:'Hotel housekeeping',payments:[{id:'P1',date:'2026-10-05',amount:3000,account:'ICICI Bank 0992'}]},
  HOTELOTHER:{id:'HOTELOTHER',nature:'PERSONAL',reportingUnit:'SEWAK_PARK_HOTEL',status:'paid',date:'2026-10-05',amount:400,ledger:'Hotel supplies',payments:[{id:'P1',date:'2026-10-05',amount:400,account:'ICICI Bank 0993'}]},
  PERSONALUSE:{id:'PERSONALUSE',nature:'PERSONAL',reportingTreatment:'personal_use',status:'paid',date:'2026-10-06',amount:600,ledger:'Personal food',payments:[{id:'P1',date:'2026-10-06',amount:600,account:'ICICI Bank 0992'}]},
  PERSONALUSESOURCE:{id:'PERSONALUSESOURCE',nature:'PERSONAL',sourceReportingUnit:'SEWAK_PARK_HOTEL',status:'paid',date:'2026-10-06',amount:400,ledger:'Personal travel',payments:[{id:'P1',date:'2026-10-06',amount:400,account:'ICICI Bank 0992'}]},
  UNCLASSIFIEDOUT:{id:'UNCLASSIFIEDOUT',nature:'PERSONAL',status:'paid',date:'2026-10-06',amount:250,ledger:'Needs review',payments:[{id:'P1',date:'2026-10-06',amount:250,account:'ICICI Bank 0992'}]},
  PERSONALOTHER:{id:'PERSONALOTHER',nature:'PERSONAL',status:'paid',date:'2026-10-06',amount:500,ledger:'Personal food',payments:[{id:'P1',date:'2026-10-06',amount:500,account:'ICICI Bank 0993'}]},
  CARD:{id:'CARD',nature:'PERSONAL',status:'paid',date:'2026-10-07',amount:2000,ledger:'Credit Card repayment',payments:[{id:'P1',date:'2026-10-07',amount:2000,account:'ICICI Bank 0992'}]}
 };
 s.bankReconciliationDrafts={PENDING:{id:'PENDING'}};
 s.expenseRefunds=[{id:'HOTEL-REFUND',nature:'PERSONAL',date:'2026-10-09',status:'received',sources:[{kind:'payment',expenseId:'HOTEL',amount:500,paymentAllocations:[{paymentId:'P1',amount:500}]}],components:[{mode:'bank',amount:500}]}];
 s.transfers=[
  {id:'WITHDRAW',nature:'PERSONAL',fromNature:'PERSONAL',toNature:'PERSONAL',fromReportingUnit:'SEWAK_PARK_HOTEL',fromAccount:'ICICI Bank 0992',toAccount:'ICICI Bank 0993',date:'2026-10-08',amount:1000},
  {id:'FUND',nature:'PERSONAL',fromNature:'PERSONAL',toNature:'PERSONAL',toReportingUnit:'SEWAK_PARK_HOTEL',fromAccount:'ICICI Bank 0993',toAccount:'ICICI Bank 0992',date:'2026-10-08',amount:2000},
  {id:'HOTEL-INTERNAL',nature:'PERSONAL',fromNature:'PERSONAL',toNature:'PERSONAL',fromReportingUnit:'SEWAK_PARK_HOTEL',toReportingUnit:'SEWAK_PARK_HOTEL',fromAccount:'Hotel cash',toAccount:'ICICI Bank 0992',date:'2026-10-08',amount:3000},
  {id:'UNCLASSIFIED-TRANSFER',nature:'PERSONAL',fromNature:'PERSONAL',toNature:'PERSONAL',fromAccount:'ICICI Bank 0992',toAccount:'ICICI Bank 0993',date:'2026-10-08',amount:4000}
 ];
 const before=JSON.stringify(src),r=build(src,{...options,unit:'SEWAK_PARK_HOTEL',accounts:[{name:'ICICI Bank 0992',balance:123},{name:'ICICI Bank 0993',balance:456}]});
 assert.deepEqual(r.totals,{income:10000,expenses:2900,costOfSales:0,result:7100,personalUse:2000,fundsIntroduced:2000,internalTransfers:3000,unclassified0992:5150});
 assert.equal(r.resultLabel,'Cash operating surplus');assert.deepEqual(r.accounts.map(x=>x.name),['ICICI Bank 0992']);
 assert.deepEqual(r.transactions.filter(x=>x.kind==='income').map(x=>x.id).sort(),['HOTEL-EXPLICIT','HOTEL-LEGACY-SOURCE','HOTEL-LEGACY-TYPE']);
 assert.equal(r.personalUse.length,3);assert.equal(r.fundsIntroduced.length,1);assert.equal(r.hotelInternalTransfers.length,1);assert.equal(r.unclassified0992Movements.length,3);
 assert.match(r.warnings.join('\n'),/3 ICICI Bank 0992 movement/);assert.equal(JSON.stringify(src),before);

 const personal=build(src,options);
 assert.equal(personal.unit,undefined);assert.equal(personal.totals.income,12711);assert.equal(personal.totals.expenses,4650);
});

test('Sewak Park reporting unit is valid only within PERSONAL accounting',()=>{
 assert.throws(()=>build(sources(),{...options,unit:'UNKNOWN'}),/valid reporting unit/);
 assert.throws(()=>build(sources(),{...options,entity:'SAMAST',unit:'SEWAK_PARK_HOTEL'}),/only inside PERSONAL/);
});
