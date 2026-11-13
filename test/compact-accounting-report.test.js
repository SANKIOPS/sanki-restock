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
