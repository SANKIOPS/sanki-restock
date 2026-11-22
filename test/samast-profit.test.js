'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {build}=require('../modules/compact-accounting-report');
const options={entity:'SAMAST',from:'2026-09-01',to:'2026-10-15',today:'2026-11-15'};
function sources(){return {expenses:{expenses:{},receipts:[],transfers:[],rentals:{tenancies:[],deposits:[],allocations:[]}},orders:{orders:{}},purchases:{pos:{}},samastSalary:{employees:{},salaryPayments:[]},salary:{},incentives:{payments:[]},manualSales:{sales:[]},shopifyCosts:{items:[]},feeds:[]};}
const cost=(id,nature,amount,ledger,date='2026-09-10')=>({id,nature,amount,ledger,date,status:'paid',payments:[{id:'P1',date,amount,account:'IndusInd Bank 7883'}]});
test('Samast monthly profit splits confirmed rent rates, excludes personal spending and non-rent receipts without source changes',()=>{
 const src=sources(),s=src.expenses;s.receipts=[{id:'R1',nature:'SAMAST',receiptType:'rental_income',date:'2026-09-05',amount:108000},{id:'R2',nature:'SAMAST',receiptType:'rental_income',date:'2026-10-05',amount:54000},...['rental_security_deposit','loan_received','other_income'].map((receiptType,i)=>({id:'X'+i,nature:'SAMAST',receiptType,date:'2026-09-05',amount:10000}))];
 s.expenses.E=cost('E','SAMAST',20000,'Repairs');s.expenses.P=cost('P','PERSONAL',9000,'Food');s.expenses.S=cost('S','SAMAST',5000,'Salary');s.expenses.G=cost('G','SAMAST',18000,'GST payment');s.expenses.U={...cost('U','SAMAST',500,'Repairs'),status:'pending',payments:[]};
 const before=JSON.stringify(src),r=build(src,options).samastProfit;
 assert.deepEqual(r.months.map(m=>[m.month,m.baseRent,m.expense,m.salary,m.profit,m.gst,m.tds,m.received]),[['2026-09',100000,20000,5000,75000,18000,10000,108000],['2026-10',50000,0,0,50000,9000,5000,54000]]);
 assert.equal(r.months[1].partial,true);assert.equal(r.totals.profit,125000);assert.equal(JSON.stringify(src),before);
});
test('salary linked to an expense is separated once, standalone payroll also appears with proof',()=>{
 const src=sources();src.expenses.expenses.S=cost('S','SAMAST',5000,'Staff costs');src.samastSalary.salaryPayments=[{id:'SAL1',date:'2026-09-10',amount:5000,linkedLedgerEntryId:'S/P1'},{id:'SAL2',date:'2026-10-10',amount:6000,employeeName:'Guard',proofs:['/proof.png']}];
 const r=build(src,options).samastProfit;assert.equal(r.totals.salary,11000);assert.equal(r.totals.expense,0);assert.equal(r.entries.filter(e=>e.profitKind==='salary').length,2);assert.equal(r.entries.find(e=>e.id.startsWith('SAL2')).proof,'/proof.png');
});
test('draft rental receipts excluded and rounded component split preserves bank amount exactly; empty months retained',()=>{
 const src=sources();src.expenses.bankReconciliationDrafts={D:{}};src.expenses.receipts=[{id:'P',nature:'SAMAST',receiptType:'rental_income',amount:108000,date:'2026-09-01',bankReconciliationEvidence:{draftId:'D'}},{id:'R',nature:'SAMAST',receiptType:'rental_income',amount:100,date:'2026-10-02'}];
 const r=build(src,options).samastProfit;assert.equal(r.entries.length,1);assert.equal(r.months[0].baseRent,0);const e=r.entries[0];assert.equal(Math.round((e.baseRent+e.gst-e.tds)*100),10000);assert.equal(e.received,100);
 assert.equal(build(sources(),{...options,entity:'PERSONAL'}).samastProfit,undefined);
});
test('actual GST and income tax payments are separate from profit and rent tax components; unpaid and personal tax excluded',()=>{
 const src=sources(),s=src.expenses;
 s.receipts=[{id:'RENT',nature:'SAMAST',receiptType:'rental_income',amount:108000,date:'2026-09-05'}];
 s.expenses.GST=cost('GST','SAMAST',74428,'GST Fee');s.expenses.GST.particulars='GST/26100700531448 /GST';
 s.expenses.GST2=cost('GST2','SAMAST',1000,'GST payment');s.expenses.IT=cost('IT','SAMAST',2500,'Self assessed income tax');
 s.expenses.IT2=cost('IT2','SAMAST',500,'Self Assessed I.T.');s.expenses.FEE=cost('FEE','SAMAST',2000,'GST filing service');
 s.expenses.P=cost('P','PERSONAL',6000,'Income Tax');s.expenses.U={...cost('U','SAMAST',9000,'Income Tax'),status:'pending',payments:[]};
 const before=JSON.stringify(src),r=build(src,options).samastProfit;
 assert.equal(r.totals.received,108000);assert.equal(r.totals.baseRent,100000);assert.equal(r.totals.gst,18000);assert.equal(r.totals.tds,10000);
 assert.equal(r.totals.gstPaid,75428);assert.equal(r.totals.incomeTaxPaid,3000);assert.equal(r.totals.expense,2000);assert.equal(r.totals.profit,98000);
 assert.equal(r.entries.filter(e=>e.profitKind==='tax').length,4);assert.equal(JSON.stringify(src),before);
});
test('income tax compliance fees remain operating costs, not income tax paid',()=>{
 const {taxKind}=require('../modules/samast-profit');
 for(const ledger of ['Income tax filing fees','Income tax professional service','GST Fee'])assert.equal(taxKind({ledger}), '');
 assert.equal(taxKind({ledger:'GST Fee',reconciliationSource:{bankDescription:'GST/123/GST'}}),'gstPaid');
});
test('monthly rent tracking separates approved net due, actual bank receipts and verified TDS credit',()=>{
 const src=sources(),s=src.expenses;
 s.rentals={tenancies:[{id:'TEN-A',tenant:'Synthetic tenant',startMonth:'2026-09',trackingFrom:'2026-09',confirmedAt:'2026-09-01'}],invoices:[{id:'INV-A',tenancyId:'TEN-A',month:'2026-09',dueDate:'2026-09-05',baseRent:110000,cgst:9900,sgst:9900,tds:11000,bankDue:118800,tdsStatus:'verified',tdsReference:'26AS synthetic reference'}],allocations:[],deposits:[]};
 s.receipts=[{id:'RENT-A',nature:'SAMAST',receiptType:'rental_income',date:'2026-09-07',amount:108000,account:'Synthetic bank'}];
 const r=build(src,{...options,to:'2026-09-30'}).samastProfit,m=r.months[0];
 assert.equal(m.desiredNet,118800);assert.equal(m.received,108000);assert.equal(m.rentDifference,10800);
 assert.equal(m.gstExpected,19800);assert.equal(m.gst,18000);assert.equal(m.gstPending,1800);
 assert.equal(m.tdsExpected,11000);assert.equal(m.tdsVerified,11000);assert.equal(m.tdsPending,0);
 assert.equal(m.baseRent,100000);assert.equal(m.profit,100000);
 assert.equal(r.entries.find(x=>x.id==='INV-A').tdsReference,'26AS synthetic reference');
});
test('desired rent and monthly tax gaps remain Needs review when an active tenancy has no approved charge',()=>{
 const src=sources(),s=src.expenses;
 s.rentals={tenancies:[{id:'TEN-A',tenant:'Missing monthly charge',startMonth:'2026-09',trackingFrom:'2026-09',confirmedAt:'2026-09-01'}],invoices:[],allocations:[],deposits:[]};
 s.receipts=[{id:'RENT-A',nature:'SAMAST',receiptType:'rental_income',date:'2026-09-07',amount:108000}];
 const r=build(src,{...options,to:'2026-09-30'}).samastProfit,m=r.months[0];
 assert.equal(m.desiredNet,null);assert.equal(m.rentDifference,null);assert.equal(m.gstPending,null);assert.equal(m.tdsPending,null);
 assert.deepEqual(m.rentDueReview,['Missing monthly charge']);assert.equal(r.totals.desiredNet,null);
 assert.equal(m.received,108000);assert.equal(m.baseRent,100000);assert.equal(m.profit,100000);
});
