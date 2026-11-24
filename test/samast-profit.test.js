'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {build}=require('../modules/compact-accounting-report');
const options={entity:'SAMAST',from:'2026-09-01',to:'2026-10-15',today:'2026-11-15'};
const tenant=(overrides={})=>({id:'TEN-A',tenant:'Synthetic tenant',floor:'First',baseRent:100000,netRent:108000,startMonth:'2026-09',trackingFrom:'2026-09',cgst:9,sgst:9,tds:10,history:[],...overrides});
function sources(){return {expenses:{expenses:{},receipts:[],transfers:[],rentals:{tenancies:[tenant()],invoices:[],tdsEvidence:[],deposits:[],allocations:[]}},orders:{orders:{}},purchases:{pos:{}},samastSalary:{employees:{},salaryPayments:[]},salary:{},incentives:{payments:[]},manualSales:{sales:[]},shopifyCosts:{items:[]},feeds:[]};}
function livePayroll(src,rows={'2026-09':{paidDays:5}}){src.samastSalary={employees:{E1:{id:'E1',name:'Guard',salary:30000,active:true,joiningDate:'2026-09-01'}},months:Object.fromEntries(Object.entries(rows).map(([month,row])=>[month,{rows:{E1:row},attendance:{}}])),divisor:30,salaryPayments:[],advances:{}};return src.samastSalary;}
const cost=(id,nature,amount,ledger,date='2026-09-10')=>({id,nature,amount,ledger,date,status:'paid',payments:[{id:'P1',date,amount,account:'IndusInd Bank 7883'}]});
test('Samast monthly profit splits confirmed rent rates, excludes personal spending and non-rent receipts without source changes',()=>{
 const src=sources(),s=src.expenses;s.receipts=[{id:'R1',nature:'SAMAST',receiptType:'rental_income',tenancyId:'TEN-A',date:'2026-09-05',amount:108000},{id:'R2',nature:'SAMAST',receiptType:'rental_income',tenancyId:'TEN-A',date:'2026-10-05',amount:54000},...['rental_security_deposit','loan_received','other_income'].map((receiptType,i)=>({id:'X'+i,nature:'SAMAST',receiptType,date:'2026-09-05',amount:10000}))];
 s.expenses.E=cost('E','SAMAST',20000,'Repairs');s.expenses.P=cost('P','PERSONAL',9000,'Food');s.expenses.S=cost('S','SAMAST',5000,'Salary');s.expenses.G=cost('G','SAMAST',18000,'GST payment');s.expenses.U={...cost('U','SAMAST',500,'Repairs'),status:'pending',payments:[]};livePayroll(src);
 const before=JSON.stringify(src),r=build(src,options).samastProfit;
 assert.deepEqual(r.months.map(m=>[m.month,m.baseRent,m.expense,m.salary,m.profit,m.gst,m.tds,m.received]),[['2026-09',100000,20000,5000,75000,18000,10000,108000],['2026-10',50000,0,0,50000,9000,5000,54000]]);
 assert.deepEqual(r.months.map(m=>[m.baseDue,m.gstExpected,m.tdsExpected,m.desiredNet]),[[100000,18000,10000,108000],[100000,18000,10000,108000]]);
 assert.equal(r.months[1].partial,true);assert.equal(r.totals.profit,125000);assert.equal(JSON.stringify(src),before);
});
test('live attendance Payroll earnings replace salary payments and update with an adjustment',()=>{
 const src=sources();src.expenses.expenses.S=cost('S','SAMAST',5000,'Staff costs');livePayroll(src,{'2026-09':{paidDays:5},'2026-10':{paidDays:6,salaryAdjustment:500}});src.samastSalary.salaryPayments=[{id:'SAL1',date:'2026-09-10',amount:99999,linkedLedgerEntryId:'S/P1'}];
 let r=build(src,options).samastProfit;assert.equal(r.totals.salary,11500);assert.equal(r.totals.expense,0);assert.equal(r.entries.filter(e=>e.profitKind==='salary').length,2);assert.ok(r.entries.every(e=>e.profitKind!=='salary'||e.calculationSource==='Attendance-based Payroll'));
 src.samastSalary.months['2026-10'].rows.E1.salaryAdjustment=1500;r=build(src,options).samastProfit;assert.equal(r.totals.salary,12500);
});
test('draft rental receipts excluded and rounded component split preserves bank amount exactly; empty months retained',()=>{
 const src=sources();src.expenses.bankReconciliationDrafts={D:{}};src.expenses.receipts=[{id:'P',nature:'SAMAST',receiptType:'rental_income',tenancyId:'TEN-A',amount:108000,date:'2026-09-01',bankReconciliationEvidence:{draftId:'D'}},{id:'R',nature:'SAMAST',receiptType:'rental_income',tenancyId:'TEN-A',amount:100,date:'2026-10-02'}];
 const r=build(src,options).samastProfit,received=r.entries.filter(x=>x.profitKind==='rent');assert.equal(received.length,1);assert.equal(r.months[0].baseRent,0);const e=received[0];assert.equal(Math.round((e.baseRent+e.gst-e.tds)*100),10000);assert.equal(e.received,100);
 assert.equal(build(sources(),{...options,entity:'PERSONAL'}).samastProfit,undefined);
});
test('actual GST and income tax payments are separate from profit and rent tax components; unpaid and personal tax excluded',()=>{
 const src=sources(),s=src.expenses;
 s.receipts=[{id:'RENT',nature:'SAMAST',receiptType:'rental_income',tenancyId:'TEN-A',amount:108000,date:'2026-09-05'}];
 s.expenses.GST=cost('GST','SAMAST',74428,'GST Fee');s.expenses.GST.particulars='GST/26100700531448 /GST';
 s.expenses.GST2=cost('GST2','SAMAST',1000,'GST payment');s.expenses.IT=cost('IT','SAMAST',2500,'Self assessed income tax');
 s.expenses.IT2=cost('IT2','SAMAST',500,'Self Assessed I.T.');s.expenses.FEE=cost('FEE','SAMAST',2000,'GST filing service');
 s.expenses.P=cost('P','PERSONAL',6000,'Income Tax');s.expenses.U={...cost('U','SAMAST',9000,'Income Tax'),status:'pending',payments:[]};
 const before=JSON.stringify(src),r=build(src,options).samastProfit;
 assert.equal(r.totals.received,108000);assert.equal(r.totals.baseRent,100000);assert.equal(r.totals.gst,18000);assert.equal(r.totals.tds,10000);
 assert.equal(r.totals.gstPaid,75428);assert.equal(r.totals.incomeTaxPaid,3000);assert.equal(r.totals.expense,2000);assert.equal(r.totals.profit,98000);
 assert.equal(r.totals.gstLiability,0);assert.equal(r.totals.gstCredit,57428);assert.equal(r.totals.gstEvidencePending,75428);assert.equal(r.reconciliation.gst.status,'pending');assert.equal(r.incomeTax.status,'awaiting_rules');
 assert.equal(r.entries.filter(e=>e.profitKind==='tax').length,4);assert.equal(JSON.stringify(src),before);
});
test('income tax compliance fees remain operating costs, not income tax paid',()=>{
 const {taxKind}=require('../modules/samast-profit');
 for(const ledger of ['Income tax filing fees','Income tax professional service','GST Fee'])assert.equal(taxKind({ledger}), '');
 assert.equal(taxKind({ledger:'GST Fee',reconciliationSource:{bankDescription:'GST/123/GST'}}),'gstPaid');
});
test('monthly rent tracking calculates charge taxes from base rent and ignores inconsistent saved net fields',()=>{
 const src=sources(),s=src.expenses;
 s.rentals={tenancies:[tenant()],invoices:[{id:'INV-A',tenancyId:'TEN-A',month:'2026-09',dueDate:'2026-09-05',baseRent:110000,cgst:1,sgst:2,tds:3,bankDue:999999,tdsStatus:'verified',tdsReference:'26AS synthetic reference'}],tdsEvidence:[],allocations:[],deposits:[]};
 s.receipts=[{id:'RENT-A',nature:'SAMAST',receiptType:'rental_income',tenancyId:'TEN-A',date:'2026-09-07',amount:108000,account:'Synthetic bank'}];
 const r=build(src,{...options,to:'2026-09-30'}).samastProfit,m=r.months[0];
 assert.equal(m.baseDue,110000);assert.equal(m.desiredNet,118800);assert.equal(m.received,108000);assert.equal(m.rentDifference,10800);
 assert.equal(m.gstExpected,19800);assert.equal(m.gst,18000);assert.equal(m.gstPending,1800);
 assert.equal(m.tdsExpected,11000);assert.equal(m.tdsVerified,11000);assert.equal(m.tdsPending,0);
 assert.equal(m.baseRent,100000);assert.equal(m.profit,100000);
 assert.equal(r.entries.find(x=>x.id==='INV-A').tdsReference,'26AS synthetic reference');
});
test('tenant setup automatically supplies desired rent and prorates the first active month',()=>{
 const src=sources(),s=src.expenses;
 s.rentals={tenancies:[tenant({tenant:'Partial tenant',baseRent:30000,netRent:32400,startDate:'2026-09-11'})],invoices:[],tdsEvidence:[],allocations:[],deposits:[]};
 s.receipts=[{id:'RENT-A',nature:'SAMAST',receiptType:'rental_income',tenancyId:'TEN-A',date:'2026-09-17',amount:21600}];
 const r=build(src,{...options,to:'2026-09-30'}).samastProfit,m=r.months[0];
 assert.equal(m.baseDue,20000);assert.equal(m.desiredNet,21600);assert.equal(m.received,21600);assert.equal(m.rentDifference,0);
 assert.equal(m.gstExpected,3600);assert.equal(m.tdsExpected,2000);assert.equal(m.tdsStatus,'Pending');
 assert.equal(m.baseRent,20000);assert.equal(m.gst,3600);assert.equal(m.tds,2000);assert.equal(m.profit,20000);
 assert.match(r.entries.find(x=>x.profitKind==='rent_due').description,/automatic 20\/30 day proration/);
});
test('an unassigned bank rent receipt remains visible without inventing its tax split or profit',()=>{
 const src=sources();src.expenses.receipts=[{id:'UNASSIGNED',nature:'SAMAST',receiptType:'rental_income',date:'2026-09-07',amount:108000}];
 const r=build(src,{...options,to:'2026-09-30'}).samastProfit,m=r.months[0];
 assert.equal(m.desiredNet,108000);assert.equal(m.received,108000);assert.equal(m.rentDifference,0);assert.equal(m.unassignedRent,108000);
 assert.equal(m.baseRent,null);assert.equal(m.gst,null);assert.equal(m.tds,null);assert.equal(m.profit,null);assert.equal(m.taxSplitComplete,false);
});
test('TDS status becomes submitted only from saved tenant-month evidence',()=>{
 const src=sources();src.expenses.rentals.tdsEvidence=[{id:'TDS-1',tenancyId:'TEN-A',month:'2026-09',status:'verified',reference:'26AS-REF'}];
 const r=build(src,{...options,to:'2026-09-30'}).samastProfit,m=r.months[0],due=r.entries.find(x=>x.profitKind==='rent_due');
 assert.equal(m.tdsExpected,10000);assert.equal(m.tdsVerified,10000);assert.equal(m.tdsPending,0);assert.equal(m.tdsStatus,'Submitted');assert.equal(due.tdsReference,'26AS-REF');
});
test('Samast monthly UI follows the simple rent formula and expands floor receipts inline',()=>{
 const js=require('node:fs').readFileSync(require('node:path').join(__dirname,'../public/compact-reports.js'),'utf8');
 assert.match(js,/headers=\['Month','Agreed base rent','GST \(18%\)','TDS \(10%\)','Rent to collect','Actual rent received · base \/ GST \/ TDS','Difference','Expenses','Live Payroll salary','Profit before tax'\]/);
 assert.match(js,/samastMonthBreakdown\(expandedSamastMonth\)/);assert.match(js,/className='rent-receipt-evidence'/);
 assert.match(js,/\['Actual rent received',actualRentCell/);assert.match(js,/\['GST liability'/);assert.match(js,/\['TDS reconciliation'/);assert.match(js,/\['Self-assessed income tax'/);
 assert.doesNotMatch(js,/Search profit months/);
});
