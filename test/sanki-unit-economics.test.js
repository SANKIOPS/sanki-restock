'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {build,bucket}=require('../modules/sanki-unit-economics');
function report(){const base={date:'2026-10-01',grossSales:0,discounts:0,returns:0,gst:0,netSales:0,otherIncome:0,cogs:0,expenses:0,details:[]};return {totals:{grossSales:1200,discounts:50,returns:100,gst:50,netSales:1000,cogs:400,grossProfit:600,otherIncome:30,expenses:295,netProfit:335,complete:true,salesCount:1},costSource:{name:'Verified FIFO'},transactions:[
 {...base,id:'SALE',kind:'sale',grossSales:1200,discounts:50,gst:55,netSales:1095,cogs:450,details:[{qty:3,itemKind:'product'}]},
 {...base,id:'RETURN',kind:'return',returns:100,gst:-5,netSales:-95,cogs:-50,details:[{qty:1}]},
 ...[['PACK','Courier Bags Expense',20],['AD','Website Meta',50],['SALARY','Salary',150],['INTEREST','Loan Interest',30],['TAX','Income Tax',20],['DA','Depreciation',25]].map(([id,category,expenses])=>({...base,id,kind:'expense',category,expenses})),
 {...base,id:'OTHER',kind:'income',category:'Other income',otherIncome:20},
 {...base,id:'BANK-INTEREST',kind:'income',category:'Bank interest',otherIncome:10}
 ]};}
test('simple trading rows reconcile through EBITDA and net profit, including returns, salary and finance costs',()=>{
 const src=report(),before=JSON.stringify(src),r=build(src),v=r.values;
 assert.equal(v.discountsReturns,150);assert.equal(v.netSales,1000);assert.equal(v.cogs,400);assert.equal(v.grossProfit,600);
 assert.equal(v.orderCosts,20);assert.equal(v.marketing,50);assert.equal(v.operatingExpenses,150);
 assert.equal(v.otherIncome,20);assert.equal(v.ebitda,400);assert.equal(v.otherDeductions,75);assert.equal(v.netProfit,335);
 assert.equal(v.ebitda+v.interestIncome-v.otherDeductions,v.netProfit);
 for(const key of Object.keys(r.entries[0].values)){const sum=Math.round(r.entries.reduce((n,e)=>n+(e.values[key]||0),0)*100)/100;if(v[key]!=null)assert.equal(sum,v[key],key+' drill-down agrees');}
 assert.deepEqual(r.stats,{orders:1,units:2,averageOrder:1000,profitPerItem:167.5});assert.equal(JSON.stringify(src),before);
});
test('unknown COGS and unresolved repayment review never produce an EBITDA or per-item profit',()=>{
 const src=report();Object.assign(src.totals,{complete:false,cogs:null,grossProfit:null,netProfit:null});
 const r=build(src);assert.equal(r.values.cogs,null);assert.equal(r.values.ebitda,null);assert.equal(r.values.netProfit,null);assert.equal(r.stats.profitPerItem,null);
 const blocked=build(report(),{blocked:true});assert.equal(blocked.values.ebitda,null);assert.equal(blocked.values.netProfit,null);
});
test('expense classification uses saved category and marketing type, never vendor narration guesses',()=>{
 assert.equal(bucket({category:'Bank Interest Paid'}),'interest');assert.equal(bucket({category:'Furniture',particulars:'delivery packing marketing interest'}),'operatingExpenses');
 assert.equal(bucket({category:'Custom Campaign',group:'marketing'}),'marketing');assert.equal(bucket({category:'Income Tax'}),'tax');assert.equal(bucket({category:'Electricity'}),'operatingExpenses');
});
test('expense refunds reduce their original bucket, missing quantities remain unavailable and zero sales divide safely',()=>{
 const src=report();src.transactions.push({...src.transactions[2],id:'PACK-REFUND',kind:'expense_refund',expenses:-5});
 assert.equal(build(src).values.orderCosts,15);
 src.transactions[0].details=[];assert.equal(build(src).stats.units,null);assert.equal(build(src).stats.profitPerItem,null);
 src.totals.salesCount=0;assert.equal(build(src).stats.averageOrder,null);
});

test('explicit bank-interest receipt type wins over a custom display category',()=>{
 const src=report();const interest=src.transactions.find(r=>r.id==='BANK-INTEREST');interest.reference='REC-INTEREST';interest.category='Custom bank credit';
 const r=build(src,{books:{receipts:[{id:'REC-INTEREST',receiptType:'bank_interest'}]}});assert.equal(r.values.interestIncome,10);assert.equal(r.values.ebitda,400);
});
