'use strict';
const test=require('node:test'),assert=require('node:assert/strict');const {group}=require('../public/report-expense-groups');
test('each paid expense appears in exactly one group, never sales or COGS',()=>{
 const rows=[{kind:'income',category:'POS sales',amount:10000},{kind:'cost_of_sales',category:'POS sales',amount:4000},{kind:'expense',category:'Salary',amount:1000,sourceKind:'salary'},{kind:'expense',category:'Salary Expense',amount:200},{kind:'expense',category:'Store Rent Expense',amount:2000},{kind:'expense',category:'Bank Interest Paid',amount:100},{kind:'expense',category:'IG Others',amount:50},{kind:'expense',category:'Fittings',amount:30},{kind:'expense',category:'Fittings',amount:-10}];
 const before=JSON.stringify(rows),groups=group(rows);assert.deepEqual(groups.map(g=>[g.key,g.amount]),[['salary',1200],['rent',2000],['interest',100],['operating',70]]);assert.equal(groups.flatMap(g=>g.rows).length,7);assert.equal(JSON.stringify(rows),before);
});
