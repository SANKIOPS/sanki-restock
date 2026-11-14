'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {relevant,costs,sum}=require('../public/unit-economics-details');
const entries=[{id:'S',reference:'#1',date:'2026-11-01',kind:'sale',discounts:10,values:{cogs:null,grossProfit:null,gst:5},details:[{title:'Shirt',sku:'SKU1',qty:2,cogs:120},{title:'Tee',sku:'SKU2',qty:1,cogs:null}]},{id:'R',reference:'#1',date:'2026-11-02',kind:'return',returns:105,values:{cogs:-60,grossProfit:-40,gst:-5},details:[{title:'Shirt',sku:'SKU1',qty:1,cogs:-60,returnedToStock:true}]},{id:'E',kind:'expense',values:{operatingExpenses:20,grossProfit:0}}];
test('COGS includes missing product rows and identifies reversals without inventing a unit cost',()=>{
 const before=JSON.stringify(entries),rows=costs(entries);assert.equal(rows.length,3);assert.equal(rows[0].unitCost,60);assert.equal(rows[1].cost,null);assert.equal(rows[1].unitCost,null);assert.match(rows[1].status,/Cost missing/);assert.equal(rows[2].cost,-60);assert.match(rows[2].status,/restocked/);assert.equal(sum(rows,'cost'),60);assert.equal(JSON.stringify(entries),before);
});
test('profit and GST pop-ups retain unknown or zero entries rather than silently excluding them',()=>{
 assert.deepEqual(relevant(entries,'grossProfit').map(r=>r.id),['S','R']);assert.equal(relevant(entries,'grossProfit')[0].values.grossProfit,null);
 assert.deepEqual(relevant(entries,'gst').map(r=>r.id),['S','R']);assert.deepEqual(relevant(entries,'discountsReturns').map(r=>r.id),['S','R']);assert.deepEqual(relevant(entries,'operatingExpenses').map(r=>r.id),['E']);
});
test('unmapped product details remain visible, and a return without restocking reverses no cost',()=>{
 const rows=costs([{id:'X',kind:'sale',values:{cogs:null},details:[]},{id:'Y',kind:'return',values:{cogs:0},details:[{sku:'SKU',qty:1,cogs:0,returnedToStock:false}]}]);assert.equal(rows[0].cost,null);assert.match(rows[0].product,/unavailable/);assert.equal(rows[1].cost,0);assert.match(rows[1].status,/No stock cost reversed/);
});
