'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {buildStatement}=require('../modules/gokwik-statement'),g=require('../modules/gokwik-accounting');
function fixture(){
 const tx={paymentId:'P1',orderId:'O1',date:'2026-08-24',settledDate:'2026-08-25',utr:'UTR1',provider:'easebuzz',gross:150,fee:3.75,tax:.68,additionalFee:0,additionalTax:0,deduction:75,net:70.57};
 return {store:{gokwikTransactions:{P1:tx}},entries:[{id:'OPENING',kind:'opening',credit:100,debit:0},{id:'GKW-PAYMENT/P1',kind:'sale',date:tx.date,description:'Collection #7001',credit:150,debit:0}]};
}
test('one date-ordered statement includes each unposted fee, deduction and net payout as Out',()=>{
 const {store,entries}=fixture(),before=JSON.stringify({store,entries}),report=buildStatement(entries,store,{recordedBalance:250});
 assert.equal(report.entries.length,4);assert.deepEqual(report.entries.map(r=>r.balance),[250,245.57,170.57,100]);
 assert.equal(report.totalIn,150);assert.equal(report.totalOut,150);assert.equal(report.pendingOut,150);assert.equal(report.closingBalance,100);assert.equal(report.recordedBalance,250);
 assert.equal(report.entries.filter(r=>r.pendingPosting).length,3);assert.ok(report.entries.filter(r=>r.pendingPosting).every(r=>r.editable===false&&r.credit===0));assert.equal(JSON.stringify({store,entries}),before);
});
test('posted batches use actual ledger rows, without adding the report outflows twice',()=>{
 const {store,entries}=fixture(),id=g.batches(Object.values(store.gokwikTransactions))[0].id;
 store.gokwikPostings={[id]:{transferId:'TR1',expenseIds:['EX1','EX2']}};
 entries.push({id:'TR1',kind:'transfer',date:'2026-08-25',debit:70.57},{id:'EX1/PAY-001',kind:'expense',date:'2026-08-25',debit:4.43},{id:'EX2/PAY-001',kind:'expense',date:'2026-08-25',debit:75});
 const report=buildStatement(entries,store,{recordedBalance:100});assert.equal(report.entries.length,4);assert.equal(report.pendingOut,0);assert.equal(report.totalOut,150);assert.equal(report.closingBalance,100);
});
test('range opening carries earlier report events and future outflows are not subtracted',()=>{
 const {store,entries}=fixture();const before=buildStatement(entries,store,{to:'2026-08-24'});assert.equal(before.closingBalance,250);assert.equal(before.pendingOut,0);
 const day=buildStatement(entries,store,{from:'2026-08-25',to:'2026-08-25'});assert.equal(day.openingBalance,250);assert.equal(day.entries.length,3);assert.equal(day.totalIn,0);assert.equal(day.closingBalance,100);
 const after=buildStatement(entries,store,{from:'2026-08-26',to:'2026-08-30'});assert.equal(after.openingBalance,100);assert.equal(after.closingBalance,100);assert.equal(after.entries.length,0);
});
test('manual movements and existing exact opening remain part of the common statement',()=>{
 const {store,entries}=fixture();entries[0].credit=100.13;entries.push({id:'MANUAL',kind:'receipt',date:'2026-08-26',credit:10.24});const report=buildStatement(entries,store);assert.equal(report.openingBalance,100.13);assert.equal(report.closingBalance,110.37);assert.equal(report.totalIn,160.24);
});
test('approval is shown as pending and never treated as a posting',()=>{
 const {store,entries}=fixture(),id=g.batches(Object.values(store.gokwikTransactions))[0].id;store.gokwikApprovals={[id]:{requestedBy:'Admin'}};const before=JSON.stringify(store),report=buildStatement(entries,store);assert.match(report.entries[1].status,/Unposted.*awaiting Owner/);assert.equal(report.pendingOut,150);assert.equal(JSON.stringify(store),before);
});
test('an unrelated expense-entity filter cannot expose SANKI provider outflows',()=>{
 const {store,entries}=fixture();entries[1].entity='SANKI';const report=buildStatement(entries,store,{expenseNature:'SAMAST'});assert.equal(report.entries.length,0);assert.equal(report.pendingOut,0);
});
test('zero-net batches have deductions but never invent a zero bank payout',()=>{
 const {store,entries}=fixture();Object.assign(store.gokwikTransactions.P1,{fee:0,tax:0,deduction:150,net:0});const report=buildStatement(entries,store);assert.equal(report.entries.length,2);assert.equal(report.totalOut,150);assert.ok(!report.entries.some(r=>r.kind==='gokwik_report_payout'));
});
test('multiple payments sharing one UTR produce one set of settlement outflows',()=>{
 const {store,entries}=fixture();store.gokwikTransactions.P2={...store.gokwikTransactions.P1,paymentId:'P2',orderId:'O2'};entries.push({...entries[1],id:'GKW-PAYMENT/P2'});const report=buildStatement(entries,store);assert.equal(report.entries.filter(r=>r.pendingPosting).length,3);assert.equal(report.totalIn,300);assert.equal(report.totalOut,300);assert.equal(report.closingBalance,100);
});
