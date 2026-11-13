'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {present,isBankNarration}=require('../public/expense-report-presentation');
const {enrich}=require('../modules/report-expense-entries');
test('screenshot bank narrations are never displayed as app particulars',()=>{
 for(const particulars of ['UPI/Mr SAJID K/XX7864@axl/UPI/CENTRAL BA/630336018188/ICIb7428bedd7354e1e9a8faf5a','SANTOSH KU UPI/SANTOSH KU/XX7468@fbpe/Pay to Bha/FEDERAL BA/630375058922/ICIb3067c893064421f91565a5','RAM SINGH UPI/RAM SINGH/XXtfj2@ptys/UPI/YES BANK L/666836237876/ICI17a393ba8a5346429616aea5f','UPI/MUNAVATH V/XX02-6@axl/UPI/State Bank/630291986097/ICIdaa5b761f9b54ccda94851']){
  const e={vendor:'Common Vendor',particulars,type:'variable',ledger:'Fittings'},r=present(e);assert.equal(r.particulars,'Particulars need review');assert.equal(r.particularsNeedReview,true);assert.equal(r.vendor,'Common Vendor');assert.equal(r.type,'variable');assert.equal(r.category,'Fittings');assert.equal(e.particulars,particulars);
 }
});
test('genuine saved app particulars, type and category survive, including normal UPI wording and product references',()=>{
 const e=Object.freeze({vendor:'Indigo',particulars:'Flight tickets paid via UPI for spiritual travel',productType:'Travel booking',type:'variable',ledger:'Spiritual Travelling'});assert.equal(present(e).particulars,e.particulars);assert.equal(present(e).type,'Travel booking');assert.equal(present(e).particularsNeedReview,false);assert.equal(isBankNarration('Product SKU 630336018188, two fittings'),false);assert.equal(isBankNarration('POS/ACME terminal replacement'),false);
});
test('only a separate saved app field can supply missing particulars; bank payee and category cannot invent them',()=>{
 const e={vendor:'Carpenter',particulars:'UPI/CARPENTER/123456789012',ledger:'Furniture',telegramNarration:'Dining chair repair'};assert.equal(present(e).particulars,'Dining chair repair');assert.equal(present({...e,telegramNarration:'',productDescription:'Dining table legs'}).particulars,'Dining table legs');assert.equal(present({...e,telegramNarration:''}).particulars,'Particulars need review');assert.equal(present({vendor:'UPI/Mr SAJID/123456789012'}).vendor,'Vendor not specified');
});
test('bank ACH/IMPS references are suppressed while expense metadata and raw supporting evidence stay unchanged',()=>{
 for(const value of ['ACH DR INW PAY/0000160044258/ HDFC BANK LIMITED','MMT/IMPS/630521866735/RAJU SUREK/HDFC0000368','NEFT CR-HDFC BANK REF 123456789012'])assert.equal(isBankNarration(value),true,value);
 const bank='Mr SAJID UPI/Mr SAJID/123456789012',expense={id:'EX-1',nature:'PERSONAL',particulars:bank,vendor:'Common Vendor',type:'variable',ledger:'Fittings',date:'2026-10-30',amount:1000,reconciliationSource:{bankDescription:bank,bankReference:'123456789012'}};const store={expenses:{'EX-1':expense}},before=JSON.stringify(store);const row={id:'EX-1/PAY-001',reference:'EX-1/PAY-001',entity:'PERSONAL',kind:'expense',date:'2026-10-30',amount:1000};const r=enrich(store,row);assert.equal(r.appExpense.particulars,'Particulars need review');assert.equal(r.amount,1000);assert.equal(r.date,row.date);assert.match(r.appExpense.searchNarration,/UPI/);assert.equal(JSON.stringify(store),before);
});
