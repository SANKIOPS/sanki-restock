'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const root=path.join(__dirname,'..');
const read=file=>fs.readFileSync(path.join(root,file),'utf8');

test('Results naming retains navigation identity, route and existing role visibility',()=>{
 const {visibleFor,MODULES}=require('../modules/module-registry');
 const results=visibleFor({username:'test-owner',role:'owner',roles:['owner']}).find(m=>m.key==='pl');
 assert.equal(results.title,'Results');assert.equal(results.href,'/pnl.html');assert.equal(results.section,'Accounts');
 assert.deepEqual(MODULES.find(m=>m.key==='pl').roles,['admin','accounting','owner']);
 for(const role of ['staff','claimant'])assert.equal(visibleFor({username:'test-'+role,role,roles:[role]}).some(m=>m.key==='pl'),false);
});

test('report branding and application cross-references use Results without renaming metrics or DOM keys',()=>{
 for(const file of ['public/pnl.html','public/pnl.js','public/pnl-corrections.js','public/pnl-order-corrections.js','public/accounting.html','public/expenses.html','public/salary.html','public/expense-loans.js','public/expense-refunds.js','public/index.html']){
  assert.doesNotMatch(read(file),/P&(?:amp;)?L|Profit &amp; loss|Profit & loss/i,file);
 }
 const html=read('public/pnl.html'),js=read('public/pnl.js');
 assert.match(html,/<title>Results · SANKI<\/title>/);assert.match(html,/<h1>Results<\/h1>/);
 for(const key of ['pnlCorrections','pnlOrderCorrections','pnlManualCorrections'])assert.ok(html.includes('id="'+key+'"'));
 for(const label of ['Management profit / loss','Gross profit on sales','Profit margin'])assert.ok(js.includes(label),label);
 assert.ok(js.includes("fetch('/api/pl/report?"));assert.ok(js.includes("location.href='/api/pl/report/export?"));
 assert.ok(read('public/accounting.html').includes('data-v="pl"'));
});

test('Results naming does not change accounting values, source facts or permission paths',()=>{
 const {buildReport}=require('../modules/pnl-report');
 const sources={orders:{orders:{O:{id:'O',name:'#SYNTHETIC',channel:'POS',financialStatus:'paid',createdAt:'2026-10-01',processedAt:'2026-10-01',taxesIncluded:true,total:1050,lineItems:[{id:'L',sku:'DEMO',qty:1,price:1050}]}}},shopifyCosts:{at:'2026-10-01T00:00:00Z',items:[{id:'I',sku:'DEMO',unitCost:200,currency:'INR'}]},expenses:{expenses:{E:{id:'E',nature:'SANKI',amount:100,paidAmount:100,status:'paid',payments:[{id:'P',date:'2026-10-02',amount:100}]}}}};
 const before=JSON.stringify(sources),report=buildReport(sources,{from:'2026-10-01',to:'2026-10-08'});
 for(const [key,value] of Object.entries({grossSales:1050,discounts:0,gst:50,netSales:1000,cogs:200,expenses:100,grossProfit:800,netProfit:700,complete:true}))assert.equal(report.totals[key],value,key);
 assert.equal(JSON.stringify(sources),before);
 const {userCanAccessPath}=require('../modules/auth-users');
 assert.equal(userCanAccessPath({username:'test-accounting',role:'accounting',roles:['accounting']},'/pnl.html'),true);
 assert.equal(userCanAccessPath({username:'test-revenue',role:'revenue',roles:['revenue']},'/pnl.html'),false);
});

test('Results workbook uses new display labels and download name while retaining metric and sheet structure',()=>{
 const XLSX=require('xlsx'),{buildReport}=require('../modules/pnl-report'),{exportWorkbook}=require('../modules/pnl-routes');
 const report=buildReport({}, {from:'2026-10-01',to:'2026-10-08'}),workbook=XLSX.read(exportWorkbook(report),{type:'buffer'});
 assert.deepEqual(workbook.SheetNames,['Summary','Transactions','SKU allocations','Tax','Collections','Warnings','Policy']);
 const rows=name=>XLSX.utils.sheet_to_json(workbook.Sheets[name],{header:1});
 assert.equal(rows('Summary')[0][0],'SANKI Results');
 assert.ok(rows('Summary').some(r=>r[0]==='Management profit / loss'));
 assert.ok(rows('Transactions')[0].includes('Results-calculated GST'));
 assert.ok(rows('Tax')[0].includes('Results included GST'));
 assert.doesNotMatch(JSON.stringify(Object.fromEntries(workbook.SheetNames.map(n=>[n,rows(n)]))),/P&(?:amp;)?L/);
 const routes=read('modules/pnl-routes.js');
 assert.ok(routes.includes('SANKI_Results_'));assert.ok(routes.includes("router.get('/api/pl/report/export'"));
});
