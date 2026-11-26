'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const quick=require('../modules/quick-financial-search');

function fixture(){return{
  expenses:{
    'EX-00747':{id:'EX-00747',date:'2026-11-08',nature:'PERSONAL',status:'paid',amount:1250,vendor:'Home Store',particulars:'Cotton bedsheet',ledger:'Props Expenses',categoryGroup:'Content & Marketing',billNote:'October bedroom purchase',billPhoto:'/api/expenses/photo/bedsheet-bill.jpg',payments:[{id:'PAY-001',date:'2026-11-08',amount:1250,account:'ICICI Bank 0993',reference:'UPI-BEDSHEET',proof:'/api/expenses/photo/bedsheet-payment.jpg'}]},
    'EX-HIDDEN':{id:'EX-HIDDEN',date:'2026-11-09',nature:'PERSONAL',status:'paid',amount:900,vendor:'Private hidden',particulars:'Never disclose',payments:[{id:'PAY-001',date:'2026-11-09',amount:900,account:'Hidden 0001'}]}
  },reconciliationExpenses:[],
  transfers:[
    {id:'TR-001',date:'2026-11-10',fromNature:'PERSONAL',toNature:'SANKI',fromAccount:'ICICI Bank 0993',toAccount:'Axis Bank 3448',amount:300000,classification:'owner_contribution',note:'Business funding'},
    {id:'TR-002',date:'2026-11-20',fromNature:'PERSONAL',toNature:'SANKI',fromAccount:'ICICI Bank 0993',toAccount:'Axis Bank 3448',amount:200000,classification:'owner_contribution',note:'Second business funding'},
    {id:'TR-003',date:'2026-11-21',fromNature:'SANKI',toNature:'PERSONAL',fromAccount:'Axis Bank 3448',toAccount:'ICICI Bank 0993',amount:10000,classification:'owner_withdrawal',note:'Reverse direction'}
  ]
};}
function harness(){let reads=0;const store=fixture(),service=quick.create({read:()=>{reads+=1;return{store};},canSearch:req=>req.user.allowed!==false,natures:req=>req.user.natures||['SANKI','PERSONAL'],canPersonal:req=>req.user.personal===true,accountVisible:(req,account)=>!String(account).startsWith('Hidden'),canViewExpense:(req,expense)=>expense.id!=='EX-HIDDEN'});return{store,service,reads:()=>reads,user:{allowed:true,personal:true,natures:['SANKI','PERSONAL']}};}

test('account-pair search returns an instant all-period total and only the requested direction',()=>{
  const h=harness(),before=JSON.stringify(h.store),result=h.service.search({user:h.user},{q:'0993 to 3448',kind:'all'});
  assert.deepEqual(result.pair,{from:'0993',to:'3448'});assert.equal(result.summary.transferCount,2);assert.equal(result.summary.totalTransferred,500000);assert.deepEqual(result.transfers.map(row=>row.id),['TR-002','TR-001']);assert.equal(result.expenses.length,0);assert.equal(JSON.stringify(h.store),before);assert.equal(h.reads(),1);
});

test('date filter narrows the pair total without scanning statements manually',()=>{
  const h=harness(),result=h.service.search({user:h.user},{q:'0993 → 3448',kind:'transfer',from:'2026-11-15',to:'2026-11-30'});assert.equal(result.summary.transferCount,1);assert.equal(result.summary.totalTransferred,200000);assert.equal(result.transfers[0].id,'TR-002');
});

test('partially typed account routes and ordinary related words find transfers without exact syntax',()=>{
  const h=harness();for(const query of ['0993 to','0993 business','ICICI 3448','TR-001']){const result=h.service.search({user:h.user},{q:query,kind:'transfer'});assert.ok(result.transfers.some(row=>row.id==='TR-001'),query);}
});

test('expense search finds reference, vendor, narration, category, account and amount',()=>{
  const h=harness();for(const query of ['EX-00747','Home Store','cotton bedsheet','Content & Marketing','Props Expenses','0993','1250','UPI-BEDSHEET','bedroom purchase','bedsheet-bill','bedsheet-payment']){const result=h.service.search({user:h.user},{q:query,kind:'expense'});assert.deepEqual(result.expenses.map(row=>row.id),['EX-00747'],query);assert.equal(result.summary.totalExpenses,1250);assert.equal(result.expenses[0].group,'Content & Marketing');}
});

test('expense group filter shows grouped spending without a text search',()=>{
  const h=harness(),options=h.service.search({user:h.user},{q:'',kind:'expense'});assert.deepEqual(options.groups,['Content & Marketing']);assert.equal(options.expenses.length,0);
  const result=h.service.search({user:h.user},{q:'',kind:'expense',group:'Content & Marketing'});assert.equal(result.summary.expenseCount,1);assert.equal(result.summary.totalExpenses,1250);assert.equal(result.expenses[0].category,'Props Expenses');
});

test('permissions and account visibility fail closed without revealing private records',()=>{
  const h=harness();assert.throws(()=>h.service.search({user:{...h.user,allowed:false}},{q:'0993 to 3448'}),/access/i);assert.equal(h.service.search({user:{...h.user,personal:false}},{q:'0993 to 3448'}).summary.transferCount,0);assert.equal(h.service.search({user:h.user},{q:'Never disclose'}).summary.expenseCount,0);
});

test('route accepts only valid quick searches and stays read-only',()=>{
  const h=harness(),routes=[],router={get(route,handler){routes.push({route,handler});}};h.service.register(router);let status=200,body;routes[0].handler({user:h.user,query:{q:'0993 to 3448'}},{status(code){status=code;return this;},json(value){body=value;return this;}});assert.equal(status,200);assert.equal(body.summary.totalTransferred,500000);
  routes[0].handler({user:h.user,query:{q:'x'}},{status(code){status=code;return this;},json(value){body=value;return this;}});assert.equal(status,400);assert.match(body.error,/two characters/i);
});

test('shared sidebar exposes one global finder with flexible related-field search and safe local links',()=>{
  const script=fs.readFileSync(path.join(__dirname,'../public/sidebar.js'),'utf8'),expenses=fs.readFileSync(path.join(__dirname,'../modules/expenses.js'),'utf8');assert.match(script,/Find expense or transfer/);assert.match(script,/0993 to 3448/);assert.match(script,/Exact wording is not required/);assert.match(script,/qf-expense-group/);assert.match(script,/row\.group&&'Group: '/);assert.match(script,/\/api\/expenses\/quick-find/);assert.match(script,/Ctrl\/Command \+ K/);assert.match(script,/encodeURIComponent\(row\.id\)/);assert.doesNotMatch(script,/qf-results[^\n]*innerHTML/);assert.match(expenses,/read:\(\)=>\(\{store:JSON\.parse\(fs\.readFileSync\(EXP_PATH,'utf8'\)\)\}\)/);
});
