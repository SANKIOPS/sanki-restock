'use strict';
const {test,after}=require('node:test'),assert=require('node:assert/strict'),fs=require('fs'),os=require('os'),path=require('path');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'counter-cash-order-'));
process.env.DATA_PATH=path.join(dir,'data.json');
process.env.ORDERS_PATH=path.join(dir,'orders.json');
const expenseFile=path.join(dir,'expenses.json');
fs.writeFileSync(expenseFile,JSON.stringify({expenses:{},oneTimeMigrations:{}}));
const {router}=require('../modules/expenses');
function get(route,query){let status=200,result;const layer=router.stack.find(l=>l.route?.path===route&&l.route.methods.get);layer.route.stack.at(-1).handle({query,user:{role:'owner',roles:['owner'],username:'owner'},params:{},headers:{},get(){return '';}},{status(n){status=n;return this;},json(x){result=x;}});assert.equal(status,200,JSON.stringify(result));assert.ok(result.success,JSON.stringify(result));return result;}
after(()=>fs.rmSync(dir,{recursive:true,force:true}));

test('recovered Shopify POS cash order 2951 credits rounded Counter Cash once',()=>{
  get('/api/expenses/config',{});
  const state=JSON.parse(fs.readFileSync(expenseFile));
  Object.assign(state,{expenses:{},transfers:[],receipts:[],adjustments:[],bankTruthMovements:[],bankDateOverrides:{},bankReconciliationLinks:{},bankReconciliationDrafts:{},openingBalances:{'Counter Cash':0},openingBalanceDates:{},bankStatements:{}});
  fs.writeFileSync(expenseFile,JSON.stringify(state));
  fs.writeFileSync(process.env.ORDERS_PATH,JSON.stringify({orders:{'shopify-2951':{
    id:'shopify-2951',name:'#2951',orderNumber:2951,channel:'POS',financialStatus:'paid',
    createdAt:'2026-11-11T21:45:00+05:30',processedAt:'2026-11-11T21:45:00+05:30',
    paymentGateways:['Cash'],total:3598,refundAmount:0,moneyRefunded:0,
    paymentTransactions:[{id:'cash-2951',kind:'sale',status:'success',gateway:'Cash',amount:3598,processedAt:'2026-11-11T21:45:00+05:30'}]
  }}}));
  const ledger=get('/api/expenses/account-ledger',{nature:'SANKI',account:'Counter Cash',from:'2026-11-11',to:'2026-11-11'});
  const rows=ledger.entries.filter(row=>row.id==='SHOPIFY/shopify-2951');
  assert.equal(rows.length,1);
  assert.equal(rows[0].credit,3600);
  assert.equal(rows[0].debit,0);
});
