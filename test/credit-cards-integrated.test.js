'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');

const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-credit-card-'));
process.env.DATA_PATH=path.join(dir,'data.json');
const {router}=require('../modules/credit-cards');
test.after(()=>fs.rmSync(dir,{recursive:true,force:true}));

function invoke(method,routePath,{params={},query={},body={}}={}){
  const layer=router.stack.find(item=>item.route&&item.route.path===routePath&&item.route.methods[method.toLowerCase()]);
  assert.ok(layer,`route exists: ${method} ${routePath}`);
  const req={params,query,body,user:{username:'owner-user',roles:['owner']}};let status=200,result;
  const res={status(code){status=code;return this;},json(value){result=value;return this;},end(){return this;}};
  layer.route.stack.at(-1).handle(req,res);return{status,body:result};
}

test('logged card expenses increase liability and link to a statement without duplication',()=>{
  const card={id:'CC-0001',name:'HDFC Regalia',last4:'4321',openingOutstanding:0,active:true};
  fs.writeFileSync(path.join(dir,'credit-cards.json'),JSON.stringify({cards:{[card.id]:card},statements:{'CCS-00001':{id:'CCS-00001',cardId:card.id,status:'review',rows:[{id:'CCR-1',date:'2026-08-29',narration:'CARD VENDOR',amount:321,classification:'expense',confirmed:true,duplicateWarnings:[{kind:'expense',id:'EX-CARD'}],duplicateResolution:'link',linkedExpenseId:'EX-CARD'}]}},payments:[],merchantRules:{},audit:[]}));
  fs.writeFileSync(path.join(dir,'expenses.json'),JSON.stringify({expenses:{'EX-CARD':{id:'EX-CARD',nature:'SANKI',date:'2026-08-29',vendor:'Card Vendor',particulars:'Card purchase',amount:321,status:'paid',approvedAt:'2026-08-29T10:00:00.000Z',payments:[{id:'PAY-001',date:'2026-08-29',amount:321,account:'HDFC Regalia 4321',creditCardId:card.id,paymentType:'Credit'}]}},reconciliationExpenses:[],transfers:[],transferSeq:0}));
  const before=invoke('GET','/api/expenses/credit-cards').body;assert.equal(before.cards[0].outstanding,321);
  const finalized=invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:'CCS-00001'}});assert.equal(finalized.status,200,JSON.stringify(finalized.body));assert.equal(finalized.body.outstanding,321);
  const expenses=JSON.parse(fs.readFileSync(path.join(dir,'expenses.json'),'utf8'));assert.equal(expenses.reconciliationExpenses.length,0);assert.equal(expenses.expenses['EX-CARD'].payments[0].creditCardStatementId,'CCS-00001');
  const ledger=invoke('GET','/api/expenses/credit-cards/:id/ledger',{params:{id:card.id}}).body;assert.equal(ledger.outstanding,321);assert.equal(ledger.entries.filter(row=>row.expenseId==='EX-CARD').length,1);
});
