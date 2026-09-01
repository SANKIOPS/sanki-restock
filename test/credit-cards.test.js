'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const temp=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-credit-cards-'));
process.env.DATA_PATH=path.join(temp,'data.json');
const {router,merchantKey,inferClassification}=require('../modules/credit-cards');
test.after(()=>fs.rmSync(temp,{recursive:true,force:true}));
function invoke(method,routePath,{body={},params={},query={},role='owner'}={}){const layer=router.stack.find(x=>x.route&&x.route.path===routePath&&x.route.methods[method.toLowerCase()]);assert.ok(layer,'route exists '+method+' '+routePath);let status=200,result;const req={body,params,query,user:{username:'tester',role,roles:[role]}};const res={status(n){status=n;return this;},json(x){result=x;return this;},end(){return this;}};let i=0;const next=()=>{const h=layer.route.stack[i++];if(h)h.handle(req,res,next);};next();return{status,body:result};}
test('credit cards are entity-neutral liabilities with permanent statement review logs',()=>{
  const made=invoke('POST','/api/expenses/credit-cards',{body:{name:'HDFC Regalia',last4:'1234',cardholder:'Owner',issuingBank:'HDFC',cycleDay:5,dueDay:25,creditLimit:200000,openingOutstanding:1000}});
  assert.equal(made.status,200);assert.equal(made.body.card.displayName,'HDFC Regalia 1234');assert.equal(made.body.card.outstanding,1000);assert.equal(made.body.card.nature,undefined);
  const manual=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:made.body.card.id,date:'2026-08-27',narration:'Swiggy order Delhi',amount:500,classification:'expense'}});
  assert.equal(manual.status,200);assert.equal(manual.body.statement.status,'review');assert.equal(manual.body.statement.rows[0].suggestedCategory,'FOOD EXPENSE');assert.equal(manual.body.statement.originalName,'Manual entry');
  const row=manual.body.statement.rows[0];const reviewed=invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:manual.body.statement.id},body:{rows:[{id:row.id,classification:'expense',category:'FOOD EXPENSE',nature:'SANKI',channel:'POS',confirmed:true}]}});
  assert.equal(reviewed.status,200);assert.equal(reviewed.body.statement.rows[0].confirmed,true);
  const finalized=invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:manual.body.statement.id}});
  assert.equal(finalized.status,200);assert.equal(finalized.body.outstanding,1500);
  const expenses=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8'));const posting=expenses.reconciliationExpenses.find(x=>x.creditCardStatementId===manual.body.statement.id);
  assert.equal(posting.category,'FOOD EXPENSE');assert.equal(posting.nature,'SANKI');assert.equal(posting.channel,'POS');assert.equal(posting.account,'HDFC Regalia 1234');
  const logs=invoke('GET','/api/expenses/credit-cards/statements').body.statements;assert.equal(logs[0].status,'finalized');assert.equal(logs[0].fileUrl,'','manual logs do not expose a broken source-file link');
});
test('confirmed merchant categories are remembered but future rows remain unconfirmed',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0];const first=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-08-28',narration:'Cafe Blue Saket',amount:250}}).body.statement,row=first.rows[0];
  invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:first.id},body:{rows:[{id:row.id,classification:'expense',category:'FOOD EXPENSE',nature:'SANKI',channel:'Website',confirmed:true}]}});invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:first.id}});
  const next=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-08-29',narration:'Cafe Blue Saket',amount:300}}).body.statement.rows[0];
  assert.equal(next.category,'FOOD EXPENSE');assert.equal(next.confirmed,false);assert.ok(next.suggestedRule);
});
test('card payment reduces liability and debits only the linked bank ledger',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0],before=card.outstanding;
  const paid=invoke('POST','/api/expenses/credit-cards/payments',{body:{cardId:card.id,nature:'SANKI',account:'Axis Bank 3448',paymentKind:'partial',amount:400,date:'2026-08-30',reference:'UTR400'}});
  assert.equal(paid.status,200);assert.equal(paid.body.outstanding,before-400);
  const expenses=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8')),transfer=expenses.transfers.find(x=>x.creditCardPaymentId===paid.body.payment.id);
  assert.equal(transfer.fromAccount,'Axis Bank 3448');assert.equal(transfer.toAccount,'HDFC Regalia 1234');assert.equal(transfer.classification,'credit_card_payment');
  const ledger=invoke('GET','/api/expenses/credit-cards/:id/ledger',{params:{id:card.id}}).body;assert.equal(ledger.outstanding,paid.body.outstanding);assert.ok(ledger.entries.some(x=>x.id===paid.body.payment.id));
});
test('linked duplicate statement payment never creates a second bank transfer or liability reduction',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0],before=card.outstanding,expBefore=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8')),transfersBefore=expBefore.transfers.length;
  const st=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-08-30',narration:'Card payment Axis Bank 3448',amount:400,classification:'card_payment'}}).body.statement,row=st.rows[0];
  assert.ok(row.duplicateWarnings.some(x=>x.kind==='card_payment'));
  const reviewed=invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:st.id},body:{rows:[{id:row.id,classification:'card_payment',duplicateResolution:'link',confirmed:true}]}});assert.equal(reviewed.status,200);
  const finalized=invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:st.id}});assert.equal(finalized.status,200);assert.equal(finalized.body.outstanding,before);
  const expAfter=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8'));assert.equal(expAfter.transfers.length,transfersBefore);
});
test('owner can reopen a finalized statement with a reason while retaining its log',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0],st=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-08-30',narration:'Reopen test expense',amount:99}}).body.statement,row=st.rows[0];
  invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:st.id},body:{rows:[{id:row.id,classification:'expense',category:'OFFICE EXP',nature:'SANKI',channel:'POS',confirmed:true}]}});invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:st.id}});
  const reopened=invoke('POST','/api/expenses/credit-cards/statements/:id/reopen',{params:{id:st.id},body:{reason:'Correct the category'}});assert.equal(reopened.status,200);assert.equal(reopened.body.statement.status,'review');assert.equal(reopened.body.statement.reopenReason,'Correct the category');
  const exp=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8'));assert.equal(exp.reconciliationExpenses.some(x=>x.creditCardStatementId===st.id),false);
  const log=invoke('GET','/api/expenses/credit-cards/statements').body.statements.find(x=>x.id===st.id);assert.ok(log);assert.equal(log.status,'review');
});
test('merchant and transaction inference recognizes refunds, fees and EMI',()=>{
  assert.equal(merchantKey('UPI ZOMATO ORDER 12345'),'zomato order');
  assert.equal(inferClassification({description:'Annual card fee',debit:500}),'fee');
  assert.equal(inferClassification({description:'EMI interest',debit:100}),'emi_interest');
  assert.equal(inferClassification({description:'Merchant refund',credit:100}),'refund');
});
test('expenses UI exposes credit cards, statement logs, review and merchant learning',()=>{const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');assert.match(html,/data-t="creditcards"/);assert.match(html,/Statement Logs/);assert.match(html,/Merchant rules/);assert.match(html,/Finalize and post to ledgers/);assert.match(html,/Possible duplicate/);assert.match(html,/Full payment/);assert.match(html,/Reopen with reason/);assert.match(html,/Optional bill\/proof URL/);});
