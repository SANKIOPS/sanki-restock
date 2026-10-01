'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const temp=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-credit-cards-'));
process.env.DATA_PATH=path.join(temp,'data.json');
const {router,merchantKey,inferClassification}=require('../modules/credit-cards');
const expenseRouter=require('../modules/expenses').router;
test.after(()=>fs.rmSync(temp,{recursive:true,force:true}));
function invoke(method,routePath,{body={},params={},query={},role='owner'}={}){const layer=router.stack.find(x=>x.route&&x.route.path===routePath&&x.route.methods[method.toLowerCase()]);assert.ok(layer,'route exists '+method+' '+routePath);let status=200,result;const req={body,params,query,user:{username:'tester',role,roles:[role]}};const res={status(n){status=n;return this;},json(x){result=x;return this;},end(){return this;}};let i=0;const next=()=>{const h=layer.route.stack[i++];if(h)h.handle(req,res,next);};next();return{status,body:result};}
function invokeExpense(method,routePath,{body={},params={},query={},role='owner',username='tester'}={}){const layer=expenseRouter.stack.find(x=>x.route&&x.route.path===routePath&&x.route.methods[method.toLowerCase()]);assert.ok(layer,'expense route exists '+method+' '+routePath);let status=200,result;const req={body,params,query,user:{username,role,roles:[role]}};const res={status(n){status=n;return this;},json(x){result=x;return this;},end(){return this;}};let i=0;const next=()=>{const h=layer.route.stack[i++];if(h)h.handle(req,res,next);};next();return{status,body:result};}
test('credit cards are entity-neutral liabilities with permanent statement review logs',()=>{
  const made=invoke('POST','/api/expenses/credit-cards',{body:{name:'HDFC Regalia',last4:'1234',cardholder:'Owner',issuingBank:'HDFC',cycleDay:5,dueDay:25,creditLimit:200000,openingOutstanding:1000}});
  assert.equal(made.status,200);assert.equal(made.body.card.displayName,'HDFC Regalia 1234');assert.equal(made.body.card.outstanding,1000);assert.equal(made.body.card.nature,undefined);
  const manual=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:made.body.card.id,date:'2026-08-27',narration:'Swiggy order Delhi',amount:500,classification:'expense'}});
  assert.equal(manual.status,200);assert.equal(manual.body.statement.status,'review');assert.equal(manual.body.statement.rows[0].suggestedCategory,'FOOD EXPENSE');assert.equal(manual.body.statement.originalName,'Manual entry');
  const row=manual.body.statement.rows[0];const reviewed=invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:manual.body.statement.id},body:{rows:[{id:row.id,classification:'expense',category:'FOOD EXPENSE',nature:'SANKI',channel:'Both',type:'marketing',merchant:'Merchant name',confirmed:true}]}});
  assert.equal(reviewed.status,200);assert.equal(reviewed.body.statement.rows[0].confirmed,true);
  const finalized=invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:manual.body.statement.id}});
  assert.equal(finalized.status,200);assert.equal(finalized.body.outstanding,1500);
  const expenses=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8'));const posting=expenses.reconciliationExpenses.find(x=>x.creditCardStatementId===manual.body.statement.id);
  assert.equal(posting.category,'FOOD EXPENSE');assert.equal(posting.nature,'SANKI');assert.equal(posting.channel,'Both');assert.equal(posting.type,'marketing');assert.equal(posting.vendor,'Merchant name');assert.equal(posting.account,'HDFC Regalia 1234');
  const logs=invoke('GET','/api/expenses/credit-cards/statements').body.statements;assert.equal(logs[0].status,'finalized');assert.equal(logs[0].fileUrl,'','manual logs do not expose a broken source-file link');
});
test('Owner can edit card cycle and due days without changing finalized statements',()=>{
  const before=invoke('GET','/api/expenses/credit-cards').body.cards[0],logsBefore=invoke('GET','/api/expenses/credit-cards/statements').body.statements.map(x=>({id:x.id,periodFrom:x.periodFrom,periodTo:x.periodTo,status:x.status}));
  const updated=invoke('POST','/api/expenses/credit-cards',{body:{id:before.id,name:before.name,last4:before.last4,cardholder:before.cardholder,issuingBank:before.issuingBank,cycleDay:18,dueDay:7,creditLimit:before.creditLimit,openingOutstanding:before.openingOutstanding,ownerOnly:before.ownerOnly,active:true}});
  assert.equal(updated.status,200);assert.equal(updated.body.card.cycleDay,18);assert.equal(updated.body.card.dueDay,7);assert.equal(updated.body.card.outstanding,before.outstanding);
  const logsAfter=invoke('GET','/api/expenses/credit-cards/statements').body.statements.map(x=>({id:x.id,periodFrom:x.periodFrom,periodTo:x.periodTo,status:x.status}));assert.deepEqual(logsAfter,logsBefore);
});
test('confirmed merchant categories are remembered but future rows remain unconfirmed',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0];const first=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-08-28',narration:'Cafe Blue Saket',amount:250}}).body.statement,row=first.rows[0];
  invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:first.id},body:{rows:[{id:row.id,classification:'expense',category:'FOOD EXPENSE',nature:'SANKI',channel:'Website',type:'running',confirmed:true}]}});invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:first.id}});
  const next=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-08-29',narration:'Cafe Blue Saket',amount:300}}).body.statement.rows[0];
  assert.equal(next.category,'FOOD EXPENSE');assert.equal(next.confirmed,false);assert.ok(next.suggestedRule);
});
test('whole-card payment debits the bank while statement accounting controls liability',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0],before=card.outstanding;
  const paid=invoke('POST','/api/expenses/credit-cards/payments',{body:{cardId:card.id,nature:'SANKI',account:'Axis Bank 3448',paymentKind:'partial',amount:400,date:'2026-08-30',reference:'UTR400'}});
  assert.equal(paid.status,200);assert.equal(paid.body.outstanding,before);
  const expenses=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8')),transfer=expenses.transfers.find(x=>x.creditCardPaymentId===paid.body.payment.id);
  assert.equal(transfer.fromAccount,'Axis Bank 3448');assert.equal(transfer.toAccount,'HDFC Regalia 1234');assert.equal(transfer.classification,'credit_card_payment');
  const ledger=invoke('GET','/api/expenses/credit-cards/:id/ledger',{params:{id:card.id}}).body;assert.equal(ledger.outstanding,paid.body.outstanding);assert.equal(ledger.entries.some(x=>x.id===paid.body.payment.id),false);
});
test('mixed-use card payment moves one bank amount and records entity ownership separately',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0];
  const paid=invoke('POST','/api/expenses/credit-cards/payments',{body:{cardId:card.id,nature:'SANKI',account:'Axis Bank 3448',paymentKind:'full',amount:1000,date:'2026-09-01',allocations:[{nature:'SANKI',amount:500},{nature:'SAMAST',amount:300},{nature:'PERSONAL',amount:200}]}});
  assert.equal(paid.status,200);const expenses=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8')),transfers=expenses.transfers.filter(x=>x.creditCardPaymentId===paid.body.payment.id),allocations=expenses.creditCardSettlementAllocations.filter(x=>x.creditCardPaymentId===paid.body.payment.id);
  assert.equal(transfers.length,1);assert.equal(transfers[0].amount,1000);assert.deepEqual(allocations.map(x=>[x.beneficiaryNature,x.amount,x.status]),[['SANKI',500,'self'],['SAMAST',300,'due'],['PERSONAL',200,'due']]);
});
test('mixed-use card payment rejects an allocation that does not equal the bank payment',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0],before=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8')).transfers.length;
  const paid=invoke('POST','/api/expenses/credit-cards/payments',{body:{cardId:card.id,nature:'SANKI',account:'Axis Bank 3448',amount:1000,date:'2026-09-02',allocations:[{nature:'SANKI',amount:500},{nature:'PERSONAL',amount:200}]}});
  assert.equal(paid.status,400);assert.equal(JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8')).transfers.length,before);
});
test('legacy logged purchases do not double count statement card liability',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0],before=card.outstanding;
  const made=invokeExpense('POST','/api/expenses',{role:'admin',body:{date:'2026-09-11',amount:321,particulars:'Shoot accessory',nature:'SANKI',ledger:'OFFICE EXP',type:'variable',vendor:'Amazon',paymentType:'Credit',paidAlready:true,personalAccount:card.id,personalPaymentProof:'/card-proof.jpg',billPhoto:'/bill.jpg'}});
  assert.equal(made.status,200);assert.equal(made.body.expense.creditCardId,card.id);assert.equal(made.body.expense.payments[0].creditCardId,card.id);assert.equal(made.body.expense.payments[0].account,card.displayName);assert.equal(made.body.expense.payments[0].personalFunds,false);
  const stored=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8'));stored.expenses[made.body.expense.id].status='approved';stored.expenses[made.body.expense.id].approvedAt='2026-09-11T00:00:00.000Z';fs.writeFileSync(path.join(temp,'expenses.json'),JSON.stringify(stored));
  const after=invoke('GET','/api/expenses/credit-cards').body.cards.find(x=>x.id===card.id);assert.equal(after.outstanding,before);
  const ledger=invoke('GET','/api/expenses/credit-cards/:id/ledger',{params:{id:card.id}}).body;assert.equal(ledger.entries.some(x=>x.id===made.body.expense.id+'/PAY-001'),false);
});
test('Prashant can select an accessible card and log a credit-card expense without broad admin access',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0];
  const config=invokeExpense('GET','/api/expenses/config',{role:'claimant',username:'prashant'});
  assert.equal(config.status,200);assert.equal(config.body.isAdmin,false);assert.equal(config.body.canLogCreditCardExpense,true);assert.ok(config.body.creditCards.some(x=>x.id===card.id));
  const made=invokeExpense('POST','/api/expenses',{role:'claimant',username:'prashant',body:{date:'2026-09-26',amount:275,particulars:'Admin card purchase',nature:'SANKI',vendor:'Office supplier',paymentType:'Credit',paidAlready:true,creditCardId:card.id,personalAccount:card.id,personalPaymentProof:'/card-proof-prashant.jpg',billPhoto:'/bill-prashant.jpg'}});
  assert.equal(made.status,200);assert.equal(made.body.expense.creditCardId,card.id);assert.equal(made.body.expense.payments[0].account,card.displayName);assert.equal(made.body.expense.payments[0].personalFunds,false);assert.equal(made.body.expense.status,'pending');
});
test('an unpaid credit expense retains the selected card for its later payment',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0];
  const made=invokeExpense('POST','/api/expenses',{body:{date:'2026-09-11',amount:789,particulars:'Equipment awaiting payment',nature:'SANKI',ledger:'OFFICE EXP',type:'variable',vendor:'Amazon',paymentType:'Credit',paidAlready:false,creditCardId:card.id,billPhoto:'/bill-pending.jpg'}});
  assert.equal(made.status,200);assert.equal(made.body.expense.creditCardId,card.id);assert.equal(made.body.expense.paidAmount,0);assert.deepEqual(made.body.expense.payments,[]);assert.equal(made.body.expense.status,'pending');
});
test('statement payment is authoritative without a second bank transfer or duplicate decision',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0],before=card.outstanding,expBefore=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8')),transfersBefore=expBefore.transfers.length;
  const st=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-08-30',narration:'Card payment Axis Bank 3448',amount:400,classification:'card_payment'}}).body.statement,row=st.rows[0];
  assert.ok(row.duplicateWarnings.some(x=>x.kind==='card_payment'));
  const reviewed=invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:st.id},body:{rows:[{id:row.id,classification:'card_payment',duplicateResolution:'link',confirmed:true}]}});assert.equal(reviewed.status,200);
  const finalized=invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:st.id}});assert.equal(finalized.status,200);assert.equal(finalized.body.outstanding,before-400);
  const expAfter=JSON.parse(fs.readFileSync(path.join(temp,'expenses.json'),'utf8'));assert.equal(expAfter.transfers.length,transfersBefore);
});
test('owner can reopen a finalized statement with a reason while retaining its log',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0],st=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-08-30',narration:'Reopen test expense',amount:99}}).body.statement,row=st.rows[0];
  invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:st.id},body:{rows:[{id:row.id,classification:'expense',category:'OFFICE EXP',nature:'SANKI',channel:'Both',type:'marketing',merchant:'Merchant name',confirmed:true}]}});invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:st.id}});
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
test('expenses UI exposes credit cards, statement logs, review and merchant learning',()=>{const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');assert.match(html,/data-t="creditcards"/);assert.match(html,/value="Credit">Credit Card/);assert.match(html,/Select credit card used/);assert.match(html,/Credit card to use/);assert.match(html,/Add or manage credit cards/);assert.match(html,/populatePaymentSource/);assert.match(html,/cfg\.creditCards/);assert.match(html,/Statement Logs/);assert.match(html,/Merchant rules/);assert.match(html,/Finalize and post to ledgers/);assert.doesNotMatch(html,/class="cc_dup"/);assert.match(html,/Full payment/);assert.match(html,/Reopen with reason/);assert.doesNotMatch(html,/Optional bill\/proof URL/);assert.match(html,/<th>Merchant<\/th>/);assert.match(html,/<th>Type<\/th>/);assert.match(html,/id="cc_password"/);assert.match(html,/password is used once.*never saved/i);assert.match(html,/fd\.append\('password'/);assert.match(html,/Edit card settings/);assert.match(html,/Statement cycle day/);assert.match(html,/Payment due day/);assert.match(html,/Existing statement dates were not changed/);});
test('credit-card router mounts before the generic expense-id route',()=>{const server=fs.readFileSync(path.join(__dirname,'..','server.js'),'utf8'),cards=server.indexOf("require('./modules/credit-cards').router"),expenses=server.indexOf("require('./modules/expenses').router");assert.ok(cards>=0&&expenses>=0&&cards<expenses);});

test('review rejects classifications opposite to the statement direction and finalizes without row confirmations',()=>{
  const card=invoke('GET','/api/expenses/credit-cards').body.cards[0];
  const st=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-10-01',narration:'New payment',amount:50,classification:'card_payment'}}).body.statement;
  const params={id:st.id},id=st.rows[0].id;
  assert.equal(invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params,body:{rows:[{id,classification:'expense'}]}}).status,400);
  assert.equal(invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params,body:{rows:[{id,classification:'card_payment'}]}}).status,200);
  assert.equal(invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params}).status,200);
});

test('merchant cleanup and statement CR/DR totals preserve recognizable names and paise',()=>{
  const {merchantName,totals}=require('../modules/credit-card-accounting');
  for(const text of ['RAZ*Facebook IndiaGurugram','EMIFACEBOOK SIGURGAON','facebook.com Gurgaon'])assert.equal(merchantName(text,'HDFC'),'Facebook');
  assert.equal(merchantName('IND*LINKEDIN (PGSI)WWW.LINKED'),'LinkedIn');
  for(const text of ['FIRST YEAR MEMBERSHIPFEE','HDFC statement amount-due rounding','IGST-VPS2724916693596-RATE 18.0'])assert.equal(merchantName(text,'HDFC BANK'),'HDFC Bank');
  assert.deepEqual(totals([{debit:1536.12,credit:0},{debit:0,credit:3},{debit:0,credit:.09}]),{debit:1536.12,credit:3.09,netMovement:1533.03});
});
test('finalized card expenses appear in search, vendor ledgers, spending and profit exactly once',()=>{
  const {summaryForPL}=require('../modules/expenses');
  const card=invoke('POST','/api/expenses/credit-cards',{body:{name:'Reporting card',last4:'9999',issuingBank:'HDFC'}}).body.card;
  const make=(classification,amount,narration,type='marketing')=>{
    const st=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-10-15',amount,narration,classification}}).body.statement;
    const reviewed=invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:st.id},body:{rows:[{id:st.rows[0].id,classification,nature:'SANKI',category:'MARKETING EXPENSE',type,channel:'Both'}]}});assert.equal(reviewed.status,200);
    assert.equal(invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:st.id}}).status,200);return st;
  };
  const purchase=make('expense',1000,'RAZ*Facebook IndiaGurugram');make('refund',100,'FACEBOOKGURGAON');make('card_payment',200,'PAYMENT THANK YOU');make('emi_principal',300,'EMIRAZ*Facebook IndiaGurugram');make('emi_interest',20,'EMI INTEREST','running');
  const list=invokeExpense('GET','/api/expenses/list',{query:{from:'2026-10-15',to:'2026-10-15',search:'Facebook'}}).body;
  assert.equal(list.expenses.length,2);assert.equal(list.totals.all,900);assert.ok(list.expenses.every(e=>e.statementBacked&&e.readOnly&&e.vendor==='Facebook'&&e.paymentType==='Credit'));
  assert.equal(invokeExpense('GET','/api/expenses/list',{query:{from:'2026-10-15',to:'2026-10-15',type:'marketing'}}).body.totals.all,900);
  const vendor=invokeExpense('GET','/api/expenses/vendors',{query:{nature:'SANKI',search:'Facebook',from:'2026-10-15',to:'2026-10-15'}}).body.vendors.find(v=>v.name==='Facebook');
  assert.ok(vendor.tags.includes('Credit-card merchant'));assert.equal(vendor.billed,900);assert.equal(vendor.outstanding,0);
  const spending=invokeExpense('GET','/api/expenses/spending-dashboard',{query:{from:'2026-10-15',to:'2026-10-15'}}).body;
  assert.equal(spending.totalPaid,920);assert.ok(spending.payments.every(e=>e.kind==='Credit Card'));
  const pl=summaryForPL('2026-10-15','2026-10-15');assert.equal(pl.Shared.marketing,900);assert.equal(pl.Shared.running,20);
  assert.equal(invokeExpense('GET','/api/expenses/list',{role:'claimant',username:'someone',query:{search:purchase.id}}).body.expenses.length,0);
});
test('unbilled transactions are replaced by billed rows without doubling reports and restored on reopening',()=>{
  const {summaryForPL}=require('../modules/expenses');
  const card=invoke('POST','/api/expenses/credit-cards',{body:{name:'Unbilled card',last4:'8888',issuingBank:'HDFC'}}).body.card;
  const provisional=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,kind:'unbilled',date:'2026-10-20',narration:'Facebook IndiaGurugram',amount:650,classification:'expense'}}).body.statement;
  assert.equal(provisional.kind,'unbilled');assert.equal(provisional.totals.debit,650);
  invoke('POST','/api/expenses/credit-cards/statements/:id/review',{params:{id:provisional.id},body:{rows:[{id:provisional.rows[0].id,classification:'expense',category:'MARKETING EXPENSE',type:'marketing',nature:'SANKI',channel:'Website'}]}});
  assert.equal(invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:provisional.id}}).status,200);
  assert.equal(summaryForPL('2026-10-20','2026-10-20').Website.marketing,650);
  const billed=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-10-20',narration:'RAZ*Facebook IndiaGurugram (Ref# 12345)',amount:650,classification:'expense'}}).body.statement;
  assert.equal(billed.rows[0].replaces.statementId,provisional.id);assert.equal(billed.rows[0].type,'marketing');
  assert.equal(invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:billed.id}}).status,200);
  let rows=invokeExpense('GET','/api/expenses/list',{query:{from:'2026-10-20',to:'2026-10-20'}}).body.expenses;
  assert.equal(rows.length,1);assert.equal(rows[0].creditCardStatementId,billed.id);assert.equal(rows[0].unbilled,false);
  assert.equal(summaryForPL('2026-10-20','2026-10-20').Website.marketing,650);
  assert.equal(invoke('GET','/api/expenses/credit-cards/:id/ledger',{params:{id:card.id}}).body.outstanding,650);
  const repeat=invoke('POST','/api/expenses/credit-cards/statements/manual',{body:{cardId:card.id,date:'2026-10-20',narration:'RAZ*Facebook IndiaGurugram (Ref# 12345)',amount:650,classification:'expense'}}).body.statement;
  assert.ok(repeat.rows[0].duplicateOf);assert.equal(invoke('POST','/api/expenses/credit-cards/statements/:id/finalize',{params:{id:repeat.id}}).status,200);
  assert.equal(summaryForPL('2026-10-20','2026-10-20').Website.marketing,650);
  invoke('POST','/api/expenses/credit-cards/statements/:id/reopen',{params:{id:billed.id},body:{reason:'Correct generated statement'}});
  rows=invokeExpense('GET','/api/expenses/list',{query:{from:'2026-10-20',to:'2026-10-20'}}).body.expenses;assert.equal(rows.length,1);assert.equal(rows[0].unbilled,true);
});
test('current-cycle screenshot text imports purchases and refunds without balance columns',()=>{
  const {parseUnbilledTransactions}=require('../modules/credit-card-accounting');
  const rows=parseUnbilledTransactions('Current cycle\n01 Oct 2026 Facebook India Gurgaon\n₹1,536.12 DR\n02/10/2026 LinkedIn refund\nINR 100.00 CR\n2026-10-03 Shopify\nRs. 250.00');
  assert.equal(rows.length,3);assert.equal(rows[0].debit,1536.12);assert.equal(rows[1].credit,100);assert.equal(rows[2].debit,250);assert.equal(rows[0].date,'2026-10-01');
  assert.equal(parseUnbilledTransactions('31/02/2026 Facebook ₹20.00 DR').length,0);
});
