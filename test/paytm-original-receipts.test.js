'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const root=fs.mkdtempSync(path.join(__dirname,'.tmp-paytm-original-'));
process.env.DATA_PATH=path.join(root,'data.json');
process.env.SHOPIFY_STORE='synthetic-original-receipts.myshopify.com';
process.env.SHOPIFY_ACCESS_TOKEN='';
for(const [env,file] of Object.entries({ORDERS_PATH:'orders.json',SALES_PATH:'sales.json',PROCUREMENT_PATH:'procurement.json',INCENTIVES_PATH:'incentives.json',VELOCITY_REMITTANCE_PATH:'velocity.json'}))process.env[env]=path.join(root,file);
for(const file of ['expenses.json','orders.json','sales.json','procurement.json','incentives.json','velocity.json','salary.json','credit-cards.json'])fs.writeFileSync(path.join(root,file),'{}');
const {router}=require('../modules/expenses');
const {validateHistoricalSettlementAttachment,validatePayoutPosting}=require('../modules/paytm-accounting');
const baseline=JSON.parse(fs.readFileSync(path.join(root,'expenses.json'),'utf8'));
const copy=x=>JSON.parse(JSON.stringify(x)),current=()=>JSON.parse(fs.readFileSync(path.join(root,'expenses.json'),'utf8'));
function invoke(url,{method='GET',body={},query={},files=[],role='admin'}={}){
 const layer=router.stack.find(x=>x.route&&x.route.path===url&&x.route.methods[method.toLowerCase()]);assert.ok(layer,url);
 const out={status:200};layer.route.stack.at(-1).handle({body,query,files,user:{username:role==='admin'?'prashant':'synthetic-owner',role,roles:[role]},headers:{}},{status(n){out.status=n;return this;},json(value){out.body=value;return this;}});return out;
}
function setup(refund=0){
 const s=copy(baseline),orderId='9006001',transactionId='SYNTHETIC-600001',utr='SYNTHETIC-ORIGINAL';
 Object.assign(s,{expenses:{},receipts:[],transfers:[],adjustments:[],bankTruthMovements:[],reconciliationExpenses:[],paytmReportTransactions:{},paytmOrderLinks:{},paytmShopifyPayments:{},paytmRoundingReviews:{},paytmHistoricalEvidenceLinks:{},paytmVerifiedSettlements:[],paytmPayoutPostings:[],paytmSettlements:[],paytmExcludedTransactions:{},bankStatements:{},bankDateOverrides:{},saleAllocationOverrides:{},saleComponentAmountOverrides:{},openingBalances:{'Paytm Settlement Clearing':0,'Axis Bank 3448':0}});
 const order={id:orderId,number:6001,name:'#6001',channel:'POS',financialStatus:refund===100?'refunded':refund?'partially_refunded':'paid',total:100,moneyRefunded:refund,createdAt:'2026-09-04T12:00:00Z',paymentGateways:['Card Machine'],note:'600001 Synthetic',paymentTransactions:[{id:'SYNTHETIC-CAPTURE',kind:'sale',status:'success',gateway:'Card Machine',amount:100}],refundTransactions:refund?[{id:'SYNTHETIC-REFUND',parentId:'SYNTHETIC-CAPTURE',kind:'refund',status:'success',gateway:'Card Machine',amount:refund,processedAt:'2026-09-05T13:10:00Z'}]:[]};
 const tx={transactionId,date:'2026-09-04',amount:100,commission:2,gst:0,platformFee:0,settledAmount:98,payoutId:utr,utr,settledDate:'2026-09-05',transactionType:'ACQUIRING',isCustomerPayment:true};
 s.paytmReportTransactions[transactionId]=tx;s.paytmOrderLinks[transactionId]={transactionId,orderId,orderNumber:'6001',amount:100,transactionSuffix:'600001',reason:'Synthetic original receipt reviewed'};
 fs.writeFileSync(process.env.ORDERS_PATH,JSON.stringify({orders:{[orderId]:order}}));save(s);return{s,order,tx};
}
function save(s){fs.writeFileSync(path.join(root,'expenses.json'),JSON.stringify(s));}
const view=role=>invoke('/api/expenses/paytm-reports',{role}).body;
const finalize=()=>invoke('/api/expenses/paytm-reports/finalize-settlement',{method:'POST',body:{payoutId:'SYNTHETIC-ORIGINAL'}});
const clearing=()=>invoke('/api/expenses/account-ledger',{query:{nature:'SANKI',account:'Paytm Settlement Clearing',from:'2026-09-04',to:'2026-09-06'}}).body;
test.after(()=>fs.rmSync(root,{recursive:true,force:true}));

test('full and partial refunds preserve original receipt evidence without changing net operational sales or Shopify',()=>{
 for(const refund of [25,100]){
  setup(refund);const source=fs.readFileSync(process.env.ORDERS_PATH,'utf8');
  const before=clearing();const credits=before.entries.filter(x=>x.kind==='paytm_customer_receipt').reduce((n,x)=>n+Number(x.credit||0),0);
  assert.equal(credits,100-refund);
  const status=view();assert.equal(status.reconciliationStatus.counts.ready,1);
  assert.match(status.orderMatches[0].originalReceiptExplanation,/subsequent Shopify refund/);
  assert.equal(finalize().status,200);
  assert.equal(current().paytmVerifiedSettlements[0].receiptEvidence[0].subsequentRefundAmount,refund);
  assert.equal(fs.readFileSync(process.env.ORDERS_PATH,'utf8'),source);
  assert.deepEqual(current().receipts,[]);assert.deepEqual(current().reconciliationExpenses,[]);
  assert.equal(clearing().entries.filter(x=>x.kind==='paytm_customer_receipt').reduce((n,x)=>n+Number(x.credit||0),0),credits);
 }
});
test('missing or genuinely reduced original capture remains blocked, not excused as a refund',()=>{
 for(const paymentTransactions of [[],[{id:'SYNTHETIC-CAPTURE',kind:'sale',status:'success',gateway:'Card Machine',amount:50}]]){
  const {order}=setup(100);order.paymentTransactions=paymentTransactions;
  fs.writeFileSync(process.env.ORDERS_PATH,JSON.stringify({orders:{[order.id]:order}}));
  assert.equal(view().reconciliationStatus.counts.incomplete,1);assert.equal(finalize().status,400);
  assert.deepEqual(current().paytmVerifiedSettlements,[]);
 }
});

const rentalCsv='transaction_id,transaction_type,transaction_date,status,amount,commission,gst,settled_amount,payout_id,utr_no,settled_date,comments\nSYNTHETIC-RENT,VAS Deductions,2026-08-25,SUCCESS,352,0,0,352,SYNTHETIC-RENT-BATCH,SYNTHETIC-RENT-BATCH,2026-08-26,Recurring EDC rental\n';
function rentalSetup(){const {s}=setup();s.paytmReportTransactions['SYNTHETIC-RENT']={transactionId:'SYNTHETIC-RENT',date:'2026-08-25',amount:352,commission:0,gst:0,platformFee:0,settledAmount:352,payoutId:'SYNTHETIC-RENT-BATCH',utr:'SYNTHETIC-RENT-BATCH',settledDate:'2026-08-26',isCustomerPayment:true};s.paytmExcludedTransactions['SYNTHETIC-RENT']={reason:'Synthetic rental excluded from Shopify'};save(s);return s;}
function rentalPreview(){return invoke('/api/expenses/paytm-reports/preview',{method:'POST',files:[{originalname:'synthetic-rental.csv',buffer:Buffer.from(rentalCsv)}]});}
test('source re-import corrects the legacy derived customer flag, keeps exclusion history and audits exact before/after',()=>{
 rentalSetup();const preview=rentalPreview();assert.equal(preview.status,200);assert.equal(preview.body.enrichedTransactions,1);
 const out=invoke('/api/expenses/paytm-reports/confirm',{method:'POST',body:{draftId:preview.body.draftId}});assert.equal(out.status,200);
 const s=current();assert.equal(s.paytmReportTransactions['SYNTHETIC-RENT'].isCustomerPayment,false);assert.equal(s.paytmReportTransactions['SYNTHETIC-RENT'].transactionType,'VAS Deductions');
 assert.ok(s.paytmExcludedTransactions['SYNTHETIC-RENT']);assert.equal(s.paytmOrderLinks['SYNTHETIC-RENT'],undefined);
 assert.deepEqual(s.receipts,[]);assert.deepEqual(s.reconciliationExpenses,[]);
 assert.ok(JSON.stringify(s).includes('PAYTM_EVIDENCE_ENRICHED'));
 assert.equal(rentalPreview().body.enrichedTransactions,0);
});
test('source classification cannot silently rewrite an existing linked, rounded, verified or posted payment',()=>{
 for(const field of ['paytmOrderLinks','paytmRoundingReviews','paytmVerifiedSettlements','paytmPayoutPostings']){
  const s=rentalSetup();if(Array.isArray(s[field]))s[field].push({settlementId:'SYNTHETIC-RENT-BATCH',transactionIds:['SYNTHETIC-RENT']});else s[field]['SYNTHETIC-RENT']={orderId:'9006001'};save(s);
  const before=fs.readFileSync(path.join(root,'expenses.json'),'utf8');assert.equal(rentalPreview().status,400);assert.equal(fs.readFileSync(path.join(root,'expenses.json'),'utf8'),before);
 }
});
function historicalSetup(){const {s,tx}=setup();const bank={id:'SYNTHETIC-BANK',date:tx.settledDate,credit:98,debit:0,reference:tx.utr};s.bankStatements={'Axis Bank 3448':{transactions:{[bank.id]:bank},imports:[{id:'SYNTHETIC-BST',finalizedAt:'2026-09-06T12:00:00Z',finalizedBy:'synthetic-owner',reconciliationRows:[{bank,linkedRecordIds:['SYNTHETIC-HIST']}]}]}};s.paytmSettlements=[{id:'SYNTHETIC-HIST',bankAccount:'Axis Bank 3448',date:tx.settledDate,bankReference:tx.utr,netAmount:98,grossAmount:100,chargeAmount:2}];save(s);return{s,bank};}
const attach=role=>invoke('/api/expenses/paytm-reports/attach-historical',{method:'POST',role,body:{payoutId:'SYNTHETIC-ORIGINAL',reason:'Original report agrees with the exact existing finalized bank and historical settlement evidence'}});
test('historical attachment reuses exact finalized evidence without bypassing the normal duplicate-posting lock',()=>{
 const {s,bank}=historicalSetup(),rows=[{id:'SHOPIFY/9006001/PAYMENT-1',orderId:'9006001',account:'Paytm Settlement Clearing',amount:100}];
 assert.throws(()=>validatePayoutPosting(s,'SYNTHETIC-ORIGINAL',bank.id,rows),/finalized reconciliation/);
 assert.equal(validateHistoricalSettlementAttachment(s,'SYNTHETIC-ORIGINAL',rows).historicalSettlementId,'SYNTHETIC-HIST');
 const before=clearing(),financialBefore=current();assert.ok(view().payouts[0].reconciliationStatus.historicalCandidate);
 assert.equal(attach('admin').status,200);assert.equal(attach('admin').status,400);
 const after=current();for(const key of ['paytmSettlements','paytmPayoutPostings','receipts','reconciliationExpenses','bankStatements','bankDateOverrides'])assert.deepEqual(after[key],financialBefore[key],key);
 assert.deepEqual(clearing().entries.map(x=>[x.credit,x.debit]),before.entries.map(x=>[x.credit,x.debit]));
 assert.equal(view('owner').reconciliationStatus.counts.posted,1);assert.deepEqual(view('owner').reconciliationStatus,view('admin').reconciliationStatus);
 assert.equal(attach('claimant').status,403);
});
test('historical attachment rejects changed net, gross, charges, dates, multiple ledger claims or unfinalized evidence',()=>{
 const mutations=[s=>s.paytmSettlements[0].netAmount++,s=>s.paytmSettlements[0].grossAmount++,s=>s.paytmSettlements[0].chargeAmount++,s=>s.paytmSettlements[0].date='2026-09-06',s=>s.bankStatements['Axis Bank 3448'].imports[0].reconciliationRows[0].linkedRecordIds.push('FOREIGN'),s=>delete s.bankStatements['Axis Bank 3448'].imports[0].finalizedAt,s=>s.bankDateOverrides.FOREIGN={bankTransactionId:'SYNTHETIC-BANK'}];
 for(const mutate of mutations){const {s}=historicalSetup();mutate(s);save(s);assert.equal(attach('admin').status,400);assert.deepEqual(current().paytmHistoricalEvidenceLinks,{});}
});
test('a re-imported bank row ID cannot bypass finalized evidence or create another bank posting',()=>{
 const {s,bank}=historicalSetup();s.bankStatements['Axis Bank 3448'].imports[0].reconciliationRows[0].bank={...bank,id:'SYNTHETIC-OLD-IMPORT-ID'};save(s);
 const rows=[{id:'SHOPIFY/9006001/PAYMENT-1',orderId:'9006001',account:'Paytm Settlement Clearing',amount:100}];
 assert.throws(()=>validatePayoutPosting(s,'SYNTHETIC-ORIGINAL',bank.id,rows),/finalized reconciliation/);
 assert.equal(attach('admin').status,200);assert.deepEqual(current().paytmPayoutPostings,[]);
 assert.equal(view('admin').reconciliationStatus.counts.posted,1);
});
test('on-page Paytm review keeps confirmation mandatory, cancel safe and submissions one-shot',()=>{
 const vm=require('node:vm'),html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
 const start=html.indexOf('    function confirmPaytmReview('),end=html.indexOf('    function showPaytmSaved(',start),nodes=new Map();
 const document={getElementById:id=>nodes.get(id),createElement:tag=>({tag,children:[],style:{},setAttribute(){},append(...children){this.children.push(...children);},addEventListener(name,fn){this[name]=fn;},showModal(){this.open=true;},focus(){this.focused=true;},close(){this.open=false;},remove(){nodes.delete(this.id);}}),body:{appendChild(node){nodes.set(node.id,node);}}};
 const ctx={document};vm.runInNewContext(html.slice(start,end),ctx);let posted=0;
 ctx.confirmPaytmReview('Verify original receipt, exact UTR, amount and date',()=>posted++);let d=nodes.get('paytmReviewConfirmation');assert.equal(posted,0);assert.equal(d.open,true);assert.equal(d.children[1].textContent,'Verify original receipt, exact UTR, amount and date');assert.equal(d.children[2].focused,true);
 d.children[2].onclick();assert.equal(posted,0);assert.equal(nodes.size,0);
 ctx.confirmPaytmReview('Explicit posting review',()=>posted++);d=nodes.get('paytmReviewConfirmation');d.children[3].onclick();d.children[3].onclick();assert.equal(posted,1);assert.equal(nodes.size,0);
 ctx.confirmPaytmReview('Escape cancels',()=>posted++);d=nodes.get('paytmReviewConfirmation');d.cancel({preventDefault(){}});assert.equal(posted,1);assert.equal(nodes.size,0);
 assert.ok(html.includes('confirmPaytmReview(message,function(){'));
});
