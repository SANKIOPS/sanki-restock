'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const accounting=require('../modules/paytm-accounting'),report=require('../modules/paytm-report'),ui=require('../public/reconciliation-status');
const {registerPaytmReports}=require('../modules/paytm-reports-routes');
function transaction(id,extra={}){return {transactionId:id,date:'2026-10-10',time:'2026-10-10 12:00:00',amount:100,commission:1,gst:.18,platformFee:0,settledAmount:98.82,payoutId:'P-'+id,utr:'UTR-'+id,settledDate:'2026-10-11',...extra};}
function fixture(){
  const s={paytmReportTransactions:{},paytmOrderLinks:{},paytmManualResolutions:{},paytmExcludedTransactions:{},paytmVerifiedSettlements:[],paytmPayoutPostings:[],receipts:[],transfers:[],bankStatements:{}};
  for(let i=0;i<32;i++){const t=transaction('DONE-'+i);s.paytmReportTransactions[t.transactionId]=t;(i<2?s.paytmPayoutPostings:s.paytmVerifiedSettlements).push({settlementId:t.utr,payoutId:t.utr});}
  for(const id of ['EXCLUDED','UNLINKED','NO-UTR','WRONG-TOTAL','LOST-PROOF'])s.paytmReportTransactions[id]=transaction(id,id==='NO-UTR'?{utr:''}:{});
  s.paytmExcludedTransactions.EXCLUDED={reason:'Monthly charge; not a Shopify sale'};
  s.paytmOrderLinks['WRONG-TOTAL']={transactionId:'WRONG-TOTAL',orderId:'O1',orderNumber:'2801',amount:99};
  s.paytmManualResolutions['LOST-PROOF']={transactionId:'LOST-PROOF',type:'manual_sale',amount:100,recordId:'MISSING'};
  return s;
}
function routes(store){const handlers={};let saves=0,audits=[];registerPaytmReports({get:(url,fn)=>handlers[url]=fn,post:(url,...fn)=>handlers[url]=fn.at(-1)},{loadStore:()=>store,saveStore:()=>saves++,audit:(...args)=>audits.push(args),canAccess:()=>true,upload:{array:()=>()=>{}},view:s=>({reconciliationStatus:accounting.reconciliationStatus(s,[])}),today:()=> '2026-10-13',orders:()=>[],saleRows:()=>[]});return{handlers,get saves(){return saves;},audits};}
function reply(){return{statusCode:200,status(n){this.statusCode=n;return this;},json(x){this.body=x;return this;}};}
test('37 saved batches show five exact failures, not zero total finalizations, with no read mutations',()=>{
  const s=fixture(),before=JSON.stringify(s),status=accounting.reconciliationStatus(s,[]);
  assert.deepEqual(status.counts,{total:37,posted:2,finalized:30,ready:0,incomplete:5,bankLinkPending:35});
  assert.match(status.incompleteSettlements.find(x=>x.payoutId==='UTR-EXCLUDED').reasons[0],/contains an excluded payment/);
  assert.match(status.incompleteSettlements.find(x=>x.payoutId==='UTR-UNLINKED').reasons[0],/Every Paytm payment/);
  assert.match(status.incompleteSettlements.find(x=>x.payoutId==='P-NO-UTR').reasons[0],/UTR and settled date/);
  assert.match(status.incompleteSettlements.find(x=>x.payoutId==='UTR-WRONG-TOTAL').reasons[0],/do not equal/);
  assert.match(status.incompleteSettlements.find(x=>x.payoutId==='UTR-LOST-PROOF').reasons[0],/changed or lost its proof/);
  assert.equal(JSON.stringify(s),before);
});
test('Owner and Admin get the identical shared Paytm state, and a zero-new attempt persists its exact reasons',()=>{
  const s=fixture(),r=routes(s),owner=reply(),admin=reply();r.handlers['/api/expenses/paytm-reports']({user:{role:'owner'}},owner);r.handlers['/api/expenses/paytm-reports']({user:{role:'admin'}},admin);assert.deepEqual(owner.body,admin.body);assert.equal(r.saves,0);
  const beforeMovements=JSON.stringify({receipts:s.receipts,transfers:s.transfers,postings:s.paytmPayoutPostings,verified:s.paytmVerifiedSettlements}),result=reply();
  r.handlers['/api/expenses/paytm-reports/finalize-all']({user:{username:'Owner'}},result);
  assert.equal(result.body.finalized,0);assert.equal(result.body.skipped,5);assert.equal(s.paytmLastFinalizationAttempt.newlyFinalized,0);assert.equal(s.paytmLastFinalizationAttempt.skippedSettlements.length,5);assert.equal(r.saves,1);
  assert.equal(JSON.stringify({receipts:s.receipts,transfers:s.transfers,postings:s.paytmPayoutPostings,verified:s.paytmVerifiedSettlements}),beforeMovements);assert.equal(r.audits.at(-1)[2],'PAYTM_BULK_FINALIZATION_REVIEWED');
  const refreshed=reply();r.handlers['/api/expenses/paytm-reports']({user:{role:'admin'}},refreshed);assert.equal(refreshed.body.reconciliationStatus.lastAttempt.by,'Owner');assert.equal(refreshed.body.reconciliationStatus.counts.finalized,30);
});
test('status readiness cannot bypass link, amount, proof or bank uniqueness validation',()=>{
  const t=transaction('READY'),s={paytmReportTransactions:{READY:t},paytmOrderLinks:{READY:{transactionId:'READY',orderId:'O1',orderNumber:'2801',amount:100}},bankStatements:{'Axis Bank 3448':{transactions:{B:{id:'B',date:t.settledDate,credit:98.82,debit:0,reference:t.utr}}}}};
  const sales=[{id:'SHOPIFY/O1',orderId:'O1',amount:100,account:accounting.CLEARING}],p=report.summarizePayouts([t])[0];
  let state=accounting.settlementStatus(s,p,sales);assert.equal(state.code,'ready');assert.equal(state.bankLinkReady,true);
  s.bankDateOverrides={'SHOPIFY/O1':{bankTransactionId:'OTHER'}};state=accounting.settlementStatus(s,p,sales);assert.equal(state.code,'incomplete');assert.equal(state.bankLinkReady,false);assert.match(state.reasons[0],/already linked/);assert.throws(()=>accounting.validatePayoutPosting(s,p.payoutId,'B',sales),/already linked/);
  delete s.bankDateOverrides;s.bankStatements['Axis Bank 3448'].transactions.B2={...s.bankStatements['Axis Bank 3448'].transactions.B,id:'B2'};state=accounting.settlementStatus(s,p,sales);assert.equal(state.bankLinkReady,false);assert.match(state.bankReasons[0],/More than one/);
});
test('excluded non-Shopify payments retain the exact bank-link workflow rather than being restored automatically',()=>{
  const t=transaction('MONTHLY'),s={paytmReportTransactions:{MONTHLY:t},paytmExcludedTransactions:{MONTHLY:{reason:'Not a Shopify sale — monthly charge'}},bankStatements:{'Axis Bank 3448':{transactions:{B:{id:'B',date:t.settledDate,credit:98.82,debit:0,reference:t.utr}}}}};
  const state=accounting.settlementStatus(s,report.summarizePayouts([t])[0],[]);assert.equal(state.code,'incomplete');assert.equal(state.bankLinkReady,true);assert.match(state.blockedTransactions[0].reason,/monthly charge/);assert.ok(s.paytmExcludedTransactions.MONTHLY);
  s.bankStatements['Axis Bank 3448'].transactions.B.date='2026-10-12';const changed=accounting.reconciliationStatus(s,[]).settlements[0];assert.equal(changed.bankLinkReady,false);assert.match(changed.bankReasons[0],/exact Paytm settled date/);
});
test('all shared renderers are independent of role and escape exact blocker data',()=>{
  const d={reconciliationStatus:accounting.reconciliationStatus(fixture(),[])};d.reconciliationStatus.incompleteSettlements[0].reasons.push('<script>unsafe</script>');
  assert.equal(ui.paytm({...d,isOwner:true}),ui.paytm({...d,isOwner:false}));assert.match(ui.paytm(d),/Why 5 settlement/);assert.match(ui.paytm(d),/&lt;script&gt;/);assert.doesNotMatch(ui.paytm(d),/<script>/);
  const bank={draftId:'BRD',statementSummary:{validated:true},unresolved:0,balanceResolved:true,ledgerClosing:100,bankClosing:100,balanceDifference:0,finalizationApproval:{status:'pending',requestedBy:'Admin',requestedAt:'2026-10-13T08:00:00Z'}};
  assert.equal(ui.bank({...bank,isOwner:true}),ui.bank({...bank,isOwner:false}));assert.match(ui.bank(bank),/Awaiting Owner approval/);assert.doesNotMatch(ui.bank(bank),/class="msg err"/);
  const cash={latest:{verifiedThrough:'2026-10-12',countedBalance:100,ledgerBalance:100,difference:0,approvedBy:'Owner'},pending:[{id:'CCR'}]};assert.equal(ui.cash({...cash,isOwner:true}),ui.cash({...cash,isOwner:false}));assert.match(ui.cash(cash),/Awaiting Owner approval/);
  const card={id:'CCS',status:'review',rows:[],movementDifference:0};assert.equal(ui.card({...card,isOwner:true}),ui.card({...card,isOwner:false}));assert.match(ui.card({...card,status:'finalized'}),/statement finalized/);
  assert.equal(ui.when('2026-10-13T08:00:00Z'),'13/10/26, 13:30');
  assert.equal(ui.when('2026-10-13 13:30'),'2026-10-13 13:30 IST');
});
test('shared status refresh never overlaps and pauses for editing or hidden views',async()=>{
  let busy=true,hidden=false,calls=0,resolve;const control=ui.createRefreshController({isBusy:()=>busy,isHidden:()=>hidden,refresh:()=>{calls++;return new Promise(r=>resolve=r);}});
  assert.equal(await control.run(),false);busy=false;hidden=true;assert.equal(await control.run(),false);hidden=false;
  const running=control.run();assert.equal(control.isRunning(),true);assert.equal(await control.run(),false);assert.equal(calls,1);resolve();assert.equal(await running,true);assert.equal(control.isRunning(),false);
});
test('refresh failures retain the old view and expose a recoverable warning',async()=>{let errors=[];const control=ui.createRefreshController({refresh:async()=>{throw Error('Connection lost');},onError:e=>errors.push(e.message)});assert.equal(await control.run(),false);assert.deepEqual(errors,['Connection lost']);assert.equal(control.isRunning(),false);});
test('one bank loader and shared status renderers are wired into every reconciliation view',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');assert.equal((html.match(/function loadBankStatement\(/g)||[]).length,1);
  for(const name of ['paytm(d)','paytmRow(p.reconciliationStatus)','bank(d)','bankSaved(d)','cash(d)','card(st)'])assert.ok(html.includes('ReconciliationStatus.'+name),name);
  assert.match(html,/initializeReconciliationStatusRefresh\(\);/);assert.match(html,/30000/);assert.match(html,/reconciliationDirty/);assert.match(html,/token!==bankStatementLoadGeneration/);assert.match(html,/paytmToken===paytmStatusGeneration/);
  assert.doesNotMatch(html,/cfg\.isOwner\?'Owner approval is required/);
  for(const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))if(script[1].trim())assert.doesNotThrow(()=>new vm.Script(script[1]));
});

test('the bank loader rejects stale responses and does not overwrite an edited review',async()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
  const source=html.slice(html.indexOf('    function loadBankStatement('),html.indexOf('    function uploadBankStatement('));
  const elements={bs_account:{value:'Namita 5464'},bs_table:{textContent:'',innerHTML:'previous'},bs_history:{innerHTML:''},bs_finalize:{style:{display:''}},bs_discard:{style:{display:''}}};
  const requests=[];let dirty=false,rendered=[];
  const context=vm.createContext({Promise,encodeURIComponent,Error,bankNature:'PERSONAL',bankStatementLoadGeneration:0,bankDraftId:'OLD',bankReconView:{},bankDraftOptions:[],el:id=>elements[id],api:(url)=>new Promise(resolve=>requests.push({url,resolve})),reconciliationStatusBusy:()=>dirty,renderBankHistory:()=> 'history',renderBankReconciliation:d=>rendered.push(d.draftId),ReconciliationStatus:ui,bankDraftSwitcher:()=>'',fmt:String,esc:String});
  // Run the real navigation dependency introduced by refresh persistence;
  // preserve every stale-response/dirty-review assertion below.
  context.window=context;context.URL=URL;context.location={href:'https://example.test/expenses.html',pathname:'/expenses.html',search:'',hash:''};context.history={state:null,replaceState(){}};
  new vm.Script(fs.readFileSync(path.join(__dirname,'../public/navigation-state.js'),'utf8')).runInContext(context);
  new vm.Script(source).runInContext(context);
  const old=context.loadBankStatement('OLD',true),current=context.loadBankStatement('NEW',true);
  assert.equal(requests[1].url,'/api/expenses/bank-statements?nature=PERSONAL&account=Namita%205464&draftId=NEW');
  requests[0].resolve({success:true,draft:{draftId:'OLD'},transactions:[]});await old;assert.deepEqual(rendered,[]);
  requests[1].resolve({success:true,draft:{draftId:'NEW'},transactions:[]});await current;assert.deepEqual(rendered,['NEW']);
  const edited=context.loadBankStatement('NEW',true);dirty=true;requests[2].resolve({success:true,draft:{draftId:'NEW'},transactions:[]});await edited;assert.deepEqual(rendered,['NEW']);
  dirty=false;const switched=context.loadBankStatement('NEW',true);elements.bs_account.value='Other account';requests[3].resolve({success:true,draft:{draftId:'NEW'},transactions:[]});await switched;assert.deepEqual(rendered,['NEW']);
  elements.bs_account.value='Namita 5464';const finalized=context.loadBankStatement('',false);requests[4].resolve({success:true,transactions:[],draftOptions:[],account:'Namita 5464',lastReconciliation:{to:'2026-10-13',balanceReconciled:true,by:'Owner',at:'2026-10-13T08:00:00Z'}});await finalized;
  assert.equal(context.bankDraftId,'');assert.equal(elements.bs_finalize.style.display,'none');assert.equal(elements.bs_discard.style.display,'none');assert.match(elements.bs_table.innerHTML,/Finalized reconciliation saved/);assert.equal(elements.bs_history.innerHTML,'history');
});

test('initial Paytm loading cannot replace a newer saved action or another workspace',async()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
  const start=html.indexOf('      var initialPaytmToken=');const source=html.slice(start,html.indexOf('\n    }',start));
  let resolve,shown=[];const host={textContent:''};let current=host;
  const context=vm.createContext({paytmStatusGeneration:1,el:()=>current,api:()=>new Promise(r=>resolve=r),showPaytmSaved:d=>shown.push(d)});
  new vm.Script(source).runInContext(context);context.paytmStatusGeneration=2;resolve({success:true,marker:'stale'});await new Promise(r=>setImmediate(r));assert.deepEqual(shown,[]);
  new vm.Script(source).runInContext(context);current={textContent:''};resolve({success:true,marker:'previous workspace'});await new Promise(r=>setImmediate(r));assert.deepEqual(shown,[]);
  new vm.Script(source).runInContext(context);resolve({success:true,marker:'fresh'});await new Promise(r=>setImmediate(r));assert.equal(shown[0].marker,'fresh');
});
