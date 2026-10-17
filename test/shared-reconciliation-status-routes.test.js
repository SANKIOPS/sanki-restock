'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const temp=fs.mkdtempSync(path.join(__dirname,'.tmp-shared-status-'));process.env.DATA_PATH=path.join(temp,'data.json');
const file=path.join(temp,'expenses.json');fs.writeFileSync(file,JSON.stringify({expenses:{},receivables:{},receipts:[],oneTimeMigrations:{}}));
const {router}=require('../modules/expenses');
test.after(()=>fs.rmSync(temp,{recursive:true,force:true}));
const baseline=JSON.parse(fs.readFileSync(file,'utf8')),account='IndusInd Bank 8181';
function invoke(route,{role='admin',username='other-admin',query={},body={},params={},method='GET'}={}){const layer=router.stack.find(x=>x.route&&x.route.path===route&&x.route.methods[method.toLowerCase()]);assert.ok(layer);let status=200,result;layer.route.stack[0].handle({query,body,params,headers:{},get(){return '';},user:{role,roles:[role],username}},{status(n){status=n;return this;},json(r){result=r;return this;}},()=>{throw Error('Unexpected next');});return{status,body:result};}
function reset(extra={}){const s={...baseline,expenses:{},receivables:{},receipts:[],loanRepayments:[],transfers:[],adjustments:[],vendorAdvances:[],salesRefunds:[],reconciliationExpenses:[],bankStatements:{},bankDateOverrides:{},bankReconciliationDrafts:{},bankReconciliationApprovals:{},paytmReportTransactions:{},paytmOrderLinks:{},paytmVerifiedSettlements:[],paytmPayoutPostings:[],cashReconciliations:[],...extra};fs.writeFileSync(file,JSON.stringify(s));return s;}
test('shared Paytm GET exposes the same saved exact reasons to real Owner/Admin routes',()=>{
  const tx={transactionId:'PT-SHARED',date:'2098-10-10',amount:100,commission:1,gst:.18,platformFee:0,settledAmount:98.82,payoutId:'P1',utr:'UTR-SHARED',settledDate:'2098-10-11'};
  reset({paytmReportTransactions:{'PT-SHARED':tx},paytmLastFinalizationAttempt:{at:'2098-10-11T08:00:00Z',by:'Owner',newlyFinalized:0,incomplete:1,skippedSettlements:[{settlementId:tx.utr,reason:'Every Paytm payment must be reviewed.'}]}});
  // Let the existing one-time legacy fixture migrations complete first; the
  // shared status reads below must then leave the whole saved store unchanged.
  invoke('/api/expenses/paytm-reports',{role:'owner'});
  const before=fs.readFileSync(file,'utf8'),owner=invoke('/api/expenses/paytm-reports',{role:'owner'}),admin=invoke('/api/expenses/paytm-reports');
  assert.equal(owner.status,200);assert.equal(admin.status,200);assert.deepEqual(owner.body.reconciliationStatus,admin.body.reconciliationStatus);assert.equal(admin.body.reconciliationStatus.counts.incomplete,1);assert.match(admin.body.reconciliationStatus.incompleteSettlements[0].reasons[0],/Every Paytm payment/);assert.equal(admin.body.reconciliationStatus.lastAttempt.by,'Owner');assert.equal(fs.readFileSync(file,'utf8'),before);
  assert.equal(invoke('/api/expenses/paytm-reports',{role:'claimant'}).status,403);
});
test('same business-bank draft carries identical approval and reconciliation results for both roles',()=>{
  const id='BRD-SHARED',date='2098-10-10';reset({openingBalances:{[account]:0},receipts:[{id:'REC-SHARED',nature:'SANKI',account,date,amount:100,receiptType:'other_income',category:'Other Income',source:'Test',bankReference:'SHARED-REF'}],bankReconciliationDrafts:{[id]:{id,nature:'SANKI',account,transactions:[{date,reference:'SHARED-REF',description:'Test',credit:100,debit:0,balance:100}],summary:{from:date,to:date,openingBalance:0,totalDebits:0,totalCredits:100,closingBalance:100,validated:true},resolutions:{},matchingPolicy:'balanced_date_amount_v5',createdAt:'2098-10-10T01:00:00Z'}},bankReconciliationApprovals:{A:{id:'A',draftId:id,status:'pending',requestedBy:'Admin',requestedAt:'2098-10-10T08:00:00Z'}}});
  const query={nature:'SANKI',account,draftId:id},owner=invoke('/api/expenses/bank-statements',{role:'owner',query}),admin=invoke('/api/expenses/bank-statements',{query});assert.equal(owner.status,200);assert.equal(admin.status,200);assert.deepEqual(owner.body.draft,admin.body.draft);assert.equal(admin.body.draft.finalizationApproval.status,'pending');assert.equal(admin.body.draft.balanceDifference,0);
});
test('cash status is readable by Admin but count submission and approval rights are unchanged',()=>{
  reset({cashReconciliations:[{id:'CCR-SHARED',nature:'SANKI',account:'Counter Cash',verifiedThrough:'2098-10-10',countedBalance:100,ledgerBalance:100,difference:0,status:'pending',requestedBy:'prashant',requestedAt:'2098-10-10T08:00:00Z'}]});
  const owner=invoke('/api/expenses/cash-reconciliations',{role:'owner'}),admin=invoke('/api/expenses/cash-reconciliations');assert.equal(owner.status,200);assert.deepEqual(owner.body,admin.body);assert.equal(admin.body.pending[0].status,'pending');
  assert.equal(invoke('/api/expenses/cash-reconciliations',{role:'claimant'}).status,403);
  assert.equal(invoke('/api/expenses/cash-reconciliations',{method:'POST',body:{countedBalance:100,verifiedThrough:'2026-10-13'}}).status,403);
  assert.equal(invoke('/api/expenses/cash-reconciliations/:id/decision',{method:'POST',params:{id:'CCR-SHARED'},body:{decision:'approve'}}).status,403);
});
test('reconciliation status parity does not expose private Owner accounts or entities',()=>{
  reset();assert.equal(invoke('/api/expenses/bank-statements',{query:{nature:'PERSONAL',account:'ICICI Bank 0992'}}).status,403);assert.equal(invoke('/api/expenses/bank-statements',{role:'claimant',query:{nature:'SANKI',account}}).status,403);
});
