'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-expenses-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const { router, summaryForPL, createTelegramPersonalExpense, createTelegramPersonalReceipt, createTelegramBusinessPaidExpense, telegramBusinessCategories, telegramExpense, telegramApproveExpense, telegramRecordPayment, telegramRecordTransfer, telegramRecordNamitaTransfer, telegramApi, parseBankStatementFile, parseBankStatementText, applyFinalizedOpeningVendorPayables, applyFinalizedInternalTransfers, applyFinalizedCompositeLinks, applyFinalizedConfirmedMatches, applyEx00122CashPaymentCorrection, applyMissingPerfumeSale, applyOwnerConfirmedAxis3645Cases, applyKaluFlowersFruitsVendorMerge, applyEx00120ExactBankAmountCorrection, applyStrictReconciliationIdentityPolicy } = require('../modules/expenses');
const XLSX = require('xlsx');

test.after(() => {
  fs.rmSync(tempDir, { recursive: true, force: true });
});

test('EX-00122 keeps its ₹58 expense while correcting Counter Cash from ₹34 to ₹60',()=>{
  const store={expenses:{'EX-00122':{id:'EX-00122',nature:'SANKI',amount:58,paidAmount:34,status:'partially_paid',payments:[{id:'PAY-001',amount:34,account:'Counter Cash',paymentType:'Cash'}]}},oneTimeMigrations:{},auditLog:[],auditSeq:0};
  assert.equal(applyEx00122CashPaymentCorrection(store),true);
  const expense=store.expenses['EX-00122'],payment=expense.payments[0];
  assert.equal(expense.amount,58);assert.equal(payment.amount,60);assert.equal(payment.cashRoundingAmount,2);assert.equal(expense.paidAmount,60);assert.equal(expense.status,'paid');assert.equal(expense.cashSettlementDifference,2);
  assert.equal(store.auditLog.at(-1).action,'PAYMENT_AMOUNT_CORRECTED');assert.equal(store.auditLog.at(-1).before.payment.amount,34);assert.equal(store.auditLog.at(-1).after.payment.amount,60);
  assert.equal(applyEx00122CashPaymentCorrection(store),false,'the production correction is idempotent');
});

test('26 August perfume sale posts ₹1,000 to Counter Cash and ₹799 to Paytm clearing once',()=>{
  const store={receipts:[],receiptSeq:4,oneTimeMigrations:{},auditLog:[],auditSeq:0};
  assert.equal(applyMissingPerfumeSale(store),true);
  assert.equal(store.receipts.length,2);
  const cash=store.receipts.find(x=>x.account==='Counter Cash'),paytm=store.receipts.find(x=>x.account==='Paytm Settlement Clearing');
  assert.equal(cash.amount,1000);assert.equal(paytm.amount,799);assert.equal(cash.receiptType,'product_sale');assert.equal(paytm.receiptType,'product_sale');
  assert.equal(cash.manualSaleId,paytm.manualSaleId);assert.equal(cash.saleTotal,1799);assert.equal(paytm.saleTotal,1799);assert.equal(paytm.inventoryStatus,'unmapped_no_sku');
  assert.equal(store.auditLog.length,2);assert.equal(store.auditLog.every(x=>x.action==='PRODUCT_SALE_RECEIPT_RECORDED'),true);
  assert.equal(applyMissingPerfumeSale(store),false,'the split sale cannot be duplicated on another load');
  assert.equal(store.receipts.length,2);
});

test('EX-00120 uses the exact ₹651.90 bank amount and removes only ADJ-0003',()=>{
  const account='Prashant Axis 3645',store={
    expenses:{'EX-00120':{id:'EX-00120',nature:'SANKI',amount:652,requestedAmount:652,paidAmount:652,status:'paid',payments:[{id:'PAY-001',amount:652,date:'2026-08-28',account}]}},
    adjustments:[{id:'ADJ-0003',nature:'SANKI',account,amount:.10,date:'2026-08-27',bankReference:'660545756771'},{id:'ADJ-KEEP',nature:'SANKI',account,amount:25,date:'2026-08-27'}],
    reconciliationExpenses:[{id:'BRE-ADJ-0003',adjustmentId:'ADJ-0003'},{id:'BRE-KEEP',adjustmentId:'ADJ-KEEP'}],bankDateOverrides:{'ADJ-0003':{bankDate:'2026-08-27'},'EX-00120/PAY-001':{bankDate:'2026-08-27'},'ADJ-KEEP':{bankDate:'2026-08-27'}},
    bankReconciliationDrafts:{D1:{id:'D1',nature:'SANKI',account,transactions:[],summary:{},resolutions:{'bank-61':{action:'link_multiple_existing',appIds:['EX-00120/PAY-001','ADJ-0003'],reason:'Old rounding link'},'app-0':{action:'exclude',appId:'ADJ-0003',reason:'Old adjustment'},'app-1':{action:'exclude',appId:'ADJ-KEEP',reason:'Keep this resolution'},'bank-55':{action:'exclude',reason:'Unrelated bank decision'}}}},
    oneTimeMigrations:{},auditLog:[],auditSeq:0,transfers:[],receipts:[],vendorAdvances:[],vendorOpeningPayables:[],paytmSettlements:[]
  };
  assert.equal(applyEx00120ExactBankAmountCorrection(store),true);
  const expense=store.expenses['EX-00120'];assert.equal(expense.amount,651.90);assert.equal(expense.requestedAmount,651.90);assert.equal(expense.payments[0].amount,651.90);assert.equal(expense.paidAmount,651.90);assert.equal(expense.status,'paid');
  assert.equal(store.adjustments.some(x=>x.id==='ADJ-0003'),false);assert.equal(store.adjustments.some(x=>x.id==='ADJ-KEEP'),true);assert.equal(store.reconciliationExpenses.some(x=>x.adjustmentId==='ADJ-0003'),false);assert.equal(store.reconciliationExpenses.some(x=>x.adjustmentId==='ADJ-KEEP'),true);assert.equal(store.bankDateOverrides['ADJ-0003'],undefined);assert.ok(store.bankDateOverrides['EX-00120/PAY-001']);
  assert.equal(store.bankReconciliationDrafts.D1.resolutions['bank-61'],undefined);assert.equal(store.bankReconciliationDrafts.D1.resolutions['app-0'].appId,'ADJ-KEEP');assert.deepEqual(store.bankReconciliationDrafts.D1.resolutions['bank-55'],{action:'exclude',reason:'Unrelated bank decision'});
  assert.equal(store.auditLog.at(-1).action,'EXPENSE_BANK_AMOUNT_CORRECTED');assert.equal(store.auditLog.at(-1).before.expenseAmount,652);assert.equal(store.auditLog.at(-1).after.expenseAmount,651.90);
  assert.equal(applyEx00120ExactBankAmountCorrection(store),false,'the correction is idempotent');
});

test('strict reconciliation policy separates same-date same-amount parties and preserves manual decisions',()=>{
  const account='Prashant Axis 3645',store={expenses:{'EX-00032':{id:'EX-00032',nature:'SANKI',status:'paid',approvedAt:'2026-08-24T10:00:00Z',vendor:'Kalu Flowers & Fruits',amount:200,paidAmount:200,payments:[{id:'PAY-001',date:'2026-08-24',amount:200,account}]},'EX-MANUAL':{id:'EX-MANUAL',nature:'SANKI',status:'paid',approvedAt:'2026-08-24T10:00:00Z',vendor:'Manual vendor',amount:50,paidAmount:50,payments:[{id:'PAY-001',date:'2026-08-24',amount:50,account}]},'EX-00120':{id:'EX-00120',nature:'SANKI',status:'paid',approvedAt:'2026-08-27T10:00:00Z',vendor:'Vi',amount:651.90,paidAmount:651.90,payments:[{id:'PAY-001',date:'2026-08-27',amount:651.90,account}]}},adjustments:[],vendorAdvances:[],receipts:[],transfers:[],receivables:{},vendorOpeningPayables:[],paytmSettlements:[],oneTimeMigrations:{},auditLog:[],auditSeq:0,bankDateOverrides:{'EX-00120/PAY-001':{bankDate:'2026-08-27',bankReference:'660545756771'}},bankReconciliationDrafts:{D1:{id:'D1',account,nature:'SANKI',transactions:[{date:'2026-08-24',description:'UPI/P2M/623647818325/Ferns n petals',reference:'623647818325',debit:200,credit:0},{date:'2026-08-24',description:'Unrelated bank narration',reference:'KEEP',debit:50,credit:0},{date:'2026-08-27',description:'UPI/P2M/660545756771/Euronet Services India',reference:'660545756771',debit:651.90,credit:0}],summary:{from:'2026-08-24',to:'2026-08-27',closingBalance:-901.90},resolutions:{'bank-1':{action:'link_existing',appId:'EX-MANUAL/PAY-001',reason:'Keep manual decision'}}}}};
  assert.equal(applyStrictReconciliationIdentityPolicy(store),true);assert.equal(store.bankReconciliationDrafts.D1.matchingPolicy,'strict_identity_v3');assert.deepEqual(store.bankReconciliationDrafts.D1.resolutions['bank-1'],{action:'link_existing',appId:'EX-MANUAL/PAY-001',reason:'Keep manual decision'});
  const report=store.oneTimeMigrations['require-date-amount-direction-and-party-identity-for-bank-auto-match-v3'].updatedDrafts[0];assert.deepEqual(report.removedAutomaticMatches.map(x=>[x.rowId,x.appId]),[['bank-0','EX-00032/PAY-001']]);assert.equal(report.preservedResolutionCount,1);assert.equal(report.unresolvedAfter,2,'only the unrelated FNP bank row and Kalu ledger row remain unresolved');assert.equal(report.removedAutomaticMatches.some(x=>x.appId==='EX-00120/PAY-001'),false,'a previously confirmed bank reference stays matched');assert.equal(store.auditLog.at(-1).action,'BANK_AUTO_MATCH_POLICY_UPDATED');assert.equal(applyStrictReconciliationIdentityPolicy(store),false);
});

test('Kalu vendor aliases merge within SANKI while preserving transactions, credits and SAMAST',()=>{
  const store={
    expenses:{
      'EX-KALU':{id:'EX-KALU',nature:'SANKI',status:'partially_paid',vendor:'Kalu flower',amount:720,paidAmount:400},
      'EX-VIJAY':{id:'EX-VIJAY',nature:'SANKI',status:'approved',vendor:'Vijay',amount:160,paidAmount:0},
      'EX-VIJAY-KUMAR':{id:'EX-VIJAY-KUMAR',nature:'SANKI',status:'approved',vendor:'Vijay Kumar',amount:380,paidAmount:0},
      'EX-SAMAST':{id:'EX-SAMAST',nature:'SAMAST',status:'approved',vendor:'Vijay Kumar',amount:99,paidAmount:0}
    },
    vendors:{'kalu flower':{name:'Kalu flower'},vijay:{name:'Vijay'},'vijay kumar':{name:'Vijay Kumar'}},vendorsByNature:{SAMAST:{'vijay kumar':{name:'Vijay Kumar'}}},
    vendorOpeningPayables:[{id:'VOP-1',nature:'SANKI',vendor:'Vijay',amount:100,paidAmount:100}],vendorAdvances:[{id:'VADV-1',nature:'SANKI',vendor:'Kalu flower',amount:218,remainingAmount:18}],
    bankReconciliationDrafts:{D1:{id:'D1',nature:'SANKI',resolutions:{'bank-1':{action:'vendor_advance_split',vendor:'Vijay Kumar'}}}},oneTimeMigrations:{},auditLog:[],auditSeq:0
  };
  assert.equal(applyKaluFlowersFruitsVendorMerge(store),true);
  assert.equal(applyKaluFlowersFruitsVendorMerge(store),false,'the production merge is idempotent');
  assert.equal(store.expenses['EX-KALU'].vendor,'Kalu Flowers & Fruits');assert.equal(store.expenses['EX-VIJAY'].vendor,'Kalu Flowers & Fruits');assert.equal(store.expenses['EX-VIJAY-KUMAR'].vendor,'Kalu Flowers & Fruits');
  assert.equal(store.expenses['EX-SAMAST'].vendor,'Vijay Kumar');assert.equal(store.vendorsByNature.SAMAST['vijay kumar'].name,'Vijay Kumar');
  assert.equal(store.vendorOpeningPayables[0].vendor,'Kalu Flowers & Fruits');assert.equal(store.vendorAdvances[0].vendor,'Kalu Flowers & Fruits');assert.equal(store.bankReconciliationDrafts.D1.resolutions['bank-1'].vendor,'Kalu Flowers & Fruits');
  assert.deepEqual(Object.keys(store.vendors),['kalu flowers & fruits']);assert.equal(store.oneTimeMigrations['merge-sanki-vijay-vijay-kumar-kalu-flower-into-kalu-flowers-fruits-v1'].result.after.outstanding,842);
  assert.equal(store.auditLog.filter(x=>x.action==='VENDOR_MERGED').length,3);
});

test('owner-confirmed Axis 3645 cases preserve unrelated reconciliation progress and post exact ledger effects',()=>{
  const account='Prashant Axis 3645',paid=(id,date,vendor,amount)=>({id,date,nature:'SANKI',status:'paid',approvedAt:date+'T10:00:00Z',vendor,particulars:vendor,amount,paidAmount:amount,payments:[{id:'PAY-001',date,amount,account}]}),store={
    expenses:{
      'EX-PERSONAL-HARYANA':{id:'EX-PERSONAL-HARYANA',date:'2026-08-22',nature:'PERSONAL',status:'paid',approvedAt:'2026-08-22T10:00:00Z',vendor:'Haryana Trading Company',particulars:'Owner purchase',amount:6560,paidAmount:6560,paidAlready:true,fundedBy:'claimant',personalPaidAmount:6560,reimbursementStatus:'not_applicable',payments:[{id:'PAY-001',date:'2026-08-22',amount:6560,account:'ICICI Bank 0993',personalFunds:true}]},
      'EX-00031':paid('EX-00031','2026-08-24','FNP',100),'EX-00038':paid('EX-00038','2026-08-24','FNP',100),'EX-00032':paid('EX-00032','2026-08-24','Kalu Fruits and Flowers',200),
      'EX-00073':paid('EX-00073','2026-08-25','Kalu Fruits and Flowers',100),'EX-00077':paid('EX-00077','2026-08-26','Kalu Fruits and Flowers',100),
      'EX-00095':{id:'EX-00095',date:'2026-08-27',nature:'SANKI',status:'approved',approvedAt:'2026-08-27T10:00:00Z',vendor:'Kalu Fruits and Flowers',particulars:'Fruit',amount:160,paidAmount:0,payments:[]},
      'EX-00027':paid('EX-00027','2026-08-23','Alok Kumar',600),'EX-00035':paid('EX-00035','2026-08-24','Alok Kumar',600),
      'EX-00033':{id:'EX-00033',date:'2026-08-27',nature:'SANKI',status:'paid',approvedAt:'2026-08-27T10:00:00Z',amount:46,paidAmount:46,reimbursementPayments:[{id:'REIM-001',date:'2026-08-27',amount:46,account}]},
      'EX-00037':{id:'EX-00037',date:'2026-08-27',nature:'SANKI',status:'paid',approvedAt:'2026-08-27T10:00:00Z',amount:20,paidAmount:20,reimbursementPayments:[{id:'REIM-001',date:'2026-08-27',amount:20,account}]},
      'EX-00083':{id:'EX-00083',date:'2026-08-27',nature:'SANKI',status:'paid',approvedAt:'2026-08-27T10:00:00Z',amount:73,paidAmount:73,reimbursementPayments:[{id:'REIM-001',date:'2026-08-27',amount:73,account}]},
      'EX-00120':paid('EX-00120','2026-08-28','Vi Recharge',652)
    },vendors:{'kalu fruits and flowers':{name:'Kalu Fruits and Flowers'}},vendorsByNature:{PERSONAL:{}},vendorAdvances:[],oneTimeMigrations:{},auditLog:[],auditSeq:0,adjustments:[],receipts:[],openingBalances:{},openingBalancesByNature:{PERSONAL:{}},bankDateOverrides:{},transferSeq:8,adjSeq:0,
    transfers:[
      {id:'TR-00006',nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',fromAccount:'Counter Cash',toAccount:account,amount:20000,date:'2026-08-26',proof:'/cash.jpg'},
      {id:'TR-00007',nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',fromAccount:account,toAccount:'Axis Bank 3448',amount:20000,date:'2026-08-26',proof:'/paytm.jpg'},
      {id:'TR-00008',nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',fromAccount:'Axis Bank 3448',toAccount:account,amount:1203,date:'2026-08-26',proof:'/first.jpg'}
    ],bankReconciliationDrafts:{'BRD-CASES':{id:'BRD-CASES',account,nature:'SANKI',transactions:[
      {date:'2026-08-22',reference:'612673856994',description:'GAGAN LAM',debit:0,credit:6560},{date:'2026-08-22',reference:'623430655870',description:'HARYANATRADING CO',debit:6560,credit:0},
      {date:'2026-08-24',reference:'623647818325',description:'Ferns n petals',debit:200,credit:0},{date:'2026-08-24',reference:'623662965347',description:'Kalu Fruits',debit:418,credit:0},
      {date:'2026-08-24',reference:'623646085829',description:'ANOKHMAL BAIRWA',debit:1200,credit:0},
      {date:'2026-08-26',description:'THIRD PARTY CASH DEP',debit:0,credit:19500},{date:'2026-08-26',description:'THIRD PARTY CASH DEP',debit:0,credit:500},{date:'2026-08-26',reference:'623833323062',description:'SANKI THE CRAZY ATTIR',debit:20000,credit:0},
      {date:'2026-08-26',description:'TPARTY TRAN BHARAT SAMAST GARMENTS',debit:0,credit:1203},{date:'2026-08-26',description:'TPARTY TRAN BHARAT SAMAST GARMENTS',debit:0,credit:1203},
      {date:'2026-08-26',reference:'623818860235',description:'SHIVAM KUMAR',debit:139,credit:0},{date:'2026-08-27',reference:'660545756771',description:'Euronet Services Indi',debit:651.90,credit:0},
      {date:'2026-08-28',reference:'KEEP',description:'Previously resolved',debit:50,credit:0}
    ],summary:{from:'2026-08-22',to:'2026-08-28'},resolutions:{'bank-12':{action:'exclude',reason:'Keep me'}},matchingPolicy:'strict_identity_v2'}}
  };
  assert.equal(applyOwnerConfirmedAxis3645Cases(store),true);
  assert.deepEqual(store.bankReconciliationDrafts['BRD-CASES'].resolutions['bank-12'],{action:'exclude',reason:'Keep me'});
  const personal=store.expenses['EX-PERSONAL-HARYANA'],personalTransfer=store.transfers.find(x=>x.ownerConfirmedMigration&&x.classification==='reimbursement');assert.equal(personal.payments[0].account,account);assert.equal(personal.nature,'PERSONAL');assert.equal(personalTransfer.fromAccount,'ICICI Bank 0993');assert.equal(personalTransfer.toAccount,account);
  const advance=store.vendorAdvances[0];assert.equal(advance.amount,218);assert.equal(advance.remainingAmount,18);assert.deepEqual(advance.applications.map(x=>[x.expenseId,x.amount]),[['EX-00073',100],['EX-00077',100]]);assert.equal(advance.reclassifiedPayments.length,2);assert.equal(store.expenses['EX-00073'].payments.length,0);assert.equal(store.expenses['EX-00077'].payments.length,0);assert.equal(store.expenses['EX-00073'].paidAmount,100);assert.equal(store.expenses['EX-00077'].paidAmount,100);assert.equal(store.expenses['EX-00095'].paidAmount,0);
  assert.equal(store.transfers.find(x=>x.id==='TR-00006').amount,19500);assert.ok(store.transfers.some(x=>x.fromAccount==='Counter Cash'&&x.toAccount===account&&x.amount===500));assert.equal(store.transfers.find(x=>x.id==='TR-00007').toAccount,'Paytm Settlement Clearing');
  assert.equal(store.transfers.filter(x=>x.fromAccount==='Axis Bank 3448'&&x.toAccount===account&&x.amount===1203).length,2);assert.equal(store.adjustments.find(x=>x.bankReference==='660545756771').amount,.10);
  assert.deepEqual(store.bankReconciliationDrafts['BRD-CASES'].resolutions['bank-10'].appIds,['EX-00033/REIM-001','EX-00037/REIM-001','EX-00083/REIM-001']);
  assert.equal(applyOwnerConfirmedAxis3645Cases(store),false,'all seven corrections are idempotent');
});

function invoke(method, routePath, { body = {}, params = {}, query = {}, role = 'claimant' } = {}) {
  const layer = router.stack.find(item => item.route && item.route.path === routePath && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, `route exists: ${method} ${routePath}`);
  const req = {
    body, params, query,
    headers: { 'user-agent':'SANKI Test Mobile', 'x-forwarded-for':'203.0.113.10' },
    get(name) { return this.headers[String(name).toLowerCase()] || ''; },
    ip: '203.0.113.10',
    user: { username: role === 'claimant' ? 'arshpreet' : (role === 'admin' ? 'prashant' : role + '-user'), role, roles: [role] }
  };
  let status = 200;
  let result;
  const res = {
    status(code) { status = code; return this; },
    json(value) { result = value; return this; },
    end() { return this; },
    sendFile() { return this; }
  };
  layer.route.stack[0].handle(req, res, () => {
    throw new Error(`unexpected next() for ${method} ${routePath}`);
  });
  return { status, body: result };
}

test('claimant cannot submit an expense without a bill photo', async () => {
  const result = invoke('POST', '/api/expenses', { body: { ledger: 'FOOD EXPENSE', amount: 100, bill: 'handwritten' } });
  assert.equal(result.status, 400);
  assert.match(result.body.error, /Bill photo is required/);
});

test('new vendor submits immediately and approver correction becomes reusable', async () => {
  const created = invoke('POST', '/api/expenses', { body: { ledger: 'FOOD EXPENSE', vendor: 'Vender Typo', amount: 100, billPhoto: '/api/expenses/photo/bill.jpg', paymentType: 'Cash' } });
  assert.equal(created.status, 200);
  const corrected = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { vendor: 'Vendor Corrected', ledger: 'FOOD EXPENSE' }, role: 'owner' });
  assert.equal(corrected.status, 200);
  const approved = invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'accounting' });
  assert.equal(approved.status, 200);
  const config = invoke('GET', '/api/expenses/config').body;
  assert.ok(config.vendors.includes('Vendor Corrected'));
});

test('only owner can rename, merge and safely delete vendor ledgers', () => {
  invoke('POST','/api/expenses/vendors',{role:'owner',body:{nature:'SANKI',name:'Ferns N Petals'}});
  invoke('POST','/api/expenses/vendors',{role:'owner',body:{nature:'SANKI',name:'Ferns n Petal'}});
  const expense=invoke('POST','/api/expenses',{body:{vendor:'Ferns n Petal',amount:200,billPhoto:'/api/expenses/photo/bill.jpg',paymentType:'Cash'}}).body.expense;
  invoke('POST','/api/expenses/:id',{role:'owner',params:{id:expense.id},body:{ledger:'Flowers'}});
  invoke('POST','/api/expenses/:id/approve',{role:'owner',params:{id:expense.id}});
  const denied=invoke('POST','/api/expenses/vendors/manage/merge',{role:'admin',body:{nature:'SANKI',sourceName:'Ferns n Petal',targetName:'Ferns N Petals'}});
  assert.equal(denied.status,403);
  const merged=invoke('POST','/api/expenses/vendors/manage/merge',{role:'owner',body:{nature:'SANKI',sourceName:'Ferns n Petal',targetName:'Ferns N Petals'}});
  assert.equal(merged.status,200);assert.equal(merged.body.updatedExpenses,1);
  const vendors=invoke('GET','/api/expenses/vendors',{role:'owner',query:{nature:'SANKI'}}).body.vendors;
  assert.equal(vendors.filter(v=>v.name.toLowerCase().startsWith('ferns')).length,1);
  assert.equal(vendors.find(v=>v.name==='Ferns N Petals').count,1);
  const blocked=invoke('POST','/api/expenses/vendors/manage/delete',{role:'owner',body:{nature:'SANKI',name:'Ferns N Petals',reason:'cleanup'}});
  assert.equal(blocked.status,409);assert.match(blocked.body.error,/linked expense/);
  invoke('POST','/api/expenses/vendors',{role:'owner',body:{nature:'SANKI',name:'Unused Vendor'}});
  const deleted=invoke('POST','/api/expenses/vendors/manage/delete',{role:'owner',body:{nature:'SANKI',name:'Unused Vendor',reason:'duplicate created by mistake'}});
  assert.equal(deleted.status,200);
});

test('vendor ledger UI offers Delete only when its entry count is zero', () => {
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/v\.count===0\?' <button class="btn mini danger"/);
});

test('vendor ledgers can sort current outstanding amounts in both directions',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/id="vf_sort"/);
  assert.match(html,/Amount: high to low/);
  assert.match(html,/Amount: low to high/);
  assert.match(html,/sort==='amount_asc'/);
  assert.match(html,/sort==='amount_desc'/);
  assert.match(html,/Number\(a\.outstanding\|\|0\)-Number\(b\.outstanding\|\|0\)/);
  assert.match(html,/Number\(b\.outstanding\|\|0\)-Number\(a\.outstanding\|\|0\)/);
});

test('expense reference filter accepts partial numbers and compact rows are numbered', () => {
  const created=invoke('POST','/api/expenses',{body:{vendor:'Reference Search Vendor',amount:57,billPhoto:'/api/expenses/photo/bill.jpg',paymentType:'Cash'}}).body.expense;
  const digits=created.id.replace(/\D/g,'').replace(/^0+/, '');
  const found=invoke('GET','/api/expenses/list',{query:{reference:digits},role:'owner'}).body.expenses;
  assert.ok(found.some(e=>e.id===created.id));
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/Expense \/ bill no\./);
  assert.match(html,/expenseCard\(e,i\+1\)/);
  assert.match(html,/<b>#'\+\(vi\+1\)/);
});

test('bill remains mandatory at approval and payment proof is mandatory for cash', async () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', amount: 250, bill: 'printed', billPhoto: '/api/expenses/photo/bill.jpg',
    vendor: 'Vendor Corrected', paymentType: 'Cash'
  } });
  assert.equal(created.status, 200);
  const id = created.body.expense.id;

  const expenseFile = path.join(tempDir, 'expenses.json');
  const stored = JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
  stored.expenses[id].billPhoto = '';stored.expenses[id].billPhotos=[];
  fs.writeFileSync(expenseFile, JSON.stringify(stored));
  const blockedApproval = invoke('POST', '/api/expenses/:id/approve', { params: { id }, role: 'admin' });
  assert.equal(blockedApproval.status, 400);
  assert.match(blockedApproval.body.error, /Bill photo required/);

  invoke('POST', '/api/expenses/:id', { params: { id }, body: { billPhoto: '/api/expenses/photo/bill.jpg', ledger: 'FOOD EXPENSE' }, role: 'admin' });
  const approved = invoke('POST', '/api/expenses/:id/approve', { params: { id }, role: 'admin' });
  assert.equal(approved.status, 200);

  const blockedPayment = invoke('POST', '/api/expenses/:id/pay', { params: { id }, body: { account: 'Counter Cash' }, role: 'admin' });
  assert.equal(blockedPayment.status, 400);
  assert.match(blockedPayment.body.error, /Payment screenshot required/);

  const paid = invoke('POST', '/api/expenses/:id/pay', { params: { id }, body: { account: 'Counter Cash', paymentProof: '/api/expenses/photo/cash.jpg' }, role: 'admin' });
  assert.equal(paid.status, 200);
  assert.equal(paid.body.expense.status, 'paid');
});

test('one expense preserves multiple vendor bills and multiple payment proofs',()=>{
  const bills=['/api/expenses/photo/bill-a.jpg','/api/expenses/photo/bill-b.jpg'];
  const made=invoke('POST','/api/expenses',{body:{ledger:'FOOD EXPENSE',amount:450,bill:'printed',billPhoto:bills[0],billPhotos:bills,vendor:'Multi Proof Vendor',paymentType:'Cash'},role:'admin'});
  assert.equal(made.status,200);assert.deepEqual(made.body.expense.billPhotos,bills);
  const id=made.body.expense.id;assert.equal(invoke('POST','/api/expenses/:id/approve',{params:{id},role:'admin'}).status,200);
  const proofs=['/api/expenses/photo/pay-a.jpg','/api/expenses/photo/pay-b.jpg'];
  const paid=invoke('POST','/api/expenses/:id/pay',{params:{id},body:{account:'Counter Cash',paymentProof:proofs[0],paymentProofs:proofs},role:'admin'});
  assert.equal(paid.status,200);assert.deepEqual(paid.body.expense.paymentProofs,proofs);assert.deepEqual(paid.body.expense.payments.at(-1).proofs,proofs);
  const dashboard=invoke('GET','/api/expenses/spending-dashboard',{query:{},role:'admin'}).body.payments.find(x=>x.id===id);
  assert.deepEqual(dashboard.billPhotos,bills);assert.deepEqual(dashboard.proofs,proofs);
});

test('claimant identity is automatic and claimant-only fields are enforced server-side', () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', amount: 175, bill: 'printed', billPhoto: '/api/expenses/photo/bill.jpg',
    vendor: 'Vendor Corrected', type: 'fixed',
    claimant: 'spoofed-name', account: 'Private account', channel: 'POS', paymentType: 'UPI',
    qrPhoto: '/api/expenses/photo/vendor-qr.jpg'
  } });
  assert.equal(created.status, 200);
  assert.equal(created.body.expense.claimant, 'arshpreet');
  assert.equal(created.body.expense.account, '');
  assert.equal(created.body.expense.channel, 'Shared');
  assert.equal(created.body.expense.paymentType, 'UPI');
  assert.equal(created.body.expense.qrPhoto, '/api/expenses/photo/vendor-qr.jpg');
  assert.equal(created.body.expense.type, 'variable');
  assert.equal(created.body.expense.ledger, '');

  const edited = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: {
    claimant: 'another-spoof', channel: 'POS'
  }, role: 'claimant' });
  assert.equal(edited.body.expense.claimant, 'arshpreet');
  assert.equal(edited.body.expense.channel, 'Shared');

  const approverEdit = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { channel: 'POS' }, role: 'accounting' });
  assert.equal(approverEdit.body.expense.channel, 'POS');
});

test('UPI requires a vendor QR photo while Cash and Credit do not', () => {
  const upiWithoutQr = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', vendor: 'Vendor Corrected', amount: 100, bill: 'printed', billPhoto: '/api/expenses/photo/bill.jpg', paymentType: 'UPI'
  } });
  assert.equal(upiWithoutQr.status, 400);
  assert.match(upiWithoutQr.body.error, /QR-code photo is required/);

  for (const paymentType of ['Cash', 'Credit']) {
    const result = invoke('POST', '/api/expenses', { body: {
      ledger: 'FOOD EXPENSE', vendor: 'Vendor Corrected', amount: 100, bill: 'printed', billPhoto: '/api/expenses/photo/bill.jpg', paymentType
    } });
    assert.equal(result.status, 200);
    assert.equal(result.body.expense.paymentType, paymentType);
    assert.equal(result.body.expense.qrPhoto, '');
  }
});

test('SAMAST expenses are separate and only its accounting role can approve them', () => {
  const sankiPlBefore = summaryForPL();
  const created = invoke('POST', '/api/expenses', { body: {
    nature: 'SAMAST', vendor: 'Kirti Nagar Service', amount: 700,
    billPhoto: '/api/expenses/photo/samast-bill.jpg', paymentType: 'Cash'
  } });
  assert.equal(created.status, 200);
  assert.equal(created.body.expense.nature, 'SAMAST');
  invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'Operating Expense' }, role: 'owner' });

  const wrongBooks = invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'accounting' });
  assert.equal(wrongBooks.status, 403);
  const samastApproval = invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'samast_accounting' });
  assert.equal(samastApproval.status, 200);
  const adminConfig = invoke('GET', '/api/expenses/config', { role: 'admin' });
  assert.deepEqual(adminConfig.body.payingAccountsByNature.SAMAST, ['Prashant Axis 3645','Counter Cash','Prashant Cash','IndusInd Bank 7883','ICICI Bank 0993','ICICI Bank 0992','Kirti Nagar Cash']);
  const paid = invoke('POST', '/api/expenses/:id/pay', { params: { id: created.body.expense.id }, role: 'admin', body: { account:'Prashant Axis 3645',paymentProof:'/api/expenses/photo/samast-payment.jpg' } });
  assert.equal(paid.status, 200, JSON.stringify(paid.body));
  assert.equal(paid.body.expense.payments.at(-1).account, 'Prashant Axis 3645');
  assert.deepEqual(summaryForPL(), sankiPlBefore, 'SAMAST must never enter the SANKI P&L');

  const samastList = invoke('GET', '/api/expenses/list', { query: {}, role: 'samast_accounting' });
  assert.ok(samastList.body.expenses.length > 0);
  assert.ok(samastList.body.expenses.every(e => e.nature === 'SAMAST'));
  const sankiList = invoke('GET', '/api/expenses/list', { query: {}, role: 'accounting' });
  assert.ok(sankiList.body.expenses.every(e => (e.nature || 'SANKI') !== 'SAMAST'));
});

test('PERSONAL stays outside business P&L and its data is visible only to Owner', () => {
  const sankiPlBefore = summaryForPL();
  const personalConfig = invoke('GET', '/api/expenses/config', { role: 'personal_claimant' });
  assert.deepEqual(personalConfig.body.natures, ['PERSONAL']);
  const adminConfig = invoke('GET', '/api/expenses/config', { role: 'admin' });
  assert.ok(!adminConfig.body.natures.includes('PERSONAL'));
  assert.ok(!adminConfig.body.approvalNatures.includes('PERSONAL'));
  const claimantConfig = invoke('GET', '/api/expenses/config', { role: 'claimant' });
  assert.ok(claimantConfig.body.natures.includes('PERSONAL'), 'Pradeep, Shivam and Arshpreet claimant accounts can log PERSONAL expenses');

  const created = invoke('POST', '/api/expenses', { role: 'personal_claimant', body: {
    nature: 'PERSONAL', vendor: 'Private Vendor', amount: 900,
    billPhoto: '/api/expenses/photo/personal-bill.jpg', paymentType: 'Cash'
  } });
  assert.equal(created.status, 200);
  assert.equal(created.body.expense.nature, 'PERSONAL');

  const adminList = invoke('GET', '/api/expenses/list', { query: { nature: 'PERSONAL' }, role: 'admin' });
  assert.equal(adminList.status, 403);
  const adminApproval = invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'admin' });
  assert.equal(adminApproval.status, 403, 'Admin must not access PERSONAL approval data');

  invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'General Expense' }, role: 'owner' });
  const ownerApproval = invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'owner' });
  assert.equal(ownerApproval.status, 200);
  assert.deepEqual(summaryForPL(), sankiPlBefore, 'PERSONAL must never enter a business P&L');
});

test('audit log groups each expense into a readable complete lifecycle with user, device and IP', () => {
  const created=invoke('POST','/api/expenses',{role:'claimant',body:{nature:'SANKI',vendor:'Audit Timeline Vendor',particulars:'Audit test',amount:125.5,billPhoto:'/api/expenses/photo/audit.jpg',paymentType:'Cash'}}).body.expense;
  invoke('POST','/api/expenses/:id',{role:'admin',params:{id:created.id},body:{ledger:'General Expense'}});
  invoke('POST','/api/expenses/:id/approve',{role:'admin',params:{id:created.id}});
  invoke('POST','/api/expenses/:id/pay',{role:'admin',params:{id:created.id},body:{amount:125.5,account:'Counter Cash',paymentType:'Cash',paymentProof:'/api/expenses/photo/audit-payment.jpg'}});
  const result=invoke('GET','/api/expenses/audit-log',{role:'owner',query:{subject:created.id}});
  assert.equal(result.status,200);const record=result.body.records.find(x=>x.id===created.id);assert.ok(record);
  assert.deepEqual(record.timeline.map(x=>x.action),['CREATED','EDITED','APPROVED','PAYMENT_RECORDED']);
  assert.equal(record.timeline[0].user,'arshpreet');assert.equal(record.timeline[0].device,'Mobile');assert.equal(record.timeline[0].ip,'203.0.113.10');
  assert.equal(record.amount,125.5);assert.equal(record.status,'paid');
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8'));
  stored.auditLog=stored.auditLog.filter(x=>!(x.subjectId===created.id&&x.action==='CREATED'));fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const reconstructed=invoke('GET','/api/expenses/audit-log',{role:'owner',query:{subject:created.id}}).body.records.find(x=>x.id===created.id);
  assert.equal(reconstructed.timeline[0].action,'CREATED');assert.match(reconstructed.timeline[0].note,/reconstructed/i);assert.equal(reconstructed.timeline[0].user,'arshpreet');
});

test('deleted expenses remain visible in audit logs with snapshot, actor and required reason', () => {
  const created=invoke('POST','/api/expenses',{role:'claimant',body:{nature:'SANKI',vendor:'Deleted Audit Vendor',particulars:'Incorrect duplicate expense',amount:777,billPhoto:'/api/expenses/photo/deleted-audit.jpg',paymentType:'Cash'}}).body.expense;
  const refused=invoke('DELETE','/api/expenses/:id',{role:'admin',params:{id:created.id},body:{}});
  assert.equal(refused.status,400);
  const removed=invoke('DELETE','/api/expenses/:id',{role:'admin',params:{id:created.id},body:{reason:'Duplicate entered by mistake'}});
  assert.equal(removed.status,200);
  const result=invoke('GET','/api/expenses/audit-log',{role:'owner',query:{subject:created.id,action:'DELETED'}});
  const record=result.body.records.find(x=>x.id===created.id);
  assert.ok(record);
  assert.equal(record.status,'deleted');
  assert.equal(record.vendor,'Deleted Audit Vendor');
  const deleted=record.timeline.find(x=>x.action==='DELETED');
  assert.equal(deleted.note,'Duplicate entered by mistake');
  assert.equal(deleted.before.amount,777);
  assert.ok(deleted.user);
});

test('claimant form supports searchable direct vendor entry and phone gallery uploads', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /id="f_vendor"/);
  assert.match(html, /id="vendorSuggestions" class="combo-menu"/);
  assert.match(html, /No existing match/);
  assert.match(html, /id="f_totalamount"/);
  assert.doesNotMatch(html, /id="reqLedBtn"/);
  assert.doesNotMatch(html, /function requestLedger/);
  assert.match(html, /Have you already paid for this expense/);
  assert.match(html, /id="f_personalproof"/);
  assert.match(html, /id="f_installment"/);
  assert.match(html, /id="f_paymenttype"/);
  assert.match(html, /id="f_qrphoto"/);
  assert.match(html, /id="f_billcamera"[^>]*capture="environment"/);
  assert.match(html, /id="f_billfile"[^>]*accept="image\/\*"/);
  assert.doesNotMatch(html, /id="f_runner"/);
  assert.doesNotMatch(html, /id="f_funded"/);
  assert.doesNotMatch(html, /id="f_account"/);
  assert.match(html, /id="f_nature"/);
  assert.match(html, /SAMAST \(Kirti Nagar\)/);
});

test('Admin or Owner can assign a category directly in the pending expense list', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /placeholder="Search or create category…"/);
  assert.match(html, /window\.assignCategory\s*=\s*function/);
  assert.match(html, /New category created and assigned/);
});

test('approving an uncategorized expense opens a mobile-safe category review dialog', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /id="approveDlg"/);
  assert.match(html, /if\(!e\.ledger\)/);
  assert.match(html, /Assign category and approve/);
  assert.match(html, /approveCategory/);
  assert.match(html, /return api\('\/api\/expenses\/'\+approveId\+'\/approve'/);
  assert.match(html, /list="approvalLedgerList"/);
  assert.match(html, /type a new category name to create it/);
  assert.match(html, /New category created and assigned/);
});

test('payment dialog lists stored accounts and lets Admin or Owner add one', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  const server = fs.readFileSync(path.join(__dirname, '..', 'modules', 'expenses.js'), 'utf8');
  assert.match(html, /id="payAddAcct"/);
  assert.match(html, /api\('\/api\/expenses\/accounts'/);
  assert.match(server, /function storedAccountNames\(s\)/);
  assert.match(server, /router\.post\('\/api\/expenses\/accounts'/);
  assert.match(server, /Select the account used for this payment/);
});

test('claimant list defaults to all own entities and exposes payment proof', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /<option value="">All<\/option>/);
  assert.match(html, /paymentProofs=e\.paymentProofs/);
  assert.match(html, /proofGallery\(paymentProofs,'Payment'\)/);
});

test('expense deep links load only the selected record and normal lists are bounded', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  const server = fs.readFileSync(path.join(__dirname, '..', 'modules', 'expenses.js'), 'utf8');
  assert.match(html, /focusedExpenseId/);
  assert.match(html, /&limit=75/);
  assert.match(html, /&id='\+encodeURIComponent\(focused\)/);
  assert.match(html, /card\.open=true/);
  assert.match(server, /if \(id && e\.id !== id\) return false/);
  assert.match(server, /hasMore: totalCount > list\.length/);
});

test('expense history uses expandable compact rows for every role', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /function expenseCard\(e,rowNo\)/);
  assert.match(html, /<details class="claim-card"/);
  assert.match(html, /<summary>/);
  assert.match(html, /class="claim-detail"/);
  assert.match(html, /d\.expenses\.map\(function\(e,i\)\{return expenseCard\(e,i\+1\);\}\)/);
  assert.match(html, /Payment history/);
});

test('approvers get a pending payments workspace with balances and partial-payment history', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  const server = fs.readFileSync(path.join(__dirname, '..', 'modules', 'expenses.js'), 'utf8');
  assert.match(html, /data-t="pending"/);
  assert.match(html, /function renderPendingPayments\(\)/);
  assert.match(html, /Record payment/);
  assert.match(server, /router\.get\('\/api\/expenses\/pending-payments'/);
  assert.match(server, /\['approved', 'partially_paid'\]\.includes\(e\.status\)/);
  assert.match(server, /canApproveExpenseNature\(req, e\)/);
});

test('account transfers create equal debit and credit ledger entries', () => {
  const before = invoke('GET', '/api/expenses/balances', { query: { nature: 'SANKI' }, role: 'owner' }).body.accounts;
  const cashBefore = before.find(a => a.name === 'Counter Cash').balance;
  const paytmBefore = before.find(a => a.name === 'Tiana 0425').balance;
  const transfer = invoke('POST', '/api/expenses/transfers', { role: 'owner', body: {
    nature: 'SANKI', fromAccount: 'Counter Cash', toAccount: 'Tiana 0425', amount: 250,
    date: '2026-08-22', proof: '/api/expenses/photo/transfer.jpg', note: 'Test transfer'
  } });
  assert.equal(transfer.status, 200);
  const after = invoke('GET', '/api/expenses/balances', { query: { nature: 'SANKI' }, role: 'owner' }).body.accounts;
  assert.equal(after.find(a => a.name === 'Counter Cash').balance, cashBefore - 250);
  assert.equal(after.find(a => a.name === 'Tiana 0425').balance, paytmBefore + 250);
  const laterPeriod=invoke('GET','/api/expenses/balances',{query:{nature:'SANKI',from:'2026-08-23',to:'2026-08-31'},role:'owner'}).body.accounts.find(a=>a.name==='Tiana 0425');
  assert.equal(laterPeriod.transferredIn,0);
  assert.equal(laterPeriod.balance,paytmBefore+250); // closing balance retains earlier money
  const cashLedger = invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account: 'Counter Cash' }, role: 'owner' }).body;
  const paytmLedger = invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account: 'Tiana 0425' }, role: 'owner' }).body;
  assert.equal(cashLedger.entries.find(x => x.id === transfer.body.transfer.id).debit, 250);
  assert.equal(paytmLedger.entries.find(x => x.id === transfer.body.transfer.id).credit, 250);
  const personalBlocked = invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'PERSONAL', account: 'Counter Cash' }, role: 'admin' });
  assert.equal(personalBlocked.status, 403);
});

test('only Owner can delete a transfer and both ledger sides disappear with an audit trail', () => {
  const made=invoke('POST','/api/expenses/transfers',{role:'owner',body:{nature:'SANKI',fromAccount:'Counter Cash',toAccount:'Tiana 0425',amount:321,date:'2026-08-25',proof:'/api/expenses/photo/delete-transfer.jpg',note:'Temporary transfer'}});
  const id=made.body.transfer.id;
  const denied=invoke('POST','/api/expenses/transfers/:id/delete',{role:'admin',params:{id},body:{reason:'Wrong entry'}});assert.equal(denied.status,403);
  const missingReason=invoke('POST','/api/expenses/transfers/:id/delete',{role:'owner',params:{id},body:{}});assert.equal(missingReason.status,400);
  const removed=invoke('POST','/api/expenses/transfers/:id/delete',{role:'owner',params:{id},body:{reason:'Entered by mistake'}});assert.equal(removed.status,200);
  const cash=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Counter Cash'}}).body;
  const tiana=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Tiana 0425'}}).body;
  assert.ok(!cash.entries.some(x=>x.id===id));assert.ok(!tiana.entries.some(x=>x.id===id));
  const auditRows=invoke('GET','/api/expenses/audit-log',{role:'owner'}).body.entries||invoke('GET','/api/expenses/audit-log',{role:'owner'}).body.auditLog||[];
  assert.ok(auditRows.some(x=>x.subjectId===id&&x.action==='TRANSFER_DELETED'));
});

test('claimant accounts are valid transfer endpoints in the app and Telegram', () => {
  const config=invoke('GET','/api/expenses/config',{role:'owner'}).body;
  assert.ok(config.transferAccountsByNature.SANKI.includes('Arshpreet 1919'));
  assert.ok(config.transferAccountsByNature.SAMAST.includes('Arshpreet 1919'));
  assert.ok(!config.accountsByNature.SANKI.includes('Arshpreet 1919'));
  const appTransfer=invoke('POST','/api/expenses/transfers',{role:'owner',body:{fromNature:'SANKI',fromAccount:'Axis Bank 3448',toNature:'SANKI',toAccount:'Arshpreet 1919',amount:1000,date:'2026-08-26',proof:'/api/expenses/photo/claimant-transfer.jpg'}});
  assert.equal(appTransfer.status,200);
  assert.equal(appTransfer.body.transfer.toAccount,'Arshpreet 1919');
  const telegramTransfer=telegramRecordTransfer('gaganlambasanki',{fromAccount:'3448',toAccount:'1919',amount:500,date:'2026-08-26',proof:'/api/expenses/photo/claimant-transfer-tg.jpg'});
  assert.equal(telegramTransfer.success,true);
  assert.equal(telegramTransfer.transfer.fromNature,'SANKI');
  assert.equal(telegramTransfer.transfer.toNature,'SANKI');
  assert.equal(telegramTransfer.transfer.toAccount,'Arshpreet 1919');
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/transferAccountsByNature/);
});

test('asset-sale receipt and cross-entity withdrawal create a complete money trail', () => {
  const receipt=invoke('POST','/api/expenses/receipts',{role:'owner',body:{nature:'SAMAST',account:'Kirti Nagar Cash',receiptType:'asset_sale',source:'Buyer of basement iron racks',amount:27500,date:'2026-08-22',note:'Iron racks sold',proof:'/api/expenses/photo/rack-sale.jpg'}});
  assert.equal(receipt.status,200);
  const transfer=invoke('POST','/api/expenses/transfers',{role:'owner',body:{fromNature:'SAMAST',fromAccount:'Kirti Nagar Cash',toNature:'PERSONAL',toAccount:'Gagan Personal Cash',classification:'owner_withdrawal',amount:27500,date:'2026-08-22',note:'Cash used for personal nanny payment',proof:'/api/expenses/photo/cash-transfer.jpg'}});
  assert.equal(transfer.status,200);
  const samast=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SAMAST',account:'Kirti Nagar Cash'}}).body;
  const personal=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'PERSONAL',account:'Gagan Personal Cash'}}).body;
  assert.equal(samast.entries.find(x=>x.id===receipt.body.receipt.id).credit,27500);
  assert.equal(samast.entries.find(x=>x.id===transfer.body.transfer.id).debit,27500);
  assert.equal(personal.entries.find(x=>x.id===transfer.body.transfer.id).credit,27500);
  assert.match(personal.entries.find(x=>x.id===transfer.body.transfer.id).description,/owner withdrawal/);
  const samastBalance=invoke('GET','/api/expenses/balances',{role:'owner',query:{nature:'SAMAST'}}).body.accounts.find(x=>x.name==='Kirti Nagar Cash');
  const personalBalance=invoke('GET','/api/expenses/balances',{role:'owner',query:{nature:'PERSONAL'}}).body.accounts.find(x=>x.name==='Gagan Personal Cash');
  assert.equal(samastBalance.balance,0);
  assert.equal(personalBalance.balance,27500);
});

test('Owner may declare a cash receipt without proof only with a reason', () => {
  const missingReason=invoke('POST','/api/expenses/receipts',{role:'owner',body:{nature:'SAMAST',account:'Kirti Nagar Cash',receiptType:'asset_sale',source:'Iron racks buyer',amount:27500}});assert.equal(missingReason.status,400);
  const declared=invoke('POST','/api/expenses/receipts',{role:'owner',body:{nature:'SAMAST',account:'Kirti Nagar Cash',receiptType:'asset_sale',source:'Iron racks buyer',amount:27500,note:'Cash received; no external proof was available.'}});assert.equal(declared.status,200);assert.match(declared.body.receipt.proofException,/Owner cash declaration/);
  const adminDenied=invoke('POST','/api/expenses/receipts',{role:'admin',body:{nature:'SANKI',account:'Counter Cash',receiptType:'other_income',source:'Cash source',amount:100,note:'No proof'}});assert.equal(adminDenied.status,403);
});

test('account adjustments require a reason and support explicit add or deduct entries', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /Add account adjustment/);
  assert.match(html, /Reason — required/);
  assert.doesNotMatch(html, /\＋ Top up/);
  const blocked = invoke('POST', '/api/expenses/balances', { role: 'owner', body: { nature: 'SANKI', adjust: { account: 'Counter Cash', direction: 'add', amount: 100 } } });
  assert.equal(blocked.status, 400);
  assert.match(blocked.body.error, /reason is required/i);
  const before = invoke('GET', '/api/expenses/balances', { query: { nature: 'SANKI' }, role: 'owner' }).body.accounts.find(a => a.name === 'Counter Cash').balance;
  assert.equal(invoke('POST', '/api/expenses/balances', { role: 'owner', body: { nature: 'SANKI', adjust: { account: 'Counter Cash', direction: 'add', amount: 500, date: '2026-08-22', note: 'Cash introduced', proof: '/api/expenses/photo/adjust.jpg' } } }).status, 200);
  assert.equal(invoke('POST', '/api/expenses/balances', { role: 'owner', body: { nature: 'SANKI', adjust: { account: 'Counter Cash', direction: 'deduct', amount: 125, date: '2026-08-22', note: 'Counting correction' } } }).status, 200);
  const after = invoke('GET', '/api/expenses/balances', { query: { nature: 'SANKI' }, role: 'owner' }).body.accounts.find(a => a.name === 'Counter Cash').balance;
  assert.equal(after, before + 375);
  const ledger = invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account: 'Counter Cash' }, role: 'owner' }).body;
  assert.equal(ledger.entries.find(x => x.description === 'Cash introduced').proof, '/api/expenses/photo/adjust.jpg');
  assert.equal(ledger.entries.find(x => x.description === 'Counting correction').debit, 125);
});

test('date-range spending dashboard shows only actual payment transactions', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.doesNotMatch(html, /<label>Breakdown by<\/label>/);
  assert.match(html, /Actual expenses paid/);
  assert.match(html, /id="sd_account"/);
  assert.match(html, /id="sd_category"/);
  assert.match(html, /id="sd_category_go">Show spending by category/);
  assert.doesNotMatch(html, /id="sd_view"/);
  const created = invoke('POST', '/api/expenses', { body: {
    vendor: 'Dashboard Vendor', amount: 900, date: '2026-08-20', billPhoto: '/api/expenses/photo/dash-bill.jpg', paymentType: 'Credit'
  } });
  invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'FOOD EXPENSE' }, role: 'owner' });
  invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'owner' });
  invoke('POST', '/api/expenses/:id/pay', { params: { id: created.body.expense.id }, body: { amount:339, account:'Counter Cash', date:'2026-08-20', paymentProof:'/api/expenses/photo/dashboard-pay.jpg' }, role:'owner' });
  const personal = invoke('POST', '/api/expenses', { role: 'owner', body: { nature: 'PERSONAL', ledger: 'FOOD EXPENSE', vendor: 'Private Dashboard Vendor', amount: 100, date: '2026-08-20', billPhoto: '/api/expenses/photo/private-dash.jpg', paymentType: 'Credit' } });
  invoke('POST', '/api/expenses/:id/approve', { params: { id: personal.body.expense.id }, role: 'owner' });
  const owner = invoke('GET', '/api/expenses/spending-dashboard', { query: { from: '2026-08-20', to: '2026-08-20' }, role: 'owner' });
  assert.equal(owner.status, 200);
  assert.ok(owner.body.totalPaid >= 339);
  const payment = owner.body.payments.find(x => x.id === created.body.expense.id);
  assert.equal(payment.amount, 339);
  assert.equal(payment.account, 'Counter Cash');
  assert.equal(payment.proof, '/api/expenses/photo/dashboard-pay.jpg');
  assert.equal(payment.reference, created.body.expense.id + '/PAY-001');
  const byCategory=invoke('GET','/api/expenses/spending-dashboard',{query:{from:'2026-08-20',to:'2026-08-20',category:'FOOD EXPENSE'},role:'owner'}).body;
  assert.ok(byCategory.payments.length>0);
  assert.ok(byCategory.payments.every(x=>x.category==='FOOD EXPENSE'));
  const admin = invoke('GET', '/api/expenses/spending-dashboard', { query: { from: '2026-08-20', to: '2026-08-20' }, role: 'admin' });
  assert.equal(admin.status, 200);
  assert.equal(admin.body.payments.some(x => x.entity === 'PERSONAL'), false);
  const claimant = invoke('GET', '/api/expenses/spending-dashboard', { role: 'claimant' });
  assert.equal(claimant.status, 403);
});

test('receivables support partial collections and credit the receiving account ledger', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /data-t="receivables"/);
  assert.match(html, /Record money received/);
  const before = invoke('GET', '/api/expenses/balances', { query: { nature: 'SANKI' }, role: 'owner' }).body.accounts.find(a => a.name === 'Counter Cash').balance;
  const created = invoke('POST', '/api/expenses/receivables', { role: 'owner', body: { nature:'SANKI',party:'Refund Vendor',reason:'Purchase refund',amount:1000,date:'2026-08-20',dueDate:'2026-08-25' } });
  assert.equal(created.status, 200);
  const blocked = invoke('POST', '/api/expenses/receivables/:id/receive', { params:{id:created.body.receivable.id},role:'owner',body:{amount:400,account:'Counter Cash'} });
  assert.equal(blocked.status, 400);
  assert.match(blocked.body.error,/proof is required/i);
  const partial = invoke('POST', '/api/expenses/receivables/:id/receive', { params:{id:created.body.receivable.id},role:'owner',body:{amount:400,account:'Counter Cash',date:'2026-08-22',proof:'/api/expenses/photo/collection.jpg'} });
  assert.equal(partial.body.receivable.status,'partially_received');
  const list = invoke('GET','/api/expenses/receivables',{query:{nature:'SANKI'},role:'owner'}).body;
  assert.equal(list.receivables.find(x=>x.id===created.body.receivable.id).receivedAmount,400);
  const after = invoke('GET', '/api/expenses/balances', { query: { nature: 'SANKI' }, role: 'owner' }).body.accounts.find(a => a.name === 'Counter Cash').balance;
  assert.equal(after,before+400);
  const ledger=invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account:'Counter Cash'},role:'owner'}).body;
  assert.equal(ledger.entries.find(x=>x.id===created.body.receivable.id+'/COL-001').credit,400);
  const personal=invoke('POST','/api/expenses/receivables',{role:'owner',body:{nature:'PERSONAL',party:'Private',reason:'Loan return',amount:100}});
  assert.equal(personal.status,200);
  assert.equal(invoke('GET','/api/expenses/receivables',{query:{nature:'PERSONAL'},role:'admin'}).status,403);
});

test('Shopify history is filtered by the permanent accounting reset boundary', () => {
  const pl = fs.readFileSync(path.join(__dirname, '..', 'modules', 'pl.js'), 'utf8');
  const orders = fs.readFileSync(path.join(__dirname, '..', 'modules', 'orders.js'), 'utf8');
  assert.match(pl, /accounting-boundary\.json/);
  assert.match(pl, /filter\(o => !startAt/);
  assert.match(orders, /const resetBoundary = accountingStartAt\(\)/);
  assert.match(orders, /Order predates the accounting reset/);
});

test('Telegram setup is Owner-only and supports per-user notification management', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  const telegram = fs.readFileSync(path.join(__dirname, '..', 'modules', 'telegram.js'), 'utf8');
  assert.match(html, /cfg\.isOwner\?'<div class="card"><b>Telegram notifications \(Owner only\)/);
  assert.match(html, /id="telegramUser"/);
  assert.match(html, /Test all linked accounts/);
  assert.match(html, /Disconnect all/);
  assert.match(html, /Add another Telegram/);
  assert.match(html, /Disconnect this account/);
  assert.doesNotMatch(html, /Link your own Telegram/);
  assert.match(telegram, /router\.post\('\/api\/telegram\/link'.*if\(!owner\(req\)\)/);
  assert.match(telegram, /router\.post\('\/api\/telegram\/test'.*if\(!owner\(req\)\)/);
  assert.match(telegram, /router\.post\('\/api\/telegram\/unlink'.*if\(!owner\(req\)\)/);
});

test('one app user can retain multiple Telegram notification accounts', () => {
  const { chatIdsForUser, linkedAccounts } = require('../modules/telegram');
  assert.deepEqual(chatIdsForUser({ userChats:{prashant:'111'} }, 'Prashant'), ['111']);
  const multi={userChats:{prashant:['111','222','111']},chatMeta:{'111':{telegramName:'Prashant phone'},'222':{telegramName:'Office Telegram'}}};
  assert.deepEqual(chatIdsForUser(multi,'prashant'),['111','222']);
  assert.deepEqual(linkedAccounts(multi),[{username:'prashant',count:2,connections:[{chatId:'111',label:'Prashant phone',linkedAt:''},{chatId:'222',label:'Office Telegram',linkedAt:''}]}]);
});

test('Owner Telegram narration and screenshot OCR create one categorized paid PERSONAL expense', () => {
  const {parsePersonalCaption,parsePersonalIntent,parsePaymentOcr,parseReceiptOcr,inferPersonalCategory,applyCaptureEdit,capturePreview}=require('../modules/telegram');
  const parsed=parsePersonalCaption('Personal | Nanny salary August | ICICI 0993 | ₹27,500');
  assert.deepEqual(parsed,{ok:true,amount:27500,account:'ICICI 0993',particulars:'Nanny salary August',date:''});
  assert.deepEqual(parsePersonalCaption('Personal Food tip 0993 200'),{ok:true,amount:200,account:'0993',particulars:'Food tip',date:''});
  assert.equal(parsePersonalCaption('Personal | Nanny salary').ok,false);
  assert.deepEqual(parsePersonalIntent('Food tip'),{ok:true,particulars:'Food tip',received:false,cash:false,upi:false,amount:0});
  assert.deepEqual(parsePersonalIntent('food tip 200'),{ok:true,particulars:'food tip',received:false,cash:false,upi:false,amount:200});
  assert.deepEqual(parsePersonalIntent('personal cash Nanny payment 27500'),{ok:true,particulars:'Nanny payment',received:false,cash:true,upi:false,amount:27500});
  assert.deepEqual(parsePersonalIntent('Personal upi Received Refund from Mukesh'),{ok:true,particulars:'Refund from Mukesh',received:true,cash:false,upi:true,amount:0});
  assert.equal(parsePersonalIntent('SAMAST electrician payment').ok,false);
  const ocr=parsePaymentOcr('Transaction Successful\n23 August 2026 at 4:14 PM\nPaid to\nMr MUKESH KUMAR ₹200\nDebited from\nXXXXXXXXXXX93 ₹200\nUTR: 412656746520');
  assert.equal(ocr.amount,200);assert.equal(ocr.amountConfidence,true);assert.equal(ocr.account,'93');assert.equal(ocr.recipient,'Mr MUKESH KUMAR');assert.equal(ocr.date,'2026-08-23');
  const safeOcr=parsePaymentOcr('Transaction Successful\nPaid to\nFOOD RESTAURANT 35190\nAmount ₹5,000.50\nDebited from XXXXX0993\n₹5,000.50\nUTR 351901234567');
  assert.equal(safeOcr.amount,5000.5,'OCR must ignore trailing account/UTR-like numbers and preserve paise');
  assert.equal(safeOcr.amountConfidence,true);
  const ambiguousOcr=parsePaymentOcr('Payment successful\nPaid to ZOMATO ₹35,121\nAvailable balance ₹5,121');
  assert.equal(ambiguousOcr.amountConfidence,false,'conflicting screenshot amounts must require owner confirmation');
  assert.deepEqual(inferPersonalCategory('Food tip',ocr.recipient),{ledger:'Food & Dining',confidence:true});
  const input={amount:200,account:'93',particulars:'Food tip',vendor:ocr.recipient,ledger:'Food & Dining',needsReview:false,ocrText:ocr.text,username:'gaganlambasanki',proof:'/api/expenses/photo/personal-telegram.jpg',sourceKey:'telegram:owner:file-1',rawNarration:'Personal Food tip'};
  const created=createTelegramPersonalExpense(input);
  assert.equal(created.success,true);assert.equal(created.expense.nature,'PERSONAL');assert.equal(created.expense.status,'paid');assert.equal(created.expense.account,'ICICI Bank 0993');assert.equal(created.expense.approvedBy,'gaganlambasanki');assert.equal(created.expense.telegramNeedsReview,false);assert.equal(created.expense.ledger,'Food & Dining');
  const corrected=invoke('POST','/api/expenses/:id',{role:'owner',params:{id:created.expense.id},body:{amount:121,requestedAmount:121,paidAlready:true,paymentType:'UPI',personalAccount:'ICICI Bank 0993',editReason:'Correct Telegram OCR amount'}});
  assert.equal(corrected.status,200);assert.equal(corrected.body.expense.amount,121);assert.equal(corrected.body.expense.paidAmount,121);assert.equal(corrected.body.expense.personalPaidAmount,121);assert.equal(corrected.body.expense.payments.find(p=>p.personalFunds).amount,121);
  const adminAmountEdit=invoke('POST','/api/expenses/:id',{role:'admin',params:{id:created.expense.id},body:{amount:122,requestedAmount:122,editReason:'Admin tries amount change'}});
  assert.equal(adminAmountEdit.status,403);assert.match(adminAmountEdit.body.error,/Only the Owner/);
  const vendors=invoke('GET','/api/expenses/vendors',{role:'owner',query:{nature:'PERSONAL'}}).body.vendors;assert.ok(vendors.some(v=>v.name==='Mr MUKESH KUMAR'));
  assert.match(fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8'),/NEEDS REVIEW/);
  const duplicate=createTelegramPersonalExpense(input);assert.equal(duplicate.duplicate,true);assert.equal(duplicate.expense.id,created.expense.id);
  const draft={kind:'expense',payload:{amount:35000,account:'93',particulars:'Food restaurant',vendor:'Restaurant',ledger:'Food & Dining'}};
  assert.match(capturePreview(draft),/Nothing is recorded until you press Confirm/);
  assert.equal(applyCaptureEdit(draft,'Amount 5000'),true);assert.equal(draft.payload.amount,5000);
  assert.equal(applyCaptureEdit(draft,'Amount 94.5'),true);assert.equal(draft.payload.amount,94.5);assert.match(capturePreview(draft),/₹94\.5/);
  assert.equal(applyCaptureEdit(draft,'Account 0992'),true);assert.equal(draft.payload.account,'0992');
  assert.equal(applyCaptureEdit(draft,'Particulars Dinner restaurant'),true);assert.equal(draft.payload.particulars,'Dinner restaurant');
  const telegramSource=fs.readFileSync(path.join(__dirname,'..','modules','telegram.js'),'utf8');
  assert.match(telegramSource,/callback_data:'personal_confirm'/);assert.match(telegramSource,/allowed_updates:\['message','callback_query'\]/);
  const received=parseReceiptOcr('Money received\n23 August 2026\nReceived from\nMukesh Kumar ₹1,500\nCredited to\nXXXXXXXXXXX93');
  assert.deepEqual({amount:received.amount,account:received.account,source:received.source,date:received.date},{amount:1500,account:'93',source:'Mukesh Kumar',date:'2026-08-23'});
  const receipt=createTelegramPersonalReceipt({amount:received.amount,account:received.account,source:'Refund from Mukesh',date:received.date,proof:'/api/expenses/photo/received.jpg',username:'gaganlambasanki',sourceKey:'telegram:owner:receipt-1',ocrText:received.text});
  assert.equal(receipt.success,true);assert.equal(receipt.receipt.account,'ICICI Bank 0993');assert.equal(receipt.receipt.nature,'PERSONAL');assert.equal(receipt.receipt.receiptType,'refund');
});

test('Prashant Telegram menu records a screenshot-first paid expense only after selected account and category', () => {
  const categories=telegramBusinessCategories();
  assert.ok(categories.includes('PETROL'));
  const input={username:'prashant',nature:'SANKI',vendor:'Fuel Station',particulars:'Petrol',ledger:'PETROL',amount:720,account:'Prashant Axis 3645',proof:'/api/expenses/photo/quick-paid.jpg',sourceKey:'telegram-quick:test-1'};
  const created=createTelegramBusinessPaidExpense(input);
  assert.equal(created.success,true);
  assert.equal(created.expense.status,'paid');
  assert.equal(created.expense.account,'Prashant Axis 3645');
  assert.equal(created.expense.approvedBy,'prashant');
  assert.equal(created.expense.payments[0].proof,input.proof);
  assert.equal(createTelegramBusinessPaidExpense(input).duplicate,true);
  assert.equal(createTelegramBusinessPaidExpense({...input,sourceKey:'telegram-quick:test-2',account:'Axis Bank 3448'}).success,false);
  const telegramSource=fs.readFileSync(path.join(__dirname,'..','modules','telegram.js'),'utf8');
  assert.match(telegramSource,/Attach the payment screenshot now/);
  assert.match(telegramSource,/Prashant Axis 3645','Prashant Cash','Counter Cash/);
  assert.match(telegramSource,/Best matching categories/);
});

test('only Admin or Owner can add a missing category during review', () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', vendor: 'Vendor Corrected', amount: 90,
    billPhoto: '/api/expenses/photo/bill.jpg', paymentType: 'Cash'
  } });
  const denied = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'New Review Category' }, role: 'accounting' });
  assert.equal(denied.status, 403);
  const added = invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'New Review Category', type: 'running' }, role: 'owner' });
  assert.equal(added.status, 200);
  assert.equal(added.body.expense.ledger, 'New Review Category');
  const config = invoke('GET', '/api/expenses/config', { role: 'owner' }).body;
  assert.ok(config.ledgers.some(l => l.name === 'New Review Category' && l.type === 'running'));
});

test('personally paid expense becomes reimbursement pending only after approval', () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'FOOD EXPENSE', vendor: 'Personal Vendor', amount: 500, billPhoto: '/api/expenses/photo/bill.jpg',
    paidAlready: true, paymentType: 'UPI', personalAccount: 'Arshpreet 1919', personalPaymentProof: '/api/expenses/photo/personal.jpg'
  } });
  assert.equal(created.status, 200);
  assert.equal(created.body.expense.reimbursementStatus, 'awaiting_approval');
  invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'FOOD EXPENSE' }, role: 'owner' });
  const approved = invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'accounting' });
  assert.equal(approved.body.expense.reimbursementStatus, 'pending');
  assert.equal(approved.body.expense.status, 'paid');
  assert.equal(approved.body.expense.vendorPaymentCompleted, true);
  const blocked = invoke('POST', '/api/expenses/:id/reimburse', { params: { id: created.body.expense.id }, body: { amount: 500 }, role: 'accounting' });
  assert.equal(blocked.status, 400);
  const reimbursed = invoke('POST', '/api/expenses/:id/reimburse', { params: { id: created.body.expense.id }, body: { amount: 500, account: 'Counter Cash', paymentProof: '/api/expenses/photo/reimburse.jpg' }, role: 'admin' });
  assert.equal(reimbursed.body.expense.reimbursementStatus, 'reimbursed');
  assert.equal(reimbursed.body.expense.paidAmount, 500);
});

test('Owner can reimburse a SAMAST claimant from an authorised SANKI account', () => {
  const created=invoke('POST','/api/expenses',{role:'owner',body:{nature:'SAMAST',ledger:'KN Expenses',vendor:'Batra Paints',amount:720,billPhoto:'/api/expenses/photo/batra.jpg',paidAlready:true,paymentType:'UPI',personalAccount:'Arshpreet 1919',personalPaymentProof:'/api/expenses/photo/arshpreet-pay.jpg'}});
  assert.equal(created.status,200);
  invoke('POST','/api/expenses/:id',{params:{id:created.body.expense.id},body:{ledger:'KN Expenses'},role:'owner'});
  invoke('POST','/api/expenses/:id/approve',{params:{id:created.body.expense.id},role:'owner'});
  const reimbursed=invoke('POST','/api/expenses/:id/reimburse',{params:{id:created.body.expense.id},role:'owner',body:{amount:720,account:'Prashant Axis 3645',paymentProof:'/api/expenses/photo/prashant-reimburse.jpg'}});
  assert.equal(reimbursed.status,200);
  assert.equal(reimbursed.body.expense.reimbursementStatus,'reimbursed');
  assert.equal(reimbursed.body.expense.reimbursementPayments.at(-1).account,'Prashant Axis 3645');
  assert.deepEqual(reimbursed.body.expense.reimbursementPayments.at(-1).accountNatures,['SANKI']);
  const ledger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Prashant Axis 3645'}}).body;
  assert.equal(ledger.entries.find(x=>x.id===created.body.expense.id+'/REIM-001').debit,720);
  const claimantLedger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SAMAST',account:'Arshpreet 1919'}}).body;
  assert.equal(claimantLedger.entries.find(x=>x.id===created.body.expense.id+'/PAY-001').debit,720);
  assert.equal(claimantLedger.entries.find(x=>x.id===created.body.expense.id+'/REIM-001/RECEIVED').credit,720);
  assert.equal(claimantLedger.balance,0);
  const reimbursementHistory=invoke('GET','/api/expenses/reimbursements',{role:'owner',query:{status:'reimbursed'}}).body.reimbursements.find(x=>x.id===created.body.expense.id);
  assert.equal(reimbursementHistory.closingBalance,0);
  assert.equal(reimbursementHistory.reimbursementPayments.at(-1).transactionReference,created.body.expense.id+'/REIM-001');
  const claimantSummary=invoke('GET','/api/expenses/balances',{role:'owner',query:{nature:'SAMAST'}}).body;
  assert.equal(claimantSummary.accounts.find(x=>x.name==='Arshpreet 1919').balance,0);
});

test('multiple pending expenses can be reimbursed together and debit the paying account ledger',()=>{
  const ids=[410,590].map((amount,i)=>{
    const made=invoke('POST','/api/expenses',{role:'owner',body:{nature:'SANKI',ledger:'FOOD EXPENSE',vendor:'Batch Vendor '+i,amount,billPhoto:'/api/expenses/photo/bill-'+i+'.jpg',paidAlready:true,paymentType:'UPI',personalAccount:'Arshpreet 1919',personalPaymentProof:'/api/expenses/photo/personal-'+i+'.jpg'}}).body.expense;
    invoke('POST','/api/expenses/:id',{params:{id:made.id},body:{ledger:'FOOD EXPENSE'},role:'owner'});invoke('POST','/api/expenses/:id/approve',{params:{id:made.id},role:'owner'});return made.id;
  });
  const paid=invoke('POST','/api/expenses/reimbursements/batch',{role:'owner',body:{expenseIds:ids,account:'Counter Cash',date:'2026-08-24',paymentType:'UPI',paymentProof:'/api/expenses/photo/batch-reimbursement.jpg',note:'Combined claimant transfer'}});
  assert.equal(paid.status,200);assert.equal(paid.body.total,1000);assert.match(paid.body.batchId,/^RB-/);assert.equal(paid.body.expenses.length,2);
  assert.ok(paid.body.expenses.every(e=>e.reimbursementStatus==='reimbursed'&&e.reimbursementPayments.at(-1).batchId===paid.body.batchId));
  const ledger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Counter Cash',from:'2026-08-22',to:'2026-08-31'}}).body;
  assert.equal(ids.reduce((n,id)=>n+ledger.entries.filter(x=>x.id.startsWith(id+'/REIM-')).reduce((m,x)=>m+x.debit,0),0),1000);
});

test('reimbursements UI groups transactions by person before showing expense details',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/class="claim-card reimbursement-person"/);
  assert.match(html,/group\.items\.length\+' transaction'/);
  assert.match(html,/class="reimburse-person-select"/);
  assert.match(html,/Select all pending for /);
  assert.match(html,/Closing balance '\+fmt\(group\.due\)/);
  assert.match(html,/Transaction reference/);
  assert.match(html,/Reimbursed<\/th><th>Closing balance/);
});

test('batch reimbursement validates every expense before recording any payment',()=>{
  const made=invoke('POST','/api/expenses',{role:'owner',body:{nature:'SANKI',ledger:'FOOD EXPENSE',vendor:'Atomic Batch Vendor',amount:300,billPhoto:'/api/expenses/photo/atomic.jpg',paidAlready:true,paymentType:'UPI',personalAccount:'Arshpreet 1919',personalPaymentProof:'/api/expenses/photo/atomic-pay.jpg'}}).body.expense;
  invoke('POST','/api/expenses/:id',{params:{id:made.id},body:{ledger:'FOOD EXPENSE'},role:'owner'});invoke('POST','/api/expenses/:id/approve',{params:{id:made.id},role:'owner'});
  const blocked=invoke('POST','/api/expenses/reimbursements/batch',{role:'owner',body:{expenseIds:[made.id,'EX-99999'],account:'Counter Cash',paymentProof:'/api/expenses/photo/batch.jpg'}});
  assert.equal(blocked.status,404);
  const after=invoke('GET','/api/expenses/reimbursements',{role:'owner',query:{status:'pending'}}).body.reimbursements.find(e=>e.id===made.id);
  assert.equal(after.reimbursementAmount,0);assert.equal(after.reimbursementStatus,'pending');
});

test('personally paid non-cash expense requires the account used and cash is named automatically', () => {
  const missing = invoke('POST', '/api/expenses', { body: { vendor:'Vendor', amount:100, billPhoto:'/api/expenses/photo/bill.jpg', paidAlready:true, paymentType:'UPI', personalPaymentProof:'/api/expenses/photo/pay.jpg' } });
  assert.equal(missing.status, 400);
  assert.match(missing.body.error, /account used/i);
  const cash = invoke('POST', '/api/expenses', { body: { vendor:'Vendor', amount:100, billPhoto:'/api/expenses/photo/bill.jpg', paidAlready:true, paymentType:'Cash', personalPaymentProof:'/api/expenses/photo/pay.jpg' } });
  assert.equal(cash.status, 200);
  assert.match(cash.body.expense.payments[0].account, /cash$/i);
});

test('payment accounts are scoped by claimant and accounting entity', () => {
  const claimantConfig = invoke('GET', '/api/expenses/config').body;
  assert.deepEqual(claimantConfig.personalAccounts, ['Arshpreet 1919']);
  assert.deepEqual(claimantConfig.accountsByNature.SANKI, ['Axis Bank 3448','Tiana 0425','Prashant Axis 3645','Counter Cash','Gagan Sir Cash','Prashant Cash']);
  assert.deepEqual(claimantConfig.accountsByNature.SAMAST, ['IndusInd Bank 7883','ICICI Bank 0993','ICICI Bank 0992','Kirti Nagar Cash']);
  assert.deepEqual(claimantConfig.accountsByNature.PERSONAL, ['Arshpreet 1919']);
  assert.ok(!claimantConfig.accounts.includes('Federal Bank 7328'));
  const blocked = invoke('POST', '/api/expenses', { body:{vendor:'Scoped Vendor',amount:100,billPhoto:'/api/expenses/photo/scoped.jpg',paidAlready:true,paymentType:'UPI',personalAccount:'Shivam 4807',personalPaymentProof:'/api/expenses/photo/scoped-pay.jpg'} });
  assert.equal(blocked.status, 400);
  const ownerConfig=invoke('GET','/api/expenses/config',{role:'owner'}).body;assert.ok(ownerConfig.payingAccountsByNature.PERSONAL.includes('Prashant Axis 3645'),'Owner can record a PERSONAL expense actually paid by SANKI');
  const adminConfig=invoke('GET','/api/expenses/config',{role:'admin'}).body;assert.equal(adminConfig.payingAccountsByNature.PERSONAL.includes('Prashant Axis 3645'),false,'Admin cannot see or post owner-private expenses');
});

test('claimant ledgers remain visible under both SANKI and SAMAST with zero activity', () => {
  for (const nature of ['SANKI','SAMAST']) {
    const balances=invoke('GET','/api/expenses/balances',{role:'owner',query:{nature}});
    assert.equal(balances.status,200);
    for (const account of ['Arshpreet 1919','Shivam 4807','Pradeep 8606']) {
      assert.ok(balances.body.accounts.some(x=>x.name===account),`${account} missing from ${nature}`);
    }
  }
});

test('installments support partial payments and prevent overpayment', () => {
  const created = invoke('POST', '/api/expenses', { body: {
    ledger: 'Furniture Expense-A3', vendor: 'Carpenter', amount: 10000, isInstallment: true, requestedAmount: 1000,
    billPhoto: '/api/expenses/photo/bill.jpg', paymentType: 'Cash'
  } });
  invoke('POST', '/api/expenses/:id', { params: { id: created.body.expense.id }, body: { ledger: 'Furniture Expense-A3' }, role: 'owner' });
  invoke('POST', '/api/expenses/:id/approve', { params: { id: created.body.expense.id }, role: 'accounting' });
  const partial = invoke('POST', '/api/expenses/:id/pay', { params: { id: created.body.expense.id }, body: { amount: 1000, account: 'Counter Cash', paymentProof: '/api/expenses/photo/pay.jpg' }, role: 'admin' });
  assert.equal(partial.body.expense.status, 'partially_paid');
  const payables = invoke('GET', '/api/expenses/pending-payments', { query: { bucket:'partial' }, role:'owner' }).body;
  const payable = payables.expenses.find(e => e.id === created.body.expense.id);
  assert.equal(payable.balanceDue, 9000);
  const over = invoke('POST', '/api/expenses/:id/pay', { params: { id: created.body.expense.id }, body: { amount: 10000, account: 'Counter Cash', paymentProof: '/api/expenses/photo/pay2.jpg' }, role: 'admin' });
  assert.equal(over.status, 400);
  assert.match(over.body.error, /cannot exceed/i);
  const final = invoke('POST', '/api/expenses/:id/pay', { params: { id: created.body.expense.id }, body: { amount: 9000, account: 'Counter Cash', paymentProof: '/api/expenses/photo/pay3.jpg' }, role: 'admin' });
  assert.equal(final.body.expense.status, 'paid');
  assert.equal(final.body.expense.payments.length, 2);
});

test('approved expenses for the same vendor can be paid together with one proof', () => {
  function approved(vendor, amount, date) {
    const made=invoke('POST','/api/expenses',{body:{date,vendor,amount,billPhoto:'/api/expenses/photo/bill.jpg',qrPhoto:'/api/expenses/photo/qr.jpg',paymentType:'UPI'}});
    invoke('POST','/api/expenses/:id',{params:{id:made.body.expense.id},body:{ledger:'FLOWERS'},role:'owner'});
    invoke('POST','/api/expenses/:id/approve',{params:{id:made.body.expense.id},role:'owner'});
    return made.body.expense.id;
  }
  const first=approved('Daily Flowers',500,'2026-08-20'), second=approved('Daily  Flowers.',500,'2026-08-21');
  const candidates=invoke('GET','/api/expenses/:id/payment-candidates',{params:{id:second},role:'owner'});
  assert.ok(candidates.body.expenses.some(x=>x.id===first&&x.balanceDue===500));
  const paid=invoke('POST','/api/expenses/batch-pay',{role:'owner',body:{expenseIds:[first,second],account:'Counter Cash',paymentProof:'/api/expenses/photo/combined.jpg',date:'2026-08-22'}});
  assert.equal(paid.body.total,1000); assert.equal(paid.body.expenses.every(x=>x.status==='paid'),true);
  assert.equal(paid.body.expenses[0].payments.at(-1).batchPaymentId,paid.body.expenses[1].payments.at(-1).batchPaymentId);
});

test('consolidated vendor payment consumes available advance before creating the bank outflow', () => {
  function approved(amount,date) {
    const made=invoke('POST','/api/expenses',{body:{date,vendor:'Vendor Credit Test',amount,billPhoto:'/api/expenses/photo/credit-bill.jpg',qrPhoto:'/api/expenses/photo/credit-qr.jpg',paymentType:'UPI'}});
    invoke('POST','/api/expenses/:id',{params:{id:made.body.expense.id},body:{ledger:'FLOWERS'},role:'owner'});
    invoke('POST','/api/expenses/:id/approve',{params:{id:made.body.expense.id},role:'owner'});
    return made.body.expense.id;
  }
  const older=approved(400,'2026-09-01'),newer=approved(300,'2026-09-02');
  const expenseStorePath=path.join(path.dirname(process.env.DATA_PATH),'expenses.json');
  const store=JSON.parse(fs.readFileSync(expenseStorePath,'utf8'));store.vendorAdvances=store.vendorAdvances||[];
  store.vendorAdvances.push({id:'VADV-CREDIT-TEST',nature:'SANKI',vendor:'Vendor Credit Test',date:'2026-08-31',amount:18,remainingAmount:18,applications:[]});
  fs.writeFileSync(expenseStorePath,JSON.stringify(store));
  const candidates=invoke('GET','/api/expenses/:id/payment-candidates',{params:{id:newer},role:'owner'});
  assert.equal(candidates.body.availableVendorCredit,18);assert.equal(candidates.body.vendorAdvances[0].id,'VADV-CREDIT-TEST');
  const paid=invoke('POST','/api/expenses/batch-pay',{role:'owner',body:{expenseIds:[newer,older],amount:682,applyVendorCredit:true,account:'Counter Cash',paymentProof:'/api/expenses/photo/net-payment.jpg',date:'2026-09-03'}});
  assert.equal(paid.status,200);assert.equal(paid.body.combinedOutstanding,700);assert.equal(paid.body.vendorCreditApplied,18);assert.equal(paid.body.total,682);
  assert.equal(paid.body.expenses.every(x=>x.status==='paid'),true);
  assert.equal(paid.body.allocations.reduce((sum,x)=>sum+x.amount,0),682);assert.equal(paid.body.vendorCreditAllocations.reduce((sum,x)=>sum+x.amount,0),18);
  const saved=JSON.parse(fs.readFileSync(expenseStorePath,'utf8')),advance=saved.vendorAdvances.find(x=>x.id==='VADV-CREDIT-TEST');
  assert.equal(advance.remainingAmount,0);assert.equal(advance.applications.reduce((sum,x)=>sum+x.amount,0),18);
  const selected=[saved.expenses[older],saved.expenses[newer]];
  assert.equal(selected.reduce((sum,e)=>sum+(e.payments||[]).reduce((n,p)=>n+p.amount,0),0),682);
  assert.equal(selected.reduce((sum,e)=>sum+(e.vendorAdvanceApplications||[]).reduce((n,a)=>n+a.amount,0),0),18);
  assert.ok(saved.auditLog.some(x=>x.action==='VENDOR_ADVANCE_APPLIED'&&x.paymentId==='VADV-CREDIT-TEST'));
});

test('All Expenses exposes a clear same-vendor consolidated payment selector', () => {
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/class="combined-pay-select"/);
  assert.match(html,/Select to combine/);
  assert.match(html,/Pay selected together/);
  assert.match(html,/openPay\(ids\[0\],ids\)/);
  assert.match(html,/Apply available vendor advance/);
  assert.match(html,/applyVendorCredit:applyVendorCredit/);
});

test('one consolidated payment partially allocates across selected bills oldest first', () => {
  function approved(amount,date) {
    const made=invoke('POST','/api/expenses',{body:{date,vendor:'Partial Flower Vendor',amount,billPhoto:'/api/expenses/photo/pbill.jpg',paymentType:'Cash'}});
    invoke('POST','/api/expenses/:id',{params:{id:made.body.expense.id},body:{ledger:'FLOWERS'},role:'owner'});
    invoke('POST','/api/expenses/:id/approve',{params:{id:made.body.expense.id},role:'owner'});
    return made.body.expense.id;
  }
  const older=approved(5000,'2026-08-21'),newer=approved(5000,'2026-08-22');
  const paid=invoke('POST','/api/expenses/batch-pay',{role:'owner',body:{expenseIds:[newer,older],amount:7000,account:'Counter Cash',paymentProof:'/api/expenses/photo/partial-combined.jpg',date:'2026-08-22'}});
  assert.equal(paid.body.total,7000);
  assert.deepEqual(paid.body.allocations.map(x=>[x.expenseId,x.amount,x.status,x.balanceDue]),[[older,5000,'paid',0],[newer,2000,'partially_paid',3000]]);
  assert.equal(paid.body.expenses.find(x=>x.id===newer).payments.at(-1).batchTotal,7000);
  const over=invoke('POST','/api/expenses/batch-pay',{role:'owner',body:{expenseIds:[newer],amount:3001,account:'Counter Cash',paymentProof:'/api/expenses/photo/over.jpg'}});
  assert.equal(over.status,400);assert.match(over.body.error,/combined outstanding/i);
});

test('approved self-paid expenses appear once in spending, vendor and personal account ledgers', () => {
  const created = invoke('POST','/api/expenses',{body:{date:'2026-08-21',ledger:'FOOD EXPENSE',vendor:'Self Paid Surface Vendor',particulars:'market supplies',amount:450,billPhoto:'/api/expenses/photo/self-bill.jpg',paidAlready:true,paymentType:'UPI',personalAccount:'Arshpreet 1919',personalPaymentProof:'/api/expenses/photo/self-pay.jpg'}});
  const id=created.body.expense.id;
  invoke('POST','/api/expenses/:id',{params:{id},body:{ledger:'FOOD EXPENSE'},role:'owner'});
  invoke('POST','/api/expenses/:id/approve',{params:{id},role:'owner'});
  const spending=invoke('GET','/api/expenses/spending-dashboard',{query:{from:'2026-08-21',to:'2026-08-21'},role:'owner'}).body;
  assert.equal(spending.payments.filter(p=>p.id===id).length,1);
  assert.equal(spending.payments.find(p=>p.id===id).kind,'Paid personally');
  const ledger=invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account:'Arshpreet 1919'},role:'owner'}).body;
  assert.equal(ledger.entries.find(x=>x.id.startsWith(id)).debit,450);
  const vendors=invoke('GET','/api/expenses/vendors',{query:{nature:'SANKI',search:'Self Paid Surface'},role:'owner'}).body;
  assert.equal(vendors.vendors[0].paid,450);
  assert.equal(vendors.vendors[0].outstanding,0);
});

test('posted advanced purchases become mediator payables without changing procurement', () => {
  const procurementFile = path.join(tempDir, 'procurement.json');
  const original = { pos: { 'PO-9001': { id:'PO-9001', status:'posted', postedAt:'2026-08-21T08:00:00.000Z', dateReceive:'2026-08-21', vendor:'CHINA SUPPLIER', billNo:'CN-77',
    newProducts:[{variants:[{qty:2,landed:500}]}], existingAdds:[{qty:1,landed:250}] } } };
  fs.writeFileSync(procurementFile, JSON.stringify(original));
  const pending = invoke('GET', '/api/expenses/pending-payments', { query:{nature:'SANKI'}, role:'owner' });
  assert.equal(pending.body.purchases[0].amount, 1250);
  assert.equal(pending.body.purchases[0].supplier, 'CHINA SUPPLIER');
  const partial = invoke('POST', '/api/expenses/procurement-payables/:id/pay', { params:{id:'PO-9001'}, role:'owner', body:{amount:500,account:'Counter Cash',date:'2026-08-22',paymentProof:'/api/expenses/photo/proc-pay.jpg'} });
  assert.equal(partial.body.payable.balanceDue, 750);
  const ledger = invoke('GET', '/api/expenses/account-ledger', { query:{nature:'SANKI',account:'Counter Cash'}, role:'owner' }).body;
  assert.ok(ledger.entries.some(x => x.kind === 'purchase' && x.debit === 500));
  assert.deepEqual(JSON.parse(fs.readFileSync(procurementFile, 'utf8')), original);
});

test('advanced accounting workspaces are Owner/Admin only and claimant classification is forced', () => {
  const protectedGets = ['/api/expenses/pending-payments','/api/expenses/spending-dashboard','/api/expenses/reimbursements','/api/expenses/receivables','/api/expenses/vendors','/api/expenses/balances','/api/expenses/account-ledger'];
  protectedGets.forEach(route => assert.equal(invoke('GET', route, { query:{nature:'SANKI',account:'Counter Cash'}, role:'accounting' }).status, 403, route));
  const created = invoke('POST', '/api/expenses', { role:'accounting', body:{ledger:'FOOD EXPENSE',type:'fixed',vendor:'Forced Variable Vendor',amount:100,billPhoto:'/api/expenses/photo/forced.jpg',paymentType:'Cash'} });
  assert.equal(created.status, 200);
  assert.equal(created.body.expense.ledger, '');
  assert.equal(created.body.expense.type, 'variable');
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /cfg\.isAdmin\?'<div class="fld full"><label>Category/);
  assert.match(html, /if\(!cfg\.isAdmin\).*every non-Owner\/Admin user/);
  assert.match(html, /function syncApprovalsTab\(\)\{\s*if\(!cfg\.isAdmin\) return/);
});

test('account ledgers render an expandable one-click money trail', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /<h3 style="margin:0 0 4px">Money Trail<\/h3>/);
  assert.match(html, /ontoggle="if\(this\.open\)loadMoneyTrail/);
  assert.match(html, /Expenses paid '\+fmt\(expensesPaid\)/);
  assert.match(html, /Transfers out '\+fmt\(transfersOut\)/);
  assert.match(html, /Closing balance as of/);
  assert.match(html, /Entity \| Vendor \| Particulars/);
  assert.match(html, /data-cfdays="7"/);
  assert.match(html, /Reconciliation issue/);
  assert.match(html, /id="payOverrideReason"/);
  assert.match(html, /id="editPersonalAccount"/);
  assert.match(html, /id="editBillFile"/);
  assert.match(html, /Upload bank statement for reconciliation/);
  assert.match(html, /box\.appendChild\(recon\);loadBankStatement\(\)/);
  assert.match(html, /id="ledgerRecon" style="display:none/);
  assert.match(html, /id="bs_upload"/);
  assert.match(html, /id="bs_reconcile"/);
});

test('bank statement rows are normalized from cumulative Excel exports', () => {
  const file=path.join(tempDir,'axis-3645.xlsx'),sheet=XLSX.utils.json_to_sheet([
    {Date:'24/08/2026',Narration:'UPI reimbursement',Reference:'UTR720',Debit:'720.00',Credit:'',Balance:'1527.00'},
    {Date:'25/08/2026',Narration:'Customer receipt',Reference:'UTR500',Debit:'',Credit:'500',Balance:'2027'}
  ]),book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,sheet,'Statement');XLSX.writeFile(book,file);
  const rows=parseBankStatementFile(file);
  assert.deepEqual(rows.map(x=>({date:x.date,debit:x.debit,credit:x.credit,reference:x.reference,balance:x.balance})),[
    {date:'2026-08-24',debit:720,credit:0,reference:'UTR720',balance:1527},
    {date:'2026-08-25',debit:0,credit:500,reference:'UTR500',balance:2027}
  ]);
});

test('bank statement multipart fields are parsed before account permission is checked',()=>{
  const route=router.stack.find(x=>x.route&&x.route.path==='/api/expenses/bank-statements/import'&&x.route.methods.post);
  assert.ok(route);
  assert.equal(route.route.stack.length,3);
  assert.match(String(route.route.stack[0].handle.name),/multer/i);
  assert.match(route.route.stack[1].handle.toString(),/canAccessBankReconciliation/);
  assert.match(route.route.stack[1].handle.toString(),/unlinkSync/);
});

test('any manual ledger movement can be linked to a bank row, remarked and undone',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),now=new Date().toISOString();
  stored.openingBalances=stored.openingBalances||{};stored.openingBalances['Axis Bank 3448']=53341;
  stored.transfers=stored.transfers||[];stored.transfers.push({id:'TR-LINK-ALL',nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',fromAccount:'Axis Bank 3448',toAccount:'Prashant Axis 3645',amount:3341,date:'2026-08-25',proof:'/proof.jpg'});
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};stored.bankReconciliationDrafts['BRD-LINK-ALL']={id:'BRD-LINK-ALL',account:'Axis Bank 3448',nature:'SANKI',transactions:[{date:'2026-08-24',description:'IFT transfer',reference:'BANK3341',debit:3341,credit:0,balance:50000}],summary:{from:'2026-08-24',to:'2026-08-25',openingBalance:53341,closingBalance:50000,totalDebits:3341,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',originalName:'link.csv',fileHash:'link',createdAt:now,createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const linked=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:'BRD-LINK-ALL',rowId:'bank-0',action:'link_existing',appId:'TR-LINK-ALL',reason:'Bank date is final',remark:'Verified transfer'}});
  assert.equal(linked.status,200,JSON.stringify(linked.body));assert.ok(linked.body.rows.some(x=>x.status==='resolved'&&x.bank&&x.app&&x.app.id==='TR-LINK-ALL'));
  const remarked=invoke('POST','/api/expenses/bank-statements/remark',{role:'admin',body:{draftId:'BRD-LINK-ALL',remark:'August continuation statement'}});assert.equal(remarked.body.periodRemark,'August continuation statement');
  const undone=invoke('POST','/api/expenses/bank-statements/undo',{role:'admin',body:{draftId:'BRD-LINK-ALL',rowId:'bank-0'}});assert.equal(undone.status,200);assert.ok(undone.body.rows.some(x=>x.id==='bank-0'&&x.status!=='resolved'));
  assert.equal(invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:'BRD-LINK-ALL',rowId:'bank-0',action:'link_existing',appId:'TR-LINK-ALL',reason:'Bank date is final'}}).status,200);
  const final=invoke('POST','/api/expenses/bank-statements/finalize',{role:'admin',body:{draftId:'BRD-LINK-ALL'}});assert.equal(final.status,200,JSON.stringify(final.body));
  const after=JSON.parse(fs.readFileSync(expenseFile,'utf8'));assert.equal(after.bankDateOverrides['TR-LINK-ALL'].originalDate,'2026-08-25');assert.equal(after.bankDateOverrides['TR-LINK-ALL'].bankDate,'2026-08-24');
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('confirming a displayed possible match needs no second bank selection and finalizes with the bank date',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),account='Axis Bank 3448',id='BRD-CONFIRM-DISPLAYED',appId='TR-CONFIRM-DISPLAYED',amount=4321.23;
  stored.transfers=stored.transfers||[];stored.transfers.push({id:appId,nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',fromAccount:account,toAccount:'Prashant Axis 3645',amount,date:'2098-01-01',proof:'/proof.jpg'});
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};stored.bankReconciliationDrafts[id]={id,account,nature:'SANKI',transactions:[{date:'2098-01-02',description:'Miscellaneous official debit',reference:'QZ983274',debit:amount,credit:0,balance:0}],summary:{from:'2098-01-01',to:'2098-01-02',openingBalance:amount,closingBalance:0,totalDebits:amount,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',originalName:'confirm.csv',fileHash:'confirm',createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const before=invoke('POST','/api/expenses/bank-statements/reconcile',{role:'admin',body:{draftId:id,account}});assert.equal(before.status,200);const suggested=before.body.rows.find(x=>x.bank&&x.app&&x.app.id===appId);assert.equal(suggested.status,'possible_match');
  const confirmed=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:id,rowId:suggested.id,action:'accept_match'}});assert.equal(confirmed.status,200,JSON.stringify(confirmed.body));assert.equal(confirmed.body.rows.find(x=>x.id===suggested.id).resolution.appId,appId);
  const aligned=invoke('POST','/api/expenses/bank-statements/resolve-balance',{role:'admin',body:{draftId:id,amount:confirmed.body.proposedOpening+confirmed.body.balanceDifference,reason:'Align isolated confirmation test'}});assert.equal(aligned.status,200);assert.equal(aligned.body.canFinalize,true);
  const draftSnapshot=JSON.parse(JSON.stringify(JSON.parse(fs.readFileSync(expenseFile,'utf8')).bankReconciliationDrafts[id])),finalized=invoke('POST','/api/expenses/bank-statements/finalize',{role:'admin',body:{draftId:id}});assert.equal(finalized.status,200,JSON.stringify(finalized.body));applyFinalizedConfirmedMatches(draftSnapshot,'prashant');const after=JSON.parse(fs.readFileSync(expenseFile,'utf8'));assert.equal(after.bankDateOverrides[appId].originalDate,'2098-01-01');assert.equal(after.bankDateOverrides[appId].bankDate,'2098-01-02');assert.match(after.bankDateOverrides[appId].remark,/Confirmed displayed match/);fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('loading an account ledger never auto-opens an unfinished reconciliation form',()=>{const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8'),render=html.slice(html.indexOf('function renderBankReconciliation'),html.indexOf('function bankDecisionLabel'));assert.doesNotMatch(render,/showBankReconForm|showModal|restoreBankReconForm/);assert.doesNotMatch(html,/function restoreBankReconForm/);assert.match(html,/official transaction date will be.*bank statement when finalized/);});

test('reviewed bank transactions can finalize while the closing balance remains pending',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),original=fs.readFileSync(expenseFile,'utf8'),account='Prashant Axis 3645',draftId='BRD-DEFER-BALANCE';
  const store={expenses:{'EX-DEFER':{id:'EX-DEFER',date:'2098-02-01',nature:'SANKI',status:'paid',approvedAt:'2098-02-01T10:00:00Z',vendor:'Acme Supplies',particulars:'Acme Supplies',amount:100,paidAmount:100,payments:[{id:'PAY-001',date:'2098-02-01',amount:100,account}]}},receivables:{},vendors:{'acme supplies':{name:'Acme Supplies'}},vendorsByNature:{SAMAST:{},PERSONAL:{}},accounts:[],people:[],openingBalances:{[account]:0},openingBalancesByNature:{SAMAST:{},PERSONAL:{}},adjustments:[],transfers:[],receipts:[],vendorOpeningPayables:[],vendorAdvances:[],paytmSettlements:[],reconciliationExpenses:[],bankDateOverrides:{},bankStatements:{},bankReconciliationDrafts:{[draftId]:{id:draftId,account,nature:'SANKI',transactions:[{date:'2098-02-01',description:'UPI/ACME SUPPLIES',reference:'ACME-100',debit:100,credit:0,balance:0}],summary:{from:'2098-02-01',to:'2098-02-01',openingBalance:100,totalDebits:100,totalCredits:0,closingBalance:0,validated:true},resolutions:{},matchingPolicy:'strict_identity_v3',createdAt:'2098-02-01T12:00:00Z',expiresAt:'2098-02-08T12:00:00Z'}},auditLog:[],auditSeq:0,oneTimeMigrations:{}};
  try{
    fs.writeFileSync(expenseFile,JSON.stringify(store));
    const blocked=invoke('POST','/api/expenses/bank-statements/finalize',{role:'admin',body:{draftId}});assert.equal(blocked.status,409);assert.match(blocked.body.error,/closing balance differs/i);
    const finalized=invoke('POST','/api/expenses/bank-statements/finalize',{role:'admin',body:{draftId,deferClosingBalance:true}});assert.equal(finalized.status,200,JSON.stringify(finalized.body));assert.equal(finalized.body.balanceReconciled,false);assert.equal(finalized.body.balanceDifference,100);
    const saved=JSON.parse(fs.readFileSync(expenseFile,'utf8')),book=saved.bankStatements[account],record=book.imports.at(-1);assert.equal(saved.openingBalances[account],0,'no opening-balance adjustment is posted');assert.equal(saved.bankReconciliationDrafts[draftId],undefined);assert.equal(book.lastReconciliation.transactionsReconciled,true);assert.equal(book.lastReconciliation.balanceReconciled,false);assert.equal(book.lastReconciliation.reconciled,false);assert.equal(record.closingBalanceDeferred,true);assert.equal(record.balanceDifference,100);
  }finally{fs.writeFileSync(expenseFile,original);}
});

test('bank UI offers transaction finalization when only the closing balance is pending',()=>{const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');assert.match(html,/d\.canFinalizeTransactions\?'inline-flex'/);assert.match(html,/Finalize reviewed transactions — balance pending/);assert.match(html,/Confirm: finalize transactions only/);assert.match(html,/dataset\.confirmPending/);assert.match(html,/deferClosingBalance:deferClosingBalance/);assert.match(html,/Transactions ✓ · Balance pending/);});

test('a missing internal transfer can be created from an official bank row without fabricated proof',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),now=new Date().toISOString(),account='Prashant Axis 3645',id='BRD-CREATE-TRANSFER';
  stored.bankReconciliationDrafts[id]={id,account,nature:'SANKI',transactions:[{date:'2026-08-26',description:'Second transfer from Axis 3448',reference:'BANK-SECOND',debit:0,credit:1203,balance:1203}],summary:{from:'2026-08-26',to:'2026-08-26',openingBalance:0,closingBalance:1203,totalDebits:0,totalCredits:1203,validated:true},resolutions:{},temporaryFile:'',createdAt:now,createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const resolved=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:id,rowId:'bank-0',action:'create_internal_transfer',otherAccount:'Axis Bank 3448',reason:'Second bank-confirmed transfer'}});assert.equal(resolved.status,200,JSON.stringify(resolved.body));assert.equal(resolved.body.rows.find(x=>x.id==='bank-0').status,'resolved');
  const afterResolve=JSON.parse(fs.readFileSync(expenseFile,'utf8'));applyFinalizedInternalTransfers(afterResolve.bankReconciliationDrafts[id],'prashant');const after=JSON.parse(fs.readFileSync(expenseFile,'utf8')),transfer=after.transfers.find(x=>x.reconciliationDraft===id);
  assert.equal(transfer.fromAccount,'Axis Bank 3448');assert.equal(transfer.toAccount,account);assert.equal(transfer.amount,1203);assert.equal(transfer.bankStatementEvidence,true);assert.equal((after.auditLog||[]).some(x=>x.action==='BANK_CONFIRMED_TRANSFER_CREATED'&&x.subjectId===transfer.id),true);
  const issues=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account}}).body.reconciliationIssues;assert.equal(issues.some(x=>x.reference===transfer.id&&x.code==='missing_transfer_proof'),false);
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('bank review offers a bank-confirmed internal transfer action',()=>{const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');assert.match(html,/Create missing internal transfer/);assert.match(html,/id="brOtherAccount"/);assert.match(html,/action:'create_internal_transfer'/);});

test('bank review exposes multi-entry linking directly for unmatched debits',()=>{const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');assert.match(html,/openBankAssignment\(\\'\'\+x\.id\+\'\\',\\'multiple\\'\).*Link multiple existing entries/);assert.match(html,/payload\.action=kind==='multiple'\?'link_multiple_existing'/);});

test('bank reconciliation provides a searchable selectable ledger-entry picker',()=>{const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');assert.match(html,/id="brAppSearch"[^>]+Search by amount, expense\/payment reference, vendor, narration or date/);assert.match(html,/id="brAppCandidates"/);assert.match(html,/toggleBankAppCandidate/);assert.match(html,/selected · /);assert.match(html,/Difference /);});

test('one ledger movement cannot be selected twice in the same reconciliation',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),account='Prashant Axis 3645',id='BRD-NO-DOUBLE-LINK',date='2026-09-01';
  stored.expenses['EX-NO-DOUBLE-A']={id:'EX-NO-DOUBLE-A',date,nature:'SANKI',status:'paid',approvedAt:date+'T10:00:00Z',vendor:'Alok',amount:600,paidAmount:600,payments:[{id:'PAY-001',date,amount:600,account}]};stored.expenses['EX-NO-DOUBLE-B']={id:'EX-NO-DOUBLE-B',date,nature:'SANKI',status:'paid',approvedAt:date+'T10:00:00Z',vendor:'Alok',amount:600,paidAmount:600,payments:[{id:'PAY-001',date,amount:600,account}]};
  stored.bankReconciliationDrafts[id]={id,account,nature:'SANKI',transactions:[{date,description:'Combined payment',reference:'BANK-1200',debit:1200,credit:0,balance:-1200},{date,description:'Wrong mismatch suggestion',reference:'BANK-610',debit:610,credit:0,balance:-1810}],summary:{from:date,to:date,openingBalance:0,closingBalance:-1810,totalDebits:1810,totalCredits:0,validated:true},resolutions:{},matchingPolicy:'strict_identity_v2',temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const first=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:id,rowId:'bank-0',action:'link_multiple_existing',appIds:['EX-NO-DOUBLE-A/PAY-001','EX-NO-DOUBLE-B/PAY-001'],reason:'Combined vendor payment'}});assert.equal(first.status,200,JSON.stringify(first.body));
  const reused=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:id,rowId:'bank-1',action:'link_multiple_existing',appIds:['EX-NO-DOUBLE-A/PAY-001'],reason:'Try duplicate link'}});assert.equal(reused.status,409);assert.match(reused.body.error,/already matched or resolved/);fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('Prashant admin can create a bank-confirmed transfer to Paytm clearing',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),id='BRD-ADMIN-PAYTM',account='Prashant Axis 3645';
  stored.bankReconciliationDrafts[id]={id,account,nature:'SANKI',transactions:[{date:'2026-09-01',description:'Paytm load',reference:'PAYTM-LOAD',debit:20000,credit:0,balance:-20000}],summary:{from:'2026-09-01',to:'2026-09-01',openingBalance:0,closingBalance:-20000,totalDebits:20000,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const result=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:id,rowId:'bank-0',action:'create_internal_transfer',otherAccount:'Paytm Settlement Clearing',reason:'Cash routed through Paytm'}});assert.equal(result.status,200,JSON.stringify(result.body));
  const resolved=JSON.parse(fs.readFileSync(expenseFile,'utf8')).bankReconciliationDrafts[id].resolutions['bank-0'];assert.equal(resolved.otherAccount,'Paytm Settlement Clearing');fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('reconciliation supports multiple links, rounding, and reusable vendor advances',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),account='Prashant Axis 3645',paid=(id,date,vendor,amount)=>({id,date,nature:'SANKI',status:'paid',approvedAt:date+'T10:00:00Z',vendor,particulars:vendor,amount,paidAmount:amount,payments:[{id:'PAY-001',date,amount,account}]});
  stored.expenses['EX-MULTI-A']=paid('EX-MULTI-A','2026-09-01','Alok',600);stored.expenses['EX-MULTI-B']=paid('EX-MULTI-B','2026-09-01','Alok',600);stored.expenses['EX-ROUND']=paid('EX-ROUND','2026-09-02','Recharge',652);stored.expenses['EX-ADV-BASE']=paid('EX-ADV-BASE','2026-09-03','Kalu',200);stored.expenses['EX-ADV-NEXT']={id:'EX-ADV-NEXT',date:'2026-09-04',nature:'SANKI',status:'approved',approvedAt:'2026-09-04T10:00:00Z',vendor:'Kalu',particulars:'Fruit',amount:100,paidAmount:0,payments:[]};
  const drafts=[['BRD-MULTI',1200,'2026-09-01'],['BRD-ROUND',651.90,'2026-09-02'],['BRD-ADV',418,'2026-09-03']];stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};drafts.forEach(([id,debit,date])=>{stored.bankReconciliationDrafts[id]={id,account,nature:'SANKI',transactions:[{date,description:'Official bank debit',reference:id,debit,credit:0,balance:-debit}],summary:{from:date,to:date,openingBalance:0,closingBalance:-debit,totalDebits:debit,totalCredits:0,validated:true},resolutions:{},matchingPolicy:'strict_identity_v2',temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};});fs.writeFileSync(expenseFile,JSON.stringify(stored));
  let out=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:'BRD-MULTI',rowId:'bank-0',action:'link_multiple_existing',appIds:['EX-MULTI-A/PAY-001','EX-MULTI-B/PAY-001'],reason:'One payment covers two expenses'}});assert.equal(out.status,200,JSON.stringify(out.body));assert.equal(out.body.rows.find(x=>x.id==='bank-0').resolution.appIds.length,2);
  out=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:'BRD-ROUND',rowId:'bank-0',action:'link_with_rounding',appIds:['EX-ROUND/PAY-001'],reason:'Bank settled ten paise lower'}});assert.equal(out.status,200,JSON.stringify(out.body));assert.equal(out.body.rows.find(x=>x.id==='bank-0').resolution.roundingAmount,.10);
  out=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:'BRD-ADV',rowId:'bank-0',action:'vendor_advance_split',appIds:['EX-ADV-BASE/PAY-001'],vendor:'Kalu',advanceAmount:218,advanceExpenseIds:['EX-ADV-NEXT'],reason:'Overpayment retained by vendor'}});assert.equal(out.status,200,JSON.stringify(out.body));const snapshot=JSON.parse(fs.readFileSync(expenseFile,'utf8'));applyFinalizedCompositeLinks(snapshot.bankReconciliationDrafts['BRD-ROUND'],'prashant');applyFinalizedCompositeLinks(snapshot.bankReconciliationDrafts['BRD-ADV'],'prashant');const after=JSON.parse(fs.readFileSync(expenseFile,'utf8')),advance=after.vendorAdvances.find(x=>x.reconciliationDraft==='BRD-ADV');assert.equal(after.adjustments.find(x=>x.reconciliationDraft==='BRD-ROUND').amount,.10);assert.equal(advance.amount,218);assert.equal(advance.remainingAmount,118);assert.equal(after.expenses['EX-ADV-NEXT'].paidAmount,100);fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('amount-mismatch rows can correct an editable ledger entry with an audit trail',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),now=new Date().toISOString();
  stored.transfers=stored.transfers||[];stored.transfers.push({id:'TR-AMOUNT-FIX',nature:'SANKI',fromNature:'SANKI',toNature:'SANKI',fromAccount:'Axis Bank 3448',toAccount:'Prashant Axis 3645',amount:2500,date:'2026-08-29',proof:'/proof.jpg'});
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};stored.bankReconciliationDrafts['BRD-AMOUNT-FIX']={id:'BRD-AMOUNT-FIX',account:'Axis Bank 3448',nature:'SANKI',transactions:[{date:'2026-08-29',description:'Transfer to Prashant',reference:'TR-AMOUNT-FIX',debit:2505.90,credit:0,balance:1000}],summary:{from:'2026-08-29',to:'2026-08-29',openingBalance:3505.90,closingBalance:1000,totalDebits:2505.90,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',createdAt:now,createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const before=invoke('POST','/api/expenses/bank-statements/reconcile',{role:'admin',body:{draftId:'BRD-AMOUNT-FIX',account:'Axis Bank 3448'}});assert.ok(before.body.rows.some(x=>x.status==='amount_mismatch'&&x.app&&x.app.id==='TR-AMOUNT-FIX'));
  assert.equal(invoke('POST','/api/expenses/bank-statements/correct-ledger-entry',{role:'admin',body:{draftId:'BRD-AMOUNT-FIX',rowId:'bank-0',amount:2505.90}}).status,400);
  const corrected=invoke('POST','/api/expenses/bank-statements/correct-ledger-entry',{role:'admin',body:{draftId:'BRD-AMOUNT-FIX',rowId:'bank-0',amount:2505.90,reason:'Bank amount is authoritative'}});assert.equal(corrected.status,200,JSON.stringify(corrected.body));assert.ok(corrected.body.rows.some(x=>x.status==='matched'&&x.app&&x.app.id==='TR-AMOUNT-FIX'));
  const after=JSON.parse(fs.readFileSync(expenseFile,'utf8'));assert.equal(after.transfers.find(x=>x.id==='TR-AMOUNT-FIX').amount,2505.90);assert.ok((after.auditLog||[]).some(x=>x.action==='BANK_RECONCILIATION_LEDGER_AMOUNT_CORRECTED'&&x.subjectId==='TR-AMOUNT-FIX'));
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('unrelated bank and app amounts remain separate missing entries',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),now=new Date().toISOString(),account='Prashant Axis 3645';
  stored.receipts=stored.receipts||[];stored.receipts.push({id:'RCPT-UNRELATED-CREDIT',nature:'SANKI',account,date:'2026-09-30',source:'Transfer from Axis Bank 3448',amount:2247});
  stored.adjustments=stored.adjustments||[];stored.adjustments.push({id:'ADJ-UNRELATED-DEBIT',nature:'SANKI',account,date:'2026-09-30',note:'Manish agencies',amount:-332});
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};stored.bankReconciliationDrafts['BRD-UNRELATED']={id:'BRD-UNRELATED',account,nature:'SANKI',transactions:[
    {date:'2026-09-30',description:'SANJAN SI UPI',reference:'660007395196',debit:0,credit:2000,balance:2000},
    {date:'2026-09-30',description:'CHANDER PRAKASH UPI',reference:'623409923274',debit:6090,credit:0,balance:-4090}
  ],summary:{from:'2026-09-30',to:'2026-09-30',openingBalance:0,closingBalance:-4090,totalDebits:6090,totalCredits:2000,validated:true},resolutions:{},temporaryFile:'',createdAt:now,createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const view=invoke('POST','/api/expenses/bank-statements/reconcile',{role:'admin',body:{draftId:'BRD-UNRELATED',account}}).body;
  assert.equal(view.summary.amount_mismatch||0,0);
  assert.equal(view.summary.missing_in_app,2);
  assert.equal(view.summary.missing_in_bank,2);
  assert.ok(view.rows.filter(x=>x.status==='missing_in_app').every(x=>x.bank&&!x.app));
  assert.ok(view.rows.filter(x=>x.status==='missing_in_bank').every(x=>x.app&&!x.bank));
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('same or nearby amounts never pair automatically when date or meaningful narration differs',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),account='Prashant Axis 3645';
  stored.expenses['EX-STRICT-ALOK']={id:'EX-STRICT-ALOK',date:'2026-08-24',nature:'SANKI',status:'paid',vendor:'Alok Kumar',particulars:'Vendor payment',amount:600,paidAmount:600,approvedAt:'2026-08-24T10:00:00Z',payments:[{id:'PAY-001',date:'2026-08-24',amount:600,account}]};
  stored.expenses['EX-STRICT-FNP']={id:'EX-STRICT-FNP',date:'2026-08-26',nature:'SANKI',status:'paid',vendor:'FNP',particulars:'Flowers',amount:100,paidAmount:100,approvedAt:'2026-08-26T10:00:00Z',payments:[{id:'PAY-001',date:'2026-08-26',amount:100,account}]};
  stored.expenses['EX-STRICT-REIM']={id:'EX-STRICT-REIM',date:'2026-08-27',nature:'SANKI',status:'paid',vendor:'Local vendor',claimant:'shivam',particulars:'Reimbursement',amount:20,paidAmount:20,approvedAt:'2026-08-27T10:00:00Z',reimbursementPayments:[{id:'REIM-001',date:'2026-08-27',amount:20,account}]};
  stored.bankReconciliationDrafts['BRD-STRICT-MATCH']={id:'BRD-STRICT-MATCH',account,nature:'SANKI',transactions:[
    {date:'2026-08-22',description:'UPI payment to Shivam Kumar',reference:'623437186160',debit:610,credit:0,balance:1000},
    {date:'2026-08-28',description:'UPI payment to Tushar Kumar',reference:'624022552635',debit:100,credit:0,balance:900},
    {date:'2026-08-28',description:'UPI payment to Ranjit Kumar',reference:'624078183564',debit:20,credit:0,balance:880}
  ],summary:{from:'2026-08-22',to:'2026-08-28',openingBalance:1490,closingBalance:880,totalDebits:730,totalCredits:0,validated:true},resolutions:{},matchingPolicy:'strict_identity_v2',temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));const view=invoke('POST','/api/expenses/bank-statements/reconcile',{role:'admin',body:{draftId:'BRD-STRICT-MATCH',account}}).body;
  assert.equal(view.summary.matched||0,0);assert.equal(view.summary.possible_match||0,0);assert.equal(view.summary.amount_mismatch||0,0);assert.equal(view.summary.missing_in_app,3);assert.ok(view.rows.filter(x=>x.status==='missing_in_bank').some(x=>x.app&&x.app.id==='EX-STRICT-ALOK/PAY-001'));assert.ok(view.rows.filter(x=>x.status==='missing_in_bank').some(x=>x.app&&x.app.id==='EX-STRICT-FNP/PAY-001'));assert.ok(view.rows.filter(x=>x.status==='missing_in_bank').some(x=>x.app&&x.app.id==='EX-STRICT-REIM/REIM-001'));
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('three owner-confirmed false matches are isolated without changing active draft decisions',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),key='preserve-axis-3645-progress-and-separate-three-false-matches',account='Prashant Axis 3645';
  delete stored.oneTimeMigrations[key];
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};
  const savedResolution={action:'exclude',reason:'Previously reviewed by owner',remark:'Keep this completed decision',resolvedBy:'prashant',resolvedAt:'2026-08-29T10:00:00.000Z'};
  stored.bankReconciliationDrafts['BRD-PRESERVE-PROGRESS']={id:'BRD-PRESERVE-PROGRESS',account,nature:'SANKI',transactions:[
    {date:'2026-08-22',description:'UPI payment to Shivam Kumar',reference:'623437186160',debit:610,credit:0,balance:1000},
    {date:'2026-08-28',description:'UPI payment to Tushar Kumar',reference:'624022552635',debit:100,credit:0,balance:900},
    {date:'2026-08-28',description:'UPI payment to Ranjit Kumar',reference:'624078183564',debit:20,credit:0,balance:880},
    {date:'2026-08-28',description:'Unrelated reviewed row',reference:'KEEP-RESOLUTION',debit:50,credit:0,balance:830}
  ],summary:{from:'2026-08-22',to:'2026-08-28',openingBalance:1440,closingBalance:830,totalDebits:780,totalCredits:0,validated:true},resolutions:{'bank-3':savedResolution},temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));invoke('GET','/api/expenses/config',{role:'owner'});
  const corrected=JSON.parse(fs.readFileSync(expenseFile,'utf8')),draft=corrected.bankReconciliationDrafts['BRD-PRESERVE-PROGRESS'];
  assert.deepEqual(draft.resolutions['bank-3'],savedResolution);assert.deepEqual(Object.keys(draft.matchingExclusions).sort(),['bank-0','bank-1','bank-2']);assert.equal(draft.matchingPolicy,undefined);assert.equal(corrected.oneTimeMigrations[key].preservedExistingResolutions,true);
  const view=invoke('POST','/api/expenses/bank-statements/reconcile',{role:'admin',body:{draftId:draft.id,account}}).body;
  assert.ok(view.rows.slice(0,3).every(x=>x.status==='missing_in_app'&&x.matchingExclusion));assert.equal(view.rows.find(x=>x.id==='bank-3').status,'resolved');
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('existing FNP bank row links three existing expenses without touching other progress',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),oldKey='resolve-fnp-300-as-current-100-and-opening-payable-200',key='link-fnp-300-to-three-existing-100-expenses',account='Prashant Axis 3645';
  delete stored.oneTimeMigrations[oldKey];delete stored.oneTimeMigrations[key];['20','21','22'].forEach((day,i)=>{const id='EX-FNP-LIVE-'+day;stored.expenses[id]={id,date:'2026-08-'+day,nature:'SANKI',status:'paid',vendor:'FNP',particulars:'Flowers',amount:100,paidAmount:100,approvedAt:'2026-08-'+day+'T10:00:00Z',payments:[{id:'PAY-001',date:'2026-08-22',amount:100,account}]};});
  const savedResolution={action:'exclude',reason:'Existing completed decision',by:'prashant',at:'2026-08-29T09:00:00.000Z'};stored.bankReconciliationDrafts['BRD-FNP-LIVE']={id:'BRD-FNP-LIVE',account,nature:'SANKI',transactions:[{date:'2026-08-22',description:'Ferns n petals UPI',reference:'623499417992',debit:300,credit:0,balance:1000},{date:'2026-08-22',description:'Other row',reference:'OTHER-ROW',debit:50,credit:0,balance:950}],summary:{from:'2026-08-22',to:'2026-08-22',openingBalance:1300,closingBalance:950,totalDebits:350,totalCredits:0,validated:true},resolutions:{'bank-1':savedResolution},temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));invoke('GET','/api/expenses/config',{role:'owner'});const corrected=JSON.parse(fs.readFileSync(expenseFile,'utf8')),draft=corrected.bankReconciliationDrafts['BRD-FNP-LIVE'];
  assert.deepEqual(draft.resolutions['bank-1'],savedResolution);assert.equal(draft.resolutions['bank-0'].action,'link_multiple_existing');assert.deepEqual(draft.resolutions['bank-0'].appIds,['EX-FNP-LIVE-20/PAY-001','EX-FNP-LIVE-21/PAY-001','EX-FNP-LIVE-22/PAY-001']);
  const view=invoke('POST','/api/expenses/bank-statements/reconcile',{role:'admin',body:{draftId:draft.id,account}}).body;const row=view.rows.find(x=>x.id==='bank-0');assert.equal(row.status,'resolved');assert.equal(row.app.debit,300);assert.equal(row.app.linkedEntries.length,3);assert.ok(!view.rows.some(x=>x.status==='missing_in_bank'&&/^EX-FNP-LIVE-/.test(x.app&&x.app.id||'')));assert.equal(corrected.oneTimeMigrations[key].preservedOtherResolutions,true);
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('owner-confirmed FNP expenses migrate from SAMAST to SANKI with vendor history preserved',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),key='move-all-fnp-expenses-from-samast-to-sanki';
  delete stored.oneTimeMigrations[key];stored.vendorsByNature.SAMAST.fnp={name:'Fnp',notes:'Flower vendor'};delete stored.vendors.fnp;stored.expenses['EX-FNP-ENTITY-FIX']={id:'EX-FNP-ENTITY-FIX',date:'2026-08-22',nature:'SAMAST',status:'paid',vendor:'Fnp',particulars:'Flowers',ledger:'Flowers',amount:100,paidAmount:100,approvedAt:'2026-08-22T10:00:00Z',payments:[{id:'PAY-001',date:'2026-08-22',amount:100,account:'Prashant Axis 3645'}]};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  invoke('GET','/api/expenses/config',{role:'owner'});const corrected=JSON.parse(fs.readFileSync(expenseFile,'utf8'));assert.equal(corrected.expenses['EX-FNP-ENTITY-FIX'].nature,'SANKI');assert.equal(corrected.vendors.fnp.name,'FNP');assert.equal(corrected.vendorsByNature.SAMAST.fnp,undefined);assert.deepEqual(corrected.oneTimeMigrations[key].changedExpenses,['EX-FNP-ENTITY-FIX']);assert.ok(corrected.auditLog.some(x=>x.action==='EXPENSE_ENTITY_CORRECTED'&&x.subjectId==='EX-FNP-ENTITY-FIX'));
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('a missing outgoing bank row can create and link a normal paid expense',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),now=new Date().toISOString();
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};stored.bankReconciliationDrafts['BRD-DIRECT-EXPENSE']={id:'BRD-DIRECT-EXPENSE',account:'Axis Bank 3448',nature:'SANKI',transactions:[{date:'2026-09-02',description:'UPI PAUL MOTOR',reference:'UTR2300',debit:2300,credit:0,balance:5000}],summary:{from:'2026-09-02',to:'2026-09-02',openingBalance:7300,closingBalance:5000,totalDebits:2300,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',createdAt:now,createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const missingBill=invoke('POST','/api/expenses/bank-statements/create-expense',{role:'admin',body:{draftId:'BRD-DIRECT-EXPENSE',rowId:'bank-0',ledger:'FOOD EXPENSE',vendor:'Paul Motor',particulars:'Motor repair',remark:'Verified statement debit'}});assert.equal(missingBill.status,400);
  const made=invoke('POST','/api/expenses/bank-statements/create-expense',{role:'admin',body:{draftId:'BRD-DIRECT-EXPENSE',rowId:'bank-0',ledger:'FOOD EXPENSE',vendor:'Paul Motor',particulars:'Motor repair',bill:'printed',billPhoto:'/api/expenses/photo/paul.jpg',remark:'Verified statement debit'}});
  assert.equal(made.status,200,JSON.stringify(made.body));assert.equal(made.body.expense.status,'paid');assert.equal(made.body.expense.amount,2300);assert.equal(made.body.expense.payments[0].account,'Axis Bank 3448');assert.equal(made.body.expense.reconciliationSource.rowId,'bank-0');assert.ok(made.body.rows.some(x=>x.id==='bank-0'&&x.status==='resolved'&&x.resolution.action==='create_expense'));
  const duplicate=invoke('POST','/api/expenses/bank-statements/create-expense',{role:'admin',body:{draftId:'BRD-DIRECT-EXPENSE',rowId:'bank-0',ledger:'FOOD EXPENSE',vendor:'Paul Motor',particulars:'Duplicate',billPhoto:'/api/expenses/photo/paul.jpg',remark:'Duplicate'}});assert.notEqual(duplicate.status,200);
  const after=JSON.parse(fs.readFileSync(expenseFile,'utf8'));assert.ok((after.auditLog||[]).some(x=>x.action==='CREATED_FROM_BANK_RECONCILIATION'&&x.subjectId===made.body.expense.id));fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('unrelated same-date bank and ledger entries are never presented as an amount mismatch',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),account='ICICI Bank 0993';
  stored.expenses['EX-GENNI-STRICT']={id:'EX-GENNI-STRICT',date:'2026-09-01',nature:'PERSONAL',status:'paid',vendor:'Genni Di',particulars:'Genni Di',ledger:'FOOD EXPENSE',amount:10000,paidAmount:10000,payments:[{id:'PAY-001',date:'2026-09-01',amount:10000,account}]};
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};stored.bankReconciliationDrafts['BRD-IDENTITY-STRICT']={id:'BRD-IDENTITY-STRICT',account,nature:'PERSONAL',transactions:[{date:'2026-09-01',description:'UPIIntent Blinkit',reference:'797523596495',debit:186,credit:0,balance:100}],summary:{from:'2026-09-01',to:'2026-09-01',openingBalance:286,closingBalance:100,totalDebits:186,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'owner-user',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const view=invoke('POST','/api/expenses/bank-statements/reconcile',{role:'owner',body:{draftId:'BRD-IDENTITY-STRICT',account}}).body;
  assert.equal(view.summary.amount_mismatch||0,0);assert.ok(view.rows.some(x=>x.status==='missing_in_app'&&x.bank&&x.bank.debit===186));assert.ok(view.rows.some(x=>x.status==='missing_in_bank'&&x.app&&x.app.debit===10000));
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('masked account suffixes match exact bank payments and reconciliation rows sort newest first',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),account='ICICI Bank 0993';
  stored.expenses['EX-MASKED-200']={id:'EX-MASKED-200',date:'2026-08-30',nature:'PERSONAL',status:'paid',vendor:'xxx0465ptyes',particulars:'Transfer',amount:200,paidAmount:200,payments:[{id:'PAY-001',date:'2026-08-30',amount:200,account}]};
  stored.expenses['EX-MASKED-310']={id:'EX-MASKED-310',date:'2026-08-30',nature:'PERSONAL',status:'paid',vendor:'xxx0465ptyes',particulars:'Transfer',amount:310,paidAmount:310,payments:[{id:'PAY-001',date:'2026-08-30',amount:310,account}]};
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};stored.bankReconciliationDrafts['BRD-MASKED-SUFFIX']={id:'BRD-MASKED-SUFFIX',account,nature:'PERSONAL',transactions:[{date:'2026-08-29',description:'Older unrelated payment',reference:'OLD100',debit:100,credit:0,balance:510},{date:'2026-08-30',description:'UPI/9891900465/Payment fr/BANK OF BA',reference:'9891900465',debit:310,credit:0,balance:200},{date:'2026-08-30',description:'UPI/9891900465/Payment fr/BANK OF BA',reference:'9891900465',debit:200,credit:0,balance:0}],summary:{from:'2026-08-29',to:'2026-08-30',openingBalance:610,closingBalance:0,totalDebits:610,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'owner-user',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const view=invoke('POST','/api/expenses/bank-statements/reconcile',{role:'owner',body:{draftId:'BRD-MASKED-SUFFIX',account}}).body,matched=view.rows.filter(x=>x.status==='matched');
  assert.equal(matched.length,2);assert.deepEqual(matched.map(x=>x.bank.debit).sort((a,b)=>a-b),[200,310]);assert.equal(view.rows[0].bank.date,'2026-08-30');assert.equal(view.rows.at(-1).bank.date,'2026-08-29');
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('incoming reconciliation records an internal transfer in both account ledgers',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),account='ICICI Bank 0993';
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};stored.bankReconciliationDrafts['BRD-INCOMING-TRANSFER']={id:'BRD-INCOMING-TRANSFER',account,nature:'PERSONAL',transactions:[{date:'2026-08-31',description:'Sent using GAGAN LAMB',reference:'9810863742',debit:0,credit:3000,balance:3000}],summary:{from:'2026-08-31',to:'2026-08-31',openingBalance:0,closingBalance:3000,totalDebits:0,totalCredits:3000,validated:true},resolutions:{},temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'owner-user',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const made=invoke('POST','/api/expenses/bank-statements/create-incoming',{role:'owner',body:{draftId:'BRD-INCOMING-TRANSFER',rowId:'bank-0',sourceKind:'internal',fromNature:'SANKI',fromAccount:'Axis Bank 3448',classification:'owner_withdrawal',note:'Business account funded personal account'}});
  assert.equal(made.status,200,JSON.stringify(made.body));assert.equal(made.body.transfer.amount,3000);assert.equal(made.body.transfer.toAccount,account);assert.ok(made.body.rows.some(x=>x.id==='bank-0'&&x.status==='resolved'&&x.resolution.action==='create_transfer'));
  const source=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Axis Bank 3448'}}).body,destination=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'PERSONAL',account}}).body;
  assert.ok(source.entries.some(x=>x.id===made.body.transfer.id&&x.debit===3000));assert.ok(destination.entries.some(x=>x.id===made.body.transfer.id&&x.credit===3000));
  const balances=invoke('GET','/api/expenses/balances',{role:'owner',query:{nature:'PERSONAL',from:'2026-08-31',to:'2026-08-31'}}).body,received=balances.accounts.find(x=>x.name===account);assert.equal(received.transferredIn,3000);assert.equal(received.reconciliationIssues.some(x=>x.reference===made.body.transfer.id),false);
  assert.notEqual(invoke('POST','/api/expenses/bank-statements/create-incoming',{role:'owner',body:{draftId:'BRD-INCOMING-TRANSFER',rowId:'bank-0',sourceKind:'internal',fromNature:'SANKI',fromAccount:'Axis Bank 3448',classification:'owner_withdrawal',note:'duplicate'}}).status,200);
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('discarding a temporary bank preview removes only that draft and its temporary file',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored)),temporaryFile=path.join(tempDir,'discard-preview.csv');
  fs.writeFileSync(temporaryFile,'temporary statement');stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};stored.bankReconciliationDrafts['BRD-DISCARD']={id:'BRD-DISCARD',account:'Axis Bank 3448',nature:'SANKI',transactions:[],summary:{from:'2026-08-25',to:'2026-08-27'},resolutions:{'bank-0':{action:'exclude'}},temporaryFile,createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};stored.bankStatements=stored.bankStatements||{};stored.bankStatements['Axis Bank 3448']=stored.bankStatements['Axis Bank 3448']||{transactions:{official:{id:'official'}},imports:[]};fs.writeFileSync(expenseFile,JSON.stringify(stored));
  assert.equal(invoke('POST','/api/expenses/bank-statements/discard-draft',{role:'admin',body:{draftId:'BRD-DISCARD'}}).status,400);
  const discarded=invoke('POST','/api/expenses/bank-statements/discard-draft',{role:'admin',body:{draftId:'BRD-DISCARD',reason:'Restart with new reconciliation workflow'}});assert.equal(discarded.status,200,JSON.stringify(discarded.body));assert.equal(discarded.body.discarded.from,'2026-08-25');assert.equal(fs.existsSync(temporaryFile),false);
  const after=JSON.parse(fs.readFileSync(expenseFile,'utf8'));assert.equal(after.bankReconciliationDrafts['BRD-DISCARD'],undefined);assert.ok(after.bankStatements['Axis Bank 3448'].transactions.official);fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('personal bank reconciliation is Owner-only and stored separately from business books',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8'));
  stored.bankStatements=stored.bankStatements||{};
  stored.bankStatements['IndusInd Bank 7883']={transactions:{business:{id:'BTX-BUSINESS',date:'2026-08-20',debit:10,credit:0,balance:90}},imports:[],reconciledThrough:'2026-08-20'};
  stored.bankStatements['PERSONAL|IndusInd Bank 7883']={transactions:{personal:{id:'BTX-PERSONAL',date:'2026-08-21',debit:20,credit:0,balance:80}},imports:[],reconciledThrough:'2026-08-21'};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const denied=invoke('GET','/api/expenses/bank-statements',{role:'admin',query:{nature:'PERSONAL',account:'IndusInd Bank 7883'}});
  assert.equal(denied.status,403);
  const personal=invoke('GET','/api/expenses/bank-statements',{role:'owner',query:{nature:'PERSONAL',account:'IndusInd Bank 7883'}});
  assert.equal(personal.status,200);assert.deepEqual(personal.body.transactions.map(x=>x.id),['BTX-PERSONAL']);
  const business=invoke('GET','/api/expenses/bank-statements',{role:'admin',query:{nature:'SAMAST',account:'IndusInd Bank 7883'}});
  assert.equal(business.status,200);assert.deepEqual(business.body.transactions.map(x=>x.id),['BTX-BUSINESS']);
  const adminConfig=invoke('GET','/api/expenses/config',{role:'admin'}).body;
  assert.deepEqual(adminConfig.accountsByNature.PERSONAL,[]);
  assert.deepEqual(adminConfig.bankAccountsByNature.PERSONAL,[]);
  const ownerConfig=invoke('GET','/api/expenses/config',{role:'owner'}).body;
  assert.deepEqual(ownerConfig.bankAccountsByNature.PERSONAL,['ICICI Bank 0992','ICICI Bank 0993','IndusInd Bank 7883','Namita 5464']);
});

test('finalizing removes every visible draft summary for that entity and bank',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),now=new Date().toISOString();
  stored.bankReconciliationDrafts=stored.bankReconciliationDrafts||{};
  ['BRD-PERSONAL-CURRENT','BRD-PERSONAL-OLDER'].forEach((id,i)=>{stored.bankReconciliationDrafts[id]={id,account:'Namita 5464',nature:'PERSONAL',transactions:[],summary:{from:'2026-08-01',to:'2026-08-27',openingBalance:0,closingBalance:0,totalDebits:0,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',originalName:'personal.csv',fileHash:'hash-'+i,createdAt:now,createdBy:'owner-user',expiresAt:'2099-01-01T00:00:00.000Z'};});
  fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const finalized=invoke('POST','/api/expenses/bank-statements/finalize',{role:'owner',body:{draftId:'BRD-PERSONAL-CURRENT'}});
  assert.equal(finalized.status,200);assert.equal(finalized.body.summary,undefined);
  const after=JSON.parse(fs.readFileSync(expenseFile,'utf8'));
  assert.equal(Object.values(after.bankReconciliationDrafts).filter(x=>x.account==='Namita 5464'&&x.nature==='PERSONAL').length,0);
  assert.equal(after.bankStatements['PERSONAL|Namita 5464'].reconciledThrough,'2026-08-27');
  const view=invoke('GET','/api/expenses/bank-statements',{role:'owner',query:{nature:'PERSONAL',account:'Namita 5464'}});
  assert.equal(view.body.draft,null);assert.equal(view.body.updatedThrough,'2026-08-27');
});

test('personal bank reconciliation UI uses Owner-only entity accounts and clears finalized actions',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/function bankAccountsForNature\(nature\)/);
  assert.match(html,/id="bs_nature"/);
  assert.match(html,/bankAccountsByNature/);
  assert.match(html,/encodeURIComponent\(bankNature\)/);
  assert.match(html,/selected account is automatic/);
  assert.match(html,/bank-statements\?nature='\+encodeURIComponent\(bankNature\)/);
  assert.match(html,/bankDraftId='';el\('bs_msg'\)\.textContent=d\.balanceReconciled\?/);
  assert.match(html,/Reconciliation history/);
  assert.match(html,/Statement period: /);
  assert.match(html,/Finalized: /);
  assert.doesNotMatch(html,/Reconciled through '\+\(d\.lastReconciliation\.through/);
  assert.match(html,/bs_history/);
  assert.match(html,/Decision and actual reason/);
  assert.match(html,/Open original statement/);
  assert.match(html,/Export CSV/);
  assert.match(html,/Review \/ Resolve/);
  assert.match(html,/Remark \/ action performed/);
  assert.match(html,/Only actions valid for this reconciliation result are shown/);
  assert.match(html,/Link existing transaction/);
  assert.match(html,/split_allocation/);
  assert.match(html,/Connected transfer references/);
  assert.match(html,/Other receipt description/);
  assert.match(html,/Link existing/);
  assert.match(html,/Undo decision/);
  assert.match(html,/Reconciliation-period remark/);
  assert.match(html,/Transaction remark — required/);
  assert.match(html,/sankiBankReconForm/);
  assert.match(html,/Switching tabs will not remove it/);
  assert.match(html,/Discard temporary preview/);
  assert.match(html,/bank-statements\/discard-draft/);
  assert.doesNotMatch(html,/resolvePaytmSettlement/);
  assert.doesNotMatch(html,/Record missing bank transaction/);
  assert.doesNotMatch(html,/>Paytm settlement<\/button>/);
});

test('stored bank statements can only be opened through an authorised reconciliation account',()=>{
  assert.ok(router.stack.some(x=>x.route&&x.route.path==='/api/expenses/bank-statements/file/:id'&&x.route.methods.get));
});

test('PDF and image OCR statement text uses the same normalized bank rows', () => {
  const rows=parseBankStatementText('24/08/2026 UPI reimbursement UTR720 720.00 0.00 1527.00\n25/08/2026 Customer receipt UTR500 0.00 500.00 2027.00');
  assert.deepEqual(rows.map(x=>({date:x.date,debit:x.debit,credit:x.credit,balance:x.balance})),[
    {date:'2026-08-24',debit:720,credit:0,balance:1527},
    {date:'2026-08-25',debit:0,credit:500,balance:2027}
  ]);
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/\.pdf,.png,.jpg,.jpeg,.webp/);
  const telegram=fs.readFileSync(path.join(__dirname,'..','modules','telegram.js'),'utf8');
  assert.match(telegram,/callback_data:'am:bank'/);
  assert.match(telegram,/Temporary statement preview ready/);
  assert.match(telegram,/Nothing has been stored in the official bank history/);
});

test('Axis Bank PDF text ignores the statement-period header and validates the transaction table', () => {
  const text='Account Statement Report\nStatement of Axis Bank Account No : XXXX3448 for the period ( From : 22/08/2026 To : 25/08/2026 )\nOpening Balance: INR 47,844.86\nS.NOTransaction\nDate\n(dd/mm/yyyy)\nValue Date\n(dd/mm/yyyy)\nParticularsAmount(INR)Debit/CreditBalance(INR)Cheque\nNumber\nBranch Name(SOL)\n122/08/202622/08/2026\nIFT/PB0307710755/PAYTM PAYMENTS SERVICES LIMITED/2/\n14,755.36CR62,600.22 (100)\n222/08/202622/08/2026GST @18% on BNA Convenience Chrgs\n45.00DR62,555.22 (4820)\n322/08/202622/08/2026BNA Convenience Chrgs\n250.00DR62,305.22 (4820)\n422/08/202622/08/2026\nINB/IFT/REDACTED/TPARTY TRANSFER\n2,247.00DR60,058.22 (4820)\n524/08/202624/08/2026\nINB/IFT/REDACTED/TPARTY TRANSFER\n3,341.00DR56,717.22 (4820)\n6TRANSACTION TOTAL DR/CR\n5,883.00/14,755.36\nClosing Balance: INR 56,717.22';
  const rows=parseBankStatementText(text);
  assert.equal(rows.length,5);
  assert.deepEqual(rows.map(x=>[x.date,x.debit,x.credit,x.balance]),[
    ['2026-08-22',0,14755.36,62600.22],['2026-08-22',45,0,62555.22],['2026-08-22',250,0,62305.22],['2026-08-22',2247,0,60058.22],['2026-08-24',3341,0,56717.22]
  ]);
  assert.equal(rows.statementSummary.validated,true);
  assert.equal(rows.statementSummary.closingBalance,56717.22);
});

test('Axis salary-account PDF reads the declared period and every debit and credit column',()=>{
  const text=`Statement of Axis Account No: 925010025223645 for the period (From: 22-08-2026 To: 28-08-2026)
Tran DateChq NoParticularsDebitCreditBalanceInit.
Br
OPENING BALANCE 5090.00
22-08-2026UPI/P2A/660007395196/SANJAN SI/KKBK/UPI/ 2000.00 7090.001101
22-08-2026 UPI/P2A/623409923274/CHANDER PRAKASH /UPI/FEDERAL BANK 6090.00 1000.001101
28-08-2026 UPI/P2M/624031462183/Vaibhav Filling Stati/UPI/YES BANK LIMITED YBS 967.08 32.921101
TRANSACTION TOTAL 7057.08 2000.00
CLOSING BALANCE 32.92`;
  const rows=parseBankStatementText(text);
  assert.equal(rows.length,3);
  assert.deepEqual(rows.map(x=>({date:x.date,debit:x.debit,credit:x.credit,balance:x.balance})),[
    {date:'2026-08-22',debit:0,credit:2000,balance:7090},
    {date:'2026-08-22',debit:6090,credit:0,balance:1000},
    {date:'2026-08-28',debit:967.08,credit:0,balance:32.92}
  ]);
  assert.deepEqual(rows.statementSummary,{format:'Axis Bank salary-account PDF',from:'2026-08-22',to:'2026-08-28',openingBalance:5090,closingBalance:32.92,totalDebits:7057.08,totalCredits:2000,validated:true});
});

test('ICICI detailed statement preserves its transaction fields and negative balances', () => {
  const text=`Detailed
Statement
Name:TIANA TRADERSA/C Branch:DELHI VIKASPURI C FIVE
Branch Address:ICICI BANK LTD C 5 VIKASPURI DELHI
A/C No:194405000425A/C Type:CAA
Transaction Period:From 22/08/2026 To 29/08/2026IFSC Code:ICIC0005613
Sl
No
Tran
Id
Value
Date
Transaction
Date
Transaction
Posted
Date
Cheque no /
Ref No
Transaction
Remarks
Withdrawal (Dr)
Deposit
(Cr)
Balance
1S1616
9495
27/Aug/2
026
27/Aug/202627/08/2026
08:27:36 PM
INF/INFT/045660500
511/GAGANLAMBA
2,99,000.
00
-
24,47,837
.66
Page Total
Opening Bal:-21,48,837.66
Withdrawls:2,99,000.00
Deposits:0.00
Closing Bal:-24,47,837.66
Legends Used in Account Statement
ICICI BANK LTD`;
  const rows=parseBankStatementText(text);
  assert.equal(rows.length,1);
  assert.deepEqual({date:rows[0].date,reference:rows[0].reference,debit:rows[0].debit,credit:rows[0].credit,balance:rows[0].balance},{date:'2026-08-27',reference:'S16169495',debit:299000,credit:0,balance:-2447837.66});
  assert.match(rows[0].description,/INF\/INFT\/045660500 511\/GAGANLAMBA/);
  assert.deepEqual(rows.statementSummary,{format:'ICICI Bank PDF',from:'2026-08-22',to:'2026-08-29',openingBalance:-2148837.66,closingBalance:-2447837.66,totalDebits:299000,totalCredits:0,validated:true});
});

test('ICICI 0425 one-time opening remains exact to paise from 22 August 2026',()=>{
  const balances=invoke('GET','/api/expenses/balances',{role:'owner',query:{nature:'SANKI',from:'2026-08-22',to:'2026-08-22'}}).body;
  const account=balances.accounts.find(x=>x.name==='Tiana 0425');
  assert.equal(account.opening,-2148837.66);
  const ledger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Tiana 0425',from:'2026-08-22',to:'2026-08-22'}}).body;
  assert.equal(ledger.entries.find(x=>x.kind==='opening').credit,-2148837.66);
  const stored=JSON.parse(fs.readFileSync(path.join(tempDir,'expenses.json'),'utf8'));
  assert.equal(stored.openingBalanceDates['Tiana 0425'],'2026-08-22');
  assert.ok(stored.oneTimeMigrations['icici-0425-opening-2026-08-22-negative-2148837-66']);
});

test('legacy ICICI 0425 reconciliation history uses the statement-declared period',()=>{
  const expenseFile=path.join(tempDir,'expenses.json'),original=JSON.parse(fs.readFileSync(expenseFile,'utf8')),stored=JSON.parse(JSON.stringify(original));
  delete stored.oneTimeMigrations['icici-0425-reconciliation-period-2026-08-22-to-2026-08-29'];
  stored.bankStatements=stored.bankStatements||{};
  stored.bankStatements['Tiana 0425']={transactions:{one:{id:'BTX-ICICI-ONE',date:'2026-08-27',debit:299000,credit:0,balance:-2447837.66}},reconciledThrough:'2026-08-27',lastReconciliation:{through:'2026-08-27',closingBalance:-2447837.66},imports:[{id:'BST-1788012502554',from:'2026-08-27',to:'2026-08-27',statementSummary:{format:'ICICI Bank PDF',from:'2026-08-27',to:'2026-08-27',openingBalance:-2148837.66,closingBalance:-2447837.66}}]};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));
  invoke('GET','/api/expenses/config',{role:'owner'});
  const repaired=JSON.parse(fs.readFileSync(expenseFile,'utf8')),book=repaired.bankStatements['Tiana 0425'],record=book.imports[0];
  assert.deepEqual({from:record.from,to:record.to,summaryFrom:record.statementSummary.from,summaryTo:record.statementSummary.to,reconciledThrough:book.reconciledThrough,lastThrough:book.lastReconciliation.through},{from:'2026-08-22',to:'2026-08-29',summaryFrom:'2026-08-22',summaryTo:'2026-08-29',reconciledThrough:'2026-08-29',lastThrough:'2026-08-29'});
  assert.equal(repaired.oneTimeMigrations['icici-0425-reconciliation-period-2026-08-22-to-2026-08-29'].result,'corrected');
  fs.writeFileSync(expenseFile,JSON.stringify(original));
});

test('legacy finalized payments without approvedAt remain visible in their account ledger', () => {
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8'));
  stored.expenses['EX-LEGACY-240']={id:'EX-LEGACY-240',date:'2026-08-22',nature:'SANKI',status:'paid',vendor:'Legacy vendor',particulars:'Legacy bill',amount:240,paidAmount:240,account:'Prashant Axis 3645',payments:[{id:'PAY-001',amount:240,date:'2026-08-22',account:'Prashant Axis 3645',proof:'/api/expenses/photo/legacy-240.jpg',paidBy:'prashant'}]};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const ledger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Prashant Axis 3645',from:'2026-08-01',to:'2026-08-31'}}).body;
  assert.ok(ledger.entries.some(x=>x.id==='EX-LEGACY-240/PAY-001'&&x.debit===240));
  const spending=invoke('GET','/api/expenses/spending-dashboard',{role:'owner',query:{nature:'SANKI',account:'Prashant Axis 3645',from:'2026-08-01',to:'2026-08-31'}}).body;
  assert.ok(spending.payments.some(x=>x.id==='EX-LEGACY-240'&&x.amount===240));
  const filtered=invoke('GET','/api/expenses/list',{role:'owner',query:{nature:'SANKI',payingAccount:'Prashant Axis 3645'}}).body;
  assert.ok(filtered.expenses.some(x=>x.id==='EX-LEGACY-240'));
  const excluded=invoke('GET','/api/expenses/list',{role:'owner',query:{nature:'SANKI',payingAccount:'Tiana 0425'}}).body;
  assert.ok(!excluded.expenses.some(x=>x.id==='EX-LEGACY-240'));
});

test('paying-account expense totals reconcile to ledger movements by payment date', () => {
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8'));
  stored.expenses['EX-SPLIT-ACCOUNT']={id:'EX-SPLIT-ACCOUNT',date:'2026-08-20',nature:'SANKI',status:'paid',vendor:'Split account vendor',particulars:'Split payment',amount:1000,paidAmount:1000,approvedAt:'2026-08-20T10:00:00.000Z',billPhoto:'/api/expenses/photo/split.jpg',payments:[
    {id:'PAY-3645',amount:300,date:'2026-08-24',account:'Prashant Axis 3645',proof:'/api/expenses/photo/3645.jpg',paidBy:'prashant'},
    {id:'PAY-OTHER',amount:700,date:'2026-08-23',account:'Tiana 0425',proof:'/api/expenses/photo/other.jpg',paidBy:'prashant'}
  ]};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const filtered=invoke('GET','/api/expenses/list',{role:'owner',query:{nature:'SANKI',payingAccount:'Prashant Axis 3645',from:'2026-08-24',to:'2026-08-24'}}).body;
  const row=filtered.expenses.find(x=>x.id==='EX-SPLIT-ACCOUNT');
  assert.equal(row.payingAccountAmount,300);
  assert.equal(filtered.totals.all,300);
  assert.equal(filtered.totals.paid,300);
  const ledger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Prashant Axis 3645',from:'2026-08-24',to:'2026-08-24'}}).body;
  assert.equal(ledger.entries.find(x=>x.id==='EX-SPLIT-ACCOUNT/PAY-3645').debit,300);
});

test('a cross-entity expense appears in the ledger of the account that paid it', () => {
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8'));
  stored.expenses['EX-CROSS-240']={id:'EX-CROSS-240',date:'2026-08-22',nature:'SAMAST',status:'paid',vendor:'Geeta Poojan Bhandar',particulars:'Cross-entity payment',amount:240,paidAmount:240,approvedAt:'2026-08-22T10:00:00.000Z',billPhoto:'/api/expenses/photo/cross-240.jpg',payments:[{id:'PAY-240',amount:240,date:'2026-08-22',account:'Prashant Axis 3645',proof:'/api/expenses/photo/pay-240.jpg',paidBy:'prashant'}]};
  stored.expenses['EX-CROSS-PERSONAL-125']={id:'EX-CROSS-PERSONAL-125',date:'2026-08-22',nature:'PERSONAL',status:'paid',vendor:'Personal purchase',particulars:'Owner expense paid through company account',amount:125,paidAmount:125,approvedAt:'2026-08-22T11:00:00.000Z',billPhoto:'/api/expenses/photo/personal-125.jpg',payments:[{id:'PAY-125',amount:125,date:'2026-08-22',account:'Prashant Axis 3645',proof:'/api/expenses/photo/pay-125.jpg',paidBy:'owner-user'}]};
  fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const ledger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Prashant Axis 3645',from:'2026-08-01',to:'2026-08-31'}}).body;
  const movement=ledger.entries.find(x=>x.id==='EX-CROSS-240/PAY-240');
  assert.equal(movement.debit,240);
  assert.match(movement.description,/\[SAMAST\]/);
  const samastOnly=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Prashant Axis 3645',expenseNature:'SAMAST',from:'2026-08-01',to:'2026-08-31'}}).body;
  assert.ok(samastOnly.entries.some(x=>x.id==='EX-CROSS-240/PAY-240'));
  const personalOnly=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Prashant Axis 3645',expenseNature:'PERSONAL',from:'2026-08-01',to:'2026-08-31'}}).body;
  assert.ok(!personalOnly.entries.some(x=>x.id==='EX-CROSS-240/PAY-240'));
  const balances=invoke('GET','/api/expenses/balances',{role:'owner',query:{nature:'SANKI',from:'2026-08-01',to:'2026-08-31'}}).body;
  assert.ok(balances.accounts.find(x=>x.name==='Prashant Axis 3645').spent>=240);
  const refreshed=JSON.parse(fs.readFileSync(expenseFile,'utf8'));refreshed.bankReconciliationDrafts=refreshed.bankReconciliationDrafts||{};refreshed.bankReconciliationDrafts['BRD-CROSS-240']={id:'BRD-CROSS-240',account:'Prashant Axis 3645',nature:'SANKI',transactions:[{date:'2026-08-22',description:'UPI payment to Geeta Poojan Bhandar',reference:'623412221042',debit:240,credit:0,balance:125},{date:'2026-08-22',description:'Personal purchase',reference:'PERSONAL125',debit:125,credit:0,balance:0}],summary:{from:'2026-08-22',to:'2026-08-22',openingBalance:365,closingBalance:0,totalDebits:365,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};fs.writeFileSync(expenseFile,JSON.stringify(refreshed));
  const reconciliation=invoke('POST','/api/expenses/bank-statements/reconcile',{role:'admin',body:{draftId:'BRD-CROSS-240',account:'Prashant Axis 3645'}}).body;
  assert.ok(reconciliation.rows.some(x=>x.status==='matched'&&x.bank&&x.app&&x.app.id==='EX-CROSS-240/PAY-240'));
  assert.ok(reconciliation.rows.some(x=>x.status==='matched'&&x.bank&&x.app&&x.app.id==='EX-CROSS-PERSONAL-125/PAY-125'));
});

test('bank payment can split between a current expense and a paid pre-system vendor opening balance', () => {
  const expenseFile=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseFile,'utf8')),baseline=JSON.parse(JSON.stringify(stored));
  stored.vendorsByNature.SAMAST.fnp={name:'FNP',notes:''};
  stored.expenses['EX-FNP-100']={id:'EX-FNP-100',date:'2026-08-22',nature:'SAMAST',status:'paid',vendor:'FNP',particulars:'Flowers for 22 August',ledger:'Flowers',amount:100,paidAmount:100,approvedAt:'2026-08-22T10:00:00.000Z',billPhoto:'/api/expenses/photo/fnp.jpg',payments:[{id:'PAY-001',amount:100,date:'2026-08-22',account:'Prashant Axis 3645',proof:'/api/expenses/photo/fnp-payment.jpg',paidBy:'prashant'}]};
  const draft={id:'BRD-FNP-OPENING',account:'Prashant Axis 3645',nature:'SANKI',transactions:[{date:'2026-08-22',description:'UPI payment to Ferns n petals',reference:'623499417992',debit:300,credit:0,balance:1000}],summary:{from:'2026-08-22',to:'2026-08-22',openingBalance:1300,closingBalance:1000,totalDebits:300,totalCredits:0,validated:true},resolutions:{},temporaryFile:'',createdAt:new Date().toISOString(),createdBy:'prashant',expiresAt:'2099-01-01T00:00:00.000Z'};
  stored.bankReconciliationDrafts['BRD-FNP-OPENING']=draft;fs.writeFileSync(expenseFile,JSON.stringify(stored));
  const resolved=invoke('POST','/api/expenses/bank-statements/resolve',{role:'admin',body:{draftId:draft.id,rowId:'bank-0',action:'opening_vendor_payable_split',appId:'EX-FNP-100/PAY-001',principalAmount:100,openingPayableAmount:200,vendor:'FNP',preSystemDates:'2026-08-20, 2026-08-21',reason:'Opening payable for flowers from 20 and 21 August; paid with 22 August expense',remark:'Split current expense and pre-system balance'}});
  assert.equal(resolved.status,200,JSON.stringify(resolved.body));assert.equal(resolved.body.rows.find(x=>x.id==='bank-0').status,'resolved');assert.equal(resolved.body.rows.find(x=>x.app&&x.app.id==='EX-FNP-100/PAY-001').status,'resolved');
  const afterResolve=JSON.parse(fs.readFileSync(expenseFile,'utf8')),savedDraft=afterResolve.bankReconciliationDrafts[draft.id];afterResolve.bankStatements['Prashant Axis 3645']={transactions:{fnp:{id:'BTX-FNP-300',date:'2026-08-22',description:'UPI payment to Ferns n petals',reference:'623499417992',debit:300,credit:0,balance:1000}},imports:[]};fs.writeFileSync(expenseFile,JSON.stringify(afterResolve));
  applyFinalizedOpeningVendorPayables(savedDraft,'prashant');
  const finalized=JSON.parse(fs.readFileSync(expenseFile,'utf8')),opening=finalized.vendorOpeningPayables.find(x=>x.reconciliationDraft===draft.id);
  assert.equal(opening.nature,'SAMAST');assert.equal(opening.amount,200);assert.equal(opening.paidAmount,200);assert.equal(opening.status,'paid');assert.equal(opening.bankTransactionId,'BTX-FNP-300');assert.equal(finalized.reconciliationExpenses.some(x=>x.reconciliationDraft===draft.id),false);
  const vendors=invoke('GET','/api/expenses/vendors',{role:'owner',query:{nature:'SAMAST',search:'FNP',from:'2026-08-22',to:'2026-08-22'}}).body.vendors,book=vendors.find(x=>x.name==='FNP');assert.equal(book.billed,300);assert.equal(book.paid,300);assert.equal(book.outstanding,0);assert.ok(book.entries.some(x=>x.source==='opening_vendor_payable'));
  const ledger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Prashant Axis 3645',from:'2026-08-22',to:'2026-08-22'}}).body.entries;assert.equal(ledger.find(x=>x.id==='EX-FNP-100/PAY-001').debit,100);assert.equal(ledger.find(x=>x.id===opening.adjustmentId).debit,200);
  fs.writeFileSync(expenseFile,JSON.stringify(baseline));
});

test('reconciliation UI offers the pre-system vendor payable split with an audit remark', () => {
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/Split: current expense \+ pre-system vendor payable/);assert.match(html,/opening_vendor_payable_split/);assert.match(html,/Pre-system opening payable/);assert.match(html,/does not enter the current-period P&amp;L/);
});

test('new accounting UI defaults to current month, uses compact rows and opens proofs in-page', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /function monthStart\(\)/);
  assert.match(html, /id="sd_from" value="'\+monthStart\(\)/);
  assert.match(html, /id="vf_from" value="'\+monthStart\(\)/);
  assert.match(html, /id="cf_from" value="'\+monthStart\(\)/);
  assert.match(html, /id="rf_from" value="'\+monthStart\(\)/);
  assert.match(html, /id="rf_nature"><option value="">All<\/option>/);
  assert.match(html, /id="rf_status"><option value="" selected>All<\/option>/);
  assert.match(html, /Transaction ref:/);
  assert.match(html, /nature='\+encodeURIComponent\(el\('rf_nature'\)\.value\)/);
  assert.match(html, /id="rv_from" value="'\+monthStart\(\)/);
  assert.match(html, /id="proofViewer" class="proof-viewer"/);
  assert.match(html, /window\.viewProof=function/);
  assert.match(html, /matches\('img\.thumb'\)/);
  assert.match(html, /\.claim-card summary \{ padding:3px 7px/);
  assert.doesNotMatch(html, /fmt\(approvedNow\)\+' now/);
  assert.match(html,/el\(x\)\.disabled=!cfg\.isOwner/);
  assert.match(html,/Only the Owner can change amounts/);
  assert.match(html,/<label>Paying account<\/label><select id="lf_account">/);
  assert.match(html,/payingAccount='\+encodeURIComponent/);
  assert.match(html,/var claimantName=e\.claimant\|\|e\.createdBy/);
  assert.match(html,/var payingAccounts=Array\.from\(new Set/);
  assert.match(html,/class="claim-row-meta">'\+esc\(claimantName\)\+' │ '\+esc\(payingAccountLabel\)/);
  assert.match(html,/recordedPaymentWithProof=\(e\.payments\|\|\[\]\)\.slice\(\)\.reverse\(\)\.find/);
  assert.match(html,/function reimbursementAccountOptions\(\)/);
  assert.match(html,/Select actual paying account/);
  assert.match(html,/String\(nature\|\|'SANKI'\)\.trim\(\)\.toUpperCase\(\)/);
  assert.match(html,/function populateVendorPayAccounts\(nature,refreshIfEmpty\)/);
  assert.match(html,/populateVendorPayAccounts\(e\.nature\|\|'SANKI',true\)/);
  assert.match(html,/Refreshing paying accounts/);
  assert.match(html,/<label>Expense entity<\/label><select id="lg_expense_nature"><option value="">All<\/option>/);
  assert.match(html,/expenseNature='\+encodeURIComponent\(el\('lg_expense_nature'\)\.value\)/);
  assert.match(html,/id="editPayingAccount"/);
  assert.match(html,/paymentAccount:el\('editPayingAccount'\)\.value/);
});

test('credit payables filter includes fully and partially unpaid credit expenses', () => {
  const full = invoke('POST','/api/expenses',{body:{date:'2026-08-21',vendor:'Credit Full',amount:600,billPhoto:'/api/expenses/photo/c1.jpg',paymentType:'Credit'}});
  invoke('POST','/api/expenses/:id',{params:{id:full.body.expense.id},body:{ledger:'FOOD EXPENSE'},role:'owner'});
  invoke('POST','/api/expenses/:id/approve',{params:{id:full.body.expense.id},role:'owner'});
  const partial = invoke('POST','/api/expenses',{body:{date:'2026-08-21',vendor:'Credit Partial',amount:800,billPhoto:'/api/expenses/photo/c2.jpg',paymentType:'Credit'}});
  invoke('POST','/api/expenses/:id',{params:{id:partial.body.expense.id},body:{ledger:'FOOD EXPENSE'},role:'owner'});
  invoke('POST','/api/expenses/:id/approve',{params:{id:partial.body.expense.id},role:'owner'});
  invoke('POST','/api/expenses/:id/pay',{params:{id:partial.body.expense.id},body:{amount:200,account:'Counter Cash',paymentProof:'/api/expenses/photo/cpay.jpg'},role:'owner'});
  const credit = invoke('GET','/api/expenses/pending-payments',{query:{bucket:'credit',from:'2026-08-01',to:'2026-08-21'},role:'owner'});
  assert.equal(credit.status,200);
  assert.ok(credit.body.expenses.some(x=>x.id===full.body.expense.id&&x.balanceDue===600));
  assert.ok(credit.body.expenses.some(x=>x.id===partial.body.expense.id&&x.balanceDue===600));
});

test('Owner/Admin can edit finalized expenses with a mandatory audit reason', () => {
  const created=invoke('POST','/api/expenses',{body:{date:'2026-08-21',vendor:'Audit Vendor',amount:300,billPhoto:'/api/expenses/photo/audit.jpg',paymentType:'Cash'}});
  invoke('POST','/api/expenses/:id',{params:{id:created.body.expense.id},body:{ledger:'FOOD EXPENSE'},role:'owner'});
  invoke('POST','/api/expenses/:id/approve',{params:{id:created.body.expense.id},role:'owner'});
  const blocked=invoke('POST','/api/expenses/:id',{params:{id:created.body.expense.id},body:{vendor:'Corrected Audit Vendor'},role:'owner'});
  assert.equal(blocked.status,400);
  const edited=invoke('POST','/api/expenses/:id',{params:{id:created.body.expense.id},body:{vendor:'Corrected Audit Vendor',amount:450,requestedAmount:450,editReason:'Corrected vendor and amount'},role:'owner'});
  assert.equal(edited.status,200);
  assert.equal(edited.body.expense.vendor,'Corrected Audit Vendor');
  assert.equal(edited.body.expense.amount,450);
  assert.equal(edited.body.expense.requestedAmount,450);
  assert.equal(edited.body.expense.auditHistory.at(-1).reason,'Corrected vendor and amount');
  assert.ok(edited.body.expense.auditHistory.at(-1).changes.some(x=>x.field==='amount'&&x.before===300&&x.after===450));
  assert.equal(invoke('POST','/api/expenses/:id',{params:{id:created.body.expense.id},body:{vendor:'No Access',editReason:'test'},role:'accounting'}).status,403);
  invoke('POST','/api/expenses/:id/pay',{params:{id:created.body.expense.id},role:'owner',body:{amount:450,account:'Axis Bank 3448',paymentProof:'/api/expenses/photo/account-before.jpg'}});
  const accountFixed=invoke('POST','/api/expenses/:id',{params:{id:created.body.expense.id},role:'owner',body:{paymentAccount:'Prashant Axis 3645',editReason:'Correct paying account'}});
  assert.equal(accountFixed.status,200);assert.equal(accountFixed.body.expense.account,'Prashant Axis 3645');assert.equal(accountFixed.body.expense.payments.at(-1).account,'Prashant Axis 3645');
  const oldLedger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Axis Bank 3448'}}).body;
  const newLedger=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Prashant Axis 3645'}}).body;
  assert.ok(!oldLedger.entries.some(x=>x.id===created.body.expense.id+'/PAY-001'));assert.ok(newLedger.entries.some(x=>x.id===created.body.expense.id+'/PAY-001'));
  assert.equal(invoke('POST','/api/expenses/:id',{params:{id:created.body.expense.id},role:'admin',body:{paymentAccount:'Tiana 0425',editReason:'Admin tries account'}}).status,403);
});

test('Owner/Admin can correct an expense entity before payment and the change is audited', () => {
  const made=invoke('POST','/api/expenses',{body:{nature:'SANKI',vendor:'Entity Fix Vendor',amount:250,billPhoto:'/api/expenses/photo/entity.jpg',paymentType:'Cash'}});
  const id=made.body.expense.id;
  const moved=invoke('POST','/api/expenses/:id',{params:{id},role:'owner',body:{nature:'SAMAST'}});
  assert.equal(moved.body.expense.nature,'SAMAST');
  assert.ok(moved.body.expense.auditHistory.at(-1).changes.some(x=>x.field==='nature'&&x.before==='SANKI'&&x.after==='SAMAST'));
  const claimantBlocked=invoke('POST','/api/expenses/:id',{params:{id},role:'claimant',body:{nature:'SANKI'}});
  assert.equal(claimantBlocked.status,403);
});

test('account ledgers show newest entries first and Axis sales begin on 2026-08-21', () => {
  invoke('POST','/api/expenses/transfers',{role:'owner',body:{nature:'SANKI',fromAccount:'Counter Cash',toAccount:'Tiana 0425',amount:10,date:'2026-08-19',proof:'/api/expenses/photo/t-old.jpg'}});
  invoke('POST','/api/expenses/transfers',{role:'owner',body:{nature:'SANKI',fromAccount:'Counter Cash',toAccount:'Tiana 0425',amount:20,date:'2026-08-21',proof:'/api/expenses/photo/t-new.jpg'}});
  const cash=invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account:'Counter Cash'},role:'owner'}).body.entries.filter(x=>x.kind!=='opening');
  assert.ok(cash[0].date>=cash.at(-1).date);
  fs.writeFileSync(path.join(tempDir,'sales.json'),JSON.stringify({sales:[
    {id:'OLD-SALE',day:'2026-08-20',paymentMode:'UPI',total:100,channel:'POS'},
    {id:'NEW-SALE',day:'2026-08-21',paymentMode:'UPI',total:200,channel:'POS'}
  ]}));
  const axis=invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account:'Axis Bank 3448'},role:'owner'}).body.entries;
  assert.equal(axis.some(x=>x.id==='SALE/OLD-SALE'),false);
  assert.equal(axis.some(x=>x.id==='SALE/NEW-SALE'),true);
});

test('Counter Cash resets on 22 August and cash sales round upward to the next ₹10', () => {
  fs.writeFileSync(path.join(tempDir,'sales.json'),JSON.stringify({sales:[
    {id:'CASH-OLD',day:'2026-08-21',paymentMode:'Cash',total:999,channel:'POS'},
    {id:'CASH-1999',day:'2026-08-22',paymentMode:'Cash',total:1999,channel:'POS'},
    {id:'CASH-1496',day:'2026-08-22',paymentMode:'Cash',total:1496,channel:'POS'},
    {id:'CASH-9992',day:'2026-08-22',paymentMode:'Cash',total:9992,channel:'POS'},
    {id:'CASH-1749',day:'2026-08-22',paymentMode:'Cash',total:1749,channel:'POS'}
  ]}));
  const ledger=invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account:'Counter Cash'},role:'owner'}).body;
  assert.equal(ledger.entries.some(x=>x.id==='SALE/CASH-OLD'),false);
  assert.equal(ledger.entries.find(x=>x.id==='OPENING').credit,5240);
  assert.equal(ledger.entries.find(x=>x.id==='OPENING').date,'2026-08-22');
  assert.deepEqual(['CASH-1999','CASH-1496','CASH-9992','CASH-1749'].map(id=>ledger.entries.find(x=>x.id==='SALE/'+id).credit),[2000,1500,10000,1750]);
});

test('Shopify Paytm sales use clearing while store-credit and test orders never hit Axis', () => {
  fs.writeFileSync(path.join(tempDir,'orders.json'),JSON.stringify({orders:{
    paytm:{id:'paytm',name:'#2718',orderNumber:2718,createdAt:'2026-08-23T10:00:00Z',financialStatus:'paid',paymentGateways:['Paytm'],total:50000,refundAmount:0},
    credit:{id:'credit',name:'#2717',orderNumber:2717,createdAt:'2026-08-23T10:00:00Z',financialStatus:'paid',paymentGateways:['store credit'],total:20000,refundAmount:0},
    test:{id:'test',name:'#2720',orderNumber:2720,createdAt:'2026-08-23T10:00:00Z',financialStatus:'paid',paymentGateways:['Paytm'],total:100,refundAmount:0}
  }}));
  const axis=invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account:'Axis Bank 3448'},role:'owner'}).body.entries;
  const clearing=invoke('GET','/api/expenses/account-ledger',{query:{nature:'SANKI',account:'Paytm Settlement Clearing'},role:'owner'}).body.entries;
  assert.equal(axis.some(x=>String(x.id).startsWith('SHOPIFY/')),false);
  const receipt=clearing.find(x=>x.id==='PAYTM-RECEIPTS/2026-08-23');assert.equal(receipt.credit,50000);assert.deepEqual(receipt.connectedSales.map(x=>x.id),['SHOPIFY/paytm']);
  assert.equal(clearing.some(x=>['SHOPIFY/credit','SHOPIFY/test'].includes(x.id)),false);
  const config=invoke('GET','/api/expenses/config',{role:'owner'}).body;
  assert.equal(config.ledgerAccountsByNature.SANKI.includes('Paytm Settlement Clearing'),true);
  assert.equal(config.accountsByNature.SANKI.includes('Paytm Settlement Clearing'),false);
  assert.equal(fs.readFileSync(path.join(tempDir,'orders.json'),'utf8').includes('store credit'),true);
});

test('bank-charge reconciliation adjustments become visible spending without double-posting the bank', () => {
  invoke('POST','/api/expenses/balances',{role:'owner',body:{nature:'SANKI',adjust:{account:'Axis Bank 3448',direction:'deduct',amount:250,date:'2026-08-22',note:'Bank charges'}}});
  invoke('POST','/api/expenses/balances',{role:'owner',body:{nature:'SANKI',adjust:{account:'Axis Bank 3448',direction:'deduct',amount:45,date:'2026-08-22',note:'Bank charges'}}});
  const spending=invoke('GET','/api/expenses/spending-dashboard',{role:'owner',query:{from:'2026-08-22',to:'2026-08-22',nature:'SANKI',category:'BANK CHARGES'}}).body;
  const repaired=spending.payments.filter(x=>x.kind==='Bank-reconciled expense'&&x.account==='Axis Bank 3448');
  assert.equal(repaired.reduce((n,x)=>n+x.amount,0),295);
  assert.deepEqual(repaired.map(x=>x.amount).sort((a,b)=>a-b),[45,250]);
  const expenseStorePath=path.join(tempDir,'expenses.json'),store=JSON.parse(fs.readFileSync(expenseStorePath,'utf8'));
  store.paytmSettlements.push({id:'PTM-LEGACY-NOTE',date:'2026-08-22',bankAccount:'Axis Bank 3448',netAmount:14755.36,grossAmount:14755.36,chargeAmount:0,orderIds:['2718','2721'],reason:'Paytm settlement; charges ₹539.64'});
  fs.writeFileSync(expenseStorePath,JSON.stringify(store));
  const repairedPaytm=invoke('GET','/api/expenses/spending-dashboard',{role:'owner',query:{from:'2026-08-22',to:'2026-08-22',nature:'SANKI',category:'PAYTM CHARGES'}}).body.payments.find(x=>x.vendor==='Paytm');
  assert.equal(repairedPaytm.amount,540);
  const persisted=JSON.parse(fs.readFileSync(expenseStorePath,'utf8')).paytmSettlements.find(x=>x.id==='PTM-LEGACY-NOTE');
  assert.equal(persisted.chargeAmount,539.64);
  assert.equal(persisted.grossAmount,15295);
  assert.equal(JSON.parse(fs.readFileSync(expenseStorePath,'utf8')).reconciliationExpenses.find(x=>x.settlementId==='PTM-LEGACY-NOTE').category,'PAYTM CHARGES');
  const source=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(source,/Connected sales:/);
  assert.match(source,/Hidden \/ unknown charges/);
});

test('Paytm clearing shows customer receipt date, bank settlement and charges as separate rows',()=>{
  const expenseStorePath=path.join(tempDir,'expenses.json'),stored=JSON.parse(fs.readFileSync(expenseStorePath,'utf8')),baseline=JSON.parse(JSON.stringify(stored));
  fs.writeFileSync(path.join(tempDir,'orders.json'),JSON.stringify({orders:{sale:{id:'sale-15295',name:'#SALE15295',orderNumber:'SALE15295',createdAt:'2026-08-21T10:00:00Z',financialStatus:'paid',paymentGateways:['Paytm'],total:15295,refundAmount:0}}}));
  stored.paytmSettlements=[{id:'PTM-DISPLAY',date:'2026-08-22',bankAccount:'Axis Bank 3448',bankTransactionId:'BTX-PAYTM-DISPLAY',netAmount:14755.36,grossAmount:15295,chargeAmount:539.64,orderIds:['SALE15295'],reason:'Daily Paytm settlement'}];stored.bankStatements=stored.bankStatements||{};stored.bankStatements['Axis Bank 3448']={transactions:{paytm:{id:'BTX-PAYTM-DISPLAY',date:'2026-08-22',credit:14755.36,debit:0,reference:'PAYTM-REF-22'}},imports:[]};fs.writeFileSync(expenseStorePath,JSON.stringify(stored));
  const entries=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Paytm Settlement Clearing',from:'2026-08-21',to:'2026-08-22'}}).body.entries,receipt=entries.find(x=>x.kind==='paytm_customer_receipts'),settlement=entries.find(x=>x.kind==='paytm_settlement'),charge=entries.find(x=>x.kind==='paytm_charge');
  assert.equal(receipt.date,'2026-08-21');assert.equal(receipt.credit,15295);assert.equal(receipt.paytmSummary.knownCharges,539.64);assert.equal(receipt.paytmSummary.unknownCharges,0);assert.deepEqual(receipt.connectedSales.map(x=>x.orderNumber),['15295']);
  assert.equal(settlement.date,'2026-08-22');assert.equal(settlement.debit,14755.36);assert.equal(settlement.reference,'PAYTM-REF-22');assert.equal(charge.debit,539.64);assert.equal(charge.reference,'PAYTM-REF-22');assert.equal(charge.balance,0);
  fs.writeFileSync(path.join(tempDir,'orders.json'),JSON.stringify({orders:{}}));
  const legacyEntries=invoke('GET','/api/expenses/account-ledger',{role:'owner',query:{nature:'SANKI',account:'Paytm Settlement Clearing',from:'2026-08-21',to:'2026-08-22'}}).body.entries,legacyReceipt=legacyEntries.find(x=>x.kind==='paytm_customer_receipts');
  assert.equal(legacyReceipt.date,'2026-08-21');assert.equal(legacyReceipt.credit,15295);assert.equal(legacyReceipt.connectedSales[0].orderNumber,'SALE15295');assert.equal(legacyEntries.find(x=>x.kind==='paytm_charge').balance,0);
  fs.writeFileSync(expenseStorePath,JSON.stringify(baseline));
});

test('Telegram approval and payment keep one expense isolated through its complete lifecycle', () => {
  const created=invoke('POST','/api/expenses',{body:{nature:'SANKI',vendor:'Telegram Vendor',particulars:'Telegram fuel',amount:225,bill:'printed',billPhoto:'/api/expenses/photo/tg-bill.jpg',qrPhoto:'/api/expenses/photo/tg-qr.jpg',paymentType:'UPI'}});
  const id=created.body.expense.id;
  const approved=telegramApproveExpense(id,'prashant',{ledger:'Fuel Expenses',amount:225});
  assert.equal(approved.success,true);
  assert.equal(approved.expense.status,'approved');
  assert.equal(approved.expense.approvedBy,'prashant');
  assert.equal(approved.expense.ledger,'Fuel Expenses');
  const paid=telegramRecordPayment(id,'prashant',{amount:225,account:'3645',date:'2026-08-24',proof:'/api/expenses/photo/tg-payment.jpg'});
  assert.equal(paid.success,true);
  assert.equal(paid.expense.status,'paid');
  assert.equal(paid.payment.account,'Prashant Axis 3645');
  const final=telegramExpense(id);
  assert.equal(final.payments.length,1);
  const stored=JSON.parse(fs.readFileSync(path.join(tempDir,'expenses.json'),'utf8'));
  const lifecycle=(stored.auditLog||[]).filter(x=>x.subjectId===id);
  assert.ok(lifecycle.some(x=>x.action==='APPROVED'&&x.device==='Telegram'));
  assert.ok(lifecycle.some(x=>x.action==='PAYMENT_RECORDED'&&x.device==='Telegram'));
  const telegramSource=fs.readFileSync(path.join(__dirname,'..','modules','telegram.js'),'utf8');
  assert.match(telegramSource,/callback_data:'ba:approve:'\+id/);
  assert.match(telegramSource,/callback_data:'bp:start:'\+e\.id/);
  assert.match(telegramSource,/msg\.reply_to_message&&msg\.reply_to_message\.message_id/);
  assert.match(telegramSource,/notifyExpenseForApproval/);
  assert.match(telegramSource,/is already fully paid\. No second payment can be recorded/);
  assert.match(telegramSource,/is already fully paid\. This screenshot was not recorded/);
  assert.match(telegramSource,/function notifyUserWithPhoto/);
  assert.match(telegramSource,/callback_data:'am:transfer'/);
  assert.match(telegramSource,/callback_data:'am:received'/);
  assert.match(telegramSource,/callback_data:'am:pay'/);
  assert.match(telegramSource,/callback_data:'am:reimburse'/);
  assert.match(telegramSource,/callback_data:'am:more'/);
  assert.match(telegramSource,/Only the Owner or Admin can operate the Accounts menu/);
  assert.match(telegramSource,/Account adjustment/);
  assert.match(telegramSource,/Receivable collection/);
  assert.match(telegramSource,/Partial vendor payment/);
  assert.match(telegramSource,/Consolidated bill payment/);
  assert.match(telegramSource,/Procurement payable/);
  assert.match(telegramSource,/TELEGRAM_PERSONAL_BOT_TOKEN/);
  assert.match(telegramSource,/personal-webhook/);
  assert.match(telegramSource,/handlePersonalMessage/);
  assert.match(telegramSource,/handlePersonalCallback/);
  assert.match(telegramSource,/function sendPersonalApprovalPreview/);
  assert.match(telegramSource,/action==='categories'\|\|action==='catpage'/);
  assert.match(telegramSource,/return sendPersonalApprovalPreview\(id,draft\)/);
  assert.match(telegramSource,/Already paid by claimant/);
  assert.match(telegramSource,/Not paid yet/);
  assert.match(telegramSource,/function approvalCategorySuggestions/);
  assert.match(telegramSource,/Search category/);
  assert.match(telegramSource,/Closest existing categories/);
  assert.match(telegramSource,/PERSONAL screenshots now belong in/);
  assert.match(telegramSource,/Only an authorised PERSONAL user can use this bot/);
  const expenseSource=fs.readFileSync(path.join(__dirname,'..','modules','expenses.js'),'utf8');
  assert.match(expenseSource,/Payment proof is attached below/);
  assert.match(expenseSource,/Amount paid:/);
  assert.match(expenseSource,/telegram\.notifyUserWithPhoto\(username,proof,details,button\)/);
  const duplicate=telegramRecordPayment(id,'prashant',{amount:225,account:'3645',date:'2026-08-24',proof:'/api/expenses/photo/tg-payment-duplicate.jpg'});
  assert.equal(duplicate.success,false);
  assert.match(duplicate.error,/not awaiting a vendor payment/);
  assert.equal(telegramExpense(id).payments.length,1);
});

test('Namita Personal bot expenses use separate Namita account and cash ledgers', () => {
  const upi=createTelegramPersonalExpense({username:'namita',amount:850,account:'0992',date:'2026-08-25',particulars:'Household shopping',vendor:'Local Store',ledger:'Shopping',proof:'/api/expenses/photo/namita-upi.jpg',sourceKey:'namita-upi-1'});
  assert.equal(upi.success,true);assert.equal(upi.expense.account,'Namita 5464');assert.equal(upi.expense.createdBy,'namita');
  const cash=createTelegramPersonalExpense({username:'namita',amount:300,account:'cash',date:'2026-08-25',particulars:'Household cash',vendor:'Local Store',ledger:'Household Staff',proof:'/api/expenses/photo/namita-cash.jpg',sourceKey:'namita-cash-1'});
  assert.equal(cash.success,true);assert.equal(cash.expense.account,'Namita Cash');
  const source=fs.readFileSync(path.join(__dirname,'..','modules','telegram.js'),'utf8');assert.match(source,/isNamita/);assert.match(source,/Only the Owner can approve or settle PERSONAL expenses/);
});

test('Telegram resolves ambiguous Personal bank digits when funding Namita', () => {
  const transfer=telegramRecordNamitaTransfer('gaganlambasanki',{fromAccount:'7883',toAccount:'5464',amount:2500,date:'2026-08-25',proof:'/api/expenses/photo/namita-funding.jpg',note:'Household funds'});
  assert.equal(transfer.success,true);assert.equal(transfer.transfer.fromNature,'PERSONAL');assert.equal(transfer.transfer.fromAccount,'IndusInd Bank 7883');assert.equal(transfer.transfer.toAccount,'Namita 5464');assert.equal(transfer.transfer.classification,'internal_transfer');
});

test('native Telegram accounting actions reuse API permissions and ledger posting', () => {
  const owner={username:'gaganlambasanki',roles:['owner']},prashant={username:'prashant',roles:['admin']};
  const receipt=telegramApi('POST','/api/expenses/receipts',owner,{body:{nature:'SANKI',account:'3645',amount:250,source:'Telegram refund',receiptType:'refund',proof:'/api/expenses/photo/tg-receipt.jpg'}});
  assert.equal(receipt.success,true);
  assert.equal(receipt.receipt.account,'Prashant Axis 3645');
  const denied=telegramApi('POST','/api/expenses/receipts',prashant,{body:{nature:'SANKI',account:'3645',amount:250,source:'Not allowed',receiptType:'refund',proof:'/api/expenses/photo/tg-receipt.jpg'}});
  assert.equal(denied.status,403);
  const telegramSource=fs.readFileSync(path.join(__dirname,'..','modules','telegram.js'),'utf8');
  assert.match(telegramSource,/NATIVE_ACCOUNT_ACTIONS/);
  assert.match(telegramSource,/callback_data:'am:native-confirm'/);
  assert.match(telegramSource,/callback_data:'am:native-edit'/);
  assert.match(telegramSource,/callback_data:'am:native-cancel'/);
  assert.match(telegramSource,/After this, attach the payment\/proof screenshot/);
});

test('internal reconciliation flags malformed transfers and requires a recorded payment override', () => {
  const expenseFile = path.join(tempDir, 'expenses.json');
  const stored = JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
  stored.transfers = stored.transfers || [];
  stored.transfers.push({ id:'TR-BROKEN', nature:'SANKI', fromAccount:'Counter Cash', toAccount:'Axis Bank 3448', amount:50, date:'2026-08-21', proof:'' });
  fs.writeFileSync(expenseFile, JSON.stringify(stored));
  const balances = invoke('GET', '/api/expenses/balances', { role:'owner', query:{nature:'SANKI'} });
  assert.equal(balances.status, 200);
  assert.equal(balances.body.accounts.find(x => x.name === 'Counter Cash').reconciled, false);

  const created = invoke('POST', '/api/expenses', { body:{vendor:'Override Vendor',amount:75,billPhoto:'/api/expenses/photo/bill.jpg',paymentType:'Cash'} });
  invoke('POST', '/api/expenses/:id', { params:{id:created.body.expense.id}, body:{ledger:'FOOD EXPENSE'}, role:'owner' });
  invoke('POST', '/api/expenses/:id/approve', { params:{id:created.body.expense.id}, role:'owner' });
  const blocked = invoke('POST', '/api/expenses/:id/pay', { params:{id:created.body.expense.id}, role:'owner', body:{account:'Counter Cash',paymentProof:'/api/expenses/photo/pay.jpg'} });
  assert.equal(blocked.status, 409);
  assert.equal(blocked.body.requiresOverride, true);
  const paid = invoke('POST', '/api/expenses/:id/pay', { params:{id:created.body.expense.id}, role:'owner', body:{account:'Counter Cash',paymentProof:'/api/expenses/photo/pay.jpg',reconciliationOverrideReason:'Urgent approved vendor payment'} });
  assert.equal(paid.status, 200);
  assert.equal(paid.body.expense.payments.at(-1).reconciliationOverrideReason, 'Urgent approved vendor payment');
});
