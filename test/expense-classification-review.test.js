'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const classificationReview = require('../modules/expense-classification-review');
const creditCardAccounting = require('../modules/credit-card-accounting');

function clone(value) { return JSON.parse(JSON.stringify(value)); }

function fixture() {
  let persisted = {
    expenses:{
      'EX-100':{
        id:'EX-100',nature:'SANKI',date:'2026-11-01',ledger:'Props Expense',vendor:'Old Vendor',particulars:'Bed sheet',amount:1200,status:'paid',account:'Axis Bank 3448',paymentType:'UPI',billPhoto:'/proof/bill.jpg',purchasePaymentProof:'/proof/pay.jpg',reconciliationSource:{draftId:'BRD-1',rowId:'bank-1',bankReference:'BANK-100'},payments:[{id:'PAY-001',amount:1200,date:'2026-11-01',account:'Axis Bank 3448',paymentType:'UPI',proof:'/proof/pay.jpg',bankReconciliationDraft:'BRD-1',bankReconciliationRow:'bank-1'}]
      },
      'EX-200':{id:'EX-200',nature:'SANKI',date:'2026-10-20',ledger:'Fittings',vendor:'Shop',particulars:'Display fitting',amount:500,status:'approved',account:'Counter Cash',paymentType:'Cash'},
      'EX-DRAFT':{id:'EX-DRAFT',nature:'SANKI',date:'2026-11-04',ledger:'Fittings',vendor:'Draft Vendor',particulars:'Unposted draft',amount:300,status:'pending',account:'Counter Cash',paymentType:'Cash'},
      'EX-MOVE':{id:'EX-MOVE',nature:'SANKI',date:'2026-11-04',ledger:'Fittings',vendor:'Alias Vendor',particulars:'Entity move draft',amount:325,status:'pending',account:'Counter Cash',paymentType:'Cash'},
      'EX-HOTEL':{id:'EX-HOTEL',nature:'PERSONAL',date:'2026-11-04',ledger:'Home Expense',vendor:'Hotel Vendor',particulars:'Sewak Park repair',amount:350,status:'pending',account:'ICICI Bank 0992',paymentType:'UPI',reportingUnit:'SEWAK_PARK_HOTEL'},
      'EX-REFUND':{id:'EX-REFUND',nature:'SANKI',date:'2026-11-04',ledger:'Fittings',vendor:'Refund Vendor',particulars:'Returned goods',amount:375,status:'pending',account:'Counter Cash',paymentType:'Cash'},
      'EX-UNCLASSIFIED':{id:'EX-UNCLASSIFIED',nature:'SANKI',date:'2026-11-05',ledger:'',vendor:'Unknown Vendor',particulars:'Needs classification',amount:55,status:'pending',account:'Counter Cash',paymentType:'Cash'},
      'EX-PRIVATE':{id:'EX-PRIVATE',nature:'PERSONAL',date:'2026-11-02',ledger:'Home Expense',vendor:'Private Vendor',particulars:'Private spend',amount:900,status:'paid',account:'ICICI Bank 0993',paymentType:'UPI'}
    },
    reconciliationExpenses:[
      {id:'BRE-001',nature:'SANKI',date:'2026-11-03',category:'Props Expense',vendor:'Bank Vendor',particulars:'Statement charge',amount:75,account:'Axis Bank 3448',paymentType:'Bank',adjustmentId:'ADJ-1',bankTransactionId:'BTX-1',reconciliationDraft:'BRD-2'},
      {id:'BRE-PENDING',nature:'SANKI',date:'2026-11-03',category:'Props Expense',vendor:'Pending bank row',particulars:'Not finalized',amount:50,account:'Axis Bank 3448',reconciliationDraft:'BRD-PENDING'},
      {id:'BRE-REFUND',nature:'SANKI',date:'2026-11-03',category:'Props Expense',vendor:'Card reversal',particulars:'Refund credit',amount:-25,account:'ICICI 8000',creditCardStatementId:'CCS-1'},
      {id:'BRE-LINKED-REFUND',nature:'SANKI',date:'2026-11-03',category:'Props Expense',vendor:'Supplier refund',particulars:'Allocated refund',amount:20,account:'Axis Bank 3448',expenseRefundReceiptId:'RF-1'},
      {id:'CCE-ST-1-ROW-1',nature:'SANKI',date:'2026-11-03',category:'Fittings',type:'variable',categoryGroup:'Operations',vendor:'Card Vendor',particulars:'Card purchase',amount:400,account:'Synthetic Card 1234',expenseRefundReceiptId:'',creditCardId:'CC-1',ownerOnly:false,creditCardStatementId:'ST-1',creditCardRowId:'ROW-1',classification:'expense',unbilled:false,source:'credit_card_statement'},
      {id:'CCE-PRIVATE-ROW-1',nature:'SANKI',date:'2026-11-03',category:'Fittings',type:'variable',categoryGroup:'Operations',vendor:'Private Card Vendor',particulars:'Owner-only card purchase',amount:425,account:'Owner Card 9999',creditCardId:'CC-PRIVATE',ownerOnly:true,creditCardStatementId:'ST-PRIVATE',creditCardRowId:'ROW-1',classification:'expense',unbilled:false,source:'credit_card_statement'}
    ],
    expenseRefunds:[{id:'RF-SOURCE',status:'received',sources:[{expenseId:'EX-REFUND',amount:100}],components:[]}],
    bankDateOverrides:{'EX-100/PAY-001':{bankReference:'BANK-100'}},
    bankStatements:{'Axis Bank 3448':{transactions:{'BTX-1':{id:'BTX-1',date:'2026-11-03',debit:75,reference:'BANK-100'}}}},
    bankReconciliationDrafts:{'BRD-PENDING':{id:'BRD-PENDING',account:'Axis Bank 3448',transactions:[{date:'2026-11-03',debit:50}]}},
    auditLog:[],auditSeq:0
  };
  let saves = 0, saveOptions = [];
  const routes = new Map(), router = {};
  for (const method of ['get','patch','post']) router[method] = (path, handler) => routes.set(method.toUpperCase() + ' ' + path, handler);
  const catalogs = {
    SANKI:[{name:'Accessories',group:'Operations',type:'variable'},{name:'Fittings',group:'Operations',type:'variable'},{name:'Props Expense',group:'Marketing',type:'marketing'}],
    SAMAST:[{name:'Props Expense',group:'Marketing',type:'marketing'}],
    PERSONAL:[{name:'Home Expense',group:'Personal',type:'variable'},{name:'Props Expense',group:'Personal',type:'variable'}]
  };
  const vendorCatalogs = {
    SANKI:['Studio Store','Old Vendor','studio store',' Alias Vendor ','Correct Bank Vendor','Correct Legacy Vendor','Identified Vendor','Meta Ads Vendor'],
    SAMAST:['Samast Supplier'],
    PERSONAL:['Private Vendor','Correct Vendor','Canonical Vendor']
  };
  const deps = {
    loadStore:() => clone(persisted),
    saveStore:(store,options) => { persisted = clone(store); saveOptions.push(clone(options || {})); saves += 1; },
    canManageCategoriesAndVendors:req => req.user.roles.includes('owner') || req.user.username === 'prashant',
    claimantOnly:req => req.user.roles.includes('claimant') && !req.user.roles.some(role => ['owner','admin'].includes(role)),
    approvalNatures:req => req.user.roles.includes('owner') ? ['SANKI','SAMAST','PERSONAL'] : ['SANKI','SAMAST'].concat(req.user.permissions.personalAccounting ? ['PERSONAL'] : []),
    canManagePersonalAccounting:req => req.user.roles.includes('owner') || req.user.permissions.personalAccounting === true,
    canViewExpense:(req,expense) => expense.nature !== 'PERSONAL' || req.user.roles.includes('owner') || req.user.permissions.personalAccounting === true,
    canAccessExpense:(req,expense) => !expense.ownerOnly || req.user.roles.includes('owner') || (expense.nature === 'PERSONAL' && req.user.permissions.personalAccounting === true),
    pickableLedgers:(_store,nature) => clone(catalogs[nature] || []),
    vendorChoicesForNature:(_store,nature) => clone(vendorCatalogs[nature] || []),
    audit:(store,req,action,subjectType,subjectId,data) => {
      store.auditSeq = (store.auditSeq || 0) + 1;
      store.auditLog = store.auditLog || [];
      store.auditLog.push({id:'AUD-'+store.auditSeq,user:req.user.username,action,subjectType,subjectId,before:data.before,after:data.after,note:data.note});
    }
  };
  classificationReview.register(router,deps);
  function invoke(method, routePath, options = {}) {
    const pattern = options.pattern || routePath, handler = routes.get(method + ' ' + pattern);
    assert.ok(handler, method + ' ' + pattern + ' route registered');
    const req = {query:options.query || {},body:options.body || {},params:options.params || {},user:{username:options.username || (options.role === 'owner' ? 'owner-user' : 'prashant'),roles:[options.role || 'owner'],permissions:options.permissions || {}}};
    let status = 200, body, headers = {};
    const res = {status(code){status=code;return this;},json(value){body=value;return this;},setHeader(name,value){headers[name.toLowerCase()]=value;}};
    handler(req,res);
    return {status,body,headers};
  }
  function token(recordId, sourceType = 'expense') {
    const expense = sourceType === 'reconciliation_expense' ? [].concat(persisted.reconciliationExpenses || []).find(record => record.id === recordId) : (persisted.expenses || {})[recordId];
    assert.ok(expense, 'token source exists: ' + sourceType + ':' + recordId);
    return {expectedRevision:Number(expense.classificationRevision) || 0,fingerprint:classificationReview.recordFingerprint(expense, sourceType)};
  }
  return {invoke,token,mutateStore(change){const next=clone(persisted);change(next);persisted=next;},get store(){return clone(persisted);},get saves(){return saves;},get saveOptions(){return clone(saveOptions);}};
}

test('classification review summarizes valid and reconciliation-created expenses without exposing PERSONAL rows', () => {
  const app = fixture();
  const response = app.invoke('GET','/api/expenses/classification-review',{role:'admin',query:{entities:'SANKI',category:'Props Expense',from:'2026-11-01',to:'2026-11-30'}});
  assert.equal(response.status,200);
  assert.deepEqual(response.body.selectedEntities,['SANKI']);
  assert.deepEqual([response.body.selectedFrom,response.body.selectedTo],['2026-11-01','2026-11-30']);
  assert.deepEqual(response.body.expenses.map(row => [row.sourceType,row.recordId,row.amount]),[
    ['expense','EX-100',1200],['reconciliation_expense','BRE-001',75]
  ]);
  assert.equal(response.body.expenses[0].proofCount,2);
  assert.deepEqual(response.body.expenses[0].proofs,[
    {kind:'bill',label:'Bill 1',url:'/proof/bill.jpg'},
    {kind:'personal_payment',label:'Personal payment 1',url:'/proof/pay.jpg'}
  ]);
  assert.equal(response.body.expenses[0].reference,'BANK-100');
  assert.equal(response.body.expenses[0].reconciled,true);
  assert.deepEqual(response.body.expenses[0].editableFields,['category','vendor','narration']);
  assert.match(response.body.expenses[0].fieldLocks.entity,/reconciliation/i);
  assert.deepEqual(response.body.expenses[1].reference,'BTX-1');
  assert.deepEqual(response.body.expenses[1].editableFields,['category','vendor','narration']);
  const props = response.body.categories.find(category => category.entity === 'SANKI' && category.name === 'Props Expense');
  assert.deepEqual([props.count,props.total,props.valid],[2,1275,true]);
  assert.equal(response.body.categories.some(category => category.entity === 'PERSONAL'),false);
  assert.equal(response.headers['cache-control'],'no-store');
  assert.deepEqual(response.body.vendorChoices,{SANKI:['Alias Vendor','Correct Bank Vendor','Correct Legacy Vendor','Identified Vendor','Meta Ads Vendor','Old Vendor','Studio Store'],SAMAST:['Samast Supplier']},'vendor choices include only permitted entity masters, sorted and deduplicated');
  assert.deepEqual(response.body.categories.map(category => category.name),['Fittings','Props Expense','Unclassified','Accessories'],'categories with activity are alphabetical before alphabetical zero-value categories');

  const searched = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',search:'Old Vendor'}});
  assert.deepEqual(searched.body.categories.map(category => [category.name,category.count,category.total]),[['Props Expense',1,1200]],'summary search narrows category totals');
  const unclassified = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',category:'Unclassified'}});
  assert.deepEqual(unclassified.body.expenses.map(expense => expense.recordId),['EX-UNCLASSIFIED']);
  assert.deepEqual(unclassified.body.vendorChoices,{SANKI:['Alias Vendor','Correct Bank Vendor','Correct Legacy Vendor','Identified Vendor','Meta Ads Vendor','Old Vendor','Studio Store'],SAMAST:['Samast Supplier'],PERSONAL:['Canonical Vendor','Correct Vendor','Private Vendor']},'category detail responses carry the same permitted vendor masters');
});

test('classification evidence exposes safe labeled proof links once and ignores text or unsafe URLs', () => {
  const app = fixture();
  app.mutateStore(store => {
    const expense = store.expenses['EX-100'];
    expense.billPhotos = ['/proof/bill.jpg','javascript:alert(1)','//evil.example/bill.jpg','/proof\\bill.jpg','/proof/control\u0007.jpg','\n/proof/trimmed-control.jpg'];
    expense.qrPhoto = 'https://files.example.test/vendor-qr.png';
    expense.paymentProofs = ['/proof/company-payment.pdf','http://files.example.test/plain-http.pdf','https://user:secret@files.example.test/credential.pdf'];
    expense.proofs = ['/proof/reconciliation.jpg'];
    expense.exceptionEvidence = 'Uploaded bank statement';
    expense.payments[0].proofs = ['/proof/company-payment.pdf','data:text/plain,unsafe'];
    expense.reimbursementPayments = [{proofs:['/proof/reimbursement.jpg']}];
  });
  const response = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',category:'Props Expense',search:'Old Vendor'}});
  assert.equal(response.status,200);
  assert.deepEqual(response.body.expenses[0].proofs,[
    {kind:'bill',label:'Bill 1',url:'/proof/bill.jpg'},
    {kind:'vendor_qr',label:'QR 1',url:'https://files.example.test/vendor-qr.png'},
    {kind:'personal_payment',label:'Personal payment 1',url:'/proof/pay.jpg'},
    {kind:'payment',label:'Payment 1',url:'/proof/company-payment.pdf'},
    {kind:'reconciliation',label:'Reconciliation 1',url:'/proof/reconciliation.jpg'},
    {kind:'reimbursement',label:'Reimbursement 1',url:'/proof/reimbursement.jpg'}
  ]);
  assert.equal(response.body.expenses[0].proofCount,response.body.expenses[0].proofs.length);
  assert.equal(response.body.expenses[0].proofs.some(proof => /javascript:|data:|evil|http:|user:secret|\\|\u0007|Uploaded bank statement/.test(proof.url)),false);
});

test('PERSONAL classification review requires the existing personal-accounting grant', () => {
  const app = fixture();
  const denied = app.invoke('GET','/api/expenses/classification-review',{role:'admin',query:{entities:'PERSONAL'}});
  assert.equal(denied.status,403);
  const allowed = app.invoke('GET','/api/expenses/classification-review',{role:'admin',permissions:{personalAccounting:true},query:{entities:'PERSONAL',category:'Home Expense'}});
  assert.equal(allowed.status,200);
  assert.equal(allowed.body.expenses[0].recordId,'EX-PRIVATE');
  assert.deepEqual(Object.keys(allowed.body.vendorChoices),['SANKI','SAMAST','PERSONAL']);
  assert.deepEqual(allowed.body.vendorChoices.PERSONAL,['Canonical Vendor','Correct Vendor','Private Vendor']);
  const claimant = app.invoke('GET','/api/expenses/classification-review',{role:'claimant',username:'arshpreet'});
  assert.equal(claimant.status,403);
});

test('owner-only business and card expenses stay hidden from Prashant and cannot be edited', () => {
  const app = fixture();
  const hidden = app.invoke('GET','/api/expenses/classification-review',{role:'admin',query:{entities:'SANKI',category:'Fittings',search:'Owner-only card purchase'}});
  assert.equal(hidden.status,200);
  assert.equal(hidden.body.expenses.length,0);
  const denied = app.invoke('PATCH','/api/expenses/classification-review/:id',{role:'admin',pattern:'/api/expenses/classification-review/:id',params:{id:'CCE-PRIVATE-ROW-1'},body:{sourceType:'reconciliation_expense',...app.token('CCE-PRIVATE-ROW-1','reconciliation_expense'),category:'Props Expense',reason:'Unauthorized classification attempt'}});
  assert.equal(denied.status,403);
  assert.match(denied.body.error,/restricted to the Owner/i);
  assert.equal(app.saves,0);
  const owner = app.invoke('GET','/api/expenses/classification-review',{role:'owner',query:{entities:'SANKI',category:'Fittings',search:'Owner-only card purchase'}});
  assert.equal(owner.status,200);
  assert.deepEqual(owner.body.expenses.map(row => row.recordId),['CCE-PRIVATE-ROW-1']);
});

test('single classification update moves only an unposted unlinked expense and derives category metadata', () => {
  const app = fixture(), before = app.store.expenses['EX-DRAFT'];
  const totalBefore = Object.values(app.store.expenses).reduce((sum,expense) => sum + expense.amount, 0) + app.store.reconciliationExpenses.reduce((sum,expense) => sum + expense.amount, 0);
  const lockedBefore = clone({amount:before.amount,date:before.date,account:before.account,paymentType:before.paymentType});
  const response = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-DRAFT'},body:{sourceType:'expense',...app.token('EX-DRAFT'),entity:'PERSONAL',category:'Props Expense',vendor:'Correct Vendor',narration:'Instagram shoot bed sheet',reason:'Correct reporting classification'},permissions:{personalAccounting:true},role:'admin'});
  assert.equal(response.status,200,JSON.stringify(response.body));
  assert.deepEqual(response.body.expense,{...response.body.expense,entity:'PERSONAL',category:'Props Expense',vendor:'Correct Vendor',narration:'Instagram shoot bed sheet'});
  const after = app.store.expenses['EX-DRAFT'];
  assert.deepEqual({amount:after.amount,date:after.date,account:after.account,paymentType:after.paymentType},lockedBefore);
  assert.equal(after.nature,'PERSONAL');assert.equal(after.ledger,'Props Expense');assert.equal(after.vendor,'Correct Vendor');assert.equal(after.particulars,'Instagram shoot bed sheet');
  assert.equal(after.type,'variable');assert.equal(after.categoryGroup,'Personal');
  assert.equal(after.auditHistory.at(-1).classificationOnly,true);
  assert.equal(app.store.auditLog.at(-1).action,'EXPENSE_CLASSIFICATION_CHANGED');
  assert.match(app.store.auditLog.at(-1).note,/reconciliation evidence unchanged/);
  assert.deepEqual(app.saveOptions,[{skipCategoryNormalization:true}]);
  const totalAfter = Object.values(app.store.expenses).reduce((sum,expense) => sum + expense.amount, 0) + app.store.reconciliationExpenses.reduce((sum,expense) => sum + expense.amount, 0);
  assert.equal(totalAfter,totalBefore,'classification corrections preserve total spend');
});

test('protected update fields are rejected before any expense or evidence is changed', () => {
  const app = fixture(), before = app.store;
  const response = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-100'},body:{sourceType:'expense',...app.token('EX-100'),category:'Fittings',amount:1,reason:'Attempt protected edit'}});
  assert.equal(response.status,400);
  assert.match(response.body.error,/amount.*locked/i);
  assert.deepEqual(app.store,before);
  assert.equal(app.saves,0);
});

test('reconciliation-created expense classification keeps its financial source links immutable', () => {
  const app = fixture(), before = app.store.reconciliationExpenses[0], bankBefore = clone({bankStatements:app.store.bankStatements,bankReconciliationDrafts:app.store.bankReconciliationDrafts,bankDateOverrides:app.store.bankDateOverrides});
  const lockedBefore = clone({amount:before.amount,date:before.date,account:before.account,paymentType:before.paymentType,adjustmentId:before.adjustmentId,bankTransactionId:before.bankTransactionId,reconciliationDraft:before.reconciliationDraft});
  const response = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'BRE-001'},body:{sourceType:'reconciliation_expense',...app.token('BRE-001','reconciliation_expense'),category:'Fittings',vendor:'Correct Bank Vendor',narration:'Corrected bank fee description',reason:'Correct reporting only'}});
  assert.equal(response.status,200,JSON.stringify(response.body));
  const after = app.store.reconciliationExpenses[0];
  assert.deepEqual({amount:after.amount,date:after.date,account:after.account,paymentType:after.paymentType,adjustmentId:after.adjustmentId,bankTransactionId:after.bankTransactionId,reconciliationDraft:after.reconciliationDraft},lockedBefore);
  assert.deepEqual([after.nature,after.category,after.vendor,after.particulars,after.type,after.categoryGroup],['SANKI','Fittings','Correct Bank Vendor','Corrected bank fee description','variable','Operations']);
  assert.deepEqual({bankStatements:app.store.bankStatements,bankReconciliationDrafts:app.store.bankReconciliationDrafts,bankDateOverrides:app.store.bankDateOverrides},bankBefore,'classification edit does not mutate reconciliation evidence graphs');
  assert.equal(response.body.expense.sourceType,'reconciliation_expense');
  assert.equal(response.body.expense.reconciled,true);
});

test('entity changes are blocked for reconciled, paid and hotel-tagged rows while category corrections remain available', () => {
  const app = fixture(), before = app.store;
  const reconciled = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-100'},permissions:{personalAccounting:true},role:'admin',body:{sourceType:'expense',...app.token('EX-100'),entity:'PERSONAL',category:'Props Expense',reason:'Unsafe entity move'}});
  assert.equal(reconciled.status,409);
  assert.match(reconciled.body.error,/reconciliation/i);
  const sourceOwned = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'BRE-001'},body:{sourceType:'reconciliation_expense',...app.token('BRE-001','reconciliation_expense'),entity:'SAMAST',category:'Props Expense',reason:'Unsafe source move'}});
  assert.equal(sourceOwned.status,409);
  assert.match(sourceOwned.body.error,/finalized bank or card/i);
  const hotel = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-HOTEL'},permissions:{personalAccounting:true},role:'admin',body:{sourceType:'expense',...app.token('EX-HOTEL'),entity:'SANKI',category:'Fittings',reason:'Unsafe hotel entity move'}});
  assert.equal(hotel.status,409);
  assert.match(hotel.body.error,/Sewak Park Hotel/i);
  const refunded = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-REFUND'},body:{sourceType:'expense',...app.token('EX-REFUND'),vendor:'Different Vendor',reason:'Unsafe refund vendor move'}});
  assert.equal(refunded.status,409);
  assert.match(refunded.body.error,/refund/i);
  assert.deepEqual(app.store,before);
  assert.equal(app.saves,0);
});

test('entity changes select an existing target vendor case-insensitively without creating a vendor', () => {
  const app = fixture();
  const response = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-MOVE'},permissions:{personalAccounting:true},role:'admin',body:{sourceType:'expense',...app.token('EX-MOVE'),entity:'PERSONAL',category:'Props Expense',vendor:'canonical vendor',reason:'Move unposted draft to its correct entity'}});
  assert.equal(response.status,200,JSON.stringify(response.body));
  assert.equal(app.store.expenses['EX-MOVE'].vendor,'Canonical Vendor');
  assert.equal(app.store.vendorsByNature,undefined,'classification review never creates a vendor master record');
  assert.ok(response.body.changes.some(change => change.field === 'vendor' && change.before === 'Alias Vendor' && change.after === 'Canonical Vendor'));
});

test('forged vendor and entity-change requests cannot create or copy unregistered vendors', () => {
  const app = fixture(), before = app.store;
  const forgedVendor = app.invoke('PATCH','/api/expenses/classification-review/:id',{
    pattern:'/api/expenses/classification-review/:id',params:{id:'EX-DRAFT'},
    body:{sourceType:'expense',...app.token('EX-DRAFT'),vendor:'Forged Vendor',reason:'Attempt to create vendor from review'}
  });
  assert.equal(forgedVendor.status,400);
  assert.match(forgedVendor.body.error,/existing vendor/i);
  assert.deepEqual(app.store,before);
  assert.equal(app.saves,0);

  const forgedEntityMove = app.invoke('PATCH','/api/expenses/classification-review/:id',{
    role:'admin',permissions:{personalAccounting:true},pattern:'/api/expenses/classification-review/:id',params:{id:'EX-DRAFT'},
    body:{sourceType:'expense',...app.token('EX-DRAFT'),entity:'PERSONAL',category:'Props Expense',reason:'Attempt to copy an unregistered vendor to another entity'}
  });
  assert.equal(forgedEntityMove.status,400);
  assert.match(forgedEntityMove.body.error,/existing vendor/i);
  assert.deepEqual(app.store,before);
  assert.equal(app.saves,0);
});

test('bulk classification updates mixed source types atomically and rolls back every row on validation failure', () => {
  const app = fixture();
  const response = app.invoke('POST','/api/expenses/classification-review/bulk',{body:{records:[{sourceType:'expense',recordId:'EX-100',...app.token('EX-100')},{sourceType:'reconciliation_expense',recordId:'BRE-001',...app.token('BRE-001','reconciliation_expense')}],changes:{category:'Fittings'},reason:'Move both reviewed lines'}});
  assert.equal(response.status,200,JSON.stringify(response.body));
  assert.equal(response.body.count,2);
  assert.equal(app.store.expenses['EX-100'].ledger,'Fittings');
  assert.equal(app.store.reconciliationExpenses[0].category,'Fittings');
  assert.equal(app.store.auditLog.at(-1).action,'EXPENSE_CLASSIFICATION_BULK_CHANGED');

  const beforeFailure = app.store, savesBefore = app.saves;
  const rejected = app.invoke('POST','/api/expenses/classification-review/bulk',{body:{records:[{sourceType:'expense',recordId:'EX-100',...app.token('EX-100')},{sourceType:'expense',recordId:'EX-MISSING',expectedRevision:0,fingerprint:'0'.repeat(64)}],changes:{category:'Props Expense'},reason:'Atomic failure check'}});
  assert.equal(rejected.status,404);
  assert.deepEqual(app.store,beforeFailure);
  assert.equal(app.saves,savesBefore);

  for (const changes of [{vendor:'Should Not Persist'},{narration:'Should Not Persist'},{particulars:'Should Not Persist'}]) {
    const denied = app.invoke('POST','/api/expenses/classification-review/bulk',{body:{records:[{sourceType:'expense',recordId:'EX-100',...app.token('EX-100')}],changes,reason:'Bulk label safety check'}});
    assert.equal(denied.status,400);
    assert.match(denied.body.error,/cannot change/i);
    assert.deepEqual(app.store,beforeFailure);
    assert.equal(app.saves,savesBefore);
  }
});

test('classification review validates date ranges and active destination categories', () => {
  const app = fixture();
  assert.equal(app.invoke('GET','/api/expenses/classification-review',{query:{from:'11/01/2026'}}).status,400);
  assert.equal(app.invoke('GET','/api/expenses/classification-review',{query:{from:'2026-11-03',to:'2026-11-01'}}).status,400);
  const invalidCategory = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-100'},body:{sourceType:'expense',...app.token('EX-100'),category:'Retired Category',reason:'Invalid category check'}});
  assert.equal(invalidCategory.status,400);
  assert.match(invalidCategory.body.error,/active category/i);
  assert.equal(app.saves,0);
});

test('vendor and narration corrections preserve unclassified or retired categories until an explicit category change', () => {
  const app = fixture();
  const unclassified = app.invoke('PATCH','/api/expenses/classification-review/:id',{
    pattern:'/api/expenses/classification-review/:id',params:{id:'EX-UNCLASSIFIED'},
    body:{sourceType:'expense',...app.token('EX-UNCLASSIFIED'),vendor:'Identified Vendor',narration:'Identified historical purchase',reason:'Correct labels without guessing a category'}
  });
  assert.equal(unclassified.status,200,JSON.stringify(unclassified.body));
  assert.deepEqual([
    app.store.expenses['EX-UNCLASSIFIED'].ledger,
    app.store.expenses['EX-UNCLASSIFIED'].vendor,
    app.store.expenses['EX-UNCLASSIFIED'].particulars
  ],['','Identified Vendor','Identified historical purchase']);

  app.mutateStore(store => {
    store.expenses['EX-DRAFT'].ledger = 'Retired Category';
    store.expenses['EX-DRAFT'].type = 'legacy';
    store.expenses['EX-DRAFT'].categoryGroup = 'Archived';
  });
  const retired = app.invoke('PATCH','/api/expenses/classification-review/:id',{
    pattern:'/api/expenses/classification-review/:id',params:{id:'EX-DRAFT'},
    body:{sourceType:'expense',...app.token('EX-DRAFT'),vendor:'Correct Legacy Vendor',reason:'Correct vendor while preserving historical classification'}
  });
  assert.equal(retired.status,200,JSON.stringify(retired.body));
  assert.deepEqual([
    app.store.expenses['EX-DRAFT'].ledger,
    app.store.expenses['EX-DRAFT'].type,
    app.store.expenses['EX-DRAFT'].categoryGroup,
    app.store.expenses['EX-DRAFT'].vendor
  ],['Retired Category','legacy','Archived','Correct Legacy Vendor']);

  const entityWithoutCategory = app.invoke('PATCH','/api/expenses/classification-review/:id',{
    role:'admin',permissions:{personalAccounting:true},pattern:'/api/expenses/classification-review/:id',params:{id:'EX-UNCLASSIFIED'},
    body:{sourceType:'expense',...app.token('EX-UNCLASSIFIED'),entity:'PERSONAL',reason:'Entity changes must choose a valid category'}
  });
  assert.equal(entityWithoutCategory.status,400);
  assert.match(entityWithoutCategory.body.error,/active category/i);
  assert.equal(app.store.expenses['EX-UNCLASSIFIED'].nature,'SANKI');
});

test('classification edits reject stale rows and overlong reasons without overwriting newer data', () => {
  const app = fixture(), staleToken = app.token('EX-DRAFT');
  const saved = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-DRAFT'},body:{sourceType:'expense',...staleToken,category:'Props Expense',reason:'First classification correction'}});
  assert.equal(saved.status,200,JSON.stringify(saved.body));
  const afterFirst = app.store, savesAfterFirst = app.saves;
  const stale = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-DRAFT'},body:{sourceType:'expense',...staleToken,vendor:'Stale Vendor',reason:'Stale classification correction'}});
  assert.equal(stale.status,409);
  assert.match(stale.body.error,/changed after it was loaded/i);
  assert.deepEqual(app.store,afterFirst);
  assert.equal(app.saves,savesAfterFirst);
  const tooLong = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-DRAFT'},body:{sourceType:'expense',...app.token('EX-DRAFT'),vendor:'New Vendor',reason:'x'.repeat(1001)}});
  assert.equal(tooLong.status,400);
  assert.match(tooLong.body.error,/1,000/);
  assert.deepEqual(app.store,afterFirst);
});

test('credit-card classification override survives posting rebuild and is ignored after source identity changes', () => {
  const app = fixture();
  const response = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'CCE-ST-1-ROW-1'},body:{sourceType:'reconciliation_expense',...app.token('CCE-ST-1-ROW-1','reconciliation_expense'),category:'Props Expense',vendor:'Meta Ads Vendor',narration:'Corrected card purchase',reason:'Correct card reporting labels'}});
  assert.equal(response.status,200,JSON.stringify(response.body));
  const cards = {cards:{'CC-1':{id:'CC-1',name:'Synthetic Card',last4:'1234',ownerOnly:false}},statements:{'ST-1':{id:'ST-1',cardId:'CC-1',status:'finalized',kind:'billed',finalizedBy:'owner-user',finalizedAt:'2026-11-04T00:00:00.000Z',rows:[{id:'ROW-1',nature:'SANKI',date:'2026-11-03',amount:400,classification:'expense',category:'Fittings',type:'variable',merchant:'Card Vendor',narration:'Card purchase'}]}}};
  const expenseStore = app.store;
  creditCardAccounting.syncPostings(cards, expenseStore);
  let rebuilt = expenseStore.reconciliationExpenses.find(expense => expense.id === 'CCE-ST-1-ROW-1');
  assert.deepEqual([rebuilt.category,rebuilt.type,rebuilt.categoryGroup,rebuilt.vendor,rebuilt.particulars,rebuilt.classificationRevision],['Props Expense','marketing','Marketing','Meta Ads Vendor','Corrected card purchase',1]);
  assert.equal(creditCardAccounting.cardExpenseRecords(expenseStore).find(expense => expense.id === rebuilt.id).vendor,'Meta Ads Vendor','vendor books preserve an explicit card classification correction verbatim');
  assert.equal(rebuilt.auditHistory.at(-1).classificationOnly,true);
  assert.deepEqual([rebuilt.amount,rebuilt.date,rebuilt.account,rebuilt.creditCardStatementId,rebuilt.creditCardRowId],[400,'2026-11-03','Synthetic Card 1234','ST-1','ROW-1']);

  cards.statements['ST-1'].rows[0].amount = 401;
  creditCardAccounting.syncPostings(cards, expenseStore);
  rebuilt = expenseStore.reconciliationExpenses.find(expense => expense.id === 'CCE-ST-1-ROW-1');
  assert.deepEqual([rebuilt.amount,rebuilt.category,rebuilt.vendor,rebuilt.particulars,rebuilt.classificationRevision],[401,'Fittings','Card Vendor','Card purchase',undefined],'source identity change invalidates a stale override');
});

test('active reconciliation drafts stay excluded until their source draft is removed', () => {
  const app = fixture();
  const before = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',category:'Props Expense',search:'Pending bank row'}});
  assert.equal(before.body.expenses.length,0);
  const store = app.store;
  delete store.bankReconciliationDrafts['BRD-PENDING'];
  assert.equal(classificationReview.reviewableExpense(store.reconciliationExpenses.find(expense => expense.id === 'BRE-PENDING'),'reconciliation_expense',store),true);
});

test('ordinary expenses created from an active bank draft cannot be reclassified before finalization', () => {
  const app = fixture();
  app.mutateStore(store => { store.bankReconciliationDrafts['BRD-1'] = {id:'BRD-1',account:'Axis Bank 3448'}; });
  const hidden = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',category:'Props Expense',search:'Bed sheet'}});
  assert.equal(hidden.status,200);
  assert.equal(hidden.body.expenses.length,0);
  const denied = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-100'},body:{sourceType:'expense',...app.token('EX-100'),category:'Fittings',reason:'Attempt correction before reconciliation finalization'}});
  assert.equal(denied.status,409);
  assert.match(denied.body.error,/pending-reconciliation|source workflow/i);
  assert.equal(app.saves,0);
  app.mutateStore(store => { delete store.bankReconciliationDrafts['BRD-1']; });
  const visible = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',category:'Props Expense',search:'Bed sheet'}});
  assert.deepEqual(visible.body.expenses.map(row => row.recordId),['EX-100']);
});

test('active single and grouped bank links block classification even when provenance exists only in reconciliation maps', () => {
  const app = fixture(), movementId = 'EX-100/PAY-001';
  app.mutateStore(store => {
    delete store.expenses['EX-100'].reconciliationSource;
    delete store.expenses['EX-100'].payments[0].bankReconciliationDraft;
    delete store.expenses['EX-100'].payments[0].bankReconciliationRow;
    store.bankReconciliationDrafts['BRD-LINK'] = {id:'BRD-LINK',resolutions:{'bank-0':{action:'link_existing',appId:movementId}}};
    store.bankDateOverrides[movementId] = {reconciliationDraft:'BRD-LINK',provisional:true};
  });
  let hidden = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',category:'Props Expense',search:'Bed sheet'}});
  assert.equal(hidden.body.expenses.length,0,'link_existing keeps the source-owned expense hidden');
  let denied = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-100'},body:{sourceType:'expense',...app.token('EX-100'),category:'Fittings',reason:'Attempt correction during a single bank link'}});
  assert.equal(denied.status,409);
  app.mutateStore(store => { delete store.bankReconciliationDrafts['BRD-LINK']; });
  let visible = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',category:'Props Expense',search:'Bed sheet'}});
  assert.deepEqual(visible.body.expenses.map(row => row.recordId),['EX-100']);

  app.mutateStore(store => {
    delete store.bankDateOverrides[movementId];
    store.bankReconciliationDrafts['BRD-GROUP'] = {id:'BRD-GROUP',resolutions:{'bank-0':{action:'link_multiple_bank_entries',appId:movementId,bankRowIds:['bank-0','bank-1']},'bank-1':{action:'link_multiple_bank_entries',appId:movementId,bankRowIds:['bank-0','bank-1']}}};
  });
  hidden = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',category:'Props Expense',search:'Bed sheet'}});
  assert.equal(hidden.body.expenses.length,0,'grouped link resolution blocks editing before overrides exist');
  denied = app.invoke('PATCH','/api/expenses/classification-review/:id',{pattern:'/api/expenses/classification-review/:id',params:{id:'EX-100'},body:{sourceType:'expense',...app.token('EX-100'),category:'Fittings',reason:'Attempt correction during grouped bank linking'}});
  assert.equal(denied.status,409);
  app.mutateStore(store => { delete store.bankReconciliationDrafts['BRD-GROUP']; });
  visible = app.invoke('GET','/api/expenses/classification-review',{query:{entities:'SANKI',category:'Props Expense',search:'Bed sheet'}});
  assert.deepEqual(visible.body.expenses.map(row => row.recordId),['EX-100']);
});

test('active reimbursement and gross vendor-payment batches block every linked expense until finalization', () => {
  const reimbursement = fixture();
  reimbursement.mutateStore(store => {
    store.expenses['EX-DRAFT'].reimbursementPayments = [{id:'REIM-001',batchId:'RB-ACTIVE',amount:300,date:'2026-11-04',account:'Axis Bank 3448'}];
    store.bankReconciliationDrafts['BRD-REIM'] = {id:'BRD-REIM',resolutions:{'bank-0':{action:'link_existing',appId:'RB-ACTIVE'}}};
  });
  assert.equal(classificationReview.reviewableExpense(reimbursement.store.expenses['EX-DRAFT'],'expense',reimbursement.store),false,'reimbursement aggregate owns the pending bank movement');
  const reimbursementDenied = reimbursement.invoke('PATCH','/api/expenses/classification-review/:id',{
    pattern:'/api/expenses/classification-review/:id',params:{id:'EX-DRAFT'},
    body:{sourceType:'expense',...reimbursement.token('EX-DRAFT'),vendor:'Unsafe Vendor',reason:'Attempt during reimbursement reconciliation'}
  });
  assert.equal(reimbursementDenied.status,409);
  reimbursement.mutateStore(store => { delete store.bankReconciliationDrafts['BRD-REIM']; });
  assert.equal(classificationReview.reviewableExpense(reimbursement.store.expenses['EX-DRAFT'],'expense',reimbursement.store),true);

  const vendorBatch = fixture();
  vendorBatch.mutateStore(store => {
    store.expenses['EX-DRAFT'].payments = [{id:'PAY-BATCH',batchPaymentId:'BPAY-ACTIVE',amount:300,date:'2026-11-04',account:'Axis Bank 3448'}];
    store.vendorAdvances = [{id:'ADV-BATCH',batchPaymentId:'BPAY-ACTIVE',paymentReference:'EX-DRAFT/PAY-BATCH',grossPaymentAmount:500,amount:200}];
    store.bankReconciliationDrafts['BRD-VENDOR'] = {id:'BRD-VENDOR',resolutions:{'bank-0':{action:'link_existing',appId:'EX-DRAFT/PAY-BATCH'}}};
  });
  assert.equal(classificationReview.reviewableExpense(vendorBatch.store.expenses['EX-DRAFT'],'expense',vendorBatch.store),false,'vendor batch maps through its bank movement paymentReference');
  vendorBatch.mutateStore(store => {
    store.bankReconciliationDrafts['BRD-VENDOR'].resolutions['bank-0'].appId = 'BPAY-ACTIVE';
  });
  assert.equal(classificationReview.reviewableExpense(vendorBatch.store.expenses['EX-DRAFT'],'expense',vendorBatch.store),false,'raw batch id is also guarded defensively');
  vendorBatch.mutateStore(store => { delete store.bankReconciliationDrafts['BRD-VENDOR']; });
  assert.equal(classificationReview.reviewableExpense(vendorBatch.store.expenses['EX-DRAFT'],'expense',vendorBatch.store),true);
});
