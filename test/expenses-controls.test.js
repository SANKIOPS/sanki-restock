'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-expenses-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const { router, summaryForPL, createTelegramPersonalExpense, createTelegramPersonalReceipt, createTelegramBusinessPaidExpense, telegramBusinessCategories, telegramExpense, telegramApproveExpense, telegramRecordPayment, telegramRecordTransfer, telegramRecordNamitaTransfer, telegramApi, parseBankStatementFile, parseBankStatementText } = require('../modules/expenses');
const XLSX = require('xlsx');

test.after(() => {
  fs.rmSync(tempDir, { recursive: true, force: true });
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
  stored.expenses[id].billPhoto = '';
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
  assert.match(html, /paymentProof=e\.paymentProof/);
  assert.match(html, /alt="payment proof"/);
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
  assert.match(html,/Select all for /);
  assert.match(html,/class="claim-amount">'\+fmt\(group\.due\)\+' due/);
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

test('All Expenses exposes a clear same-vendor consolidated payment selector', () => {
  const html=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(html,/class="combined-pay-select"/);
  assert.match(html,/Select to combine/);
  assert.match(html,/Pay selected together/);
  assert.match(html,/openPay\(ids\[0\],ids\)/);
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
  assert.match(html, /Source \/ destination/);
  assert.match(html, /data-cfdays="7"/);
  assert.match(html, /Reconciliation issue/);
  assert.match(html, /id="payOverrideReason"/);
  assert.match(html, /id="editPersonalAccount"/);
  assert.match(html, /id="editBillFile"/);
  assert.match(html, /Bank statement reconciliation/);
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
  assert.match(html,/PERSONAL bank reconciliation is visible only to Owner/);
  assert.match(html,/bank-statements\?nature='\+encodeURIComponent\(bankNature\)/);
  assert.match(html,/bankDraftId='';el\('bs_msg'\)\.textContent='Reconciliation finalized through/);
  assert.match(html,/Reconciliation history/);
  assert.match(html,/bs_history/);
  assert.match(html,/Decision and actual reason/);
  assert.match(html,/Open original statement/);
  assert.match(html,/Export CSV/);
  assert.match(html,/Create &amp; assign ledger entry/);
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
});

test('new accounting UI defaults to current month, uses compact rows and opens proofs in-page', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'expenses.html'), 'utf8');
  assert.match(html, /function monthStart\(\)/);
  assert.match(html, /id="sd_from" value="'\+monthStart\(\)/);
  assert.match(html, /id="vf_from" value="'\+monthStart\(\)/);
  assert.match(html, /id="cf_from" value="'\+monthStart\(\)/);
  assert.match(html, /id="rf_from" value="'\+monthStart\(\)/);
  assert.match(html, /id="rf_nature"><option value="">All<\/option>/);
  assert.match(html, /id="rf_status"><option value="pending" selected>Pending<\/option>/);
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
  assert.equal(clearing.some(x=>x.id==='SHOPIFY/paytm'&&x.credit===50000),true);
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
  const repairedPaytm=invoke('GET','/api/expenses/spending-dashboard',{role:'owner',query:{from:'2026-08-22',to:'2026-08-22',nature:'SANKI',category:'BANK CHARGES'}}).body.payments.find(x=>x.vendor==='Paytm');
  assert.equal(repairedPaytm.amount,540);
  const persisted=JSON.parse(fs.readFileSync(expenseStorePath,'utf8')).paytmSettlements.find(x=>x.id==='PTM-LEGACY-NOTE');
  assert.equal(persisted.chargeAmount,539.64);
  assert.equal(persisted.grossAmount,15295);
  const source=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(source,/Connected sales:/);
  assert.match(source,/Bank\/Paytm charges/);
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
