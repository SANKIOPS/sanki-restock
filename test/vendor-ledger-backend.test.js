'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-vendor-ledger-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
process.env.PROCUREMENT_PATH = path.join(temp, 'procurement.json');
process.env.ORDERS_PATH = path.join(temp, 'orders.json');
process.env.SALES_PATH = path.join(temp, 'sales.json');
const filename = path.join(temp, 'expenses.json');
fs.writeFileSync(filename, '{}');
const { router, mergeVendorRecords } = require('../modules/expenses');
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
function invoke(method, routePath, { body = {}, query = {}, params = {}, role = 'owner' } = {}) {
  const layer = router.stack.find(item => item.route && item.route.path === routePath && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'route exists: ' + routePath);
  let status = 200, result;
  const req = { body, query, params, ip: '203.0.113.10', headers: { 'user-agent': 'Isolated vendor regression' }, get(name) { return this.headers[String(name).toLowerCase()] || ''; }, user: { username: role + '-vendor-test', role, roles: [role] } };
  layer.route.stack[0].handle(req, { status(code) { status = code; return this; }, json(value) { result = value; return this; } }, () => { throw new Error('Unexpected next'); });
  return { status, body: result };
}
invoke('GET', '/api/expenses/config');
const baseline = JSON.parse(fs.readFileSync(filename, 'utf8'));
function seed(extra = {}) {
  const store = structuredClone(baseline);
  Object.assign(store, { expenses: {}, vendors: {}, vendorsByNature: { SAMAST: {}, PERSONAL: {} }, vendorDirectory: {}, vendorOpeningPayables: [], vendorAdvances: [], expenseRefunds: [], reconciliationExpenses: [], bankReconciliationDrafts: {}, bankDateOverrides: {}, bankStatements: {}, bankReconciliationLinks: {}, cashReconciliations: [], auditLog: [], auditSeq: 0 }, extra);
  fs.writeFileSync(filename, JSON.stringify(store));
  return store;
}
function expense(id, vendor, nature = 'SANKI') {
  return { id, date: '2097-02-01', nature, vendor, particulars: 'Test purchase', ledger: 'Flowers', amount: 100, paidAmount: 100, status: 'paid', approvedAt: '2097-02-01T10:00:00Z', billPhoto: '/api/expenses/photo/bill.jpg', payments: [{ id: 'PAY-001', date: '2097-02-02', amount: 100, account: 'Counter Cash', transactionReference: 'BANK-REF-ONLY', proof: '/api/expenses/photo/exact-payment.jpg', proofs: ['/api/expenses/photo/exact-payment.jpg', '/api/expenses/photo/second-payment.jpg'] }] };
}

test('ledger rows expose exact regular expense/payment identity and payment-specific proofs', () => {
  seed({ expenses: { 'EX-REGULAR': expense('EX-REGULAR', 'UJ') }, vendors: { uj: { name: 'UJ' } } });
  const out = invoke('GET', '/api/expenses/vendors', { query: { nature: 'SANKI', search: 'UJ' } });
  assert.equal(out.status, 200);
  const rows = out.body.vendors[0].ledgerRows, bill = rows.find(row => row.kind === 'expense'), payment = rows.find(row => row.kind === 'payment');
  assert.equal(bill.expenseId, 'EX-REGULAR'); assert.equal(bill.editableExpense, true); assert.equal(bill.source, 'expense');
  assert.equal(payment.expenseId, 'EX-REGULAR'); assert.equal(payment.paymentId, 'PAY-001'); assert.equal(payment.reference, 'BANK-REF-ONLY');
  assert.deepEqual(payment.proofs, ['/api/expenses/photo/exact-payment.jpg', '/api/expenses/photo/second-payment.jpg']);
  assert.equal(payment.paymentProofAvailable, true);
});

test('opening, advance and credit-card statement rows retain provenance and cannot edit a regular expense', () => {
  seed({ vendors: { uj: { name: 'UJ' } }, vendorOpeningPayables: [{ ...expense('VOP-1', 'UJ'), source: 'opening_vendor_payable' }], vendorAdvances: [{ id: 'VADV-1', nature: 'SANKI', vendor: 'UJ', date: '2097-02-03', amount: 30, remainingAmount: 30, proof: '/api/expenses/photo/advance-only.jpg' }], reconciliationExpenses: [{ id: 'CCE-ST-ROW', nature: 'SANKI', vendor: 'UJ', date: '2097-02-01', amount: 40, category: 'Flowers', account: 'Card 1234', creditCardStatementId: 'ST-1', creditCardRowId: 'ROW-1' }] });
  const rows = invoke('GET', '/api/expenses/vendors', { query: { nature: 'SANKI', search: 'UJ' } }).body.vendors[0].ledgerRows;
  assert.ok(rows.every(row => !row.expenseId && row.editableExpense === false));
  assert.equal(rows.find(row => row.entryId === 'CCE-ST-ROW').source, 'credit_card_statement');
  assert.equal(rows.find(row => row.entryId === 'VOP-1').source, 'opening_vendor_payable');
  const advance = rows.find(row => row.entryId === 'VADV-1');
  assert.deepEqual(advance.proofs, ['/api/expenses/photo/advance-only.jpg']); assert.equal(advance.paymentProofAvailable, true);
});

test('vendor API default search puts UJ first while private proofs remain unavailable to Admin', () => {
  const privateExpense = expense('EX-PRIVATE', 'Private UJ', 'PERSONAL'); privateExpense.createdBy = 'someone-else';
  seed({ expenses: { 'EX-RELATED': { ...expense('EX-RELATED', 'Bikanervala'), particulars: 'UJ bill' }, 'EX-UJ': expense('EX-UJ', 'UJ'), 'EX-PRIVATE': privateExpense }, vendors: { uj: { name: 'UJ' }, bikanervala: { name: 'Bikanervala' } }, vendorsByNature: { PERSONAL: { 'private uj': { name: 'Private UJ' } } } });
  const owner = invoke('GET', '/api/expenses/vendors', { query: { search: 'UJ' } }).body.vendors;
  assert.equal(owner[0].name, 'UJ'); assert.ok(owner.some(row => row.name === 'Bikanervala'));
  const admin = invoke('GET', '/api/expenses/vendors', { role: 'admin', query: { search: 'UJ' } }).body.vendors;
  assert.equal(admin[0].name, 'UJ'); assert.equal(admin.some(row => row.name === 'Private UJ'), false);
});

test('global rename and merge remain Owner-only and retain reconciled transaction facts', () => {
  const source = expense('EX-SOURCE', 'Old Vendor'), samast = expense('EX-SAMAST', 'Old Vendor', 'SAMAST'), target = expense('EX-TARGET', 'Final Vendor');
  seed({ expenses: { [source.id]: source, [samast.id]: samast, [target.id]: target }, vendors: { 'old vendor': { name: 'Old Vendor' }, 'final vendor': { name: 'Final Vendor' } }, vendorsByNature: { SAMAST: { 'old vendor': { name: 'Old Vendor' } } }, bankDateOverrides: { 'EX-SOURCE/PAY-001': { bankDate: '2097-02-02', bankReference: 'PRESERVE' } } });
  const facts = row => ({ amount: row.amount, date: row.date, payments: row.payments });
  for (const action of ['edit', 'merge']) assert.equal(invoke('POST', '/api/expenses/vendors/manage/' + action, { role: 'admin', body: { name: 'Old Vendor', newName: 'Renamed Vendor', sourceName: 'Old Vendor', targetName: 'Final Vendor' } }).status, 403);
  const renamed = invoke('POST', '/api/expenses/vendors/manage/edit', { body: { name: 'Old Vendor', newName: 'Renamed Vendor', reason: 'Correct the approved vendor display name' } });
  assert.equal(renamed.status, 200); assert.equal(renamed.body.updatedExpenses, 2);
  const merged = invoke('POST', '/api/expenses/vendors/manage/merge', { body: { sourceName: 'Renamed Vendor', targetName: 'Final Vendor', reason: 'These are the same supplier' } });
  assert.equal(merged.status, 200); assert.equal(merged.body.updatedExpenses, 2);
  const after = JSON.parse(fs.readFileSync(filename, 'utf8'));
  for (const original of [source, samast, target]) { assert.equal(after.expenses[original.id].vendor, 'Final Vendor'); assert.deepEqual(facts(after.expenses[original.id]), facts(original)); }
  assert.equal(after.bankDateOverrides['EX-SOURCE/PAY-001'].bankReference, 'PRESERVE');
  assert.match(after.auditLog.find(row => row.action === 'VENDOR_RENAMED').note, /Correct the approved vendor display name/);
  assert.match(after.auditLog.find(row => row.action === 'VENDOR_MERGED').note, /same supplier/);
});

test('same-identity merge is rejected without removing the existing vendor', () => {
  seed({ vendors: { uj: { name: 'UJ' } } });
  for (const targetName of ['uj', ' U\u200bJ ']) {
    const out = invoke('POST', '/api/expenses/vendors/manage/merge', { body: { sourceName: 'UJ', targetName } });
    assert.equal(out.status, 400); assert.match(out.body.error, /different vendor/);
  }
  assert.equal(JSON.parse(fs.readFileSync(filename, 'utf8')).vendors.uj.name, 'UJ');
});

test('derived card-merchant rename or merge fails closed before any vendor transaction changes', () => {
  const source = expense('EX-CARD-SAME', 'UJ');
  seed({ expenses: { [source.id]: source }, vendors: { uj: { name: 'UJ' }, other: { name: 'Other' } }, reconciliationExpenses: [{ id: 'CCE-ST-ROW', nature: 'SANKI', vendor: 'UJ', date: '2097-02-01', amount: 40, category: 'Flowers', account: 'Card 1234', creditCardStatementId: 'ST-1', creditCardRowId: 'ROW-1' }] });
  for (const action of ['edit', 'merge']) {
    const out = invoke('POST', '/api/expenses/vendors/manage/' + action, { body: { name: 'UJ', newName: 'Changed', sourceName: 'UJ', targetName: 'Other' } });
    assert.equal(out.status, 409); assert.match(out.body.error, /credit-card workflow/);
    const after = JSON.parse(fs.readFileSync(filename, 'utf8'));
    assert.equal(after.expenses[source.id].vendor, 'UJ'); assert.equal(after.reconciliationExpenses[0].vendor, 'UJ');
    assert.equal(after.auditLog.some(row => ['VENDOR_RENAMED', 'VENDOR_MERGED'].includes(row.action)), false);
  }
});

test('merge follows exact normalized identity across hidden spacing, refund issuers and master metadata', () => {
  const sourceName = 'Old\u200b Vendor', original = expense('EX-NORMALIZED', sourceName);
  const store = { expenses: { [original.id]: structuredClone(original) }, vendors: { 'raw-hidden-key': { name: sourceName, notes: 'Source note', tags: ['Source tag'] }, 'final vendor': { name: 'Final Vendor', notes: 'Target note', tags: ['Target tag'] } }, vendorOpeningPayables: [{ id: 'VOP-1', nature: 'SANKI', vendor: sourceName, amount: 30, paidAmount: 0 }], vendorAdvances: [{ id: 'VA-1', nature: 'SANKI', vendor: sourceName, issuer: sourceName, creditOnly: true, amount: 12, remainingAmount: 12 }], expenseRefunds: [{ id: 'ER-1', nature: 'SANKI', vendor: sourceName, status: 'voided', sources: [], components: [{ issuer: sourceName }] }], bankReconciliationDrafts: { D: { id: 'D', nature: 'SANKI', resolutions: { 'bank-1': { vendor: sourceName } } } }, auditLog: [] };
  const out = mergeVendorRecords(store, 'SANKI', ['Old Vendor'], 'Final Vendor', { skipAudit: true });
  assert.equal(out.updatedExpenses, 1); assert.equal(out.before.outstanding, out.after.outstanding);
  assert.equal(store.expenses[original.id].vendor, 'Final Vendor');
  assert.deepEqual(store.expenses[original.id].payments, original.payments);
  assert.equal(store.vendorOpeningPayables[0].vendor, 'Final Vendor');
  assert.equal(store.vendorAdvances[0].issuer, 'Final Vendor'); assert.equal(store.vendorAdvances[0].vendor, 'Final Vendor');
  assert.equal(store.expenseRefunds[0].vendor, 'Final Vendor'); assert.equal(store.expenseRefunds[0].components[0].issuer, 'Final Vendor');
  assert.equal(store.bankReconciliationDrafts.D.resolutions['bank-1'].vendor, 'Final Vendor');
  assert.deepEqual(Object.keys(store.vendors), ['final vendor']);
  assert.deepEqual(store.vendors['final vendor'].tags, ['Source tag', 'Target tag']);
  assert.match(store.vendors['final vendor'].notes, /Source note/); assert.match(store.vendors['final vendor'].notes, /Target note/);
});

test('persisted salary/reimbursement synthetic rows share the detail endpoint read-only permission', () => {
  const generated = { ...expense('EX-SALARY-GENERATED', 'Employee advance'), salaryAdvanceId: 'ADV-SOURCE', nonExpenseReimbursement: true };
  seed({ expenses: { [generated.id]: generated }, vendors: { 'employee advance': { name: 'Employee advance' } } });
  const ledger = invoke('GET', '/api/expenses/vendors', { query: { nature: 'SANKI', search: 'Employee advance' } }).body.vendors[0];
  assert.ok(ledger.ledgerRows.every(row => row.editableExpense === false));
  const detail = invoke('GET', '/api/expenses/:id', { params: { id: generated.id } });
  assert.equal(detail.body.expense.canEditMetadata, false);
});

test('a reassigned gross-payment bill renders its stored allocation at the new vendor without moving the advance or bank gross', () => {
  const bill = expense('EX-CROSS-VENDOR', 'Vendor A'), batchId = 'BPAY-CROSS-VENDOR';
  Object.assign(bill.payments[0], { batchPaymentId: batchId, batchTotal: 300, linkedExpenseIds: [bill.id] });
  const advance = { id: 'VADV-CROSS-VENDOR', nature: 'SANKI', vendor: 'Vendor A', date: '2097-02-02', account: 'Counter Cash', amount: 200, remainingAmount: 200, grossPaymentAmount: 300, allocatedExpenseAmount: 100, batchPaymentId: batchId, bankReference: 'BANK-GROSS-300', proof: '/api/expenses/photo/gross-payment.jpg', applications: [] };
  seed({ expenses: { [bill.id]: bill }, vendors: { 'vendor a': { name: 'Vendor A' }, 'vendor b': { name: 'Vendor B' } }, vendorAdvances: [advance] });
  const query = { nature: 'SANKI', search: 'Vendor ' }, before = invoke('GET', '/api/expenses/vendors', { query }).body.vendors;
  const beforeA = before.find(row => row.name === 'Vendor A');
  assert.deepEqual(beforeA.ledgerRows.map(row => [row.type, row.in, row.out]), [['Expense', 0, 100], ['Payment', 300, 0]], 'one gross row remains while every allocation uses one vendor');
  const financialSources = JSON.parse(fs.readFileSync(filename, 'utf8'));
  const edited = invoke('POST', '/api/expenses/:id', { role: 'admin', params: { id: bill.id }, body: { vendor: 'Vendor B', editReason: 'Correct the vendor on the already-paid bill' } });
  assert.equal(edited.status, 200, JSON.stringify(edited.body));
  const after = invoke('GET', '/api/expenses/vendors', { query }).body.vendors, a = after.find(row => row.name === 'Vendor A'), b = after.find(row => row.name === 'Vendor B');
  assert.deepEqual(a.ledgerRows.map(row => [row.type, row.in, row.out]), [['Vendor advance', 200, 0]]);
  assert.deepEqual(b.ledgerRows.map(row => [row.type, row.in, row.out]), [['Expense', 0, 100], ['Payment', 100, 0]]);
  assert.equal(a.ledgerClosingBalance, -200); assert.equal(b.ledgerClosingBalance, 0);
  assert.equal(after.reduce((sum, row) => sum + row.ledgerRows.reduce((total, item) => total + item.in, 0), 0), 300);
  const stored = JSON.parse(fs.readFileSync(filename, 'utf8'));
  assert.deepEqual(stored.vendorAdvances, financialSources.vendorAdvances, 'the advance and original gross bank movement never move');
  assert.deepEqual(stored.expenses[bill.id].payments, financialSources.expenses[bill.id].payments, 'the stored allocation, proof and batch references remain unchanged');
  const bank = invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account: 'Counter Cash', from: '2097-02-01', to: '2097-02-03' } }).body.entries;
  const bankGross = bank.filter(row => row.reference === advance.bankReference);
  assert.equal(bankGross.length, 1, JSON.stringify(bank));
  assert.equal(bankGross[0].debit, 300);
  assert.equal(bank.filter(row => row.debit === 100).length, 0, 'a separate bank debit must not be invented for the displayed vendor allocation');
});
