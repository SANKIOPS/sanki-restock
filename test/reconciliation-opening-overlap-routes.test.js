'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Every dependency resolves its data beside DATA_PATH. Never read or modify a
// production ledger while exercising the real router and finalization hooks.
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-opening-overlap-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const expenseFile = path.join(tempDir, 'expenses.json');
fs.writeFileSync(expenseFile, '{}');
const { router } = require('../modules/expenses');

const account = 'Axis Bank 3448';
const date = '2026-10-18';
const draftId = 'BRD-OCT18-OPENING-OVERLAP';
const paytmId = 'PTM-OCT18-FINALIZED';
const previousDraftId = 'BRD-OCT18-PAYTM-ONLY';
const previousImportId = 'BST-OCT18-PAYTM-ONLY';
const baseOpening = 47844.86;
const periodOpening = 234515.82;
const closing = 165673.07;
let baseline;

const read = () => JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
const write = value => fs.writeFileSync(expenseFile, JSON.stringify(value));
const clone = value => JSON.parse(JSON.stringify(value));
const money = value => Math.round(value * 100) / 100;

function invoke(method, route, { body = {}, query = {} } = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, complete = false;
    const timer = setTimeout(() => reject(new Error('Route did not complete: ' + route)), 10000);
    const finish = value => {
      if (complete) return;
      complete = true;
      clearTimeout(timer);
      resolve({ status, body: value });
    };
    const req = { method, path: route, url: route, originalUrl: route, baseUrl: '', body, query,
      headers: {}, get: () => '', ip: '127.0.0.1',
      user: { username: 'owner-opening-regression', role: 'owner', roles: ['owner'] } };
    const res = { statusCode: 200, headersSent: false,
      status(value) { status = value; this.statusCode = value; return this; },
      setHeader() {}, getHeader() {}, removeHeader() {},
      json(value) { this.headersSent = true; finish(value); return this; },
      send(value) { this.headersSent = true; finish(value); return this; },
      end: finish };
    try {
      router.handle(req, res, error => {
        if (error) { clearTimeout(timer); reject(error); }
        else finish(undefined);
      });
    } catch (error) { clearTimeout(timer); reject(error); }
  });
}

test.before(async () => {
  const result = await invoke('GET', '/api/expenses/config');
  assert.equal(result.status, 200);
  baseline = read();
});
test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

function seed({ cutoff = date, bankGap = 0, staged = true } = {}) {
  const transactions = [
    { date, reference: '629100000001', description: 'NEFT/PAYTM PAYMENTS SERVICES',
      debit: 0, credit: 13694, balance: money(248209.82 + bankGap) },
    { date, reference: '629100000002', description: 'IMPS/P2A/629100000002/INDUSIND BANK/X681883',
      debit: 10449.90, credit: 0, balance: money(237759.92 + bankGap) },
    { date, reference: '629100000003', description: 'IMPS/P2A/629100000003/INDUSIND BANK/X687883',
      debit: 71123.90, credit: 0, balance: money(166636.02 + bankGap) },
    { date, reference: '629100000004', description: 'IMPS/P2A/629100000004/ARSHPREET SINGH/X681919',
      debit: 962.95, credit: 0, balance: money(closing + bankGap) }
  ];
  const resolutions = staged ? {
    // Retain the already-reviewed legacy counterpart decision exactly as a
    // saved draft does; this regression must not recategorize its transfers.
    'bank-2': { action: 'create_internal_transfer', otherAccount: 'IndusInd Bank 7883',
      reason: 'Reviewed business transfer to IndusInd Bank 7883' },
    'bank-3': { action: 'create_internal_transfer', otherAccount: 'Arshpreet 1919',
      reason: 'Reviewed transfer to Arshpreet 1919' }
  } : {};
  const stored = Object.assign(clone(baseline), {
    expenses: { 'EX-PRIOR-SUPPLIER': { id: 'EX-PRIOR-SUPPLIER', date: '2026-10-17', nature: 'SANKI',
      status: 'paid', approvedAt: '2026-10-17T08:00:00Z', vendor: 'Prior period supplier',
      amount: 13329.04, paidAmount: 13329.04,
      payments: [{ id: 'PAY-001', date: '2026-10-17', amount: 13329.04, account,
        bankReference: '629000000002', proof: '/fixture-prior-payment.jpg' }] } },
    receipts: [{ id: 'REC-PRIOR-SALES', nature: 'SANKI', account, date: '2026-10-01', amount: 200000,
      receiptType: 'other_income', category: 'Other Income', source: 'Prior sales receipts', bankReference: '627400000001' }],
    transfers: [{ id: 'TR-00181', nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI',
      fromAccount: account, toAccount: 'IndusInd Bank 8181', amount: 10444, date,
      classification: 'internal_transfer', bankReference: transactions[1].reference,
      proof: '/fixture-transfer.jpg', createdAt: date + 'T09:00:00Z' }],
    paytmSettlements: [{ id: paytmId, date, bankAccount: account, netAmount: 13694, grossAmount: 13694,
      chargeAmount: 0, bankReference: transactions[0].reference, reconciliationDraft: previousDraftId,
      createdAt: date + 'T08:00:00Z' }],
    adjustments: [], bankTruthMovements: [], vendorAdvances: [], vendorOpeningPayables: [],
    reconciliationExpenses: [], receivables: {}, loanRepayments: [], salesRefunds: [], restrictedFunds: [],
    paytmPayoutPostings: [], paytmVerifiedSettlements: [], paytmReportTransactions: {}, paytmOrderLinks: {},
    bankDateOverrides: {}, bankReconciliationLinks: {}, bankReconciliationApprovals: {},
    openingBalances: { [account]: baseOpening }, openingBalanceDates: {}, transferSeq: 181, adjSeq: 0,
    bankStatements: cutoff ? { [account]: {
      reconciledThrough: cutoff,
      transactions: { 'paytm-finalized': { ...transactions[0], id: 'BTX-PAYTM-OCT18',
        firstSeenImport: previousImportId, lastSeenImport: previousImportId,
        reconciliationDraftId: previousDraftId, reconciliationDraftRowIndex: 0 } },
      imports: [{ id: previousImportId, draftId: previousDraftId, from: date, to: date,
        finalizedAt: date + 'T08:30:00Z', rows: 1,
        statementSummary: { from: date, to: date, openingBalance: periodOpening + bankGap,
          closingBalance: 248209.82 + bankGap, totalDebits: 0, totalCredits: 13694, validated: true },
        reconciliationRows: [{ id: 'bank-0', bank: { ...transactions[0], transactionId: 'BTX-PAYTM-OCT18' },
          linkedRecordIds: [paytmId], ledger: { id: paytmId, date, debit: 0, credit: 13694 } }] }],
      lastReconciliation: { through: cutoff, closingBalance: 248209.82 + bankGap,
        draftId: previousDraftId, balanceReconciled: true, transactionsReconciled: true }
    } } : {},
    bankReconciliationDrafts: { [draftId]: { id: draftId, nature: 'SANKI', account, transactions,
      summary: { from: date, to: date, openingBalance: periodOpening + bankGap,
        closingBalance: closing + bankGap, totalDebits: 82536.75, totalCredits: 13694, validated: true },
      resolutions, matchingPolicy: 'balanced_date_amount_v5', temporaryFile: '',
      createdAt: date + 'T12:00:00Z', createdBy: 'owner-opening-regression' } }
  });
  write(stored);
  return stored;
}

async function preview(selectedDraftId = draftId) {
  const result = await invoke('GET', '/api/expenses/bank-statements', {
    query: { nature: 'SANKI', account, draftId: selectedDraftId }
  });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.ok(result.body.draft);
  return result.body.draft;
}

function financialSnapshot(store) {
  const expenses = Object.values(store.expenses).map(expense => ({ id: expense.id, nature: expense.nature,
    date: expense.date, status: expense.status, amount: expense.amount, paidAmount: expense.paidAmount,
    payments: expense.payments.map(payment => ({ id: payment.id, date: payment.date, amount: payment.amount,
      account: payment.account, bankReference: payment.bankReference })) }));
  return JSON.stringify({ openingBalances: store.openingBalances, expenses,
    receipts: store.receipts, transfers: store.transfers, adjustments: store.adjustments,
    paytmSettlements: store.paytmSettlements, reconciliationExpenses: store.reconciliationExpenses,
    bankTruthMovements: store.bankTruthMovements, vendorAdvances: store.vendorAdvances,
    vendorOpeningPayables: store.vendorOpeningPayables });
}

function assertBalancedPeriod(view) {
  assert.equal(view.currentOpening, periodOpening, 'account opening plus pre-period ledger net 186670.96');
  assert.equal(view.postedPeriodNet, 3250, 'posted Paytm credit less the existing transfer principal');
  assert.equal(view.pendingPostingNet, -72092.75, 'two pending transfers plus the separate 5.90 fee');
  assert.equal(view.ledgerClosing, closing, 'include the finalized Paytm credit and each staged debit once');
  assert.notEqual(view.ledgerClosing, -20997.89, 'the account lifetime opening is not this statement opening');
  assert.equal(view.bankClosing, closing);
  assert.equal(view.balanceDifference, 0);
  assert.equal(view.balanceResolved, true);
  assert.equal(view.canFinalize, true);
  assert.equal(view.unresolved, 0);
  assert.deepEqual(view.reconciliationConflicts, []);
  const transfer = view.rows.find(row => row.id === 'bank-1');
  assert.equal(transfer.status, 'matched');
  assert.equal(transfer.app.id, 'TR-00181');
  assert.equal(transfer.app.debit, 10444);
  assert.equal(transfer.axisTransferCharge, true);
  assert.equal(transfer.chargeAmount, 5.9);
}

test('same-cutoff partial reconciliation derives Oct18 opening from prior ledger activity', async () => {
  seed();
  const before = financialSnapshot(read());
  const first = await preview();
  assertBalancedPeriod(first);
  assert.equal(first.rows.find(row => row.id === 'bank-0').status, 'already_reconciled');
  assert.equal(first.rows.filter(row => row.app && row.app.id === paytmId).length, 0,
    'prior Paytm credit is accounted for without reopening its matching decision');
  const second = await preview();
  assert.deepEqual(second, first, 'repeated preview is deterministic');
  assert.equal(financialSnapshot(read()), before, 'preview neither posts money nor changes account opening');
  assert.equal(read().bankStatements[account].lastReconciliation.closingBalance, 248209.82);
  assert.equal(read().bankReconciliationDrafts[draftId].openingResolution, undefined);
});

// Reproduce the live topology, not a lifetime ledger invented to equal the
// statement: an Oct15 verified closing, Oct16/17 activity subsequently covered
// by a partial Oct18 finalization, and unreconciled Oct18 transfer decisions.
function seedEarlierAnchor() {
  const stored = seed();
  delete stored.expenses['EX-PRIOR-SUPPLIER'];
  stored.receipts[0].amount = 550000;
  const book = stored.bankStatements[account];
  const anchor = { id: 'BST-OCT15', draftId: 'BRD-OCT15', from: '2026-10-15', to: '2026-10-15',
    balanceReconciled: true, balanceDifference: 0, closingBalanceDeferred: false,
    statementSummary: { from: '2026-10-15', to: '2026-10-15', closingBalance: 264515.82 },
    ledgerClosingBalance: 264515.82, reconciliationRows: [] };
  book.imports.push(anchor);
  const later = book.imports[0];
  const movement = (id, day, debit, credit = 0, fee = false) => {
    const d = '2026-10-' + day;
    if (fee) stored.adjustments.push({ id, date: d, account, nature: 'SANKI', amount: -debit,
      automaticAxisTransferCharge: id === 'ADJ-GAP-AUTO', reconciliationDraft: previousDraftId,
      note: 'Verified transfer fee' });
    else if (credit) stored.receipts.push({ id, date: d, account, nature: 'SANKI', amount: credit, source: 'Verified prior receipt' });
    else stored.expenses[id] = { id, date: d, nature: 'SANKI', status: 'paid', approvedAt: d,
      vendor: 'Verified prior payment', amount: debit, paidAmount: debit,
      payments: [{ id: 'PAY-001', date: d, amount: debit, account, proof: '/fixture.jpg' }] };
    const appId = fee || credit ? id : id + '/PAY-001';
    later.reconciliationRows.push({ id: 'covered-' + id, linkedRecordIds: [appId],
      ledger: { id: appId, date: d, debit, credit } });
  };
  // Synthetic bridge amounts preserve the accounting shape without publishing
  // a customer's historical statement transactions.
  movement('FIXTURE-LOAN-PAYMENT', '16', 50000);
  movement('FIXTURE-CONTRIBUTION', '16', 0, 50000);
  movement('PRIOR-TRANSFER-A', '16', 1200);
  movement('ADJ-GAP-A', '16', 5.9, 0, true);
  movement('PRIOR-PAYTM', '17', 0, 7423.6);
  movement('EX-BNA', '17', 200);
  movement('EX-GST', '17', 100);
  movement('PRIOR-ADVANCE', '17', 2900);
  movement('ADJ-GAP-AUTO', '17', 5.9, 0, true);
  movement('PRIOR-TRANSFER-B', '17', 23000);
  movement('ADJ-GAP-B', '17', 5.9, 0, true);
  movement('PRIOR-TRANSFER-C', '17', 10000);
  movement('ADJ-GAP-C', '17', 5.9, 0, true);
  const draft = stored.bankReconciliationDrafts[draftId];
  Object.assign(draft.resolutions['bank-2'], { principalAmount: 71118, chargeAmount: 5.9,
    postingNature: 'SANKI', otherNature: 'PERSONAL', classification: 'owner_withdrawal', chargeCategory: 'BANK CHARGES' });
  Object.assign(draft.resolutions['bank-3'], { principalAmount: 960, chargeAmount: 2.95, chargeCategory: 'BANK CHARGES' });
  write(stored);
  return stored;
}

test('Axis Oct18 overlap uses Oct15 verified closing and includes later finalized principal and fees once', async () => {
  seedEarlierAnchor();
  const before = financialSnapshot(read());
  const view = await preview();
  assertBalancedPeriod(view);
  assert.equal(view.openingSource, 'previous_reconciliation');
  assert.equal(view.openingBaselineDate, '2026-10-15');
  assert.equal(view.previousClosing, 264515.82);
  assert.equal(view.carryForwardNet, -30000);
  assert.equal(view.carryForwardMovements.filter(row => row.id === 'ADJ-GAP-AUTO').length, 1);
  assert.equal(view.historicalOpeningNet, 0, 'legacy activity before the verified anchor is already represented');
  assert.equal(view.openingAnchorRecordId, 'BST-OCT15');
  assert.deepEqual(await preview(), view);
  assert.equal(financialSnapshot(read()), before);
});

test('earlier-anchor overlap finalization keeps reviewed principal/fee splits and posts each exactly once', async () => {
  seedEarlierAnchor();
  const before = read();
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  assert.equal(finalized.body.balanceDifference, 0);
  const after = read();
  assert.equal(after.openingBalances[account], baseOpening);
  assert.deepEqual(JSON.parse(financialSnapshot(after)).expenses, JSON.parse(financialSnapshot(before)).expenses);
  assert.deepEqual(after.receipts, before.receipts);
  assert.deepEqual(after.transfers.filter(row => row.reconciliationDraft === draftId).map(row => row.amount).sort((a,b)=>a-b), [960,71118]);
  const fees = after.adjustments.filter(row => row.reconciliationDraft === draftId);
  assert.deepEqual(fees.map(row=>row.amount).sort((a,b)=>a-b), [-5.9,-5.9,-2.95]);
  assert.equal(after.transfers.filter(row=>row.id==='TR-00181').length,1);
  assert.equal(after.transfers.find(row=>row.id==='TR-00181').amount,10444);
  assert.equal(after.paytmSettlements.length,1);
  const completeImport = after.bankStatements[account].imports.find(row=>row.draftId===draftId);
  assert.ok(completeImport.carryForwardMovements.every(row=>row.bankEvidencePending===false));
  assert.equal(completeImport.reconciliationBankCoverageComplete,true);
  assert.equal(completeImport.reconciliationBankTransactionIds.length,4);
  assert.ok(completeImport.reconciliationBankTransactionIds.includes('BTX-PAYTM-OCT18'));
  const saved = financialSnapshot(after);
  assert.equal((await invoke('POST','/api/expenses/bank-statements/finalize',{body:{draftId}})).status,404);
  assert.equal(financialSnapshot(read()),saved);
});

test('an earlier anchor never conceals an actual missing movement or creates a balancing adjustment', async () => {
  const stored = seedEarlierAnchor();
  stored.expenses['EX-BNA'].payments[0].amount = 225;
  write(stored);
  const before = financialSnapshot(read());
  const view = await preview();
  assert.equal(view.currentOpening, periodOpening - 25);
  assert.equal(view.balanceDifference,25);
  assert.equal(view.canFinalize,false);
  assert.equal((await invoke('POST','/api/expenses/bank-statements/finalize',{body:{draftId}})).status,409);
  assert.equal(financialSnapshot(read()),before);
});

test('the next day uses the completed statement instead of its superseded partial closing', async () => {
  seedEarlierAnchor();
  const finalized = await invoke('POST','/api/expenses/bank-statements/finalize',{body:{draftId}});
  assert.equal(finalized.status,200,JSON.stringify(finalized.body));
  const stored = read(), nextId = 'BRD-OCT19-AFTER-COMPLETE';
  stored.receipts.push({id:'REC-NEXT-DAY',nature:'SANKI',account,date:'2026-10-19',amount:1000,
    source:'Next day receipt',receiptType:'other_income',category:'Other Income',bankReference:'629200000099'});
  stored.bankReconciliationDrafts[nextId]={id:nextId,nature:'SANKI',account,
    transactions:[{date:'2026-10-19',reference:'629200000099',description:'Next day receipt',debit:0,credit:1000,balance:closing+1000}],
    summary:{from:'2026-10-19',to:'2026-10-19',openingBalance:closing,closingBalance:closing+1000,totalDebits:0,totalCredits:1000,validated:true},
    resolutions:{},matchingPolicy:'balanced_date_amount_v5',temporaryFile:'',createdBy:'owner-opening-regression'};
  write(stored);
  const before=financialSnapshot(read()),view=await preview(nextId);
  assert.equal(view.currentOpening,closing);
  assert.equal(view.ledgerClosing,closing+1000);
  assert.equal(view.balanceDifference,0);
  assert.deepEqual(view.reconciliationConflicts,[]);
  assert.equal(view.canFinalize,true);
  assert.equal(financialSnapshot(read()),before);
});

test('an overlapping statement cannot bypass a deferred earlier closing', async () => {
  const stored=seedEarlierAnchor();
  const book=stored.bankStatements[account];
  book.imports[1].balanceReconciled=false;
  book.imports[1].closingBalanceDeferred=true;
  write(stored);
  const before=financialSnapshot(read()),view=await preview();
  assert.equal(view.canFinalize,false);
  assert.ok(view.reconciliationConflicts.some(row=>row.type==='ambiguous_opening_anchor'));
  assert.equal(financialSnapshot(read()),before);
});

test('a spanning statement rebuilds from its opening when the overlapping cutoff closing was deferred', async () => {
  const stored=seedEarlierAnchor(),book=stored.bankStatements[account];
  book.imports[0].balanceReconciled=false;
  book.imports[0].closingBalanceDeferred=true;
  book.lastReconciliation.balanceReconciled=false;
  book.lastReconciliation.closingBalanceDeferred=true;
  const draft=stored.bankReconciliationDrafts[draftId];
  draft.summary.from='2026-10-17';
  draft.summary.to='2026-10-19';
  write(stored);
  const before=financialSnapshot(read()),view=await preview();
  assert.equal(view.canFinalize,true);
  assert.equal(view.balanceDifference,0);
  assert.deepEqual(view.reconciliationConflicts,[]);
  assert.equal(financialSnapshot(read()),before);
});

test('conflicting earlier finalized closings block an overlapping statement without replacing history', async () => {
  const stored = seedEarlierAnchor();
  const conflict = clone(stored.bankStatements[account].imports[1]);
  conflict.id = 'BST-OCT15-CONFLICT';
  conflict.statementSummary.closingBalance += 100;
  conflict.ledgerClosingBalance += 100;
  stored.bankStatements[account].imports.push(conflict);
  write(stored);
  const before = financialSnapshot(read());
  const view = await preview();
  assert.equal(view.canFinalize,false);
  assert.ok(view.reconciliationConflicts.some(row=>row.type==='ambiguous_opening_anchor'));
  assert.equal((await invoke('POST','/api/expenses/bank-statements/finalize',{body:{draftId}})).status,409);
  assert.equal(financialSnapshot(read()),before);
});

test('a first partial statement with no finalized cutoff includes all earlier ledger movements', async () => {
  seed({ cutoff: '' });
  const before = financialSnapshot(read());
  const view = await preview();
  assertBalancedPeriod(view);
  assert.equal(view.rows.find(row => row.id === 'bank-0').app.id, paytmId);
  assert.equal(financialSnapshot(read()), before);
  assert.deepEqual(read().bankStatements, {});
});

test('a historical overlap ignores the later finalized closing and later ledger activity', async () => {
  const stored = seed({ cutoff: '2026-10-20' });
  stored.bankStatements[account].lastReconciliation.closingBalance = 999999.99;
  stored.receipts.push({ id: 'REC-FUTURE', nature: 'SANKI', account, date: '2026-10-20',
    amount: 8765, source: 'Later sales receipt' });
  write(stored);
  const before = financialSnapshot(read());
  const view = await preview();
  assertBalancedPeriod(view);
  assert.equal(view.rows.some(row => row.app && row.app.id === 'REC-FUTURE'), false);
  assert.equal(financialSnapshot(read()), before);
  assert.equal(read().bankStatements[account].lastReconciliation.closingBalance, 999999.99);
});

test('a statement spanning the finalized cutoff counts the entire covered period once', async () => {
  const stored = seed();
  const draft = stored.bankReconciliationDrafts[draftId];
  const supplierBank = { date: '2026-10-17', reference: '629000000002', description: 'Prior period supplier',
    debit: 13329.04, credit: 0, balance: periodOpening };
  draft.transactions.unshift(supplierBank);
  draft.transactions.push({ date: '2026-10-19', reference: '629200000001', description: 'Next day receipt',
    debit: 0, credit: 1000, balance: closing + 1000 });
  draft.resolutions = { 'bank-3': draft.resolutions['bank-2'], 'bank-4': draft.resolutions['bank-3'] };
  draft.summary = { from: '2026-10-17', to: '2026-10-19', openingBalance: 247844.86,
    closingBalance: closing + 1000, totalDebits: 95865.79, totalCredits: 14694, validated: true };
  stored.receipts.push({ id: 'REC-OCT19', nature: 'SANKI', account, date: '2026-10-19',
    amount: 1000, source: 'Next day receipt', bankReference: '629200000001' });
  const book = stored.bankStatements[account];
  book.transactions['supplier-finalized'] = { ...supplierBank, id: 'BTX-SUPPLIER-OCT17',
    firstSeenImport: previousImportId, lastSeenImport: previousImportId };
  book.imports[0].from = '2026-10-17';
  book.imports[0].rows = 2;
  book.imports[0].statementSummary.from = '2026-10-17';
  book.imports[0].statementSummary.openingBalance = 247844.86;
  book.imports[0].statementSummary.totalDebits = 13329.04;
  book.imports[0].reconciliationRows.unshift({ id: 'bank-supplier', bank: supplierBank,
    linkedRecordIds: ['EX-PRIOR-SUPPLIER/PAY-001'],
    ledger: { id: 'EX-PRIOR-SUPPLIER/PAY-001', date: '2026-10-17', debit: 13329.04, credit: 0 } });
  write(stored);
  const before = financialSnapshot(read());
  const view = await preview();
  assert.equal(view.currentOpening, 248209.82, 'forward overlap compares the unfinished tail from its finalized closing');
  assert.equal(view.ledgerClosing, closing + 1000);
  assert.equal(view.bankClosing, closing + 1000);
  assert.equal(view.balanceDifference, 0);
  assert.equal(view.canFinalize, true);
  assert.equal(view.unresolved, 0);
  assert.equal(view.rows.find(row => row.id === 'bank-0').status, 'already_reconciled');
  assert.equal(view.rows.find(row => row.id === 'bank-1').status, 'already_reconciled');
  assert.equal(view.rows.find(row => row.id === 'bank-2').chargeAmount, 5.9);
  assert.equal(financialSnapshot(read()), before);
});

test('a genuine bank difference remains visible and blocks finalization without an opening plug', async () => {
  seed({ bankGap: 25 });
  const before = financialSnapshot(read());
  const view = await preview();
  assert.equal(view.currentOpening, periodOpening);
  assert.equal(view.ledgerClosing, closing);
  assert.equal(view.bankClosing, closing + 25);
  assert.equal(view.balanceDifference, 25);
  assert.equal(view.balanceResolved, false);
  assert.equal(view.canFinalize, false);
  assert.equal(view.unresolved, 0);
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 409, JSON.stringify(finalized.body));
  assert.match(finalized.body.error, /closing balance differs/);
  assert.equal(financialSnapshot(read()), before);
  assert.ok(read().bankReconciliationDrafts[draftId]);
  assert.equal(read().bankReconciliationDrafts[draftId].openingResolution, undefined);
});

test('resolving, previewing and finalizing the overlap posts the two transfers and fee exactly once', async () => {
  seed();
  const before = financialSnapshot(read());
  const resolved = await invoke('POST', '/api/expenses/bank-statements/resolve', { body: {
    draftId, rowId: 'bank-3', action: 'create_internal_transfer', otherAccount: 'Arshpreet 1919',
    reason: 'Confirmed the statement transfer to Arshpreet 1919'
  } });
  assert.equal(resolved.status, 200, JSON.stringify(resolved.body));
  assertBalancedPeriod(resolved.body);
  assertBalancedPeriod(await preview());
  assert.equal(financialSnapshot(read()), before, 'saving a transfer decision does not post it');

  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  assert.equal(finalized.body.balanceReconciled, true);
  assert.equal(finalized.body.balanceDifference, 0);
  const after = read();
  assert.equal(after.openingBalances[account], baseOpening, 'the lifetime account opening remains intact');
  assert.equal(after.transfers.length, 3);
  assert.equal(after.transfers.find(row => row.id === 'TR-00181').amount, 10444);
  assert.deepEqual(after.transfers.filter(row => row.reconciliationDraft === draftId)
    .map(row => [row.toAccount, row.amount]).sort(), [
      ['Arshpreet 1919', 962.95], ['IndusInd Bank 7883', 71123.9]
    ]);
  assert.equal(after.adjustments.length, 1);
  assert.equal(after.adjustments[0].amount, -5.9);
  assert.equal(after.adjustments[0].automaticAxisTransferCharge, true);
  assert.equal(after.reconciliationExpenses.length, 1);
  assert.equal(after.reconciliationExpenses[0].amount, 5.9);
  assert.equal(after.reconciliationExpenses[0].category, 'BANK CHARGES');
  assert.equal(after.paytmSettlements.length, 1, 'the previously finalized Paytm credit is not duplicated');
  assert.equal(after.paytmSettlements[0].netAmount, 13694);
  assert.equal(after.bankStatements[account].lastReconciliation.ledgerClosingBalance, closing);
  assert.equal(after.bankStatements[account].lastReconciliation.closingBalance, closing);
  assert.equal(Object.values(after.bankStatements[account].transactions).length, 4);
  assert.equal(after.bankStatements[account].imports.find(row => row.draftId === draftId).rows, 3);
  assert.equal(after.bankReconciliationDrafts[draftId], undefined);

  const ledger = await invoke('GET', '/api/expenses/account-ledger', { query: {
    nature: 'SANKI', account, from: date, to: date
  } });
  assert.equal(ledger.status, 200, JSON.stringify(ledger.body));
  assert.equal(money(ledger.body.balance), closing, 'saved ledger agrees with the preview after posting');
  const finalSnapshot = financialSnapshot(read());
  const repeated = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(repeated.status, 404, JSON.stringify(repeated.body));
  const finalPreview = await invoke('GET', '/api/expenses/bank-statements', { query: { nature: 'SANKI', account } });
  assert.equal(finalPreview.status, 200);
  assert.equal(finalPreview.body.draft, null);
  assert.equal(financialSnapshot(read()), finalSnapshot, 'repeat finalization and preview create no money');
});

test('finalizing a historical gap preserves the latest finalized date and closing', async () => {
  const stored = seed({ cutoff: '2026-10-20' });
  const book = stored.bankStatements[account];
  const futureBank = { date: '2026-10-20', reference: '629300000001', description: 'Later finalized sales',
    debit: 0, credit: 8765, balance: closing + 8765 };
  stored.receipts.push({ id: 'REC-FUTURE', nature: 'SANKI', account, date: '2026-10-20', amount: 8765,
    source: 'Later finalized sales', bankReference: futureBank.reference });
  book.transactions['future-finalized'] = { ...futureBank, id: 'BTX-OCT20', firstSeenImport: 'BST-OCT20' };
  book.imports.push({ id: 'BST-OCT20', draftId: 'BRD-OCT20', from: '2026-10-20', to: '2026-10-20',
    finalizedAt: '2026-10-20T12:00:00Z', rows: 1,
    statementSummary: { from: '2026-10-20', to: '2026-10-20', openingBalance: closing,
      closingBalance: closing + 8765, totalDebits: 0, totalCredits: 8765, validated: true },
    reconciliationRows: [{ bank: futureBank, linkedRecordIds: ['REC-FUTURE'],
      ledger: { id: 'REC-FUTURE', date: '2026-10-20', debit: 0, credit: 8765 } }] });
  book.lastReconciliation = { through: '2026-10-20', at: '2026-10-20T12:00:00Z', by: 'original-owner',
    closingBalance: closing + 8765, ledgerClosingBalance: closing + 8765, draftId: 'BRD-OCT20',
    balanceReconciled: true, transactionsReconciled: true, balanceDifference: 0 };
  const latest = clone(book.lastReconciliation);
  write(stored);
  assertBalancedPeriod(await preview());
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  assert.equal(finalized.body.reconciledThrough, '2026-10-20');
  const after = read();
  assert.equal(after.bankStatements[account].reconciledThrough, '2026-10-20');
  assert.deepEqual(after.bankStatements[account].lastReconciliation, latest,
    'Oct18 gap filling must not replace the latest Oct20 closing with an earlier balance');
  assert.ok(after.bankStatements[account].imports.some(record => record.draftId === draftId));
  assert.equal(after.bankReconciliationDrafts[draftId], undefined);
});

test('an explicitly reviewed period-opening correction changes the lifetime opening only by its delta', async () => {
  seed({ bankGap: 25 });
  const reviewed = await invoke('POST', '/api/expenses/bank-statements/resolve-balance', { body: {
    draftId, amount: periodOpening + 25, reason: 'Owner reviewed the genuine missing opening amount of 25'
  } });
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  assert.equal(reviewed.body.currentOpening, periodOpening);
  assert.equal(reviewed.body.ledgerClosing, closing + 25);
  assert.equal(reviewed.body.balanceDifference, 0);
  assert.equal(read().openingBalances[account], baseOpening, 'review stages the correction before finalization');
  assert.equal(read().bankReconciliationDrafts[draftId].openingResolution.previousAmount, periodOpening);
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read();
  assert.equal(after.openingBalances[account], baseOpening + 25,
    'a period opening contains historical movements and cannot replace the lifetime account opening');
  assert.equal(after.bankStatements[account].lastReconciliation.ledgerClosingBalance, closing + 25);
  const ledger = await invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account, to: date } });
  assert.equal(ledger.status, 200);
  assert.equal(money(ledger.body.balance), closing + 25);
});

test('a stale opening review blocks finalization until the owner reviews the recalculated opening', async () => {
  const stored = seed();
  stored.bankReconciliationDrafts[draftId].openingResolution = {
    amount: periodOpening, previousAmount: baseOpening,
    reason: 'Legacy review made before historical movements were included',
    by: 'owner-opening-regression', at: date + 'T11:00:00Z'
  };
  write(stored);
  const before = financialSnapshot(read());
  const view = await preview();
  assert.equal(view.currentOpening, periodOpening);
  assert.equal(view.canFinalize, false, 'stale previousAmount cannot authorize an unrelated opening adjustment');
  const rejected = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(rejected.status, 409, JSON.stringify(rejected.body));
  assert.ok(rejected.body.reconciliation.reconciliationConflicts.some(conflict => /opening/i.test(conflict.type)),
    'the rejection identifies the stale opening review');
  assert.equal(financialSnapshot(read()), before);
  assert.ok(read().bankReconciliationDrafts[draftId]);
  const reviewed = await invoke('POST', '/api/expenses/bank-statements/resolve-balance', { body: {
    draftId, amount: periodOpening, reason: 'Owner reviewed the corrected opening; no further adjustment'
  } });
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  assert.equal(read().bankReconciliationDrafts[draftId].openingResolution.previousAmount, periodOpening);
  assert.equal(reviewed.body.canFinalize, true);
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  assert.equal(read().openingBalances[account], baseOpening);
});

test('creating another transfer for the posted TR-00181 principal and Axis fee is rejected', async () => {
  seed();
  const before = financialSnapshot(read());
  const rejected = await invoke('POST', '/api/expenses/bank-statements/resolve', { body: {
    draftId, rowId: 'bank-1', action: 'create_internal_transfer', otherAccount: 'IndusInd Bank 8181',
    principalAmount: 10444, chargeAmount: 5.9, reason: 'Attempt to recreate the already posted transfer'
  } });
  assert.equal(rejected.status, 409, JSON.stringify(rejected.body));
  assert.match(rejected.body.error, /TR-00181/);
  assert.equal(read().bankReconciliationDrafts[draftId].resolutions['bank-1'], undefined);
  assert.equal(financialSnapshot(read()), before);
  assertBalancedPeriod(await preview());
});

test('a distinct verified bank reference permits another transfer with the same date, route and principal', async () => {
  const stored = seed(), draft = stored.bankReconciliationDrafts[draftId], book = stored.bankStatements[account];
  const previousBank = { date, reference: '629100000099', description: 'IMPS/P2A/629100000099/INDUSIND BANK/X681883',
    debit: 10444, credit: 0, balance: 237765.82 };
  stored.transfers[0].bankReference = previousBank.reference;
  stored.transfers[0].reconciliationDraft = previousDraftId;
  stored.transfers[0].bankRowId = 'bank-1';
  draft.transactions.splice(1, 0, previousBank);
  draft.transactions.slice(2).forEach(bank => { bank.balance = money(bank.balance - 10444); });
  draft.resolutions = { 'bank-3': draft.resolutions['bank-2'], 'bank-4': draft.resolutions['bank-3'] };
  draft.summary.totalDebits = money(draft.summary.totalDebits + 10444);
  draft.summary.closingBalance = money(closing - 10444);
  book.transactions['previous-transfer-finalized'] = { ...previousBank, id: 'BTX-TR181',
    firstSeenImport: previousImportId, reconciliationDraftId: previousDraftId, reconciliationDraftRowIndex: 1 };
  book.imports[0].rows = 2;
  book.imports[0].statementSummary.totalDebits = 10444;
  book.imports[0].statementSummary.closingBalance = 237765.82;
  book.lastReconciliation.closingBalance = 237765.82;
  book.imports[0].reconciliationRows.push({ id: 'bank-1',
    bank: { ...previousBank, transactionId: 'BTX-TR181' }, linkedRecordIds: ['TR-00181'],
    ledger: { id: 'TR-00181', date, debit: 10444, credit: 0 } });
  write(stored);
  const before = financialSnapshot(read());
  const resolved = await invoke('POST', '/api/expenses/bank-statements/resolve', { body: {
    draftId, rowId: 'bank-2', action: 'create_internal_transfer', otherAccount: 'IndusInd Bank 8181',
    principalAmount: 10444, chargeAmount: 5.9, reason: 'Different verified bank reference confirms another transfer'
  } });
  assert.equal(resolved.status, 200, JSON.stringify(resolved.body));
  assert.equal(resolved.body.ledgerClosing, money(closing - 10444));
  assert.equal(resolved.body.balanceDifference, 0);
  assert.equal(resolved.body.canFinalize, true);
  assert.equal(financialSnapshot(read()), before);
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read(), samePrincipal = after.transfers.filter(transfer => transfer.toAccount === 'IndusInd Bank 8181');
  assert.equal(after.transfers.length, 4);
  assert.deepEqual(samePrincipal.map(transfer => [transfer.amount, transfer.bankReference]).sort(),
    [[10444, '629100000002'], [10444, '629100000099']]);
  assert.equal(after.adjustments.length, 1);
  assert.equal(after.adjustments[0].amount, -5.9);
  assert.equal(after.bankStatements[account].lastReconciliation.ledgerClosingBalance, money(closing - 10444));
});

test('duplicate pending transfer decisions in two drafts block preview and finalization without posting', async () => {
  const stored = seed(), original = stored.bankReconciliationDrafts[draftId], duplicateId = 'BRD-OCT18-DUPLICATE';
  stored.bankReconciliationDrafts[duplicateId] = { id: duplicateId, nature: 'SANKI', account,
    transactions: [clone(original.transactions[2])],
    summary: { from: date, to: date, openingBalance: 237759.92, closingBalance: 166636.02,
      totalDebits: 71123.9, totalCredits: 0, validated: true },
    resolutions: { 'bank-0': clone(original.resolutions['bank-2']) },
    matchingPolicy: 'balanced_date_amount_v5', createdAt: date + 'T12:01:00Z', createdBy: 'owner-opening-regression' };
  write(stored);
  const before = financialSnapshot(read());
  for (const [currentId, otherId] of [[draftId, duplicateId], [duplicateId, draftId]]) {
    const view = await preview(currentId);
    assert.equal(view.canFinalize, false);
    assert.ok(JSON.stringify(view.reconciliationConflicts).includes(otherId), 'conflict identifies the other pending draft');
    const rejected = await invoke('POST', '/api/expenses/bank-statements/finalize', {
      body: { draftId: currentId, deferClosingBalance: true }
    });
    assert.equal(rejected.status, 409, JSON.stringify(rejected.body));
    assert.ok(read().bankReconciliationDrafts[currentId]);
  }
  const resolveRejected = await invoke('POST', '/api/expenses/bank-statements/resolve', { body: {
    draftId, rowId: 'bank-2', action: 'create_internal_transfer', otherAccount: 'IndusInd Bank 7883',
    otherNature: 'PERSONAL', reason: 'Attempt to re-save the duplicated pending transfer'
  } });
  assert.equal(resolveRejected.status, 409, JSON.stringify(resolveRejected.body));
  assert.ok(JSON.stringify(resolveRejected.body).includes(duplicateId));
  assert.equal(financialSnapshot(read()), before);
});

function seedPostedTransferFee({ savedCreation = false, postedFee = true } = {}) {
  const stored = seed(), transfer = stored.transfers[0];
  transfer.reconciliationDraft = draftId;
  transfer.bankRowId = 'bank-1';
  const adjustment = { id: 'ADJ-EXISTING-FEE', nature: 'SANKI', account, date, amount: -5.9,
    note: 'Already posted transfer bank fee', reconciliationDraft: draftId, bankRowId: 'bank-1' };
  if (savedCreation) {
    stored.bankReconciliationDrafts[draftId].resolutions['bank-1'] = {
      action: 'create_internal_transfer', otherAccount: 'IndusInd Bank 8181', principalAmount: 10444,
      chargeAmount: 5.9, reason: 'Saved decision whose principal and fee already posted'
    };
    adjustment.transferChargeFor = transfer.id;
    if (postedFee) transfer.confirmedBankSplit = { gross: 10449.9, principal: 10444, charge: 5.9, chargeId: adjustment.id };
  } else adjustment.automaticAxisTransferCharge = true;
  if (postedFee) {
    stored.adjustments.push(adjustment);
    stored.reconciliationExpenses.push({ id: 'BRE-EXISTING-FEE', nature: 'SANKI', account, date, amount: 5.9,
      category: 'BANK CHARGES', type: 'running', vendor: 'Axis Bank', adjustmentId: adjustment.id,
      reconciliationDraft: draftId, particulars: 'Already posted transfer bank fee' });
  }
  write(stored);
}

for (const [savedCreation, postedFee, label] of [
  [false, true, 'matched transfer with an already posted automatic fee'],
  [true, true, 'saved transfer creation with posted principal and fee'],
  [true, false, 'saved transfer creation with posted principal and a pending fee']
]) {
  test(label + ' is counted once through preview and finalization', async () => {
      seedPostedTransferFee({ savedCreation, postedFee });
      const before = financialSnapshot(read());
      const view = await preview();
      assert.equal(view.currentOpening, periodOpening);
      assert.equal(view.ledgerClosing, closing);
      assert.equal(view.balanceDifference, 0);
      assert.equal(view.pendingPostingNet, postedFee ? -72086.85 : -72092.75);
      assert.equal(view.canFinalize, true);
      assert.equal(view.unresolved, 0);
      assert.equal(view.rows.some(row => row.status === 'missing_in_bank' &&
        row.app && ['TR-00181', 'ADJ-EXISTING-FEE'].includes(row.app.id)), false,
      'posted principal and fee remain associated with their original bank row');
      assert.deepEqual(await preview(), view);
      assert.equal(financialSnapshot(read()), before);
      const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
      assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
      const after = read();
      assert.equal(after.transfers.length, 3);
      assert.equal(after.transfers.filter(transfer => transfer.id === 'TR-00181').length, 1);
      assert.equal(after.adjustments.length, 1);
      if (postedFee) assert.equal(after.adjustments[0].id, 'ADJ-EXISTING-FEE');
      assert.equal(after.adjustments[0].amount, -5.9);
      assert.equal(after.reconciliationExpenses.length, 1);
      assert.equal(after.bankStatements[account].lastReconciliation.ledgerClosingBalance, closing);
      const ledger = await invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account, to: date } });
      assert.equal(ledger.status, 200);
      assert.equal(money(ledger.body.balance), closing);
      const postedSnapshot = financialSnapshot(after);
      const repeated = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
      assert.equal(repeated.status, 404);
      assert.equal(financialSnapshot(read()), postedSnapshot);
    });
}

test('a stale create-transfer decision on an already finalized row never posts another transfer', async () => {
  const stored = seed();
  stored.bankReconciliationDrafts[draftId].resolutions['bank-0'] = {
    action: 'create_internal_transfer', otherAccount: 'Counter Cash',
    reason: 'Stale legacy decision superseded by the finalized Paytm bank row'
  };
  write(stored);
  const view = await preview();
  assertBalancedPeriod(view);
  assert.equal(view.rows.find(row => row.id === 'bank-0').status, 'already_reconciled');
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read();
  assert.equal(after.transfers.length, 3);
  assert.equal(after.transfers.some(transfer => transfer.reconciliationDraft === draftId && transfer.bankRowId === 'bank-0'), false);
  assert.equal(after.paytmSettlements.length, 1);
  assert.equal(after.adjustments.length, 1);
  assert.equal(after.bankStatements[account].lastReconciliation.ledgerClosingBalance, closing);
  const ledger = await invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account, to: date } });
  assert.equal(ledger.status, 200);
  assert.equal(money(ledger.body.balance), closing);
});

function seedFinalizedTransferWithStaleDecision(action) {
  seedPostedTransferFee();
  const stored = read(), draft = stored.bankReconciliationDrafts[draftId], book = stored.bankStatements[account];
  const bank = draft.transactions[1], transfer = stored.transfers[0];
  transfer.reconciliationDraft = previousDraftId;
  stored.adjustments[0].reconciliationDraft = previousDraftId;
  stored.reconciliationExpenses[0].reconciliationDraft = previousDraftId;
  book.transactions['transfer-finalized'] = { ...bank, id: 'BTX-TR181-FEE',
    firstSeenImport: previousImportId, reconciliationDraftId: previousDraftId, reconciliationDraftRowIndex: 1 };
  book.imports[0].rows = 2;
  book.imports[0].statementSummary.totalDebits = bank.debit;
  book.imports[0].statementSummary.closingBalance = bank.balance;
  book.lastReconciliation.closingBalance = bank.balance;
  book.imports[0].reconciliationRows.push({ id: 'bank-1', bank: { ...bank, transactionId: 'BTX-TR181-FEE' },
    linkedRecordIds: [transfer.id, stored.adjustments[0].id], ledger: { id: transfer.id, date, debit: 10444, credit: 0,
      linkedEntries: [{ id: transfer.id, date, debit: 10444, credit: 0 },
        { id: stored.adjustments[0].id, date, debit: 5.9, credit: 0 }] } });
  draft.resolutions['bank-1'] = { action, reason: 'Legacy decision superseded by finalized transfer and fee evidence',
    principalAmount: 10444, chargeAmount: 5.9, category: 'FOOD EXPENSE', chargeCategory: 'BANK CHARGES',
    appId: transfer.id, appIds: [transfer.id], roundingAmount: -5.9, openingPayableAmount: 5.9,
    advanceAmount: 5.9, advanceApplications: [], vendor: 'Previously settled vendor', openingNature: 'SANKI' };
  write(stored);
  return stored;
}

for (const action of ['create_adjustment', 'create_split_adjustment', 'split_allocation',
  'exclude', 'opening_vendor_payable_split', 'link_with_rounding', 'vendor_advance_split']) {
  test('finalized bank evidence supersedes a stale ' + action + ' decision without posting it again', async () => {
    const before = seedFinalizedTransferWithStaleDecision(action);
    const view = await preview();
    assert.equal(view.rows.find(row => row.id === 'bank-1').status, 'already_reconciled');
    assert.equal(view.ledgerClosing, closing);
    assert.equal(view.pendingPostingNet, -72086.85);
    assert.equal(view.canFinalize, true);
    const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
    assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
    const after = read();
    assert.equal(after.transfers.length, 3);
    assert.deepEqual(after.adjustments, before.adjustments);
    assert.deepEqual(after.reconciliationExpenses, before.reconciliationExpenses);
    assert.deepEqual(after.vendorOpeningPayables, before.vendorOpeningPayables);
    assert.deepEqual(after.vendorAdvances, before.vendorAdvances);
    assert.deepEqual(after.bankTruthMovements, before.bankTruthMovements);
    assert.equal(after.bankStatements[account].lastReconciliation.ledgerClosingBalance, closing);
    const ledger = await invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account, to: date } });
    assert.equal(ledger.status, 200);
    assert.equal(money(ledger.body.balance), closing);
  });
}

test('a create-split-adjustment decision already posted in this draft stays exact once', async () => {
  const stored = seed(), draft = stored.bankReconciliationDrafts[draftId];
  stored.transfers = [];
  stored.adjustments.push({ id: 'ADJ-POSTED-SPLIT', nature: 'SANKI', account, date, amount: -10449.9,
    note: 'Already posted expense and bank charge', reconciliationDraft: draftId, bankRowId: 'bank-1' });
  stored.reconciliationExpenses.push(
    { id: 'BRE-POSTED-MAIN', nature: 'SANKI', account, date, amount: 10444, category: 'FOOD EXPENSE',
      adjustmentId: 'ADJ-POSTED-SPLIT', reconciliationDraft: draftId },
    { id: 'BRE-POSTED-FEE', nature: 'SANKI', account, date, amount: 5.9, category: 'BANK CHARGES',
      adjustmentId: 'ADJ-POSTED-SPLIT', reconciliationDraft: draftId });
  draft.resolutions['bank-1'] = { action: 'create_split_adjustment', principalAmount: 10444, chargeAmount: 5.9,
    category: 'FOOD EXPENSE', chargeCategory: 'BANK CHARGES', reason: 'Recover the already posted split without duplicate money' };
  write(stored);
  const before = financialSnapshot(read()), view = await preview();
  assert.equal(view.ledgerClosing, closing);
  assert.equal(view.pendingPostingNet, -72086.85);
  assert.equal(view.unresolved, 0);
  assert.equal(view.canFinalize, true);
  assert.equal(financialSnapshot(read()), before);
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read();
  assert.equal(after.transfers.length, 2);
  assert.deepEqual(after.adjustments, stored.adjustments);
  assert.deepEqual(after.reconciliationExpenses, stored.reconciliationExpenses);
  assert.equal(after.bankStatements[account].lastReconciliation.ledgerClosingBalance, closing);
  const ledger = await invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account, to: date } });
  assert.equal(ledger.status, 200);
  assert.equal(money(ledger.body.balance), closing);
  const posted = financialSnapshot(read());
  const repeated = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(repeated.status, 404);
  assert.equal(financialSnapshot(read()), posted);
});

test('a same-origin Paytm settlement already posted is counted once through finalization', async () => {
  const stored = seed({ cutoff: '' }), draft = stored.bankReconciliationDrafts[draftId];
  stored.paytmSettlements[0].reconciliationDraft = draftId;
  stored.paytmSettlements[0].bankRowId = 'bank-0';
  draft.resolutions['bank-0'] = { action: 'paytm_settlement', grossAmount: 13694,
    chargeAmount: 0, paytmChargeAmount: 0, hiddenChargeAmount: 0,
    transferIds: [], orderIds: [], otherReceipts: [], reason: 'Retain the exact existing Paytm posting' };
  write(stored);
  const before = financialSnapshot(read()), view = await preview();
  assert.equal(view.canFinalize, true);
  assert.equal(view.ledgerClosing, closing);
  assert.equal(view.pendingPostings.some(row => row.rowId === 'bank-0'), false);
  assert.equal(financialSnapshot(read()), before);
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read();
  assert.equal(after.paytmSettlements.length, 1);
  assert.equal(after.paytmSettlements[0].id, paytmId);
  assert.equal(after.paytmSettlements[0].netAmount, 13694);
  assert.equal(after.receipts.length, stored.receipts.length, 'settlement recovery does not repeat component receipts');
  assert.equal(after.bankStatements[account].lastReconciliation.ledgerClosingBalance, closing);
  const ledger = await invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account, to: date } });
  assert.equal(ledger.status, 200);
  assert.equal(money(ledger.body.balance), closing);
});

test('a partial posted adjustment blocks finalization instead of silently skipping the pending remainder', async () => {
  const stored = seed();
  stored.bankReconciliationDrafts[draftId].resolutions['bank-2'] = {
    action: 'create_adjustment', category: 'BANK CHARGES', reason: 'Review an incomplete existing adjustment'
  };
  stored.adjustments.push({ id: 'ADJ-PARTIAL', nature: 'SANKI', account, date, amount: -100,
    reconciliationDraft: draftId, bankRowId: 'bank-2' });
  write(stored);
  const before = financialSnapshot(read()), view = await preview();
  assert.equal(view.canFinalize, false);
  assert.ok(view.reconciliationConflicts.some(conflict => conflict.type === 'mismatched_posted_adjustment'));
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', {
    body: { draftId, deferClosingBalance: true }
  });
  assert.equal(finalized.status, 409, JSON.stringify(finalized.body));
  assert.equal(financialSnapshot(read()), before);
  assert.ok(read().bankReconciliationDrafts[draftId]);
});

test('a partial posted transfer principal blocks finalization instead of posting only its fee', async () => {
  const stored = seed(), transfer = stored.transfers[0];
  transfer.reconciliationDraft = draftId;
  transfer.bankRowId = 'bank-1';
  transfer.amount = 10000;
  stored.bankReconciliationDrafts[draftId].resolutions['bank-1'] = {
    action: 'create_internal_transfer', otherAccount: 'IndusInd Bank 8181', principalAmount: 10444,
    chargeAmount: 5.9, reason: 'Review an incomplete existing transfer principal'
  };
  write(stored);
  const before = financialSnapshot(read()), view = await preview();
  assert.equal(view.canFinalize, false);
  assert.ok(view.reconciliationConflicts.some(conflict => conflict.type === 'mismatched_posted_principal'));
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', {
    body: { draftId, deferClosingBalance: true }
  });
  assert.equal(finalized.status, 409, JSON.stringify(finalized.body));
  assert.equal(financialSnapshot(read()), before);
  assert.equal(read().transfers[0].amount, 10000);
  assert.equal(read().adjustments.length, 0, 'no fee or balancing entry was posted');
});
