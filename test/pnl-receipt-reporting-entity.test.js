'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { buildReport } = require('../modules/pnl-report');

const range = { from: '2026-10-01', to: '2026-10-23' };
const evidence = () => ({ draftId: 'BRD-SYNTHETIC-RECEIPT', rowId: 'bank-0', reference: 'REF-SYNTHETIC', description: 'Synthetic incoming receipt' });
const receipt = overrides => ({ id: 'REC-SYNTHETIC', nature: 'SANKI', account: 'Synthetic Bank', amount: 100, date: '2026-10-23', receiptType: 'other_income', source: 'Synthetic source', bankReconciliationEvidence: evidence(), ...overrides });
function report(receipts, additions = {}) {
  const source = { expenses: { receipts, ...additions } }, before = JSON.stringify(source);
  const result = buildReport(source, range);
  assert.equal(JSON.stringify(source), before, 'reporting never rewrites receipts, ownership, bank evidence or other records');
  return result;
}

test('explicit SAMAST or PERSONAL classification excludes a SANKI-owned receipt from SANKI Results', () => {
  for (const reportingNature of ['SAMAST', 'PERSONAL']) {
    const result = report([receipt({ reportingNature })]);
    assert.equal(result.transactions.length, 0);
    assert.equal(result.totals.otherIncome, 0);
    assert.equal(result.warnings.length, 0);
  }
});

test('explicit SANKI classification includes eligible PERSONAL-owned income exactly once without changing ownership', () => {
  for (const receiptType of ['other_income', 'bank_interest']) {
    const source = receipt({ nature: 'PERSONAL', reportingNature: 'SANKI', receiptType, account: 'Synthetic Personal Bank' });
    const result = report([source, { ...source }]);
    assert.equal(result.transactions.filter(row => row.id === 'RECEIPT/REC-SYNTHETIC').length, 1);
    assert.equal(result.totals.otherIncome, 100);
    assert.equal(result.transactions[0].account, 'Synthetic Personal Bank');
    assert.equal(source.nature, 'PERSONAL');
  }
});

test('legacy receipts without new metadata preserve their prior entity and provisional handling exactly', () => {
  for (const nature of ['SANKI', 'A3', 'SAMAST', 'PERSONAL']) {
    const old = receipt({ nature }), result = report([old]);
    assert.equal(result.totals.otherIncome, ['SANKI', 'A3'].includes(nature) ? 100 : 0);
    const pendingEvidence = report([old], { bankReconciliationDrafts: { 'BRD-SYNTHETIC-RECEIPT': { id: 'BRD-SYNTHETIC-RECEIPT' } } });
    assert.deepEqual(pendingEvidence, result, 'new evidence-only exclusion must not reinterpret historical receipts');
    const oldProvisional = report([{ ...old, reconciliationDraft: 'BRD-LEGACY' }], { bankReconciliationDrafts: { 'BRD-LEGACY': {} } });
    assert.equal(oldProvisional.transactions.length, 0);
  }
});

test('invalid explicit classification or missing reconciliation identity fails closed without defaulting to SANKI', () => {
  for (const reportingNature of ['', null, undefined, {}, ['SANKI'], 'A3', 'UNKNOWN', 'sanki', ' SANKI ']) {
    const result = report([receipt({ reportingNature })]);
    assert.equal(result.transactions.length, 0);
    assert.equal(result.totals.otherIncome, 0);
    assert.ok(result.warnings.some(row => row.reference === 'REC-SYNTHETIC' && /classification or bank evidence is invalid/.test(row.message)));
  }
  for (const bankReconciliationEvidence of [null, {}, { draftId: 'BRD-1', rowId: 'app-0' }, { draftId: '', rowId: 'bank-0' }]) {
    const result = report([receipt({ reportingNature: 'SANKI', bankReconciliationEvidence })]);
    assert.equal(result.transactions.length, 0);
    assert.ok(result.warnings.some(row => row.reference === 'REC-SYNTHETIC'));
  }
});

test('new classified or account-inferred receipts stay excluded while their evidence draft exists', () => {
  const drafts = { 'BRD-SYNTHETIC-RECEIPT': { id: 'BRD-SYNTHETIC-RECEIPT' } };
  for (const metadata of [{ reportingNature: 'SANKI' }, { receiptAccount: { version: 'receipt-account-v1', nature: 'SANKI', account: 'Synthetic Bank' } }]) {
    const result = report([receipt(metadata)], { bankReconciliationDrafts: drafts });
    assert.equal(result.transactions.length, 0);
    assert.equal(result.totals.otherIncome, 0);
    assert.equal(result.warnings.length, 0);
  }
});

test('finalized new receipt is included after draft removal while its original evidence remains retained', () => {
  const source = receipt({ nature: 'PERSONAL', reportingNature: 'SANKI' });
  const result = report([source], { bankReconciliationDrafts: {} });
  assert.equal(result.totals.otherIncome, 100);
  assert.equal(result.transactions[0].id, 'RECEIPT/REC-SYNTHETIC');
  assert.deepEqual(source.bankReconciliationEvidence, evidence());
  assert.equal(source.nature, 'PERSONAL');
});

test('classification never converts loans, contributions, rental/security or refunds into ordinary income', () => {
  for (const receiptType of ['loan_received', 'owner_contribution', 'rental_income', 'rental_security', 'personal_income', 'home_received', 'other_personal_received', 'refund', 'asset_sale']) {
    const result = report([receipt({ nature: 'PERSONAL', reportingNature: 'SANKI', receiptType })]);
    assert.equal(result.totals.otherIncome, 0);
    assert.equal(result.transactions.length, 0);
    assert.equal(result.warnings.length, ['refund', 'asset_sale'].includes(receiptType) ? 1 : 0);
  }
});

test('existing linked-refund duplicate suppression and partial-refund warning survive reporting attribution', () => {
  const refunded = receipt({ id: 'R', nature: 'PERSONAL', reportingNature: 'SANKI', receiptType: 'refund', amount: 50 });
  const additions = {
    expenses: { E: { id: 'E', nature: 'SANKI', amount: 200, status: 'paid', type: 'variable', payments: [{ id: 'P', date: '2026-10-01', amount: 200 }] } },
    expenseRefunds: [{ id: 'RF', nature: 'SANKI', date: '2026-10-23', status: 'received', sources: [{ kind: 'expense', expenseId: 'E', amount: 50, paymentAllocations: [{ paymentId: 'P', amount: 50 }] }], components: [{ mode: 'bank', amount: 50, externalMovementId: 'R' }] }]
  };
  const full = report([refunded], additions);
  assert.equal(full.totals.expenses, 150);
  assert.equal(full.totals.otherIncome, 0);
  assert.equal(full.transactions.filter(row => row.kind === 'expense_refund').length, 1);
  assert.ok(!full.warnings.some(row => row.reference === 'R'));
  const partial = report([{ ...refunded, amount: 60 }], additions);
  assert.equal(partial.totals.expenses, 150);
  assert.equal(partial.totals.otherIncome, 0);
  assert.ok(partial.warnings.some(row => row.reference === 'R' && /requires profit classification/.test(row.message)));
});

test('manual product-sale receipt interpretation and corrections remain outside the new incoming-receipt metadata scope', () => {
  const legacy = receipt({ receiptType: 'product_sale', source: 'Synthetic manual sale' });
  const baseline = report([legacy]);
  assert.deepEqual(report([{ ...legacy, reportingNature: 'PERSONAL' }]), baseline);
  const personal = receipt({ receiptType: 'product_sale', nature: 'PERSONAL' });
  assert.deepEqual(report([{ ...personal, reportingNature: 'SANKI' }]), report([personal]));
});
