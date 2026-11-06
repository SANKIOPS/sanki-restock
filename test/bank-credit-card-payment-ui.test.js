'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');

test('bank reconciliation offers card-payment attachment from both 7883 and 0993 only', () => {
  const start = html.indexOf('    function bankCanAttachCreditCardPayment(');
  const end = html.indexOf('    window.openBankReview=', start);
  assert.ok(start >= 0 && end > start);
  const context = vm.createContext({});
  vm.runInContext(html.slice(start, end), context);
  assert.equal(context.bankCanAttachCreditCardPayment('IndusInd Bank 7883'), true);
  assert.equal(context.bankCanAttachCreditCardPayment('ICICI Bank 0993'), true);
  assert.equal(context.bankCanAttachCreditCardPayment('Axis Bank 3448'), false);
  assert.equal(context.bankCanAttachCreditCardPayment('HDFC 7376'), false);
});

test('card-payment dialog uses the selected bank and keeps the card choice generic', () => {
  assert.match(html, /paid from '\+account/);
  assert.match(html, /Choose the related credit card and enter a note/);
  assert.match(html, /available in that card’s reconciliation/);
  assert.doesNotMatch(html, /paid from ICICI Bank 0993/);
  assert.doesNotMatch(html, /Choose Card 8000 and enter a note/);
  assert.match(html, /credit-cards\/payments\/attach-bank/);
  assert.match(html, /draftId:bankDraftId,rowId:bankCardPaymentRowId/);
});
