'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');

test('paying-account correction explains Manage permission without claiming Owner-only access', () => {
  const hint = html.match(/id="editPayingAccountWrap">[\s\S]*?<div class="hint">([^<]*)<\/div>/);
  assert.ok(hint, 'paying-account correction hint is present');
  assert.equal(hint[1], 'Requires Manage expense financial-edit permission. This moves the existing payment to the selected ledger. Approval and reconciliation locks still apply.');
  assert.doesNotMatch(hint[1], /Owner-only/);
});

test('credit-card visibility label describes the restricted PERSONAL scope', () => {
  const label = html.match(/<label><input id="cc_owner" type="checkbox">([^<]*)<\/label>/);
  assert.ok(label, 'existing restricted-visibility control is retained');
  assert.equal(label[1].trim(), 'Restricted PERSONAL visibility');
  assert.doesNotMatch(label[1], /Owner-only/);
});

test('genuine Owner historical, opening and approval messages remain unchanged', () => {
  assert.match(html, /<b>Owner-only historical correction\.<\/b> This closes the selected reimbursement liability without deducting any bank or cash account\./);
  assert.match(html, /Owner-only correction\. This replaces the account's opening balance and is recorded in the audit history\./);
  assert.match(html, /Owner approval is required; a count, review or refresh never adjusts the ledger automatically\./);
  assert.match(html, /Finalized corrections are Owner-only\./);
});
