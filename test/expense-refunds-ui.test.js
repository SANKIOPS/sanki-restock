'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const source = fs.readFileSync(path.join(__dirname, '../public/expense-refunds.js'), 'utf8');
test('refund script URL changes with source content to invalidate cached releases', () => {
  const version = crypto.createHash('sha256').update(source.replace(/\r\n/g, '\n').trimEnd()).digest('hex').slice(0, 12);
  const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
  assert.ok(html.includes('src="/expense-refunds.js?v=' + version + '"'), 'Update the refund script content version when its source changes');
});
test('refund dialog overrides the shared narrow dialog maximum width', () => {
  assert.match(source, /#rf_dialog\{width:min\(960px,95vw\);max-width:95vw;/);
});
test('refund mode hidden fields override shared flex-field display rules', () => {
  assert.match(source, /#rf_dialog \[hidden\]\{display:none!important\}/);
  assert.match(source, /\.rf_issuer_field'\)\.hidden = !credit/);
  assert.match(source, /\.rf_existing_field'\)\.hidden = !money \|\| receiver === 'payer'/);
});
