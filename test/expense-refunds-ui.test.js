'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../public/expense-refunds.js'), 'utf8');
test('refund dialog overrides the shared narrow dialog maximum width', () => {
  assert.match(source, /#rf_dialog\{width:min\(960px,95vw\);max-width:95vw;/);
});
test('refund mode hidden fields override shared flex-field display rules', () => {
  assert.match(source, /#rf_dialog \[hidden\]\{display:none!important\}/);
  assert.match(source, /\.rf_issuer_field'\)\.hidden = !credit/);
  assert.match(source, /\.rf_existing_field'\)\.hidden = !money \|\| receiver === 'payer'/);
});
