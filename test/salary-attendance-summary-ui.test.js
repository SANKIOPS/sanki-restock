'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../public/salary.html'), 'utf8');

test('attendance grid shows literal present, absent and half-day counts beside paid days', () => {
  assert.match(html, /<th>Present days<\/th><th>Absent days<\/th><th>Half days<\/th><th>Paid days<\/th>/);
  assert.match(html, /if\(m==='P'\)presentDays\+=1/);
  assert.match(html, /else if\(m==='A'\)absentDays\+=1/);
  assert.match(html, /else if\(m==='H'\|\|m==='HD'\)halfDays\+=1/);
});

test('salary inline scripts still parse after attendance summary change', () => {
  for (const match of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))
    assert.doesNotThrow(() => new vm.Script(match[1]));
});
