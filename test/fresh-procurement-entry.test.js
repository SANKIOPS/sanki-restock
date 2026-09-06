const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

test('Fresh Procurement has one detailed entry point', () => {
  const registry = fs.readFileSync(path.join(__dirname, '..', 'modules', 'module-registry.js'), 'utf8');
  const simple = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement-simple.html'), 'utf8');
  const v2 = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement-v2.html'), 'utf8');
  const source = fs.readFileSync(path.join(__dirname, '..', 'modules', 'casuals.js'), 'utf8');

  assert.match(registry, /key: 'fresh-proc'[\s\S]*href: '\/fresh-procurement\.html'/);
  assert.match(simple, /location\.replace\('\/fresh-procurement\.html'\)/);
  assert.match(v2, /location\.replace\('\/fresh-procurement\.html'\)/);
  assert.doesNotMatch(simple, /Approve AI plan/);
  assert.doesNotMatch(simple, /Open legacy Fresh Procurement/);
  assert.doesNotMatch(source, /\/api\/casuals\/simple-(?:plan|recommend)/);
});
