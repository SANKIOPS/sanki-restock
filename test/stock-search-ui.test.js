'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

test('Stock Search is live under Sales with SKU, rack, upload and camera search', () => {
  const registry = fs.readFileSync(path.join(__dirname, '..', 'modules', 'module-registry.js'), 'utf8');
  const page = fs.readFileSync(path.join(__dirname, '..', 'public', 'rack-locations.html'), 'utf8');

  assert.match(registry, /key: 'stock-search'[\s\S]*section: 'Sales'[\s\S]*status: 'live'/);
  assert.match(page, /id="q"[^>]*placeholder="Search SKU/);
  assert.match(page, /id="uploadBtn"/);
  assert.match(page, /id="cameraBtn"/);
  assert.match(page, /fetch\('\/api\/identify-product'/);
  assert.match(page, /qEl\.value = sku \|\| product\.name/);
  assert.match(page, /<th>Rack<\/th>/);
});
