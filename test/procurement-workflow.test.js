const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { parseSerial, nextSerial, buildSku, canManagePurchases } = require('../modules/procurement');

test('purchase SKU serials roll from Z999 to AA1 without punctuation', () => {
  assert.deepEqual(nextSerial({ alpha: 'Z', num: 999 }), { alpha: 'AA', num: 1 });
  assert.deepEqual(nextSerial({ alpha: 'AA', num: 999 }), { alpha: 'AB', num: 1 });
  assert.deepEqual(parseSerial('SA111AA134'), { alpha: 'AA', num: 1 });
  assert.equal(
    buildSku({ brand: 'SA', products: { Trouser: 11 }, colours: { Black: 1 }, sizes: {} }, 'Trouser', 'Black', '34', { alpha: 'AA', num: 1 }).sku,
    'SA111AA134'
  );
});

test('owner and procurement roles receive the full Purchases workflow in the UI', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  assert.match(html, /userRoles\.indexOf\('owner'\)>=0/);
  assert.match(html, /userRoles\.indexOf\('procurement'\)>=0/);
  assert.match(html, /userRoles\.indexOf\('inventory'\)>=0/);
  assert.match(html, /if\(me\.canManage && !posted\)/);
  assert.match(html, /Final preview/);
  assert.match(html, /Confirm &amp; post to Shopify/);
});

test('Nida-style Inventory users can call the complete Purchases workflow', () => {
  assert.equal(canManagePurchases({ user: { role: 'inventory', roles: ['inventory'] } }), true);
  assert.equal(canManagePurchases({ user: { role: 'inventory', roles: ['inventory', 'procurement'] } }), true);
  assert.equal(canManagePurchases({ user: { role: 'owner', roles: ['owner'] } }), true);
  assert.equal(canManagePurchases({ user: { role: 'sales', roles: ['sales'] } }), false);
});

test('Purchases Summary has a category-first PO explorer plus the complete history', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  assert.match(html, /Purchase history/);
  assert.match(html, /fetch\('\/api\/procurement\/history'\)/);
  assert.match(html, /data-history-po/);
  assert.match(html, /historyBody/);
  assert.match(html, /Ordered<\/span>/);
  assert.match(html, /Received<\/span>/);
  assert.match(html, /Posted to Shopify/);
  assert.match(html, /Recovered from Shopify/);
  assert.match(html, /original PO unavailable/);
  assert.match(html, /Explore purchases by category/);
  assert.match(html, /id="historyCategory"/);
  assert.match(html, /data-history-scope="all"/);
  assert.match(html, /data-history-scope="choose"/);
  assert.match(html, /data-history-pick/);
  assert.match(html, /All matching POs/);
  assert.match(html, /historyCategoryRows/);
  assert.match(html, /Matching purchase details/);
  assert.match(html, /data-hx-po/);
  assert.match(html, /data-hx-design/);
  assert.match(html, /Click to enlarge/);
  assert.match(html, /Category<\/th><th>Colour<\/th><th>Size/);
  assert.match(html, /historyStatus\(po\)/);
  assert.doesNotMatch(html, /data-history-select/);
  assert.match(html, /historyDesignKey\(po,line,category\)/);
  assert.doesNotMatch(html.match(/function historyDesignKey[\s\S]*?\n    \}/)[0], /colour/);
});

test('Audit Purchases has a strict on-the-way category and vendor explorer', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  assert.match(html, /What is on the way\?/);
  assert.match(html, /data-owview="category"/);
  assert.match(html, /data-owview="vendor"/);
  assert.match(html, /if\(po\.status!==['"]advance['"]\) return/);
  assert.match(html, /All categories/);
  assert.match(html, /All fits/);
  assert.match(html, /All vendors/);
  assert.match(html, /All colours/);
  assert.match(html, /All sizes/);
  assert.match(html, /Arriving by/);
  assert.match(html, /data-owpo/);
  assert.match(html, /title="Click to enlarge"/);
  // Colourways and size rows are not separate designs. The identity stays
  // vendor + design code/name + category, while colour remains filterable.
  assert.match(html, /designKey:\[vendor,code\|\|name,category\]/);
  assert.doesNotMatch(html, /designKey:\[vendor,code\|\|name,category,owText\(l\.colour\)\]/);
});

test('audience and fit can be corrected during purchase audit', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  const js = fs.readFileSync(path.join(__dirname, '..', 'modules', 'procurement.js'), 'utf8');
  assert.match(html, /edSelect\(l,'audience'/);
  assert.match(html, /edSelect\(l,'fit'/);
  assert.match(js, /ORDERED_FIELDS = \[[^\]]*'audience'/);
  assert.match(js, /LINE_EDIT_FIELDS = \[[^\]]*'audience'/);
});
