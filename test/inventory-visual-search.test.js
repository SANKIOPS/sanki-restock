const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {cleanMatches} = require('../modules/inventory-visual-search');
test('visual search excludes invented IDs and duplicate matches',()=>{
  assert.deepEqual(cleanMatches([{id:2,confidence:'high',reason:'Same print'},{id:999,confidence:'high'},{id:2},{id:'3'},{id:3,confidence:'invented'}],new Set([2,3])),[{id:2,confidence:'high',reason:'Same print'},{id:3,confidence:'low',reason:''}]);
});
test('inventory photo search provides camera, upload and expandable SKU navigation',()=>{
  const ui=fs.readFileSync('public/inventory-visual-search.js','utf8');
  assert.match(ui,/capture="environment"/);assert.match(ui,/Upload product photo/);assert.match(ui,/inventory:open-product/);
  const auth=fs.readFileSync('auth.js','utf8');assert.match(auth,/image-search', roles: \['admin', 'inventory', 'warehouse', 'sales', 'stocksearch'\]/);
});
