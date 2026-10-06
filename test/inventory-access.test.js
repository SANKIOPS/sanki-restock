'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const source = file => fs.readFileSync(path.join(__dirname, '../public', file), 'utf8');
async function accessFor(user, ok = true) {
  const elements = Object.fromEntries(['careShortcut','moveShortcut','categorizationPanel'].map(id => [id,{hidden:true}]));
  const window = {};
  vm.runInNewContext(source('inventory-access.js'), {
    window, document: { getElementById: id => elements[id] },
    fetch: async () => ({ok,json:async()=>user})
  });
  const access = await window.sankiInventoryAccess;
  await Promise.resolve();
  return {access,elements};
}
test('Stylist and sales dashboard controls exclude cleaning, movements and editing', async () => {
  for (const role of ['stocksearch','sales']) {
    const {access,elements} = await accessFor({success:true,roles:[role],allowedPages:['/inventory.html']});
    assert.deepEqual(JSON.parse(JSON.stringify(access)),{care:false,movements:false,costs:false,categorization:false});
    assert.ok(Object.values(elements).every(el=>el.hidden));
  }
});
test('Inventory access keeps cleaning visible only with page permission and retains existing admin controls', async () => {
  const inventory = await accessFor({success:true,roles:['inventory'],allowedPages:['/inventory.html','/inventory-care.html']});
  assert.equal(inventory.elements.careShortcut.hidden,false);
  assert.equal(inventory.elements.moveShortcut.hidden,false);
  assert.equal(inventory.elements.categorizationPanel.hidden,true);
  assert.equal(inventory.access.costs,true);
  const removed = await accessFor({success:true,roles:['inventory'],allowedPages:['/inventory.html']});
  assert.equal(removed.elements.careShortcut.hidden,true);
  const admin = await accessFor({success:true,roles:['admin'],allowedPages:'*'});
  assert.ok(Object.values(admin.elements).every(el=>!el.hidden));
});
test('Failed permission lookup stays closed; view-only pages never start operational requests', async () => {
  const failed = await accessFor({},false);
  assert.ok(Object.values(failed.elements).every(el=>el.hidden));
  for (const file of ['inventory-movements.js','inventory-shopify.js']) {
    vm.runInNewContext(source(file), {
      window:{sankiInventoryAccess:Promise.resolve(failed.access)},
      document:{getElementById(){assert.fail('Operational UI booted for view-only user');}},
      fetch(){assert.fail('Operational request sent for view-only user');}
    });
    await Promise.resolve();
  }
});
