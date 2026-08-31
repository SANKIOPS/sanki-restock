const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

test('Procurement V2 owns approval validation after legacy plan inputs change', () => {
  const scriptPath = path.join(__dirname, '..', 'public', 'fresh-procurement-v2-addon.js');
  const script = fs.readFileSync(scriptPath, 'utf8');

  assert.doesNotThrow(() => new Function(script));
  assert.match(script, /id=\"v2validation\"/);
  assert.match(script, /function syncApprovalState\(\)/);
  assert.match(script, /\['gender', 'category', 'styleTarget', 'launchDate', 'budget', 'planName'\]/);
  assert.match(script, /addEventListener\('change', syncApprovalState\)/);
  assert.match(script, /create\.disabled = submitting \|\| Boolean\(message\)/);
});
