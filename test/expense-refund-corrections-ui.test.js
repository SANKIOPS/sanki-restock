'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../public/expense-refunds.js'), 'utf8');
const original = { id: 'RF-SYNTHETIC', nature: 'SANKI', vendor: '<img src=x>', amount: 213, date: '2098-06-10', reason: 'Synthetic return', sources: [{ reference: 'EX-SYNTHETIC' }], components: [{ reference: 'REF-SYNTHETIC' }] };
function harness(overrides = {}) {
  const nodes = {}, requests = [], notices = [];
  function element() {
    let html = '';
    const node = { id: '', value: '', textContent: '', disabled: false, open: false, listeners: {}, controls: [],
      appendChild(child) { nodes[child.id] = child; }, addEventListener(name, fn) { this.listeners[name] = fn; },
      showModal() { this.open = true; }, close() { this.open = false; if (this.listeners.close) this.listeners.close(); },
      querySelectorAll(selector) { return selector === 'input,select,textarea,button' ? this.controls : []; } };
    Object.defineProperty(node, 'innerHTML', { get: () => html, set(markup) {
      html = String(markup); node.controls = [];
      for (const match of html.matchAll(/<(input|textarea|button|div)\b[^>]*\bid="([^"]+)"([^>]*)>/g)) {
        const control = element(), attrs = match[0]; control.id = match[2]; control.value = (attrs.match(/\bvalue="([^"]*)"/) || [])[1] || '';
        control.disabled = /\bdisabled\b/.test(attrs);
        if (match[1] === 'textarea') control.value = html.slice(match.index + attrs.length).split('</textarea>')[0];
        nodes[control.id] = control; if (match[1] !== 'div') node.controls.push(control);
      }
    } }); return node;
  }
  const cfg = { canEditLedgerTransactions: true, canManageExpenseRefunds: true, approvalNatures: ['SANKI'], ...overrides.cfg };
  let answer = overrides.answer;
  async function api(url, options) {
    requests.push({ url, options });
    if (answer) return answer(url, options);
    if (!options && url.includes('/correction-preview')) return { success: true, editable: true, baseFingerprint: 'baseline', current: { refund: original }, today: '2098-06-30' };
    if (options && options.method === 'POST') { const body = JSON.parse(options.body); return { success: true, fingerprint: 'bound-plan', before: { refund: original }, after: { refund: { ...original, amount: Number(body.amount), date: body.date, reason: body.description, components: [{ reference: body.reference }] } }, impact: { billCreditBefore: 213, billCreditAfter: Number(body.amount), receiptIds: ['RF-SYNTHETIC/PART-001'], creditIds: [] } }; }
    if (options) return { success: true, refundId: original.id };
    return { success: true, refunds: [] };
  }
  nodes.panel=element();
  const context = { cfg, tab: overrides.url ? 'receivables' : 'not-receivables', location:{search:overrides.url||''}, URLSearchParams, window: {}, document: { getElementById: id => nodes[id], createElement: element, head: element(), body: element() },
    api, esc: value => String(value == null ? '' : value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'), fmtBank: value => '₹' + Number(value).toFixed(2), setMsg: (message, success) => notices.push({ message, success }) };
  let script=source.replace(/\}\(\)\);\s*$/, 'window.correctionTest = { openCorrection }; }());');
  if(overrides.url)script=script.replace('function draw() {','function draw() { return;');
  vm.runInNewContext(script, context);
  return { nodes, requests, notices, cfg, open: () => context.window.correctionTest.openCorrection(original), async render(){context.window.renderRefundsSection('panel');await new Promise(resolve=>setImmediate(resolve));}, answer(fn) { answer = fn; } };
}

test('Manage refund deep link opens only an exact authorized source using read-only requests once', async()=>{
  const ui=harness({url:'?manageRefund=RF-SYNTHETIC&nature=SANKI',answer:async(url)=>url.includes('/correction-preview')?{success:true,editable:false,blockedReason:'Pending approval'}:{success:true,refunds:[original]}});
  await ui.render();assert.equal(ui.requests.length,2);assert.match(ui.requests[0].url,/nature=SANKI/);assert.match(ui.requests[0].url,/search=RF-SYNTHETIC/);assert.match(ui.requests[1].url,/RF-SYNTHETIC\/correction-preview/);assert.match(ui.nodes.rf_message.textContent,/Pending approval/);assert.ok(ui.requests.every(r=>!r.options));
  await ui.render();assert.equal(ui.requests.filter(r=>r.url.includes('/correction-preview')).length,1);
});

test('Manage refund deep link denies inaccessible or ambiguous targets without fallback or writes',async()=>{
  for(const options of [{cfg:{canEditLedgerTransactions:false}},{url:'?manageRefund=RF-SYNTHETIC&nature=PERSONAL'},{records:[]},{records:[original,original]},{records:[{...original,id:'RF-OTHER'}]}]){
    const ui=harness({url:'?manageRefund=RF-SYNTHETIC&nature=SANKI',...options,answer:async()=>({success:true,refunds:options.records||[original]})});await ui.render();assert.ok(ui.requests.every(r=>!r.options&&!r.url.includes('/correction-preview')));assert.equal(ui.nodes.rf_dialog,undefined);
  }
});
test('correction UI opens GET-only, shows immutable identity and requires reviewed explicit Apply', async () => {
  const ui = harness(); await ui.open();
  assert.equal(ui.requests.length, 1); assert.equal(ui.requests[0].options, undefined);
  assert.match(ui.nodes.rf_dialog.innerHTML, /Source, account, entity, mode, proof and movement IDs cannot change/);
  assert.match(ui.nodes.rf_dialog.innerHTML, /&lt;img src=x&gt;/); assert.equal(ui.nodes.rf_correct_apply.disabled, true);
  await ui.nodes.rf_correct_apply.onclick(); assert.equal(ui.requests.length, 1);
  ui.nodes.rf_correct_amount.value = '211.37'; ui.nodes.rf_correct_reason.value = 'Synthetic evidence review';
  await ui.nodes.rf_correct_review.onclick();
  assert.equal(ui.requests[1].options.method, 'POST'); assert.equal(ui.nodes.rf_correct_apply.disabled, false);
  assert.match(ui.nodes.rf_correct_preview.innerHTML, /Review exact correction/); assert.match(ui.nodes.rf_correct_preview.innerHTML, /₹213\.00/); assert.match(ui.nodes.rf_correct_preview.innerHTML, /₹211\.37/);
  assert.match(ui.nodes.rf_correct_preview.innerHTML, /No new movement/);
  await ui.nodes.rf_correct_apply.onclick();
  const patch = ui.requests.find(request => request.options && request.options.method === 'PATCH');
  assert.ok(patch); assert.equal(JSON.parse(patch.options.body).fingerprint, 'bound-plan');
  assert.equal(JSON.parse(patch.options.body).baseFingerprint, 'baseline'); assert.equal(JSON.parse(patch.options.body).reason, 'Synthetic evidence review');
  assert.equal(ui.nodes.rf_dialog.open, false); assert.match(ui.notices[0].message, /audit trail/);
});
test('changing any form input invalidates reviewed fingerprint and disables Apply', async () => {
  const ui = harness(); await ui.open(); ui.nodes.rf_correct_amount.value = '210'; ui.nodes.rf_correct_reason.value = 'Synthetic check';
  await ui.nodes.rf_correct_review.onclick(); assert.equal(ui.nodes.rf_correct_apply.disabled, false);
  ui.nodes.rf_correct_reference.value = 'changed-after-preview'; ui.nodes.rf_dialog.oninput();
  assert.equal(ui.nodes.rf_correct_apply.disabled, true); assert.equal(ui.nodes.rf_correct_preview.innerHTML, '');
  await ui.nodes.rf_correct_apply.onclick(); assert.equal(ui.requests.filter(request => request.options && request.options.method === 'PATCH').length, 0);
});
test('blocked source remains read-only and displays server safeguard reason', async () => {
  const ui = harness({ answer: async () => ({ success: true, editable: false, blockedReason: 'Awaiting Owner approval; resolve normal approval workflow.' }) });
  await ui.open(); assert.match(ui.nodes.rf_message.textContent, /Owner approval/); assert.equal(ui.nodes.rf_correct_apply, undefined); assert.equal(ui.requests.length, 1);
});
test('lack of Manage edit permission does not open the correction or request private details', async () => {
  const ui = harness({ cfg: { canEditLedgerTransactions: false } }); await ui.open(); assert.equal(ui.requests.length, 0); assert.equal(ui.nodes.rf_dialog, undefined);
});
test('stale/rejected Apply retains dialog but clears the reviewed plan and cannot retry blindly', async () => {
  const ui = harness(); await ui.open(); ui.nodes.rf_correct_reason.value = 'Synthetic check'; await ui.nodes.rf_correct_review.onclick();
  ui.answer(async () => ({ success: false, error: 'Owner approval was requested after preview.' })); await ui.nodes.rf_correct_apply.onclick();
  assert.equal(ui.nodes.rf_dialog.open, true); assert.equal(ui.nodes.rf_correct_apply.disabled, true); assert.match(ui.nodes.rf_message.textContent, /Owner approval/);
  const count = ui.requests.length; await ui.nodes.rf_correct_apply.onclick(); assert.equal(ui.requests.length, count);
});
