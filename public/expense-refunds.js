/* Linked expense refunds. The server validates every allocation again at save. */
(function () {
  'use strict';
  var data, draft, preview, filter = { nature: 'SANKI', from: '', to: '', status: '', search: '' }, busy = false, revision = 0, labelled = 0, frozen = [];
  var labels = { cash: 'Cash', upi: 'UPI', bank: 'Bank transfer', card: 'Credit-card reversal', voucher: 'Voucher', store_credit: 'Store credit', vendor_credit: 'Vendor credit', credit_note: 'Credit note — unpaid bill' };
  var endpoint = '/api/expenses/expense-refunds';
  function node(id) { return document.getElementById(id); }
  function options(values, selected) { return values.map(function (v) { var key = typeof v === 'string' ? v : v.value, label = typeof v === 'string' ? v : v.label; return '<option value="' + esc(key) + '"' + (key === selected ? ' selected' : '') + '>' + esc(label) + '</option>'; }).join(''); }
  function post(path, body) { return api(endpoint + path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); }
  function amount(n) { return fmtBank(n); }
  function message(text, ok) { node('rf_message').className = 'msg ' + (ok ? 'ok' : 'err'); node('rf_message').textContent = text; }
  function uid() { return window.crypto.randomUUID ? window.crypto.randomUUID() : 'refund-' + Date.now() + '-' + Math.random().toString(36).slice(2); }
  function labelFields(container) { container.querySelectorAll('.fld').forEach(function (field) { var label = field.querySelector('label'), control = field.querySelector('input,select,textarea'); if (!label || !control) return; if (!control.id) control.id = 'rf_field_' + (++labelled); label.htmlFor = control.id; }); }
  function lockForm(locked) {
    busy = locked;
    if (locked) { frozen = Array.from(node('rf_dialog').querySelectorAll('input,select,textarea,button')).map(function (n) { var prior = n.disabled; n.disabled = true; return [n, prior]; }); }
    else { frozen.forEach(function (entry) { entry[0].disabled = entry[1]; }); frozen = []; }
  }
  function load() {
    var params = new URLSearchParams(filter);
    return api(endpoint + '?' + params).then(function (result) {
      if (!result.success) throw new Error(result.error);
      data = result; return result;
    });
  }
  window.renderRefunds = function () {
    if (!cfg.canManageExpenseRefunds) { el('panel').textContent = 'Only Admin or Owner can manage expense refunds.'; return; }
    el('panel').innerHTML = '<div class="card">Loading Refunds &amp; Returns…</div>';
    load().then(draw).catch(function (error) { el('panel').textContent = error.message; });
  };
  function draw() {
    if (tab !== 'refunds') return;
    var s = data.summary;
    el('panel').innerHTML = '<div class="card"><div class="row-actions"><h2 style="margin:0;flex:1">Refunds &amp; Returns</h2><button class="btn primary" id="rf_log">＋ Log refund / return</button></div><p class="hint">Linked to the original expense or payment. Money, non-cash credits and unpaid-bill credit notes are recorded separately. Proof is optional; a reason is required.</p><div class="filters"><div class="fld"><label>Entity</label><select id="rf_entity">' + options(cfg.approvalNatures, filter.nature) + '</select></div><div class="fld"><label>From</label><input type="date" id="rf_from" value="' + esc(filter.from) + '"></div><div class="fld"><label>To</label><input type="date" id="rf_to" value="' + esc(filter.to) + '"></div><div class="fld"><label>Status</label><select id="rf_status">' + options([{ value: '', label: 'All' }, { value: 'received', label: 'Received' }, { value: 'pending', label: 'Refund pending' }, { value: 'completed', label: 'Pending return completed' }, { value: 'voided', label: 'Voided (audit history)' }], filter.status) + '</select></div><div class="fld"><label>Search vendor / reference</label><input id="rf_search" value="' + esc(filter.search) + '"></div><button class="btn ghost" id="rf_filter">Filter</button></div></div>' +
      '<div class="stats">' + [['Money received by company', s.moneyReceived], ['Credit available (non-cash)', s.creditsAvailable], ['Refund still pending', s.pending], ['Unpaid-bill credit notes', s.creditNotes], ['Refunds to original payer', s.personalReceived]].map(function (x) { return '<div class="stat"><div class="k">' + x[0] + '</div><div class="v">' + amount(x[1]) + '</div></div>'; }).join('') + '</div><p class="hint">Available credit is the remaining value of credits issued in the selected period. Expired credit: ' + amount(s.creditsExpired) + '. Vouchers never increase cash/bank balances.</p>' +
      '<div class="card" style="overflow:auto"><table><thead><tr><th>Date / refund reference</th><th>Expense / payment</th><th>Vendor</th><th>Amount</th><th>Mode / received into</th><th>Status / history</th></tr></thead><tbody>' + (data.refunds.length ? data.refunds.map(recordRow).join('') : '<tr><td colspan="6" class="muted">No refunds or returns in this view.</td></tr>') + '</tbody></table></div>' + recoverableView() + '<div class="msg" id="rf_page_message" role="status"></div>';
    node('rf_log').onclick = function () { openForm(); };
    node('rf_filter').onclick = function () { filter = { nature: node('rf_entity').value, from: node('rf_from').value, to: node('rf_to').value, status: node('rf_status').value, search: node('rf_search').value }; renderRefunds(); };
    el('panel').onclick = function (event) {
      var b = event.target.closest('[data-rf-action]'); if (!b) return;
      var record = data.refunds.find(function (r) { return r.id === b.dataset.id; });
      if (b.dataset.rfAction === 'receive') return openForm(record);
      if (b.dataset.rfAction === 'void') return correction('/' + encodeURIComponent(record.id) + '/void', 'Reason for reversing this refund/return:');
      if (b.dataset.rfAction === 'redeem') return redemption(record, b.dataset.component);
      if (b.dataset.rfAction === 'undo-use') return correction('/' + encodeURIComponent(record.id) + '/redemptions/' + encodeURIComponent(b.dataset.use) + '/void', 'Reason for reversing this credit redemption:');
      if (b.dataset.rfAction === 'recover') return recovery(b.dataset.expense);
      if (b.dataset.rfAction === 'undo-recovery') return correction('/recoveries/' + encodeURIComponent(b.dataset.id) + '/void', 'Reason for reversing this employee recovery:');
    };
  }
  function action(label, kind, id, extras) { return '<button class="btn mini ghost" data-rf-action="' + kind + '" data-id="' + esc(id) + '" ' + (extras || '') + '>' + label + '</button>'; }
  function recordRow(r) {
    var components = r.components.map(function (c) { return '<div>' + esc(labels[c.mode]) + ' · ' + amount(c.amount) + '<br><span class="muted">' + esc(c.account || c.issuer || 'Bill adjustment') + (c.receiver === 'payer' ? ' · original payer' : '') + (c.reference ? ' · ' + esc(c.reference) : '') + '</span></div>'; }).join('') || 'Awaiting receipt — no ledger posting';
    var history = '<p>' + esc(r.reasonType.replaceAll('_', ' ')) + ' · ' + esc(r.reason) + '</p><p class="hint">Logged by ' + esc(r.createdBy) + ' · ' + esc(r.createdAt) + (r.parentId ? ' · pending return ' + esc(r.parentId) : '') + '</p>';
    history += proofGallery(r.proofs || [], 'Refund');
    r.components.forEach(function (c) {
      if (c.externalMovementId) history += '<p class="hint">Linked existing receipt ' + esc(c.externalMovementId) + ' — no duplicate account movement.</p>';
      if (!c.vendorAdvanceId) return;
      history += '<p><b>' + esc(labels[c.mode]) + '</b> · remaining ' + amount(c.remainingCredit) + (c.expiryDate ? ' · expires ' + esc(c.expiryDate) : '') + (c.expired ? ' · EXPIRED — redemption blocked' : '') + '</p>';
      if (r.status === 'received' && c.remainingCredit > 0 && !c.expired) history += action('Redeem credit', 'redeem', r.id, 'data-component="' + esc(c.id) + '"');
      (c.applications || []).forEach(function (use) {
        history += '<p>' + esc(use.date) + ' · ' + esc(use.expenseId) + ' · ' + amount(use.amount) + ' · ' + esc(use.reason) + (use.accountingExcluded ? ' · REVERSED: ' + esc(use.voidReason) : action('Undo redemption', 'undo-use', r.id, 'data-use="' + esc(use.id) + '"')) + '</p>';
      });
    });
    if (r.status === 'voided') history += '<p>Voided by ' + esc(r.voidedBy) + ' · ' + esc(r.voidReason) + '</p>';
    else { if (r.displayStatus === 'pending') history += action('Record received refund', 'receive', r.id); history += action('Reverse with reason', 'void', r.id); }
    return '<tr><td>' + esc(r.date) + '<br><b>' + esc(r.id) + '</b></td><td>' + r.sources.map(function (source) { return esc(source.reference) + ' · ' + amount(source.amount); }).join('<br>') + '</td><td>' + esc(r.vendor) + '<br><span class="muted">' + esc(r.nature) + '</span></td><td class="r">' + amount(r.amount) + (r.pendingAmount ? '<br><small>' + amount(r.pendingAmount) + ' pending</small>' : '') + '</td><td>' + components + '</td><td><details><summary><b>' + esc(r.displayStatus.replaceAll('_', ' ')) + '</b> · Details</summary>' + history + '</details></td></tr>';
  }
  function recoverableView() {
    var rows = data.recoverables.map(function (r) { return '<tr><td>' + esc(r.claimant) + '</td><td>' + esc(r.expenseId) + '</td><td>' + amount(r.amount) + '</td><td>' + action('Record recovery', 'recover', '', 'data-expense="' + esc(r.expenseId) + '"') + '</td></tr>'; }).join('');
    var history = (data.recoveries || []).map(function (r) { return '<p>' + esc(r.date) + ' · ' + esc(r.source) + ' · ' + amount(r.amount) + ' · ' + esc(r.account) + (r.accountingExcluded ? ' · REVERSED' : action('Reverse recovery', 'undo-recovery', r.id)) + '</p>'; }).join('');
    return rows || history ? '<div class="card"><h3>Employee recoveries</h3><p class="hint">An employee received a vendor refund after being reimbursed. This is money due back to the company, not another expense. No company receipt is posted until the recovery is received.</p><table><thead><tr><th>Person</th><th>Expense</th><th>Still due</th><th>Action</th></tr></thead><tbody>' + rows + '</tbody></table>' + history + '</div>' : '';
  }
  function modal() {
    if (node('rf_dialog')) return;
    var style = document.createElement('style'); style.textContent = '#rf_dialog{width:min(960px,95vw);max-width:95vw;max-height:90vh;overflow:auto;border:1px solid #d9e0e8;border-radius:14px;padding:22px;color:#172536}#rf_dialog [hidden]{display:none!important}#rf_dialog::backdrop{background:#152f4866}#rf_dialog .rf-allocation{display:grid;grid-template-columns:minmax(210px,1fr) 140px auto;gap:10px;align-items:end;margin:10px 0}#rf_dialog .rf-component{border:1px solid #dce4eb;border-radius:10px;padding:14px;margin:12px 0}#rf_dialog .rf-component .grid{grid-template-columns:repeat(2,minmax(0,1fr))}#rf_dialog select,#rf_dialog input,#rf_dialog textarea{max-width:100%;box-sizing:border-box}#rf_dialog .rf-preview{padding:14px;background:#f0f8f5;border:1px solid #9bd1ba;border-radius:10px;margin-top:16px}#rf_dialog .hint{line-height:1.5}@media(max-width:600px){#rf_dialog{padding:14px}#rf_dialog .rf-allocation,#rf_dialog .rf-component .grid{grid-template-columns:1fr}}'; document.head.appendChild(style);
    var dialog = document.createElement('dialog'); dialog.id = 'rf_dialog'; document.body.appendChild(dialog);
    dialog.addEventListener('cancel', function (event) { if (busy) event.preventDefault(); });
  }
  function openForm(parent) {
    modal(); busy = false; preview = null; revision++;
    draft = { requestId: uid(), parentId: parent && parent.id || '', sources: [], status: 'received', date: data.today, reasonType: 'return', reason: parent && parent.reason || '', proofs: [] };
    if (parent) draft.sources = parent.pendingSources.filter(function (s) { return s.amount > 0; }).map(function (s) { return { key: s.key, amount: s.amount }; });
    node('rf_dialog').innerHTML = '<div class="row-actions"><h2 style="margin:0;flex:1">' + (parent ? 'Record receipt for ' + esc(parent.id) : 'Log refund / return') + '</h2><button class="btn ghost" id="rf_close">Close</button></div><p class="hint">Expense refunds received from a supplier — not refunds paid to customers. Original bills and payment history remain unchanged.</p><div class="grid"><div class="fld"><label>Status</label><select id="rf_form_status">' + options([{ value: 'received', label: 'Refund / credit received' }, { value: 'pending', label: 'Return logged — refund pending' }], 'received') + '</select></div><div class="fld"><label>Actual receipt / return date</label><input id="rf_date" type="date" value="' + draft.date + '" max="' + data.today + '"></div><div class="fld"><label>Reason type</label><select id="rf_reason_type">' + options(data.reasons.map(function (r) { return { value: r, label: r.replaceAll('_', ' ') }; }), 'return') + '</select></div></div><h3>1. Link original expense or payment</h3><div class="filters"><div class="fld" style="flex:1"><label>Find by vendor, expense or payment reference</label><input id="rf_source_search" placeholder="e.g. EX-00300 or supplier name"></div><div class="fld"><label>Source type</label><select id="rf_source_kind">' + options(['expense', 'payment', 'advance'], 'expense') + '</select></div></div><div class="filters"><div class="fld" style="flex:1"><label>Original source</label><select id="rf_source"></select></div><button class="btn ghost" id="rf_add_source">Add source</button></div><p class="hint">For a consolidated payment, select the separate bill allocations. Select an unallocated advance for an overpayment refund; it will not reduce expenses.</p><div id="rf_allocations"></div><h3>2. Amount and mode received</h3><p class="hint">“Full” means the remaining refundable amount, after earlier refunds. For a credit note it means the remaining unpaid bill. Split receipts can have multiple modes.</p><div id="rf_components"></div><button class="btn ghost" id="rf_add_component">＋ Add another mode</button><div class="fld" style="margin-top:16px"><label>Reason / description — required</label><textarea id="rf_reason" rows="2">' + esc(draft.reason) + '</textarea></div><div class="fld"><label>Refund proof — optional</label><input id="rf_proofs" type="file" accept="image/*" multiple><div class="hint">No proof is required at this stage. Uploaded images are retained with the refund history.</div></div><div class="msg" id="rf_message" role="status"></div><div id="rf_preview"></div><div class="row-actions"><button class="btn primary" id="rf_preview_button">Review accounting effect</button><button class="btn green" id="rf_save" disabled>Confirm &amp; save refund</button></div>';
    if (parent) node('rf_form_status').disabled = true;
    node('rf_close').onclick = function () { if (!busy) node('rf_dialog').close(); };
    node('rf_source_search').oninput = sourceOptions; node('rf_source_kind').onchange = sourceOptions;
    node('rf_add_source').onclick = function () {
      var key = node('rf_source').value, s = data.sources.find(function (x) { return x.key === key; });
      if (!s || draft.sources.some(function (x) { return x.key === key; })) return message('Select a distinct source.', false);
      if (draft.parentId) return message('Keep the sources on the original pending return.', false);
      if (draft.sources.length && data.sources.find(function (x) { return x.key === draft.sources[0].key; }).vendor !== s.vendor) return message('Combined refunds must use the same vendor.', false);
      draft.sources.push({ key: key, amount: s.remaining || s.unpaidAmount }); allocations(); invalidate(); refreshAccounts(); var c=document.querySelectorAll('#rf_components .rf_component_amount');if(c.length===1&&!c[0].value)c[0].value=draft.sources.reduce(function(n,a){return n+a.amount;},0);
    };
    node('rf_add_component').onclick = function () { addComponent(); invalidate(); };
    node('rf_form_status').onchange = function () { var pending = this.value === 'pending'; node('rf_components').hidden = pending; node('rf_add_component').hidden = pending; invalidate(); };
    node('rf_proofs').onchange = function () { draft.proofs = []; invalidate(); };
    
    node('rf_dialog').oninput = invalidate; node('rf_dialog').onchange = invalidate;
    node('rf_preview_button').onclick = doPreview; node('rf_save').onclick = save;
    sourceOptions(); allocations(); addComponent(); labelFields(node('rf_dialog')); node('rf_dialog').showModal();
  }
  function invalidate() { revision++; preview = null; if (node('rf_save')) node('rf_save').disabled = true; if (node('rf_preview')) node('rf_preview').innerHTML = ''; }
  function sourceOptions() {
    var search = node('rf_source_search').value.toLowerCase(), kind = node('rf_source_kind').value;
    var list = data.sources.filter(function (s) { return s.nature === filter.nature && s.kind === kind && [s.reference, s.vendor, s.particulars, s.account].join(' ').toLowerCase().includes(search); });
    node('rf_source').innerHTML = options(list.slice(0, 150).map(function (s) { return { value: s.key, label: s.reference + ' · ' + s.vendor + ' · ' + s.date + ' · refundable ' + amount(s.remaining) + ' / unpaid ' + amount(s.unpaidAmount) }; }));
  }
  function allocations() {
    node('rf_allocations').innerHTML = draft.sources.map(function (a, index) {
      var s = data.sources.find(function (x) { return x.key === a.key; }); if (!s) return '';
      return '<div class="rf-allocation"><div><b>' + esc(s.reference) + ' · ' + esc(s.vendor) + '</b><div class="hint">Original bill ' + amount(s.originalAmount) + ' · paid ' + amount(s.paidAmount) + ' · earlier refunds ' + amount(s.refundedAmount) + '<br>Refundable ' + amount(s.remaining) + ' · unpaid ' + amount(s.unpaidAmount) + '</div></div><div class="fld"><label>Refund allocation ₹</label><input type="number" class="rf_allocation_amount" step="0.01" min="0.01" data-index="' + index + '" value="' + a.amount + '"></div><div><button class="btn mini ghost rf_full" data-index="' + index + '">Full</button> <button class="btn mini ghost rf_remove_source" data-index="' + index + '">Remove</button></div></div>';
    }).join('') || '<p class="muted">Add the original expense or payment first.</p>';
    node('rf_allocations').onclick = function (event) {
      var button = event.target.closest('button'); if (!button) return; var index = Number(button.dataset.index), source = data.sources.find(function (s) { return s.key === draft.sources[index].key; });
      if (button.classList.contains('rf_full')) {
        var creditNote = Array.from(document.querySelectorAll('#rf_components .rf_mode')).every(function (n) { return n.value === 'credit_note'; });
        draft.sources[index].amount = node('rf_form_status').value === 'pending' ? source.unpaidAmount + source.remaining : creditNote ? source.unpaidAmount : source.remaining;
      } else draft.sources.splice(index, 1);
      allocations(); refreshAccounts(); invalidate();
      var single = document.querySelectorAll('#rf_components .rf_component_amount'); if (single.length === 1) single[0].value = draft.sources.reduce(function (n, s) { return n + s.amount; }, 0);
    };
    labelFields(node('rf_allocations'));
  }
  function accounts(component) {
    var mode = component.querySelector('.rf_mode').value, receiver = component.querySelector('.rf_receiver').value;
    var originals = draft.sources.flatMap(function (a) { var s = data.sources.find(function (x) { return x.key === a.key; }); return s ? s.payments : []; });
    var values = receiver === 'payer' ? Array.from(new Set(originals.filter(function (p) { return p.personal; }).map(function (p) { return p.account; }))) : mode === 'card' ? (cfg.creditCards || []).map(function (c) { return c.name; }) : (cfg.accountsByNature[filter.nature] || []);
    if (mode === 'cash') values = values.filter(function (a) { return /cash/i.test(a); });
    if (['bank', 'upi'].includes(mode)) values = values.filter(function (a) { return !/cash/i.test(a); });
    var selected = component.querySelector('.rf_account').value;
    component.querySelector('.rf_account').innerHTML = options(values, selected);
    var credit = ['voucher', 'store_credit', 'vendor_credit'].includes(mode), money = ['cash', 'upi', 'bank', 'card'].includes(mode);
    component.querySelector('.rf_account_field').hidden = !money;
    component.querySelector('.rf_issuer_field').hidden = !credit; component.querySelector('.rf_expiry_field').hidden = !credit;
    component.querySelector('.rf_existing_field').hidden = !money || receiver === 'payer';
    var existing = data.existingMovements.filter(function (m) { return receiver !== 'payer' && m.nature === filter.nature && m.mode === (mode === 'upi' ? 'bank' : mode); }), priorExisting = component.querySelector('.rf_existing').value;
    component.querySelector('.rf_existing').innerHTML = options([{ value: '', label: 'New receipt — not already in the ledger' }].concat(existing.map(function (m) { return { value: m.id, label: m.id + ' · ' + m.date + ' · ' + amount(m.remaining) + ' · ' + m.account }; })), priorExisting);
  }
  function refreshAccounts() { document.querySelectorAll('#rf_components .rf-component').forEach(accounts); }
  function addComponent() {
    var div = document.createElement('div'); div.className = 'rf-component';
    div.innerHTML = '<div class="grid"><div class="fld"><label>Mode</label><select class="rf_mode">' + options(data.modes.map(function (m) { return { value: m, label: labels[m] }; }), 'bank') + '</select></div><div class="fld"><label>Amount received ₹</label><input class="rf_component_amount" type="number" step="0.01" min="0.01" value="' + (document.querySelector('#rf_components .rf-component') ? '' : draft.sources.reduce(function (n, a) { return n + a.amount; }, 0) || '') + '"></div><div class="fld"><label>Who received it?</label><select class="rf_receiver">' + options([{ value: 'company', label: 'Company' }, { value: 'payer', label: 'Original personal payer' }]) + '</select></div><div class="fld rf_account_field"><label>Receiving account</label><select class="rf_account"></select></div><div class="fld rf_issuer_field"><label>Voucher / credit issuer</label><input class="rf_issuer" placeholder="Merchant who will accept this credit"></div><div class="fld"><label>Transaction / voucher reference</label><input class="rf_reference" placeholder="Required for voucher or credit"></div><div class="fld rf_expiry_field"><label>Expiry — optional</label><input class="rf_expiry" type="date"></div><div class="fld rf_existing_field"><label>Existing refund receipt — avoid double posting</label><select class="rf_existing"></select></div></div><button class="btn mini ghost rf_remove_component" style="margin-top:10px">Remove this mode</button>';
    node('rf_components').appendChild(div); accounts(div); labelFields(div);
    div.querySelector('.rf_mode').onchange = function () { accounts(div); invalidate(); };
    div.querySelector('.rf_receiver').onchange = function () { accounts(div); invalidate(); };
    div.querySelector('.rf_existing').onchange = function () { var m = data.existingMovements.find(function (x) { return x.id === this.value; }, this); if (!m) return; div.querySelector('.rf_account').value = m.account; div.querySelector('.rf_reference').value = m.reference; div.querySelector('.rf_component_amount').value = m.remaining; node('rf_date').value = m.date; invalidate(); };
    div.querySelector('.rf_remove_component').onclick = function () { div.remove(); invalidate(); };
  }
  function body() {
    document.querySelectorAll('.rf_allocation_amount').forEach(function (n) { draft.sources[Number(n.dataset.index)].amount = Number(n.value); });
    return Object.assign({}, draft, { status: node('rf_form_status').value, date: node('rf_date').value, reasonType: node('rf_reason_type').value, reason: node('rf_reason').value.trim(),
      components: Array.from(document.querySelectorAll('#rf_components .rf-component')).map(function (c) { function value(selector) { return c.querySelector(selector).value; } return { mode: value('.rf_mode'), amount: Number(value('.rf_component_amount')), receiver: value('.rf_receiver'), account: value('.rf_account'), issuer: value('.rf_issuer'), reference: value('.rf_reference'), expiryDate: value('.rf_expiry'), externalMovementId: value('.rf_existing') }; }) });
  }
  async function doPreview() {
    if (busy) return; lockForm(true); message('Checking source allocations and accounting effects…', true);
    var rev = revision;
    try {
      if (!draft.proofs.length && node('rf_proofs').files.length) draft.proofs = await uploadFiles(node('rf_proofs').files, filter.nature);
      var payload = body(), result = await post('/preview', payload);
      if (rev !== revision) throw new Error('The form changed during review. Review it again before saving.');
      if (!result.success) throw new Error(result.error);
      preview = payload;
      var p = result.preview, pending = payload.status === 'pending';
      node('rf_preview').innerHTML = '<div class="rf-preview"><h3 style="margin-top:0">Review before confirming</h3><p><b>' + amount(p.amount || p.already && p.already.amount) + '</b> · ' + esc(payload.date) + ' · ' + esc(payload.reason) + '</p>' + (pending ? '<p>Return pending: no cash, bank, vendor balance or P&amp;L posting yet.</p>' : '<p>Bill / expense reduction: ' + amount(result.impact.billCredit) + '</p>' + payload.components.map(function (c) { return '<p>' + esc(labels[c.mode]) + ': ' + amount(c.amount) + ' → ' + esc(c.account || c.issuer || 'Unpaid bill') + (c.externalMovementId ? ' · existing receipt linked; no new account movement' : c.receiver === 'payer' ? ' · original payer; reimbursement/recovery adjusts, not company cash' : ['voucher', 'store_credit', 'vendor_credit'].includes(c.mode) ? ' · non-cash credit wallet' : c.mode === 'credit_note' ? ' · payable reduced; no receipt' : c.mode === 'card' ? ' · card outstanding reduced' : ' · company account credited') + '</p>'; }).join('')) + '<p class="hint">The original bill and payment history are preserved. Server checks run again when you confirm.</p></div>';
      node('rf_save').disabled = false; message('Review is valid. Confirm below to record it.', true);
    } catch (error) { message(error.message, false); }
    finally { lockForm(false); node('rf_save').disabled = !preview; }
  }
  async function save() {
    if (busy || !preview) return;
    lockForm(true);
    try { var result = await post('', preview); if (!result.success) throw new Error(result.error); node('rf_dialog').close(); await load(); draw(); setMsg('Recorded ' + result.refund.id + '. Original payment history retained.', true); }
    catch (error) { message(error.message, false); node('rf_save').disabled = false; }
    finally { lockForm(false); }
  }
  async function correction(path, promptText) {
    var reason = prompt(promptText); if (!reason || !reason.trim()) return;
    var result = await post(path, { reason: reason.trim() }); if (!result.success) return setMsg(result.error, false);
    await load(); draw(); setMsg('Correction saved with its audit history.', true);
  }
  function simpleForm(title, fields, path) {
    modal(); node('rf_dialog').innerHTML = '<h2>' + esc(title) + '</h2><form id="rf_simple">' + fields + '<div class="fld"><label>Date</label><input name="date" type="date" value="' + data.today + '" max="' + data.today + '" required></div><div class="fld"><label>Reason — required</label><textarea name="reason" required></textarea></div><div class="msg" id="rf_message"></div><div class="row-actions"><button class="btn green">Review &amp; confirm</button><button class="btn ghost" type="button" id="rf_simple_close">Cancel</button></div></form>';
    node('rf_simple_close').onclick = function () { if (!busy) node('rf_dialog').close(); };
    node('rf_simple').onsubmit = async function (event) {
      event.preventDefault(); if (busy) return; var payload = Object.fromEntries(new FormData(this)); payload.requestId = this.dataset.requestId || (this.dataset.requestId = uid());
      if (!confirm('Confirm ' + amount(payload.amount) + ' on ' + payload.date + '? ' + payload.reason + (path.includes('redeem') ? ' No bank/cash movement will be created.' : ' The selected company account will be credited.'))) return;
      lockForm(true);
      try { var result = await post(path, payload); if (!result.success) throw new Error(result.error); node('rf_dialog').close(); await load(); draw(); setMsg('Saved with an audit trail.', true); }
      catch (error) { message(error.message, false); } finally { lockForm(false); }
    }; busy = false; labelFields(node('rf_dialog')); node('rf_dialog').showModal();
  }
  function redemption(record, componentId) {
    var c = record.components.find(function (x) { return x.id === componentId; });
    var sources = data.sources.filter(function (s) { return s.kind === 'expense' && s.nature === record.nature && s.vendor.toLowerCase() === c.issuer.toLowerCase() && s.unpaidAmount > 0; });
    if (!sources.length) return setMsg('No approved unpaid expense with this credit issuer. Approve the bill first.', false);
    simpleForm('Redeem ' + labels[c.mode] + ' · available ' + amount(c.remainingCredit), '<input type="hidden" name="componentId" value="' + esc(c.id) + '"><div class="fld"><label>Approved expense with ' + esc(c.issuer) + '</label><select name="expenseId">' + options(sources.map(function (s) { return { value: s.expenseId, label: s.reference + ' · unpaid ' + amount(s.unpaidAmount) }; })) + '</select></div><div class="fld"><label>Credit applied ₹</label><input name="amount" type="number" min="0.01" step="0.01" max="' + c.remainingCredit + '" required></div>', '/' + encodeURIComponent(record.id) + '/redeem');
  }
  function recovery(expenseId) {
    var r = data.recoverables.find(function (x) { return x.expenseId === expenseId; });
    simpleForm('Recovery from ' + r.claimant + ' · still due ' + amount(r.amount), '<input type="hidden" name="expenseId" value="' + esc(expenseId) + '"><div class="fld"><label>Company receiving account</label><select name="account">' + options(cfg.accountsByNature[r.nature]) + '</select></div><div class="fld"><label>Mode</label><select name="mode">' + options(['bank', 'upi', 'cash']) + '</select></div><div class="fld"><label>Amount actually recovered ₹</label><input name="amount" type="number" step="0.01" min="0.01" max="' + r.amount + '" required></div><div class="fld"><label>Transaction reference — optional</label><input name="reference"></div>', '/recoveries');
  }
  window.refundExpenseHistory = function (expense) {
    if (!(expense.refundHistory || []).length) return '';
    return '<div class="expense-section"><b>Returns / refunds · ' + esc((expense.refundStatus || '').replaceAll('_', ' ')) + '</b><p>Original bill ' + amount(expense.amount) + ' · net expense ' + amount(expense.netExpenseAmount) + ' · refunds ' + amount(expense.refundedAmount) + '</p>' + expense.refundHistory.map(function (r) { return '<p>' + esc(r.date) + ' · ' + esc(r.id) + ' · ' + amount(r.amount) + ' · ' + esc(r.status) + ' · ' + esc(r.reason) + '</p>'; }).join('') + (cfg.canManageExpenseRefunds ? '<a class="btn mini ghost" href="/expenses.html?tab=refunds">Open Refunds &amp; Returns</a>' : '') + '</div>';
  };
}());
