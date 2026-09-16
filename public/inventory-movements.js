(function () {
  'use strict';
  var cards = document.getElementById('cards'); if (!cards) return;
  var host = document.createElement('section'); host.className = 'moves'; host.id = 'stock-movements';
  host.innerHTML = '<header><h2>Stock movements</h2><nav aria-label="Stock movement views"><button data-view="move" aria-selected="true">Move stock</button><button data-view="pending" aria-selected="false">Pending approval</button><button data-view="history" aria-selected="false">History</button></nav></header><div class="move-body"><p class="move-message" id="move-message" role="status" aria-live="polite">Checking counted inventory…</p><form id="move-form"><div class="move-grid"><label><span>SKU · scan or type</span><input id="move-sku" list="move-skus" autocomplete="off" required placeholder="Scan barcode or enter SKU"><datalist id="move-skus"></datalist></label><label><span>From location</span><select id="move-from"><option>Warehouse</option><option>Display</option></select></label><label><span>From rack</span><select id="move-from-rack"></select></label><label><span>Pieces</span><input id="move-quantity" type="number" min="1" step="1" value="1" required></label><label><span>To location</span><select id="move-to"><option>Display</option><option>Warehouse</option></select></label><label><span>To rack</span><select id="move-to-rack"></select></label></div><div class="move-position" id="move-position">Select a SKU to see its counted locations and racks.</div><div class="move-foot"><small class="sub">Submit only after physically moving the pieces. Shopify updates after approval.</small><button class="primary" id="move-submit" disabled>Submit movement</button></div></form><div class="move-list" id="move-list" hidden></div></div>';
  cards.insertAdjacentElement('afterend', host);
  var state, view = 'move', requestId;
  function el(id) { return document.getElementById(id); }
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];}); }
  function sku() { return el('move-sku').value.trim().toUpperCase(); }
  function message(s) { el('move-message').textContent = s; }
  function options(id, racks) { el(id).innerHTML = '<option value="">Rack unassigned</option>' + racks.map(function(r){return '<option>'+esc(r)+'</option>';}).join(''); }
  function racks() {
    if (!state) return;
    var rows = state.positions.filter(function(p){return p.sku === sku() && p.quantity > 0;});
    options('move-from-rack', Array.from(new Set(rows.filter(function(p){return p.location === el('move-from').value;}).map(function(p){return p.rack;}).filter(Boolean))).sort());
    options('move-to-rack', (state.baseline && state.baseline.racks && state.baseline.racks[el('move-to').value]) || []);
    el('move-position').textContent = rows.length ? rows.map(function(p){return p.location+' · '+(p.rack || 'Rack unassigned')+' · '+p.quantity+' pieces';}).join('\n') : 'No counted stock found for this SKU.';
    if (state.movements.some(function(m){return m.sku === sku() && m.status !== 'approved';})) el('move-position').textContent += '\nReported positions include movements awaiting approval.';
  }
  async function api(url, body) {
    var r = await fetch(url, body ? {method:'POST', headers:{'Content-Type':'application/json'},body:JSON.stringify(body)} : {});
    var d = await r.json(); if (!r.ok || !d.success) throw Error(d.error || 'Stock movement request failed.'); return d;
  }
  function render() {
    el('move-form').hidden = view !== 'move'; el('move-list').hidden = view === 'move';
    host.querySelectorAll('[data-view]').forEach(function(b){b.setAttribute('aria-selected', String(b.dataset.view === view));});
    if (!state) return;
    el('move-submit').disabled = !state.ready;
    if (view === 'move') { racks(); return; }
    var rows = state.movements.filter(function(m){return view === 'history' ? m.status === 'approved' : m.status !== 'approved';});
    el('move-list').innerHTML = rows.map(function(m){var label = m.status === 'pending' ? 'Awaiting approval' : m.status === 'sync_pending' ? 'Shopify sync pending' : m.status === 'approved' ? 'Approved' : 'Physical correction required';return '<article class="move-row"><div><b>'+esc(m.sku)+'</b><small>'+m.quantity+' pieces · '+esc(m.submittedBy)+'</small></div><div>'+esc(m.from.location)+' / '+esc(m.from.rack || 'Unassigned')+' → '+esc(m.to.location)+' / '+esc(m.to.rack || 'Unassigned')+'<small>'+esc(new Date(m.submittedAt).toLocaleString())+'</small></div><div class="move-pending">'+label+'<small>'+esc(m.syncError || m.reviewNote || '')+'</small></div>'+(state.canApprove && m.status !== 'approved' && m.status !== 'correction_required' ? '<div class="review"><button data-id="'+esc(m.id)+'" data-action="approve">'+(m.status === 'sync_pending' ? 'Retry sync' : 'Approve')+'</button><button data-id="'+esc(m.id)+'" data-action="correction">Correction</button></div>' : '<small>'+esc(m.reviewedBy || '')+'</small>')+'</article>';}).join('') || '<p class="sub">No '+(view === 'history' ? 'approved movements' : 'movements awaiting approval')+'.</p>';
  }
  async function refresh() {
    state = await api('/api/stock-movements');
    el('move-skus').innerHTML = Array.from(new Set(state.positions.map(function(p){return p.sku;}))).sort().map(function(s){return '<option value="'+esc(s)+'">';}).join('');
    message(state.ready ? 'Reported physical position updates immediately. Manager approval confirms the Shopify transfer.' : 'Moves are locked until the new physical count and Shopify SKU mappings are reconciled. No old stock quantities will be used.');
    render();
  }
  host.querySelectorAll('[data-view]').forEach(function(b){b.onclick = function(){view = b.dataset.view;render();};});
  ['move-sku','move-from','move-to'].forEach(function(id){el(id).addEventListener('change',racks);});
  // USB/Bluetooth scanners enter the SKU exactly like a keyboard. Do not
  // interpret scanner Enter as a physical transfer confirmation.
  el('move-sku').addEventListener('keydown', function(e){if(e.key === 'Enter'){e.preventDefault();racks();el('move-quantity').focus();}});
  el('move-form').addEventListener('input', function(){requestId = null;});
  el('move-form').addEventListener('change', function(){requestId = null;});
  el('move-form').onsubmit = async function(e){
    e.preventDefault(); if (!state || !state.ready) return;
    requestId = requestId || crypto.randomUUID(); el('move-submit').disabled = true;
    try { await api('/api/stock-movements', {requestId:requestId,sku:sku(),quantity:Number(el('move-quantity').value),from:{location:el('move-from').value,rack:el('move-from-rack').value},to:{location:el('move-to').value,rack:el('move-to-rack').value}}); requestId = null;await refresh();message('Movement recorded. New physical position is shown; manager approval is pending.'); }
    catch(err){message(err.message);el('move-submit').disabled = !state.ready;}
  };
  el('move-list').onclick = async function(e){
    var b = e.target.closest('button[data-id]'); if (!b) return;
    var reason = b.dataset.action === 'correction' ? prompt('Describe the physical correction required. Stock will NOT automatically move back.') : '';
    if (b.dataset.action === 'correction' && !reason) return;
    b.disabled = true;
    try { await api('/api/stock-movements/'+encodeURIComponent(b.dataset.id)+'/review',{action:b.dataset.action,reason:reason});await refresh(); }
    catch(err){await refresh().catch(function(){});message(err.message);b.disabled = false;}
  };
  refresh().catch(function(err){message(err.message);});
})();
