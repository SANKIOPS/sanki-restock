(function () {
  'use strict';
  var cards = document.getElementById('cards'); if (!cards) return;
  var host = document.createElement('section'); host.className = 'moves'; host.id = 'stock-movements';
  host.innerHTML = '<header><h2>Stock movements</h2><nav aria-label="Stock movement views"><button data-view="move" aria-selected="true">Move stock</button><button data-view="pending" aria-selected="false">Pending approval</button><button data-view="history" aria-selected="false">History</button></nav></header><div class="move-body"><p class="move-message" id="move-message" role="status" aria-live="polite">Checking counted inventory…</p><form id="move-form"><div class="move-grid"><label><span>SKU · scan or type</span><input id="move-sku" list="move-skus" autocomplete="off" required placeholder="Scan barcode or enter SKU"><datalist id="move-skus"></datalist></label><label><span>From location</span><select id="move-from"><option>Warehouse</option><option>Display</option></select></label><label><span>From rack</span><select id="move-from-rack"></select></label><label><span>Pieces</span><input id="move-quantity" type="number" min="1" step="1" value="1" required></label><label><span>To location</span><select id="move-to"><option>Display</option><option>Warehouse</option></select></label><label><span>To rack</span><select id="move-to-rack"></select></label></div><div class="move-position" id="move-position">Select a SKU to see its counted locations and racks.</div><div class="move-foot"><small class="sub">Submit only after physically moving the pieces. Shopify updates after approval.</small><button class="primary" id="move-submit" disabled>Submit movement</button></div></form><div class="move-list" id="move-list" hidden></div></div>';
  cards.insertAdjacentElement('afterend', host);
  var state, view = 'move', requestId, busy = false, cancelId;
  var cancelForm=document.createElement('form');cancelForm.id='move-cancel-form';cancelForm.hidden=true;
  cancelForm.innerHTML='<label>Reason for cancelling this request<input id="move-cancel-reason" required maxlength="500" autocomplete="off"></label><p class="sub">Shopify quantities will not change. If pieces were already physically moved, do not move them again for the replacement request.</p><div class="review"><button class="primary" type="submit">Confirm cancellation</button><button type="button" id="move-keep-request">Keep request</button></div>';
  el('move-message').insertAdjacentElement('afterend',cancelForm);
  var confirmation=document.createElement('label');confirmation.id='move-confirm-label';confirmation.hidden=true;
  confirmation.innerHTML='<input id="move-confirm" type="checkbox"> I physically moved these pieces and checked the selected source and destination racks.';
  confirmation.style.cssText='display:block;margin:12px 0;font-size:12px';el('move-position').insertAdjacentElement('afterend',confirmation);
  el('move-confirm').style.cssText='width:auto;min-height:0;margin-right:7px';
  function el(id) { return document.getElementById(id); }
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];}); }
  function sku() { return el('move-sku').value.trim().toUpperCase(); }
  function message(s) { el('move-message').textContent = s; }
  function active(m) { return !['approved','cancelled'].includes(m.status); }
  function options(id, racks) { var old=el(id).value; el(id).innerHTML = '<option value="">Rack unassigned</option>' + racks.map(function(r){return '<option>'+esc(r)+'</option>';}).join('');if(racks.includes(old))el(id).value=old; }
  function updateSubmit() {
    var rows=state?state.positions.filter(function(p){return p.sku===sku();}):[],q=Number(el('move-quantity').value);
    var source=rows.find(function(p){return p.location===el('move-from').value&&(state.liveMode||p.rack===el('move-from-rack').value);});
    var target=!state||!state.liveMode||rows.some(function(p){return p.location===el('move-to').value&&p.stocked;});
    var different=el('move-from').value!==el('move-to').value||el('move-from-rack').value!==el('move-to-rack').value;
    el('move-submit').disabled=busy||!state||!state.ready||!source||source.quantity<q||!Number.isSafeInteger(q)||q<1||!target||!different||(state.liveMode&&!el('move-confirm').checked)||state.movements.some(function(m){return m.sku===sku()&&active(m);});
  }
  function racks() {
    if (!state) return;
    var rows = state.positions.filter(function(p){return p.sku === sku();});
    var sourceRacks = state.ready && !state.liveMode ? Array.from(new Set(rows.filter(function(p){return p.location === el('move-from').value && p.quantity>0;}).map(function(p){return p.rack;}).filter(Boolean))).sort() : (state.rackOptions && state.rackOptions[el('move-from').value]) || [];
    options('move-from-rack', sourceRacks);
    options('move-to-rack', (state.rackOptions && state.rackOptions[el('move-to').value]) || []);
    el('move-position').textContent = rows.length ? rows.map(function(p){return p.location+' · '+(state.liveMode?p.quantity+' available pieces':(p.rack || 'Rack unassigned')+' · '+p.quantity+' pieces');}).join('\n')+(state.liveMode?'\nLive quantity is checked by location. Physically verify the selected racks; rack balances have not been audited.':'') : sku() ? 'No unique tracked stock found for this SKU.' : 'Select an exact SKU to see available pieces.';
    if (state.movements.some(function(m){return m.sku === sku() && active(m);})) el('move-position').textContent += '\nReview the existing movement before submitting another for this SKU.';
    updateSubmit();
  }
  async function api(url, body) {
    var r = await fetch(url, body ? {method:'POST', headers:{'Content-Type':'application/json'},body:JSON.stringify(body)} : {});
    var d = await r.json(); if (!r.ok || !d.success) throw Error(d.error || 'Stock movement request failed.'); return d;
  }
  function render() {
    el('move-form').hidden = view !== 'move'; el('move-list').hidden = view === 'move';
    host.querySelectorAll('[data-view]').forEach(function(b){b.setAttribute('aria-selected', String(b.dataset.view === view));});
    if (!state) return;
    confirmation.hidden=!state.liveMode;updateSubmit();
    if (view === 'move') { racks(); return; }
    var rows = state.movements.filter(function(m){return view === 'history' ? !active(m) : active(m);});
    el('move-list').innerHTML = rows.map(function(m){
      var label={pending:'Awaiting approval',sync_pending:'Shopify sync pending',approved:'Approved',cancelled:'Cancelled',correction_required:'Physical correction required'}[m.status]||m.status,controls='';
      if(state.canApprove&&active(m)){
        if(m.status==='correction_required'&&m.mode==='live')controls='<button data-id="'+esc(m.id)+'" data-action="resolve">Confirm physical correction</button>';
        else if(m.status!=='correction_required')controls=(!m.syncRejected?'<button data-id="'+esc(m.id)+'" data-action="approve">'+(m.status==='sync_pending'?'Retry same sync':'Approve')+'</button>':'')+(!m.syncInput||m.syncRejected?'<button data-id="'+esc(m.id)+'" data-action="correction">Correction</button>':'')+(m.mode==='live'&&m.status==='pending'&&!m.syncInput&&!m.firstAttemptAt?'<button data-id="'+esc(m.id)+'" data-action="cancel">Cancel request</button>':'');
      }
      var audit=m.cancelledBy?'Cancelled by '+m.cancelledBy+' · '+new Date(m.cancelledAt).toLocaleString():m.resolvedBy?'Correction resolved by '+m.resolvedBy:m.reviewedBy?'Reviewed by '+m.reviewedBy+(m.status==='approved'&&m.reviewedBy===m.submittedBy?' · Self-approved':''):active(m)?'An inventory approver must review this movement.':'';
      return '<article class="move-row"><div><b>'+esc(m.sku)+'</b><small>'+m.quantity+' pieces · '+esc(m.submittedBy)+'</small></div><div>'+esc(m.from.location)+' / '+esc(m.from.rack||'Unassigned')+' → '+esc(m.to.location)+' / '+esc(m.to.rack||'Unassigned')+'<small>'+esc(new Date(m.submittedAt).toLocaleString())+'</small></div><div class="move-pending">'+esc(label)+'<small>'+esc(m.cancellationReason||m.resolutionNote||m.reviewNote||m.syncError||'')+'</small></div>'+(controls?'<div class="review">'+controls+'</div>':'<small>'+esc(audit)+'</small>')+'</article>';
    }).join('')||'<p class="sub">No '+(view==='history'?'completed movements':'movements awaiting approval')+'.</p>';
  }
  async function refresh() {
    state = await api('/api/stock-movements');
    el('move-skus').innerHTML = Array.from(new Set(state.positions.map(function(p){return p.sku;}))).sort().map(function(s){return '<option value="'+esc(s)+'">';}).join('');
    message(state.ready ? state.liveMode ? 'Live Shopify available stock · Select and verify the physical racks. Authorised inventory approvers can approve their own or other staff requests.' : 'Reported physical position updates immediately. Manager approval confirms the Shopify transfer.' : 'Configure distinct Display and Warehouse Shopify locations before submitting movements.');
    render();
  }
  host.querySelectorAll('[data-view]').forEach(function(b){b.onclick = function(){cancelId=null;cancelForm.hidden=true;view = b.dataset.view;render();};});
  el('move-keep-request').onclick=function(){cancelId=null;cancelForm.hidden=true;};
  cancelForm.onsubmit=async function(e){
    e.preventDefault();if(busy||!cancelId)return;
    var reason=el('move-cancel-reason').value.trim();if(!reason)return;
    busy=true;cancelForm.querySelectorAll('input,button').forEach(function(x){x.disabled=true;});
    try{await api('/api/stock-movements/'+encodeURIComponent(cancelId)+'/review',{action:'cancel',reason:reason});cancelId=null;cancelForm.hidden=true;await refresh();localStorage.setItem('sanki_inventory_care_updated',String(Date.now()));message('Request cancelled. It remains in History and a replacement request can now be submitted.');}
    catch(err){message(err.message);}finally{busy=false;cancelForm.querySelectorAll('input,button').forEach(function(x){x.disabled=false;});}
  };
  ['move-sku','move-from','move-to'].forEach(function(id){el(id).addEventListener('change',racks);});
  el('move-sku').addEventListener('input', racks);
  // USB/Bluetooth scanners enter the SKU exactly like a keyboard. Do not
  // interpret scanner Enter as a physical transfer confirmation.
  el('move-sku').addEventListener('keydown', function(e){if(e.key === 'Enter'){e.preventDefault();racks();el('move-quantity').focus();}});
  function edit(e){if(!busy){requestId=null;if(e.target.id!=='move-confirm')el('move-confirm').checked=false;}updateSubmit();}
  el('move-form').addEventListener('input',edit);
  el('move-form').addEventListener('change',edit);
  el('move-form').onsubmit = async function(e){
    e.preventDefault();updateSubmit();if(el('move-submit').disabled)return;
    requestId=requestId||crypto.randomUUID();var body={requestId:requestId,sku:sku(),quantity:Number(el('move-quantity').value),from:{location:el('move-from').value,rack:el('move-from-rack').value},to:{location:el('move-to').value,rack:el('move-to-rack').value},physicalConfirmed:el('move-confirm').checked};
    busy=true;host.querySelectorAll('form input,form select,form button').forEach(function(x){x.disabled=true;});
    try { await api('/api/stock-movements',body);requestId=null;el('move-confirm').checked=false;await refresh();message('Movement recorded. An authorised inventory approver can approve it in Pending approval before Shopify quantities change, including their own request.'); }
    catch(err){message(err.message);}finally{busy=false;host.querySelectorAll('form input,form select,form button').forEach(function(x){x.disabled=false;});updateSubmit();}
  };
  el('move-list').onclick = async function(e){
    var b = e.target.closest('button[data-id]'); if (!b||busy) return;
    var action=b.dataset.action;
    if(action==='cancel'){cancelId=b.dataset.id;cancelForm.hidden=false;el('move-cancel-reason').value='';el('move-cancel-reason').focus();return;}
    var reason=action==='correction'?prompt('Describe the physical correction required. Stock will not automatically move back.'):action==='resolve'?prompt('Verify the pieces have physically returned to the original source rack, then describe the correction:'):'';
    if(action!=='approve'&&!reason)return;busy=true;b.disabled=true;
    try { await api('/api/stock-movements/'+encodeURIComponent(b.dataset.id)+'/review',{action:action,reason:reason,physicalCorrected:action==='resolve'});await refresh();localStorage.setItem('sanki_inventory_care_updated',String(Date.now())); }
    catch(err){await refresh().catch(function(){});message(err.message);}finally{busy=false;b.disabled=false;}
  };
  refresh().catch(function(err){message(err.message);});
  setInterval(function(){if(!document.hidden&&!busy)refresh().catch(function(err){message(err.message);});},60000);
  window.addEventListener('storage',function(e){if(e.key==='sanki_inventory_care_updated'&&!busy)refresh().catch(function(err){message(err.message);});});
})();
