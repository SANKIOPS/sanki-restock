(function () {
  'use strict';
  var data, view = 'cleaning', draft = [], requestId, busy = false, attempted = false;
  var registerSequence = 0, quantitySequence = 0, quantityLoading = false, quantityTimer, quantityAt = '', quantityQueued = false, quantityQueuedForce = false;
  function el(id) { return document.getElementById(id); }
  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
  function status(text, error) { el('status').textContent = text; el('status').className = error ? 'error' : ''; }
  async function api(url, body) {
    var r = await fetch(url, body ? {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)} : {});
    var d = await r.json(); if (!r.ok || !d.success) throw Error(d.error || 'Inventory request failed.'); return d;
  }
  function kind() {
    var cleaning = el('kind').value === 'cleaning';
    el('vendor-label').hidden = !cleaning; el('date-label').hidden = !cleaning;
    el('vendor').required = cleaning; el('expected').required = cleaning;
    el('reason').innerHTML = (data ? data.reasons[el('kind').value] : []).map(function(r){return '<option>'+esc(r)+'</option>';}).join('');
  }
  function chosen() { return data && (data.items || []).filter(function(i){return i.sku === el('sku').value.trim().toUpperCase();}); }
  function sources() {
    var items = chosen() || [], item = items.length === 1 ? items[0] : null, previous = el('location').value;
    el('location').innerHTML = item ? item.levels.map(function(l){return '<option value="'+esc(l.locationId)+'">'+esc(l.location)+' · '+l.available+' available</option>';}).join('') : '<option value="">Choose a SKU first</option>';
    if (item && item.levels.some(function(l){return l.locationId === previous;})) el('location').value = previous;
    el('quantity-hint').textContent = item ? item.product.title+' · '+item.variant+' · '+item.levels.map(function(l){return l.location+': '+l.available+' available / '+l.on_hand+' owned';}).join(' · ') : items.length > 1 ? 'This SKU matches multiple items. Correct the duplicate in Shopify before logging it.' : 'Choose an exact SKU to see current available pieces.';
    if (item && !item.tracked) el('quantity-hint').textContent += ' · Inventory tracking must be enabled in Shopify.';
    el('add-line').disabled = busy || !item || !item.tracked;
    racks();
  }
  function racks() {
    var loc = data && Object.keys(data.mapping || {}).find(function(k){return data.mapping[k] === el('location').value;});
    var values = data && data.rackOptions && (data.rackOptions[loc] || []) || [];
    el('rack-options').innerHTML = values.map(function(r){return '<option value="'+esc(r)+'">';}).join('');
  }
  function draftRows() {
    el('draft-lines').innerHTML = draft.map(function(l,i){return '<tr><td><b>'+esc(l.sku)+'</b><small>'+esc(l.title)+'</small></td><td>'+esc(l.location)+'<small>'+esc(l.rack || 'Rack unassigned')+'</small></td><td>'+l.quantity+'</td><td><button type="button" data-remove="'+i+'" aria-label="Remove '+esc(l.sku)+'">Remove</button></td></tr>';}).join('') || '<tr><td colspan="4" class="empty">Add one or more SKUs to this batch.</td></tr>';
    el('submit').disabled = busy || attempted || !data || !draft.length;
    el('submit').textContent = data && data.canManage ? 'Confirm batch · '+draft.reduce(function(n,l){return n+l.quantity;},0)+' pieces' : 'Request approval';
  }
  function totals() {
    var t = data.totals, cards = [['Total owned',t.totalQty,'Includes unavailable and committed pieces'],['Available for sale',t.availableQty,'Used by Shopify sales and replenishment'],['Dry cleaning',t.cleaningQty,'Outstanding pieces in this register'],['Not for sale',t.notForSaleQty,'Damaged, defective and miscellaneous'],['Other unavailable / committed',t.otherUnavailableQty+t.committedQty,'Shopify reservations and other inspections']];
    el('totals').innerHTML = cards.map(function(c){return '<article class="care-total"><span>'+c[0]+'</span><strong>'+Number(c[1] || 0).toLocaleString()+'</strong><small>'+c[2]+'</small></article>';}).join('');
  }
  function filterBatch(b) { var q = el('register-search').value.trim().toLowerCase(); return !q || [b.vendor,b.reason,b.note,b.id].concat(b.lines.map(function(l){return l.sku+' '+l.title;})).join(' ').toLowerCase().includes(q); }
  function operation(op) {
    var b = data.batches.find(function(b){return b.id === op.batchId;}), labels = {open:'Set aside',release:'Returned to sale',classify:'Moved to not for sale',dispose:'Disposed'};
    return '<article class="op"><div class="batch-head"><b>'+esc(labels[op.action])+' · '+esc(b && (b.vendor || b.reason))+'</b><span class="badge '+(op.status === 'confirmed' ? '' : 'issue')+'">'+esc(op.status.replace(/_/g,' '))+'</span></div><p>'+op.lines.map(function(l){return esc(l.sku)+' · '+l.quantity+' pieces';}).join(' / ')+'</p><div class="history-note">Requested by '+esc(op.requestedBy)+' · '+esc(new Date(op.requestedAt).toLocaleString())+(op.confirmedAt?' · Confirmed by '+esc(op.confirmedBy)+' · '+esc(new Date(op.confirmedAt).toLocaleString()):'')+'</div><p class="history-note">'+esc(op.error || op.reviewNote || op.note || b && b.note || '')+'</p>'+(data.canManage && !['confirmed','cancelled'].includes(op.status)?'<div class="op-actions">'+(op.status !== 'review_required'?'<button data-op="'+esc(op.id)+'" data-action="confirm">'+(op.status === 'sync_pending'?'Retry same Shopify request':'Confirm & update Shopify')+'</button>':'')+(['awaiting_approval','review_required'].includes(op.status)?'<button data-op="'+esc(op.id)+'" data-action="cancel">Cancel with reason</button>':'')+'</div>':'')+'</article>';
  }
  function batchCard(b) {
    var outstanding = b.balances.filter(function(l){return l[view]>0;}), count = outstanding.reduce(function(n,l){return n+l[view];},0);
    if (!count) return '';
    var overdue = view === 'cleaning' && b.expectedReturn && b.expectedReturn < new Date().toLocaleDateString('en-CA');
    var blocked = data.operations.some(function(o){return o.batchId === b.id && ['sync_pending','review_required'].includes(o.status);});
    return '<article class="batch"><div class="batch-head"><div><h3>'+esc(b.vendor || b.reason)+'</h3><span class="sub">'+esc(b.reason)+' · '+esc(new Date(b.createdAt).toLocaleDateString())+' · '+esc(b.createdBy)+(b.expectedReturn?' · Due '+esc(b.expectedReturn):'')+'</span></div><span class="badge '+(overdue?'overdue':'')+'">'+count+' pieces'+(overdue?' · Overdue':'')+'</span></div><p class="batch-note">'+esc(b.note)+'</p>'+outstanding.map(function(l){return '<div class="op"><b>'+esc(l.sku)+' · '+l[view]+' outstanding</b><div class="sub">'+esc(l.title)+' · '+esc(l.variant)+' · '+esc(l.location)+' / '+esc(l.rack || 'Rack unassigned')+'</div>'+(data.canManage && !blocked?'<form class="return-form" data-batch="'+esc(b.id)+'" data-line="'+esc(l.id)+'"><label class="quantity">Pieces<input name="quantity" type="number" min="1" max="'+l[view]+'" value="1" step="1" required></label><label>Action<select name="action"><option value="release">Return ready for sale</option>'+(view === 'cleaning'?'<option value="classify">Move to not for sale</option>':'<option value="dispose">Dispose / permanently remove</option>')+'</select></label><label class="note">Inspection result / reason<input name="note" maxlength="500" required placeholder="Describe the condition"></label><button class="primary">Record action</button><label class="inspection"><input name="confirmed" type="checkbox" required> I checked these pieces and confirm the selected action. A ready-for-sale return goes back to the original location and rack; disposal reduces total owned.</label></form>':'<p class="sub">'+(blocked?'Resolve the pending Shopify action before changing this batch.':'A stock manager can record returns and reclassification.')+'</p>')+'</div>';}).join('')+'</article>';
  }
  function register() {
    if (!data) return;
    document.querySelectorAll('[data-view]').forEach(function(b){b.setAttribute('aria-selected',String(b.dataset.view === view));});
    if (view === 'history' || view === 'pending') {
      var ops = data.operations.filter(function(o){return (view === 'history' || !['confirmed','cancelled'].includes(o.status)) && filterBatch(data.batches.find(function(b){return b.id === o.batchId;}));});
      el('register').innerHTML = ops.map(operation).join('') || '<p class="empty">No '+(view === 'pending'?'pending actions':'history entries')+'.</p>'; return;
    }
    el('register').innerHTML = data.batches.filter(filterBatch).map(batchCard).join('') || '<p class="empty">No outstanding '+(view === 'cleaning'?'dry cleaning':'not-for-sale')+' pieces in this register.</p>';
  }
  async function refreshRegister() {
    var sequence = ++registerSequence;
    var previousReason = el('reason').value;
    var incoming = await api('/api/inventory-care?registerOnly=1');
    if (sequence !== registerSequence) return;
    var first = !data || !data.registerOnly;
    data = Object.assign(data || {items:[],mapping:{},alerts:[]}, incoming);
    kind(); if (data.reasons[el('kind').value].includes(previousReason)) el('reason').value = previousReason;
    el('permission').textContent = data.canManage ? 'You can confirm batches and inspect returns.' : 'Your entries need stock manager approval. Keep pieces in place until confirmation.';
    sources(); draftRows(); register();
    if (first) status('Register ready. Pending requests and history are available.');
  }
  async function refreshQuantities(force) {
    if (quantityLoading) { quantityQueued = true; quantityQueuedForce = quantityQueuedForce || force; return; }
    quantityLoading = true;
    var sequence = ++quantitySequence;
    clearTimeout(quantityTimer);
    try {
      var incoming = await api('/api/inventory-care?quantitiesOnly=1&knownAt='+encodeURIComponent(quantityAt)+(force?'&refresh=1':''));
      if (sequence !== quantitySequence) return;
      if (incoming.items) {
        quantityAt = incoming.at;
        data = Object.assign(data || {batches:[],operations:[],reasons:{cleaning:[],miscellaneous:[]}}, incoming);
        el('sku-options').innerHTML = Array.from(new Set(data.items.map(function(i){return i.sku;}).filter(Boolean))).sort().map(function(s){return '<option value="'+esc(s)+'">';}).join('');
        totals(); sources(); draftRows();
      }
      el('quantity-status').textContent = (quantityAt ? 'Shopify quantities · Last confirmed update '+new Date(quantityAt).toLocaleString() : 'Waiting for the first confirmed Shopify quantities')+(incoming.refreshing?' · Refreshing in the background…':'')+(incoming.refreshError?' · Refresh failed: '+incoming.refreshError+' · Previous quantities remain visible.':'')+(data && data.alerts.length?' · '+data.alerts.join(' '):'');
      if (incoming.refreshing) quantityTimer = setTimeout(function(){refreshQuantities(false);},3000);
    } catch (e) {
      if (sequence === quantitySequence) el('quantity-status').textContent = 'Quantities could not be refreshed: '+e.message+(quantityAt?' · Last confirmed update '+new Date(quantityAt).toLocaleString()+'. Previous quantities remain visible.':'');
    } finally {
      quantityLoading = false;
      if (quantityQueued || sequence !== quantitySequence) { var nextForce = quantityQueuedForce; quantityQueued = quantityQueuedForce = false; refreshQuantities(nextForce); }
    }
  }
  async function run(fn) {
    if (busy) return; busy = true; el('submit').disabled = true; el('refresh').disabled = true;
    registerSequence++; quantitySequence++;
    document.querySelectorAll('form input, form select, form textarea, form button').forEach(function(e){e.disabled=true;});
    try { await fn(); } catch (e) { await refreshRegister().catch(function(){}); refreshQuantities(false); status(e.message+' Review Pending & sync before creating another request.', true); }
    finally { busy = false; el('refresh').disabled = false; document.querySelectorAll('form input, form select, form textarea, form button').forEach(function(e){e.disabled=false;}); sources(); draftRows(); }
  }
  el('kind').onchange = kind;
  el('sku').oninput = sources; el('location').onchange = racks;
  el('sku').onkeydown = function(e){if(e.key==='Enter'){e.preventDefault();sources();el('quantity').focus();}};
  el('add-line').onclick = function() {
    var items = chosen(), item = items && items.length === 1 && items[0], qty = Number(el('quantity').value), loc = item && item.levels.find(function(l){return l.locationId === el('location').value;});
    if (!item || !loc || !Number.isSafeInteger(qty) || qty < 1 || qty > loc.available) {status('Choose a stocked location and a positive whole quantity within available stock.',true);return;}
    if (draft.some(function(l){return l.sku===item.sku && l.locationId===loc.locationId;})){status('This SKU/location is already in the batch. Remove it and enter the combined quantity.',true);return;}
    draft.push({sku:item.sku,quantity:qty,locationId:loc.locationId,location:loc.location,rack:el('rack').value.trim(),title:item.product.title}); requestId=null; attempted=false; draftRows();
    el('sku').value=''; el('rack').value=''; el('quantity').value='1';sources();el('sku').focus();
  };
  el('draft-lines').onclick = function(e){var b=e.target.closest('[data-remove]');if(!b || busy)return;draft.splice(Number(b.dataset.remove),1);requestId=null;attempted=false;draftRows();};
  el('batch-form').addEventListener('input',function(){if(!busy){requestId=null;attempted=false;draftRows();}});
  el('batch-form').onsubmit = function(e) {
    e.preventDefault(); if (!draft.length || busy || attempted) return;
    var body={requestId:requestId || crypto.randomUUID(),kind:el('kind').value,reason:el('reason').value,vendor:el('vendor').value.trim(),expectedReturn:el('expected').value,note:el('note').value.trim(),lines:draft.map(function(l){return {sku:l.sku,quantity:l.quantity,locationId:l.locationId,rack:l.rack};})};
    requestId=body.requestId;
    run(async function(){status('Recording batch and checking Shopify…');attempted=true;var result=await api('/api/inventory-care/batches',body);draft=[];requestId=null;attempted=false;el('note').value='';await refreshRegister();changed();refreshQuantities(true);status(result.operation.status==='confirmed'?'Batch confirmed. These pieces are now unavailable for sale.':'Request recorded. A stock manager must confirm it before the pieces are sent or separated.');});
  };
  function changed(){localStorage.setItem('sanki_inventory_care_updated',String(Date.now()));}
  el('register').onsubmit = function(e) {
    var form=e.target.closest('.return-form');if(!form)return;e.preventDefault();
    var body={requestId:crypto.randomUUID(),action:form.elements.action.value,note:form.elements.note.value.trim(),inspected:form.elements.confirmed.checked,disposed:form.elements.confirmed.checked,lines:[{lineId:form.dataset.line,from:view,quantity:Number(form.elements.quantity.value)}]};
    run(async function(){form.querySelector('button').disabled=true;status('Confirming selected action with Shopify…');await api('/api/inventory-care/batches/'+encodeURIComponent(form.dataset.batch)+'/actions',body);await refreshRegister();changed();refreshQuantities(true);status('Action confirmed. The register is updated and quantities are refreshing.');});
  };
  el('register').onclick = function(e){var b=e.target.closest('[data-op]');if(!b)return;var note=b.dataset.action==='cancel'?prompt('Reason for cancelling this unconfirmed action:'):'';if(b.dataset.action==='cancel'&&!note)return;run(async function(){b.disabled=true;await api('/api/inventory-care/operations/'+encodeURIComponent(b.dataset.op)+'/review',{action:b.dataset.action,note:note});await refreshRegister();changed();refreshQuantities(b.dataset.action!=='cancel');status('Action reviewed. The register shows its current status.');});};
  document.querySelectorAll('[data-view]').forEach(function(b){b.onclick=function(){view=b.dataset.view;register();};});
  el('register-search').oninput=register;el('refresh').onclick=function(){refreshRegister().catch(function(e){status(e.message,true);});refreshQuantities(true);};
  el('sku').value=new URLSearchParams(location.search).get('sku') || '';
  refreshRegister().catch(function(e){status(e.message,true);});
  refreshQuantities(false);
  setInterval(function(){if(!document.hidden && !busy){refreshRegister().catch(function(e){status('Register could not be refreshed: '+e.message,true);});refreshQuantities(false);}},60000);
})();
