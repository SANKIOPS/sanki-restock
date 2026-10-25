(function () {
'use strict';
var finder = document.querySelector('.finder');
if (!finder) return;
var endpoint = '/api/inventory-categorization/image-search', host = document.createElement('div');
host.className = 'visual-finder';
host.innerHTML = '<div class="visual-actions"><button type="button" id="visualUpload">Upload product photo</button><button type="button" id="visualCamera">Take photo</button><button type="button" id="visualClear" hidden>Cancel photo search</button></div><input id="visualFile" type="file" accept="image/jpeg,image/png,image/webp,image/heic,image/heif,image/avif" hidden><input id="visualCapture" type="file" accept="image/*" capture="environment" hidden><p class="visual-note">Take or upload a clear front photo of one article. Confirm the suggested design, then select its colour and size. Photos are processed temporarily by the configured recognition service.</p><div id="visualProgress" role="status" aria-live="polite" aria-atomic="true"></div><div class="visual-results" id="visualResults"></div><button type="button" class="visual-broader" id="visualBroader" hidden>Search full catalogue</button>';
finder.appendChild(host);
var upload = host.querySelector('#visualUpload'), camera = host.querySelector('#visualCamera'), clear = host.querySelector('#visualClear'), result = host.querySelector('#visualResults'), status = host.querySelector('#visualProgress'), broader = host.querySelector('#visualBroader');
var diagnosticButton = document.createElement('button');
diagnosticButton.type = 'button'; diagnosticButton.textContent = 'Check recognition service'; host.querySelector('.visual-actions').appendChild(diagnosticButton);
var generation = 0, preview = '', preparedPhoto = null, activeJob = '', pollTimer = null, polling = false, pollController = null, diagnosticController = null, busy = false;
function esc(value) { return String(value == null ? '' : value).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
function makeController() { return typeof AbortController !== 'undefined' ? new AbortController() : null; }
function request(url, options, controller) {
  options = options || {}; controller = controller || makeController();
  if (controller) options.signal = controller.signal;
  var timeout = setTimeout(function () { if (controller) controller.abort(); }, 30000);
  return fetch(url, options).then(function (response) {
    return response.json().catch(function () { throw new Error('The server returned an unreadable response. Please try again.'); }).then(function (data) {
      if (!response.ok || !data.success) { var error = new Error(data.error || 'Photo search is unavailable. Please try again.'); error.status = response.status; throw error; }
      return data;
    });
  }).catch(function (error) {
    if (error.name === 'AbortError') throw new Error('The connection timed out. Check your connection and try again.');
    throw error;
  }).finally(function () { clearTimeout(timeout); });
}
function cancelJob(id) {
  if (!id) return Promise.resolve();
  return request(endpoint, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ action: 'cancel', id: id }), keepalive: true }).catch(function () { /* Local clear still works if disconnected; server jobs have a deadline. */ });
}
function setBusy(value) {
  busy = value; upload.disabled = camera.disabled = diagnosticButton.disabled = value;
  clear.textContent = value ? 'Cancel photo search' : 'Clear photo search';
  host.setAttribute('aria-busy', value ? 'true' : 'false');
}
function stopPolling() {
  clearTimeout(pollTimer); pollTimer = null;
  if (pollController) pollController.abort();
  pollController = null; polling = false;
}
function prepared(file) {
  return new Promise(function (resolve, reject) {
    var url = URL.createObjectURL(file), image = new Image(), done = false;
    var timer = setTimeout(function () { finish(new Error('This photo took too long to open. Try a smaller JPEG image.')); }, 20000);
    function finish(error, blob) { if (done) return; done = true; clearTimeout(timer); image.onload = image.onerror = null; URL.revokeObjectURL(url); if (error) reject(error); else resolve(blob); }
    image.onload = function () {
      try {
        var width = image.naturalWidth, height = image.naturalHeight;
        if (!width || !height || width * height > 80000000) return finish(new Error('Photo resolution is too large. Take a new picture at a lower resolution.'));
        // Modern mobile image decoders apply EXIF orientation before drawing.
        var scale = Math.min(1, 1600 / Math.max(width, height)), canvas = document.createElement('canvas');
        canvas.width = Math.max(1, Math.round(width * scale)); canvas.height = Math.max(1, Math.round(height * scale));
        var context = canvas.getContext('2d');
        if (!context || !canvas.toBlob) return finish(new Error('This browser cannot prepare photos. Use an updated browser.'));
        context.fillStyle = '#ffffff'; context.fillRect(0, 0, canvas.width, canvas.height); context.drawImage(image, 0, 0, canvas.width, canvas.height);
        canvas.toBlob(function (blob) { finish(blob && blob.size <= 12 * 1024 * 1024 ? null : new Error('Could not prepare this photo. Try a smaller JPEG image.'), blob); }, 'image/jpeg', .88);
      } catch (_) { finish(new Error('Could not prepare this photo. Try a JPEG or PNG image.')); }
    };
    image.onerror = function () { finish(new Error('This browser could not open the photo. Use JPEG, PNG or WebP; on iPhone/iPad, export HEIC as JPEG or take a new photo.')); };
    image.src = url;
  });
}
function showPreview() {
  result.innerHTML = '';
  if (!preview) return;
  var image = document.createElement('img'); image.src = preview; image.alt = 'Your search photo'; image.className = 'visual-query'; result.appendChild(image);
}
function validFile(file) {
  if (!file || !file.size) return 'Choose a product photo first.';
  if (file.size > 25 * 1024 * 1024) return 'Choose a photo smaller than 25 MB.';
  if (!/^image\/(jpeg|jpg|png|webp|heic|heif|avif)$/i.test(file.type || '') && !(!file.type && /\.(jpe?g|png|webp|heic|heif|avif)$/i.test(file.name || ''))) return 'Choose a JPEG, PNG, WebP or a supported phone photo.';
  return '';
}
function start(file) {
  var error = validFile(file);
  if (error) { if (!busy) status.textContent = error; return; }
  var token = ++generation, oldJob = activeJob; activeJob = ''; stopPolling();
  if (diagnosticController) diagnosticController.abort();
  setBusy(true); clear.hidden = false; broader.hidden = true; preparedPhoto = null;
  if (preview) URL.revokeObjectURL(preview);
  preview = URL.createObjectURL(file); showPreview(); status.textContent = 'Preparing photo…';
  Promise.all([prepared(file), cancelJob(oldJob)]).then(function (values) {
    if (token !== generation) return;
    preparedPhoto = values[0]; submit(preparedPhoto, token, false);
  }).catch(function (error) { if (token === generation) { status.textContent = error.message; setBusy(false); } });
}
function submit(blob, token, full) {
  if (token !== generation) return;
  var form = new FormData(); form.append('image', blob, 'article.jpg'); form.append('full', full ? '1' : '0');
  status.textContent = full ? 'Searching all catalogue photos. This can take several minutes; you can cancel.' : 'Finding relevant articles and comparing their catalogue photos…';
  // Let an accepted upload return its ID even after Clear, so it can be cancelled.
  request(endpoint, { method: 'POST', body: form }).then(function (data) {
    if (token !== generation) { cancelJob(data.id); return; }
    activeJob = data.id; poll(data.id, token, Date.now(), full, 0);
  }).catch(function (error) { if (token === generation) { activeJob = ''; status.textContent = error.message; setBusy(false); } });
}
function quantity(value) { return Number.isFinite(Number(value)) && value != null ? String(Number(value)) : '—'; }
function money(value) { return Number.isFinite(Number(value)) && value != null ? '₹' + Number(value).toLocaleString('en-IN', { maximumFractionDigits: 2 }) : ''; }
function prices(product) {
  var values = (product.variants || []).map(function (variant) { return variant.price; }).filter(function (price) { return price != null && price !== '' && Number.isFinite(Number(price)); }).map(Number);
  if (!values.length) return 'Price unavailable';
  var low = Math.min.apply(Math, values), high = Math.max.apply(Math, values); return money(low) + (low === high ? '' : '–' + money(high));
}
function renderMatch(match) {
  var product = match.product;
  if (!product || !product.handle) return;
  var variants = product.variants || [], skus = variants.map(function (variant) { return variant.sku; }).filter(Boolean), button = document.createElement('button');
  button.type = 'button'; button.className = 'visual-match';
  button.innerHTML = '<img loading="lazy" decoding="async" src="' + esc(product.image) + '" alt="' + esc(product.title) + '"><span class="visual-confidence">' + esc(match.confidence) + ' visual similarity · confirm article</span><b>' + esc(product.title) + '</b><small>' + esc(match.reason) + '</small><span class="visual-stock">Display ' + esc(quantity(product.displayQty)) + ' · Warehouse ' + esc(quantity(product.warehouseQty)) + '<br>Total owned ' + esc(quantity(product.totalQty)) + ' · ' + esc(prices(product)) + '</span><small>SKUs: ' + esc(skus.slice(0, 2).join(', ') || 'Open article to view') + (skus.length > 2 ? ' · +' + (skus.length - 2) + ' more' : '') + '</small><span class="visual-open">View ' + variants.length + ' colour/size variant' + (variants.length === 1 ? '' : 's') + ' & details →</span>';
  button.onclick = function () {
    window.dispatchEvent(new CustomEvent('inventory:open-product', { detail: { handle: product.handle } }));
    var panel = document.querySelector('.panel'), reduced = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    if (panel && panel.scrollIntoView) panel.scrollIntoView({ behavior: reduced ? 'auto' : 'smooth', block: 'start' });
  };
  result.appendChild(button);
}
function poll(id, token, started, full, failures) {
  if (token !== generation || activeJob !== id || polling) return;
  if (Date.now() - started > (full ? 11 : 4) * 60 * 1000) { cancelJob(id); activeJob = ''; status.textContent = 'Search is taking too long. Try a clearer photo or try again shortly.'; setBusy(false); return; }
  polling = true; pollController = makeController();
  request(endpoint + '/' + encodeURIComponent(id), null, pollController).then(function (data) {
    if (token !== generation || activeJob !== id) return;
    status.textContent = data.message || 'Comparing catalogue photos…';
    if (data.status === 'running' || data.status === 'queued') { pollTimer = setTimeout(function () { poll(id, token, started, full, 0); }, 1500); return; }
    activeJob = ''; setBusy(false);
    if (data.status !== 'complete') return;
    if (data.coverage) {
      var coverage = data.coverage;
      status.textContent += ' Compared ' + coverage.inspected + ' of ' + coverage.total + ' catalogue articles.';
      if (coverage.unsearched) status.textContent += ' Other articles have not been ruled out.';
      if (coverage.missingImages) status.textContent += ' ' + coverage.missingImages + ' missing or unreadable catalogue images.';
    }
    var matches = data.matches || [];
    matches.forEach(renderMatch);
    var at = matches[0] && matches[0].product && matches[0].product.inventoryAt;
    if (at && !isNaN(Date.parse(at))) status.textContent += ' Quantities last updated ' + new Date(at).toLocaleString() + '. Open the article for current details.';
    broader.hidden = !data.canSearchFull || !preparedPhoto;
    broader.textContent = 'Search full catalogue (may take several minutes)';
  }).catch(function (error) {
    if (token !== generation || activeJob !== id) return;
    if (failures < 2 && (!error.status || error.status >= 500)) { status.textContent = 'Connection interrupted. Reconnecting to your photo search…'; pollTimer = setTimeout(function () { poll(id, token, started, full, failures + 1); }, 2000); return; }
    cancelJob(id); activeJob = ''; status.textContent = error.message; setBusy(false);
  }).finally(function () { if (token === generation) { polling = false; pollController = null; } });
}
upload.onclick = function () { host.querySelector('#visualFile').click(); };
camera.onclick = function () { host.querySelector('#visualCapture').click(); };
['visualFile', 'visualCapture'].forEach(function (id) { host.querySelector('#' + id).onchange = function () { start(this.files && this.files[0]); this.value = ''; }; });
clear.onclick = function () {
  generation++; var id = activeJob; activeJob = ''; stopPolling(); cancelJob(id);
  if (diagnosticController) diagnosticController.abort();
  if (preview) URL.revokeObjectURL(preview);
  preview = ''; preparedPhoto = null; result.innerHTML = ''; status.textContent = ''; clear.hidden = broader.hidden = true; setBusy(false); upload.focus();
};
broader.onclick = function () {
  if (!preparedPhoto || busy) return;
  var token = ++generation; stopPolling(); showPreview(); broader.hidden = true; setBusy(true); submit(preparedPhoto, token, true);
};
diagnosticButton.onclick = function () {
  var token = generation; diagnosticController = makeController(); diagnosticButton.disabled = true;
  status.textContent = 'Checking recognition service configuration…';
  request(endpoint + '/diagnostics', null, diagnosticController).then(function (data) {
    if (token !== generation) return;
    var providers = [['Gemini', data.gemini], ['Anthropic', data.anthropic]].filter(function (pair) { return pair[1] && pair[1].configured; });
    status.textContent = providers.length ? providers.map(function (pair) { var provider = pair[1]; return pair[0] + ': ' + (provider.error || (provider.modelListed ? 'configured model listed (' + provider.model + ').' : 'configured; model availability needs checking (' + provider.model + ').')); }).join(' ') + ' This checks configuration; a photo search is needed to verify recognition and quota.' : 'Photo recognition is not configured. An administrator needs to configure a Gemini or Anthropic vision key.';
  }).catch(function (error) { if (token === generation) status.textContent = error.message; }).finally(function () { if (token === generation) { diagnosticButton.disabled = busy; diagnosticController = null; } });
};
window.addEventListener('pagehide', function () { generation++; stopPolling(); cancelJob(activeJob); activeJob = ''; if (diagnosticController) diagnosticController.abort(); if (preview) URL.revokeObjectURL(preview); preview = ''; preparedPhoto = null; result.innerHTML = ''; status.textContent = ''; clear.hidden = broader.hidden = true; setBusy(false); });
})();
