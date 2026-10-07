(function () {
  'use strict';
  var search = document.getElementById('search');
  if (!search) return;
  search.setAttribute('aria-label', 'Search inventory');
  var clear = document.createElement('button');
  clear.type = 'button'; clear.className = 'inventory-search-clear'; clear.textContent = '×';
  clear.setAttribute('aria-label', 'Clear search'); clear.title = 'Clear search';
  search.parentNode.appendChild(clear);
  var scan = document.createElement('button');
  scan.type = 'button'; scan.className = 'inventory-scan-button'; scan.textContent = 'Scan barcode';
  search.parentNode.insertAdjacentElement('afterend', scan);
  var message = document.createElement('p');
  message.className = 'inventory-barcode-message'; message.setAttribute('role', 'status'); message.hidden = true;
  scan.insertAdjacentElement('afterend', message);
  var dialog = document.createElement('dialog');
  dialog.className = 'inventory-barcode-dialog'; dialog.setAttribute('aria-labelledby', 'barcodeTitle');
  dialog.innerHTML = '<div class="barcode-heading"><h2 id="barcodeTitle">Scan a product barcode</h2><button type="button" class="barcode-close" aria-label="Close barcode scanner">×</button></div><p>Point the camera at the barcode on the product label. We’ll find its SKU in Shopify.</p><video muted autoplay playsinline aria-label="Barcode camera preview"></video><p class="barcode-status" role="status" aria-live="polite"></p><button type="button" class="barcode-retry">Start camera</button><form><label for="barcodeValue">Or enter a barcode / use a handheld scanner</label><div class="barcode-entry"><input id="barcodeValue" autocomplete="off" autocapitalize="off" spellcheck="false" maxlength="200"><button type="submit">Find SKU</button></div></form><button type="button" class="barcode-cancel">Cancel</button>';
  document.body.appendChild(dialog);
  var video = dialog.querySelector('video'), status = dialog.querySelector('.barcode-status');
  var input = dialog.querySelector('input'), submit = dialog.querySelector('[type="submit"]');
  var retry = dialog.querySelector('.barcode-retry');
  var generation = 0, cameraGeneration = 0, controller = null, stream = null, controls = null, decoderPromise = null;

  function stopCamera() {
    cameraGeneration++;
    if (controls) { controls.stop(); controls = null; }
    if (stream) { stream.getTracks().forEach(function (t) { t.stop(); }); stream = null; }
    video.srcObject = null; retry.disabled = false; retry.textContent = 'Start camera';
  }
  function invalidate() {
    generation++;
    if (controller) { controller.abort(); controller = null; }
    submit.disabled = false;
  }
  function updateClear() { clear.hidden = !search.value; }
  function close() { invalidate(); stopCamera(); if (dialog.open) dialog.close(); scan.focus(); }
  clear.onclick = function () {
    invalidate(); stopCamera();
    search.value = ''; search.dispatchEvent(new Event('input', { bubbles: true }));
    message.hidden = true; search.focus();
  };
  search.addEventListener('input', function () { invalidate(); message.hidden = true; updateClear(); });
  document.getElementById('reset').addEventListener('click', function () { invalidate(); message.hidden = true; updateClear(); });
  window.addEventListener('inventory:open-product', updateClear);
  dialog.querySelector('.barcode-close').onclick = close;
  dialog.querySelector('.barcode-cancel').onclick = close;
  dialog.addEventListener('cancel', function (e) { e.preventDefault(); close(); });
  dialog.addEventListener('close', function () { invalidate(); stopCamera(); });
  window.addEventListener('pagehide', close);
  document.addEventListener('visibilitychange', function () { if (document.hidden && dialog.open) close(); });

  function loadDecoder() {
    if (window.ZXingBrowser) return Promise.resolve(window.ZXingBrowser);
    if (!decoderPromise) decoderPromise = new Promise(function (resolve, reject) {
      var script = document.createElement('script'); script.src = '/inventory-barcode-decoder.js?v=0.2.1';
      script.onload = function () { resolve(window.ZXingBrowser); };
      script.onerror = function () { script.remove(); decoderPromise = null; reject(new Error('decoder')); };
      document.head.appendChild(script);
    });
    return decoderPromise;
  }
  async function lookup(code) {
    invalidate(); stopCamera();
    code = String(code || '').trim();
    if (!code) { status.textContent = 'Enter or scan a barcode first.'; input.focus(); return; }
    var token = generation; controller = new AbortController(); submit.disabled = true;
    input.value = code; status.textContent = 'Finding the SKU in Shopify…';
    try {
      var response = await fetch('/api/inventory-categorization/barcode?barcode=' + encodeURIComponent(code), { signal: controller.signal });
      var data = await response.json();
      if (token !== generation || !dialog.open) return;
      if (!response.ok || !data.success || !data.match || !data.match.sku) throw new Error(data.error || 'Barcode lookup failed. Try again.');
      search.value = data.match.sku;
      window.dispatchEvent(new CustomEvent('inventory:barcode-match', { detail: data.match }));
      updateClear(); close();
      message.textContent = 'Barcode matched SKU ' + data.match.sku + '.'; message.hidden = false;
    } catch (e) {
      if (token === generation && e.name !== 'AbortError') status.textContent = e.message === 'Failed to fetch' ? 'Could not reach Shopify. Try again.' : e.message;
    } finally { if (token === generation) { submit.disabled = false; controller = null; } }
  }
  async function startCamera() {
    invalidate(); stopCamera();
    var token = cameraGeneration;
    retry.disabled = true; status.textContent = 'Starting camera…';
    try {
      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) throw new Error('unsupported');
      var decoder = await loadDecoder();
      if (token !== cameraGeneration || !dialog.open) return;
      var camera = await navigator.mediaDevices.getUserMedia({ video: { facingMode: { ideal: 'environment' } }, audio: false });
      if (token !== cameraGeneration || !dialog.open) { camera.getTracks().forEach(function (t) { t.stop(); }); return; }
      stream = camera;
      // Each camera session owns its video element. A late stop from a cancelled
      // decoder must not detach the preview belonging to a newer session.
      var preview = video.cloneNode(false); video.replaceWith(preview); video = preview;
      status.textContent = 'Hold the barcode steady and keep the whole label in view.';
      var reader = new decoder.BrowserMultiFormatReader();
      var scanControls = await reader.decodeFromStream(camera, preview, function (result, error, activeControls) {
        if (result && token === cameraGeneration && dialog.open) { activeControls.stop(); lookup(result.getText()); }
      });
      if (token !== cameraGeneration || !dialog.open) scanControls.stop();
      else controls = scanControls;
    } catch (e) {
      if (token !== cameraGeneration || !dialog.open) return;
      stopCamera();
      status.textContent = e.name === 'NotAllowedError' ? 'Camera access was denied. Allow camera access in your browser, or enter the barcode below.' : e.name === 'NotFoundError' ? 'No camera was found. Use a handheld scanner or enter the barcode below.' : 'The camera could not start. Try again, or enter the barcode below.';
      input.focus();
    }
  }
  scan.onclick = function () { message.hidden = true; input.value = ''; status.textContent = ''; dialog.showModal(); startCamera(); };
  retry.onclick = startCamera;
  dialog.querySelector('form').onsubmit = function (e) { e.preventDefault(); lookup(input.value); };
  updateClear();
})();
