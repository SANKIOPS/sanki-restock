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
  scan.type = 'button'; scan.className = 'inventory-scan-button';
  scan.setAttribute('aria-label', 'Scan barcode'); scan.title = 'Scan barcode';
  scan.innerHTML = '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M7 3H4a1 1 0 0 0-1 1v3m14-4h3a1 1 0 0 1 1 1v3M3 17v3a1 1 0 0 0 1 1h3m10 0h3a1 1 0 0 0 1-1v-3M7 7v10m3-10v10m4-10v10m3-10v10"/></svg>';
  search.parentNode.appendChild(scan);
  var message = document.createElement('p');
  message.className = 'inventory-barcode-message'; message.setAttribute('role', 'status'); message.hidden = true;
  search.parentNode.insertAdjacentElement('afterend', message);
  var dialog = document.createElement('dialog');
  dialog.className = 'inventory-barcode-dialog'; dialog.setAttribute('aria-labelledby', 'barcodeTitle');
  dialog.innerHTML = '<div class="barcode-heading"><h2 id="barcodeTitle">Scan a product barcode</h2><button type="button" class="barcode-close" aria-label="Close barcode scanner">×</button></div><p>Point the rear camera at the product label. Keep the barcode inside the guide, with space at both ends.</p><div class="barcode-preview"><video muted autoplay playsinline aria-label="Barcode camera preview"></video><div class="barcode-guide" aria-hidden="true"></div></div><p class="barcode-status" role="status" aria-live="polite"></p><button type="button" class="barcode-retry">Start camera</button><form><label for="barcodeValue">Or enter a barcode / use a handheld scanner</label><div class="barcode-entry"><input id="barcodeValue" autocomplete="off" autocapitalize="off" spellcheck="false" maxlength="200"><button type="submit">Find SKU</button></div></form><button type="button" class="barcode-cancel">Cancel</button>';
  document.body.appendChild(dialog);
  var video = dialog.querySelector('video'), status = dialog.querySelector('.barcode-status');
  var input = dialog.querySelector('input'), submit = dialog.querySelector('[type="submit"]');
  var retry = dialog.querySelector('.barcode-retry');
  var generation = 0, cameraGeneration = 0, controller = null, stream = null, controls = null, decoderPromise = null, lookupTimer = null;
  var nativeFormats = ['code_128', 'code_39', 'code_93', 'ean_13', 'ean_8', 'upc_a', 'upc_e', 'itf', 'codabar', 'qr_code', 'data_matrix'];

  function deadline(promise, milliseconds, reason) {
    return new Promise(function (resolve, reject) {
      var timer = setTimeout(function () { reject(new Error(reason)); }, milliseconds);
      Promise.resolve(promise).then(function (value) { clearTimeout(timer); resolve(value); }, function (error) { clearTimeout(timer); reject(error); });
    });
  }
  function stopCamera() {
    cameraGeneration++;
    if (controls) { controls.stop(); controls = null; }
    if (stream) { stream.getTracks().forEach(function (t) { t.stop(); }); stream = null; }
    if (video.pause) video.pause();
    video.srcObject = null; retry.disabled = false; retry.textContent = 'Start camera';
  }
  function invalidate() {
    generation++;
    if (lookupTimer) { clearTimeout(lookupTimer); lookupTimer = null; }
    if (controller) { controller.abort(); controller = null; }
    submit.disabled = false;
  }
  function updateClear() { clear.hidden = !search.value; }
  function close() { invalidate(); stopCamera(); if (dialog.open) dialog.close(); scan.focus({ preventScroll: true }); }
  clear.onclick = function () {
    invalidate(); stopCamera();
    search.value = ''; search.dispatchEvent(new Event('input', { bubbles: true }));
    message.hidden = true; search.focus();
  };
  search.addEventListener('input', function () { invalidate(); message.hidden = true; updateClear(); });
  var reset = document.getElementById('reset');
  if (reset) reset.addEventListener('click', function () { invalidate(); message.hidden = true; updateClear(); });
  window.addEventListener('inventory:open-product', updateClear);
  dialog.querySelector('.barcode-close').onclick = close;
  dialog.querySelector('.barcode-cancel').onclick = close;
  dialog.addEventListener('cancel', function (e) { e.preventDefault(); close(); });
  dialog.addEventListener('close', function () { invalidate(); stopCamera(); });
  window.addEventListener('pagehide', close);
  document.addEventListener('visibilitychange', function () { if (document.hidden && dialog.open) close(); });

  function loadDecoder() {
    if (window.ZXingBrowser && typeof window.ZXingBrowser.BrowserMultiFormatReader === 'function') return Promise.resolve(window.ZXingBrowser);
    if (!decoderPromise) decoderPromise = new Promise(function (resolve, reject) {
      var script = document.createElement('script'), timer, finished = false;
      script.src = '/inventory-barcode-decoder.js?v=0.2.1';
      function failed() { if (finished) return; finished = true; clearTimeout(timer); script.remove(); decoderPromise = null; reject(new Error('decoder')); }
      timer = setTimeout(failed, 12000);
      script.onload = function () {
        if (finished) return;
        clearTimeout(timer);
        if (window.ZXingBrowser && window.ZXingBrowser.BrowserMultiFormatReader) { finished = true; resolve(window.ZXingBrowser); }
        else failed();
      };
      script.onerror = failed;
      document.head.appendChild(script);
    });
    return decoderPromise;
  }
  async function fallbackReader() {
    var decoder = await loadDecoder(), reader = new decoder.BrowserMultiFormatReader(new Map());
    // Limit work to product-label formats rather than every document barcode type.
    if (decoder.BarcodeFormat) reader.possibleFormats = ['CODE_128', 'CODE_39', 'CODE_93', 'EAN_13', 'EAN_8', 'UPC_A', 'UPC_E', 'ITF', 'CODABAR', 'QR_CODE', 'DATA_MATRIX'].map(function (key) { return decoder.BarcodeFormat[key]; }).filter(function (value) { return value !== undefined; });
    return { reader: reader };
  }
  async function prepareDetector() {
    if (window.BarcodeDetector && window.BarcodeDetector.getSupportedFormats) {
      try {
        var supported = await deadline(window.BarcodeDetector.getSupportedFormats(), 1500, 'formats');
        var formats = nativeFormats.filter(function (format) { return supported.indexOf(format) !== -1; });
        if (formats.indexOf('code_128') !== -1 && formats.indexOf('ean_13') !== -1) return { native: new window.BarcodeDetector({ formats: formats }) };
      } catch (e) { /* Use the bundled decoder when native detection is unavailable. */ }
    }
    return fallbackReader();
  }
  async function lookup(code) {
    if (submit.disabled) return;
    invalidate(); stopCamera();
    code = String(code || '').trim();
    if (!code) { status.textContent = 'Enter or scan a barcode first.'; input.focus(); return; }
    var token = generation; controller = new AbortController(); submit.disabled = true;
    input.value = code; status.textContent = 'Finding the SKU in Shopify…';
    lookupTimer = setTimeout(function () {
      if (token === generation && controller) { status.textContent = 'The lookup is taking too long. Check your connection and try again.'; controller.abort(); }
    }, 20000);
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
    } finally {
      if (token === generation) { clearTimeout(lookupTimer); lookupTimer = null; submit.disabled = false; controller = null; }
    }
  }
  function scanFrames(preview, detector, token) {
    var active = true, timer = null, count = 0, nativeErrors = 0, cancelDetection = null;
    var canvas = document.createElement('canvas'), context = canvas.getContext('2d', { willReadFrequently: true });
    if (!context) throw new Error('canvas');
    var session = { stop: function () { active = false; clearTimeout(timer); if (cancelDetection) cancelDetection(); canvas.width = canvas.height = 0; } };
    controls = session;
    function current() { return active && token === cameraGeneration && dialog.open; }
    function detectNative(promise) {
      return new Promise(function (resolve, reject) {
        var done = false, timeout = setTimeout(function () {
          var error = new Error('Native barcode detection timed out.'); error.name = 'BarcodeDetectionTimeout'; finish(error);
        }, 1800);
        function finish(error, value) {
          if (done) return; done = true; clearTimeout(timeout); cancelDetection = null;
          if (error) reject(error); else resolve(value);
        }
        cancelDetection = function () { var error = new Error('Scanner closed.'); error.name = 'AbortError'; finish(error); };
        // Native detection cannot be aborted. Attach both settlement handlers
        // and retire the native detector after a timeout, so its late result
        // cannot trigger a lookup or create overlapping native requests.
        Promise.resolve(promise).then(function (value) { finish(null, value); }, function (error) { finish(error); });
      });
    }
    async function frame() {
      if (!current()) return;
      try {
        if (preview.readyState < 2 || !preview.videoWidth || !preview.videoHeight) return;
        count++;
        // Crop the usual scan area; periodically inspect the full frame for a
        // label outside the guide. Preserve horizontal detail for narrow bars.
        var full = count % 5 === 0, sourceWidth = preview.videoWidth, sourceHeight = preview.videoHeight;
        var cropHeight = full ? sourceHeight : Math.round(sourceHeight * 0.6);
        var scale = Math.min(1, 1280 / Math.max(sourceWidth, cropHeight));
        var width = Math.round(sourceWidth * scale), height = Math.round(cropHeight * scale);
        var rotate = !detector.native && count % 4 === 0;
        canvas.width = rotate ? height : width; canvas.height = rotate ? width : height;
        context.save();
        if (rotate) { context.translate(0, width); context.rotate(-Math.PI / 2); }
        context.drawImage(preview, 0, (sourceHeight - cropHeight) / 2, sourceWidth, cropHeight, 0, 0, width, height);
        context.restore();
        var code = '';
        if (detector.native) {
          try {
            var results = await detectNative(detector.native.detect(canvas));
            nativeErrors = 0;
            // Multiple labels in view require a deliberate rescan, not an
            // arbitrary first match that might select a neighbouring article.
            var codes = Array.from(new Set(results.map(function (result) { return String(result.rawValue || '').trim(); }).filter(Boolean)));
            if (codes.length === 1) code = codes[0];
            else if (codes.length > 1 && current()) status.textContent = 'More than one barcode is visible. Keep only the intended label in view.';
          } catch (error) {
            nativeErrors++;
            var timedOut = error.name === 'BarcodeDetectionTimeout';
            if ((timedOut || nativeErrors >= 2) && current()) {
              status.textContent = 'Switching scanner… Keep the label in view.';
              detector = await fallbackReader();
              if (timedOut && current()) {
                // A stuck platform decoder may still own its input image. Give
                // the fallback a fresh canvas rather than reusing that input.
                canvas = document.createElement('canvas');
                context = canvas.getContext('2d', { willReadFrequently: true });
                if (!context) throw new Error('canvas');
              }
            }
          }
        } else {
          try { code = detector.reader.decodeFromCanvas(canvas).getText(); }
          catch (error) { /* NotFound/Checksum/Format errors are normal between frames. */ }
        }
        if (!current()) return;
        if (code) { session.stop(); lookup(code); return; }
        if (count === 70) status.textContent = 'Move slightly farther away for focus, avoid glare and keep both ends of the barcode visible. You can also enter the barcode below.';
      } finally {
        // Schedule only after detection settles: slow phones never build a
        // queue of overlapping frame decodes or duplicate Shopify lookups.
        if (current()) timer = setTimeout(function () { frame().catch(failed); }, 120);
      }
    }
    function failed() {
      if (current()) { stopCamera(); status.textContent = 'Barcode scanning could not continue. Restart the camera or enter the barcode below.'; }
    }
    frame().catch(failed);
  }
  async function getCamera(token) {
    try {
      return await navigator.mediaDevices.getUserMedia({ video: { facingMode: { ideal: 'environment' }, width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: 24, max: 30 } }, audio: false });
    } catch (error) {
      // Some older tablets reject otherwise optional video constraints.
      if (error.name === 'OverconstrainedError' && token === cameraGeneration && dialog.open) return navigator.mediaDevices.getUserMedia({ video: { facingMode: { ideal: 'environment' } }, audio: false });
      throw error;
    }
  }
  async function startCamera() {
    invalidate(); stopCamera();
    var token = cameraGeneration;
    retry.disabled = true; status.textContent = 'Starting camera…';
    try {
      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) throw new Error('unsupported');
      // Request permission immediately, without waiting for a large decoder
      // download on a mobile connection. Native detection needs no download.
      var cameraPromise = getCamera(token);
      var detectorPromise = prepareDetector().catch(function (error) { return { error: error }; });
      var camera = await cameraPromise;
      if (token !== cameraGeneration || !dialog.open) { camera.getTracks().forEach(function (t) { t.stop(); }); return; }
      stream = camera;
      var preview = video.cloneNode(false); video.replaceWith(preview); video = preview;
      preview.muted = true; preview.setAttribute('playsinline', ''); preview.srcObject = camera;
      await deadline(preview.play(), 5000, 'preview');
      if (token !== cameraGeneration || !dialog.open) return;
      // Continuous focus is optional; unsupported devices retain their defaults.
      var track = camera.getVideoTracks ? camera.getVideoTracks()[0] : null;
      if (track && track.addEventListener) track.addEventListener('ended', function () {
        if (token === cameraGeneration && dialog.open) { stopCamera(); status.textContent = 'The camera stopped. Restart it or enter the barcode below.'; }
      }, { once: true });
      try {
        var capabilities = track && track.getCapabilities ? track.getCapabilities() : {};
        if (capabilities.focusMode && capabilities.focusMode.indexOf('continuous') !== -1 && track.applyConstraints) Promise.resolve(track.applyConstraints({ advanced: [{ focusMode: 'continuous' }] })).catch(function () {});
      } catch (e) { /* Camera capabilities are not available on every browser. */ }
      status.textContent = 'Preparing scanner… Keep the product label in view.';
      var detector = await detectorPromise;
      if (token !== cameraGeneration || !dialog.open) return;
      if (detector.error) throw detector.error;
      status.textContent = 'Hold one barcode steady inside the guide. Move back slightly if the label looks blurry.';
      retry.disabled = false; retry.textContent = 'Restart camera';
      scanFrames(preview, detector, token);
    } catch (e) {
      if (token !== cameraGeneration || !dialog.open) return;
      stopCamera();
      status.textContent = e.name === 'NotAllowedError' || e.name === 'SecurityError' ? 'Camera access was denied. Allow camera access in your browser, or enter the barcode below.' : e.name === 'NotFoundError' ? 'No camera was found. Use a handheld scanner or enter the barcode below.' : e.name === 'NotReadableError' ? 'The camera is busy in another app. Close it there and restart, or enter the barcode below.' : e.message === 'decoder' ? 'The scanner could not load. Check your connection and restart, or enter the barcode below.' : 'The camera could not start. Try again, or enter the barcode below.';
      input.focus();
    }
  }
  scan.onclick = function () { message.hidden = true; input.value = ''; status.textContent = ''; if (!dialog.open) dialog.showModal(); startCamera(); };
  retry.onclick = startCamera;
  dialog.querySelector('form').onsubmit = function (e) { e.preventDefault(); lookup(input.value); };
  updateClear();
})();
