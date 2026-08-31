(function () {
  var base = window.SankiProcV2Base;
  if (!base) return;

  var state = base.state;
  var plan = { sizes: {}, slots: [] };
  var submitting = false;
  var $ = function (selector, parent) {
    return (parent || document).querySelector(selector);
  };
  var esc = function (value) {
    return String(value == null ? '' : value).replace(/[&<>\"]/g, function (character) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[character];
    });
  };

  $('h1').textContent = 'Fresh Procurement V2';
  $('.sub').textContent = 'The complete existing workflow, controlled by your editable design, colourway and size-pack sourcing table.';
  $('.legacy').innerHTML = 'Trial workspace — existing system is untouched.<br><a href="/fresh-procurement-simple.html">Open existing Fresh Procurement</a>';

  var css = document.createElement('style');
  css.textContent = '.v2box{margin-top:20px;border-top:1px solid var(--line);padding-top:18px}.v2bar{display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap}.v2metrics{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}.v2tablewrap{overflow:auto}.v2table{border-collapse:collapse;min-width:1050px;width:100%}.v2table th,.v2table td{border:1px solid var(--line);padding:7px}.v2table th{font-size:10px;text-transform:uppercase;color:var(--muted);background:#eef3ef}.v2table input{width:100%;min-width:95px;padding:8px;border:1px solid var(--line);border-radius:7px}.v2size{display:flex;gap:7px;flex-wrap:wrap;margin:9px 0}.v2size label{display:grid;grid-template-columns:45px 58px;align-items:center;gap:4px}.v2size input{width:58px;padding:7px}.v2remove{border:0;background:#f1e6e2;color:var(--red);border-radius:7px;padding:7px;cursor:pointer}.v2upload{border:1px dashed var(--line);border-radius:10px;padding:11px;background:#faf8f1}.v2formula{background:#eff6f1;border-left:4px solid var(--green);padding:10px;margin:10px 0;border-radius:6px}.v2validation{min-height:20px;margin-top:8px}.v2validation.error{color:var(--red)}';
  document.head.appendChild(css);

  var oldRecipe = $('.recipe');
  var create = $('#createPlan');
  oldRecipe.style.display = 'none';
  var box = document.createElement('div');
  box.className = 'v2box';
  box.innerHTML = '<div class="v2bar"><div><h3 style="margin:0">Detailed sourcing requirement</h3><div class="meta">Upload Excel/CSV or enter the table manually. Every field stays editable.</div></div><div class="v2upload"><input id="v2file" type="file" accept=".xlsx,.xls,.csv,.tsv"> <button class="btn secondary" id="v2import" type="button">Upload table</button></div></div><div class="v2metrics" id="v2metrics"></div><div><b>Size pack per colourway</b><div class="v2size" id="v2sizes"></div><button class="add" id="v2addsize" type="button">＋ Add size</button></div><div class="v2formula">Total pieces = total colourways × size-pack quantity. Changing estimated price or budget does not silently change quantities.</div><div class="v2tablewrap"><table class="v2table"><thead><tr><th>No.</th><th>Garment type</th><th>Specific design required</th><th>Pattern / surface</th><th>Colour 1</th><th>Colour 2</th><th>Qty / colour</th><th>Total</th><th></th></tr></thead><tbody id="v2rows"></tbody></table></div><button class="add" id="v2add" type="button">＋ Add requirement row</button><div id="v2status" class="meta"></div>';
  oldRecipe.parentNode.insertBefore(box, oldRecipe);
  create.textContent = 'Approve requirement table & add vendor photos →';
  create.insertAdjacentHTML('afterend', '<div id="v2validation" class="v2validation meta" aria-live="polite"></div>');

  function setQty() {
    return Object.keys(plan.sizes || {}).reduce(function (quantity, size) {
      return quantity + (parseInt(plan.sizes[size]) || 0);
    }, 0);
  }

  function validationMessage() {
    if (!plan.slots.length) return 'Add or upload at least one requirement row.';
    if (!setQty()) return 'Enter at least one size-pack quantity.';
    return '';
  }

  function syncApprovalState() {
    var message = validationMessage();
    var validation = $('#v2validation');
    create.disabled = submitting || Boolean(message);
    create.title = message;
    validation.textContent = message;
    validation.className = 'v2validation meta' + (message ? ' error' : '');
  }

  function metrics() {
    var colourways = plan.slots.reduce(function (count, slot) {
      return count + (slot.colours || []).filter(Boolean).length;
    }, 0);
    var quantity = setQty();
    var pieces = colourways * quantity;
    var cost = +plan.landedCost || 0;
    $('#v2metrics').innerHTML = '<span class="pill good">' + plan.slots.length + ' designs</span><span class="pill">' + colourways + ' colourways</span><span class="pill">' + quantity + ' pieces / colour</span><span class="pill good">' + pieces + ' total pieces</span><span class="pill">₹' + (pieces * cost).toLocaleString('en-IN') + ' estimated</span>';
    document.querySelectorAll('[data-v2q]').forEach(function (element) {
      element.textContent = quantity;
    });
    document.querySelectorAll('[data-v2total]').forEach(function (element) {
      var slot = plan.slots[+element.dataset.v2total];
      element.textContent = (slot.colours || []).filter(Boolean).length * quantity;
    });
    syncApprovalState();
  }

  function renderSizes() {
    var holder = $('#v2sizes');
    holder.innerHTML = '';
    Object.keys(plan.sizes || {}).forEach(function (size) {
      var label = document.createElement('label');
      label.innerHTML = '<b>' + esc(size) + '</b><input type="number" min="0" value="' + plan.sizes[size] + '">';
      label.querySelector('input').oninput = function () {
        plan.sizes[size] = +this.value || 0;
        metrics();
      };
      holder.appendChild(label);
    });
    metrics();
  }

  function renderRows() {
    var holder = $('#v2rows');
    holder.innerHTML = '';
    plan.slots.forEach(function (slot, index) {
      var colours = slot.colours || [];
      var row = document.createElement('tr');
      row.innerHTML = '<td>' + (index + 1) + '</td><td><input value="' + esc(slot.type) + '"></td><td><input value="' + esc(slot.design) + '"></td><td><input value="' + esc(slot.surface) + '"></td><td><input value="' + esc(colours[0] || '') + '"></td><td><input value="' + esc(colours[1] || '') + '"></td><td data-v2q>' + setQty() + '</td><td data-v2total="' + index + '">0</td><td><button class="v2remove" type="button">×</button></td>';
      var inputs = row.querySelectorAll('input');
      inputs.forEach(function (input) {
        input.oninput = function () {
          slot.type = inputs[0].value;
          slot.design = inputs[1].value;
          slot.surface = inputs[2].value;
          slot.colours = [inputs[3].value.trim(), inputs[4].value.trim()].filter(Boolean);
          metrics();
        };
      });
      row.querySelector('button').onclick = function () {
        plan.slots.splice(index, 1);
        renderRows();
      };
      holder.appendChild(row);
    });
    metrics();
  }

  function hydrate(savedPlan) {
    plan = savedPlan || { sizes: {}, slots: [] };
    $('#gender').value = plan.gender || 'Women';
    $('#category').value = plan.category || 'Trouser';
    $('#planName').value = plan.name || '';
    renderSizes();
    renderRows();
  }

  // These fields still have legacy Fresh Procurement listeners attached. Run the
  // V2 validator after those listeners so editing plan details never leaves this
  // button disabled by the hidden "generate AI plan again" workflow.
  ['gender', 'category', 'styleTarget', 'launchDate', 'budget', 'planName'].forEach(function (id) {
    $('#' + id).addEventListener('change', syncApprovalState);
  });

  $('#v2add').onclick = function () {
    plan.slots.push({ id: 'slot-' + Date.now(), type: '', design: '', surface: '', colours: [''] });
    renderRows();
  };
  $('#v2addsize').onclick = function () {
    var size = prompt('Size label:');
    if (size && plan.sizes[size] == null) {
      plan.sizes[size] = 0;
      renderSizes();
    }
  };
  $('#v2import').onclick = async function () {
    var file = $('#v2file').files[0];
    if (!file) {
      alert('Choose an Excel or CSV table.');
      return;
    }
    var formData = new FormData();
    formData.append('file', file);
    $('#v2status').textContent = 'Reading table…';
    try {
      var response = await fetch('/api/casuals/v2-plan/import', { method: 'POST', body: formData });
      var result = await response.json();
      if (!result.success) throw Error(result.error);
      hydrate(result.plan);
      $('#v2status').textContent = '✓ ' + result.importedRows + ' requirement rows imported. Review and edit before approval.';
    } catch (error) {
      $('#v2status').textContent = 'Could not import: ' + error.message;
    }
  };

  create.onclick = async function () {
    var message = validationMessage();
    if (message) {
      syncApprovalState();
      return;
    }
    submitting = true;
    syncApprovalState();
    try {
      plan.name = $('#planName').value || 'Procurement V2 plan';
      plan.gender = $('#gender').value;
      plan.category = $('#category').value;
      var saveResponse = await fetch('/api/casuals/v2-plan', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(plan)
      });
      var saved = await saveResponse.json();
      if (!saved.success) throw Error(saved.error || 'Could not save requirement');
      plan = saved.plan;
      var requirements = plan.slots.map(function (slot, index) {
        return {
          id: slot.id,
          slotName: 'R' + (index + 1) + ' · ' + slot.type + ' · ' + slot.design,
          type: slot.type,
          design: slot.design,
          surface: slot.surface,
          colours: slot.colours,
          sizes: plan.sizes
        };
      });
      var mix = requirements.map(function (requirement) {
        return {
          name: requirement.slotName,
          styles: 1,
          role: 'Requirement',
          reason: [requirement.design, requirement.surface].filter(Boolean).join(' · '),
          colourDirection: requirement.colours.join(', ')
        };
      });
      var pieces = plan.slots.reduce(function (count, slot) {
        return count + slot.colours.length * setQty();
      }, 0);
      var planInput = {
        gender: plan.gender,
        category: plan.category,
        styleTarget: plan.slots.length,
        budget: +$('#budget').value || pieces * (plan.landedCost || 0),
        launchDate: $('#launchDate').value,
        name: plan.name,
        mix: mix,
        requirements: requirements
      };
      var batchResponse = await fetch('/api/casuals/batches', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ name: planInput.name, categories: [planInput.category] })
      });
      var batchResult = await batchResponse.json();
      if (!batchResult.success) throw Error(batchResult.error || 'Could not create batch');
      state.batch = batchResult.batch;
      state.planInput = planInput;
      state.aiPlanReady = true;
      localStorage.setItem('sanki_v2_current', JSON.stringify({ step: 2, batch: batchResult.batch, planInput: planInput }));
      base.addVendor();
      base.go(2);
    } catch (error) {
      alert(error.message);
    } finally {
      submitting = false;
      syncApprovalState();
    }
  };

  syncApprovalState();
  fetch('/api/casuals/v2-plan')
    .then(function (response) { return response.json(); })
    .then(function (result) { hydrate(result.plan); })
    .catch(function (error) {
      $('#v2status').textContent = error.message;
      syncApprovalState();
    });
})();
