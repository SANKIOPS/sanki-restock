(function (root, factory) {
  'use strict';
  var api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.SankiManageClassification = api;
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';

  function esc(value) {
    return String(value == null ? '' : value).replace(/[&<>"']/g, function (character) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[character];
    });
  }
  function clean(value) { return String(value == null ? '' : value).trim().replace(/\s+/g, ' '); }
  function amount(value) { var number = Number(value); return Number.isFinite(number) ? number : 0; }
  function formatMoney(value) {
    try { return new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 2 }).format(amount(value)); }
    catch (_) { return '₹' + amount(value).toFixed(2); }
  }
  function keyOf(entity, category) { return String(entity || '') + '\u0000' + String(category || ''); }
  function rowKey(row) { return String(row && row.sourceType || 'expense') + ':' + String(row && (row.recordId || row.id) || ''); }
  function editable(row, field) {
    if (row && row.editable === false) return false;
    if (row && Array.isArray(row.editableFields)) return row.editableFields.indexOf(field) >= 0;
    return field !== 'entity';
  }
  function fieldLock(row, field) {
    return String(row && row.fieldLocks && row.fieldLocks[field] || row && row.readOnlyReason || (field === 'entity' ? 'Entity changes are unavailable until this record is rechecked.' : 'This field is controlled by its source workflow.'));
  }
  function hasWriteToken(row) {
    return Number.isInteger(Number(row && row.revision)) && Number(row.revision) >= 0 && clean(row && row.fingerprint).length > 0;
  }
  function sortCategories(rows) {
    return (Array.isArray(rows) ? rows : []).slice().sort(function (left, right) {
      return String(left.entity || '').localeCompare(String(right.entity || '')) || String(left.name || '').localeCompare(String(right.name || ''));
    });
  }
  function queryString(filters) {
    var params = [];
    function add(key, value) { if (value != null && String(value) !== '') params.push(encodeURIComponent(key) + '=' + encodeURIComponent(String(value))); }
    if (filters.entities && filters.entities.length && !filters.allEntities) add('entities', filters.entities.join(','));
    add('category', filters.category); add('from', filters.from); add('to', filters.to); add('search', filters.search);
    if (filters.page) add('page', filters.page); if (filters.limit) add('limit', filters.limit);
    return params.length ? '?' + params.join('&') : '';
  }

  function create(doc, win, config, request, onBusy) {
    var state = {
      initialized: false,
      active: 'setup',
      loaded: false,
      loading: false,
      generation: 0,
      allowedEntities: [],
      selectedEntities: new Set(),
      categoryChoices: {},
      categories: [],
      appliedFilters: null,
      open: null,
      details: null,
      selectedRows: new Map(),
      editing: null
    };
    function el(id) { return doc.getElementById(id); }
    function setMessage(id, text, bad) {
      var node = el(id); node.textContent = text || ''; node.className = id === 'classificationMessage' ? 'classification-status ' + (bad ? 'error' : '') : 'classification-dialog-message message ' + (bad ? 'error' : 'success');
    }
    function setBusy(value) {
      state.loading = value;
      ['classificationReviewPanel', 'classificationEditDialog', 'classificationBulkDialog'].forEach(function (id) {
        el(id).querySelectorAll('button,input,select,textarea').forEach(function (node) {
          if (value) {
            node.__classificationBusyDisabled = Boolean(node.disabled);
            node.disabled = true;
          } else {
            node.disabled = Boolean(node.__classificationBusyDisabled);
            delete node.__classificationBusyDisabled;
          }
        });
      });
      if (onBusy) onBusy(value);
    }
    function initialEntities() {
      var configured = (config.approvalNatures || config.natures || []).filter(Boolean);
      return Array.from(new Set(configured.map(String)));
    }
    function renderEntities() {
      var allSelected = state.allowedEntities.length > 0 && state.allowedEntities.every(function (entity) { return state.selectedEntities.has(entity); });
      el('classificationEntities').innerHTML = '<label><input type="checkbox" data-classification-entity="*"' + (allSelected ? ' checked' : '') + '>All</label>' + state.allowedEntities.map(function (entity) {
        return '<label><input type="checkbox" data-classification-entity="' + esc(entity) + '"' + (state.selectedEntities.has(entity) ? ' checked' : '') + '>' + esc(entity === 'PERSONAL' ? 'Gagan Sir Personal' : entity) + '</label>';
      }).join('');
    }
    function selectedEntityList() { return state.allowedEntities.filter(function (entity) { return state.selectedEntities.has(entity); }); }
    function currentFilters(extra) {
      var entities = selectedEntityList();
      return Object.assign({
        entities: entities,
        allEntities: entities.length === state.allowedEntities.length,
        search: clean(el('classificationSearch').value),
        from: el('classificationFrom').value,
        to: el('classificationTo').value
      }, extra || {});
    }
    function choiceRows(entity) {
      return (state.categoryChoices && Array.isArray(state.categoryChoices[entity]) ? state.categoryChoices[entity] : []).slice().sort(function (a, b) {
        return String(a.name || a).localeCompare(String(b.name || b));
      });
    }
    function renderCategoryOptions(node, entity, selected, optionalLabel) {
      var choices = choiceRows(entity), names = choices.map(function (row) { return String(row.name || row); });
      selected = String(selected || '');
      var html = optionalLabel != null
        ? '<option value=""' + (!selected ? ' selected' : '') + '>' + esc(optionalLabel) + '</option>'
        : (!selected ? '<option value="" disabled selected>Select category</option>' : '');
      if (selected && names.indexOf(selected) < 0) html += '<option value="' + esc(selected) + '" selected>' + esc(selected) + ' · recorded</option>';
      html += choices.map(function (row) {
        var name = String(row.name || row), suffix = row.group ? ' · ' + row.group : '';
        return '<option value="' + esc(name) + '"' + (name === selected ? ' selected' : '') + '>' + esc(name + suffix) + '</option>';
      }).join('');
      node.innerHTML = html;
      node.required = optionalLabel == null;
      node.value = selected;
    }
    function acceptPayload(data) {
      if (!data || data.success !== true || !Array.isArray(data.allowedEntities) || !Array.isArray(data.categories) || !data.categoryChoices || typeof data.categoryChoices !== 'object') throw Error('The classification review response is incomplete. Refresh and try again.');
      var previous = selectedEntityList();
      state.allowedEntities = Array.from(new Set(data.allowedEntities.map(String)));
      state.selectedEntities = new Set((previous.length ? previous : data.selectedEntities || state.allowedEntities).filter(function (entity) { return state.allowedEntities.indexOf(entity) >= 0; }));
      if (!state.selectedEntities.size) state.allowedEntities.forEach(function (entity) { state.selectedEntities.add(entity); });
      state.categoryChoices = data.categoryChoices;
      state.categories = sortCategories(data.categories);
      state.loaded = true;
      renderEntities();
    }
    function skeletonRows() {
      el('classificationTable').innerHTML = '<tr><td colspan="6"><div class="classification-skeleton"></div><div class="classification-skeleton"></div><div class="classification-skeleton"></div></td></tr>';
      el('classificationSummary').innerHTML = '';
    }
    function renderSummary() {
      var totals = state.categories.reduce(function (memo, row) { memo.count += Number(row.count || 0); memo.total += amount(row.total); return memo; }, { count: 0, total: 0 });
      el('classificationSummary').innerHTML = '<span class="classification-stat"><b>' + state.categories.length + '</b> categories</span><span class="classification-stat"><b>' + totals.count + '</b> expenses</span><span class="classification-stat"><b>' + esc(formatMoney(totals.total)) + '</b> recorded amount</span><span class="classification-stat">Highest amount appears first inside each category</span>';
    }
    function protectedCell(label, value) { return '<div><span>' + esc(label) + '</span><b>' + esc(value || '—') + '</b></div>'; }
    function renderExpenses() {
      var detail = state.details;
      if (!detail) return '<div class="classification-empty"><strong>Loading expenses</strong><span>Fetching only this category…</span><div class="classification-skeleton"></div><div class="classification-skeleton"></div></div>';
      if (detail.error) return '<div class="classification-empty"><strong>Could not load this category</strong><span>' + esc(detail.error) + '</span></div>';
      var expenses = detail.expenses || [];
      if (!expenses.length) return '<div class="classification-empty"><strong>No matching expenses</strong><span>Change the search or date filters and try again.</span></div>';
      var selectedVisible = expenses.length && expenses.every(function (row) { return state.selectedRows.has(rowKey(row)); });
      var rows = expenses.map(function (row) {
        var id = rowKey(row), proofText = Number(row.proofCount || 0) + ' proof' + (Number(row.proofCount || 0) === 1 ? '' : 's');
        return '<tr data-expense-id="' + esc(id) + '"><td data-label="Select"><input type="checkbox" data-select-classification="' + esc(id) + '" aria-label="Select ' + esc(row.reference || id) + '"' + (state.selectedRows.has(id) ? ' checked' : '') + '></td><td data-label="Date / reference"><b>' + esc(row.date || '—') + '</b><span class="classification-lock">Locked</span><div class="classification-reference">' + esc(row.reference || id) + '</div></td><td data-label="Vendor / narration"><b>' + esc(row.vendor || 'Unspecified vendor') + '</b><br><span class="muted">' + esc(row.narration || 'No narration') + '</span></td><td data-label="Classification"><b>' + esc(row.entity || '—') + '</b> ' + (!editable(row, 'entity') ? '<span class="classification-lock">Entity locked</span>' : '') + '<br><span>' + esc(row.category || 'Unclassified') + '</span></td><td data-label="Payment / evidence"><span class="classification-lock">Locked</span> ' + esc(row.account || 'Account not recorded') + '<br><span class="muted">' + esc(row.paymentMethod || 'Method not recorded') + ' · ' + esc(proofText) + ' · ' + esc(row.reconciled ? 'reconciled' : 'not reconciled') + '</span></td><td data-label="Amount" class="num"><b>' + esc(formatMoney(row.amount)) + '</b><br><span class="classification-lock">Locked</span></td><td data-label="Action" class="classification-row-actions"><button type="button" data-edit-classification="' + esc(id) + '">Correct labels</button></td></tr>';
      }).join('');
      var page = detail.pagination || { page: 1, pages: 1, total: expenses.length };
      return '<div class="table-wrap"><table class="classification-expense-table"><thead><tr><th class="check-cell"><input type="checkbox" data-select-visible aria-label="Select all visible expenses"' + (selectedVisible ? ' checked' : '') + '></th><th>Date / reference</th><th>Vendor / narration</th><th>Classification</th><th>Payment / evidence</th><th class="num">Amount</th><th>Action</th></tr></thead><tbody>' + rows + '</tbody></table></div><div class="classification-pagination"><span>Page ' + Number(page.page || 1) + ' of ' + Number(page.pages || 1) + ' · ' + Number(page.total || expenses.length) + ' expenses</span><button type="button" data-classification-page="' + (Number(page.page || 1) - 1) + '"' + (Number(page.page || 1) <= 1 ? ' disabled' : '') + '>Previous</button><button type="button" data-classification-page="' + (Number(page.page || 1) + 1) + '"' + (Number(page.page || 1) >= Number(page.pages || 1) ? ' disabled' : '') + '>Next</button></div>';
    }
    function renderCategories() {
      renderSummary();
      if (!state.categories.length) {
        el('classificationTable').innerHTML = '<tr><td colspan="6" class="classification-empty"><strong>No classified expenses found</strong><span>Try another entity, date range or search.</span></td></tr>';
        return;
      }
      var lastEntity = '';
      el('classificationTable').innerHTML = state.categories.map(function (row, index) {
        var entity = String(row.entity || 'Unassigned'), category = String(row.name || 'Unclassified'), key = keyOf(entity, category), open = state.open && state.open.key === key;
        var heading = entity !== lastEntity ? '<tr class="classification-entity-row"><th colspan="6">' + esc(entity === 'PERSONAL' ? 'Gagan Sir Personal' : entity) + '</th></tr>' : '';
        lastEntity = entity;
        return heading + '<tr><td>' + esc(entity) + '</td><td><span class="classification-category-name">' + esc(category) + '</span>' + (row.valid === false ? '<span class="classification-needs-review">Needs review</span>' : '') + '</td><td>' + esc(row.group || 'Ungrouped') + '<span class="classification-group">' + esc(row.type || '—') + '</span></td><td class="num">' + Number(row.count || 0) + '</td><td class="num"><b>' + esc(formatMoney(row.total)) + '</b></td><td><button type="button" class="classification-expand" data-open-classification="' + index + '" aria-expanded="' + (open ? 'true' : 'false') + '" aria-controls="classification-detail-' + index + '">' + (open ? 'Close expenses' : 'Review expenses') + ' →</button></td></tr>' + (open ? '<tr class="classification-detail-row"><td colspan="6"><div id="classification-detail-' + index + '" class="classification-detail">' + renderExpenses() + '</div></td></tr>' : '');
      }).join('');
    }
    function syncSelection() {
      var rows = Array.from(state.selectedRows.values()), total = rows.reduce(function (sum, row) { return sum + amount(row.amount); }, 0);
      el('classificationSelection').hidden = rows.length === 0;
      el('classificationSelectionCount').textContent = rows.length + ' expense' + (rows.length === 1 ? '' : 's') + ' selected';
      el('classificationSelectionAmount').textContent = rows.length ? formatMoney(total) + ' recorded amount — financial fields remain locked' : '';
    }
    function loadSummaries(successText) {
      if (state.loading) return Promise.resolve(false);
      var selected = selectedEntityList();
      if (!selected.length) { setMessage('classificationMessage', 'Select at least one entity.', true); return Promise.resolve(false); }
      var appliedFilters = currentFilters();
      state.appliedFilters = Object.assign({}, appliedFilters, { entities: appliedFilters.entities.slice() });
      var generation = ++state.generation;
      state.open = null; state.details = null; state.selectedRows.clear(); syncSelection(); skeletonRows(); setBusy(true); setMessage('classificationMessage', 'Loading category totals…');
      return request('/api/expenses/classification-review' + queryString(appliedFilters)).then(function (data) {
        if (generation !== state.generation) return false;
        acceptPayload(data); renderCategories(); setMessage('classificationMessage', successText || 'Categories are alphabetical by entity. Open one to review the highest recorded amounts first.'); return true;
      }).catch(function (error) {
        if (generation === state.generation) { state.categories = []; el('classificationTable').innerHTML = '<tr><td colspan="6" class="classification-empty"><strong>Classification review unavailable</strong><span>' + esc(error.message) + '</span></td></tr>'; setMessage('classificationMessage', error.message, true); }
        return false;
      }).finally(function () { if (generation === state.generation) setBusy(false); });
    }
    function loadCategory(summary, page) {
      var generation = ++state.generation;
      state.details = null; renderCategories(); setBusy(true);
      var baseFilters = state.appliedFilters || currentFilters();
      var detailFilters = Object.assign({}, baseFilters, { entities: [summary.entity], allEntities: false, category: summary.name, page: page || 1, limit: 50 });
      return request('/api/expenses/classification-review' + queryString(detailFilters)).then(function (data) {
        if (generation !== state.generation || !state.open || state.open.key !== keyOf(summary.entity, summary.name)) return false;
        if (!data || data.success !== true || !Array.isArray(data.expenses)) throw Error('The expense list could not be read safely.');
        state.details = { expenses: data.expenses.slice().sort(function (a, b) { return amount(b.amount) - amount(a.amount); }), pagination: data.pagination || null };
        if (data.categoryChoices) state.categoryChoices = data.categoryChoices;
        renderCategories(); syncSelection(); return true;
      }).catch(function (error) { if (state.open && state.open.key === keyOf(summary.entity, summary.name)) { state.details = { error: error.message, expenses: [] }; renderCategories(); } return false; }).finally(function () { if (generation === state.generation) setBusy(false); });
    }
    function activate(view, updateUrl) {
      view = view === 'review' ? 'review' : 'setup'; state.active = view;
      el('categorySetupTab').setAttribute('aria-selected', String(view === 'setup'));
      el('classificationReviewTab').setAttribute('aria-selected', String(view === 'review'));
      el('categorySetupPanel').hidden = view !== 'setup'; el('classificationReviewPanel').hidden = view !== 'review';
      if (updateUrl !== false) { var url = new URL(win.location.href); if (view === 'review') url.searchParams.set('workspace', 'review'); else url.searchParams.delete('workspace'); win.history.replaceState(null, '', url.pathname + url.search + url.hash); }
      // Category setup can change names, groups and active choices while this
      // workspace is hidden. Always refetch when Review is opened so an old
      // in-memory category list can never be used for a correction.
      if (view === 'review') return loadSummaries();
      return Promise.resolve(true);
    }
    function rowById(id) {
      id = String(id);
      return (state.details && state.details.expenses || []).find(function (row) { return rowKey(row) === id; }) || state.selectedRows.get(id);
    }
    function openEditor(id) {
      if (state.loading) return;
      var row = rowById(id); if (!row) return setMessage('classificationMessage', 'This expense is no longer in the loaded page. Refresh and try again.', true);
      state.editing = row;
      el('classificationEditTitle').textContent = 'Correct classification'; el('classificationEditSubtitle').textContent = (row.reference || row.id) + ' · ' + formatMoney(row.amount);
      el('classificationEditEntity').innerHTML = state.allowedEntities.map(function (entity) { return '<option value="' + esc(entity) + '"' + (entity === row.entity ? ' selected' : '') + '>' + esc(entity === 'PERSONAL' ? 'Gagan Sir Personal' : entity) + '</option>'; }).join('');
      el('classificationEditEntity').value = row.entity; el('classificationEditEntity').disabled = !editable(row, 'entity');
      el('classificationEntityHint').textContent = editable(row, 'entity') ? 'Entity can be corrected for this unprotected source.' : 'Locked: ' + fieldLock(row, 'entity');
      renderCategoryOptions(el('classificationEditCategory'), row.entity, row.category); el('classificationEditCategory').disabled = !editable(row, 'category');
      el('classificationEditVendor').value = row.vendor || ''; el('classificationEditNarration').value = row.narration || ''; el('classificationEditReason').value = '';
      el('classificationEditVendor').disabled = !editable(row, 'vendor'); el('classificationEditNarration').disabled = !editable(row, 'narration');
      var lockedLabels = [['entity', 'Entity'], ['category', 'Category'], ['vendor', 'Vendor'], ['narration', 'Narration']].filter(function (item) { return !editable(row, item[0]); }).map(function (item) { return item[1] + ': ' + fieldLock(row, item[0]); });
      el('classificationFieldAccessNote').textContent = lockedLabels.length ? 'Protected labels — ' + lockedLabels.join(' · ') : 'All four reporting labels can be corrected. Financial evidence remains protected.';
      el('classificationLockedFields').innerHTML = protectedCell('Amount', formatMoney(row.amount)) + protectedCell('Date', row.date) + protectedCell('Payment method', row.paymentMethod) + protectedCell('Paying account', row.account) + protectedCell('Proof evidence', Number(row.proofCount || 0) + ' file' + (Number(row.proofCount || 0) === 1 ? '' : 's')) + protectedCell('Reconciliation', row.reconciled ? 'Linked and protected' : 'Not reconciled');
      var hasEditableLabel = ['entity', 'category', 'vendor', 'narration'].some(function (field) { return editable(row, field); });
      el('classificationSaveEdit').disabled = !hasEditableLabel;
      setMessage('classificationEditMessage', hasEditableLabel ? '' : 'This source workflow protects every reporting label. Correct it from the source record instead.', !hasEditableLabel); el('classificationEditDialog').showModal();
      var focusTarget = ['classificationEditEntity', 'classificationEditCategory', 'classificationEditVendor', 'classificationEditNarration'].map(el).find(function (node) { return !node.disabled; });
      (focusTarget || el('classificationCloseEdit')).focus();
    }
    function closeEditor() { if (!state.loading) { el('classificationEditDialog').close(); state.editing = null; } }
    function patchOptions(body) { return { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }; }
    function saveEditor() {
      if (state.loading || !state.editing) return Promise.resolve(false);
      if (!hasWriteToken(state.editing)) { setMessage('classificationEditMessage', 'This row is missing its current safety token. Close the editor, refresh the category and try again.', true); return Promise.resolve(false); }
      var reason = el('classificationEditReason').value.trim(), body = { reason: reason };
      var nextEntity = el('classificationEditEntity').value;
      var nextCategory = el('classificationEditCategory').value;
      var nextVendor = clean(el('classificationEditVendor').value);
      var nextNarration = el('classificationEditNarration').value.trim();
      var entityChanged = editable(state.editing, 'entity') && nextEntity !== String(state.editing.entity || '');
      if (entityChanged && !nextCategory) { setMessage('classificationEditMessage', 'Select a category for the destination entity.', true); return Promise.resolve(false); }
      if (entityChanged) body.entity = nextEntity;
      if (editable(state.editing, 'category') && (nextCategory !== String(state.editing.category || '') || entityChanged)) body.category = nextCategory;
      if (editable(state.editing, 'vendor') && nextVendor !== clean(state.editing.vendor)) body.vendor = nextVendor;
      if (editable(state.editing, 'narration') && nextNarration !== String(state.editing.narration || '').trim()) body.narration = nextNarration;
      if (Object.keys(body).length === 1) { setMessage('classificationEditMessage', 'Change at least one editable reporting label before saving.', true); return Promise.resolve(false); }
      if (body.entity === '' || body.category === '' || body.vendor === '' || body.narration === '') { setMessage('classificationEditMessage', 'Complete every editable classification field.', true); return Promise.resolve(false); }
      if (!reason) { setMessage('classificationEditMessage', 'Enter a reason for this correction.', true); return Promise.resolve(false); }
      body.sourceType = state.editing.sourceType || 'expense';
      body.expectedRevision = Number(state.editing.revision);
      body.fingerprint = String(state.editing.fingerprint || '');
      var id = String(state.editing.recordId || state.editing.id); setBusy(true); setMessage('classificationEditMessage', 'Saving classification…');
      return request('/api/expenses/classification-review/' + encodeURIComponent(id), patchOptions(body)).then(function (data) {
        if (!data || data.success !== true || !data.expense) throw Error('The correction was saved but could not be confirmed. Refresh before retrying.');
        el('classificationEditDialog').close(); state.editing = null; setBusy(false); return loadSummaries('Classification corrected. Amount, date, payment and evidence were unchanged.');
      }).catch(function (error) { setMessage('classificationEditMessage', error.message, true); return false; }).finally(function () { if (state.loading) setBusy(false); });
    }
    function syncBulkCategories() {
      var chosen = el('classificationBulkEntity').value;
      var selected = Array.from(state.selectedRows.values());
      var entitySet = Array.from(new Set(selected.map(function (row) { return row.entity; })));
      var entity = chosen || (entitySet.length === 1 ? entitySet[0] : '');
      renderCategoryOptions(el('classificationBulkCategory'), entity, '', entity ? 'Keep each current category' : 'Choose one destination entity first');
      el('classificationBulkCategory').disabled = !entity || selected.some(function (row) { return !editable(row, 'category'); });
    }
    function openBulk() {
      if (state.loading || !state.selectedRows.size) return;
      el('classificationBulkSubtitle').textContent = state.selectedRows.size + ' selected expenses · ' + formatMoney(Array.from(state.selectedRows.values()).reduce(function (sum, row) { return sum + amount(row.amount); }, 0));
      el('classificationBulkEntity').innerHTML = '<option value="">Keep each current entity</option>' + state.allowedEntities.map(function (entity) { return '<option value="' + esc(entity) + '">' + esc(entity === 'PERSONAL' ? 'Gagan Sir Personal' : entity) + '</option>'; }).join('');
      var lockedEntities = Array.from(state.selectedRows.values()).filter(function (row) { return !editable(row, 'entity'); });
      var lockedCategories = Array.from(state.selectedRows.values()).filter(function (row) { return !editable(row, 'category'); });
      el('classificationBulkEntity').disabled = lockedEntities.length > 0;
      el('classificationBulkScopeNote').textContent = lockedEntities.length || lockedCategories.length ? (lockedEntities.length ? lockedEntities.length + ' selected expense' + (lockedEntities.length === 1 ? ' has' : 's have') + ' protected ownership. ' : '') + (lockedCategories.length ? lockedCategories.length + ' selected expense' + (lockedCategories.length === 1 ? ' has' : 's have') + ' a protected category. ' : '') + 'Clear protected rows before applying that correction.' : 'All selected expenses permit entity and category correction.';
      el('classificationBulkReason').value = ''; setMessage('classificationBulkMessage', ''); syncBulkCategories(); el('classificationBulkDialog').showModal();
    }
    function closeBulk() { if (!state.loading) el('classificationBulkDialog').close(); }
    function saveBulk() {
      if (state.loading || !state.selectedRows.size) return Promise.resolve(false);
      if (Array.from(state.selectedRows.values()).some(function (row) { return !hasWriteToken(row); })) { setMessage('classificationBulkMessage', 'One or more selected rows are stale or missing a safety token. Clear the selection, refresh the category and select them again.', true); return Promise.resolve(false); }
      var entity = el('classificationBulkEntity').value, category = el('classificationBulkCategory').value, reason = el('classificationBulkReason').value.trim(), changes = {};
      if (entity && Array.from(state.selectedRows.values()).some(function (row) { return !editable(row, 'entity'); })) { setMessage('classificationBulkMessage', 'Entity is protected on one or more selected expenses. Clear those rows before changing entity.', true); return Promise.resolve(false); }
      if (category && Array.from(state.selectedRows.values()).some(function (row) { return !editable(row, 'category'); })) { setMessage('classificationBulkMessage', 'Category is protected on one or more selected expenses. Clear those rows before changing category.', true); return Promise.resolve(false); }
      if (entity) changes.entity = entity; if (category) changes.category = category;
      if (!Object.keys(changes).length) { setMessage('classificationBulkMessage', 'Choose a new entity or category.', true); return Promise.resolve(false); }
      if (!reason) { setMessage('classificationBulkMessage', 'Enter a reason for this bulk correction.', true); return Promise.resolve(false); }
      var body = { records: Array.from(state.selectedRows.values()).map(function (row) { return { sourceType: row.sourceType || 'expense', recordId: String(row.recordId || row.id), expectedRevision: Number(row.revision), fingerprint: String(row.fingerprint || '') }; }), changes: changes, reason: reason };
      setBusy(true); setMessage('classificationBulkMessage', 'Saving selected classifications…');
      return request('/api/expenses/classification-review/bulk', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(function (data) {
        if (!data || data.success !== true || !Array.isArray(data.updated)) throw Error('The bulk correction could not be confirmed. Refresh before retrying.');
        var count = Number(data.count || data.updated.length); el('classificationBulkDialog').close(); state.selectedRows.clear(); setBusy(false); return loadSummaries(count + ' classifications corrected. Financial evidence was unchanged.');
      }).catch(function (error) { setMessage('classificationBulkMessage', error.message, true); return false; }).finally(function () { if (state.loading) setBusy(false); });
    }
    function bind() {
      el('categorySetupTab').onclick = function () { activate('setup'); };
      el('classificationReviewTab').onclick = function () { activate('review'); };
      el('classificationApplyFilters').onclick = function () { loadSummaries(); };
      el('classificationSearch').addEventListener('keydown', function (event) { if (event.key === 'Enter') { event.preventDefault(); loadSummaries(); } });
      el('classificationEntities').addEventListener('change', function (event) {
        var checkbox = event.target, entity = checkbox && checkbox.getAttribute && checkbox.getAttribute('data-classification-entity'); if (!entity) return;
        if (entity === '*') { state.selectedEntities.clear(); if (checkbox.checked) state.allowedEntities.forEach(function (name) { state.selectedEntities.add(name); }); }
        else if (checkbox.checked) state.selectedEntities.add(entity); else state.selectedEntities.delete(entity);
        if (!state.selectedEntities.size && state.allowedEntities.length) { state.selectedEntities.add(state.allowedEntities[0]); setMessage('classificationMessage', 'At least one entity must remain selected.', true); }
        renderEntities();
      });
      el('classificationTable').addEventListener('click', function (event) {
        var open = event.target.closest && event.target.closest('[data-open-classification]');
        if (open && !state.loading) { var summary = state.categories[Number(open.getAttribute('data-open-classification'))]; if (!summary) return; var key = keyOf(summary.entity, summary.name); if (state.open && state.open.key === key) { state.open = null; state.details = null; renderCategories(); } else { state.open = { key: key, summary: summary }; loadCategory(summary, 1); } return; }
        var edit = event.target.closest && event.target.closest('[data-edit-classification]'); if (edit) openEditor(edit.getAttribute('data-edit-classification'));
        var page = event.target.closest && event.target.closest('[data-classification-page]'); if (page && !page.disabled && state.open) loadCategory(state.open.summary, Number(page.getAttribute('data-classification-page')));
      });
      el('classificationTable').addEventListener('change', function (event) {
        var target = event.target;
        if (target.matches && target.matches('[data-select-classification]')) { var id = String(target.getAttribute('data-select-classification')), row = rowById(id); if (target.checked && row) state.selectedRows.set(id, row); else state.selectedRows.delete(id); syncSelection(); return; }
        if (target.matches && target.matches('[data-select-visible]') && state.details) { (state.details.expenses || []).forEach(function (row) { var id = rowKey(row); if (target.checked) state.selectedRows.set(id, row); else state.selectedRows.delete(id); }); renderCategories(); syncSelection(); }
      });
      el('classificationClearSelection').onclick = function () { state.selectedRows.clear(); renderCategories(); syncSelection(); };
      el('classificationBulkEdit').onclick = openBulk;
      el('classificationEditEntity').onchange = function () { renderCategoryOptions(el('classificationEditCategory'), this.value, ''); };
      el('classificationSaveEdit').onclick = saveEditor; el('classificationCloseEdit').onclick = closeEditor;
      el('classificationBulkEntity').onchange = syncBulkCategories; el('classificationSaveBulk').onclick = saveBulk; el('classificationCloseBulk').onclick = closeBulk;
      ['classificationEditDialog', 'classificationBulkDialog'].forEach(function (id) { el(id).addEventListener('cancel', function (event) { if (state.loading) event.preventDefault(); }); });
    }
    function init() {
      if (state.initialized) return Promise.resolve(true);
      state.initialized = true; state.allowedEntities = initialEntities(); state.allowedEntities.forEach(function (entity) { state.selectedEntities.add(entity); }); renderEntities(); bind(); syncSelection();
      var params = new URL(win.location.href).searchParams;
      return activate(params.get('workspace') === 'review' ? 'review' : 'setup', false);
    }
    return { init: init, activate: activate, load: loadSummaries, loadCategory: loadCategory, state: state, renderCategories: renderCategories, openEditor: openEditor, saveEditor: saveEditor, openBulk: openBulk, saveBulk: saveBulk };
  }

  return { create: create, esc: esc, clean: clean, amount: amount, formatMoney: formatMoney, keyOf: keyOf, rowKey: rowKey, editable: editable, fieldLock: fieldLock, hasWriteToken: hasWriteToken, sortCategories: sortCategories, queryString: queryString };
});
