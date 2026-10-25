(function (root, factory) {
  'use strict';
  var ui = factory();
  if (typeof module === 'object' && module.exports) module.exports = ui;
  else {
    root.SankiManageUI = ui;
    ui.createApp(root.document, root).init();
  }
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';
  function esc(value) { return String(value == null ? '' : value).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function cleanName(value) { return String(value || '').trim().replace(/\s+/g, ' '); }
  function safeReturn(value, origin) {
    if (!value || !/^\/(?!\/)/.test(value) || /[\\\u0000-\u001f]/.test(value)) return '';
    try { var url = new URL(value, origin); return url.origin === origin && url.pathname !== '/manage.html' ? url.pathname + url.search + url.hash : ''; } catch (_) { return ''; }
  }
  function createApp(doc, win, injectedRequest) {
    var state = { config: null, catalog: [], groups: [], revision: '', selected: new Set(), busy: false, loading: 0, tool: '', editAction: '', editSources: [], preview: null, operationId: '' };
    var openingController, transactionController;
    function el(id) { return doc.getElementById(id); }
    function message(id, text, bad) { el(id).textContent = text || ''; el(id).className = 'message ' + (bad ? 'error' : 'success'); }
    function request(url, options) {
      if (injectedRequest) return injectedRequest(url, options || {});
      var controller = new win.AbortController(), timer = win.setTimeout(function () { controller.abort(); }, 25000);
      return win.fetch(url, Object.assign({ cache: 'no-store', signal: controller.signal, credentials: 'same-origin' }, options || {})).then(function (response) {
        return response.text().then(function (raw) {
          var data;
          try { data = JSON.parse(raw); } catch (_) { throw Error('The server returned an unreadable response. Refresh and try again.'); }
          if (!response.ok || !data.success) { var error = Error(data.error || 'The request could not be completed.'); error.code = data.code; error.status = response.status; throw error; }
          return data;
        });
      }).catch(function (error) {
        if (error.name === 'AbortError') throw Error('The request timed out. Check the connection and retry; the same confirmation will not be applied twice.');
        throw error;
      }).finally(function () { win.clearTimeout(timer); });
    }
    function post(url, body) { return request(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(function (data) { if (!data || !data.success) { var error = Error(data && data.error || 'The request could not be completed.'); error.code = data && data.code; throw error; } return data; }); }
    function permitted() { return !!(state.config && state.config.claimantOnly === false && state.config.canManageCategories === true); }
    function setBusy(value) {
      state.busy = value;
      ['groupWorkspace', 'categoryWorkspace', 'groupReviewDialog'].forEach(function (id) { el(id).querySelectorAll('button,input,select,textarea').forEach(function (node) { node.disabled = value; }); });
      el('backToManage').disabled = value;
      el('confirmGroupChange').disabled = value || !state.preview;
      syncSelection();
    }
    function allItems() { return state.catalog.reduce(function (items, head) { return items.concat(head.items); }, []); }
    function renderCatalog() {
      var query = cleanName(el('manageSearch').value).toLowerCase();
      el('manageHeads').innerHTML = state.catalog.map(function (head) {
        var items = head.items.filter(function (item) { return !query || (head.title + ' ' + item.title + ' ' + item.description + ' ' + item.actions.join(' ')).toLowerCase().includes(query); });
        if (!items.length) return '';
        return '<section class="manage-head"><button type="button" class="head-toggle" data-head="' + esc(head.key) + '" aria-expanded="false" aria-controls="manage-head-' + esc(head.key) + '"><span>' + esc(head.title) + '</span><span aria-hidden="true">＋</span></button><div class="head-items" id="manage-head-' + esc(head.key) + '" hidden>' + items.map(function (item) {
          return '<article class="manage-item"><h3>' + esc(item.title) + '</h3><p class="muted">' + esc(item.description) + '</p><ul>' + item.actions.map(function (action) { return '<li>' + esc(action) + '</li>'; }).join('') + '</ul><a class="button" href="' + esc(item.href) + '"' + (['groups','categories','opening-balances'].includes(item.key) || item.key === 'transactions' && item.href === '/manage.html?section=transactions' ? ' data-tool="' + esc(item.key) + '"' : '') + '>Open ' + esc(item.title.replace(/^Manage /i, '').toLowerCase()) + ' →</a></article>';
        }).join('') + '</div></section>';
      }).join('') || '<p class="muted">No permitted tools match this search.</p>';
    }
    function syncSelection() {
      var count = state.selected.size;
      el('groupSelection').textContent = count ? count + ' group' + (count === 1 ? '' : 's') + ' selected' : 'Select a group to rename, or source groups to merge.';
      el('renameSelected').disabled = state.busy || !state.revision || count !== 1;
      el('mergeSelected').disabled = state.busy || !state.revision || count < 1;
      el('selectAllGroups').checked = state.groups.length > 0 && count === state.groups.length;
      el('selectAllGroups').indeterminate = count > 0 && count < state.groups.length;
    }
    function renderGroups() {
      var current = new Set(state.groups.map(function (group) { return group.name; }));
      state.selected.forEach(function (name) { if (!current.has(name)) state.selected.delete(name); });
      el('groupsTable').innerHTML = state.groups.map(function (group, index) {
        var categories = Array.isArray(group.categories) ? group.categories : [];
        return '<tr><td><input type="checkbox" data-select-group="' + index + '" aria-label="Select ' + esc(group.name) + '"' + (state.selected.has(group.name) ? ' checked' : '') + '></td><td class="group-name">' + esc(group.name) + (categories.length ? '<details class="group-detail"><summary>View categories</summary><ul>' + categories.map(function (category) { return '<li>' + esc(category.name) + ' <span class="muted">(' + Number(category.entryCount || 0) + ' entries)</span></li>'; }).join('') + '</ul></details>' : '') + '</td><td class="num">' + Number(group.categoryCount || 0) + '</td><td class="num">' + Number(group.entryCount || 0) + '</td><td>' + esc((group.businesses || []).join(', ') || 'Unused') + '</td><td><div class="actions"><button type="button" class="compact" data-rename-group="' + index + '">Rename</button><button type="button" class="compact" data-merge-group="' + index + '">Merge</button></div></td></tr>';
      }).join('') || '<tr><td colspan="6" class="muted">No groups yet. Add the first group above.</td></tr>';
      el('knownGroups').innerHTML = state.groups.map(function (group) { return '<option value="' + esc(group.name) + '">'; }).join('');
      syncSelection();
    }
    function acceptGroups(data) {
      if (!data || data.success !== true || !Array.isArray(data.groups) || typeof data.revision !== 'string') throw Error('The group catalog could not be loaded safely. Refresh and try again.');
      state.groups = data.groups.slice().sort(function (a, b) { return a.name.localeCompare(b.name); });
      state.revision = data.revision;
      if (state.config) state.config.categoryGroups = state.groups.map(function (group) { return group.name; });
      renderGroups();
    }
    function loadGroups() {
      if (!permitted() || state.busy) return Promise.resolve(false);
      var generation = ++state.loading;
      state.revision = ''; setBusy(true);
      return request('/api/expenses/category-groups').then(function (data) { if (generation !== state.loading) return false; acceptGroups(data); return true; }).catch(function (error) { if (generation === state.loading) { state.revision = ''; message('groupMessage', error.message, true); } return false; }).finally(function () { if (generation === state.loading) setBusy(false); });
    }
    function closeReview() { if (!state.busy) { el('groupReviewDialog').close(); state.preview = null; state.operationId = ''; } }
    function cancelEdit() { if (state.busy) return; state.editAction = ''; state.editSources = []; state.preview = null; state.operationId = ''; el('groupEditForm').hidden = true; }
    function beginEdit(action, names) {
      if (!permitted() || state.busy) return;
      if (!state.revision) return message('groupMessage', 'Refresh the group list before editing groups.', true);
      names = Array.from(new Set(names || [])).filter(function (name) { return state.groups.some(function (group) { return group.name === name; }); });
      if (!names.length || action === 'rename' && names.length !== 1) return message('groupMessage', action === 'rename' ? 'Select exactly one group to rename.' : 'Select at least one source group to merge.', true);
      state.editAction = action; state.editSources = names; state.preview = null; state.operationId = '';
      el('groupAddForm').hidden = true; el('groupEditForm').hidden = false;
      el('groupEditTitle').textContent = action === 'rename' ? 'Rename group' : 'Merge groups';
      el('groupEditSources').textContent = 'Source' + (names.length > 1 ? ' groups: ' : ' group: ') + names.join(' + ');
      el('finalGroupName').value = action === 'rename' ? names[0] : '';
      el('groupChangeReason').value = '';
      el('groupTargetHint').textContent = action === 'rename' ? 'A name already used by another group must be merged, not overwritten.' : 'Choose an existing surviving group that is not selected as a source. For a new final name, add that group first. Categories stay separate.';
      message('groupMessage', ''); el('finalGroupName').focus();
    }
    function uuid() {
      if (!win.crypto || !win.crypto.randomUUID) throw Error('A secure confirmation ID is unavailable. Refresh in a supported secure browser.');
      return win.crypto.randomUUID();
    }
    function previewChange() {
      if (!permitted() || state.busy || !state.editAction) return Promise.resolve(false);
      var target = cleanName(el('finalGroupName').value), reason = el('groupChangeReason').value.trim();
      if (!target || target.length > 120 || /[\p{C}\u2028\u2029]/u.test(el('finalGroupName').value)) { message('groupMessage', 'Enter a printable group name up to 120 characters.', true); return Promise.resolve(false); }
      if (!reason || reason.length > 1000) { message('groupMessage', 'Enter a reason for this change (up to 1,000 characters).', true); return Promise.resolve(false); }
      if (/[\p{C}\u2028\u2029]/u.test(reason)) { message('groupMessage', 'Enter a printable, single-line reason for this change.', true); return Promise.resolve(false); }
      if (state.editAction === 'merge') {
        var targetRow = state.groups.find(function (group) { return group.name.toLowerCase() === target.toLowerCase(); });
        if (!targetRow || state.editSources.includes(targetRow.name)) { message('groupMessage', 'Choose an existing final group that is not selected as a source. Add the final group first if needed.', true); return Promise.resolve(false); }
        target = targetRow.name;
      }
      if (!state.revision) { message('groupMessage', 'Refresh the group list before reviewing this change.', true); return Promise.resolve(false); }
      var body = { action: state.editAction, sources: state.editSources.slice(), target: target, reason: reason, revision: state.revision };
      setBusy(true); message('groupMessage', '');
      return post('/api/expenses/category-groups/preview', body).then(function (data) {
        if (typeof data.previewToken !== 'string' || !data.previewToken || typeof data.revision !== 'string' || !Array.isArray(data.sources) || !data.target) throw Error('The change preview is incomplete. Refresh and review it again.');
        state.preview = Object.assign({}, body, { action: data.action, sources: data.sources, target: data.target, reason: data.reason || body.reason, revision: data.revision, previewToken: data.previewToken });
        state.operationId = uuid();
        el('groupReviewTitle').textContent = data.action === 'rename' ? 'Review group rename' : 'Review group merge';
        var summary = [['From', data.sources.join(' + ')], ['Final group', data.target], ['Categories affected', Number(data.categoryCount || 0)], ['Linked entries', Number(data.entryCount || 0)], ['Businesses affected', (data.businesses || []).join(', ') || 'None — unused group'], ['Reason', body.reason]];
        el('groupPreviewSummary').innerHTML = summary.map(function (row) { return '<dt>' + esc(row[0]) + '</dt><dd>' + esc(row[1]) + '</dd>'; }).join('');
        el('groupPreviewWarnings').innerHTML = (data.warnings || []).map(function (warning) { return '<p class="hint">' + esc(warning) + '</p>'; }).join('');
        el('groupPreviewCategories').innerHTML = (data.categories || []).map(function (category) { return '<li>' + esc(typeof category === 'string' ? category : category.name) + '</li>'; }).join('') || '<li>No categories — empty group.</li>';
        message('groupReviewMessage', '');
        el('confirmGroupChange').textContent = data.already ? 'Confirm unchanged name' : data.action === 'rename' ? 'Confirm rename' : 'Confirm merge';
        el('groupReviewDialog').showModal();
        return true;
      }).catch(function (error) { message('groupMessage', error.message, true); return false; }).finally(function () { setBusy(false); });
    }
    function applyChange() {
      if (!permitted() || state.busy || !state.preview || !state.operationId) return Promise.resolve(false);
      var body = Object.assign({}, state.preview, { operationId: state.operationId });
      state.loading++;
      setBusy(true); message('groupReviewMessage', 'Saving reviewed change…');
      return post('/api/expenses/category-groups/apply', body).then(function (data) {
        state.selected.clear(); acceptGroups(data); state.editAction = ''; state.editSources = []; state.preview = null; state.operationId = '';
        el('groupEditForm').hidden = true; el('groupReviewDialog').close();
        message('groupMessage', (data.already ? 'Change already recorded. Final group: ' : 'Saved. Final group: ') + data.group + '. No financial transactions were changed.');
        return true;
      }).catch(function (error) {
        if (error.status === 409 || ['STALE_REVISION', 'PREVIEW_REQUIRED', 'STALE_PREVIEW', 'SOURCE_CHANGED', 'OPERATION_REUSED'].includes(error.code)) { state.preview = null; state.operationId = ''; }
        message('groupReviewMessage', error.message + (error.status === 409 || ['STALE_REVISION', 'PREVIEW_REQUIRED', 'STALE_PREVIEW'].includes(error.code) ? ' Close this review, refresh groups and review the change again.' : ' You can retry this same confirmation safely.'), true);
        return false;
      }).finally(function () { setBusy(false); });
    }
    function addGroup() {
      if (!permitted() || state.busy) return Promise.resolve(false);
      var raw = el('newGroupName').value, name = cleanName(raw);
      if (!name || name.length > 120 || /[\p{C}\u2028\u2029]/u.test(raw)) { message('groupMessage', 'Enter a printable group name up to 120 characters.', true); return Promise.resolve(false); }
      setBusy(true); message('groupMessage', '');
      state.loading++;
      return post('/api/expenses/category-groups', { name: name }).then(function (data) {
        el('newGroupName').value = ''; el('groupAddForm').hidden = true;
        message('groupMessage', data.already ? 'This group already exists. Using ' + data.group + ' rather than creating a duplicate.' : 'Group added: ' + data.group + '.');
        return true;
      }).catch(function (error) { message('groupMessage', error.message, true); return false; }).finally(function () { setBusy(false); }).then(function (saved) { if (saved) return loadGroups().then(function () { return true; }); return false; });
    }
    function renderCategories() {
      var groupByCategory = new Map();
      state.groups.forEach(function (group) { (group.categories || []).forEach(function (category) { groupByCategory.set(String(category.name).toLowerCase(), group.name); }); });
      var current = el('newCategoryGroup').value;
      var groups = state.groups.length ? state.groups.map(function (group) { return group.name; }) : state.config.categoryGroups || [];
      el('newCategoryGroup').innerHTML = groups.map(function (name) { return '<option value="' + esc(name) + '">' + esc(name) + '</option>'; }).join('');
      if (groups.includes(current)) el('newCategoryGroup').value = current;
      var type = el('newCategoryType').value || 'variable';
      el('newCategoryType').innerHTML = (state.config.types || []).map(function (name) { return '<option value="' + esc(name) + '">' + esc(name) + '</option>'; }).join(''); el('newCategoryType').value = type;
      el('categoriesTable').innerHTML = (state.config.ledgers || []).slice().sort(function (a, b) { return a.name.localeCompare(b.name); }).map(function (category) { return '<tr><td>' + esc(category.name) + '</td><td>' + esc(groupByCategory.get(category.name.toLowerCase()) || category.group || '—') + '</td><td>' + esc(category.type || '—') + '</td></tr>'; }).join('') || '<tr><td colspan="3">No categories available.</td></tr>';
    }
    function addCategory() {
      if (!permitted() || state.busy) return Promise.resolve(false);
      var name = el('newCategoryName').value.trim(), group = el('newCategoryGroup').value, type = el('newCategoryType').value;
      if (!name || name.length > 120 || !group || !(state.config.types || []).includes(type)) { message('categoryMessage', 'Enter a category name and choose an existing group and valid type.', true); return Promise.resolve(false); }
      setBusy(true); message('categoryMessage', '');
      return post('/api/expenses/custom-ledgers', { name: name, group: group, type: type }).then(function (data) {
        if (!data.ledger || !data.ledger.name) throw Error('The category was saved but its confirmation could not be read. Refresh before trying again.');
        if (!(state.config.ledgers || []).some(function (category) { return category.name.toLowerCase() === data.ledger.name.toLowerCase(); })) state.config.ledgers.push(data.ledger);
        el('newCategoryName').value = ''; renderCategories(); message('categoryMessage', data.already ? 'This category already exists: ' + data.ledger.name + '.' : 'Category added: ' + data.ledger.name + '.'); return true;
      }).catch(function (error) { message('categoryMessage', error.message, true); return false; }).finally(function () { setBusy(false); });
    }
    function showTool(section, updateUrl) {
      if (state.busy || !['groups', 'categories', 'opening-balances', 'transactions'].includes(section) || !allItems().some(function (item) { return item.key === section; })) return Promise.resolve(false);
      if (section === 'transactions' && !(state.config.isAdmin === true && state.config.claimantOnly === false && state.config.canEditLedgerTransactions === true)) return Promise.resolve(false);
      state.tool = section; el('manageHome').hidden = true; el('manageTool').hidden = false; el('groupWorkspace').hidden = section !== 'groups'; el('categoryWorkspace').hidden = section !== 'categories';
      el('openingWorkspace').hidden = section !== 'opening-balances';
      el('transactionWorkspace').hidden = section !== 'transactions';
      el('toolTitle').textContent = section === 'groups' ? 'Manage groups' : section === 'opening-balances' ? 'Manage opening balances' : 'Manage categories';
      el('toolDescription').textContent = section === 'groups' ? 'Organize shared expense groups with a reviewed, audited rename or merge.' : section === 'opening-balances' ? 'Account-ledger openings: stage first, then explicit Owner review. Nothing is posted on opening this screen.' : 'Add approved expense categories and assign their group. Shared by SANKI, SAMAST and PERSONAL.';
      if (updateUrl !== false) { var url = new URL(win.location.href); url.searchParams.set('section', section); win.history.replaceState(null, '', url.pathname + url.search + url.hash); }
      if (section === 'transactions') {
        el('toolTitle').textContent = 'Manage transactions';
        el('toolDescription').textContent = 'Find an existing reference, check its source and open the permitted editor. Searching or opening a form never changes a record.';
        if (!transactionController && win.SankiManageTransactions) transactionController = win.SankiManageTransactions.create(doc,win,state.config,request,function(value){state.busy=value;el('backToManage').disabled=value;});
        if (!transactionController) { message('pageMessage','Transaction tools could not load. Refresh this page.',true); return Promise.resolve(false); }
        return transactionController.load();
      }
      if (section === 'opening-balances') {
        if (!openingController && win.SankiOpeningBalances) openingController = win.SankiOpeningBalances.create(doc,win,state.config,request,function(value){state.busy=value;el('backToManage').disabled=value;});
        if (!openingController) { message('pageMessage','Opening-balance tools could not load. Refresh this page.',true); return Promise.resolve(false); }
        return openingController.load();
      }
      return loadGroups().then(function () { if (state.tool === 'categories') renderCategories(); return true; });
    }
    function showHome() {
      if (state.busy) return;
      state.tool = ''; el('manageHome').hidden = false; el('manageTool').hidden = true; cancelEdit(); el('groupAddForm').hidden = true; renderCatalog();
      var url = new URL(win.location.href); url.searchParams.delete('section'); win.history.replaceState(null, '', url.pathname + url.search + url.hash);
    }
    function bind() {
      el('manageSearch').addEventListener('input', renderCatalog);
      el('manageHeads').addEventListener('click', function (event) {
        var toggle = event.target.closest('[data-head]');
        if (toggle) { var open = toggle.getAttribute('aria-expanded') !== 'true'; el('manageHeads').querySelectorAll('[data-head]').forEach(function (button) { button.setAttribute('aria-expanded', 'false'); button.lastElementChild.textContent = '＋'; el(button.getAttribute('aria-controls')).hidden = true; }); toggle.setAttribute('aria-expanded', String(open)); toggle.lastElementChild.textContent = open ? '−' : '＋'; el(toggle.getAttribute('aria-controls')).hidden = !open; return; }
        var link = event.target.closest('[data-tool]'); if (link) { event.preventDefault(); showTool(link.dataset.tool); }
      });
      el('backToManage').onclick = showHome;
      el('showGroupAdd').onclick = function () { if (!permitted() || state.busy) return; cancelEdit(); el('groupAddForm').hidden = false; el('newGroupName').focus(); };
      el('cancelGroupAdd').onclick = function () { if (!state.busy) { el('groupAddForm').hidden = true; el('newGroupName').value = ''; } };
      el('saveNewGroup').onclick = addGroup;
      el('newGroupName').addEventListener('keydown', function (event) { if (event.key === 'Enter') { event.preventDefault(); addGroup(); } });
      el('renameSelected').onclick = function () { beginEdit('rename', Array.from(state.selected)); };
      el('mergeSelected').onclick = function () { beginEdit('merge', Array.from(state.selected)); };
      el('refreshGroups').onclick = function () { if (!state.busy) { cancelEdit(); loadGroups(); } };
      el('cancelGroupEdit').onclick = cancelEdit;
      el('previewGroupChange').onclick = previewChange;
      el('confirmGroupChange').onclick = applyChange;
      el('closeGroupReview').onclick = closeReview;
      el('groupReviewDialog').addEventListener('cancel', function (event) { if (state.busy) event.preventDefault(); });
      el('groupReviewDialog').addEventListener('close', function () { if (!state.busy) { state.preview = null; state.operationId = ''; } });
      el('selectAllGroups').onchange = function () { if (state.busy) return; state.selected.clear(); if (this.checked) state.groups.forEach(function (group) { state.selected.add(group.name); }); renderGroups(); };
      el('groupsTable').addEventListener('change', function (event) { if (state.busy || !event.target.matches('[data-select-group]')) return; var group = state.groups[Number(event.target.dataset.selectGroup)]; if (!group) return; if (event.target.checked) state.selected.add(group.name); else state.selected.delete(group.name); syncSelection(); });
      el('groupsTable').addEventListener('click', function (event) { var rename = event.target.closest('[data-rename-group]'), merge = event.target.closest('[data-merge-group]'), node = rename || merge; if (!node || state.busy) return; var group = state.groups[Number(rename ? node.dataset.renameGroup : node.dataset.mergeGroup)]; if (group) beginEdit(rename ? 'rename' : 'merge', [group.name]); });
      el('manageCategoryGroups').onclick = function () { showTool('groups'); };
      el('saveNewCategory').onclick = addCategory;
      el('refreshCategories').onclick = function () { if (!state.busy) win.location.reload(); };
      win.addEventListener('beforeunload', function (event) { if (state.busy) { event.preventDefault(); event.returnValue = ''; } });
    }
    function init() {
      bind();
      var params = new URL(win.location.href).searchParams, returnTo = safeReturn(params.get('returnTo'), win.location.origin);
      if (returnTo) { el('returnLink').href = returnTo; el('returnLink').hidden = false; }
      return Promise.all([request('/api/auth/me'), request('/api/modules')]).then(function (results) {
        var user = results[0], modules = results[1].modules || [];
        if (!user.success || !results[1].success) throw Error('Your management permissions could not be loaded.');
        return (modules.some(function (item) { return item.key === 'expenses' && item.status === 'live'; }) ? request('/api/expenses/config').catch(function () { return null; }) : Promise.resolve(null)).then(function (cfg) {
          state.config = cfg;
          state.catalog = win.SankiManageNavigation.buildCatalog({ modules: modules, user: user, expenseConfig: cfg });
          message('pageMessage', cfg === null && modules.some(function (item) { return item.key === 'expenses'; }) ? 'Expense tools are temporarily unavailable because their permissions could not be loaded. Refresh this page to retry.' : '', cfg === null && modules.some(function (item) { return item.key === 'expenses'; }));
          el('manageHome').hidden = false; renderCatalog();
          if (!state.catalog.length && !el('pageMessage').textContent) message('pageMessage', 'No management tools are available for your current access. Existing app pages remain unchanged.', true);
          var section = params.get('section'); if (section) return showTool(section, false);
          return true;
        });
      }).catch(function (error) { message('pageMessage', error.message, true); return false; });
    }
    return { init: init, state: state, renderCatalog: renderCatalog, loadGroups: loadGroups, beginEdit: beginEdit, previewChange: previewChange, applyChange: applyChange, addGroup: addGroup, addCategory: addCategory, showTool: showTool, showHome: showHome, closeReview: closeReview, acceptGroups: acceptGroups };
  }
  return { createApp: createApp, esc: esc, cleanName: cleanName, safeReturn: safeReturn };
});
