(function (root, factory) {
  'use strict';
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.SankiManageTransactions = factory();
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';
  var editableKinds = ['transfer', 'receipt', 'expense', 'personal_expense'];
  function reference(value) {
    var valueText = String(value == null ? '' : value).trim();
    return /^[A-Za-z0-9][A-Za-z0-9:/_ -]{2,99}$/.test(valueText) ? valueText : '';
  }
  function dateValid(value) {
    return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value + 'T00:00:00Z')) && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value;
  }
  function amount(value) {
    var raw = String(value == null ? '' : value).trim(), n = Number(raw), cents = Math.round(n * 100);
    if (!/^\d+(?:\.\d{1,2})?$/.test(raw) || !Number.isFinite(n) || n <= 0 || !Number.isSafeInteger(cents) || Math.abs(n * 100 - cents) > 0.000001) throw Error('Enter a positive amount with no more than two decimal places.');
    return cents / 100;
  }
  function text(value) { return value == null ? '' : String(value); }
  function identifier(value) { return typeof value === 'string' && value.length > 0 && value.length <= 200 && !/[\u0000-\u001f\u007f]/.test(value); }
  function sourceWorkflowLink(destination) {
    if (!destination || typeof destination !== 'object') return '';
    if (destination.tab === 'cash' && ['SANKI', 'SAMAST', 'PERSONAL'].includes(destination.nature) && identifier(destination.account)) {
      if (destination.date && !dateValid(destination.date)) return '';
      return '/expenses.html?tab=cash&nature=' + encodeURIComponent(destination.nature) + '&account=' + encodeURIComponent(destination.account) + (destination.date ? '&ledgerFrom=' + encodeURIComponent(destination.date) + '&ledgerTo=' + encodeURIComponent(destination.date) : '');
    }
    if (destination.tab === 'creditcards' && identifier(destination.cardId)) {
      if (destination.sourceId != null && !identifier(destination.sourceId)) return '';
      return '/expenses.html?tab=creditcards&manageCard=' + encodeURIComponent(destination.cardId) + (destination.sourceId ? '&managePayment=' + encodeURIComponent(destination.sourceId) : '');
    }
    if (destination.tab === 'list' && identifier(destination.focus)) return '/expenses.html?tab=list&focus=' + encodeURIComponent(destination.focus);
    if (['refunds', 'receivables'].includes(destination.tab) && ['SANKI', 'SAMAST', 'PERSONAL'].includes(destination.nature) && identifier(destination.refundId || destination.id)) return '/expenses.html?tab=receivables&nature=' + encodeURIComponent(destination.nature) + '&manageRefund=' + encodeURIComponent(destination.refundId || destination.id);
    return '';
  }
  function actionLink(action) {
    if (!action || typeof action !== 'object') return '';
    if (action.type === 'source_workflow') return sourceWorkflowLink(action.destination);
    if (action.type === 'expense' && identifier(action.id)) return '/expenses.html?tab=list&focus=' + encodeURIComponent(action.id);
    if (action.type === 'credit_card' && identifier(action.cardId) && identifier(action.sourceId)) return '/expenses.html?tab=creditcards&manageCard=' + encodeURIComponent(action.cardId) + '&managePayment=' + encodeURIComponent(action.sourceId);
    if (action.type === 'refund' && identifier(action.id) && ['SANKI', 'SAMAST', 'PERSONAL'].includes(action.nature)) return '/expenses.html?tab=receivables&nature=' + encodeURIComponent(action.nature) + '&manageRefund=' + encodeURIComponent(action.id);
    return '';
  }
  function ledgerAction(action) {
    return !!(action && action.type === 'ledger_edit' && identifier(action.id) && editableKinds.includes(action.kind) && ['SANKI', 'SAMAST', 'PERSONAL'].includes(action.nature) && identifier(action.account));
  }
  function changesFor(entry, values) {
    var result = {}, nextAmount = amount(values.amount), nextDate = text(values.date), nextNote = text(values.note);
    if (!dateValid(nextDate)) throw Error('Enter a valid transaction date.');
    if (nextNote.length > 4000) throw Error('The note must be no more than 4,000 characters.');
    if (nextAmount !== Number(entry.amount)) result.amount = nextAmount;
    if (nextDate !== entry.date) result.date = nextDate;
    if (nextNote !== text(entry.note)) result.note = nextNote;
    if (entry.kind === 'receipt') {
      var source = text(values.source);
      if (source.length > 4000 || !source.trim() && text(entry.source).trim()) throw Error('Enter a receipt source of no more than 4,000 characters.');
      if (source !== text(entry.source)) result.source = source;
    }
    return result;
  }
  function create(doc, win, config, request, onBusy) {
    var initialized = false, generation = 0, editing = false, activeDialog = null;
    function el(id) { return doc.getElementById(id); }
    function node(tag, content, parent) {
      var item = doc.createElement(tag);
      if (content != null) item.textContent = String(content);
      if (parent) parent.appendChild(item);
      return item;
    }
    function button(label, parent, handler) {
      var item = node('button', label, parent); item.type = 'button'; item.className = 'button'; item.addEventListener('click', handler); return item;
    }
    function message(value, bad) { el('transactionMessage').textContent = value || ''; el('transactionMessage').className = 'message ' + (bad ? 'error' : 'success'); }
    function requestData(url, options) {
      return Promise.resolve().then(function () { return request(url, Object.assign({ cache: 'no-store' }, options || {})); }).then(function (data) {
        if (!data || data.success !== true) throw Error(data && data.error || 'The transaction could not be verified. Nothing was changed.');
        return data;
      });
    }
    function setEditing(value) { editing = value; el('transactionReference').disabled = value; el('findTransaction').disabled = value; if (onBusy) onBusy(value); }
    function search() {
      if (editing || activeDialog) return Promise.resolve(false);
      var query = reference(el('transactionReference').value), current = ++generation;
      el('transactionResults').replaceChildren();
      if (!query) { el('findTransaction').disabled = false; message('Enter a transaction reference with at least 3 characters, for example TR-00123, EX-00832/PAY-001 or 832.', true); return Promise.resolve(false); }
      message('Finding accessible transactions. No records are changed by this search.');
      el('findTransaction').disabled = true;
      return requestData('/api/expenses/transactions?reference=' + encodeURIComponent(query)).then(function (data) {
        if (current !== generation) return false;
        var rows = data.entries;
        if (!Array.isArray(rows)) throw Error('The transaction search returned an incomplete result. Refresh and try again.');
        rows.forEach(renderResult);
        message(rows.length ? (data.truncated === true ? 'Showing the first ' : '') + rows.length + ' matching transaction' + (rows.length === 1 ? '' : 's') + '. Choose the exact record. Approval, account-access and reconciliation safeguards still apply.' + (data.truncated === true ? '\nMore matches are available. Enter the complete reference to narrow this search.' : '') : 'No accessible transactions match that reference. Check the full reference; no record was created.');
        return true;
      }).catch(function (error) { if (current === generation) { el('transactionResults').replaceChildren(); message(error.message, true); } return false; }).finally(function () { if (current === generation) el('findTransaction').disabled = false; });
    }
    function renderResult(row) {
      if (!row || !identifier(row.id)) return;
      var box = node('article', null, el('transactionResults')); box.className = 'transaction-result';
      var kind = row.kind || row.type;
      node('h3', row.id + (kind ? ' · ' + text(kind) : ''), box);
      var summary = [text(row.date), row.amount != null && Number.isFinite(Number(row.amount)) ? '₹' + Number(row.amount).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '', text(row.nature), text(row.account)].filter(Boolean);
      node('p', summary.join(' · '), box);
      if (row.title || row.description) node('p', text(row.title || row.description), box);
      var action = row.action;
      var blockedReason = row.blockedReason || action && action.type === 'source_workflow' && action.reason;
      if (blockedReason) node('p', 'Protected: ' + text(blockedReason), box).className = 'hint';
      if (ledgerAction(action) && row.editable === true && !blockedReason) button('Review and modify transaction', box, function () { return openLedgerEditor(action); });
      else {
        var href = actionLink(action);
        if (href) { var link = node('a', action.type === 'expense' ? 'Open expense and edit controls' : action.type === 'credit_card' ? 'Open audited card-payment correction' : action.type === 'refund' ? 'Open audited refund correction' : 'Open protected source workflow', box); link.href = href; link.className = 'button'; }
        else if (!blockedReason) node('p', 'This entry must be reviewed in its source workflow. No independent edit is available.', box).className = 'hint';
      }
    }
    function openLedgerEditor(action) {
      if (editing || activeDialog || !ledgerAction(action)) return Promise.resolve(false);
      // Keep routing identity private and immutable; never read it from editable form fields.
      var identity = Object.freeze({ id: action.id, kind: action.kind, nature: action.nature, account: action.account });
      var dialog = node('dialog', null, doc.body), closed = false, busy = true, prepared = null, snapshot = null, fields = {};
      activeDialog = dialog; setEditing(true);
      dialog.className = 'transaction-editor';
      dialog.setAttribute('aria-label', 'Review and modify an existing transaction');
      dialog.style.cssText = 'width:min(850px,94vw);max-height:90vh;overflow:auto;padding:24px;border:1px solid #ccd5df;border-radius:14px;color:#152638;background:white';
      node('h2', 'Modify ' + identity.id, dialog);
      node('p', 'Only this existing transaction may be corrected. Accounts, entities, identifiers, approval decisions and reconciliation evidence are not editable here.', dialog);
      var status = node('p', 'Checking current permissions, source evidence, approvals and reconciliation locks…', dialog); status.setAttribute('role', 'status');
      var formHost = node('div', null, dialog), reviewHost = node('div', null, dialog), footer = node('div', null, dialog);
      footer.className = 'actions dialog-actions';
      var close = button('Close without saving', footer, function () { if (!busy) dialog.close(); });
      function notice(value, bad) { status.textContent = value; status.style.color = bad ? '#a11' : '#315a42'; }
      function clearReview() { prepared = null; reviewHost.replaceChildren(); }
      function busyState(value) {
        busy = value; close.disabled = value;
        Array.prototype.forEach.call(formHost.querySelectorAll('input,textarea,button'), function (item) { item.disabled = value; });
        Array.prototype.forEach.call(reviewHost.querySelectorAll('button'), function (item) { item.disabled = value; });
      }
      dialog.addEventListener('cancel', function (event) { if (busy) event.preventDefault(); });
      dialog.addEventListener('close', function () { closed = true; prepared = null; activeDialog = null; dialog.remove(); setEditing(false); });
      dialog.showModal(); busyState(true);
      var query = Object.keys(identity).map(function (key) { return encodeURIComponent(key) + '=' + encodeURIComponent(identity[key]); }).join('&');
      return requestData('/api/expenses/ledger-entry/edit-preview?' + query).then(function (data) {
        if (closed) return false;
        if (data.editable !== true || !data.entry || data.entry.id !== identity.id || data.entry.kind !== identity.kind || !data.scope || data.scope.nature !== identity.nature || data.scope.account !== identity.account || !identifier(data.fingerprint)) throw Error('The transaction identity or accounting scope changed. Close and search again; nothing was changed.');
        amount(data.entry.amount); if (!dateValid(data.entry.date)) throw Error('The saved transaction cannot be verified. Use its source workflow.');
        snapshot = Object.freeze({ id: data.entry.id, kind: data.entry.kind, amount: data.entry.amount, date: data.entry.date, source: text(data.entry.source), note: text(data.entry.note), fingerprint: data.fingerprint });
        buildForm(); notice('Current record loaded. Review the exact changes before saving; no changes have been made.', false); return true;
      }).catch(function (error) { if (!closed) notice(error.message, true); return false; }).finally(function () { if (!closed) busyState(false); });
      function buildForm() {
        var form = node('form', null, formHost); form.addEventListener('submit', function (event) { event.preventDefault(); });
        var specs = [['amount', 'Amount ₹', 'text'], ['date', 'Transaction date', 'date'], ['note', 'Transaction note', 'textarea']];
        if (identity.kind === 'receipt') specs.push(['source', 'Receipt source', 'text']);
        specs.push(['reason', 'Reason for correction (required)', 'textarea']);
        specs.forEach(function (spec) {
          var label = node('label', spec[1], form); label.style.cssText = 'display:block;margin:12px 0';
          var field = node(spec[2] === 'textarea' ? 'textarea' : 'input', null, label);
          if (spec[2] !== 'textarea') field.type = spec[2];
          field.setAttribute('aria-label', spec[1]); field.style.cssText = 'display:block;width:100%;box-sizing:border-box;padding:8px';
          field.value = spec[0] === 'reason' ? '' : text(snapshot[spec[0]]); field.maxLength = spec[0] === 'reason' ? 2000 : 4000;
          if (spec[0] === 'amount') field.inputMode = 'decimal';
          field.addEventListener('input', clearReview); field.addEventListener('change', clearReview); fields[spec[0]] = field;
        });
        button('Review changes — no records changed', form, review);
      }
      function review() {
        if (busy || closed || !snapshot) return;
        clearReview();
        try {
          var values = {}; Object.keys(fields).forEach(function (key) { values[key] = fields[key].value; });
          var changes = changesFor(snapshot, values), reason = text(values.reason).trim();
          if (!Object.keys(changes).length) throw Error('Change at least one transaction field before reviewing.');
          if (!reason || reason.length > 2000) throw Error('Enter a correction reason of no more than 2,000 characters.');
          prepared = Object.freeze(Object.assign({}, identity, changes, { fingerprint: snapshot.fingerprint, reason: reason }));
          node('h3', 'Review this exact correction', reviewHost);
          var table = node('table', null, reviewHost), head = node('tr', null, node('thead', null, table)), body = node('tbody', null, table);
          ['Field', 'Before', 'After'].forEach(function (label) { node('th', label, head); });
          Object.keys(changes).forEach(function (key) { var row = node('tr', null, body); [key, snapshot[key], changes[key]].forEach(function (value) { node('td', text(value), row); }); });
          node('p', identity.id + ' · ' + identity.nature + ' · ' + identity.account, reviewHost);
          node('p', 'Audit reason: ' + reason, reviewHost);
          node('p', 'Saving rechecks permissions, source evidence, approval and reconciliation locks. A changed record will be rejected, not overwritten.', reviewHost);
          button('Save reviewed correction', reviewHost, save);
          notice('Review only. No records have changed.', false);
        } catch (error) { clearReview(); notice(error.message, true); }
      }
      function save() {
        if (busy || closed || !prepared) return Promise.resolve(false);
        var payload = prepared; busyState(true); notice('Saving only the reviewed fields with fresh approval and reconciliation checks…', false);
        return requestData('/api/expenses/ledger-entry', { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }).then(function (result) {
          clearReview(); formHost.replaceChildren(); snapshot = null;
          notice(result.unchanged ? 'Nothing changed. The recorded transaction already matches.' : 'Correction saved with its audit trail. No reconciliation was opened or finalized.', false);
          // Refresh search only after the editor closes, preserving the success confirmation.
          dialog.addEventListener('close', function () { search(); });
          return true;
        }).catch(function (error) {
          clearReview(); formHost.replaceChildren(); snapshot = null;
          notice(error.message + ' Close this editor and search again to load a fresh record before any further correction.', true);
          return false;
        }).finally(function () { if (!closed) busyState(false); });
      }
    }
    function load() {
      if (!initialized) {
        initialized = true;
        el('findTransaction').addEventListener('click', function (event) { event.preventDefault(); search(); });
        el('transactionReference').addEventListener('keydown', function (event) { if (event.key === 'Enter') { event.preventDefault(); search(); } });
        el('transactionReference').addEventListener('input', function () { if (!editing) { generation++; el('transactionResults').replaceChildren(); el('findTransaction').disabled = false; message('Enter the full reference or its numeric part, then choose Find transaction.'); } });
        message('Find an existing transaction by reference. Searching never changes records; protected entries use their dedicated correction workflow.');
      }
      if (!editing && el('transactionReference').focus) el('transactionReference').focus();
      return Promise.resolve(true);
    }
    return { load: load, search: search, openLedgerEditor: openLedgerEditor };
  }
  return { create: create, reference: reference, dateValid: dateValid, amount: amount, actionLink: actionLink, sourceWorkflowLink: sourceWorkflowLink, changesFor: changesFor };
});
