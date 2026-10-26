(function(root, factory) {
  var exported = factory();
  if (typeof module === 'object' && module.exports) module.exports = exported;
  else root.CreditCardPaymentCorrections = exported;
})(typeof window !== 'undefined' ? window : globalThis, function() {
  'use strict';
  function changesFor(entry, fields) {
    var changes = {};
    ['amount', 'date', 'reference', 'note'].forEach(function(key) {
      var current = entry[key] == null ? '' : String(entry[key]), next = String(fields[key] == null ? '' : fields[key]);
      if (current !== next) changes[key] = next;
    });
    return changes;
  }
  function open(options) {
    var document = options.document || window.document, api = options.api, cardId = options.cardId, selected, prepared, busy = false;
    var dialog = document.createElement('dialog');
    dialog.setAttribute('aria-label', 'Correct an existing credit-card payment');
    dialog.style.cssText = 'width:min(940px,94vw);max-height:90vh;overflow:auto;padding:24px;border:1px solid #ccd5df;border-radius:14px;color:#152638;background:white';
    function node(tag, text, parent) { var element = document.createElement(tag); if (text != null) element.textContent = text; if (parent) parent.appendChild(element); return element; }
    function button(label, parent, handler) { var element = node('button', label, parent); element.type = 'button'; element.className = 'btn ghost'; element.addEventListener('click', handler); return element; }
    var heading = node('h2', 'Correct an existing credit-card payment', dialog);
    node('p', 'This changes only one existing payment and its linked transfer/allocation amounts. It does not create a payment, alter statement evidence, or bypass Owner approval or reconciliation locks.', dialog);
    var message = node('p', '', dialog); message.setAttribute('role', 'status');
    var entriesHost = node('div', '', dialog), editor = node('div', '', dialog), previewHost = node('div', '', dialog), footer = node('div', '', dialog);
    var close = button('Close without applying', footer, function() { if (!busy) { dialog.close(); dialog.remove(); } });
    dialog.addEventListener('cancel', function(event) { if (busy) event.preventDefault(); });
    dialog.addEventListener('close', function() { dialog.remove(); });
    document.body.appendChild(dialog); dialog.showModal();
    function status(text, error) { message.textContent = text; message.style.color = error ? '#a11' : '#315a42'; }
    function busyState(value) { busy = value; close.disabled = value; Array.prototype.forEach.call(editor.querySelectorAll('input,textarea,button'), function(el) { el.disabled = value; }); }
    function clearPreview() { prepared = null; previewHost.replaceChildren(); }
    function showPreview(result, payload, apply) {
      clearPreview();
      if (result.unchanged) { status('No changed fields. Nothing will be saved.', false); return; }
      prepared = { fingerprint: result.fingerprint, changes: payload.changes, reason: payload.reason };
      node('h3', 'Review the exact correction before applying', previewHost);
      var table = node('table', null, previewHost), header = node('tr', null, node('thead', null, table));
      ['Field', 'Before', 'After'].forEach(function(label) { node('th', label, header); });
      var tbody = node('tbody', null, table), before = result.before.payment || result.before.transfer, after = result.after.payment || result.after.transfer;
      Object.keys(result.changes).forEach(function(key) { var row = node('tr', null, tbody); [key, before[key] == null ? '' : before[key], after[key] == null ? '' : after[key]].forEach(function(value) { node('td', String(value), row); }); });
      node('p', 'Existing transfer: ' + result.before.transfer.id + (result.before.payment ? ' · Existing payment: ' + result.before.payment.id : ' · Legacy transfer only; no new payment will be created.'), previewHost);
      if (result.after.allocations.length) node('p', 'Linked ownership allocations after correction: ' + result.after.allocations.map(function(row) { return row.beneficiaryNature + ' ₹' + Number(row.amount).toFixed(2); }).join(', '), previewHost);
      node('p', 'Audit reason: ' + result.reason, previewHost);
      button('Apply this audited correction', previewHost, function() { if (!busy && prepared) apply(); }).className = 'btn primary';
    }
    function choose(entry) {
      if (busy) return;
      selected = entry; editor.replaceChildren(); clearPreview();
      node('h3', entry.id + ' · ' + entry.nature + ' · ' + entry.account, editor);
      var form = node('form', null, editor), fields = {};
      form.addEventListener('submit', function(event) { event.preventDefault(); });
      [['amount', 'Amount ₹', 'text'], ['date', 'Payment date', 'date'], ['reference', 'Reference', 'text'], ['note', 'Payment note', 'textarea'], ['reason', 'Correction reason and source evidence (required)', 'textarea']].forEach(function(spec) {
        var label = node('label', spec[1], form); label.style.cssText = 'display:block;margin:10px 0';
        var field = node(spec[2] === 'textarea' ? 'textarea' : 'input', null, label); if (spec[2] !== 'textarea') field.type = spec[2];
        field.setAttribute('aria-label', spec[1]); field.style.cssText = 'display:block;width:100%;box-sizing:border-box;padding:8px'; field.value = spec[0] === 'reason' ? '' : entry[spec[0]] == null ? '' : entry[spec[0]];
        if (spec[0] === 'amount') field.inputMode = 'decimal';
        field.maxLength = spec[0] === 'reason' ? 2000 : spec[0] === 'reference' ? 300 : 4000;
        field.addEventListener('input', clearPreview); fields[spec[0]] = field;
      });
      node('p', entry.allocations.length > 1 ? 'This payment has multiple ownership allocations. Amount changes are blocked; identities and allocations cannot be reassigned here.' : 'Accounts, entities, proof files, identities and payment classification remain unchanged.', form);
      function endpoint(action) { return '/api/expenses/credit-cards/' + encodeURIComponent(cardId) + '/payment-corrections/' + encodeURIComponent(entry.id) + '/' + action; }
      function apply() {
        var payload = prepared; if (!payload || busy) return;
        busyState(true); status('Applying the reviewed correction with fresh lock and approval checks…', false);
        Array.prototype.forEach.call(previewHost.querySelectorAll('button'), function(el) { el.disabled = true; });
        api(endpoint('apply'), { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }).then(function(result) {
          if (!result.success) throw new Error(result.error || 'The correction could not be saved.');
          clearPreview(); editor.replaceChildren(); entriesHost.replaceChildren();
          status(result.unchanged ? 'Nothing changed.' : 'Correction saved with audit ' + result.audit.id + '. Existing reconciliation and statement evidence were preserved.', false);
          if (options.onApplied) options.onApplied(result);
        }).catch(function(error) { clearPreview(); status(error.message, true); }).finally(function() { busyState(false); });
      }
      button('Preview correction — no records changed', form, function() {
        if (busy) return;
        clearPreview();
        var values = {}; Object.keys(fields).forEach(function(key) { values[key] = fields[key].value; });
        var payload = { changes: changesFor(entry, values), reason: values.reason.trim() };
        if (!Object.keys(payload.changes).length) return status('Change at least one payment field before previewing.', true);
        if (payload.reason.length < 10) return status('Explain the correction in at least 10 characters.', true);
        busyState(true); status('Checking the source graph, permissions, approvals and reconciliation locks…', false);
        api(endpoint('preview'), { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }).then(function(result) {
          if (!result.success) throw new Error(result.error || 'The correction is blocked.');
          status('Preview only. No records have changed.', false); showPreview(result, payload, apply);
        }).catch(function(error) { status(error.message, true); }).finally(function() { busyState(false); });
      });
    }
    status('Loading existing payments without changing any records…', false);
    api('/api/expenses/credit-cards/' + encodeURIComponent(cardId) + '/payment-corrections', { cache: 'no-store' }).then(function(result) {
      if (!result.success) throw new Error(result.error || 'Payments are unavailable.');
      heading.textContent = 'Correct existing payments · ' + result.card.name;
      if (!result.entries.length) return status('No accessible recorded card payments or legacy settlement transfers were found.', false);
      if (options.sourceId) {
        var requested = result.entries.filter(function(entry) { return entry.id === options.sourceId; });
        if (requested.length !== 1) return status('The requested payment is unavailable or inaccessible. No other payment was selected.', true);
        if (!requested[0].editable) { node('strong', requested[0].id, entriesHost); return status('Locked: ' + requested[0].blockedReason, true); }
        status('Review this exact existing payment. No records have changed.', false);
        choose(requested[0]);
        return;
      }
      status('Choose an existing payment. Protected records explain why they are locked.', false);
      result.entries.forEach(function(entry) {
        var box = node('div', null, entriesHost); box.style.cssText = 'border:1px solid #d6dde6;padding:12px;margin:8px 0;border-radius:8px';
        node('strong', entry.id + (entry.amount == null ? '' : ' · ' + entry.date + ' · ₹' + Number(entry.amount).toFixed(2)), box);
        if (entry.account) node('p', entry.nature + ' · ' + entry.account, box);
        if (entry.editable) button('Review correction', box, function() { choose(entry); });
        else node('p', 'Locked: ' + entry.blockedReason, box);
      });
    }).catch(function(error) { status(error.message, true); });
    return dialog;
  }
  return { open: open, changesFor: changesFor };
});
