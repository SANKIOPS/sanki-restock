(function (root, factory) {
  'use strict';
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.SankiOpeningBalances = factory();
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';
  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]; }); }
  function money(v) { return v == null ? 'Not set (defaults to ₹0.00)' : '₹' + Number(v).toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2}); }
  function create(doc, win, config, request, onBusy) {
    var rows = [], busy = false, canApply = false, loaded = false;
    function el(id) { return doc.getElementById(id); }
    function message(text, bad) { el('openingMessage').textContent = text || ''; el('openingMessage').className = 'message ' + (bad ? 'error' : 'success'); }
    function row() { return rows.find(function (item) { return item.account === el('openingAccount').value; }); }
    function setBusy(value) { busy = value; el('openingWorkspace').querySelectorAll('button,input,select,textarea').forEach(function (node) { node.disabled = value; }); if (onBusy) onBusy(value); }
    function permitted() { return config && config.claimantOnly === false && config.isAdmin === true; }
    function validRow(item) { return !!(item && typeof item.account === 'string' && item.account && typeof item.revision === 'string' && item.revision && item.current && (item.current.amount === null || typeof item.current.amount === 'number' && Number.isFinite(item.current.amount)) && typeof item.current.effectiveDate === 'string' && Array.isArray(item.drafts) && (!item.proposal || typeof item.proposal.id === 'string' && typeof item.proposal.amount === 'number' && Number.isFinite(item.proposal.amount) && typeof item.proposal.effectiveDate === 'string')); }
    function render() {
      var item = row();
      el('openingAcknowledge').checked = false;
      el('openingForm').hidden = !item || !!item.reason;
      el('openingOwnerReview').hidden = !item || !item.proposal || !canApply || !!item.reason;
      el('openingReconcileLink').hidden = !item;
      if (!item) { el('openingSummary').innerHTML = ''; return; }
      var proposal = item.proposal;
      el('openingReconcileLink').href = '/expenses.html?tab=cash&nature=' + encodeURIComponent(el('openingNature').value) + '&account=' + encodeURIComponent(item.account);
      var summary = [['Recorded opening', money(item.current.amount)], ['Recorded effective date', item.current.effectiveDate || 'Not set'], ['Saved opening draft', proposal ? money(proposal.amount) + ' from ' + proposal.effectiveDate + ' · staged by ' + proposal.stagedBy : 'None — no balance change staged here']];
      if (proposal) summary.push(['Draft source / reason', proposal.reason]);
      (item.drafts || []).forEach(function (draft) { summary.push(['Reconciliation draft ' + draft.from + ' to ' + draft.to, draft.openingResolution ? money(draft.openingResolution.amount) + ' · opening / carry-forward staged only · ' + draft.openingResolution.reason : 'No opening / carry-forward correction staged']); });
      if (item.reason) summary.push(['Protected workflow', item.reason]);
      else if (proposal && proposal.baseRevision !== item.baseRevision) summary.push(['Review required', 'Ledger changed since staging. Save a fresh draft before Owner apply.']);
      el('openingSummary').innerHTML = summary.map(function (entry) { return '<dt>' + esc(entry[0]) + '</dt><dd>' + esc(entry[1]) + '</dd>'; }).join('');
      el('openingAmount').value = proposal ? proposal.amount : item.current.amount == null ? '' : item.current.amount;
      el('openingDate').value = proposal ? proposal.effectiveDate : item.current.effectiveDate || '';
      el('openingReason').value = proposal ? proposal.reason : '';
    }
    function accept(data) {
      if (!data || data.success !== true || !Array.isArray(data.rows) || !data.rows.every(validRow)) throw Error('Opening balances could not be loaded safely. Refresh and try again.');
      rows = data.rows; canApply = data.canApply === true && config.isOwner === true;
      var selected = el('openingAccount').value;
      el('openingAccount').innerHTML = rows.map(function (item) { return '<option value="' + esc(item.account) + '">' + esc(item.account) + '</option>'; }).join('');
      if (rows.some(function (item) { return item.account === selected; })) el('openingAccount').value = selected;
      loaded = true; render();
    }
    function load() {
      if (!permitted() || busy) return Promise.resolve(false);
      loaded = false; rows = []; el('openingForm').hidden = true; el('openingOwnerReview').hidden = true; el('openingSummary').innerHTML = ''; setBusy(true); message('Loading opening balances…');
      return request('/api/expenses/opening-balances?nature=' + encodeURIComponent(el('openingNature').value)).then(function (data) { accept(data); message('Drafts are separate from recorded balances. Nothing is applied automatically.'); return true; }).catch(function (error) { message(error.message, true); return false; }).finally(function () { setBusy(false); });
    }
    function post(action, body) {
      return request('/api/expenses/opening-balances/' + action, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)}).then(function (data) {
        if (!data || !data.success || !validRow(data.row)) throw Error(data && data.error || 'The opening could not be saved. Refresh before retrying.');
        var index = rows.findIndex(function (item) { return item.account === data.row.account; }); if (index !== -1) rows[index] = data.row;
        render(); return data;
      });
    }
    function stage() {
      var item = row();
      if (!permitted() || busy || !loaded || !item || item.reason) return Promise.resolve(false);
      var raw = String(el('openingAmount').value).trim(), date = el('openingDate').value, reason = el('openingReason').value.trim();
      if (!/^-?\d+(?:\.\d{1,2})?$/.test(raw) || !date || !reason) { message('Enter an amount with at most two decimals, an effective date and the source / reason.', true); return Promise.resolve(false); }
      setBusy(true); message('Saving draft only…');
      return post('stage', {nature:el('openingNature').value, account:item.account, amount:raw, effectiveDate:date, reason:reason, revision:item.revision}).then(function () { message('Opening draft saved. Recorded balances, settlements and reconciliations are unchanged.'); return true; }).catch(function (error) { loaded = false; message(error.message + ' Refresh before retrying.', true); return false; }).finally(function () { setBusy(false); });
    }
    function apply() {
      var item = row();
      if (!permitted() || busy || !loaded || !canApply || !item || !item.proposal || item.reason) return Promise.resolve(false);
      if (!el('openingAcknowledge').checked) { message('Verify and acknowledge the saved proposal before applying it.', true); return Promise.resolve(false); }
      if (!win.confirm('Apply the SAVED opening ' + money(item.proposal.amount) + ' from ' + item.proposal.effectiveDate + ' to ' + item.account + '? This changes the recorded ledger balance. No settlement will be finalized.')) return Promise.resolve(false);
      setBusy(true);
      return post('apply', {nature:el('openingNature').value, account:item.account, proposalId:item.proposal.id, revision:item.revision, acknowledge:true}).then(function () { message('Owner applied the saved opening. No settlement or reconciliation was finalized.'); return true; }).catch(function (error) { loaded = false; message(error.message + ' Refresh before retrying.', true); return false; }).finally(function () { setBusy(false); });
    }
    var natures = (config.approvalNatures || []).filter(function (nature) { return ['SANKI','SAMAST'].includes(nature) || nature === 'PERSONAL' && config.isOwner === true; });
    el('openingNature').innerHTML = natures.map(function (nature) { return '<option value="' + nature + '">' + nature + '</option>'; }).join('');
    el('openingNature').onchange = load; el('openingAccount').onchange = function () { render(); message(''); };
    el('refreshOpenings').onclick = load; el('stageOpening').onclick = stage; el('applyOpening').onclick = apply;
    return { load:load, stage:stage, apply:apply, render:render };
  }
  return { create:create, esc:esc, money:money };
});
