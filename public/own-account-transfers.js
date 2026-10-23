/* Transfer-only account identity. Expense/receipt ownership is intentionally separate. */
(function (root) {
  'use strict';
  function esc(value) { return String(value == null ? '' : value).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function key(item) { return item.nature + '|' + item.account; }
  function entries(config) {
    var rows = [], byAccount = Object.create(null);
    (Array.isArray(config && config.ownTransferAccounts) ? config.ownTransferAccounts : []).forEach(function (item) {
      if (!item || ['SANKI', 'SAMAST', 'PERSONAL'].indexOf(item.nature) < 0 || typeof item.account !== 'string' || !item.account.trim()) return;
      var account = item.account.trim(), existing = byAccount[account];
      if (existing) { if (existing.nature !== item.nature) { existing.blocked = true; existing.blockedReason = 'Account ownership is ambiguous — review the account registry'; } return; }
      var row = { nature: item.nature, account: account, label: item.label || account + ' · ' + (item.nature === 'PERSONAL' ? 'Gagan Sir Personal' : item.nature) };
      rows.push(row); byAccount[account] = row;
    });
    return rows;
  }
  function find(config, value) {
    var text = String(value || ''), rows = entries(config).filter(function (item) { return !item.blocked && (key(item) === text || item.account === text); });
    return rows.length === 1 ? rows[0] : { nature: '', account: '' };
  }
  function options(config, current, selected, allowed) {
    return entries(config).filter(function (item) { return item.account !== current && (!allowed || allowed(item)); }).map(function (item) { return '<option value="' + esc(key(item)) + '"' + (key(item) === selected ? ' selected' : '') + (item.blocked ? ' disabled' : '') + '>' + esc(item.label + (item.blocked ? ' — ' + item.blockedReason : '')) + '</option>'; }).join('');
  }
  var api = { key: key, entries: entries, find: find, options: options };
  if (typeof module !== 'undefined' && module.exports) module.exports = api; else root.OwnAccountTransfers = api;
}(typeof window !== 'undefined' ? window : this));
