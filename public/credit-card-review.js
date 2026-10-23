/* Credit-card review controls. Settlement credits never submit expense fields. */
(function (root) {
  'use strict';
  function esc(value) { return String(value == null ? '' : value).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  var labels = { expense: 'Expense', refund: 'Expense refund', card_payment: 'Card repayment / settlement', unclassified_credit: 'Other / unconfirmed credit — pending', emi_principal: 'EMI principal', emi_interest: 'EMI interest', fee: 'Fee / charges' };
  function options(values, value) { return values.map(function (item) { var x = typeof item === 'string' ? { value: item, label: labels[item] || item } : item; return '<option value="' + esc(x.value) + '"' + (x.value === value ? ' selected' : '') + (x.disabled ? ' disabled' : '') + '>' + esc(x.label) + '</option>'; }).join(''); }
  function classification(row) { var values = Number(row.credit) > 0 ? ['unclassified_credit', 'refund', 'card_payment'] : ['expense', 'emi_principal', 'emi_interest', 'fee']; return values.indexOf(row.classification) >= 0 ? row.classification : values[0]; }
  function value(tr, selector) { var node = tr.querySelector(selector); return node ? node.value : ''; }
  function candidate(row, id) { return (row.settlementCandidates || []).find(function (x) { return x.id === id; }); }
  function candidateRefreshMetadata(statement, original, data) {
    if (!data || !data.success || !Array.isArray(data.statements)) throw new Error(data && data.error || 'Could not refresh existing payments. Your review was not changed.');
    var matches = data.statements.filter(function (x) { return x.id === statement.id; }), fresh = matches[0];
    if (matches.length !== 1 || !fresh || !statement.cardId || fresh.cardId !== statement.cardId || statement.status !== 'review' || fresh.status !== 'review' || !Number.isSafeInteger(statement.reviewRevision) || statement.reviewRevision < 0 || !Number.isSafeInteger(fresh.reviewRevision) || fresh.reviewRevision !== statement.reviewRevision) throw new Error('This statement review changed elsewhere. Existing payments were not refreshed and your local review was preserved. Compare saved and local work before reopening the statement.');
    var before = statement.rows || [], after = fresh.rows || [], fields = ['id', 'date', 'postingDate', 'narration', 'reference', 'amount', 'debit', 'credit', 'classification', 'merchant', 'type', 'category', 'nature', 'channel', 'settlementMode', 'settlementSourceId', 'settlementSourceFingerprint', 'settlementLegacyConfirmed', 'settlementReason', 'payingNature', 'payingAccount', 'settlementReference', 'expenseRefundReceiptId', 'refundLinkDecision', 'refundLinkReason', 'confirmed', 'duplicateResolution', 'linkedExpenseId'];
    if (!Array.isArray(after) || before.length !== after.length || new Set(before.map(function (r) { return r.id; })).size !== before.length || before.some(function (r, i) { return !after[i] || fields.some(function (key) { return r[key] !== after[i][key]; }); })) throw new Error('Statement transaction details changed. Existing payments were not refreshed; your current review was preserved.');
    var rows = after.filter(function (r) { return r.id === original.id; }), row = rows[0];
    if (rows.length !== 1 || row.settlementProtected || !Array.isArray(row.settlementCandidates) || fields.some(function (key) { return original[key] !== row[key]; })) throw new Error('This repayment row is unavailable or changed. Your current review was preserved.');
    var ids = new Set(), candidates = row.settlementCandidates.map(function (source) {
      if (!source || typeof source.id !== 'string' || !source.id || ids.has(source.id) || typeof source.eligible !== 'boolean' || typeof source.fingerprint !== 'string') throw new Error('Existing payment details were incomplete. Your current review was preserved.');
      ids.add(source.id); return JSON.parse(JSON.stringify(source));
    });
    return { settlementCandidates: candidates };
  }
  function evidence(row, fmt, statement) {
    var source = candidate(row, row.settlementSourceId);
    if (!source) return '<div class="hint">Select an existing payment explicitly. No candidate is linked automatically.</div>';
    var changed = source.eligible && row.settlementSourceFingerprint !== source.fingerprint;
    var status = source.status ? '<br>Source status: ' + esc(source.status) : '';
    if (typeof source.bankMovementRecorded === 'boolean') status += source.bankMovementRecorded ? '<br>Existing bank movement recorded.' : '<br>Bank movement is not verified by this source.';
    var out = '<div class="' + (source.eligible && !changed ? 'hint' : 'neg') + '"><b>' + esc(source.label || source.id) + '</b> · ' + esc(source.date) + ' · ' + esc(fmt(source.amount)) + '<br>Paying account: ' + esc(source.account || 'Not supplied') + ' · Entity: ' + esc(source.nature || 'Not supplied') + '<br>Reference: ' + esc(source.reference || 'Not supplied') + status + (source.eligible ? '<br>Existing payment will be linked once; no second bank entry or expense offset.' : '<br>Cannot link: ' + esc(source.blockedReason || 'Candidate is not available.')) + '<br>Linking validates payment identity; bank-statement reconciliation is a separate check.' + (changed ? '<br>Source evidence changed or was not confirmed. Check the details, choose the blank option and explicitly reselect this payment before saving.' : '') + '</div>';
    if (source.legacyConfirmationRequired) out += '<label><input type="checkbox" class="cc_settlement_legacy_confirmed"' + (row.settlementLegacyConfirmed === true ? ' checked' : '') + (statement && statement.status === 'finalized' || !source.eligible ? ' disabled' : '') + '> I confirm this recorded payment repaid ' + esc(statement && statement.cardName || 'this card') + '; keep original bank/payment history unchanged.</label><div class="hint">Legacy Personal-capture association only. Original bank/payment history is kept unchanged; this review creates no new withdrawal or expense offset. Confirm the selected card and explain this association below.</div>';
    return out;
  }
  function settlement(row, disabled, natures, fmt, statement) {
    if (statement.status === 'finalized' && !row.settlementMode) return '<div class="hint">Historical statement settlement; no bank link recorded. This is a finalized liability settlement, not an expense offset.</div>';
    var mode = ['pending', 'link_existing', 'external'].indexOf(row.settlementMode) >= 0 ? row.settlementMode : 'pending';
    var out = '<div class="hint"><b>Liability settlement only.</b> This repayment does not reduce expenses or enter Results. No new bank withdrawal is created by this review.</div><label>Settlement treatment<select class="cc_settlement_mode"' + disabled + '>' + options([{ value: 'pending', label: 'Unconfirmed source — keep pending' }, { value: 'link_existing', label: 'Link an existing payment / transfer' }, { value: 'external', label: 'External / historical source — no bank entry created' }], mode) + '</select></label>';
    if (mode === 'link_existing') {
      var candidates = row.settlementCandidates || [];
      var missing = row.settlementSourceId && !candidate(row, row.settlementSourceId);
      out += '<label>Existing payment<select class="cc_settlement_source"' + disabled + '><option value="">Select an existing payment</option>' + (missing ? '<option value="' + esc(row.settlementSourceId) + '" selected disabled>Previously selected source — unavailable; retained for review</option>' : '') + options(candidates.map(function (x) { return { value: x.id, label: (x.label || x.id) + ' · ' + x.date + ' · ' + fmt(x.amount) + ' · ' + (x.account || 'Unknown account') + (x.eligible ? '' : ' — Unavailable: ' + (x.blockedReason || 'Not eligible')), disabled: !x.eligible }; }), row.settlementSourceId) + '</select></label>';
      if (!candidates.length) out += '<div class="msg err">No accessible recorded card payment or transfer is available for this card. The saved statement review has not been removed. Refresh existing payments to check again. Bank reconciliation is a separate check, not a reason to create a duplicate payment. Keep the source pending until verified; do not classify this repayment as an expense refund.</div>';
      else if (!candidates.some(function (x) { return x.eligible; })) out += '<div class="msg err">No existing payment currently passes all linking checks. The unavailable choices show the exact reasons. Resolve those reasons without creating a duplicate payment; no source is selected automatically.</div>';
      if (statement.status !== 'finalized') out += '<button type="button" class="btn mini ghost cc_settlement_refresh">Refresh existing payments</button><div class="cc_settlement_refresh_status hint" role="status" aria-live="polite"></div>';
      if (row.settlementSourceId && !candidate(row, row.settlementSourceId)) out += '<div class="neg">Previously selected source is no longer available. Refresh existing payments to recheck it, then explicitly select a verified source. The saved choice and evidence have been retained.</div>';
      var selected = candidate(row, row.settlementSourceId), reasonPlaceholder = selected && selected.legacyConfirmationRequired ? 'Required: explain this legacy payment association in at least 10 characters' : 'Required: at least 10 characters if payment dates differ';
      out += '<div class="cc_settlement_evidence">' + evidence(row, fmt, statement) + '</div><label>Settlement remark<input class="cc_settlement_reason" value="' + esc(row.settlementReason || '') + '" placeholder="' + esc(reasonPlaceholder) + '"' + disabled + '></label><div class="hint">Exact amount, selected card, source fingerprint, availability and dates are rechecked on save/finalization.</div>';
    } else if (mode === 'external') {
      out += '<div class="hint">Use only when the repayment came from a source not posted in this app. Existing matching payments must be linked instead. This does not create a bank transaction.</div><label>Paying entity<select class="cc_paying_nature"' + disabled + '><option value="">Choose the actual payer</option>' + options(natures, row.payingNature || '') + '</select></label><label>External / historical paying account<input class="cc_paying_account" value="' + esc(row.payingAccount || '') + '" placeholder="Identify the actual source account"' + disabled + '></label><label>Payment reference<input class="cc_settlement_reference" value="' + esc(row.settlementReference || '') + '"' + disabled + '></label><label>Reason and evidence for external source<input class="cc_settlement_reason" value="' + esc(row.settlementReason || '') + '" placeholder="Required: explain in at least 10 characters"' + disabled + '></label>';
    } else out += '<div class="hint">Save review to retain this unconfirmed row. Finalization is blocked until its repayment source is verified.</div>';
    return out;
  }
  function rowCells(row, index, statement, natures, fmt) {
    var credit = Number(row.credit) > 0, cls = classification(row), disabled = statement.status === 'finalized' ? ' disabled' : '', classes = credit ? ['unclassified_credit', 'refund', 'card_payment'] : ['expense', 'emi_principal', 'emi_interest', 'fee'];
    if (row.settlementProtected) return '<td>' + (index + 1) + '</td><td class="cc-date">' + esc(row.date) + '</td><td>' + esc(row.merchant || row.narration || '') + '</td><td class="cc-narration">' + esc(row.narration) + '</td><td class="r cc-amount">' + (credit ? fmt(row.credit) : '—') + '</td><td class="r cc-amount">' + (!credit ? fmt(row.debit) : '—') + '</td><td>' + esc(labels[cls] || cls) + '</td><td colspan="4"><div class="hint">' + esc(row.settlementProtectedMessage || 'Private settlement source — Owner review required') + '. This row is protected and unchanged. Its saved settlement status is preserved.</div></td>';
    var out = '<td>' + (index + 1) + '</td><td class="cc-date">' + esc(row.date) + '</td><td><input class="cc_merchant" aria-label="Merchant" value="' + esc(row.merchant || row.narration || '') + '"' + disabled + '></td><td class="cc-narration">' + esc(row.narration) + '</td><td class="r cc-amount">' + (credit ? fmt(row.credit) : '—') + '</td><td class="r cc-amount">' + (!credit ? fmt(row.debit) : '—') + '</td><td><select class="cc_class" aria-label="Transaction classification"' + disabled + '>' + options(classes, cls) + '</select>';
    if (cls === 'refund') out += '<div class="hint">Already logged expense refund?</div><select class="cc_refund_link" aria-label="Existing expense refund"' + disabled + '><option value="">Not previously logged</option>' + options((statement.refundReceipts || []).map(function (x) { return { value: x.id, label: x.id }; }), row.expenseRefundReceiptId) + '<option value="separate"' + (row.refundLinkDecision === 'separate' ? ' selected' : '') + '>Separate transaction — explain below</option></select><input class="cc_refund_reason" aria-label="Separate refund explanation" placeholder="Reason if separate transaction" value="' + esc(row.refundLinkReason || '') + '"' + disabled + '><div class="hint">A genuine refund may reduce its corresponding expense. Link a previously logged refund so it counts only once.</div>';
    out += '</td>';
    if (cls === 'card_payment') return out + '<td colspan="4" class="cc_settlement">' + settlement(row, disabled, natures, fmt, statement) + '</td>';
    if (cls === 'unclassified_credit') return out + '<td colspan="4"><div class="hint">Unconfirmed credit — no expense category or offset will be posted. Save it pending, then identify whether it is a repayment or a genuine expense refund before finalization. Other credits require a verified supported treatment; do not classify them as refunds just to continue.</div></td>';
    return out + '<td><select class="cc_type" aria-label="Expense type"' + disabled + '><option value="">Choose</option>' + options(['running', 'variable', 'marketing'], row.type || '') + '</select></td><td><input class="cc_category" aria-label="Expense category" list="ccCategories" value="' + esc(row.category || '') + '"' + disabled + '></td><td><select class="cc_nature" aria-label="Expense entity"' + disabled + '><option value="">Choose</option>' + options(natures, row.nature || '') + '</select></td><td><select class="cc_channel" aria-label="Expense channel"' + disabled + '><option value="">Choose</option>' + options(['POS', 'Website', 'Both'], row.channel || '') + '</select></td>';
  }
  function collectRow(tr, row) {
    if (row && row.settlementProtected) return null;
    var cls = value(tr, '.cc_class'), out = { id: tr.dataset.id, merchant: value(tr, '.cc_merchant'), classification: cls };
    if (cls === 'card_payment') {
      out.settlementMode = value(tr, '.cc_settlement_mode') || 'pending';
      if (out.settlementMode === 'link_existing') {
        out.settlementSourceId = value(tr, '.cc_settlement_source');
        var source = candidate(row, out.settlementSourceId);
        out.settlementSourceFingerprint = row.settlementSourceId && row.settlementSourceId === out.settlementSourceId ? row.settlementSourceFingerprint || '' : source && source.eligible ? source.fingerprint || '' : '';
        var confirmation = tr.querySelector('.cc_settlement_legacy_confirmed');
        if (source && source.legacyConfirmationRequired || Object.prototype.hasOwnProperty.call(row, 'settlementLegacyConfirmed')) out.settlementLegacyConfirmed = confirmation ? confirmation.checked === true : row.settlementLegacyConfirmed === true;
        out.settlementReason = value(tr, '.cc_settlement_reason');
      } else if (out.settlementMode === 'external') {
        out.payingNature = value(tr, '.cc_paying_nature'); out.payingAccount = value(tr, '.cc_paying_account');
        out.settlementReference = value(tr, '.cc_settlement_reference'); out.settlementReason = value(tr, '.cc_settlement_reason');
      }
    } else if (cls !== 'unclassified_credit') {
      out.type = value(tr, '.cc_type'); out.category = value(tr, '.cc_category'); out.nature = value(tr, '.cc_nature'); out.channel = value(tr, '.cc_channel');
      if (cls === 'refund') {
        var refund = value(tr, '.cc_refund_link'); out.expenseRefundReceiptId = refund === 'separate' ? '' : refund;
        out.refundLinkDecision = refund === 'separate' ? 'separate' : ''; out.refundLinkReason = value(tr, '.cc_refund_reason');
      }
    }
    return out;
  }
  function bindRow(tr, original, index, statement, natures, fmt, markDirty, loadCandidates) {
    tr._ccReviewDraft = Object.assign({}, original, tr._ccReviewDraft || {});
    tr._ccReviewStatement = statement;
    function remember(changed) {
      var previous = {};
      [['classification', '.cc_class'], ['merchant', '.cc_merchant'], ['type', '.cc_type'], ['category', '.cc_category'], ['nature', '.cc_nature'], ['channel', '.cc_channel'], ['refundLinkReason', '.cc_refund_reason'], ['settlementMode', '.cc_settlement_mode'], ['settlementSourceId', '.cc_settlement_source'], ['settlementReason', '.cc_settlement_reason'], ['payingNature', '.cc_paying_nature'], ['payingAccount', '.cc_paying_account'], ['settlementReference', '.cc_settlement_reference']].forEach(function (field) { if (tr.querySelector(field[1])) previous[field[0]] = value(tr, field[1]); });
      var refund = tr.querySelector('.cc_refund_link');
      if (refund) { previous.expenseRefundReceiptId = refund.value === 'separate' ? '' : refund.value; previous.refundLinkDecision = refund.value === 'separate' ? 'separate' : ''; }
      var legacy = tr.querySelector('.cc_settlement_legacy_confirmed');
      if (legacy) previous.settlementLegacyConfirmed = legacy.checked === true;
      var changedFields = Object.keys(previous).some(function (key) { return tr._ccReviewDraft[key] !== previous[key]; });
      tr._ccReviewDraft = Object.assign({}, tr._ccReviewDraft, previous); if ((changed !== false || changedFields) && markDirty) markDirty();
    }
    function redraw(changed) { remember(changed); tr.innerHTML = rowCells(tr._ccReviewDraft, index, statement, natures, fmt); bindRow(tr, original, index, statement, natures, fmt, markDirty, loadCandidates); }
    var cls = tr.querySelector('.cc_class'), mode = tr.querySelector('.cc_settlement_mode'), source = tr.querySelector('.cc_settlement_source');
    if (cls) cls.onchange = redraw;
    if (mode) mode.onchange = redraw;
    if (source) source.onchange = function () { var priorSource = tr._ccReviewDraft.settlementSourceId || ''; remember(); var selected = candidate(tr._ccReviewDraft, source.value); tr._ccReviewDraft.settlementSourceFingerprint = selected && selected.eligible ? selected.fingerprint || '' : ''; if (source.value !== priorSource) tr._ccReviewDraft.settlementLegacyConfirmed = false; tr.innerHTML = rowCells(tr._ccReviewDraft, index, statement, natures, fmt); bindRow(tr, original, index, statement, natures, fmt, markDirty, loadCandidates); };
    var refresh = tr.querySelector('.cc_settlement_refresh');
    if (refresh) {
      refresh.disabled = !!tr._ccCandidateRefreshRunning || typeof loadCandidates !== 'function';
      refresh.onclick = function () {
        if (refresh.disabled || tr._ccCandidateRefreshRunning || statement.status !== 'review' || typeof loadCandidates !== 'function') return Promise.resolve(false);
        remember(false);
        var token = {}, status = tr.querySelector('.cc_settlement_refresh_status');
        tr._ccCandidateRefreshRunning = token; tr.dataset.ccCandidateRefreshing = 'true'; refresh.disabled = true;
        if (status) status.textContent = 'Checking existing payments only. Your review remains unchanged.';
        function current() { return tr.isConnected !== false && tr.dataset.id === original.id && tr._ccReviewStatement === statement && tr._ccCandidateRefreshRunning === token; }
        return Promise.resolve().then(function () { return loadCandidates(statement.cardId); }).then(function (data) {
          if (!current()) return false;
          var metadata = candidateRefreshMetadata(statement, original, data);
          remember(false); Object.assign(tr._ccReviewDraft, metadata);
          redraw(false);
          var message = tr.querySelector('.cc_settlement_refresh_status');
          if (message) message.textContent = 'Existing payments refreshed. All review fields and saved source evidence were retained; choose a verified source explicitly.';
          return true;
        }).catch(function (error) {
          if (current()) { var message = tr.querySelector('.cc_settlement_refresh_status'); if (message) message.textContent = error.message || 'Could not refresh existing payments. Your review was preserved.'; }
          return false;
        }).finally(function () {
          if (tr._ccCandidateRefreshRunning === token) { delete tr._ccCandidateRefreshRunning; delete tr.dataset.ccCandidateRefreshing; var button = tr.querySelector('.cc_settlement_refresh'); if (button) button.disabled = button.dataset && button.dataset.ccWasDisabled != null || typeof loadCandidates !== 'function'; }
        });
      };
    }
  }
  var api = { labels: labels, classification: classification, rowCells: rowCells, collectRow: collectRow, bindRow: bindRow, candidateRefreshMetadata: candidateRefreshMetadata };
  if (typeof module !== 'undefined' && module.exports) module.exports = api; else root.CreditCardReview = api;
}(typeof window !== 'undefined' ? window : this));
