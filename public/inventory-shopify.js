(function () {
  'use strict';
  var text = document.getElementById('syncText');
  var check = document.getElementById('previewSync');
  var apply = document.getElementById('applySync');
  var pollTimer = null;
  function request(url, options) {
    return fetch(url, options).then(function (response) {
      return response.json().then(function (body) {
        if (!response.ok || !body.success) throw new Error(body.error || 'Request failed');
        return body;
      });
    });
  }
  function renderJob(job) {
    text.textContent = job.status === 'running'
      ? 'Updating ' + job.completed + ' of ' + job.total + ' products…'
      : job.status === 'succeeded'
        ? 'Complete: ' + job.updated + ' products updated'
        : job.status === 'completed_with_errors'
          ? 'Completed: ' + job.updated + ' updated, ' + job.failed + ' failed, ' + job.skipped + ' not found'
          : job.status === 'failed' ? 'Update failed' : 'Not started';
    if (job.status === 'running') {
      clearTimeout(pollTimer);
      pollTimer = setTimeout(poll, 3000);
    }
  }
  function poll() {
    request('/api/inventory-categorization/status').then(function (data) { renderJob(data.job); }).catch(function (error) { text.textContent = error.message; });
  }
  check.onclick = function () {
    check.disabled = true; text.textContent = 'Comparing with Shopify…';
    request('/api/inventory-categorization/preview').then(function (data) {
      text.textContent = data.matchedProducts + ' matched · ' + data.missingProducts + ' not found · quantities untouched';
      apply.disabled = data.matchedProducts < 1;
    }).catch(function (error) { text.textContent = error.message; }).finally(function () { check.disabled = false; });
  };
  apply.onclick = function () {
    if (!confirm('Apply the approved categorization to matched Shopify products? Quantities and product records will not be removed.')) return;
    apply.disabled = true; text.textContent = 'Starting Shopify update…';
    request('/api/inventory-categorization/apply', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ confirmation: 'APPLY APPROVED CATEGORIZATION' }) })
      .then(function (data) { renderJob(data.job); })
      .catch(function (error) { text.textContent = error.message; apply.disabled = false; });
  };
  poll();
})();
