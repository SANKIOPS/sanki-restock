(function () {
  'use strict';
  // Only view context belongs here. Never save form drafts, credentials or actions,
  // and never replay clicks/requests to restore a page after a browser refresh.
  function read(key) {
    try { return new URL(window.location.href).searchParams.get(key); }
    catch (_) { return null; }
  }
  function choice(key, values, fallback) {
    var value = read(key);
    return values.indexOf(value) >= 0 ? value : fallback;
  }
  function text(key, pattern, fallback) {
    var value = read(key);
    return value !== null && value.length <= 200 && pattern.test(value) ? value : fallback;
  }
  function date(key, fallback) {
    var value = text(key, /^\d{4}-\d{2}-\d{2}$/, '');
    if (!value) return fallback;
    var parsed = new Date(value + 'T00:00:00Z');
    return !isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value ? value : fallback;
  }
  function month(key, fallback) { return text(key, /^\d{4}-(0[1-9]|1[0-2])$/, fallback); }
  function set(values, hash) {
    try {
      var url = new URL(window.location.href);
      Object.keys(values).forEach(function (key) {
        var value = values[key];
        if (value === null || value === undefined || value === '') url.searchParams.delete(key);
        else url.searchParams.set(key, String(value));
      });
      if (hash !== undefined) url.hash = hash;
      var target = url.pathname + url.search + url.hash;
      if (target !== window.location.pathname + window.location.search + window.location.hash)
        window.history.replaceState(window.history.state, '', target);
      return true;
    } catch (_) { return false; } // View navigation still works if history is unavailable.
  }
  window.SankiNavigation = Object.freeze({ choice: choice, text: text, date: date, month: month, set: set });
})();
