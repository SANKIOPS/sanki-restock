(function () {
  'use strict';
  if (window.sankiDailySopReminders) return; window.sankiDailySopReminders = true;
  let busy = false;
  async function refresh() {
    if (busy || document.hidden) return; busy = true;
    try {
      const response = await fetch('/api/daily-sop/reminders', { method: 'POST', headers: { Accept: 'application/json' } });
      if (!response.ok) return;
      const data = await response.json(); if (!data.success) return;
      let panel = document.getElementById('daily-sop-reminder');
      if (!data.pending.length) { panel?.remove(); return; }
      if (!panel && !data.tasks.length) return;
      if (!panel) { panel = document.createElement('aside'); panel.id = 'daily-sop-reminder'; panel.setAttribute('role', 'status'); panel.style.cssText = 'position:fixed;right:18px;bottom:18px;width:min(350px,calc(100vw - 80px));max-height:55vh;overflow:auto;z-index:1200;background:#fff;border:1px solid #e1c68f;border-radius:10px;padding:16px;box-shadow:0 8px 30px #0002;font:14px/1.5 -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;color:#222'; document.body.append(panel); }
      panel.replaceChildren();
      const close = document.createElement('button'); close.textContent = '×'; close.setAttribute('aria-label', 'Dismiss SOP reminder'); close.style.cssText = 'float:right;border:0;background:none;font-size:22px;cursor:pointer'; close.onclick = () => panel.remove();
      const title = document.createElement('strong'); title.textContent = `${data.pending.length} SOP task${data.pending.length === 1 ? '' : 's'} pending`;
      panel.append(close, title);
      const list = document.createElement('ul'); list.style.cssText = 'padding-left:18px;margin:10px 0';
      data.pending.slice(0, 5).forEach(task => { const item = document.createElement('li'); item.textContent = task.title + (task.date !== data.today ? ' (' + task.date + ')' : ''); list.append(item); });
      const link = document.createElement('a'); link.href = '/daily-sop.html?date=' + encodeURIComponent(data.pending[0].date); link.textContent = 'Open Daily SOP';
      const note = document.createElement('small'); note.textContent = 'Reminds every 30 minutes until Done.'; note.style.cssText = 'display:block;color:#666;margin-top:8px';
      panel.append(list, link, note);
    } catch {} finally { busy = false; }
  }
  // Remove an out-of-date popup immediately after a completion; the next tick checks the remaining work.
  window.addEventListener('daily-sop-updated', () => { document.getElementById('daily-sop-reminder')?.remove(); });
  refresh(); setInterval(refresh, 60000); document.addEventListener('visibilitychange', refresh);
})();
