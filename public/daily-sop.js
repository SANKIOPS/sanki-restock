(function () {
  'use strict';
  const body = document.getElementById('tasks'), dateInput = document.getElementById('date'), error = document.getElementById('error');
  let selectedDate = new URLSearchParams(location.search).get('date') || '', busy = false, initial = true;
  function fail(message) { error.textContent = message; error.hidden = false; }
  async function request(url, options) {
    const response = await fetch(url, options), data = await response.json();
    if (!response.ok || !data.success) throw new Error(data.error || 'Could not load SOP.');
    return data;
  }
  function render(data) {
    selectedDate = data.date; dateInput.value = data.date; dateInput.min = data.startedOn; dateInput.max = data.today;
    const done = data.tasks.filter(t => t.status === 'Done').length;
    document.getElementById('summary').textContent = `${done} / ${data.tasks.length} Done`;
    body.replaceChildren();
    data.tasks.forEach(task => {
      const row = document.createElement('tr'); row.classList.toggle('overdue', task.overdue);
      const title = document.createElement('td'); title.textContent = task.title;
      const time = document.createElement('td'); time.textContent = new Intl.DateTimeFormat('en-IN', { timeZone: 'Asia/Kolkata', hour: 'numeric', minute: '2-digit' }).format(new Date(task.completeBy));
      const cell = document.createElement('td'), select = document.createElement('select');
      select.setAttribute('aria-label', task.title + ' status');
      ['Pending', 'Done'].forEach(value => { const option = document.createElement('option'); option.value = value; option.textContent = value; select.append(option); });
      select.value = task.status; select.disabled = !task.canEdit; select.classList.toggle('done', task.status === 'Done');
      if (!task.canEdit) select.title = 'Sir must verify this task.';
      if (task.completedAt) cell.title = `Done by ${task.completedBy} at ${new Date(task.completedAt).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`;
      select.addEventListener('change', async () => {
        busy = true; select.disabled = true; error.hidden = true;
        try { render(await request('/api/daily-sop/task', { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ date: data.date, taskId: task.id, status: select.value }) })); window.dispatchEvent(new Event('daily-sop-updated')); }
        catch (err) { select.value = task.status; select.disabled = !task.canEdit; fail(err.message); }
        finally { busy = false; }
      });
      cell.append(select); row.append(title, time, cell); body.append(row);
    });
  }
  async function refresh() {
    if (busy || document.hidden || body.contains(document.activeElement)) return;
    busy = true;
    try {
      const data = await request('/api/daily-sop' + (selectedDate ? '?date=' + encodeURIComponent(selectedDate) : ''));
      // At midnight, move today's view to the new IST day. Explicit history stays on its selected date.
      if (!initial && dateInput.dataset.followToday === 'true' && data.date !== data.today) {
        render(await request('/api/daily-sop')); dateInput.dataset.followToday = 'true';
      } else { render(data); if (initial) dateInput.dataset.followToday = String(!selectedDate || data.date === data.today); }
      initial = false; error.hidden = true;
    } catch (err) { fail(err.message); }
    finally { busy = false; }
  }
  dateInput.addEventListener('change', () => { selectedDate = dateInput.value; dateInput.dataset.followToday = 'false'; refresh(); });
  refresh(); setInterval(refresh, 60000); document.addEventListener('visibilitychange', refresh);
})();
