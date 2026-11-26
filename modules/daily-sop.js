'use strict';
const express = require('express');
const fs = require('fs');
const path = require('path');
const { rolesOf, isPrashant } = require('./auth-users');
const INTERVAL = 30 * 60 * 1000;
const ACCOUNTS = ['Cash Counter', 'Axis Bank 3448', 'IndusInd 8181', 'Tiana 0425', 'Tiana 0437', 'Paytm', 'Velocity', 'GoKwik', 'HDFC CC', 'ICICI CC', 'ICICI 0992', 'ICICI 0993', 'IndusInd 7883'];
const TASKS = [
  { id: 'attendance', title: 'Attendance update & send', time: '13:00' },
  ...ACCOUNTS.map((name, i) => ({ id: `reconciliation-${i + 1}`, title: `Reconciliation — ${name}`, time: '14:00' })),
  { id: 'finalisation', title: 'Reconciliation finalisation by Sir', time: '18:00', ownerOnly: true },
  ...['Expenses', 'Balance', 'Categories', 'Vendors'].map(name => ({ id: name.toLowerCase(), title: `${name} — verified by Sir`, time: '20:00', ownerOnly: true })),
  { id: 'screenshots', title: 'Send balance screenshots', time: '21:00' }
];
function businessDate(now = new Date()) { return new Date(now.getTime() + 330 * 60000).toISOString().slice(0, 10); }
function validDate(date) { return /^\d{4}-\d{2}-\d{2}$/.test(date) && !Number.isNaN(Date.parse(date)) && new Date(date).toISOString().slice(0, 10) === date; }
function canAccess(user) { return !!user && (isPrashant(user) || rolesOf(user).includes('owner')); }
function createService({ file = process.env.DAILY_SOP_PATH || path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'), 'daily-sop.json'), clock = () => new Date() } = {}) {
  function save(store) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = file + '.tmp-' + process.pid;
    fs.writeFileSync(tmp, JSON.stringify(store)); fs.renameSync(tmp, file);
  }
  function load() {
    try {
      const store = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (!validDate(store.startedOn) || !store.days || typeof store.days !== 'object' || Array.isArray(store.days)) throw new Error('Invalid SOP store');
      return store;
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      const store = { startedOn: businessDate(clock()), days: {} }; save(store); return store;
    }
  }
  function dateFor(store, value) {
    const date = value == null ? businessDate(clock()) : value;
    if (typeof date !== 'string' || !validDate(date) || date < store.startedOn || date > businessDate(clock())) throw Object.assign(new Error('Choose a date between SOP start and today.'), { status: 400 });
    return date;
  }
  function taskRows(store, date, user) {
    const now = clock();
    return TASKS.map(task => {
      const entry = store.days[date]?.[task.id];
      const due = Date.parse(`${date}T${task.time}:00+05:30`);
      const status = entry?.status === 'Done' ? 'Done' : 'Pending';
      return { ...task, date, status, completeBy: new Date(due).toISOString(), overdue: status === 'Pending' && now.getTime() >= due, completedAt: entry?.completedAt || null, completedBy: entry?.completedBy || null, canEdit: !task.ownerOnly || rolesOf(user).includes('owner') };
    });
  }
  function view(user, requestedDate) {
    const store = load(), date = dateFor(store, requestedDate);
    return { success: true, date, today: businessDate(clock()), startedOn: store.startedOn, owner: rolesOf(user).includes('owner'), tasks: taskRows(store, date, user), reminderMinutes: 30 };
  }
  function update(user, { date, taskId, status }) {
    const store = load(); date = dateFor(store, date);
    const task = TASKS.find(t => t.id === taskId);
    if (!task || !['Pending', 'Done'].includes(status)) throw Object.assign(new Error('Invalid task or status.'), { status: 400 });
    if (task.ownerOnly && !rolesOf(user).includes('owner')) throw Object.assign(new Error('Sir must verify this task.'), { status: 403 });
    const day = store.days[date] ||= {}, previous = day[taskId] || {};
    if (previous.status !== status) {
      day[taskId] = { ...previous, status, completedAt: status === 'Done' ? clock().toISOString() : null, completedBy: status === 'Done' ? user.username : null };
      // Reopening work should make the next due reminder eligible immediately.
      if (status === 'Pending') delete day[taskId].lastRemindedAt;
      save(store);
    }
    return view(user, date);
  }
  function remind(user) {
    const store = load(), now = clock(), today = businessDate(now), due = [], pending = [];
    // Pending items from earlier days remain pending; a new day never erases them.
    for (let date = store.startedOn; date <= today; date = new Date(Date.parse(date) + 86400000).toISOString().slice(0, 10)) {
      for (const task of taskRows(store, date, user)) {
        if (task.overdue) pending.push(task);
        const last = store.days[date]?.[task.id]?.lastRemindedAt;
        if (!task.overdue || (last && now.getTime() - Date.parse(last) < INTERVAL)) continue;
        due.push(task);
        const day = store.days[date] ||= {};
        day[task.id] = { ...day[task.id], lastRemindedAt: now.toISOString() };
      }
    }
    if (due.length) save(store);
    return { success: true, today, tasks: due, pending, reminderMinutes: 30 };
  }
  return { view, update, remind };
}
function createRouter(options) {
  const router = express.Router(), service = createService(options);
  router.use('/api/daily-sop', (req, res, next) => {
    res.setHeader('Cache-Control', 'no-store');
    if (!canAccess(req.user)) return res.status(req.user ? 403 : 401).json({ success: false, error: 'Prashant and Sir only.' });
    if (req.method !== 'GET' && req.get('Sec-Fetch-Site') === 'cross-site') return res.status(403).json({ success: false, error: 'Open the SOP in the portal.' });
    next();
  });
  const handle = fn => (req, res) => {
    try { res.json(fn(req)); }
    catch (err) { if (!err.status) console.error('[daily-sop]', err.message); res.status(err.status || 503).json({ success: false, error: err.status ? err.message : 'SOP is unavailable. Please retry.' }); }
  };
  router.get('/api/daily-sop', handle(req => service.view(req.user, req.query.date)));
  router.patch('/api/daily-sop/task', handle(req => service.update(req.user, req.body || {})));
  // Only Prashant's active portal claims reminders. Owner visits cannot consume his reminder cadence.
  router.post('/api/daily-sop/reminders', (req, res, next) => {
    if (!isPrashant(req.user)) return res.status(403).json({ success: false, error: 'Prashant only.' }); next();
  }, handle(req => service.remind(req.user)));
  return router;
}
module.exports = { TASKS, businessDate, canAccess, createService, createRouter, router: createRouter() };
