'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('fs'), os = require('os'), path = require('path');
const express = require('express');
const { TASKS, businessDate, createService, createRouter } = require('../modules/daily-sop');
const { apiAllowedForUser } = require('../auth');
const { userCanAccessPath } = require('../modules/auth-users');
const { visibleFor } = require('../modules/module-registry');
const prashant = { username: 'prashant', role: 'admin', roles: ['admin'] };
const owner = { username: 'gaganlambasanki', role: 'owner', roles: ['owner'] };
function setup(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-sop-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const file = path.join(dir, 'daily-sop.json'); let time = new Date('2026-11-26T06:00:00Z');
  const options = { file, clock: () => time };
  return { ...options, service: createService(options), at: value => { time = new Date(value); } };
}
test('SOP contains only the agreed 20 tasks, 13 accounts and five Sir verifications', t => {
  const { service } = setup(t), view = service.view(prashant);
  assert.equal(view.tasks.length, 20);
  assert.equal(TASKS.filter(t => t.time === '14:00').length, 13);
  assert.equal(TASKS.filter(t => t.ownerOnly).length, 5);
  assert.equal(TASKS.filter(t => t.time === '20:00').length, 4);
  assert.ok(!TASKS.some(t => /follow.up|daily reports|sales|^OB$|^INV$|^SM$/i.test(t.title)));
  assert.ok(view.tasks.every(t => t.status === 'Pending'));
  assert.equal(view.tasks.find(t => t.id === 'expenses').canEdit, false);
  assert.equal(view.tasks.find(t => t.id === 'attendance').canEdit, true);
});
test('reminders start at Complete By, repeat at 30 minutes, and stop on Done', t => {
  const { service, at } = setup(t); service.view(prashant);
  at('2026-11-26T07:29:59Z'); assert.equal(service.remind(prashant).tasks.length, 0);
  at('2026-11-26T07:30:00Z'); assert.deepEqual(service.remind(prashant).tasks.map(t => t.id), ['attendance']);
  assert.equal(service.remind(prashant).tasks.length, 0); // a second tab cannot duplicate the alert
  at('2026-11-26T07:59:59Z'); assert.equal(service.remind(prashant).tasks.length, 0);
  at('2026-11-26T08:00:00Z'); assert.deepEqual(service.remind(prashant).tasks.map(t => t.id), ['attendance']);
  service.update(prashant, { taskId: 'attendance', status: 'Done' });
  at('2026-11-26T08:30:00Z'); const next = service.remind(prashant);
  assert.equal(next.tasks.length, 13); assert.ok(next.pending.every(t => t.id !== 'attendance'));
});
test('only Sir can complete or reopen verification tasks; verification persists', t => {
  const { service, file, clock } = setup(t);
  for (const task of TASKS.filter(t => t.ownerOnly)) {
    assert.throws(() => service.update(prashant, { taskId: task.id, status: 'Done' }), { status: 403 });
    const result = service.update(owner, { taskId: task.id, status: 'Done' });
    assert.equal(result.tasks.find(t => t.id === task.id).completedBy, owner.username);
    assert.throws(() => service.update(prashant, { taskId: task.id, status: 'Pending' }), { status: 403 });
  }
  assert.equal(createService({ file, clock }).view(owner).tasks.filter(t => t.status === 'Done').length, 5);
});
test('IST midnight starts fresh tasks without losing old completions or pending reminders', t => {
  const { service, at } = setup(t); service.view(prashant);
  service.update(prashant, { taskId: 'attendance', status: 'Done' });
  assert.equal(businessDate(new Date('2026-11-26T18:29:59Z')), '2026-11-26');
  at('2026-11-26T18:30:00Z'); assert.equal(businessDate(new Date('2026-11-26T18:30:00Z')), '2026-11-27');
  const today = service.view(prashant); assert.equal(today.date, '2026-11-27'); assert.ok(today.tasks.every(t => t.status === 'Pending'));
  assert.equal(service.view(prashant, '2026-11-26').tasks[0].status, 'Done');
  const pending = service.remind(prashant).pending; assert.equal(pending.length, 19); assert.ok(pending.every(t => t.date === '2026-11-26'));
});
test('reopening work resumes reminders and future or invalid dates cannot be marked Done', t => {
  const { service, at } = setup(t); service.view(prashant); at('2026-11-26T08:00:00Z');
  service.remind(prashant); service.update(prashant, { taskId: 'attendance', status: 'Done' });
  service.update(prashant, { taskId: 'attendance', status: 'Pending' }); assert.equal(service.remind(prashant).tasks[0].id, 'attendance');
  for (const date of ['2026-11-27', '2026-02-30', '../../data', '2026-11-25']) assert.throws(() => service.update(prashant, { date, taskId: 'attendance', status: 'Done' }), { status: 400 });
  assert.throws(() => service.update(owner, { taskId: '__proto__', status: 'Done' }), { status: 400 });
});
test('SOP access is restricted to Prashant and Owner in APIs, page gates and navigation', () => {
  for (const user of [prashant, owner]) {
    assert.equal(apiAllowedForUser(user, '/api/daily-sop/task', 'PATCH'), true);
    assert.equal(userCanAccessPath(user, '/daily-sop.html'), true);
    assert.ok(visibleFor(user).some(m => m.key === 'daily-sop' && m.section === 'Admin'));
  }
  for (const role of ['admin', 'accounting', 'claimant', 'sales', 'stocksearch', 'content_team']) {
    const user = { username: 'another-user', roles: [role], role };
    assert.equal(apiAllowedForUser(user, '/api/daily-sop', 'GET'), false, role);
    assert.equal(userCanAccessPath(user, '/daily-sop.html'), false, role);
    assert.ok(!visibleFor(user).some(m => m.key === 'daily-sop'), role);
  }
});
test('HTTP routes enforce verification and keep Owner from consuming Prashant reminders', async t => {
  const options = setup(t), app = express(); app.use(express.json());
  app.use((req, res, next) => { req.user = req.get('X-Test-Role') === 'owner' ? owner : req.get('X-Test-Role') === 'prashant' ? prashant : { username: 'other', roles: ['admin'] }; next(); });
  app.use(createRouter(options)); const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve)); t.after(() => server.close());
  const url = `http://127.0.0.1:${server.address().port}/api/daily-sop`;
  assert.equal((await fetch(url)).status, 403);
  const get = await fetch(url, { headers: { 'X-Test-Role': 'prashant' } }); assert.equal(get.status, 200); assert.match(get.headers.get('Cache-Control'), /no-store/);
  const patch = role => fetch(url + '/task', { method: 'PATCH', headers: { 'X-Test-Role': role, 'Content-Type': 'application/json' }, body: JSON.stringify({ taskId: 'expenses', status: 'Done' }) });
  assert.equal((await patch('prashant')).status, 403); assert.equal((await patch('owner')).status, 200);
  assert.equal((await fetch(url + '/reminders', { method: 'POST', headers: { 'X-Test-Role': 'owner' } })).status, 403);
  assert.equal((await fetch(url + '/task', { method: 'PATCH', headers: { 'X-Test-Role': 'owner', 'Sec-Fetch-Site': 'cross-site' } })).status, 403);
});
test('corrupt storage never silently resets completed work', t => {
  const { service, file } = setup(t); service.view(prashant); fs.writeFileSync(file, '{invalid');
  assert.throws(() => service.view(prashant)); assert.equal(fs.readFileSync(file, 'utf8'), '{invalid');
});
