'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const tempDir = fs.mkdtempSync(path.join(__dirname, '.tmp-samast-salary-policy-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const storePath = entity => path.join(tempDir, entity === 'SAMAST' ? 'salary-samast.json' : 'salary.json');
const empty = () => ({ employees:{}, months:{}, salaryPayments:[], salaryPaymentBatchSeq:0, advances:{}, oneTimeMigrations:{} });
fs.writeFileSync(storePath('SANKI'), JSON.stringify(empty()));
fs.writeFileSync(path.join(tempDir, 'expenses.json'), '{}');
const { router } = require('../modules/salary');
test.after(() => fs.rmSync(tempDir, { recursive:true, force:true }));
test.beforeEach(() => {
  for (const entity of ['SANKI', 'SAMAST']) fs.writeFileSync(storePath(entity), JSON.stringify(empty()));
});
const read = entity => JSON.parse(fs.readFileSync(storePath(entity), 'utf8'));

// Exercise the actual entity-selection middleware as well as the route guard,
// without opening a localhost socket or touching a production data directory.
function invoke(method, routePath, { entity='SAMAST', body={}, params={}, username='prashant', role='admin' }={}) {
  const layer = router.stack.find(x => x.route && x.route.path === routePath && x.route.methods[method.toLowerCase()]);
  const context = router.stack.find(x => !x.route && x.regexp.test('/api/salary'));
  assert.ok(layer && context, 'salary route and entity middleware exist');
  const req = {body, params, query:{entity}, method, path:routePath, route:{path:routePath}, user:{username, role, roles:[role]}};
  let status = 200, result, index = 0;
  const res = {status(n){status=n;return this;}, json(value){result=value;return this;}};
  const next = error => {if(error)throw error;const step=layer.route.stack[index++];if(step)step.handle(req,res,next);};
  context.handle(req, res, next);
  return {status, body:result};
}
function employee(entity='SAMAST', name='SAMAST payroll test', salary=17000) {
  const created = invoke('POST', '/api/salary/employees', {entity, body:{name, salary, joiningDate:'2099-10-01'}});
  assert.equal(created.status, 200);
  const emp = created.body.employee;
  assert.equal(invoke('POST', '/api/salary/row/:ym', {entity, params:{ym:'2099-10'}, body:{empId:emp.id, paidDays:30}}).status, 200);
  return emp;
}
const payment = (emp, account='IndusInd Bank 7883', amount=17000) => ({
  ym:'2099-10', date:'2099-10-20', account, proofs:['/api/expenses/photo/samast-salary.jpg'],
  reference:'SAMAST-PAYROLL-TEST', items:[{empId:emp.id, amount}]
});

test('SAMAST exposes shared 7883 to authorized salary users without broadening SANKI or private accounts', () => {
  const original = ['Axis Bank 3448','IndusInd Bank 8181','Prashant Cash','Gagan Sir Cash','Counter Cash'];
  for (const [username, role] of [['prashant','admin'], ['owner','owner'], ['accountant','accounting']]) {
    const samast = invoke('GET', '/api/salary/employees', {username, role}).body;
    assert.equal(samast.entity, 'SAMAST');
    assert.deepEqual(samast.salaryPayingAccounts, original.concat('IndusInd Bank 7883'));
    assert.deepEqual(invoke('GET', '/api/salary/employees', {entity:'SANKI', username, role}).body.salaryPayingAccounts, original);
    for (const account of ['ICICI Bank 0993','ICICI Bank 0992','Gagan Personal Cash','Namita 5464','Namita Cash','Prashant Axis 3645']) {
      assert.ok(!samast.salaryPayingAccounts.includes(account), account+' is not a new ordinary salary source');
    }
  }
  assert.equal(invoke('GET', '/api/salary/employees', {username:'claimant', role:'claimant'}).status, 403);
});

test('admin SAMAST salary from 7883 stays in SAMAST payroll and ignores forged posting entities', () => {
  const emp = employee(), sankiBefore = read('SANKI');
  const paid = invoke('POST', '/api/salary/payments/batch', {
    body:{...payment(emp), entity:'PERSONAL', payingNature:'PERSONAL'}
  });
  assert.equal(paid.status, 200);
  const saved = read('SAMAST').salaryPayments;
  assert.equal(saved.length, 1);
  assert.equal(saved[0].account, 'IndusInd Bank 7883');
  assert.equal(saved[0].entity, 'SAMAST');
  assert.equal(saved[0].payingNature, 'SAMAST');
  assert.equal(saved[0].createdBy, 'prashant');
  assert.equal(saved[0].reference, 'SAMAST-PAYROLL-TEST');
  assert.deepEqual(saved[0].proofs, ['/api/expenses/photo/samast-salary.jpg']);
  assert.deepEqual(read('SANKI'), sankiBefore);
  const row = invoke('GET', '/api/salary/month/:ym', {params:{ym:'2099-10'}}).body.rows.find(r => r.id === emp.id);
  assert.equal(row.salaryAmt, 17000);
  assert.equal(row.paid, 17000);
  assert.equal(row.balance, 0);
});

test('SAMAST multi-employee salary batches preserve each employee and the shared bank source', () => {
  const first = employee(), second = employee('SAMAST','Second SAMAST employee',9000);
  const body = payment(first);
  body.items.push({empId:second.id, amount:9000});
  const paid = invoke('POST', '/api/salary/payments/batch', {body});
  assert.equal(paid.status, 200);
  assert.equal(paid.body.count, 2);
  assert.equal(paid.body.total, 26000);
  const rows = read('SAMAST').salaryPayments;
  assert.equal(new Set(rows.map(x => x.id)).size, 2);
  assert.ok(rows.every(x => x.entity==='SAMAST' && x.payingNature==='SAMAST' && x.account==='IndusInd Bank 7883'));
});

test('new account option does not bypass proof, account, employee or role checks', () => {
  const emp = employee(), before = read('SAMAST');
  for (const body of [
    {...payment(emp), proofs:[]},
    payment(emp,'ICICI Bank 0993'),
    payment(emp,'Namita 5464'),
    payment(emp,'Prashant Axis 3645'),
    {...payment(emp), items:[{empId:'MISSING', amount:17000}]}
  ]) {
    assert.equal(invoke('POST', '/api/salary/payments/batch', {body}).status, 400);
    assert.deepEqual(read('SAMAST'), before);
  }
  assert.equal(invoke('POST', '/api/salary/payments/batch', {username:'claimant', role:'claimant', body:payment(emp)}).status, 403);
  assert.deepEqual(read('SAMAST'), before);
});

test('7883 is rejected for SANKI salary even when the request body claims SAMAST', () => {
  const emp = employee('SANKI');
  assert.equal(invoke('POST', '/api/salary/payments/batch', {
    entity:'SANKI', body:{...payment(emp), entity:'SAMAST', payingNature:'SAMAST'}
  }).status, 400);
  assert.equal(read('SANKI').salaryPayments.length, 0);
  const allowed = invoke('POST', '/api/salary/payments/batch', {entity:'SANKI', body:payment(emp,'Axis Bank 3448')});
  assert.equal(allowed.status, 200);
  const recorded = read('SANKI').salaryPayments[0];
  assert.equal(recorded.entity, 'SANKI');
  assert.equal(recorded.payingNature, 'SANKI');
});

test('SAMAST payment correction can use 7883 and records bank/entity changes in its audit', () => {
  const emp = employee();
  assert.equal(invoke('POST', '/api/salary/payments/batch', {body:payment(emp,'Axis Bank 3448')}).status, 200);
  const original = read('SAMAST').salaryPayments[0];
  assert.equal(original.entity, 'SAMAST');
  assert.equal(original.payingNature, 'SANKI');
  const body = {amount:17000, date:'2099-10-20', account:'IndusInd Bank 7883', proofs:original.proofs, reference:original.reference, reason:'Correct actual bank source', payingNature:'PERSONAL'};
  const corrected = invoke('PATCH', '/api/salary/payments/:id', {params:{id:original.id}, body});
  assert.equal(corrected.status, 200);
  assert.equal(corrected.body.payment.entity, 'SAMAST');
  assert.equal(corrected.body.payment.payingNature, 'SAMAST');
  const audit = read('SAMAST').salaryPaymentAudit.at(-1);
  assert.equal(audit.before.account, 'Axis Bank 3448');
  assert.equal(audit.before.payingNature, 'SANKI');
  assert.equal(audit.after.account, 'IndusInd Bank 7883');
  assert.equal(audit.after.entity, 'SAMAST');
  assert.equal(audit.after.payingNature, 'SAMAST');
  const stable = read('SAMAST');
  assert.equal(invoke('PATCH', '/api/salary/payments/:id', {params:{id:original.id}, body:{...body, account:'Namita Cash'}}).status, 400);
  assert.deepEqual(read('SAMAST'), stable);
  assert.equal(invoke('PATCH', '/api/salary/payments/:id', {username:'other-admin', params:{id:original.id}, body:{...body, amount:16999}}).status, 403);
  const reverted = invoke('PATCH', '/api/salary/payments/:id', {params:{id:original.id}, body:{...body, account:'Axis Bank 3448', reason:'Correct source back to the proof-backed business bank'}});
  assert.equal(reverted.status, 200);
  assert.equal(reverted.body.payment.entity, 'SAMAST');
  assert.equal(reverted.body.payment.payingNature, 'SANKI');
  assert.equal(read('SAMAST').salaryPaymentAudit.at(-1).before.payingNature, 'SAMAST');
  assert.equal(read('SAMAST').salaryPaymentAudit.at(-1).after.payingNature, 'SANKI');
});
