'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-salary-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const { router } = require('../modules/salary');
test.after(() => fs.rmSync(tempDir, { recursive:true, force:true }));

function invoke(method, routePath, { body={}, params={}, query={}, role='admin' }={}) {
  const layer=router.stack.find(x=>x.route&&x.route.path===routePath&&x.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'route exists: '+method+' '+routePath); let status=200,result;
  const req={body,params,query,user:{username:'tester',role,roles:[role]}};
  const res={status(n){status=n;return this;},json(v){result=v;return this;}};
  let i=0; const next=()=>{const h=layer.route.stack[i++];if(h)h.handle(req,res,next);}; next();
  return {status,body:result};
}

test('salary advances require proof, summarize balances and recover oldest first', () => {
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Employee A',salary:30000,channel:'Shared'}}).body.employee;
  const missing=invoke('POST','/api/salary/advances',{body:{empId:emp.id,amount:1000,date:'2026-08-22',account:'Axis Bank 3448'}});
  assert.equal(missing.status,400); assert.match(missing.body.error,/proof/i);
  [1000,2000,2000].forEach((amount,i)=>{
    const made=invoke('POST','/api/salary/advances',{body:{empId:emp.id,amount,date:'2026-08-'+String(22+i).padStart(2,'0'),account:'Axis Bank 3448',proof:'/proof-'+i+'.jpg',recoveryStartMonth:'2026-08'}});
    assert.equal(made.status,200);
  });
  let list=invoke('GET','/api/salary/advances',{query:{summaryMonth:'2026-08'}}).body;
  const summary=list.summary.find(x=>x.empId===emp.id); assert.equal(summary.total,5000); assert.equal(summary.thisMonth,5000); assert.equal(summary.outstanding,5000);
  const recovered=invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,amount:2500}});
  assert.equal(recovered.status,200);
  list=invoke('GET','/api/salary/advances').body; const own=list.advances.filter(x=>x.empId===emp.id).sort((a,b)=>a.date.localeCompare(b.date));
  assert.equal(own[0].status,'Recovered'); assert.equal(own[1].recovered,1500); assert.equal(own[1].status,'Partially recovered'); assert.equal(list.summary.find(x=>x.empId===emp.id).outstanding,2500);
  const payroll=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(payroll.loggedAdvanceRecovery,2500); assert.equal(payroll.outstandingAdvance,2500);
  const tooMuch=invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,amount:6000}}); assert.equal(tooMuch.status,400);
});

test('advance UI is collapsed by default and exposes account posting and audit concepts', () => {
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/data-v="advances"/); assert.match(html,/<details class="card"><summary>Employee advance summary/); assert.match(html,/Payment proof \(required\)/); assert.match(html,/saveRecovery/); assert.match(html,/oldest eligible advance first/);
});
