'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {plan,apply,decorate,register}=require('../modules/expense-duplicate-links');
const clone=v=>JSON.parse(JSON.stringify(v));
const req={user:{role:'owner',username:'owner'}},body={pairs:[{sourceId:'EX-1',targetId:'EX-2'}],reason:'Owner verified identical original bill and paid copy'};
function fixture(){return {expenses:{'EX-1':{id:'EX-1',nature:'PERSONAL',status:'pending',date:'2026-10-01',amount:100,paidAmount:0,payments:[],billPhoto:'/original.jpg'},'EX-2':{id:'EX-2',nature:'SANKI',status:'paid',date:'2026-10-01',amount:100,paidAmount:100,payments:[{id:'P',date:'2026-10-02',amount:100,account:'Bank'}],billPhoto:'/kept.jpg'}},bankStatements:{Bank:{transactions:{T:{debit:100}},imports:[{id:'I',reconciliationRows:[{linkedRecordIds:['EX-2/P']}]}]}},bankDateOverrides:{'EX-2/P':{bankDate:'2026-10-02'}},bankReconciliationDrafts:{},bankReconciliationApprovals:{},transfers:[{id:'KEEP',amount:100}],auditLog:[],auditSeq:0};}
function run(store,input=body,overrides={}){let saved=null;const snapshot={store,raw:JSON.stringify(store)},deps={isOwner:r=>r.user.role==='owner',canAccess:()=>true,movements:s=>Object.values(s.expenses).flatMap(e=>e.approvedAt||['approved','paid','partially_paid'].includes(e.status)?e.payments||[]:[]),audit:(s,r,action,type,id,data)=>{s.auditSeq++;s.auditLog.push({action,id,...data});},commit:(before,after)=>{saved=after;},...overrides};const prepared=plan(snapshot,input,req,deps);return {snapshot,deps,prepared,commit:()=>{const result=apply(snapshot,{...input,fingerprint:prepared.fingerprint},req,deps);return {result,saved};}};}
test('pending copy closes with structured link, proof and every original payment and reconciled record preserved',()=>{
 const before=fixture(),original=clone(before),r=run(before);assert.deepEqual(before,original,'preview read-only');const {result,saved}=r.commit();assert.equal(result.linked.length,1);assert.equal(saved.expenses['EX-1'].status,'rejected');assert.equal(saved.expenses['EX-1'].duplicateOf,'EX-2');assert.equal(saved.expenses['EX-1'].billPhoto,'/original.jpg');assert.deepEqual(saved.expenses['EX-2'],before.expenses['EX-2']);for(const k of ['bankStatements','bankDateOverrides','bankReconciliationDrafts','bankReconciliationApprovals','transfers'])assert.deepEqual(saved[k],before[k]);assert.equal(saved.auditLog[0].action,'EXPENSE_DUPLICATE_LINKED');assert.deepEqual(before,original);assert.equal(result.integrity.accountMovementsUnchanged,true);
 const again=run(saved).commit();assert.equal(again.result.linked.length,0);assert.equal(again.result.alreadyLinked.length,1);assert.equal(again.saved,null);
});
test('paid-personally pending duplicate may close without losing its retained payment or changing already-reimbursed kept copy',()=>{
 const s=fixture(),p={id:'P',date:'2026-10-01',amount:100,account:'Claimant bank',personalFunds:true};Object.assign(s.expenses['EX-1'],{paidAlready:true,paidAmount:100,personalPaidAmount:100,claimant:'staff',payments:[p],reimbursementStatus:'awaiting_approval'});Object.assign(s.expenses['EX-2'],{nature:'PERSONAL',claimant:'staff',payments:[clone(p)],reimbursementStatus:'reimbursed',reimbursementPayments:[{id:'R',account:'Bank',amount:100,date:'2026-10-03',batchId:'B'}]});const kept=clone(s.expenses['EX-2']),{saved}=run(s).commit();assert.deepEqual(saved.expenses['EX-1'].payments,[p]);assert.equal(saved.expenses['EX-1'].reimbursementStatus,'rejected');assert.deepEqual(saved.expenses['EX-2'],kept);
});
test('stale fingerprints, protected originals, amount mismatch and incomplete paid graph fail before save',()=>{
 const r=run(fixture());assert.throws(()=>apply(r.snapshot,{...body,fingerprint:'old'},req,r.deps),/changed after preview/);
 for(const mutate of [s=>s.expenses['EX-1'].amount=99,s=>s.expenses['EX-1'].approvedAt='2026-10-01',s=>s.expenses['EX-1'].status='paid',s=>s.expenses['EX-2'].payments=[],s=>s.bankDateOverrides['EX-1/P']={bankDate:'2026-10-01'},s=>s.expenses['EX-1'].payments=[{personalFunds:false,amount:100}],s=>s.expenses['EX-1'].reimbursementPayments=[{amount:100}],s=>s.expenses['EX-1'].reconciliationSource={draftId:'D'}]){const s=fixture();mutate(s);const before=clone(s);assert.throws(()=>run(s));assert.deepEqual(s,before);}
});
test('any movement difference blocks commit, even with unchanged payment objects',()=>{
 const r=run(fixture(),body,{movements:s=>Object.values(s.expenses).filter(e=>e.status==='pending').map(e=>e.id)});assert.throws(()=>r.commit(),/change account movements/);
});
test('cycles, chains and repeated source IDs fail; multiple genuine submissions may point to one paid record',()=>{
 for(const pairs of [[{sourceId:'EX-1',targetId:'EX-1'}],[...body.pairs,...body.pairs],[...body.pairs,{sourceId:'EX-2',targetId:'EX-3'}],[null]])assert.throws(()=>run(fixture(),{...body,pairs}));
 const s=fixture();s.expenses['EX-3']={...clone(s.expenses['EX-1']),id:'EX-3'};assert.equal(run(s,{...body,pairs:[...body.pairs,{sourceId:'EX-3',targetId:'EX-2'}]}).commit().result.linked.length,2);
});
test('related original proofs respect access and preview/apply are Owner-only',()=>{
 const s=run(fixture()).commit().saved;assert.equal(decorate(s,req,s.expenses['EX-2'],()=>true).duplicateSubmissions[0].id,'EX-1');assert.equal(decorate(s,req,s.expenses['EX-2'],()=>false).duplicateSubmissions.length,0);
 const r=run(fixture());assert.throws(()=>plan(r.snapshot,body,{user:{role:'admin'}},r.deps),/Only the Owner/);assert.throws(()=>run(fixture(),body,{canAccess:()=>false}),/restricted/);
 const routes=[];register({post:(path,fn)=>routes.push({path,fn})},r.deps);for(const route of routes){let status;route.fn({user:{role:'admin'},body},{status:n=>({json:b=>{status=n;}})});assert.equal(status,403);}
});
test('legacy movement normalizers operate on detached copies, never leak unrelated fields into a correction',()=>{const s=fixture(),before=clone(s);const r=run(s,body,{movements:view=>{view.procurementAccounting={derived:true};return [];}});assert.deepEqual(s,before);const {saved}=r.commit();assert.equal(saved.procurementAccounting,undefined);assert.deepEqual(s,before);});
