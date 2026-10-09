'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {build}=require('../public/salary-advance-activity');
const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
const helper=fs.readFileSync(path.join(__dirname,'..','public','salary-advance-activity.js'),'utf8');
const inline=[...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)].map(x=>x[1]).filter(x=>x.trim()).join('\n');
const today='2026-10-09';
function advance(id,overrides={}){return Object.assign({id,empId:'E1',employeeName:'Employee One',date:'2026-09-15',amount:1000,recovered:0,outstanding:1000,active:true,account:'Axis Bank 3448',proofs:['/proof.jpg'],recoveries:[]},overrides);}
function request(id,overrides={}){return Object.assign({id,empId:'E1',employeeName:'Employee One',date:'2026-09-14',createdAt:'2026-09-13T10:00:00Z',amount:1000,account:'Axis Bank 3448',status:'Pending approval'},overrides);}

test('recent activity includes all undeducted advances and unfinished requests, even older ones',()=>{
  const data={activityAdvances:[advance('OLD',{date:'2026-01-01'}),advance('PARTIAL',{date:'2026-01-02',recovered:600,outstanding:400}),advance('SETTLED',{date:'2026-01-03',recovered:1000,outstanding:0}),advance('RECENT',{recovered:1000,outstanding:0}),advance('CANCELLED',{active:false})],requests:[request('PENDING',{date:'2026-02-01',createdAt:'2026-02-01T12:00:00Z'}),request('PROOF',{date:'2026-02-02',createdAt:'2026-02-02T12:00:00Z',status:'Approved – proof required'}),request('REJECTED',{status:'Rejected',rejectionReason:'Not approved'}),request('OLDREJECTED',{date:'2026-01-01',createdAt:'2026-01-01T12:00:00Z',status:'Rejected'})]};
  const before=JSON.stringify(data),view=build(data,{today});
  assert.deepEqual(view.rows.map(x=>x.id).sort(),['CANCELLED','OLD','PARTIAL','PENDING','PROOF','RECENT','REJECTED']);
  assert.equal(view.outstanding,1400);assert.equal(view.outstandingCount,2);assert.equal(view.pendingApproval,1);assert.equal(view.proofRequired,1);
  assert.equal(view.rows.find(x=>x.id==='PARTIAL').status,'Partly deducted');
  assert.equal(view.rows.find(x=>x.id==='RECENT').status,'Fully deducted');
  assert.equal(view.rows.find(x=>x.id==='CANCELLED').outstanding,0);
  assert.equal(view.rows.find(x=>x.id==='REJECTED').note,'Not approved');
  assert.equal(view.rows.find(x=>x.id==='PENDING').outstanding,null);
  assert.equal(JSON.stringify(data),before,'view never alters financial records');
});

test('posted request and advance are one row and one outstanding amount',()=>{
  const r=request('ADVR-1',{status:'Posted',advanceId:'ADV-1',postedAt:'2026-09-15T12:00:00Z'}),a=advance('ADV-1',{requestId:r.id});
  const view=build({activityAdvances:[a],requests:[r]},{today});
  assert.equal(view.rows.length,1);assert.equal(view.outstanding,1000);assert.equal(view.rows[0].requestId,'ADVR-1');assert.equal(view.rows[0].requestStatus,'Posted');
  const linkedByAdvance=build({activityAdvances:[advance('ADV-1')],requests:[r]},{today});
  assert.equal(linkedByAdvance.rows.length,1,'legacy advanceId link also deduplicates');
});

test('rolling windows use the current day, inclusive boundaries and latest request outcomes or deductions',()=>{
  const data={activityAdvances:[advance('START60',{date:'2026-08-11',outstanding:0,recovered:1000}),advance('OUTSIDE60',{date:'2026-08-10',outstanding:0,recovered:1000}),advance('START30',{date:'2026-09-10',outstanding:0,recovered:1000}),advance('OLDNEWDEDUCTION',{date:'2026-01-01',outstanding:0,recovered:1000,recoveries:[{amount:1000,deductionDate:'2026-10-01'}]})],requests:[request('LATE_REJECTION',{date:'2026-01-01',createdAt:'2026-01-01T12:00:00Z',status:'Rejected',rejectedAt:'2026-10-02T12:00:00Z'}),request('FUTURE_PROPOSED',{date:'2026-11-01',createdAt:'2026-10-08T12:00:00Z',status:'Rejected'})]};
  const sixty=build(data,{today,days:60});assert.equal(sixty.cutoff,'2026-08-11');
  assert.deepEqual(sixty.rows.map(x=>x.id).sort(),['FUTURE_PROPOSED','LATE_REJECTION','OLDNEWDEDUCTION','START30','START60']);
  const thirty=build(data,{today,days:30});assert.equal(thirty.cutoff,'2026-09-10');assert.equal(thirty.rows.some(x=>x.id==='START60'),false);
  assert.equal(thirty.rows[0].id,'FUTURE_PROPOSED','sorts by recorded activity, not future proposed payout');
});

test('awaiting deduction only excludes unpaid, rejected, settled and cancelled rows',()=>{
  const view=build({activityAdvances:[advance('OPEN',{date:'2020-01-01'}),advance('DONE',{outstanding:0,recovered:1000}),advance('VOID',{active:false})],requests:[request('UNPAID'),request('DECLINED',{status:'Rejected'})]},{today,onlyOutstanding:true});
  assert.deepEqual(view.rows.map(x=>x.id),['OPEN']);assert.equal(view.outstanding,1000);assert.equal(view.pendingApproval,0);
});

test('complete activity is independent of register filters; fallback histories deduplicate safely',()=>{
  const old=advance('OLD',{date:'2020-01-01'}),recent=advance('RECENT',{outstanding:0,recovered:1000});
  assert.deepEqual(build({advances:[],activityAdvances:[old,recent]},{today}).rows.map(x=>x.id),['RECENT','OLD']);
  const fallback=build({advances:[recent],summary:[{transactions:[old,recent]}]},{today});
  assert.equal(fallback.rows.length,2);assert.equal(fallback.outstanding,1000);
});

test('small amounts are summed in currency precision and empty activity is valid',()=>{
  const view=build({activityAdvances:[advance('A',{amount:.1,outstanding:.1}),advance('B',{amount:.2,outstanding:.2})]},{today});
  assert.equal(view.outstanding,.3);assert.deepEqual(build({}, {today}).rows,[]);
});

function uiHarness(data,{owner=false}={}){
  const nodes=new Map(),calls=[];
  function node(id){
    if(!nodes.has(id)){
      const item={id,value:'',open:false,dataset:{},children:[],classList:{toggle(){},add(){},remove(){}},addEventListener(){},insertAdjacentHTML(){},querySelector(){return null;},scrollIntoView(){this.scrolled=true;},focus(){this.focused=true;},querySelectorAll(selector){if(selector==='[data-advance-employee]')return [...this.innerHTML.matchAll(/data-advance-employee="([^"]+)"/g)].map(m=>({dataset:{advanceEmployee:m[1]}}));return [];}};
      let content='';Object.defineProperty(item,'innerHTML',{get(){return content;},set(value){content=String(value);for(const tag of content.matchAll(/<[^>]+\bid="([^"]+)"[^>]*>/g)){const child=node(tag[1]);child.open=/\sopen(?:\s|>)/.test(tag[0]);}}});
      nodes.set(id,item);
    }
    return nodes.get(id);
  }
  const document={getElementById:node,querySelectorAll(){return [];}};
  class FixedDate extends Date{constructor(...args){super(...(args.length?args:[today+'T12:00:00Z']));}}
  const context={document,Date:FixedDate,FormData:class{},console,fetch(url,options){calls.push({url,options});if(url==='/api/auth/me')return new Promise(()=>{});let response=url.startsWith('/api/salary/employees')?{success:true,employees:[{id:'E1',name:'Employee One',active:true}],advancePayingAccounts:[],salaryPayingAccounts:[]}:url.startsWith('/api/expenses/config')?{accountsByNature:{SANKI:[]}}:data;return Promise.resolve({json:()=>Promise.resolve(response)});}};
  context.window=context;vm.createContext(context);vm.runInContext(helper,context);vm.runInContext(inline,context);
  data.permissions={canRequest:true,canApprove:owner,canDirectPost:owner,canPostProof:true,canEdit:true,canCancel:owner};
  context.state.ym='2026-07';context.loadAdvances();
  return {context,node,calls};
}
const flush=()=>new Promise(resolve=>setImmediate(resolve));

test('UI replaces the banner with a collapsed row, escaped history, accurate balances and independent range switching',async()=>{
  const a=advance('ADV-1',{requestId:'ADVR-1',date:'2020-01-01'}),r=request('ADVR-1',{advanceId:a.id,status:'Posted'}),pending=request('ADVR-2',{status:'Approved – proof required',note:'<script>alert(1)</script>'});
  const data={success:true,activityAdvances:[a],advances:[],requests:[r,pending],summary:[{empId:'E1',name:'Employee <One>',transactions:[a],requests:[pending],outstanding:1000}],totals:{outstanding:1000},editRequests:[],sourceSheet:{fileName:'old.xlsx',items:[],total:0}};
  const {context,node,calls}=uiHarness(data);await flush();
  const main=node('host').innerHTML,body=node('recentAdvanceBody').innerHTML;
  assert.doesNotMatch(main,/Uploaded advances|Archived source sheet/);
  assert.match(main,/<details class="card" id="recentAdvanceActivity">/,'collapsed by default');
  assert.ok(main.indexOf('recentAdvanceActivity')<main.indexOf('employeeAdvanceRegister'));
  assert.match(node('recentAdvanceSummary').textContent,/2 entries · ₹1,000 left/);
  assert.match(body,/Employee &lt;One&gt;/);assert.match(body,/&lt;script&gt;alert\(1\)&lt;\/script&gt;/);assert.doesNotMatch(body,/<script>/);
  assert.match(body,/Request approved &amp; paid/);assert.match(body,/Approved – proof required/);assert.match(body,/Not deducted/);assert.match(body,/View payment proof 1/);
  assert.equal((main+body).match(/id="adr_proof_ADVR-2"/g).length,1,'posting proof inputs are not duplicated');
  node('ad_note').value='Unsubmitted reason';const reads=calls.length;
  node('recentAdvanceRange').value='outstanding';node('recentAdvanceRange').onchange();
  assert.match(node('recentAdvanceSummary').textContent,/1 entries · ₹1,000 left/);assert.doesNotMatch(node('recentAdvanceBody').innerHTML,/ADVR-2/);
  assert.equal(node('ad_note').value,'Unsubmitted reason');assert.equal(calls.length,reads,'range changes neither refetch nor post');
  node('recentAdvanceActivity').open=true;node('recentAdvanceActivity').ontoggle.call(node('recentAdvanceActivity'));assert.equal(context.state.advanceActivityOpen,true);
  context.loadAdvances();await flush();assert.match(node('host').innerHTML,/<details class="card" id="recentAdvanceActivity" open>/,'preserves user-expanded state on reload');
  assert.equal(calls.some(x=>x.options&&x.options.method&&x.options.method!=='GET'),false);
});

test('one-time source worksheet is retained only inside collapsed owner import tools',async()=>{
  const data={success:true,advances:[],activityAdvances:[],requests:[],summary:[],totals:{outstanding:0},editRequests:[],sourceSheet:{fileName:'old.xlsx',items:[],total:0},sourceSheetPostingAccounts:[]};
  const {node}=uiHarness(data,{owner:true});await flush();const main=node('host').innerHTML;
  assert.doesNotMatch(main,/Uploaded advances/);assert.match(main,/<summary>Upload advances Excel sheet<\/summary>[\s\S]*<summary>Archived source sheet \(one-time import\)<\/summary>/);
  assert.match(main,/<details style="margin-top:14px"><summary>Archived source sheet/);assert.match(node('recentAdvanceBody').innerHTML,/No recent requests or outstanding advances/);
});

test('employee history shortcut expands and focuses the right register employee without posting',async()=>{
  const a=advance('ADV-1'),data={success:true,activityAdvances:[a],advances:[a],requests:[],summary:[{empId:'E2',name:'Other',transactions:[],requests:[]},{empId:'E1',name:'Employee One',transactions:[a],requests:[]}],totals:{outstanding:1000},editRequests:[]};
  const {context,node,calls}=uiHarness(data);await flush();
  const detail={open:false,scrollIntoView(){this.scrolled=true;},querySelector(){return summary;}},summary={focus(options){this.focused=options.preventScroll;}};
  node('employeeAdvanceRegister').querySelector=()=>({tBodies:[{rows:[{querySelector(){throw Error('wrong employee');}},{querySelector:()=>detail}]}]});
  const button={dataset:{advanceEmployee:'E1'}};node('recentAdvanceBody').querySelectorAll=()=>[button];
  const reads=calls.length;
  context.renderRecentAdvanceActivity();button.onclick();
  assert.equal(detail.open,true);assert.equal(detail.scrolled,true);assert.equal(summary.focused,true);assert.equal(summary.tabIndex,-1);
  assert.equal(calls.length,reads);
});

test('all salary inline scripts and the browser helper parse',()=>{
  assert.doesNotThrow(()=>new vm.Script(inline));assert.doesNotThrow(()=>new vm.Script(helper));assert.match(html,/src="\/salary-advance-activity\.js\?v=20261009-1"/);
});
