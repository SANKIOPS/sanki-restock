'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const ui=require('../public/manage-opening-balances'),manage=require('../public/manage'),navigation=require('../public/manage-navigation');
const html=fs.readFileSync(path.join(__dirname,'../public/manage.html'),'utf8');
function fixture(extra={}) {return {account:'Synthetic Bank',current:{amount:0,effectiveDate:'2026-08-22'},proposal:null,drafts:[],reason:'',locked:false,baseRevision:'base-1',revision:'rev-1',...extra};}
function harness({owner=false,initial=fixture(),route}={}) {
  const nodes={},calls=[],busy=[],confirmations=[];let current=structuredClone(initial);
  for(const match of html.matchAll(/<([a-z][a-z0-9]*)\b([^>]*\bid="([^"]+)"[^>]*)>/g)) {
    const [,tag,attrs,id]=match;
    const node={id,hidden:/\bhidden\b/.test(attrs),value:'',textContent:'',disabled:false,checked:false,className:'',querySelectorAll(){return Object.values(nodes);}};
    let markup='';Object.defineProperty(node,'innerHTML',{get(){return markup;},set(v){markup=v;if(tag==='select')node.value=(v.match(/<option value="([^"]*)"/)||[])[1]||'';}});nodes[id]=node;
  }
  const config={claimantOnly:false,isAdmin:true,isOwner:owner,approvalNatures:owner?['SANKI','SAMAST','PERSONAL']:['SANKI','SAMAST']};
  const win={confirm(text){confirmations.push(text);return win.confirmResult;},confirmResult:true};
  const api=async(url,options={})=>{
    const body=options.body?JSON.parse(options.body):null;calls.push({url,body});if(route){const result=route(url,body);if(result!==undefined)return result;}
    if(!options.method)return {success:true,rows:[structuredClone(current)],canApply:owner};
    if(url.endsWith('/stage'))current={...current,revision:'rev-2',proposal:{id:'p1',amount:Number(body.amount),effectiveDate:body.effectiveDate,reason:body.reason,baseRevision:'base-1',stagedBy:'synthetic-admin'}};
    else if(url.endsWith('/apply'))current={...current,revision:'rev-3',current:{amount:current.proposal.amount,effectiveDate:current.proposal.effectiveDate},proposal:null};
    else throw Error('Unexpected endpoint');
    return {success:true,row:structuredClone(current)};
  };
  const app=ui.create({getElementById(id){assert.ok(nodes[id],id);return nodes[id];}},win,config,api,value=>busy.push(value));
  return {app,nodes,calls,busy,confirmations,win,config};
}
function fill(h){h.nodes.openingAmount.value='2345.67';h.nodes.openingDate.value='2026-08-22';h.nodes.openingReason.value='Synthetic evidence';}
test('Manage opening tool uses existing Admin permissions and starts under a collapsed head',()=>{
  const catalog=cfg=>navigation.buildCatalog({modules:[{key:'expenses',href:'/expenses.html',status:'live'}],expenseConfig:cfg}).flatMap(h=>h.items);
  const cfg={claimantOnly:false,isAdmin:true};assert.equal(catalog(cfg).find(i=>i.key==='opening-balances').href,'/manage.html?section=opening-balances');
  for(const denied of [null,{}, {...cfg,claimantOnly:true},{...cfg,isAdmin:false}])assert.equal(catalog(denied).some(i=>i.key==='opening-balances'),false);
  assert.match(html,/id="openingWorkspace" hidden/);assert.match(html,/manage-opening-balances\.js\?v=/);
});
test('opening the tool only reads; no automatic stage, apply, approval or finalization',async()=>{
  const h=harness();await h.app.load();assert.equal(h.calls.length,1);assert.equal(h.calls[0].body,null);
  assert.match(h.nodes.openingSummary.innerHTML,/Recorded opening/);assert.equal(h.nodes.openingOwnerReview.hidden,true);
  assert.equal(h.nodes.openingForm.hidden,false);assert.deepEqual(h.busy,[true,false]);
});
test('only save-draft action sends input and explicitly reports no balance change',async()=>{
  const h=harness();await h.app.load();fill(h);assert.equal(await h.app.stage(),true);
  assert.equal(h.calls[1].url,'/api/expenses/opening-balances/stage');assert.equal(h.calls[1].body.revision,'rev-1');
  assert.equal(h.calls[1].body.amount,'2345.67');assert.match(h.nodes.openingMessage.textContent,/Recorded balances.*unchanged/);
  assert.equal(h.nodes.openingOwnerReview.hidden,true);assert.equal(await h.app.apply(),false);assert.equal(h.calls.length,2);
});
test('bank-draft carry-forward is visible and cannot create a competing generic opening',async()=>{
  const h=harness({owner:true,initial:fixture({reason:'An active statement exists',drafts:[{from:'2026-08-22',to:'2026-09-01',openingResolution:{amount:2345.67,reason:'Synthetic source'}}]})});
  await h.app.load();fill(h);assert.match(h.nodes.openingSummary.innerHTML,/opening \/ carry-forward staged only/);
  assert.equal(h.nodes.openingForm.hidden,true);assert.equal(h.nodes.openingOwnerReview.hidden,true);assert.equal(await h.app.stage(),false);assert.equal(h.calls.length,1);
  assert.match(h.nodes.openingReconcileLink.href,/expenses\.html\?tab=cash&nature=SANKI&account=Synthetic%20Bank/);
});
test('Owner apply requires acknowledgement and confirmation of SAVED values, never unsaved form input',async()=>{
  const h=harness({owner:true});await h.app.load();fill(h);await h.app.stage();assert.equal(h.nodes.openingOwnerReview.hidden,false);
  assert.equal(await h.app.apply(),false);assert.equal(h.calls.length,2);
  h.nodes.openingAmount.value='999999';h.nodes.openingDate.value='2026-09-01';h.nodes.openingAcknowledge.checked=true;h.win.confirmResult=false;
  assert.equal(await h.app.apply(),false);assert.equal(h.calls.length,2);assert.match(h.confirmations[0],/2,345.67.*2026-08-22/);assert.doesNotMatch(h.confirmations[0],/999999/);
  h.win.confirmResult=true;assert.equal(await h.app.apply(),true);assert.deepEqual(h.calls[2].body,{nature:'SANKI',account:'Synthetic Bank',proposalId:'p1',revision:'rev-2',acknowledge:true});
  assert.equal(h.nodes.openingOwnerReview.hidden,true);assert.match(h.nodes.openingMessage.textContent,/No settlement or reconciliation was finalized/);
});
test('malformed fields never send requests and zero/negative amounts are accepted',async()=>{
  const h=harness();await h.app.load();fill(h);
  for(const value of ['', '1.999','1e3','1,000']) {h.nodes.openingAmount.value=value;assert.equal(await h.app.stage(),false);}assert.equal(h.calls.length,1);
  for(const value of ['0','-1.25']) {h.nodes.openingAmount.value=value;assert.equal(await h.app.stage(),true);}
});
test('busy save prevents duplicate actions and failed request requires refresh before another write',async()=>{
  let reject;const pending=new Promise((_ok,no)=>{reject=no;});
  const h=harness({route(url){if(url.endsWith('/stage'))return pending;}});await h.app.load();fill(h);
  const first=h.app.stage();assert.equal(await h.app.stage(),false);assert.equal(await h.app.load(),false);assert.equal(h.calls.length,2);
  reject(Error('Connection lost'));assert.equal(await first,false);assert.equal(await h.app.stage(),false);assert.match(h.nodes.openingMessage.textContent,/Refresh before retrying/);
});
test('failed refresh hides forms and revokes cached write capability',async()=>{
  let fail=false;const h=harness({owner:true,route(url){if(fail&&url.includes('?'))throw Error('Unavailable');}});await h.app.load();fill(h);await h.app.stage();
  fail=true;assert.equal(await h.app.load(),false);assert.equal(h.nodes.openingOwnerReview.hidden,true);assert.equal(h.nodes.openingForm.hidden,true);assert.equal(await h.app.stage(),false);
});
test('HTML-sensitive data is escaped and private entity option is Owner-only',async()=>{
  const h=harness({initial:fixture({drafts:[{from:'<img>',to:'test',openingResolution:{amount:1,reason:'<svg onload=evil>'}}]})});await h.app.load();
  assert.doesNotMatch(h.nodes.openingSummary.innerHTML,/<svg|<img>/);assert.match(h.nodes.openingSummary.innerHTML,/&lt;svg/);
  assert.doesNotMatch(h.nodes.openingNature.innerHTML,/PERSONAL/);assert.match(harness({owner:true}).nodes.openingNature.innerHTML,/PERSONAL/);
});
test('malformed opening catalogue fails closed before rendering actionable controls',async()=>{
  const h=harness({route(){return {success:true,rows:[{account:'Incomplete'}]};}});assert.equal(await h.app.load(),false);
  assert.equal(h.nodes.openingForm.hidden,true);assert.equal(h.nodes.openingOwnerReview.hidden,true);assert.equal(await h.app.stage(),false);
});
test('client only uses opening APIs; no settlements or bank-finalize request is available',()=>{
  const js=fs.readFileSync(path.join(__dirname,'../public/manage-opening-balances.js'),'utf8');
  for(const forbidden of ['/bank-statements/finalize','/gokwik/','/balances','/pay','/approve'])assert.equal(js.includes(forbidden),false,forbidden);
  assert.equal(typeof manage.createApp,'function');
});
