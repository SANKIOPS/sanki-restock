'use strict';

const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {buildView}=require('../modules/incentives');
const source=fs.readFileSync(path.join(__dirname,'..','public','incentives.js'),'utf8');

function sale(id,note,amount){return{id:String(id),name:'#'+id,channel:'POS',note,total:amount,createdAt:'2026-10-01T12:00:00+05:30',customer:{name:'Customer'},paymentTransactions:[{id:String(id),gateway:'Cash',kind:'sale',status:'success',amount,processedAt:'2026-10-01T12:00:00+05:30'}]};}
function emptyState(){return{reviews:{},approvals:{},payments:[]};}
async function render(view,options={}){
  const elements=new Map(),requests=[];
  function element(id){
    if(!elements.has(id))elements.set(id,{id,value:'',innerHTML:'',textContent:'',dataset:{},classList:{toggle(){}},querySelectorAll(selector){
      const name=selector.replace(/^\[|\]$/g,''),matches=Array.from(this.innerHTML.matchAll(new RegExp(name+'="([^"]+)"','g')));
      this.buttons=matches.map(match=>({dataset:{[name.replace(/^data-/,'').replace(/-([a-z])/g,(_,letter)=>letter.toUpperCase())]:match[1]}}));this.buttonGroups=this.buttonGroups||{};this.buttonGroups[selector]=this.buttons;return this.buttons;
    },showModal(){this.open=true;},close(){this.open=false;}});
    return elements.get(id);
  }
  const document={getElementById:element,querySelectorAll(){return[];}};
  vm.runInNewContext(source,{document,fetch:async(url,requestOptions)=>{requests.push({url,options:requestOptions});return{json:async()=>options.respond?options.respond(url,requestOptions):view};},console,Intl,Date,confirm:options.confirm||(()=>{throw new Error('Unexpected approval confirmation');})},{filename:'incentives.js'});
  await new Promise(resolve=>setImmediate(resolve));
  return{elements,requests};
}

const tick=()=>new Promise(resolve=>setImmediate(resolve));
function impact(){return{success:true,orderId:'1',orderNumber:'#1',excluded:true,previewToken:'fresh-preview',totalAdjustment:-240,affectedDays:[{date:'2026-10-01',salesperson:'Shivam',previousEligible:12000,eligible:0,hasApproval:true,approvedBefore:240,approvedAfter:0}],balances:[{salesperson:'Shivam',previousBalance:140,balance:-100,overpaid:100}]};}

test('approved receipt offers exclusion, previews before saving and requires a reason and token',async()=>{
  const current=emptyState();current.approvals['2026-10-01|Shivam']={id:'2026-10-01|Shivam',date:'2026-10-01',salesperson:'Shivam',eligibleAmount:12000,incentive:240};
  const view=buildView([sale(1,'Shivam',12000)],current),{elements,requests}=await render(view,{respond:url=>url.endsWith('/preview')?impact():view});
  assert.match(elements.get('orders').innerHTML,/Exclude incentive/);assert.match(elements.get('orders').innerHTML,/approved/);
  elements.get('orders').buttons[0].onclick();await tick();
  assert.equal(elements.get('exclusionDialog').open,true);assert.equal(requests.length,2);assert.ok(requests[1].url.endsWith('/preview'));
  assert.match(elements.get('exclusionImpact').innerHTML,/₹240 → ₹0/);assert.match(elements.get('exclusionImpact').innerHTML,/Overpaid \/ recoverable ₹100/);
  elements.get('exclusionForm').onsubmit({preventDefault(){}});assert.equal(requests.length,2);assert.match(elements.get('exclusionError').textContent,/reason/);
  elements.get('exclusionReason').value='Owner reviewed';elements.get('exclusionForm').onsubmit({preventDefault(){}});await tick();
  const body=JSON.parse(requests[2].options.body);assert.equal(body.previewToken,'fresh-preview');assert.equal(body.reason,'Owner reviewed');assert.equal(body.excluded,true);
  assert.equal(elements.get('exclusionDialog').open,false);assert.match(elements.get('message').textContent,/payments are unchanged/);
});

test('restoration is explicit and escaping dialog never commits a decision',async()=>{
  const current=emptyState();current.exclusions={'1':{excluded:true,reason:'Reviewed',by:'owner',at:'2026-10-25'}};
  const view=buildView([sale(1,'Shivam',12000)],current),{elements,requests}=await render(view,{respond:url=>url.endsWith('/preview')?{...impact(),excluded:false}:view});
  assert.match(elements.get('orders').innerHTML,/Restore incentive/);assert.match(elements.get('orders').innerHTML,/Manually excluded: Reviewed/);
  elements.get('orders').buttons[0].onclick();await tick();
  assert.equal(JSON.parse(requests[1].options.body).excluded,false);assert.match(elements.get('exclusionSave').textContent,/Restore/);
  elements.get('exclusionDialog').oncancel({preventDefault(){assert.fail('Not saving, escape should be allowed');}});
  elements.get('exclusionReason').value='Restored';elements.get('exclusionForm').onsubmit({preventDefault(){}});assert.equal(requests.length,2);
});

test('stale-save error requires another impact preview before retry',async()=>{
  const view=buildView([sale(1,'Shivam',12000)],emptyState()),{elements,requests}=await render(view,{respond:url=>url.endsWith('/preview')?impact():url.includes('/exclusions/')?{success:false,error:'Orders changed. Refresh the impact preview.'}:view});
  elements.get('orders').buttons[0].onclick();await tick();elements.get('exclusionReason').value='Reviewed';elements.get('exclusionForm').onsubmit({preventDefault(){}});await tick();
  assert.equal(elements.get('exclusionDialog').open,true);assert.equal(elements.get('exclusionSave').disabled,true);assert.match(elements.get('exclusionError').textContent,/Refresh/);
  elements.get('exclusionForm').onsubmit({preventDefault(){}});assert.equal(requests.length,3);
  elements.get('exclusionRefresh').onclick();await tick();assert.equal(requests.length,4);assert.equal(elements.get('exclusionSave').disabled,false);
});

test('negative adjustment ledger shows recoverable amount and never enables another payment',async()=>{
  const current=emptyState();current.approvals['2026-10-01|Shivam']={id:'2026-10-01|Shivam',date:'2026-10-01',salesperson:'Shivam',eligibleAmount:12000,incentive:240};
  current.payments=[{id:'INCP-1',salesperson:'Shivam',date:'2026-10-02',amount:100}];
  current.exclusionAdjustments=[{id:'INCX-1',approvalKey:'2026-10-01|Shivam',salesperson:'Shivam',date:'2026-10-25',qualifyingDate:'2026-10-01',orderNumber:'#1',excluded:true,amount:-240,reason:'Manual owner decision',by:'owner'}];
  const {elements}=await render(buildView([],current)),html=elements.get('ledgers').innerHTML;
  assert.match(html,/Overpaid \/ recoverable ₹100/);assert.match(html,/data-pay="Shivam" disabled/);assert.match(html,/num excluded">-₹240/);assert.match(html,/Manual owner decision · owner/);
});

test('reviewing a manually excluded order does not silently change its underlying salesperson decision',async()=>{
  const current=emptyState();current.exclusions={'1':{excluded:true,reason:'Manual decision'}};
  const {elements}=await render(buildView([sale(1,'Shivam',12000)],current));
  elements.get('orders').buttonGroups['[data-review]'][0].onclick();
  assert.equal(elements.get('noSalesperson').checked,false,'restoring later must not be blocked by a no-salesperson decision the user never made');
});

test('record-only UI labels former sales and offers no approval or payment action',async()=>{
  const view=buildView([sale(1,'Isha',25000),sale(2,'Nandini',30000),sale(4,'Simran',35000)],emptyState()),{elements,requests}=await render(view);
  assert.match(elements.get('days').innerHTML,/Former employee · no incentive payable/);assert.doesNotMatch(elements.get('days').innerHTML,/data-approve=/);
  assert.match(elements.get('orders').innerHTML,/Edit record/);assert.match(elements.get('orders').innerHTML,/not payable/);assert.doesNotMatch(elements.get('orders').innerHTML,/below threshold|<b>2%<\/b>/);
  assert.match(elements.get('ledgers').innerHTML,/sales records only/);assert.doesNotMatch(elements.get('ledgers').innerHTML,/data-pay="(?:Isha|Nandini|Simran)"/);
  assert.match(elements.get('orders').innerHTML,/<b>Simran<\/b>: ₹0 · not payable/);
  assert.equal(requests.length,1);assert.ok(!requests[0].options);
});

test('mixed sale UI retains active approval while excluding former share',async()=>{
  for(const name of ['Isha','Nandini','Simran']){
    const view=buildView([sale(3,'Shivam + '+name,24000)],emptyState()),{elements}=await render(view);
    assert.match(elements.get('days').innerHTML,/data-approve="2026-10-01\|Shivam"/);assert.doesNotMatch(elements.get('days').innerHTML,new RegExp('data-approve="2026-10-01\\|'+name+'"'));
    assert.match(elements.get('orders').innerHTML,/<b>Shivam<\/b>: ₹240/);assert.match(elements.get('orders').innerHTML,new RegExp('<b>'+name+'<\\/b>: ₹0 · not payable'));
  }
});

test('historical former entries stay visible without a payment button; active payment still opens',async()=>{
  const current=emptyState();
  for(const salesperson of ['Shivam','Isha','Nandini','Simran'])current.approvals['2026-10-01|'+salesperson]={id:'2026-10-01|'+salesperson,date:'2026-10-01',salesperson,eligibleAmount:12000,incentive:240};
  const view=buildView([],current),{elements}=await render(view),html=elements.get('ledgers').innerHTML;
  assert.match(html,/historical entries preserved/);assert.match(html,/data-pay="Shivam"/);assert.doesNotMatch(html,/data-pay="(?:Isha|Nandini|Simran)"/);
  elements.get('ledgers').buttons.find(button=>button.dataset.pay==='Shivam').onclick();
  assert.equal(elements.get('paymentPerson').value,'Shivam');assert.equal(elements.get('paymentAmount').value,'240.00');assert.equal(elements.get('paymentDialog').open,true);
});

test('salesperson filters and manual review list all former names with explicit record-only labels',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','incentives.html'),'utf8');
  for(const name of ['Isha','Nandini','Simran']){
    assert.match(html,new RegExp('<option value="'+name+'">'+name+' — former, record only</option>'));
    assert.match(html,new RegExp('name="reviewPerson" value="'+name+'"> '+name+' — former, record only'));
  }
  assert.match(html,/<option value="record_only">Former employee — record only<\/option>/);
  assert.match(html,/<script src="\/incentives\.js\?v=20261025-manual-exclusions"><\/script>/,'exclusion UI must load a versioned script instead of a previously cached renderer');
  assert.match(html,/same order earn 2% without meeting the threshold again, on the date received/);
});

function partialSale(){
  const row=sale(2864,'Shivam',57500);row.paymentTransactions=[{...row.paymentTransactions[0],id:'first',amount:50000},{...row.paymentTransactions[0],id:'balance',amount:7500,processedAt:'2026-10-02T12:00:00+05:30'}];return row;
}

test('later collection UI explains inherited qualification and offers normal approval below threshold',async()=>{
  const unrelated=sale(67,'Shivam',1000);unrelated.paymentTransactions[0].processedAt='2026-10-02T12:00:00+05:30';
  const view=buildView([partialSale(),unrelated],emptyState(),{from:'2026-10-02',to:'2026-10-02'});let confirmation='';
  const {elements,requests}=await render(view,{confirm:message=>{confirmation=message;return false;}});
  assert.match(elements.get('days').innerHTML,/Previously qualified orders: ₹7,500 · no new daily threshold required/);
  assert.match(elements.get('days').innerHTML,/Incentive applies to ₹7,500 of ₹8,500 received/);
  assert.match(elements.get('days').innerHTML,/data-approve="2026-10-02\|Shivam"/);
  assert.match(elements.get('orders').innerHTML,/<b>Shivam<\/b>: ₹150 · order qualified on 2026-10-01/);
  assert.match(elements.get('orders').innerHTML,/<b>Shivam<\/b>: ₹0 · below threshold/);
  elements.get('days').buttons[0].onclick();assert.match(confirmation,/from ₹7,500 qualifying receipts/);assert.match(confirmation,/Includes ₹7,500 received against previously qualified orders/);assert.doesNotMatch(confirmation,/₹8,500 qualifying receipts/);assert.equal(requests.length,1,'cancelled approval must not submit');
});

test('later collection UI identifies the unresolved qualifying date and disables approval',async()=>{
  const unknown=sale(68,'Shivam',100);unknown.paymentTransactions[0].gateway='Unknown tender';
  const view=buildView([partialSale(),unknown],emptyState(),{from:'2026-10-02',to:'2026-10-02'}),{elements}=await render(view);
  assert.match(elements.get('days').innerHTML,/Review qualifying date\(s\) 2026-10-01 before approval/);
  assert.match(elements.get('days').innerHTML,/data-approve="2026-10-02\|Shivam" disabled/);
});
