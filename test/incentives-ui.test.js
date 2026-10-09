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
async function render(view){
  const elements=new Map(),requests=[];
  function element(id){
    if(!elements.has(id))elements.set(id,{id,value:'',innerHTML:'',textContent:'',dataset:{},classList:{toggle(){}},querySelectorAll(selector){
      const name=selector.replace(/^\[|\]$/g,''),matches=Array.from(this.innerHTML.matchAll(new RegExp(name+'="([^"]+)"','g')));
      this.buttons=matches.map(match=>({dataset:{[name.replace(/^data-/,'').replace(/-([a-z])/g,(_,letter)=>letter.toUpperCase())]:match[1]}}));return this.buttons;
    },showModal(){this.open=true;},close(){this.open=false;}});
    return elements.get(id);
  }
  const document={getElementById:element,querySelectorAll(){return[];}};
  vm.runInNewContext(source,{document,fetch:async(url,options)=>{requests.push({url,options});return{json:async()=>view};},console,Intl,Date,confirm:()=>{throw new Error('Unexpected approval confirmation');}},{filename:'incentives.js'});
  await new Promise(resolve=>setImmediate(resolve));
  return{elements,requests};
}

test('record-only UI labels former sales and offers no approval or payment action',async()=>{
  const view=buildView([sale(1,'Isha',25000),sale(2,'Nandini',30000)],emptyState()),{elements,requests}=await render(view);
  assert.match(elements.get('days').innerHTML,/Former employee · no incentive payable/);assert.doesNotMatch(elements.get('days').innerHTML,/data-approve=/);
  assert.match(elements.get('orders').innerHTML,/Edit record/);assert.match(elements.get('orders').innerHTML,/not payable/);assert.doesNotMatch(elements.get('orders').innerHTML,/below threshold|<b>2%<\/b>/);
  assert.match(elements.get('ledgers').innerHTML,/sales records only/);assert.doesNotMatch(elements.get('ledgers').innerHTML,/data-pay="(?:Isha|Nandini)"/);
  assert.equal(requests.length,1);assert.ok(!requests[0].options);
});

test('mixed sale UI retains active approval while excluding former share',async()=>{
  const view=buildView([sale(3,'Shivam + Isha',24000)],emptyState()),{elements}=await render(view);
  assert.match(elements.get('days').innerHTML,/data-approve="2026-10-01\|Shivam"/);assert.doesNotMatch(elements.get('days').innerHTML,/data-approve="2026-10-01\|Isha"/);
  assert.match(elements.get('orders').innerHTML,/<b>Shivam<\/b>: ₹240/);assert.match(elements.get('orders').innerHTML,/<b>Isha<\/b>: ₹0 · not payable/);
});

test('historical former entries stay visible without a payment button; active payment still opens',async()=>{
  const current=emptyState();
  for(const salesperson of ['Shivam','Isha'])current.approvals['2026-10-01|'+salesperson]={id:'2026-10-01|'+salesperson,date:'2026-10-01',salesperson,eligibleAmount:12000,incentive:240};
  const view=buildView([],current),{elements}=await render(view),html=elements.get('ledgers').innerHTML;
  assert.match(html,/historical entries preserved/);assert.match(html,/data-pay="Shivam"/);assert.doesNotMatch(html,/data-pay="Isha"/);
  elements.get('ledgers').buttons.find(button=>button.dataset.pay==='Shivam').onclick();
  assert.equal(elements.get('paymentPerson').value,'Shivam');assert.equal(elements.get('paymentAmount').value,'240.00');assert.equal(elements.get('paymentDialog').open,true);
});

test('salesperson filters and manual review list both former names with explicit record-only labels',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','incentives.html'),'utf8');
  for(const name of ['Isha','Nandini']){
    assert.match(html,new RegExp('<option value="'+name+'">'+name+' — former, record only</option>'));
    assert.match(html,new RegExp('name="reviewPerson" value="'+name+'"> '+name+' — former, record only'));
  }
  assert.match(html,/<option value="record_only">Former employee — record only<\/option>/);
});
