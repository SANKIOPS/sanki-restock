'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const publicDir=path.join(__dirname,'..','public');
const script=fs.readFileSync(path.join(publicDir,'till-date-filter.js'),'utf8');

// Small DOM model for event wiring; the browser fixture checks actual DOM/layout.
function harness(now='2026-10-15T12:00:00Z'){
  const timers=[],observers=[];
  class Node{
    constructor(tag,props={}){Object.assign(this,{tagName:tag.toLowerCase(),children:[],parentElement:null,dataset:{},attrs:{},value:'',textContent:'',id:'',name:'',title:'',type:'',className:'',min:'',max:'',disabled:false,readOnly:false,listeners:{},events:[],clicks:0},props);this.classList={contains:k=>this.className.split(' ').includes(k),add:k=>{if(!this.classList.contains(k))this.className+=' '+k;},remove:k=>{this.className=this.className.split(' ').filter(x=>x!==k).join(' ');},toggle:(k,v)=>v?this.classList.add(k):this.classList.remove(k)};}
    appendChild(n){n.parentElement=this;this.children.push(n);return n;}
    contains(n){return this===n||this.children.some(c=>c.contains(n));}
    get isConnected(){return document.documentElement.contains(this);}
    matches(selector){return selector.split(',').some(s=>{s=s.trim();if(s[0]==='.')return this.classList.contains(s.slice(1));if(s[0]==='[')return this.getAttribute(s.slice(1,-1))!=null;const m=s.match(/^(\w+)(?:\[type="(\w+)"\])?$/);return m&&this.tagName===m[1]&&(!m[2]||this.type===m[2]);});}
    closest(s){return this.matches(s)?this:this.parentElement&&this.parentElement.closest(s);}
    querySelectorAll(s){return this.children.flatMap(c=>(c.matches(s)?[c]:[]).concat(c.querySelectorAll(s)));}
    querySelector(s){return this.querySelectorAll(s)[0]||null;}
    setAttribute(k,v){this.attrs[k]=String(v);}
    getAttribute(k){return this.attrs[k]??null;}
    insertAdjacentElement(where,node){assert.equal(where,'beforebegin');node.parentElement=this.parentElement;this.parentElement.children.splice(this.parentElement.children.indexOf(this),0,node);}
    addEventListener(k,fn){(this.listeners[k]??=[]).push(fn);}
    dispatchEvent(e){if(!e.target)e.target=this;this.events.push(e.type);if(this['on'+e.type])this['on'+e.type](e);(this.listeners[e.type]||[]).forEach(fn=>fn(e));if(e.bubbles&&this.parentElement)this.parentElement.dispatchEvent(e);return true;}
    click(){this.clicks++;this.dispatchEvent({type:'click',bubbles:true,target:this});}
  }
  const html=new Node('html'),head=html.appendChild(new Node('head')),body=html.appendChild(new Node('body'));
  const document={documentElement:html,head,body,readyState:'complete',createElement:t=>new Node(t),querySelectorAll:s=>html.querySelectorAll(s),getElementById:id=>html.querySelectorAll('input').find(n=>n.id===id)||null,addEventListener:(k,fn)=>html.addEventListener(k,fn)};
  function section(prefix='lf',raw=false){const root=body.appendChild(new Node('section',{className:'card'})),filters=root.appendChild(new Node('div',{className:'filters'}));const from=new Node('input',{type:'date',id:prefix+'From',value:'2026-10-01'}),to=new Node('input',{type:'date',id:prefix+'To',value:'2026-10-15'});for(const node of [from,to]){if(raw)filters.appendChild(node);else{const wrap=filters.appendChild(new Node('div',{className:'fld'}));wrap.appendChild(new Node('label',{textContent:node===from?'From':'To'}));wrap.appendChild(node);}}const apply=filters.appendChild(new Node('button',{textContent:'Filter'}));return {root,filters,from,to,apply,buttons:()=>filters.querySelector('.sanki-date-mode').children};}
  class Clock extends Date{constructor(...args){super(...(args.length?args:[now]));}static now(){return Date.parse(now);}}
  const context=vm.createContext({window:{},document,Date:Clock,Event:class{constructor(type,opts){this.type=type;Object.assign(this,opts);}},MutationObserver:class{constructor(fn){observers.push(fn);}observe(){}},setTimeout:fn=>timers.push(fn)});
  return {document,Node,section,load:()=>vm.runInContext(script,context),enhance:()=>observers.forEach(fn=>fn()),flush:()=>{while(timers.length)timers.shift()();},context};
}

test('Last 180 days includes today and refreshes only its report',()=>{
  const h=harness(),a=h.section('lf'),b=h.section('vf');h.load();const [range,till,recent]=a.buttons();recent.click();h.flush();
  assert.equal(a.from.value,'2026-04-19');assert.equal(a.to.value,'2026-10-15');assert.equal((Date.parse(a.to.value)-Date.parse(a.from.value))/86400000+1,180);
  assert.equal(a.apply.clicks,1);assert.equal(b.apply.clicks,0);assert.equal(b.from.value,'2026-10-01');assert.equal(a.from.disabled,false);assert.equal(recent.getAttribute('aria-pressed'),'true');assert.equal(range.getAttribute('aria-pressed'),'false');assert.equal(till.getAttribute('aria-pressed'),'false');assert.ok(a.from.events.includes('input'));assert.ok(a.to.events.includes('change'));
});
test('IST day changes independently of host timezone',()=>{const h=harness('2026-10-14T19:00:00Z'),a=h.section();h.load();a.buttons()[2].click();h.flush();assert.equal(a.to.value,'2026-10-15');assert.equal(a.from.value,'2026-04-19');});
test('leap year and year boundaries remain exactly 180 days',()=>{for(const now of ['2024-03-01T12:00:00Z','2026-01-01T12:00:00Z']){const h=harness(now),a=h.section();h.load();a.buttons()[2].click();h.flush();assert.equal((Date.parse(a.to.value)-Date.parse(a.from.value))/86400000+1,180);}});
test('180-day view exits Till date; switching back retains its start',()=>{const h=harness(),a=h.section();h.load();a.buttons()[1].click();h.flush();assert.equal(a.from.disabled,true);assert.equal(a.from.value,'');a.buttons()[2].click();h.flush();assert.equal(a.from.disabled,false);assert.equal(a.from.value,'2026-04-19');a.buttons()[1].click();h.flush();a.buttons()[0].click();h.flush();assert.equal(a.from.value,'2026-04-19');assert.equal(a.buttons()[2].getAttribute('aria-pressed'),'false');});
test('P&L accounting start and report maximum date are respected',()=>{const h=harness(),a=h.section();a.from.min='2026-08-22';a.to.max='2026-10-10';h.load();a.buttons()[2].click();h.flush();assert.equal(a.from.value,'2026-08-22');assert.equal(a.to.value,'2026-10-10');});
test('raw inputs, camel-case IDs and two pairs in one container do not cross-wire',()=>{const h=harness(),a=h.section('ana',true),b=h.section('db',true);for(const child of [...b.filters.children])a.filters.appendChild(child);h.load();const controls=a.filters.querySelectorAll('.sanki-date-mode');assert.equal(controls.length,2);controls[1].children[2].click();h.flush();assert.equal(b.from.value,'2026-04-19');assert.equal(a.from.value,'2026-10-01');});
test('manual custom dates clear the preset state without an unsolicited refresh',()=>{const h=harness(),a=h.section();h.load();a.buttons()[2].click();h.flush();a.from.value='2026-09-01';a.from.dispatchEvent({type:'input',bubbles:true});h.flush();assert.equal(a.buttons()[2].getAttribute('aria-pressed'),'false');assert.equal(a.buttons()[0].getAttribute('aria-pressed'),'true');assert.equal(a.apply.clicks,1);});
test('dynamic filters gain one control and repeated script/observer calls do not duplicate it',()=>{const h=harness();h.load();const a=h.section();h.enhance();h.enhance();h.load();assert.equal(a.filters.querySelectorAll('.sanki-date-mode').length,1);a.buttons()[2].click();h.flush();assert.equal(a.apply.clicks,1);});
test('transaction dates and read-only date pairs are not enhanced',()=>{const h=harness(),a=h.section();a.from.readOnly=true;const form=h.document.body.appendChild(new h.Node('form'));form.appendChild(new h.Node('input',{id:'paymentDate',type:'date'}));h.load();assert.equal(h.document.querySelectorAll('.sanki-date-mode').length,0);});
test('native quick ranges restore editable From after Till date',()=>{const h=harness(),a=h.section();const native=a.root.appendChild(new h.Node('button',{textContent:'7 days'}));native.setAttribute('data-range','7');native.onclick=()=>{a.from.value='2026-10-09';a.to.value='2026-10-15';};h.load();a.buttons()[1].click();h.flush();native.click();h.flush();assert.equal(a.from.disabled,false);assert.equal(a.from.value,'2026-10-09');assert.equal(a.buttons()[1].getAttribute('aria-pressed'),'false');});
test('native range clicks do not clear a different report’s active 180-day state',()=>{const h=harness(),a=h.section('lf'),b=h.section('vf'),native=b.root.appendChild(new h.Node('button'));native.setAttribute('data-range','7');native.onclick=()=>{b.from.value='2026-10-09';};h.load();a.buttons()[2].click();h.flush();native.click();h.flush();assert.equal(a.buttons()[2].getAttribute('aria-pressed'),'true');assert.equal(a.from.value,'2026-04-19');});
test('explicit period-filter metadata enables the ad-report history filter',()=>{const h=harness(),a=h.section();a.from.id='periodStartFilter';a.to.id='periodEndFilter';a.from.dataset.dateFilter='from';a.to.dataset.dateFilter='to';h.load();a.buttons()[2].click();h.flush();assert.equal(a.from.value,'2026-04-19');assert.match(fs.readFileSync(path.join(publicDir,'meta-ads.html'),'utf8'),/id="periodEndFilter" data-date-filter="to"/);});
test('Hot Sellers and restock include both IST boundary days, exclude older and future dates',()=>{const html=fs.readFileSync(path.join(publicDir,'index.html'),'utf8'),fn=html.match(/function last180Orders\(orders\)[\s\S]*?\n}/)[0];class Clock extends Date{static now(){return Date.parse('2026-10-15T06:00:00Z');}}const filter=vm.runInNewContext('('+fn+')',{Date:Clock});const rows=[['old','2026-04-18T18:29:59Z'],['start','2026-04-18T18:30:00Z'],['end','2026-10-15T18:29:59Z'],['future','2026-10-15T18:30:00Z'],['invalid','x']].map(([id,created_at])=>({id,created_at}));assert.deepEqual(Array.from(filter(rows),o=>o.id),['start','end']);assert.match(html,/initialPreset === 180 \? last180Orders/);assert.match(html,/filteredOrders = last180Orders\(velocityOrders\)/);assert.match(html,/filteredOrders = last180Orders\(ALL_ORDERS\)/);});
test('all standalone range pages load the shared helper; separate dropdowns expose 180',()=>{
  for(const file of ['expenses.html','orders.html','incentives.html','accounting.html','analytics.html','owner.html','pnl.html'])assert.match(fs.readFileSync(path.join(publicDir,file),'utf8'),/sidebar\.js/,file);
  const index=fs.readFileSync(path.join(publicDir,'index.html'),'utf8');assert.match(index,/src="\/till-date-filter\.js/);
  for(const id of ['rpRangePreset','hotPeriod'])assert.match(index.match(new RegExp('<select id="'+id+'"[\\s\\S]*?<\\/select>'))[0],/value="180"/);
  assert.match(fs.readFileSync(path.join(publicDir,'velocity.html'),'utf8').match(/<select id="windowDays"[\s\S]*?<\/select>/)[0],/value="180"/);
  assert.match(fs.readFileSync(path.join(publicDir,'meta-linked-workspace.js'),'utf8'),/\['180','Last 180 days/);
  assert.match(fs.readFileSync(path.join(publicDir,'salary.html'),'utf8'),/value="180">Last 180 days \+ outstanding/);
});
test('salary activity uses 180 days without hiding older outstanding advances',()=>{
  const {build}=require('../public/salary-advance-activity');const adv=(id,date,outstanding)=>({id,empId:'E1',date,amount:100,recovered:100-outstanding,outstanding,active:true});
  const data={activityAdvances:[adv('BOUNDARY','2026-04-19',0),adv('OUTSIDE','2026-04-18',0),adv('OLD_DUE','2025-01-01',100)]},before=JSON.stringify(data),view=build(data,{today:'2026-10-15',days:180});
  assert.equal(view.cutoff,'2026-04-19');assert.deepEqual(view.rows.map(x=>x.id).sort(),['BOUNDARY','OLD_DUE']);assert.equal(view.outstanding,100);assert.equal(JSON.stringify(data),before);
});
test('changed scripts and all inline scripts parse',()=>{for(const file of ['index.html','salary.html','velocity.html'])for(const match of fs.readFileSync(path.join(publicDir,file),'utf8').matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))assert.doesNotThrow(()=>new vm.Script(match[1]),file);for(const file of ['till-date-filter.js','salary-advance-activity.js','meta-linked-workspace.js'])assert.doesNotThrow(()=>new vm.Script(fs.readFileSync(path.join(publicDir,file),'utf8')),file);});
