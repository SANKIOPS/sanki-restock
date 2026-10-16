'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const publicFile=file=>fs.readFileSync(path.join(__dirname,'../public',file),'utf8');
const helper=publicFile('navigation-state.js');
function browser(url){
  const window={location:new URL(url),history:{state:{existing:'preserved'},replaceState(state,title,target){this.state=state;window.location=new URL(target,window.location);}}};
  const ctx=vm.createContext({window,URL,URLSearchParams,Date,console});vm.runInContext(helper,ctx);ctx.SankiNavigation=window.SankiNavigation;
  return ctx;
}
function section(file,start,end){const source=publicFile(file),a=source.indexOf(start),b=source.indexOf(end,a+start.length);assert.ok(a>=0&&b>a,start);return source.slice(a,b);}
test('view context survives refresh, with existing query, hash and history state intact',()=>{
  const ctx=browser('https://example.test/expenses.html?tab=log&account=Axis%20Bank%203448&unrelated=keep#detail');
  ctx.SankiNavigation.set({tab:'cash',draft:'BST-123',ledgerFrom:'2026-10-01'});
  const fresh=browser(ctx.window.location.href);
  assert.equal(fresh.SankiNavigation.choice('tab',['log','cash'],'log'),'cash');
  assert.equal(fresh.SankiNavigation.text('draft',/^[A-Za-z0-9_-]+$/,''),'BST-123');
  assert.equal(fresh.window.location.searchParams.get('account'),'Axis Bank 3448');
  assert.equal(fresh.window.location.searchParams.get('unrelated'),'keep');assert.equal(fresh.window.location.hash,'#detail');
  assert.equal(ctx.window.history.state.existing,'preserved');
  ctx.SankiNavigation.set({focus:null,draft:null},'other');assert.equal(ctx.window.location.hash,'#other');assert.equal(ctx.window.location.searchParams.has('draft'),false);
});
test('tabs are per-address, do not leak across open tabs or modules, and ignore malformed state',()=>{
  const first=browser('https://example.test/salary.html'),second=browser('https://example.test/salary.html');
  first.SankiNavigation.set({tab:'advances',entity:'SAMAST',month:'2026-09'});
  assert.equal(second.SankiNavigation.choice('tab',['payroll','advances'],'payroll'),'payroll');
  const bad=browser('https://example.test/salary.html?tab=%22%5D%3Cscript%3E&month=2026-13&date=2026-02-30&draft=%3Cimg%3E');
  assert.equal(bad.SankiNavigation.choice('tab',['payroll','advances'],'payroll'),'payroll');
  assert.equal(bad.SankiNavigation.month('month','2026-10'),'2026-10');assert.equal(bad.SankiNavigation.date('date','2026-10-16'),'2026-10-16');assert.equal(bad.SankiNavigation.text('draft',/^[A-Za-z0-9_-]+$/,''),'');
  assert.doesNotMatch(helper,/fetch\(|localStorage|sessionStorage|\.click\(/,'restoring navigation must not save forms or replay business actions');
});
test('history failure cannot break navigation',()=>{
  const ctx=browser('https://example.test/expenses.html');ctx.window.history.replaceState=()=>{throw Error('unavailable');};
  assert.equal(ctx.SankiNavigation.set({tab:'list'}),false);
});
test('expense tabs record the selected page and permissions still gate restored pages',()=>{
  const tabs=['log','list','pending','receivables','spending','vendors','salaryledgers','creditcards','cash','reimbursements','categories','audit','approvals'];
  const ctx=browser('https://example.test/expenses.html?tab=cash'),buttons=tabs.map(t=>({dataset:{t},style:{display:''},classList:{toggle(){}}}));
  ctx.cfg={claimantOnly:false,canManageCategories:true,isAdmin:true};ctx.tab='log';ctx.setMsg=()=>{};ctx.setOpenTabGroup=()=>{};ctx.groupForTab=()=>'';
  ctx.el=id=>id==='panel'?{dataset:{}}:{querySelector:selector=>buttons.find(b=>selector.includes('"'+b.dataset.t+'"')),querySelectorAll:()=>buttons};
  for(const name of ['Log','List','PendingPayments','Receivables','SpendingDashboard','Vendors','SalaryLedgers','CreditCards','Cash','Reimbursements','Categories','Audit','Approvals'])ctx['render'+name]=()=>name;
  vm.runInContext(section('expenses.html','    function render(){','    // ── LOG form'),ctx);
  for(const tab of tabs){ctx.tab=tab;ctx.render();assert.equal(ctx.window.location.searchParams.get('tab'),tab);}
  ctx.cfg.claimantOnly=true;ctx.tab='cash';assert.equal(ctx.render(),'List');assert.equal(ctx.window.location.searchParams.get('tab'),'list');
  ctx.cfg.claimantOnly=false;buttons.find(b=>b.dataset.t==='audit').style.display='none';ctx.tab='audit';assert.equal(ctx.render(),'Log');
});
test('leaving a deep-linked expense clears focus and model-create overrides before refresh',()=>{
  const ctx=browser('https://example.test/expenses.html?tab=list&focus=EX-001&modelContractId=MC-1');let handler;
  ctx.el=()=>({addEventListener:(event,fn)=>handler=fn,querySelectorAll:()=>[]});ctx.focusId='EX-001';ctx.focusedExpenseId='EX-001';ctx.modelCalendarContractId='MC-1';ctx.setOpenTabGroup=()=>{};ctx.groupForTab=()=>'';ctx.render=()=>{};
  vm.runInContext(section('expenses.html',"    el('tabs').addEventListener('click'",'    function render(){'),ctx);
  handler({target:{closest:selector=>selector==='button[data-t]'?{dataset:{t:'vendors'}}:null}});
  assert.equal(ctx.window.location.searchParams.get('tab'),'vendors');assert.equal(ctx.window.location.searchParams.has('focus'),false);assert.equal(ctx.window.location.searchParams.has('modelContractId'),false);
  assert.equal(ctx.focusedExpenseId,'');assert.equal(ctx.modelCalendarContractId,'');
});
test('salary restores an empty selected month/entity/view without jumping to seeded July',async()=>{
  for(const tab of ['payroll','attendance','employees','increments','advances']){
    const ctx=browser('https://example.test/salary.html?tab='+tab+'&entity=SAMAST&month=2026-12');let loaded;
    ctx.state={};ctx.pad=n=>String(n).padStart(2,'0');ctx.fetch=async()=>({json:async()=>({username:'owner',roles:['owner']})});ctx.load=()=>loaded={...ctx.state};
    const boot=section('salary.html','    // A refresh must keep','  </script>');vm.runInContext(boot,ctx);await new Promise(setImmediate);
    assert.equal(loaded.ym,'2026-12');assert.equal(loaded.entity,'SAMAST');assert.equal(loaded.view,tab);
  }
});
test('restored salary context cannot expose a restricted payment-only screen or entity',async()=>{
  const ctx=browser('https://example.test/salary.html?tab=employees&entity=SAMAST&month=2026-09');let loaded;
  ctx.state={};ctx.pad=n=>String(n).padStart(2,'0');ctx.fetch=async()=>({json:async()=>({username:'prashant',roles:['claimant']})});ctx.el=()=>({querySelectorAll:()=>[]});ctx.load=()=>loaded={...ctx.state};
  vm.runInContext(section('salary.html','    // A refresh must keep','  </script>'),ctx);await new Promise(setImmediate);
  assert.equal(loaded.view,'payroll');assert.equal(loaded.entity,'SANKI');assert.equal(loaded.ym,'2026-09');
});
test('saved reconciliation period is restored by a read only request, not a posting',async()=>{
  const ctx=browser('https://example.test/expenses.html?tab=cash&draft=BST-123');const fields=new Map(),calls=[];
  ctx.el=id=>{if(!fields.has(id))fields.set(id,{value:id==='bs_account'?'IndusInd Bank 8181':'',style:{},textContent:'',innerHTML:''});return fields.get(id);};
  ctx.bankNature='SANKI';ctx.bankStatementLoadGeneration=0;ctx.bankDraftSwitcher=()=>'';ctx.renderBankHistory=()=>'';ctx.esc=String;ctx.fmt=String;
  ctx.api=async(url,options)=>{calls.push({url,options});return{success:true,transactions:[],imports:[],draftOptions:[],draft:{draftId:'BST-123'}};};
  ctx.renderBankReconciliation=d=>ctx.SankiNavigation.set({draft:d.draftId});
  vm.runInContext(section('expenses.html','    function loadBankStatement(','    function uploadBankStatement('),ctx);
  await ctx.loadBankStatement();assert.equal(calls.length,1);assert.match(calls[0].url,/draftId=BST-123/);assert.equal(calls[0].options.method,undefined);assert.equal(ctx.window.location.searchParams.get('draft'),'BST-123');
});
test('ledger refresh preserves an explicit date range and changing accounts clears only the old draft',async()=>{
  const ctx=browser('https://example.test/expenses.html?tab=cash&account=IndusInd%20Bank%208181&draft=BRD-123&ledgerFrom=2026-10-01&ledgerTo=2026-10-07');
  const fields=new Map(),calls=[];ctx.el=id=>{if(!fields.has(id))fields.set(id,{value:'',style:{},innerHTML:'',remove(){}});return fields.get(id);};
  ctx.el('lg_account').value='IndusInd Bank 8181';ctx.el('lg_from').value='2026-10-01';ctx.el('lg_to').value='2026-10-07';
  ctx.ledgerLoadedAccount='';ctx.selectedNature='SANKI';ctx.cfg={isOwner:false,isAdmin:false};ctx.bankAccountsForNature=()=>[];ctx.reconciliationAccountsForNature=()=>[];ctx.esc=String;ctx.fmt=String;
  ctx.api=async(url,options)=>{calls.push({url,options});return{success:true,account:ctx.el('lg_account').value,entries:[],lastReconciliation:{from:'2026-08-01'},balance:0};};
  vm.runInContext(section('expenses.html','    function loadAccountLedger(){',"    var bankDraftId='"),ctx);
  ctx.loadAccountLedger();await new Promise(setImmediate);
  assert.equal(ctx.el('lg_from').value,'2026-10-01');assert.equal(calls.length,1);assert.match(calls[0].url,/from=2026-10-01&to=2026-10-07/);assert.equal(ctx.window.location.searchParams.get('draft'),'BRD-123');
  ctx.el('lg_account').value='Axis Bank 3448';ctx.loadAccountLedger();await new Promise(setImmediate);
  assert.equal(ctx.window.location.searchParams.has('draft'),false);assert.equal(ctx.window.location.searchParams.get('account'),'Axis Bank 3448');assert.ok(calls.every(call=>!call.options?.method||call.options.method==='GET'));
  assert.equal(ctx.el('lg_from').value,'2026-08-01','opening a different ledger still applies its ordinary finalized-period default');assert.match(calls.at(-1).url,/from=2026-08-01/);
});
test('all Velocity sections survive refresh with the matching active navigation button',()=>{
  for(const tab of ['sku','product','category','vendor','momentum','pressure','recommend']){
    const ctx=browser('https://example.test/velocity.html?tab='+tab),buttons=['sku','product','category','vendor','momentum','pressure','recommend'].map(t=>({dataset:{tab:t},active:false,classList:{toggle(_name,on){buttons.find(b=>b.dataset.tab===t).active=on;}}}));
    ctx.State={activeTab:ctx.SankiNavigation.choice('tab',buttons.map(b=>b.dataset.tab),'sku')};let rendered='';
    ctx.document={querySelectorAll:selector=>selector==='.nav-tab'?buttons:[]};
    for(const key of ['Sku','Product','Category','Vendor','Momentum','Pressure','Recommend'])ctx['render'+key+'Tab']=()=>rendered=key.toLowerCase();
    vm.runInContext(section('velocity.html','function renderTab() {','function tableHTML('),ctx);ctx.renderTab();
    assert.equal(rendered,tab);assert.equal(buttons.find(b=>b.active).dataset.tab,tab);assert.equal(ctx.window.location.searchParams.get('tab'),tab);
  }
});
test('inventory care register restores pending/history and outstanding views without actions',()=>{
  for(const view of ['cleaning','miscellaneous','pending','history']){
    const ctx=browser('https://example.test/inventory-care.html?view='+view),register={innerHTML:''},buttons=['cleaning','miscellaneous','pending','history'].map(value=>({dataset:{view:value},setAttribute(_key,value){this.selected=value;}}));
    ctx.view=ctx.SankiNavigation.choice('view',buttons.map(b=>b.dataset.view),'cleaning');ctx.data={operations:[],batches:[]};ctx.el=()=>register;ctx.document={querySelectorAll:()=>buttons};ctx.filterBatch=()=>true;ctx.batchCard=()=>'';ctx.operation=()=>'';
    vm.runInContext(section('inventory-care.js','  function register() {','  async function refreshRegister()'),ctx);ctx.register();
    assert.equal(buttons.find(b=>b.selected==='true').dataset.view,view);assert.equal(ctx.window.location.searchParams.get('view'),view);assert.match(register.innerHTML,/No /);
  }
});
test('standalone tabbed pages load navigation helper before their application scripts',()=>{
  for(const file of ['expenses.html','salary.html','index.html','accounting.html','velocity.html','inventory-care.html','inventory.html','meta-ads.html','pnl.html','rentals.html','model-calendar.html','showroom-replenishment.html','seo-control.html','orders.html','inventory-stats.html','fresh-procurement.html']){
    const html=publicFile(file);assert.ok(html.includes('/navigation-state.js?v='),file);
    const helperAt=html.indexOf('/navigation-state.js');const inline=html.indexOf('SankiNavigation.');if(inline>=0)assert.ok(helperAt<inline,file);
  }
  // Purchases and Inventory Statistics already have their own restore flow.
  assert.match(publicFile('procurement.html'),/restorePurchasesTab\(\)/);assert.match(publicFile('inventory-stats.html'),/const _state = loadState\(\)/);
});
test('all shipped page scripts and external scripts parse',()=>{
  for(const file of fs.readdirSync(path.join(__dirname,'../public'))){
    const source=publicFile(file);
    if(file.endsWith('.js'))new vm.Script(source,{filename:file});
    if(file.endsWith('.html'))for(const match of source.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))new vm.Script(match[1],{filename:file});
  }
});
test('changed external scripts are content-versioned so first refresh cannot run stale navigation code',()=>{
  const assets=['navigation-state.js','inventory-care.js','inventory-enhancements.js','inventory-movements.js','meta-ads.js','model-calendar.js','pnl.js','rental-register.js','seo-control.js','shoot-planner.js'];
  for(const file of fs.readdirSync(path.join(__dirname,'../public')).filter(file=>file.endsWith('.html'))){
    for(const match of publicFile(file).matchAll(/<script\s+src="\/([^"?]+)(?:\?v=([^"]+))?"/g))if(assets.includes(match[1])){
      const hash=require('node:crypto').createHash('sha256').update(publicFile(match[1])).digest('hex').slice(0,12);assert.equal(match[2],hash,file+' loads '+match[1]);
    }
  }
});
test('offline refresh never substitutes another module or Dashboard',async()=>{
  const handlers={};let cacheRequest;
  const context={self:{addEventListener:(event,fn)=>handlers[event]=fn},location:{origin:'https://example.test'},URL,Response,fetch:async()=>{throw Error('offline');},caches:{match:async key=>{cacheRequest=key;return undefined;}}};
  vm.runInNewContext(publicFile('sw.js'),context);let result;
  handlers.fetch({request:{method:'GET',mode:'navigate',url:'https://example.test/salary.html?tab=advances'},respondWith:promise=>result=promise});
  const response=await result;assert.equal(response.status,503);assert.equal(cacheRequest,'/salary.html');assert.match(await response.text(),/current page address is unchanged/);
  assert.doesNotMatch(publicFile('sw.js'),/caches\.match\('\/dashboard\.html'\)/);
});
