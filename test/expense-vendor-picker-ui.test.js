'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
const pickerCode=html.slice(html.indexOf('    var vendorPickers='),html.indexOf('    var telegramLinkedUsers='));
const approvalCode=html.slice(html.indexOf('    var approveSubmitting='),html.indexOf('    window.rejectExpense ='));
const logCode=html.slice(html.indexOf('    function renderLog(){'),html.indexOf('    var salaryEntryAccounts='));
const decode=s=>s.replaceAll('&quot;','"').replaceAll('&#39;',"'").replaceAll('&lt;','<').replaceAll('&gt;','>').replaceAll('&amp;','&');
const esc=s=>String(s==null?'':s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
const flush=()=>new Promise(resolve=>setImmediate(resolve));
function ui(role='accounting'){
  const nodes={},calls=[],documentListeners={};let sandbox;
  function node(id){
    const classes=new Set(),listeners={},attrs={},children=[];
    const n={value:'',textContent:'',style:{},dataset:{},disabled:false,children,listeners,attrs,
      classList:{add:x=>classes.add(x),remove:x=>classes.delete(x),contains:x=>classes.has(x)},
      setAttribute(k,v){attrs[k]=v;},removeAttribute(k){delete attrs[k];},
      addEventListener(k,fn){(listeners[k]||(listeners[k]=[])).push(fn);},
      fire(k,extra={}){const event={target:n,key:'',preventDefault(){this.prevented=true;},...extra};(listeners[k]||[]).forEach(fn=>fn.call(n,event));return event;},
      focus(){sandbox.document.activeElement=n;n.fire('focus');},
      appendChild(child){if(child.parentElement){const old=child.parentElement.children;const i=old.indexOf(child);if(i>=0)old.splice(i,1);}children.push(child);child.parentElement=n;return child;},
      insertBefore(child,before){children.splice(children.indexOf(before),0,child);child.parentElement=n;},
      contains(child){return child===n||children.some(x=>x.contains(child));},
      querySelector(selector){if(selector==='label')return children.find(x=>x.tag==='label')||null;return (n.options||[])[0]||null;},
      querySelectorAll(){return n.options||[];},
      showModal(){n.open=true;},close(){n.open=false;n.fire('close');}
    };
    Object.defineProperty(n,'id',{get:()=>n._id,set:v=>{n._id=v;nodes[v]=n;}});if(id)n.id=id;
    Object.defineProperty(n,'innerHTML',{get:()=>n._html||'',set:source=>{
      n._html=source;
      for(const match of source.matchAll(/\bid="([^"]+)"/g))if(!nodes[match[1]])node(match[1]);
      if(n._id&&/Suggestions$/.test(n._id))n.options=Array.from(source.matchAll(/data-vendor="([^"]+)"/g),match=>{
        const o=node();o.dataset.vendor=decode(match[1]);o.parentElement=n;return o;
      });
    }});
    return n;
  }
  ['f_vendor','f_nature','vendorSuggestions','vendorDropdown','approveVendor','approveVendorSuggestions','approveVendorDropdown','editVendor','editNature','editDlg','approveDlg','approveSummary','approveCategory','approveType','approveMsg','approveGo','approveCancel','payAmountLabel','payAmountHint','panel','tabs'].forEach(node);
  const editField=node(),label=node();label.tag='label';editField.appendChild(label);editField.appendChild(nodes.editVendor);
  nodes.f_nature.value='SANKI';nodes.editNature.value='SAMAST';
  sandbox={window:{},document:{createElement:tag=>{const n=node();n.tag=tag;return n;},
    addEventListener(k,fn){documentListeners[k]=fn;},removeEventListener(){},activeElement:null},
    el:id=>nodes[id]||null,esc,fmt:n=>'₹'+n,today:()=> '2026-10-09',selectedNature:'SANKI',approveId:'EX-1',
    expensesById:{'EX-1':{id:'EX-1',status:'pending',nature:'SANKI',vendor:'Acme',ledger:'Flowers',type:'variable',amount:100,particulars:'Bill'}},
    cfg:{isAdmin:['admin','owner'].includes(role),isOwner:role==='owner',canApprove:role!=='claimant',me:'test-user',natures:role==='samast_accounting'?['SAMAST']:['SANKI','SAMAST'],
      types:['variable','fixed'],bills:['tax invoice'],channels:['Shared'],vendors:['Acme','Beta'],vendorsByNature:{SANKI:['Acme','Beta','<Vendor "quoted">'],SAMAST:['Samast Supplier'],PERSONAL:role==='owner'?['Private supplier']:[]}},
    opts:()=>'',categorySuggestions:()=>'',paymentTypeOpts:()=>'',setMsg:()=>{},loadConfig:()=>Promise.resolve(),loadList:()=>{},
    api:(url,options)=>{calls.push({url,body:JSON.parse(options.body)});return Promise.resolve({success:true});}
  };
  ['onLedgerInput','toggleQrPhoto','togglePaymentFlow','toggleInstallment','saveExpense','toggleSalaryEntry','loadSalaryEntryEmployees','saveSalaryEntry','linkTelegram','loadTelegramUsers','setupTelegram','testTelegram','unlinkTelegram','prefillModelExpense'].forEach(name=>sandbox[name]=()=>{});
  vm.createContext(sandbox);vm.runInContext(pickerCode+approvalCode+logCode,sandbox);
  return {s:sandbox,nodes,calls,documentListeners,node};
}

test('logging exposes the same vendor picker to claimants, approvers, Admin and Owner',()=>{
  for(const role of ['claimant','accounting','samast_accounting','admin','owner']){
    const {s,nodes}=ui(role);s.renderLog();assert.match(nodes.panel.innerHTML,/id="vendorDropdown"/);assert.match(nodes.panel.innerHTML,/id="f_vendor"[^>]*role="combobox"/);
    assert.equal(nodes.f_vendor.dataset.vendorPickerBound,'true');
  }
});
test('all three vendor pickers search case-insensitively and select by click or keyboard',()=>{
  for(const id of ['f_vendor','approveVendor','editVendor']){
    const {s,nodes}=ui();s.bindVendorPicker(id);if(id==='editVendor')nodes.editNature.value='SANKI';
    nodes[id].value='AC';nodes[id].fire('input');const menu=nodes[s.vendorPickers[id].menu];assert.equal(menu.options.length,1);assert.equal(menu.options[0].dataset.vendor,'Acme');
    menu.options[0].fire(id==='approveVendor'?'keydown':'click',{key:'Enter'});assert.equal(nodes[id].value,'Acme');assert.equal(menu.classList.contains('open'),false);assert.equal(nodes[id].attrs['aria-expanded'],'false');
  }
});
test('dropdown button lists alternatives even when a vendor is prefilled; suggestions are escaped',()=>{
  const {s,nodes}=ui();s.bindVendorPicker('f_vendor');nodes.f_vendor.value='Acme';nodes.vendorDropdown.fire('click');
  assert.equal(nodes.vendorSuggestions.options.length,3);assert.match(nodes.vendorSuggestions.innerHTML,/&lt;Vendor &quot;quoted&quot;&gt;/);assert.doesNotMatch(nodes.vendorSuggestions.innerHTML,/<Vendor/);
  nodes.f_vendor.fire('keydown',{key:'ArrowDown'});assert.equal(s.document.activeElement,nodes.vendorSuggestions.options[0]);
  nodes.vendorSuggestions.options[0].fire('keydown',{key:'Escape'});assert.equal(nodes.vendorSuggestions.classList.contains('open'),false);
});
test('logging, editing and approval use their own entity without a cross-entity fallback',()=>{
  const {s,nodes}=ui();s.bindVendorPicker('f_vendor');nodes.f_nature.value='SAMAST';s.renderVendorSuggestions();assert.equal(nodes.vendorSuggestions.options[0].dataset.vendor,'Samast Supplier');
  s.expensesById['EX-1'].nature='SAMAST';s.renderVendorSuggestions('approveVendor',true);assert.equal(nodes.approveVendorSuggestions.options[0].dataset.vendor,'Samast Supplier');
  s.renderVendorSuggestions('editVendor',true);assert.equal(nodes.editVendorSuggestions.options[0].dataset.vendor,'Samast Supplier');
  nodes.editNature.value='PERSONAL';s.renderVendorSuggestions('editVendor',true);assert.equal(nodes.editVendorSuggestions.options.length,0);assert.doesNotMatch(nodes.editVendorSuggestions.innerHTML,/Acme|Private supplier/);
  delete s.cfg.vendorsByNature;s.renderVendorSuggestions('editVendor',true);assert.equal(nodes.editVendorSuggestions.options.length,0);
});
test('plain edit vendor field is enhanced once and menus close outside, on entity change and dialog close',()=>{
  const {s,nodes,documentListeners,node}=ui();assert.equal(nodes.editVendor.parentElement.className,'vendor-input');assert.equal(nodes.editVendorDropdown.attrs['aria-label'],'Show vendors for expense editing');
  const count=nodes.editVendor.listeners.input.length;s.bindVendorPicker('editVendor');assert.equal(nodes.editVendor.listeners.input.length,count);
  s.renderVendorSuggestions('editVendor',true);documentListeners.click({target:nodes.editVendorDropdown});assert.equal(nodes.editVendorSuggestions.classList.contains('open'),true);
  documentListeners.click({target:node()});assert.equal(nodes.editVendorSuggestions.classList.contains('open'),false);
  s.renderVendorSuggestions('editVendor',true);nodes.editNature.fire('change');assert.equal(nodes.editVendorSuggestions.classList.contains('open'),false);
  s.renderVendorSuggestions('approveVendor',true);nodes.approveDlg.close();assert.equal(nodes.approveVendorSuggestions.classList.contains('open'),false);
});
test('approving an already categorized pending expense opens review with vendor, not an immediate approval',()=>{
  const {s,nodes,calls}=ui();s.window.approve('EX-1');assert.equal(calls.length,0);assert.equal(nodes.approveDlg.open,true);assert.equal(nodes.approveVendor.value,'Acme');assert.equal(nodes.approveCategory.value,'Flowers');
  assert.equal(nodes.approveCategory.disabled,true);assert.equal(nodes.approveGo.disabled,false);assert.equal(nodes.approveCategory.attrs.list,'approvalLedgerList_SANKI');
  s.expensesById['EX-1'].ledger='';s.window.approve('EX-1');assert.equal(nodes.approveGo.disabled,true);assert.match(nodes.approveMsg.textContent,/Admin or Owner must assign/);
});
test('unchanged approval makes one approval request; vendor correction uses only the audited vendor patch',async()=>{
  for(const changed of [false,true]){
    const {s,nodes,calls}=ui();s.window.approve('EX-1');if(changed)nodes.approveVendor.value='Beta';nodes.approveGo.fire('click');await flush();
    assert.equal(calls.length,changed?2:1);if(changed)assert.deepEqual(calls[0],{url:'/api/expenses/EX-1',body:{vendor:'Beta'}});
    assert.deepEqual(calls.at(-1),{url:'/api/expenses/EX-1/approve',body:{expectedStatus:'pending'}});assert.equal(nodes.approveDlg.open,false);assert.equal(s.approveSubmitting,false);
  }
});
test('category permission stays intact, while Admin can correct vendor and category in the existing workflow',async()=>{
  const blocked=ui();blocked.s.window.approve('EX-1');blocked.nodes.approveCategory.value='Transport';blocked.nodes.approveGo.fire('click');assert.equal(blocked.calls.length,0);
  const {s,nodes,calls}=ui('admin');s.window.approve('EX-1');nodes.approveVendor.value='Beta';nodes.approveCategory.value='Transport';nodes.approveGo.fire('click');await flush();
  assert.deepEqual(calls[0].body,{vendor:'Beta',ledger:'Transport'});assert.equal(calls.at(-1).url,'/api/expenses/EX-1/approve');
});
test('blank vendor and failed saves never approve; failed approval preserves correction and supports retry',async()=>{
  const blank=ui();blank.s.window.approve('EX-1');blank.nodes.approveVendor.value=' ';blank.nodes.approveGo.fire('click');assert.equal(blank.calls.length,0);assert.match(blank.nodes.approveMsg.textContent,/vendor/);
  const failed=ui();failed.s.api=(url,options)=>{failed.calls.push(url);return Promise.resolve({success:false,error:'Correction refused'});};failed.s.window.approve('EX-1');failed.nodes.approveVendor.value='Beta';failed.nodes.approveGo.fire('click');await flush();assert.deepEqual(failed.calls,['/api/expenses/EX-1']);assert.equal(failed.nodes.approveDlg.open,true);assert.match(failed.nodes.approveMsg.textContent,/Correction refused/);assert.equal(failed.nodes.approveGo.disabled,false);
  const retry=ui();let reject=true;retry.s.api=(url,options)=>{retry.calls.push({url,body:JSON.parse(options.body)});return Promise.resolve(url.endsWith('/approve')?{success:!reject,error:'Proof incomplete'}:{success:true,expense:{...retry.s.expensesById['EX-1'],vendor:'Beta'}});};retry.s.window.approve('EX-1');retry.nodes.approveVendor.value='Beta';retry.nodes.approveGo.fire('click');await flush();assert.equal(retry.nodes.approveDlg.open,true);assert.match(retry.nodes.approveMsg.textContent,/Proof incomplete/);reject=false;retry.nodes.approveGo.fire('click');await flush();assert.equal(retry.calls.length,3);assert.equal(retry.calls[2].url,'/api/expenses/EX-1/approve');assert.equal(retry.nodes.approveDlg.open,false);
});
test('submission lock prevents duplicate saves and undo approval retains its previous route',async()=>{
  const {s,nodes,calls}=ui();let finish;s.api=(url,options)=>{calls.push(url);return new Promise(resolve=>{finish=resolve;});};s.window.approve('EX-1');nodes.approveGo.fire('click');nodes.approveGo.fire('click');await flush();assert.equal(calls.length,1);assert.equal(nodes.approveCancel.disabled,true);assert.equal(nodes.approveDlg.fire('cancel').prevented,true);finish({success:true});await flush();
  s.expensesById['EX-1'].status='approved';s.window.approve('EX-1');assert.equal(calls.at(-1),'/api/expenses/EX-1/approve');
});
test('all inline expense scripts still parse after the shared picker change',()=>{
  for(const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))assert.doesNotThrow(()=>new vm.Script(script[1]));
});
