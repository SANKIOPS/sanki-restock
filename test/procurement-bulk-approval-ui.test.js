const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');
function between(start,end){const offset=html.indexOf(start);assert.ok(offset>=0,start);return html.slice(offset,html.indexOf(end,offset));}
const functions=[
 between('    function studioReadyFor(', '    // Load the NEW-product groups'),
 between('    function initStudioFor(', '    function renderStudio('),
 between('    function readSeoFields(', '    function refreshPostGate('),
 between('    function refreshPostGate(', '    function wireStudioCard('),
 between('    function wireStudioCard(', '    function uploadBackPhoto('),
 between('    function postToShopify(', '  })();')
].join('\n');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
function deferred(){let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};}
const copy=title=>({displayName:'Cotton',title,metaTitle:'White Cotton Shirt',metaDescription:'White cotton shirt.',imageAlt:'White cotton shirt',tags:['Cotton']});
function fixture(){
 const key='cotton|white',group={key,productType:'Shirt',colour:'White',audience:'Women',photoUrl:'/original.jpg',variants:[]};
 const images=[{type:'front',url:'/generated.jpg',approved:true,qa:{status:'pass'}}];
 const savedCopy=copy('White Cotton Shirt');
 const server={newProducts:[group],existingAdds:[],po:{id:'PO-A',status:'received',aiImages:{[key]:images},seoDraft:[{key,seo:savedCopy,seoApproved:true}]}};
 const requests=[],alerts=[];
 function node(value){return {value,disabled:false,textContent:'',innerHTML:'',isConnected:true};}
 function makeDom(){
  const inputs=Object.fromEntries(Object.entries(savedCopy).filter(([name])=>name!=='tags').map(([name,value])=>[name,node(value)]));
  const button=node();button.textContent='Approve complete PO';
  const individual=node();individual.textContent='Approve images + SEO';
  const post=node(),status=node(),note=node(),seoNote=node(),other=node();other.disabled=true;
  const controls=[...Object.values(inputs),button,individual,post,other];
  const card={querySelector:selector=>{const match=/\[data-seo="([^"]+)"\]/.exec(selector);if(match)return inputs[match[1]];if(selector.startsWith('[data-seonote='))return seoNote;return null;},querySelectorAll:selector=>selector==='[data-seo]'?Object.values(inputs):[]};
  return {inputs,button,individual,post,status,note,seoNote,other,controls,card,studioHost:node(),detail:{querySelectorAll:()=>controls}};
 }
 let dom=makeDom(),ctx;
 const state={server,requests,alerts,dom:()=>dom,fetchImpl:null,refreshes:0};
 function response(value){return {json:async()=>value};}
 ctx={productApprovalRun:null,studioLoadSequence:0,receiveId:'PO-A',lastReceive:JSON.parse(JSON.stringify(server)),studio:{seo:{[key]:{seo:JSON.parse(JSON.stringify(savedCopy)),approved:true}},images:{[key]:JSON.parse(JSON.stringify(images))},styleSaves:{},rejected:{},backRefs:{},styling:{}},confirm:()=>true,alert:message=>alerts.push(message),readJson:r=>r.json(),esc:String,paidTypesFor:()=>['front'],openaiPilotConfig:null,
  el:id=>({'scard0':dom.card,'poDetailHost':dom.detail,'approvePoBtn':dom.button,'postBtn':dom.post,'poApprovalStatus':dom.status,'studioNote':dom.note,'studioHost':dom.studioHost,'warehouse':{value:'55'}}[id]),
  renderStudio:()=>{state.refreshes++;Object.entries(ctx.studio.seo[key].editDraft||ctx.studio.seo[key].seo).forEach(([field,value])=>{if(dom.inputs[field])dom.inputs[field].value=value;});ctx.wireStudioCard(ctx.lastReceive.newProducts[0],0);ctx.refreshPostGate();},
  renderReceive:()=>ctx.refreshPostGate(),
  fetch:async(url,options={})=>{
   const request={url,method:options.method||'GET',body:options.body?JSON.parse(options.body):null};requests.push(request);
   if(state.fetchImpl){const overridden=await state.fetchImpl(request);if(overridden!==undefined)return response(overridden);}
   if(request.method==='POST'){
    if(url.endsWith('/approve-po')){for(const edit of request.body.seoDrafts){server.po.seoDraft.find(item=>item.key===edit.groupKey).seo=edit.seo;server.po.seoDraft.find(item=>item.key===edit.groupKey).seoApproved=true;}return response({success:true,approvedProducts:1});}
    if(url.endsWith('/seo')){server.po.seoDraft[0].seo=request.body.seo;server.po.seoDraft[0].seoApproved=request.body.seoApproved;return response({success:true,seoDraft:server.po.seoDraft});}
    if(url.endsWith('/approve-product')){server.po.seoDraft[0].seoApproved=true;return response({success:true});}
    throw new Error('Unexpected write '+url);
   }
   if(url.includes('/studio?'))return response({success:true,...JSON.parse(JSON.stringify(server))});
   if(url.endsWith('/openai-pilot-status'))return response({success:true,configured:true});
   throw new Error('Unexpected read '+url);
  }
 };
 vm.createContext(ctx);vm.runInContext(functions,ctx);ctx.wireStudioCard(group,0);ctx.refreshPostGate();
 state.ctx=ctx;state.group=group;state.key=key;
 state.navigate=id=>{ctx.receiveId=id;ctx.lastReceive=JSON.parse(JSON.stringify(server));ctx.lastReceive.po.id=id;ctx.studio={seo:{[key]:{seo:copy('Other PO title'),approved:true}},images:{[key]:images},styleSaves:{},rejected:{},backRefs:{}};dom=makeDom();};
 return state;
}

test('successful bulk approval awaits the saved-state refresh, reports its count and restores controls',async()=>{
 const h=fixture(),reload=deferred();h.fetchImpl=async req=>{if(req.url.includes('/studio?'))await reload.promise;};
 const button=h.dom().button,action=h.ctx.approveWholePo(button);await tick();
 assert.equal(button.disabled,true);assert.equal(button.textContent,'Checking PO…');assert.ok(Object.values(h.dom().inputs).every(input=>input.disabled));assert.equal(h.dom().post.disabled,true);
 reload.resolve();await action;await tick();
 assert.equal(button.disabled,false);assert.equal(button.textContent,'Approve complete PO');assert.equal(h.ctx.productApprovalRun,null);assert.equal(h.dom().other.disabled,true);assert.ok(Object.values(h.dom().inputs).every(input=>!input.disabled));
 assert.equal(h.dom().post.disabled,false);assert.match(h.dom().status.textContent,/Approved 1 product\(s\) on PO-A/);assert.equal(h.requests.filter(req=>req.method==='POST').length,1);assert.deepEqual(h.alerts,[]);
});

test('navigation while styling is pending makes no approval request and cannot touch the other PO',async()=>{
 const h=fixture(),style=deferred();h.ctx.studio.styleSaves[h.key]=style.promise;
 const oldButton=h.dom().button,action=h.ctx.approveWholePo(oldButton);h.navigate('PO-B');style.resolve();await action;
 assert.equal(h.requests.length,0);assert.match(h.alerts[0],/open PO changed.*PO-A/);assert.equal(oldButton.disabled,false);assert.equal(h.dom().button.disabled,false);assert.equal(h.dom().status.textContent,'');assert.equal(h.ctx.studio.seo[h.key].seo.title,'Other PO title');
});

test('navigation after sending approval keeps its captured PO URL and never refreshes the new PO',async()=>{
 const h=fixture(),posted=deferred();h.fetchImpl=async req=>{if(req.method==='POST')await posted.promise;};
 const action=h.ctx.approveWholePo(h.dom().button);await tick();h.navigate('PO-B');posted.resolve();await action;
 assert.equal(h.requests.length,1);assert.match(h.requests[0].url,/PO-A\/approve-po$/);assert.equal(h.ctx.receiveId,'PO-B');assert.equal(h.dom().status.textContent,'');assert.equal(h.ctx.studio.seo[h.key].seo.title,'Other PO title');
});

test('failed styling and failed approval restore controls and keep saved copy untouched',async()=>{
 for(const failure of ['styling','approval']){
  const h=fixture();h.dom().inputs.title.value='Edited visible title';h.dom().inputs.title.oninput();
  if(failure==='styling')h.ctx.studio.styleSaves[h.key]=Promise.reject(new Error('Styling save failed'));
  else h.fetchImpl=async req=>req.method==='POST'?{success:false,error:'Another product needs model-side'}:undefined;
  await h.ctx.approveWholePo(h.dom().button);
  assert.equal(h.ctx.studio.seo[h.key].seo.title,'White Cotton Shirt');assert.equal(h.ctx.studio.seo[h.key].editDraft.title,'Edited visible title');assert.equal(h.ctx.studio.seo[h.key].approved,false);assert.equal(h.dom().post.disabled,true);assert.equal(h.dom().button.disabled,false);assert.ok(Object.values(h.dom().inputs).every(input=>!input.disabled));assert.equal(h.alerts.length,1);
  if(failure==='styling')assert.equal(h.requests.length,0);
 }
});

test('pure SEO reads clone values; dirty input survives card refresh and blocks posting',async()=>{
 const h=fixture();h.dom().inputs.title.value='Unsaved title';const draft=h.ctx.readSeoFields(h.group,0);
 draft.seo.tags.push('Extra');assert.equal(h.ctx.studio.seo[h.key].seo.title,'White Cotton Shirt');assert.deepEqual(Array.from(h.ctx.studio.seo[h.key].seo.tags),['Cotton']);assert.equal(h.ctx.studio.seo[h.key].approved,true);
 h.dom().inputs.title.oninput();h.ctx.renderStudio();assert.equal(h.dom().inputs.title.value,'Unsaved title');assert.equal(h.ctx.studioReadyFor(h.group),false);assert.equal(h.dom().post.disabled,true);
 h.ctx.postToShopify('PO-A');h.ctx.doPost('PO-A');assert.equal(h.requests.length,0);assert.equal(h.alerts.length,2);
});

test('approval holds an exclusive UI lock even if a pending styling save redraws the card',async()=>{
 const h=fixture(),style=deferred();h.ctx.studio.styleSaves[h.key]=style.promise;
 const action=h.ctx.approveWholePo(h.dom().button);h.ctx.renderStudio();assert.ok(Object.values(h.dom().inputs).every(input=>input.disabled));
 await h.ctx.approveWholePo(h.dom().button);assert.equal(h.requests.length,0);assert.match(h.alerts[0],/already in progress/);
 style.resolve();await action;assert.equal(h.requests.filter(req=>req.method==='POST').length,1);assert.ok(Object.values(h.dom().inputs).every(input=>!input.disabled));
});

test('saved approval followed by a refresh failure explains recovery and releases the button',async()=>{
 const h=fixture();h.fetchImpl=async req=>{if(req.url.includes('/studio?'))throw new Error('Network disconnected');};
 await h.ctx.approveWholePo(h.dom().button);
 assert.equal(h.server.po.seoDraft[0].seoApproved,true);assert.match(h.alerts[0],/Approval was saved for PO-A.*could not refresh.*Reopen/);assert.equal(h.dom().button.disabled,false);assert.equal(h.dom().button.textContent,'Approve complete PO');assert.equal(h.ctx.productApprovalRun,null);
});

test('canceled bulk approval leaves saved and visible copy, controls and server requests unchanged',async()=>{
 const h=fixture();h.ctx.confirm=()=>false;h.dom().inputs.title.value='Visible unsaved text';await h.ctx.approveWholePo(h.dom().button);
 assert.equal(h.requests.length,0);assert.equal(h.ctx.studio.seo[h.key].seo.title,'White Cotton Shirt');assert.equal(h.dom().inputs.title.value,'Visible unsaved text');assert.equal(h.ctx.studio.seo[h.key].approved,true);assert.equal(h.dom().button.disabled,false);
});

test('single-product approval restores its label and applies the same scoped refresh guards',async()=>{
 const h=fixture(),button=h.dom().individual;await h.ctx.approveProduct(h.group,0,button);await tick();
 assert.equal(button.disabled,false);assert.equal(button.textContent,'Approve images + SEO');assert.match(h.dom().status.textContent,/Approved 1 product/);assert.equal(h.requests.filter(req=>req.method==='POST').length,2);
 const h2=fixture(),seoSave=deferred();h2.fetchImpl=async req=>{if(req.url.endsWith('/seo'))await seoSave.promise;};
 const action=h2.ctx.approveProduct(h2.group,0,h2.dom().individual);await tick();h2.navigate('PO-B');seoSave.resolve();await action;
 assert.equal(h2.requests.length,1);assert.match(h2.requests[0].url,/PO-A\/seo$/);assert.match(h2.alerts[0],/open PO changed/);assert.equal(h2.ctx.studio.seo[h2.key].seo.title,'Other PO title');
});

test('studio loads discard an older response when the user opens a different PO and returns',async()=>{
 const h=fixture(),older=deferred();let loads=0;
 h.fetchImpl=async req=>{if(req.url.includes('/studio?')){loads++;if(loads===1)await older.promise;return {success:true,...JSON.parse(JSON.stringify(h.server)),po:{...h.server.po,id:loads===1?'OLD':'PO-A'}};}};
 const first=h.ctx.initStudioFor('PO-A');h.ctx.receiveId='PO-B';const latest=h.ctx.initStudioFor('PO-A');await latest;const current=h.ctx.lastReceive;older.resolve();assert.equal(await first,null);assert.equal(h.ctx.lastReceive,current);
});
