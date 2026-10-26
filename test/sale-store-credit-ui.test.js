'use strict';

const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
const start=html.indexOf('    var saleCreditSavedDetails=entryDetails;'),end=html.indexOf('    window.correctSaleAllocation=',start);
assert.ok(start>=0&&end>start,'store-credit dialog consumer block exists');
const code=html.slice(start,end),previewUrl='/api/expenses/sale-store-credit-corrections/preview',saveUrl='/api/expenses/sale-store-credit-corrections';
const saleId='SHOPIFY/2903',reason='Owner confirmed this replacement uses credit from the returned order.';
function setup(options={}){
  const elements=new Map(),calls=[],messages=[],refreshes=[];
  function el(id){
    if(!elements.has(id))elements.set(id,{value:'',textContent:'',innerHTML:'',disabled:id==='scSave',open:false,readOnly:id==='scAmount',showModal(){this.open=true;},close(){this.open=false;}});
    return elements.get(id);
  }
  const owner=options.owner!==false,cfg={isAdmin:options.admin!==false,isOwner:owner};
  const row={id:saleId,kind:'sale',editableSale:true,orderNumber:'2903',gross:2499,credit:2499},ledgerEditRows={[saleId]:{account:'Counter Cash',nature:'SANKI',row},'SHOPIFY/other':{account:'Counter Cash',row:{...row,id:'SHOPIFY/other',orderNumber:'other'}}};
  const context={window:{},cfg,el,ledgerEditRows,entryDetails:row=>'retained details '+row.id,entryEditButton:row=>'retained edit '+row.id,esc:value=>String(value).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'),fmt:value=>'₹'+Number(value).toFixed(2),setMsg:(...args)=>messages.push(args),loadCash:async()=>{refreshes.push('cash');},loadAccountLedger:async()=>{refreshes.push('ledger');},loadConfig:async()=>{refreshes.push('config');},syncApprovalsTab:()=>{refreshes.push('approvals');},api:async(url,request)=>{
    calls.push({url,request,body:JSON.parse(request.body)});
    if(options.respond)return options.respond(url,request,calls.length);
    return url===previewUrl?{success:true,proposal:{orderNumber:'2903',originalOrderNumber:'2902',storeCreditAmount:2499},previewFingerprint:'source-fingerprint',ownerApprovalRequired:!owner}:owner?{success:true,approved:true,correction:{storeCreditAmount:2499,requestedBy:'owner',approvedBy:'owner'}}:{success:true,approved:false,request:{id:'RQ-SYNTHETIC',by:'admin',status:'pending'}};
  }};
  vm.runInNewContext(code,context,{filename:'sale-store-credit-dialog.js'});
  function open(){context.window.openStoreCreditCorrection(saleId);}
  function fill(){el('scOriginal').value=' #2902 ';el('scOriginal').oninput();el('scReason').value=' '+reason+' ';el('scReason').oninput();}
  async function preview(){open();fill();await el('scReview').onclick();}
  return{context,cfg,el,elements,calls,messages,refreshes,row,ledgerEditRows,open,fill,preview};
}

test('all inline expenses scripts compile, including the new dialog handlers',()=>{
  let count=0;for(const match of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script\s*>/gi)){
    if(/\bsrc\s*=/.test(match[1])||/\btype\s*=\s*["']application\/(?:ld\+)?json["']/.test(match[1]))continue;
    new vm.Script(match[2],{filename:'expenses-inline-'+(++count)+'.js'});
  }
  assert.ok(count>0,'at least one inline app script compiled');
});

test('opening requires authorized Counter Cash sale and keeps the legacy actions intact',()=>{
  const ui=setup();assert.match(ui.context.entryEditButton(ui.row,'Counter Cash'),/retained edit.*Store-credit exchange/);assert.equal(ui.context.entryDetails({id:'EX-1',kind:'expense'}),'retained details EX-1');
  ui.open();assert.equal(ui.el('saleCreditDlg').open,true);assert.match(ui.el('scSummary').textContent,/#2903/);assert.equal(ui.el('scAmount').value,'2499.00');assert.equal(ui.el('scSave').disabled,true);
  for(const patch of [{admin:false},{account:'Axis Bank 3448'},{editableSale:false}]){
    const blocked=setup({admin:patch.admin});if(patch.account)blocked.ledgerEditRows[saleId].account=patch.account;if(patch.editableSale===false)blocked.row.editableSale=false;
    blocked.open();assert.equal(blocked.el('saleCreditDlg').open,false);assert.equal(blocked.calls.length,0);
  }
});

test('store-credit amount is read-only and the modal explicitly leaves Shopify wallet adjustment to the user',()=>{
  assert.match(html,/<input\b(?=[^>]*\bid="scAmount")(?=[^>]*\breadonly\b)[^>]*>/);
  const dialog=html.slice(html.indexOf('<dialog id="saleCreditDlg"'),html.indexOf('</dialog>',html.indexOf('<dialog id="saleCreditDlg"')));
  assert.match(dialog,/does not debit that wallet/);assert.match(dialog,/preserves both orders and the original cash receipt/);
  const ui=setup();ui.open();assert.equal(ui.el('scAmount').value,'2499.00');assert.equal(ui.calls.length,0);
});

test('save cannot run before a successful preview and missing source or reason never submits',async()=>{
  const ui=setup();ui.open();await ui.el('scSave').onclick();assert.equal(ui.calls.length,0);
  ui.el('scOriginal').value='#2902';await ui.el('scReview').onclick();assert.equal(ui.calls.length,0);assert.match(ui.el('scMsg').textContent,/original returned order.*reason/);
  ui.el('scOriginal').value='';ui.el('scReason').value=reason;await ui.el('scReview').onclick();assert.equal(ui.calls.length,0);assert.equal(ui.el('scSave').disabled,true);
});

test('preview then save sends exact immutable sale ID, original order, amount, reason and source fingerprint',async()=>{
  const ui=setup();await ui.preview();assert.equal(ui.calls.length,1);assert.equal(ui.calls[0].url,previewUrl);assert.deepEqual(ui.calls[0].body,{saleId,originalOrderNumber:'#2902',storeCreditAmount:2499,reason});
  assert.equal(ui.el('scSave').disabled,false);assert.match(ui.el('scPreview').textContent,/cash ₹0.*store credit ₹2499\.00.*#2902/);assert.match(ui.el('scPreview').textContent,/No bank\/Paytm receipt or cash refund/);
  await ui.el('scSave').onclick();assert.equal(ui.calls[1].url,saveUrl);assert.equal(ui.calls[1].request.method,'POST');assert.deepEqual(ui.calls[1].body,{saleId,originalOrderNumber:'#2902',storeCreditAmount:2499,reason,previewFingerprint:'source-fingerprint'});
  assert.equal(ui.calls.length,2);assert.deepEqual(Object.keys(ui.calls[1].body).sort(),['originalOrderNumber','previewFingerprint','reason','saleId','storeCreditAmount'].sort());
});

test('editing either original order or reason invalidates preview and requires a new review',async()=>{
  for(const field of ['scOriginal','scReason']){
    const ui=setup();await ui.preview();ui.el(field).value+=' changed';ui.el(field).oninput();assert.equal(ui.el('scSave').disabled,true);assert.equal(ui.el('scPreview').textContent,'');
    await ui.el('scSave').onclick();assert.equal(ui.calls.length,1);await ui.el('scReview').onclick();assert.equal(ui.calls.length,2);assert.equal(ui.calls[1].body[field==='scOriginal'?'originalOrderNumber':'reason'],ui.el(field).value.trim());
  }
});

test('Owner apply and Admin request have distinct labels and truthful outcomes',async()=>{
  const owner=setup();await owner.preview();assert.equal(owner.el('scSave').textContent,'Confirm app correction');assert.match(owner.el('scPreview').textContent,/Confirm to apply/);
  await owner.el('scSave').onclick();assert.match(owner.el('scMsg').textContent,/App correction saved.*cash ₹0.*store credit ₹2499\.00/);assert.match(owner.el('scMsg').textContent,/wallet adjustment remains for you/);assert.equal(owner.messages.length,1);assert.equal(owner.messages[0][1],true);assert.deepEqual(owner.refreshes.sort(),['approvals','cash','config','ledger']);
  const admin=setup({owner:false});await admin.preview();assert.equal(admin.el('scSave').textContent,'Submit for Owner approval');assert.match(admin.el('scPreview').textContent,/Balances change only after Owner approval/);
  await admin.el('scSave').onclick();assert.match(admin.el('scMsg').textContent,/RQ-SYNTHETIC.*Owner approval/);assert.match(admin.el('scMsg').textContent,/Cash and incentives remain unchanged until approved/);assert.doesNotMatch(admin.el('scMsg').textContent,/App correction saved/);
});

test('preview refusal and already-requested or already-approved results never enable another save',async()=>{
  for(const response of [{success:false,error:'Counter Cash period is locked.'},{success:true,already:true,approved:false},{success:true,already:true,approved:true}]){
    const ui=setup({respond:()=>response});await ui.preview();assert.equal(ui.el('scSave').disabled,true);await ui.el('scSave').onclick();assert.equal(ui.calls.length,1);
    assert.match(ui.el('scMsg').textContent,response.success?response.approved?/already approved/:/already awaiting Owner/:/period is locked/);assert.equal(ui.messages.length,0);
  }
});

test('server save failure remains visible, clears approval token and never shows fake success',async()=>{
  const ui=setup({respond:url=>url===previewUrl?{success:true,proposal:{orderNumber:'2903',originalOrderNumber:'2902',storeCreditAmount:2499},previewFingerprint:'checked'}:{success:false,error:'The source evidence changed. Review again.'}});
  await ui.preview();await ui.el('scSave').onclick();assert.equal(ui.calls.length,2);assert.match(ui.el('scMsg').textContent,/source evidence changed/);assert.equal(ui.el('scSave').disabled,true);assert.equal(ui.messages.length,0);assert.deepEqual(ui.refreshes,[]);
  await ui.el('scSave').onclick();assert.equal(ui.calls.length,2);
});

test('uncertain network outcome prevents blind resubmission and tells user to inspect existing corrections',async()=>{
  const ui=setup({respond:url=>{if(url===previewUrl)return{success:true,proposal:{orderNumber:'2903',originalOrderNumber:'2902',storeCreditAmount:2499},previewFingerprint:'checked'};throw new Error('Connection lost after send');}});
  await ui.preview();await ui.el('scSave').onclick();assert.match(ui.el('scMsg').textContent,/outcome could not be confirmed/);assert.match(ui.el('scMsg').textContent,/check existing corrections before retrying/);assert.equal(ui.el('scSave').disabled,true);assert.equal(ui.messages.length,0);
  await ui.el('scSave').onclick();await ui.el('scSave').onclick();assert.equal(ui.calls.length,2);
});

test('preview network failure preserves user inputs but cannot submit an unverified correction',async()=>{
  const ui=setup({respond:()=>{throw new Error('Offline');}});await ui.preview();assert.match(ui.el('scMsg').textContent,/verified preview.*Offline/);assert.equal(ui.el('scOriginal').value,' #2902 ');assert.equal(ui.el('scReason').value,' '+reason+' ');assert.equal(ui.el('scSave').disabled,true);
  await ui.el('scSave').onclick();assert.equal(ui.calls.length,1);assert.equal(ui.messages.length,0);
});

test('in-flight preview and save reject duplicate clicks or switching the selected sale',async()=>{
  let resolvePreview,resolveSave;const ui=setup({respond:url=>new Promise(resolve=>{if(url===previewUrl)resolvePreview=resolve;else resolveSave=resolve;})});
  ui.open();ui.fill();const reviewing=ui.el('scReview').onclick();assert.equal(ui.calls.length,1);assert.equal(ui.el('scOriginal').disabled,true);assert.equal(ui.el('scReason').disabled,true);
  await ui.el('scReview').onclick();await ui.el('scSave').onclick();ui.context.window.openStoreCreditCorrection('SHOPIFY/other');assert.match(ui.el('scSummary').textContent,/#2903/);assert.equal(ui.calls.length,1);
  resolvePreview({success:true,proposal:{orderNumber:'2903',originalOrderNumber:'2902',storeCreditAmount:2499},previewFingerprint:'checked'});await reviewing;assert.equal(ui.el('scOriginal').disabled,false);
  const saving=ui.el('scSave').onclick();assert.equal(ui.calls.length,2);await ui.el('scSave').onclick();await ui.el('scReview').onclick();assert.equal(ui.calls.length,2);
  resolveSave({success:true,approved:true,correction:{storeCreditAmount:2499}});await saving;assert.equal(ui.messages.length,1);
});

test('retained audit details show source, reason, approved status and pending manual wallet action',()=>{
  const ui=setup(),row={id:'STORE-CREDIT/2903',kind:'sale_store_credit_correction',storeCreditAmount:2499,originalOrderNumber:'#2902',reason:'Confirmed <replacement> & credit',requestedBy:'admin',approvedBy:'owner',approvedAt:'2026-10-26T10:00:00Z',walletStatus:'pending_external_user_action'},before=JSON.stringify(row),details=ui.context.entryDetails(row);
  assert.match(details,/Cash ₹0.*Store credit ₹2499\.00/);assert.match(details,/returned order #2902/);assert.match(details,/Confirmed &lt;replacement&gt; &amp; credit/);assert.match(details,/Owner-approved/);assert.match(details,/wallet adjustment remains pending/);assert.equal(JSON.stringify(row),before);
  const stale=ui.context.entryDetails({...row,stale:true});assert.match(stale,/Source evidence changed.*review required/);assert.doesNotMatch(stale,/Owner-approved app correction/);
});

test('missing, blank or invalid preview fingerprint and missing proposal fail closed before any save',async()=>{
  const proposal={orderNumber:'2903',originalOrderNumber:'2902',storeCreditAmount:2499};
  for(const response of [{success:true,proposal},{success:true,proposal,previewFingerprint:''},{success:true,proposal,previewFingerprint:'   '},{success:true,proposal,previewFingerprint:123},{success:true,previewFingerprint:'verified-but-no-proposal'}]){
    const ui=setup({owner:false,respond:()=>response});await ui.preview();assert.equal(ui.el('scSave').disabled,true);assert.match(ui.el('scMsg').textContent,/did not return a verified preview.*Nothing was saved/);
    await ui.el('scSave').onclick();assert.equal(ui.calls.length,1);assert.equal(ui.calls[0].url,previewUrl);assert.equal(ui.messages.length,0);assert.deepEqual(ui.refreshes,[]);
  }
});

test('retained requester and Owner approval identity and date are displayed safely without implying wallet completion',()=>{
  const ui=setup(),row={id:'STORE-CREDIT/2903',kind:'sale_store_credit_correction',storeCreditAmount:2499,originalOrderNumber:'#2902',reason:'Confirmed exchange',appStoreCreditCorrection:{requestedBy:'admin <reviewer>',approvedBy:'owner & approver',approvedAt:'2026-10-26T10:12:34Z',walletStatus:'pending_external_user_action'}},before=JSON.stringify(row),details=ui.context.entryDetails(row);
  assert.match(details,/Requested by admin &lt;reviewer&gt;/);assert.match(details,/Approved by owner &amp; approver/);assert.match(details,/2026-10-26 10:12/);assert.match(details,/Owner-approved app correction/);assert.match(details,/wallet adjustment remains pending outside this app/);assert.doesNotMatch(details,/wallet (?:debited|completed|adjusted)/i);assert.equal(JSON.stringify(row),before);
  const absent=ui.context.entryDetails({...row,appStoreCreditCorrection:{}});assert.match(absent,/Requested by —.*Approved by —/);assert.doesNotMatch(absent,/undefined|null/);
});

test('customer refresh requires the original order number and never looks up an empty source',async()=>{
  const ui=setup({owner:false});ui.open();ui.el('scOriginal').value='  ';await ui.el('scRefresh').onclick();assert.equal(ui.calls.length,0);assert.match(ui.el('scMsg').textContent,/original returned order number first/);assert.equal(ui.el('scSave').disabled,true);assert.equal(ui.messages.length,0);
});

test('customer refresh sends only selected immutable sale ID and source order number',async()=>{
  const ui=setup({owner:false,respond:()=>({success:true,updated:1,customerMatchVerified:true})});ui.open();ui.fill();
  ui.el('scAmount').value='999999.99';ui.el('customerId').value='untrusted-customer';ui.el('walletAmount').value='999999.99';ui.el('scReason').value='Not evidence for customer identity';
  await ui.el('scRefresh').onclick();assert.equal(ui.calls.length,1);assert.equal(ui.calls[0].url,saveUrl+'/refresh-customers');assert.equal(ui.calls[0].request.method,'POST');assert.deepEqual(ui.calls[0].body,{saleId,originalOrderNumber:'#2902'});assert.deepEqual(Object.keys(ui.calls[0].body).sort(),['originalOrderNumber','saleId']);assert.equal(ui.el('scSave').disabled,true);assert.equal(ui.messages.length,0);
});

test('successful customer refresh invalidates prior preview and still requires review then Owner approval',async()=>{
  const ui=setup({owner:false,respond:url=>url===saveUrl+'/refresh-customers'?{success:true,updated:1,customerMatchVerified:true}:url===previewUrl?{success:true,proposal:{orderNumber:'2903',originalOrderNumber:'2902',storeCreditAmount:2499},previewFingerprint:'fresh-customer-preview',ownerApprovalRequired:true}:{success:true,approved:false,request:{id:'RQ-REFRESHED',status:'pending'}}});
  await ui.preview();assert.equal(ui.el('scSave').disabled,false);const original=ui.el('scOriginal').value,explanation=ui.el('scReason').value;
  await ui.el('scRefresh').onclick();assert.equal(ui.el('scSave').disabled,true);assert.equal(ui.el('scPreview').textContent,'');assert.match(ui.el('scMsg').textContent,/customer match verified.*Review.*again/);assert.equal(ui.el('scOriginal').value,original);assert.equal(ui.el('scReason').value,explanation);
  await ui.el('scSave').onclick();assert.equal(ui.calls.length,2);assert.equal(ui.messages.length,0);assert.deepEqual(ui.refreshes,[]);
  await ui.el('scReview').onclick();await ui.el('scSave').onclick();assert.equal(ui.calls.length,4);assert.deepEqual(ui.calls.map(call=>call.url),[previewUrl,saveUrl+'/refresh-customers',previewUrl,saveUrl]);assert.equal(ui.calls[3].body.previewFingerprint,'fresh-customer-preview');assert.match(ui.el('scMsg').textContent,/RQ-REFRESHED.*Owner approval/);assert.match(ui.el('scMsg').textContent,/Cash and incentives remain unchanged until approved/);assert.doesNotMatch(ui.el('scMsg').textContent,/App correction saved/);
});

test('failed or uncertain customer refresh preserves entered fields but cannot save using old preview',async()=>{
  for(const uncertain of [false,true]){
    const ui=setup({owner:false,respond:url=>{
      if(url===previewUrl)return{success:true,proposal:{orderNumber:'2903',originalOrderNumber:'2902',storeCreditAmount:2499},previewFingerprint:'old-preview',ownerApprovalRequired:true};
      if(uncertain)throw new Error('Lookup connection lost');return{success:false,error:'Shopify returned different customer identities.'};
    }});
    await ui.preview();const original=ui.el('scOriginal').value,explanation=ui.el('scReason').value;await ui.el('scRefresh').onclick();assert.equal(ui.el('scOriginal').value,original);assert.equal(ui.el('scReason').value,explanation);assert.equal(ui.el('scSave').disabled,true);assert.equal(ui.el('scPreview').textContent,'');
    assert.match(ui.el('scMsg').textContent,uncertain?/refresh could not be confirmed.*Verify its result before retrying/:/different customer identities/);await ui.el('scSave').onclick();assert.equal(ui.calls.length,2);assert.equal(ui.messages.length,0);assert.deepEqual(ui.refreshes,[]);
  }
});

test('in-flight customer refresh blocks duplicate refresh, preview, save and switching sale',async()=>{
  let finish;const ui=setup({owner:false,respond:()=>new Promise(resolve=>{finish=resolve;})});ui.open();ui.fill();const refreshing=ui.el('scRefresh').onclick();assert.equal(ui.calls.length,1);assert.equal(ui.el('scRefresh').disabled,true);assert.equal(ui.el('scOriginal').disabled,true);assert.equal(ui.el('scReason').disabled,true);
  await ui.el('scRefresh').onclick();await ui.el('scReview').onclick();await ui.el('scSave').onclick();ui.context.window.openStoreCreditCorrection('SHOPIFY/other');assert.equal(ui.calls.length,1);assert.match(ui.el('scSummary').textContent,/#2903/);
  finish({success:true,updated:1,customerMatchVerified:true});await refreshing;assert.equal(ui.el('scRefresh').disabled,false);assert.equal(ui.el('scOriginal').disabled,false);assert.equal(ui.el('scReason').disabled,false);assert.equal(ui.el('scSave').disabled,true);assert.equal(ui.messages.length,0);
});
