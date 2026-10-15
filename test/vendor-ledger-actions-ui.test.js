'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
const esc=value=>String(value==null?'':value).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
const section=(from,to)=>html.slice(html.indexOf(from),html.indexOf(to));
const editCode=section('    var expenseEditorGeneration=','    window.quickEdit =');
const ledgerCode=section('    var vendorPaymentProofRows=','    function renderVendors(){');
const managerCode=section('    function openVendorManager(','    // ── ACCOUNT LEDGERS & ACCOUNT-TO-ACCOUNT TRANSFERS');
const spendingCode=section('    function spendingPaymentCard(','    function loadSpendingDashboard(){');
const flush=()=>new Promise(resolve=>setImmediate(resolve));
function harness({owner=false,admin=true,expense={},response,tab='vendors'}={}){
  const nodes={},calls=[],messages=[],loads=[],confirms=[];
  const node=id=>nodes[id]||(nodes[id]={value:'',textContent:'',innerHTML:'',disabled:false,open:false,files:[],style:{},dataset:{},attrs:{},listeners:{},
    setAttribute(k,v){this.attrs[k]=v;},addEventListener(k,fn){this.listeners[k]=fn;},focus(){},showModal(){this.open=true;},close(){this.open=false;},querySelectorAll(){return[];}});
  const e={id:'EX-00112',nature:'SANKI',status:'paid',date:'2026-08-28',amount:20000,vendor:'UJ',particulars:'Old particulars',ledger:'General',type:'variable',paymentType:'UPI',paidAlready:false,payments:[{id:'PAY-001',amount:20000,account:'Axis',date:'2026-08-28',proof:'/proof-one.jpg'}],...expense};
  const s={window:{},cfg:{isAdmin:admin,isOwner:owner,natures:['SANKI','SAMAST'],types:['fixed','variable'],vendors:['UJ','Final Vendor'],vendorsByNature:{SANKI:['UJ','Final Vendor'],SAMAST:['Another Vendor']}},expensesById:{[e.id]:e},vendorManageState:null,tab,el:node,esc,fmt:n=>'₹'+n,opts:()=>'',accountsForNature:()=>['Axis'],encodeURIComponent,
    setMsg:(...args)=>messages.push(args),loadConfig:()=>Promise.resolve(),loadVendors:()=>loads.push('vendors'),loadSpendingDashboard:()=>loads.push('spending'),loadList:()=>loads.push('list'),
    uploadFiles:()=>{throw Error('No uploads expected');},uploadFile:()=>{throw Error('No uploads expected');},
    api:(url,options={})=>{calls.push({url,...options});return Promise.resolve(response?response(url,options):options.method==='POST'?{success:true,expense:e,updatedExpenses:1}:{success:true,expense:e});},
    setTimeout:fn=>fn(),confirm:message=>{confirms.push(message);return true;},procurementCostHtml:()=>'',vendorBalanceHtml:String,proofGallery:()=>''};
  ['editNature','editPaymentType','editType'].forEach(id=>node(id).value=id==='editNature'?'SANKI':id==='editPaymentType'?'UPI':'variable');
  vm.createContext(s);vm.runInContext(editCode+ledgerCode+managerCode+spendingCode,s);
  return {s,nodes,calls,messages,loads,confirms,e,node};
}

test('expense and payment references are separate keyboard-accessible ledger actions; synthetic sources stay read-only',()=>{
  const h=harness();h.node('vf_from').value='';
  const rows=[{entryId:h.e.id,expenseId:h.e.id,editableExpense:true,kind:'expense',reference:h.e.id,particulars:'Expense <name>',out:20000},{entryId:h.e.id,expenseId:h.e.id,paymentId:'PAY-001',kind:'payment',reference:'BANK-UTR',particulars:'Payment',in:20000},{entryId:'CCEXP-1',editableExpense:false,kind:'expense',reference:'CCEXP-1',particulars:'Statement-backed'}];
  const output=h.s.vendorLedgerTable({ledgerRows:rows});
  assert.match(output,/button type="button" class="ledger-action" data-expense-edit="EX-00112"/);
  assert.match(output,/data-vendor-payment-proof="1"/);assert.match(output,/Expense &lt;name&gt;/);
  assert.doesNotMatch(output,/data-expense-edit="CCEXP-1"/);assert.equal(h.s.vendorPaymentProofRows['1'].paymentId,'PAY-001');
});

test('fresh expense details are fetched before opening and metadata permissions are enforced',async()=>{
  const h=harness();await h.s.window.editExpense(h.e.id);
  assert.equal(h.calls[0].url,'/api/expenses/EX-00112');assert.equal(h.calls[0].cache,'no-store');assert.equal(h.nodes.editDlg.open,true);
  const denied=harness({expense:{canEditMetadata:false}});await denied.s.window.editExpense(denied.e.id);assert.notEqual(denied.node('editDlg').open,true);assert.match(denied.messages.at(-1)[0],/permission/);
  const viewer=harness({admin:false});await viewer.s.window.editExpense(viewer.e.id);assert.equal(viewer.calls.length,0);
});

test('late expense requests cannot reopen an editor after switching surface or cancellation',async()=>{
  for(const action of ['tab','surface','cancel','escape','close']){
    const h=harness();h.node('panel').firstElementChild={};let finish;h.s.api=()=>new Promise(resolve=>{finish=resolve;});
    const pending=h.s.window.editExpense(h.e.id);
    if(action==='tab')h.s.tab='spending';else if(action==='surface')h.nodes.panel.firstElementChild={};else if(action==='cancel')h.node('editCancel').onclick();else if(action==='escape')h.node('editDlg').listeners.cancel({preventDefault(){}});else h.node('editDlg').listeners.close();
    finish({success:true,expense:h.e});await pending;assert.notEqual(h.node('editDlg').open,true,action);
  }
});

test('all roles including Owner see reconciled financial fields locked but metadata available',async()=>{
  for(const owner of [false,true]){
    const h=harness({owner,expense:{financialFieldsLocked:true,financialLockReason:'Payment reconciled'}});await h.s.window.editExpense(h.e.id);
    for(const id of ['editNature','editDate','editAmount','editTotal','editRequested','editPaidAlready','editPaymentType','editPayingAccount','editPersonalAccount','editInstallment','editBillFile','editQrFile','editPersonalFile'])assert.equal(h.node(id).disabled,true,id);
    for(const id of ['editVendor','editParticulars','editLedger','editType'])assert.equal(h.node(id).disabled,false,id);
    assert.match(h.nodes.editMsg.textContent,/reconciled/);
  }
});

test('locked saves submit only metadata even if disabled financial inputs are tampered with; refresh preserves current surface',async()=>{
  for(const tab of ['vendors','spending','all']){
    const h=harness({owner:true,tab,expense:{financialFieldsLocked:true}});await h.s.window.editExpense(h.e.id);
    h.nodes.editDate.value='2099-01-01';h.nodes.editAmount.value='999';h.nodes.editPaidAlready.value='true';h.nodes.editVendor.value='Correct Vendor';h.nodes.editParticulars.value='Correct particulars';h.nodes.editLedger.value='Flowers';h.nodes.editReason.value='Vendor and category correction';
    h.nodes.editGo.onclick();h.nodes.editGo.onclick();await flush();
    const posts=h.calls.filter(x=>x.method==='POST');assert.equal(posts.length,1);assert.deepEqual(JSON.parse(posts[0].body),{vendor:'Correct Vendor',particulars:'Correct particulars',ledger:'Flowers',type:'variable',editReason:'Vendor and category correction'});
    assert.equal(h.nodes.editDlg.open,false);assert.deepEqual(h.loads,[tab==='all'?'list':tab]);
  }
});

test('unreconciled paid-personal descriptive changes send only sparse metadata, not hidden default accounts or reimbursement fields',async()=>{
  const h=harness({expense:{financialFieldsLocked:false,paidAlready:true,personalPaidAmount:20000,reimbursementStatus:'reimbursed',reimbursementAmount:20000,payments:[{id:'PAY-001',personalFunds:true,account:'Personal Axis',amount:20000,proof:'/personal.jpg'}]}});
  await h.s.window.editExpense(h.e.id);h.nodes.editParticulars.value='Corrected description';h.nodes.editVendor.value='Correct Vendor';h.nodes.editReason.value='Descriptive correction only';
  h.nodes.editGo.onclick();await flush();const post=h.calls.find(x=>x.method==='POST');assert.deepEqual(JSON.parse(post.body),{particulars:'Corrected description',vendor:'Correct Vendor',ledger:'General',type:'variable',editReason:'Descriptive correction only'});
  assert.equal(h.e.reimbursementStatus,'reimbursed');assert.equal(h.e.reimbursementAmount,20000);assert.equal(h.nodes.editDlg.open,false);
});

test('an actual unlocked financial correction still uses the existing full financial save workflow',async()=>{
  const h=harness({owner:true,expense:{financialFieldsLocked:false}});await h.s.window.editExpense(h.e.id);h.nodes.editDate.value='2026-08-29';h.nodes.editReason.value='Correct unreconciled expense date';
  h.nodes.editGo.onclick();await flush();const payload=JSON.parse(h.calls.find(x=>x.method==='POST').body);assert.equal(payload.date,'2026-08-29');assert.equal(payload.amount,20000);assert.equal(payload.paidAlready,false);assert.equal(payload.nature,'SANKI');
});

test('selecting replacement proof prevents metadata-only optimization and preserves file uploads',async()=>{
  const h=harness({owner:true,expense:{financialFieldsLocked:false}}),uploads=[];await h.s.window.editExpense(h.e.id);
  h.nodes.editBillFile.files=[{name:'corrected-bill.jpg'}];h.s.uploadFiles=(files,nature)=>{uploads.push({files,nature});return Promise.resolve(['/replacement-bill.jpg']);};h.nodes.editReason.value='Attach corrected bill proof';
  h.nodes.editGo.onclick();await flush();const payload=JSON.parse(h.calls.find(x=>x.method==='POST').body);assert.equal(uploads.length,1);assert.equal(uploads[0].nature,'SANKI');assert.equal(payload.billPhoto,'/replacement-bill.jpg');assert.deepEqual(payload.billPhotos,['/replacement-bill.jpg']);assert.equal(payload.date,'2026-08-28');
});

test('failed metadata corrections remain open with an error and can be retried',async()=>{
  const h=harness({expense:{financialFieldsLocked:true},response:(url,options)=>options.method==='POST'?{success:false,error:'Payment became reconciled; reload'}:{success:true,expense:{id:'EX-00112',status:'paid',amount:10,financialFieldsLocked:true}}});
  await h.s.window.editExpense(h.e.id);h.node('editReason').value='Correction reason';h.nodes.editGo.onclick();await flush();assert.equal(h.nodes.editDlg.open,true);assert.match(h.nodes.editMsg.textContent,/reload/);assert.equal(h.nodes.editGo.disabled,false);assert.equal(h.loads.length,0);
});

test('payment proof is fetched for the exact payment ID, never an unrelated payment or latest expense proof',async()=>{
  const h=harness({expense:{payments:[{id:'PAY-001',proof:'/first.jpg'},{id:'PAY-002',proofs:['/second.jpg','/second-extra.pdf']}]}});
  h.s.vendorPaymentProofRows.key={expenseId:h.e.id,paymentId:'PAY-002',reference:'BANK-UTR',date:'2026-08-28',in:5};await h.s.window.openVendorPaymentProof('key');
  assert.match(h.nodes.vendorProofContent.innerHTML,/second\.jpg/);assert.match(h.nodes.vendorProofContent.innerHTML,/second-extra\.pdf/);assert.doesNotMatch(h.nodes.vendorProofContent.innerHTML,/first\.jpg/);assert.match(h.nodes.vendorProofContent.innerHTML,/rel="noopener noreferrer"/);
  h.s.vendorPaymentProofRows.key.paymentId='MISSING';await h.s.window.openVendorPaymentProof('key');assert.match(h.nodes.vendorProofContent.textContent,/changed or removed/);
});

test('advance proofs need no expense lookup; empty and unsafe proof URLs are handled explicitly',async()=>{
  const h=harness();h.s.vendorPaymentProofRows.key={kind:'advance',reference:'ADV-1',in:5,proofs:['/advance.jpg','javascript:alert(1)','//untrusted.invalid/file']};await h.s.window.openVendorPaymentProof('key');
  assert.equal(h.calls.length,0);assert.match(h.nodes.vendorProofContent.innerHTML,/advance\.jpg/);assert.doesNotMatch(h.nodes.vendorProofContent.innerHTML,/javascript:|untrusted/);
  h.s.vendorPaymentProofRows.key.proofs=[];await h.s.window.openVendorPaymentProof('key');assert.match(h.nodes.vendorProofContent.innerHTML,/No payment proof/);
});

test('legacy payments without a stable ID use only the exact ledger row proof',async()=>{
  const h=harness();h.s.vendorPaymentProofRows.key={expenseId:h.e.id,paymentId:'',reference:'Legacy payment',in:5,proof:'/legacy-exact.jpg'};
  await h.s.window.openVendorPaymentProof('key');assert.equal(h.calls.length,0);assert.match(h.nodes.vendorProofContent.innerHTML,/legacy-exact/);assert.doesNotMatch(h.nodes.vendorProofContent.innerHTML,/proof-one/);
});

test('Owner sees rename and merge in All-entity view; Admin sees permission explanation without expanded powers',()=>{
  const owner=harness({owner:true}),v={name:'UJ',entities:['SANKI','SAMAST'],count:1};
  const actions=owner.s.vendorManagementActions(v,'','expense');assert.match(actions,/Rename vendor/);assert.match(actions,/Merge vendor/);assert.doesNotMatch(actions,/ disabled/);assert.match(actions,/data-vendor-nature="SANKI"/);
  const admin=harness().s.vendorManagementActions(v,'','expense');assert.match(admin,/disabled aria-disabled="true"/);assert.match(admin,/Owner only/);
  assert.equal(owner.s.vendorManagementActions(v,'','sourcing'),'');assert.equal(harness({admin:false}).s.vendorManagementActions(v,'','expense'),'');
});

test('merge requires a reason and explicit confirmation, retains chosen final name and audited reason',async()=>{
  const h=harness({owner:true});h.s.window.mergeVendorLedger('SANKI','UJ');assert.equal(h.nodes.vendorManageDlg.open,true);assert.equal(h.nodes.vendorMergeWrap.style.display,'');assert.match(h.nodes.vendorMergeTargets.innerHTML,/Final Vendor/);
  await h.nodes.vendorManageGo.listeners.click.call(h.nodes.vendorManageGo);assert.equal(h.calls.length,0);assert.match(h.nodes.vendorManageMsg.textContent,/reason/);
  h.nodes.vendorChangeReason.value='Duplicate vendor identity';h.nodes.vendorMergeTarget.value='Final Vendor';await h.nodes.vendorManageGo.listeners.click.call(h.nodes.vendorManageGo);await flush();
  assert.deepEqual(JSON.parse(h.calls[0].body),{nature:'SANKI',sourceName:'UJ',targetName:'Final Vendor',reason:'Duplicate vendor identity'});assert.match(h.confirms[0],/across every entity/);assert.equal(h.nodes.vendorManageDlg.open,false);
});

test('spending dashboard exposes the same expense editor for ordinary payments, not derived bank expenses',()=>{
  const h=harness();assert.match(h.s.spendingPaymentCard({id:'EX-1',kind:'Vendor payment'},1),/data-expense-edit="EX-1"/);
  assert.doesNotMatch(h.s.spendingPaymentCard({id:'RC-1',kind:'Bank-reconciled expense'},1),/data-expense-edit/);
});

test('all inline scripts parse after ledger action changes',()=>{
  for(const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi))assert.doesNotThrow(()=>new vm.Script(script[1]));
});
