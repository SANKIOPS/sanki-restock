const {test}=require('node:test'),assert=require('node:assert/strict'),{stage,finalizeStagedAudit}=require('../modules/gokwik-api-accounting');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),{createRequire}=require('node:module');
const order={id:'123456',name:'#100',orderNumber:'100',total:150,channel:'Website',paymentTransactions:[]};
const row={type:'Payment',paymentId:'P',orderId:'123456',orderNumber:'#100',providerOrderId:'GK1',date:'2026-08-25',transactionDate:'2026-08-24T10:00:00Z',utr:'UTR12345678',provider:'easebuzz',gross:150,fees:4.43,fee:3.75,tax:.68,credit:145.57,debit:0,deductions:{gokwik:75},reportedGokwikDeduction:75,net:70.57};
const clone=value=>JSON.parse(JSON.stringify(value)),pending='_gokwikApiPendingAudit';
function clockedApi(at){
 let current=Date.parse(at);
 class Clock extends Date {constructor(...args){super(...(args.length?args:[current]));}static now(){return current;}}
 const file=path.join(__dirname,'../modules/gokwik-api-accounting.js'),context={module:{exports:{}},require:createRequire(file),Buffer,Date:Clock};
 vm.runInNewContext(fs.readFileSync(file,'utf8'),context,{filename:file});
 return {...context.module.exports,setTime:at=>{current=Date.parse(at);}};
}
function candidates(store){return [...Object.values(store.gokwikImports||{}),...Object.values(store.gokwikOrderLinks||{})];}
function withoutAudit(store){const copy=clone(store);for(const entry of candidates(copy)){delete entry.at;delete entry[pending];}return copy;}
function actualSaveHarness(api,persisted){
 const expenses=fs.readFileSync(path.join(__dirname,'../modules/expenses.js'),'utf8'),start=expenses.indexOf('function saveStore('),end=expenses.indexOf('function audit(',start);
 assert.ok(start>=0&&end>start,'extract the actual production saveStore without initializing the application');
 const dataDir=path.join('synthetic-gokwik-save','data'),file=path.join(dataDir,'expenses.json');
 const files=new Map([[file,JSON.stringify(persisted)]]),events=[],writes=[],commits=[];
 const context=vm.createContext({
  path,Date,process:{pid:12345},EXP_PATH:file,DATA_DIR:dataDir,
  sankiCategories:{attachGroups:()=>events.push('categories')},
  require(name){assert.equal(name,'./gokwik-api-accounting');return {finalizeStagedAudit(store){events.push('finalize');return api.finalizeStagedAudit(store);}};},
  fs:{
   readFileSync(name){events.push('read');assert.equal(name,file);return files.get(name);},
   existsSync:name=>files.has(name),
   readdirSync(directory){events.push('list');assert.equal(directory,dataDir);return [];},
   writeFileSync(name,text){events.push('write');assert.ok(name.startsWith(file+'.tmp-'));writes.push(text);files.set(name,text);},
   renameSync(from,to){events.push('rename');assert.equal(to,file);assert.ok(files.has(from));files.set(to,files.get(from));files.delete(from);},
   unlinkSync(name){events.push('unlink');assert.ok(name.startsWith(file+'.tmp-'));files.delete(name);}
  },
  bankAuthorityTransaction:{commit(directory,entries){events.push('commit');assert.equal(directory,dataDir);commits.push(clone(entries));}}
 });
 vm.runInContext(expenses.slice(start,end),context);
 return {save:store=>context.saveStore(store),events,writes,commits};
}
function stagedSaveFixture(api){
 const store={
  expenses:{KEEP:{amount:490,payments:[{id:'PAY-001',amount:490}]}},openingBalances:{'Synthetic Bank':0.01},
  transfers:[{id:'KEEP-EXCLUDED',amount:2000,accountingExcluded:true,accountingExclusion:{by:'Owner',reason:'Preserve prior decision'}}],
  bankStatements:{KEEP:{imports:[{id:'FINALIZED-KEEP',finalizedAt:'2026-09-13T00:00:00Z',reconciliationRows:[{decision:'Excluded',ledger:{id:'KEEP-EXCLUDED',credit:2000}}]}]}},
  bankDateOverrides:{KEEP:{reconciliationDraft:'D-KEEP',originalDate:'2026-09-02',bankDate:'2026-09-03'}},
  auditLog:[{action:'LEDGER_ENTRY_EXCLUDED',subjectId:'KEEP-EXCLUDED'}]
 };
 const oldRow={...row,paymentId:'OLD',orderId:'654321',orderNumber:'#200',providerOrderId:'GK-OLD',utr:'UTR-OLD-KEEP'};
 api.stage(store,[oldRow],[{...order,id:'654321',name:'#200',orderNumber:'200'}]);
 store.gokwikImports.MANUAL={id:'MANUAL',sourceType:'csv',at:null,by:'Owner',source:'retained original'};
 store.gokwikOrderLinks.MANUAL={orderId:'OTHER',at:'2026-09-01T00:00:00Z',by:'Owner',reason:'Keep manual decision'};
 const persisted=clone(store);api.setTime('2026-10-23T10:00:00.000Z');api.stage(store,[row],[order],{readOnly:true});
 return {store,persisted};
}
function actualReportsHarness(initial,api){
 const file=path.join(__dirname,'../modules/gokwik-reports-routes.js'),localRequire=createRequire(file),context={module:{exports:{}},Buffer,require:name=>name==='./gokwik-api-accounting'?api:localRequire(name)};
 vm.runInNewContext(fs.readFileSync(file,'utf8'),context,{filename:file});
 let stored=clone(initial);const routes=new Map(),saved=[],events=[];
 const router={get(){},post(route,...handlers){routes.set(route,handlers.at(-1));}};
 context.module.exports.registerGoKwikReports(router,{
  loadStore:()=>clone(stored),
  saveStore(store){api.finalizeStagedAudit(store);const text=JSON.stringify(store);saved.push(text);stored=JSON.parse(text);},
  audit(store,req,action,kind,id,details){events.push(action);store.auditLog||=[];store.auditLog.push({action,kind,id,...details});},
  upload:{single:()=>()=>{}},orders:()=>[order,{...order,id:'654321',name:'#200',orderNumber:'200'}],
  isOwner:()=>true,canAccess:()=>true,registerVendor:()=> 'GoKwik'
 });
 return {saved,events,state:()=>clone(stored),invoke(body){
  const result={status:200},response={status(code){result.status=code;return this;},json(value){result.body=value;return this;}};
  routes.get('/api/expenses/gokwik-reports/unmatch')({body,user:{username:'synthetic-owner',role:'owner'}},response);return result;
 }};
}

test('API stages and links exact evidence without posting money; repeated reads are idempotent',()=>{const s={};let r=stage(s,[row],[order]);assert.equal(r.added,1);assert.equal(r.linked,1);assert.equal(s.gokwikOrderLinks.P.orderId,'123456');assert.equal(s.gokwikTransactions.P.net,70.57);assert.equal(s.transfers,undefined);assert.equal(s.expenses,undefined);r=stage(s,[row],[order]);assert.equal(r.added,0);assert.equal(Object.keys(s.gokwikTransactions).length,1);});
test('refund and unclassified split cannot partially stage a batch',()=>{for(const options of [{},{readOnly:true}])for(const extra of [{...row,type:'Refund',paymentId:'R'},{...row,paymentId:'P2',deductions:{other:5}}]){const s={};const r=stage(s,[row,extra],[order],options);assert.equal(r.added,0);assert.equal(Object.keys(s.gokwikTransactions).length,0);assert.ok(r.issues.length);assert.equal(finalizeStagedAudit(s).stamped,0);}});
test('changed API evidence cannot overwrite saved or posted accounting',()=>{for(const options of [{},{readOnly:true}]){const old={paymentId:'P',orderId:'123456',orderNumber:'100',utr:row.utr,settledDate:row.date,gross:150,net:71,deduction:75};const s={gokwikTransactions:{P:old},gokwikPostings:{B:{utr:row.utr}}},before=clone(old);stage(s,[row],[order],options);assert.deepEqual(s.gokwikTransactions.P,before);assert.equal(s.gokwikOrderLinks.P,undefined);assert.equal(finalizeStagedAudit(s).stamped,0);}});

test('independent read-only projections at different clocks have identical evidence and no invented audit time',()=>{
 const api=clockedApi('2026-10-23T10:00:00.000Z'),first={},second={};
 api.stage(first,[row],[order],{readOnly:true});
 api.setTime('2026-10-24T11:12:13.456Z');api.stage(second,[row],[order],{readOnly:true});
 assert.equal(JSON.stringify(first),JSON.stringify(second));
 assert.equal(candidates(first).length,2);
 for(const record of candidates(first)){assert.equal(Object.hasOwn(record,'at'),false);assert.ok(record[pending]);}
 assert.equal(first.gokwikTransactions.P.net,70.57);assert.equal(first.gokwikOrderLinks.P.orderId,order.id);
 assert.equal(first.transfers,undefined);assert.equal(first.expenses,undefined);
});

test('save boundary stamps newly staged evidence with actual save time exactly once without changing financial evidence',()=>{
 const api=clockedApi('2026-10-23T10:00:00.000Z'),s={};api.stage(s,[row],[order],{readOnly:true});
 const before=withoutAudit(s),saveAt='2026-10-24T11:12:13.456Z';api.setTime(saveAt);
 const result=api.finalizeStagedAudit(s);assert.equal(result.stamped,2);assert.equal(result.cleared,2);
 for(const record of candidates(s)){assert.equal(record.at,saveAt);assert.equal(Object.hasOwn(record,pending),false);}
 assert.deepEqual(withoutAudit(s),before);
 const persisted=clone(s),json=JSON.stringify(persisted);api.setTime('2026-10-25T00:00:00.000Z');
 assert.equal(api.finalizeStagedAudit(persisted).stamped,0);assert.equal(JSON.stringify(persisted),json);
});

test('normal staging retains real-time audit creation and read-only staging preserves saved timestamps',()=>{
 const createdAt='2026-10-23T10:00:00.000Z',api=clockedApi(createdAt),s={};api.stage(s,[row],[order]);
 for(const record of candidates(s)){assert.equal(record.at,createdAt);assert.equal(Object.hasOwn(record,pending),false);}
 const originals=clone(candidates(s));api.setTime('2026-10-24T11:12:13.456Z');
 api.stage(s,[row],[order],{readOnly:true});assert.deepEqual(clone(candidates(s)),originals);
 assert.equal(api.finalizeStagedAudit(s).stamped,0);assert.deepEqual(clone(candidates(s)),originals);
});

test('unmarked legacy and manual evidence is never backfilled or rewritten, even when audit time is absent',()=>{
 const api=clockedApi('2026-10-23T10:00:00.000Z'),s={};api.stage(s,[row],[order]);
 for(const record of candidates(s))delete record.at;
 s.gokwikImports.MANUAL={id:'MANUAL',sourceType:'csv',at:null,by:'Owner',source:'retained original'};
 s.gokwikOrderLinks.MANUAL={orderId:'OTHER',at:'2026-09-01T00:00:00Z',by:'Owner',reason:'Saved manual decision'};
 const before=clone(candidates(s));api.stage(s,[row],[order],{readOnly:true});
 assert.equal(api.finalizeStagedAudit(s).stamped,0);assert.deepEqual(clone(candidates(s)),before);
});

test('existing timestamps on valid marked candidates are preserved rather than replaced',()=>{
 const api=clockedApi('2026-10-23T10:00:00.000Z'),s={};api.stage(s,[row],[order],{readOnly:true});
 const imported=Object.values(s.gokwikImports)[0];imported.at='2026-10-22T09:08:07.654Z';
 s.gokwikOrderLinks.P.at=null;const before=withoutAudit(s);
 const result=api.finalizeStagedAudit(s);assert.equal(result.stamped,0);assert.equal(result.cleared,2);
 assert.equal(imported.at,'2026-10-22T09:08:07.654Z');assert.equal(s.gokwikOrderLinks.P.at,null);
 assert.deepEqual(withoutAudit(s),before);
});

test('malformed or non-API pending audit markers fail before any candidate is modified',()=>{
 for(const tamper of [
  s=>{Object.values(s.gokwikImports)[0].sourceType='csv';},
  s=>{Object.values(s.gokwikImports)[0][pending]='unknown';},
  s=>{s.gokwikOrderLinks.P.by='Owner';},
  s=>{s.gokwikOrderLinks.P[pending]='unknown';},
  s=>{delete s.gokwikTransactions.P;},
  s=>{s.gokwikOrderLinks.P.orderId='DIFFERENT';}
 ]){
  const api=clockedApi('2026-10-23T10:00:00.000Z'),s={};api.stage(s,[row],[order],{readOnly:true});tamper(s);
  const before=JSON.stringify(s);assert.throws(()=>api.finalizeStagedAudit(s),/Invalid pending GoKwik API/);
  assert.equal(JSON.stringify(s),before,'validation must not partially timestamp or clear a candidate');
 }
});

test('read-only staging cannot add to a posted batch or reroute a bank-linked order',()=>{
 for(const options of [{},{readOnly:true}]){
  const posted={gokwikPostings:{B:{utr:row.utr,at:'2026-09-01T00:00:00Z'}}},before=clone(posted.gokwikPostings);
  const rejected=stage(posted,[row],[order],options);assert.equal(rejected.added,0);assert.ok(rejected.issues.some(issue=>/posted settlement/.test(issue.message)));
  assert.equal(finalizeStagedAudit(posted).stamped,0);assert.deepEqual(posted.gokwikPostings,before);
  const locked={bankDateOverrides:{'SHOPIFY/123456':{bankDate:'2026-09-01',reason:'Preserve finalized bank routing'}}},overrides=clone(locked.bankDateOverrides);
  const staged=stage(locked,[row],[order],options);assert.equal(staged.linked,0);assert.equal(locked.gokwikOrderLinks.P,undefined);
  assert.ok(staged.issues.some(issue=>/bank-linked/.test(issue.message)));assert.deepEqual(locked.bankDateOverrides,overrides);
 }
});

for(const external of [false,true])test('production saveStore finalizes only pending API audit before '+(external?'linked-store atomic commit':'temporary-file atomic rename'),()=>{
 const api=clockedApi('2026-10-22T09:08:07.654Z'),{store,persisted}=stagedSaveFixture(api);
 if(external)store._bankAuthorityExternal=[{store:'creditCards',before:'{"keep":"original"}',after:'{"keep":"original"}'}];
 const expected=clone(store),saveAt='2026-10-24T11:12:13.456Z';
 for(const record of candidates(expected))if(Object.hasOwn(record,pending)){record.at=saveAt;delete record[pending];}
 api.setTime(saveAt);const h=actualSaveHarness(api,persisted);h.save(store);
 const persistenceEvent=external?'commit':'write';assert.equal(h.events.filter(event=>event==='finalize').length,1);
 assert.ok(h.events.indexOf('finalize')<h.events.indexOf(persistenceEvent),'actual helper runs before serialization and persistence');
 assert.equal(h.writes.length,external?0:1);assert.equal(h.commits.length,external?1:0);
 const saved=JSON.parse(external?h.commits[0][0].after:h.writes[0]);
 assert.deepEqual(saved,expected,'only pending API audit metadata changes; saved financial evidence and prior timestamps remain exact');
 assert.equal(saved.expenses.KEEP.payments[0].amount,490);assert.equal(saved.transfers[0].accountingExcluded,true);
 if(external){
  assert.equal(h.commits[0][0].before,JSON.stringify(persisted));
  assert.deepEqual(h.commits[0][1],{fileName:'credit-cards.json',before:'{"keep":"original"}',after:'{"keep":"original"}'});
  delete expected._bankAuthorityExternal;
 }else assert.ok(h.events.indexOf('write')<h.events.indexOf('rename'));
 assert.deepEqual(clone(store),expected);
});

for(const external of [false,true])test('production saveStore rejects malformed pending audit before '+(external?'linked-store commit':'filesystem write'),()=>{
 const api=clockedApi('2026-10-22T09:08:07.654Z'),{store,persisted}=stagedSaveFixture(api);
 if(external)store._bankAuthorityExternal=[{store:'creditCards',before:'{"keep":true}',after:'{"keep":true}'}];
 store.gokwikOrderLinks.P[pending]='not-a-valid-automatic-audit';const before=JSON.stringify(store),h=actualSaveHarness(api,persisted);
 assert.throws(()=>h.save(store),/Invalid pending GoKwik API/);
 assert.deepEqual(h.events,['categories','finalize'],'no store read, cleanup, write, rename or external commit follows a failed audit guard');
 assert.equal(h.writes.length,0);assert.equal(h.commits.length,0);assert.equal(JSON.stringify(store),before);
});

test('actual unmatch route stamps fresh API evidence before detaching its audit snapshot without changing money or old audit',()=>{
 const api=clockedApi('2026-10-22T09:08:07.654Z'),{store}=stagedSaveFixture(api),saveAt='2026-10-24T11:12:13.456Z';
 store.auditLog.push({action:'GOKWIK_ORDER_UNMATCHED',id:'HISTORICAL',before:clone(store.gokwikOrderLinks.MANUAL),note:'Historical Owner review; preserve exactly'});
 const before=clone(store);
 const batch=require('../modules/gokwik-accounting').batches(Object.values(store.gokwikTransactions)).find(batch=>batch.utr===row.utr);
 store.gokwikApprovals={[batch.id]:{fingerprint:'pending-only',requestedBy:'synthetic-reviewer'},KEEP:{fingerprint:'unrelated'}};
 api.setTime(saveAt);const h=actualReportsHarness(store,api),result=h.invoke({paymentId:'P',reason:'Review exact order funding again'});
 assert.equal(result.status,200,JSON.stringify(result.body));assert.equal(h.saved.length,1);
 assert.equal(h.saved[0].includes(pending),false,'no internal pending marker can survive anywhere in saved store or audit history');
 const saved=h.state(),audit=saved.auditLog.at(-1);
 assert.equal(audit.action,'GOKWIK_ORDER_UNMATCHED');assert.equal(audit.before.at,saveAt);assert.equal(audit.before.orderId,order.id);
 const expectedLink=clone(before.gokwikOrderLinks.P);delete expectedLink[pending];expectedLink.at=saveAt;
 assert.deepEqual(audit.before,expectedLink,'the detached audit snapshot retains every original link field plus the actual audit time');
 assert.equal(saved.gokwikOrderLinks.P,undefined);assert.equal(saved.gokwikApprovals[batch.id],undefined);
 assert.deepEqual(saved.gokwikApprovals.KEEP,{fingerprint:'unrelated'});
 assert.deepEqual(saved.auditLog.slice(0,-1),before.auditLog);assert.deepEqual(saved.gokwikOrderLinks.OLD,before.gokwikOrderLinks.OLD);
 assert.deepEqual(saved.gokwikOrderLinks.MANUAL,before.gokwikOrderLinks.MANUAL);assert.deepEqual(saved.gokwikImports.MANUAL,before.gokwikImports.MANUAL);
 for(const key of ['expenses','openingBalances','transfers','bankStatements','bankDateOverrides','gokwikTransactions'])assert.deepEqual(saved[key],before[key],key+' must be preserved');
 for(const [id,original] of Object.entries(before.gokwikImports)){const expected=clone(original);if(Object.hasOwn(expected,pending)){delete expected[pending];expected.at=saveAt;}assert.deepEqual(saved.gokwikImports[id],expected);}
});

test('actual unmatch route preserves an existing manual link audit time instead of backdating or restamping its snapshot',()=>{
 const api=clockedApi('2026-10-22T09:08:07.654Z'),{store}=stagedSaveFixture(api),link=store.gokwikOrderLinks.P;
 delete link[pending];link.at='2026-09-15T01:02:03.456Z';link.by='Owner';link.reason='Original manually reviewed funding';
 const original=clone(link),money=clone(store.expenses),history=clone(store.auditLog);api.setTime('2026-10-24T11:12:13.456Z');
 const h=actualReportsHarness(store,api),result=h.invoke({paymentId:'P',reason:'Review exact order funding again'});
 assert.equal(result.status,200);assert.equal(h.saved.length,1);assert.equal(h.saved[0].includes(pending),false);
 assert.deepEqual(h.state().auditLog.at(-1).before,original);assert.deepEqual(h.state().auditLog.slice(0,-1),history);
 assert.deepEqual(h.state().expenses,money);assert.equal(h.state().gokwikOrderLinks.P,undefined);
});

for(const kind of ['invalid reason','posted settlement','malformed audit'])test('actual unmatch route rejects '+kind+' without writes or audit changes',()=>{
 const api=clockedApi('2026-10-22T09:08:07.654Z'),{store}=stagedSaveFixture(api);
 if(kind==='posted settlement')store.gokwikPostings={KEEP:{utr:row.utr,at:'2026-09-01T00:00:00Z'}};
 if(kind==='malformed audit')store.gokwikOrderLinks.P[pending]='invalid';
 const before=JSON.stringify(store),h=actualReportsHarness(store,api),result=h.invoke({paymentId:'P',reason:kind==='invalid reason'?'short':'Review exact order funding again'});
 assert.equal(result.status,409);assert.equal(h.saved.length,0);assert.equal(h.events.length,0);assert.equal(JSON.stringify(h.state()),before);
 assert.match(result.body.error,kind==='invalid reason'?/at least 10/:kind==='posted settlement'?/posted/:/Invalid pending GoKwik API/);
});
