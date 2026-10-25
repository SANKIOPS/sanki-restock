'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const view = require('../public/inventory-view-model');
const inventory = require('../modules/inventory-state');
const ui = fs.readFileSync(require.resolve('../public/inventory-enhancements.js'), 'utf8');
const balances = {displayQty:0,warehouseQty:0,otherQty:0,availableQty:0,cleaningQty:0,notForSaleQty:0,otherUnavailableQty:0,committedQty:0,totalQty:0};
const variant = (sku,colour,quantity,extra={}) => ({...balances,sku,colour,availableQty:quantity,warehouseQty:quantity,totalQty:quantity,price:1299,...extra});
function product(handle,variants,extra={}) {
  const p={handle,title:'Everyday trousers',collection:'SANKI Casuals',category:'Trousers',productType:'Straight',variants,...extra};
  for(const key of Object.keys(balances)) p[key]=variants.reduce((n,v)=>n+v[key],0);
  return p;
}
function harness(products,filters={}) {
  const c={products,filters,view,mode:'all',barcodeHandle:'',q:'',fields:{Colour:'colour',Category:'category',Collection:'collection','Product type':'productType'}};
  vm.runInNewContext(ui.slice(ui.indexOf('function inMode('),ui.indexOf('function cards()')),c);
  vm.runInNewContext(ui.slice(ui.indexOf('function allowedValues('),ui.indexOf('function buildFilters(')),c);
  return c;
}
test('colour AND collection AND category filters retain only matching stocked articles and matching SKU quantities',()=>{
  const mixed=product('mixed',[variant('BLACK-M','Black',2,{warehouseQty:1,displayQty:1,totalQty:3,cleaningQty:1}),variant('BLACK-L','Black',0,{totalQty:1,cleaningQty:1}),variant('WHITE-M','White',5),variant('RED-M','Red',0)]);
  const products=[mixed,product('shirt',[variant('BLACK-SHIRT','Black',1)],{category:'Shirts'}),product('funky',[variant('BLACK-FUNKY','Black',1)],{collection:'SANKI Funky'}),product('white',[variant('WHITE','White',1)])];
  const c=harness(products,{Colour:'Black',Collection:'SANKI Casuals',Category:'Trousers'});
  assert.deepEqual(Array.from(c.selected(),p=>p.handle),['mixed']);
  const filtered=view.listProduct(mixed,'all','Black');
  assert.deepEqual(filtered.variants.map(v=>v.sku),['BLACK-M']);
  assert.equal(filtered.availableQty,2);assert.equal(filtered.displayQty,1);assert.equal(filtered.warehouseQty,1);assert.equal(filtered.totalQty,4);assert.equal(filtered.cleaningQty,2);
  assert.equal(filtered.hiddenSkuCount,3);
  assert.equal(mixed.availableQty,7);assert.equal(mixed.totalQty,9);assert.equal(mixed.variants.length,4);
  assert.equal(view.listProduct(mixed,'all').totalQty,9,'unfiltered owned balances keep existing semantics');
});
test('a sold-out black variant cannot appear by default because a white variant is stocked',()=>{
  const mixed=product('white-stock',[variant('BLACK-OUT','Black',0),variant('WHITE-IN','White',3)]);
  const c=harness([mixed],{Colour:'Black'});
  assert.equal(c.selected().length,0);assert.equal(view.includes(mixed,'all','black'),false);
  c.mode='outOfStock';assert.equal(c.selected().length,1);
  assert.deepEqual(view.listProduct(mixed,'outOfStock','Black').variants.map(v=>v.sku),['BLACK-OUT']);
  assert.equal(view.listProduct(mixed,'outOfStock','Black').availableQty,0);
  assert.deepEqual(view.colours(mixed,'all'),['White']);
  assert.deepEqual(view.colours(mixed,'outOfStock'),['Black']);
});
test('colour facets compose with other filters and impossible combinations do not silently broaden results',()=>{
  const products=[product('both',[variant('BLACK','Black',1),variant('WHITE','White',2)]),product('shirt',[variant('BLUE','Blue',1)],{category:'Shirts'}),product('funky',[variant('RED','Red',1)],{collection:'SANKI Funky'})];
  const c=harness(products,{Colour:'Black',Collection:'SANKI Casuals',Category:'Trousers'});
  assert.deepEqual(Array.from(c.allowedValues('Colour')),['Black','White']);
  assert.deepEqual(Array.from(c.allowedValues('Category')),['Trousers']);
  c.filters.Category='Dresses';c.normalizeFilters();assert.equal(c.filters.Category,'Dresses');assert.equal(c.filters.Colour,'Black');assert.equal(c.selected().length,0);
  c.filters.Category='Trousers';c.q='WHITE';assert.equal(c.selected().length,0,'search cannot match the excluded white SKU');
  c.q='black';assert.equal(c.selected().length,1);
  c.barcodeHandle='a-different-product';assert.equal(c.selected().length,0,'barcode product constraint still applies');
});
test('named options are authoritative, aliases normalize and ambiguous or unknown colours stay truthful',()=>{
  assert.equal(view.variantColour({selectedOptions:[{name:' COLOR ',value:'  bLaCk  '}],colour:'White',variant:'White / M'},{}),'Black');
  assert.equal(view.variantColour({colour:' gray '},{}),'Grey');
  assert.equal(view.colourLabel('multi-colored'),'Multicolour');
  assert.equal(view.colourLabel('off-white'),'Off White');
  assert.equal(view.colourLabel('BLACK/WHITE'),'Black / White');
  assert.equal(view.variantColour({variant:'Black / White / M'},{}),'Black / White');
  assert.equal(view.variantColour({colour:'See product',variant:'Black / 34'},{title:'White jeans'}),'Black');
  assert.equal(view.variantColour({variant:'M',colour:'See product'},{title:'Classic Black trousers'}),'Black');
  assert.equal(view.variantColour({variant:'M'},{title:'Black and White shirt'}),'Black / White');
  assert.equal(view.includes(product('combined',[variant('BW','Black / White',1)]),'all','Black'),false,'combined colours are not relabelled solid black');
  assert.equal(view.variantColour({variant:'Universal Size (Fits M-3XL)',colour:'See product'},{title:'Three cards shirt'}),'');
  assert.deepEqual(view.colours(product('unknown',[variant('UNKNOWN','See product',1)]),'all'),[],'placeholders are not offered as colour choices');
});
test('explicit care and location views filter colours while retaining truthful owned totals',()=>{
  const p=product('care',[variant('BLACK','Black',1,{displayQty:1,warehouseQty:0}),variant('BLACK-CLEAN','Black',0,{cleaningQty:2,totalQty:2}),variant('WHITE','White',4)]);
  assert.equal(view.includes(p,'warehouse','Black'),false);assert.equal(view.includes(p,'display','Black'),true);
  assert.deepEqual(view.visibleVariants(p,'cleaning','Black').map(v=>v.sku),['BLACK-CLEAN']);
  assert.equal(view.listProduct(p,'cleaning','Black').totalQty,3);assert.equal(view.listProduct(p,'cleaning','Black').cleaningQty,2);
  assert.equal(view.includes({...p,collection:'SANKI Funky'},'casuals','Black'),false);
});
test('Shopify colour/size options survive shared snapshot reads and override legacy catalogue placeholders without changing stock',async()=>{
  const requests=[];
  const levels={available:2,on_hand:3,committed:1,quality_control:0,damaged:0,reserved:0,safety_stock:0};
  const node={id:'gid://shopify/InventoryItem/colour',sku:'sa-colour',tracked:true,variant:{id:'v',title:'Black / M',price:'1299.00',inventoryPolicy:'DENY',selectedOptions:[{name:'Color',value:' Black '},{name:'Size',value:'M'}],product:{handle:'colour-test',title:'Trousers'}},inventoryLevels:{nodes:[{location:{id:'warehouse',name:'Warehouse'},quantities:Object.entries(levels).map(([name,quantity])=>({name,quantity}))}],pageInfo:{hasNextPage:false,endCursor:null}}};
  const client={store:'test.myshopify.com',request:async(url,options)=>{const request=JSON.parse(options.body);requests.push(request);return {ok:true,json:async()=>({data:request.variables.ids?{nodes:[node]}:{inventoryItems:{nodes:[node],pageInfo:{hasNextPage:false,endCursor:null}}}})};}};
  const snapshot=await inventory.fetchSnapshot(client);
  const targeted=await inventory.snapshotSkus(['SA-COLOUR'],client);const ids=await inventory.snapshotItems([node.id],client);
  assert.equal(requests.length,3);assert.ok(requests.every(r=>r.query.includes('selectedOptions{name value}')));
  assert.ok([snapshot,targeted,ids].every(s=>s.items[0].colour==='Black'&&s.items[0].size==='M'));
  snapshot.mapping={Warehouse:'warehouse'};
  const [catalog]=inventory.catalog(snapshot,[{handle:'colour-test',variants:[{sku:'SA-COLOUR',colour:'See product',size:'See variant'}]}],new Map());
  assert.equal(catalog.variants[0].colour,'Black');assert.equal(catalog.variants[0].size,'M');assert.deepEqual(catalog.variants[0].selectedOptions,[{name:'Color',value:'Black'},{name:'Size',value:'M'}]);
  assert.equal(catalog.availableQty,2);assert.equal(catalog.warehouseQty,2);assert.equal(catalog.totalQty,3);assert.equal(catalog.committedQty,1);
});
test('opening and switching coloured product details uses the same filtered SKUs and balances as the product row',()=>{
  const p=product('mixed-ui',[variant('BLACK-M','Black',1,{price:1199}),variant('WHITE-M','White',4,{price:2199})]);
  const panels=[],row={dataset:{handle:p.handle},label:{},setAttribute(){},querySelector(){return this.label;},insertAdjacentHTML(where,html){panels.push({html,remove(){panels.splice(panels.indexOf(this),1);}});}};
  const host={querySelectorAll:sel=>sel==='.expanded-row'?panels.slice():[row]};
  const c={document:{getElementById:()=>host},products:[p],mode:'all',filters:{Colour:'Black'},expandedHandle:p.handle,view,preserveRow:(_,change)=>change(),expansionRow:p=>JSON.stringify(p),bindExpansion(){}};
  vm.runInNewContext(ui.slice(ui.indexOf('function findProductRow('),ui.indexOf('function bindProductRows(')),c);
  c.updateExpansion(row);let expanded=JSON.parse(panels[0].html);
  assert.equal(row.label.textContent,'BLACK-M · Hide details');assert.deepEqual(expanded.variants.map(v=>v.sku),['BLACK-M']);assert.equal(expanded.warehouseQty,1);assert.equal(expanded.totalQty,1);
  c.filters.Colour='White';c.updateExpansion(row);expanded=JSON.parse(panels[0].html);
  assert.equal(panels.length,1);assert.equal(row.label.textContent,'WHITE-M · Hide details');assert.deepEqual(expanded.variants.map(v=>v.sku),['WHITE-M']);assert.equal(expanded.warehouseQty,4);assert.equal(expanded.totalQty,4);
  const moneyContext={Number,esc:String,thumb:()=>'',expandedHandle:p.handle};
  vm.runInNewContext(ui.slice(ui.indexOf('function money('),ui.indexOf('function expansionRow(')),moneyContext);
  const blackRow=moneyContext.productRow(view.listProduct(p,'all','Black'));
  assert.ok(blackRow.includes('BLACK-M'));assert.ok(blackRow.includes('₹1,199'));assert.ok(!blackRow.includes('₹2,199'));
});
test('image result opens the selected article in the correct availability view and cannot leave stale colour filters applied',()=>{
  const products=[product('stocked',[variant('IN','Black',1)]),product('sold-out',[variant('OUT','Black',0)])],handlers={},search={},rendered=[],loaded=[];
  const c={window:{addEventListener:(name,handler)=>handlers[name]=handler},document:{getElementById:()=>search},products,view,mode:'casuals',filters:{Colour:'White'},page:3,q:'old search',expandedHandle:'',barcodeHandle:'old',render(){rendered.push(true);},loadDetails:p=>loaded.push(p.handle)};
  vm.runInNewContext(ui.slice(ui.indexOf('function cancelProductOpen('),ui.indexOf('var lightbox=')),c);
  const start=ui.indexOf("window.addEventListener('inventory:open-product'");
  vm.runInNewContext(ui.slice(start,ui.indexOf('\n})();',start)),c);
  handlers['inventory:open-product']({detail:{handle:'sold-out'}});
  assert.equal(c.mode,'outOfStock');assert.equal(c.expandedHandle,'sold-out');assert.equal(c.q,'sold-out');assert.equal(c.page,1);assert.equal(Object.keys(c.filters).length,0);assert.equal(search.value,'sold-out');assert.deepEqual(loaded,['sold-out']);
  handlers['inventory:open-product']({detail:{handle:'stocked'}});
  assert.equal(c.mode,'all');assert.equal(c.expandedHandle,'stocked');assert.deepEqual(loaded,['sold-out','stocked']);
});
test('colour filter restores from URL navigation and is retained alongside existing collection/category filters',()=>{
  const urlValues={filter_colour:' black ',filter_collection:'SANKI Casuals',filter_category:'Trousers'},saved=[];
  const c={filters:{},SankiNavigation:{text:(key,pattern,fallback)=>urlValues[key]||fallback,set:context=>saved.push(context)},view,mode:'all',page:2,q:''};
  vm.runInNewContext(ui.slice(ui.indexOf('var fields='),ui.indexOf("document.getElementById('search').value=q;")),c);
  vm.runInNewContext(ui.slice(ui.indexOf('function normalizeFilters('),ui.indexOf('function buildFilters(')),c);
  c.normalizeFilters();assert.equal(c.filters.Colour,'Black');assert.equal(c.filters.Collection,'SANKI Casuals');assert.equal(c.filters.Category,'Trousers');
  const start=ui.indexOf(' var context={view:mode,page:page,search:q};');
  vm.runInNewContext(ui.slice(start,ui.indexOf("\n if(mode==='attention'",start)),c);
  assert.equal(saved[0].filter_colour,'Black');assert.equal(saved[0].filter_collection,'SANKI Casuals');assert.equal(saved[0].filter_category,'Trousers');assert.equal(saved[0].page,2);
});
test('barcode match opens exact article details and preserves an unavailable scanned SKU even when another colour is stocked',()=>{
  const p=product('mixed-barcode',[variant('BLACK-OUT','Black',0),variant('WHITE-IN','White',3)]),products=[p,product('all-out',[variant('ALL-OUT','Red',0)])],handlers={},search={},loaded=[];
  const c={window:{addEventListener:(name,handler)=>handlers[name]=handler},document:{getElementById:()=>search},products,view,mode:'casuals',filters:{Colour:'White'},page:3,q:'old',expandedHandle:'',barcodeHandle:'',normalizeFilters(){},render(){},loadDetails:p=>loaded.push(p.handle)};
  vm.runInNewContext(ui.slice(ui.indexOf('function cancelProductOpen('),ui.indexOf('var lightbox=')),c);
  const start=ui.indexOf("window.addEventListener('inventory:barcode-match'");
  vm.runInNewContext(ui.slice(start,ui.indexOf("window.addEventListener('inventory:open-product'",start)),c);
  handlers['inventory:barcode-match']({detail:{handle:'mixed-barcode',sku:'BLACK-OUT'}});
  assert.equal(c.mode,'outOfStock');assert.equal(c.expandedHandle,'mixed-barcode');assert.equal(c.barcodeHandle,'mixed-barcode');assert.equal(c.q,'black-out');assert.equal(c.page,1);assert.equal(search.value,'BLACK-OUT');assert.equal(Object.keys(c.filters).length,0);assert.deepEqual(loaded,['mixed-barcode']);
  assert.ok(view.visibleVariants(p,c.mode).some(v=>v.sku==='BLACK-OUT'));
  handlers['inventory:barcode-match']({detail:{handle:'mixed-barcode',sku:'WHITE-IN'}});
  assert.equal(c.mode,'all');assert.equal(c.expandedHandle,'mixed-barcode');assert.equal(c.q,'white-in');assert.deepEqual(loaded,['mixed-barcode','mixed-barcode']);
  handlers['inventory:barcode-match']({detail:{handle:'all-out',sku:'ALL-OUT'}});
  assert.equal(c.mode,'outOfStock');assert.equal(c.expandedHandle,'all-out');assert.equal(c.barcodeHandle,'all-out');
});
function productOpenRefreshHarness() {
  const nodes={search:{value:''},stockSource:{textContent:''},refreshStock:{disabled:false},rows:{innerHTML:''}},handlers={},requests=[],loaded=[],timers=[];
  const select={dataset:{label:'Colour'},value:'White'},filterHost={innerHTML:'',querySelectorAll:()=>[select]};nodes.filters=filterHost;
  const c={products:[],view,pendingProductOpen:null,barcodeHandle:'',mode:'all',filters:{Colour:'Black'},fields:{Colour:'colour'},page:1,q:'',expandedHandle:'',stockAt:'2026-10-25T00:00:00.000Z',stockLoading:false,stockQueued:false,stockRequest:0,stockTimer:null,
    document:{getElementById:id=>nodes[id]},window:{addEventListener:(name,handler)=>handlers[name]=handler},render(){},normalizeFilters(){},loadDetails:p=>loaded.push(p.handle),esc:String,allowedValues:()=>['Black','White'],
    setTimeout:fn=>{timers.push(fn);return timers.length;},clearTimeout(){},fetch:url=>new Promise((resolve,reject)=>requests.push({url,resolve:body=>resolve({ok:true,json:async()=>body}),reject}))};
  vm.runInNewContext(ui.slice(ui.indexOf('function cancelProductOpen('),ui.indexOf('var lightbox=')),c);
  vm.runInNewContext(ui.slice(ui.indexOf('function refreshStock('),ui.indexOf("document.getElementById('refreshStock').onclick=")),c);
  const hook=ui.indexOf("window.addEventListener('inventory:open-product'");vm.runInNewContext(ui.slice(hook,ui.indexOf('\n})();',hook)),c);
  const input=ui.indexOf("document.getElementById('search').oninput=");vm.runInNewContext(ui.slice(input,ui.indexOf("document.getElementById('reset').onclick=",input)),c);
  vm.runInNewContext(ui.slice(ui.indexOf('function buildFilters('),ui.indexOf('function thumb(')),c);
  return {c,nodes,handlers,requests,loaded,select};
}
const settleOpen=()=>new Promise(resolve=>setImmediate(resolve));
test('photo result newer than dashboard snapshot refreshes the catalogue and opens its current stock details',async()=>{
  const {c,handlers,requests,loaded,nodes}=productOpenRefreshHarness(),fresh=product('new-photo-match',[variant('NEW','Black',2)]);
  handlers['inventory:open-product']({detail:{handle:fresh.handle}});
  assert.equal(requests.length,1);assert.ok(requests[0].url.endsWith('&refresh=1'));assert.match(nodes.stockSource.textContent,/Refreshing inventory/);
  requests[0].resolve({success:true,at:'2026-10-25T00:01:00.000Z',products:[fresh],refreshing:false});await settleOpen();
  assert.equal(c.expandedHandle,fresh.handle);assert.equal(c.mode,'all');assert.deepEqual(loaded,[fresh.handle]);assert.equal(c.pendingProductOpen,null);assert.equal(c.products[0].totalQty,2);
});
test('photo open waits for a queued forced refresh instead of failing against an earlier in-flight response',async()=>{
  const {c,handlers,requests,loaded}=productOpenRefreshHarness(),fresh=product('new-after-queued-refresh',[variant('NEW','White',0)]);
  c.refreshStock(false);handlers['inventory:open-product']({detail:{handle:fresh.handle}});assert.equal(requests.length,1);
  requests[0].resolve({success:true,at:c.stockAt,products:[],refreshing:false});await settleOpen();
  assert.equal(requests.length,2);assert.ok(requests[1].url.endsWith('&refresh=1'));assert.equal(c.pendingProductOpen.handle,fresh.handle);assert.equal(loaded.length,0);
  requests[1].resolve({success:true,at:'2026-10-25T00:01:00.000Z',products:[fresh],refreshing:false});await settleOpen();
  assert.equal(c.mode,'outOfStock');assert.equal(c.expandedHandle,fresh.handle);assert.deepEqual(loaded,[fresh.handle]);
});
test('a late photo catalogue refresh cannot replace a later user search or colour selection',async()=>{
  const fresh=product('late-photo-match',[variant('NEW','Black',1)]);
  const searchRun=productOpenRefreshHarness();searchRun.handlers['inventory:open-product']({detail:{handle:fresh.handle}});
  searchRun.nodes.search.oninput({target:{value:'my new search'}});
  searchRun.requests[0].resolve({success:true,at:'new',products:[fresh],refreshing:false});await settleOpen();
  assert.equal(searchRun.c.q,'my new search');assert.equal(searchRun.c.expandedHandle,'');assert.deepEqual(searchRun.loaded,[]);
  const filterRun=productOpenRefreshHarness();filterRun.c.buildFilters();filterRun.handlers['inventory:open-product']({detail:{handle:fresh.handle}});filterRun.select.onchange();
  filterRun.requests[0].resolve({success:true,at:'new',products:[fresh],refreshing:false});await settleOpen();
  assert.equal(filterRun.c.filters.Colour,'White');assert.equal(filterRun.c.expandedHandle,'');assert.deepEqual(filterRun.loaded,[]);
});
test('an absent or failed refreshed photo match gives a visible retry instruction without opening stale data',async()=>{
  const missing=productOpenRefreshHarness();missing.handlers['inventory:open-product']({detail:{handle:'deleted-product'}});
  missing.requests[0].resolve({success:true,at:'new',products:[],refreshing:false});await settleOpen();
  assert.match(missing.nodes.stockSource.textContent,/not in the latest inventory catalogue/);assert.match(missing.nodes.stockSource.textContent,/Refresh quantities.*photo result.*retry/);assert.equal(missing.c.pendingProductOpen,null);assert.deepEqual(missing.loaded,[]);
  const failed=productOpenRefreshHarness();failed.handlers['inventory:open-product']({detail:{handle:'unread-product'}});failed.requests[0].reject(new Error('Network offline'));await settleOpen();
  assert.match(failed.nodes.stockSource.textContent,/Network offline/);assert.match(failed.nodes.stockSource.textContent,/Refresh quantities.*retry/);assert.deepEqual(failed.loaded,[]);
});
