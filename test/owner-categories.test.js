const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
test('custom category endpoints reject Admin and claimants before reading data',()=>{
  const js=fs.readFileSync(path.join(__dirname,'../modules/expenses.js'),'utf8');
  const start=js.indexOf("router.post('/api/expenses/custom-ledgers',"),end=js.indexOf('// ── Summary for the P&L',start),routes=[];
  vm.runInNewContext(js.slice(start,end),{router:{get:(p,f)=>routes.push(f),post:(p,f)=>routes.push(f)},isOwner:r=>r.role==='owner',loadStore:()=>{throw Error('Denied user must not read data');}});
  for(const handler of routes)for(const role of ['admin','claimant']){let code,result;handler({role},{status:n=>{code=n;return{json:x=>result=x}}});assert.equal(code,403);assert.equal(result.success,false);}
});
test('both private tabs and category rendering require Owner',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
  assert.match(html,/if\(cfg.isOwner\)\{el\('rentalsTab'\).*el\('categoriesTab'\)/);
  assert.match(html,/function renderCategories\(\)\{\s*if\(!cfg.isOwner\)return/);
  assert.doesNotMatch(html,/if\(cfg.isAdmin\)\{el\('categoriesTab'\)/);
});
