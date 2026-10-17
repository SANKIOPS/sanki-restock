const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
test('custom category endpoints allow Prashant but reject other non-owners before reading data',()=>{
  const js=fs.readFileSync(path.join(__dirname,'../modules/expenses.js'),'utf8');
  const start=js.indexOf("router.post('/api/expenses/custom-ledgers',"),end=js.indexOf('// ── Summary for the P&L',start),routes=[];
  vm.runInNewContext(js.slice(start,end),{router:{get:(p,f)=>routes.push(f),post:(p,f)=>routes.push(f)},isOwner:r=>r.role==='owner',canManageCategoriesAndVendors:r=>r.role==='owner'||r.username==='prashant',loadStore:()=>{throw Error('Denied user must not read data');}});
  for(const handler of routes)for(const role of ['admin','claimant']){let code,result;handler({role,username:role==='admin'?'someone-else':'arshpreet'},{status:n=>{code=n;return{json:x=>result=x}}});assert.equal(code,403);assert.equal(result.success,false);}
});
test('category tab and rendering use the explicit category-management capability',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
  assert.match(html,/if\(cfg.canManageCategories\)\{el\('categoriesTab'\)/);
  assert.match(html,/function renderCategories\(\)\{\s*if\(!cfg.canManageCategories\)return/);
  assert.doesNotMatch(html,/if\(cfg.isAdmin\)\{el\('categoriesTab'\)/);
});
