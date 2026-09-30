'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm');
const {visibleFor}=require('../modules/module-registry');
test('rental navigation is visible only to owners, including mixed roles',()=>{
  for(const roles of [['owner'],['owner','admin'],['admin'],['accounting'],['sales'],['admin','accounting']]){
    const item=visibleFor({username:'rental-test',roles,role:roles[0]}).find(m=>m.key==='rentals');
    assert.equal(Boolean(item),roles.includes('owner'));
    if(item) assert.equal(item.href,'/rentals.html');
  }
});
test('direct rental page access rejects nonowners before admin bypass',()=>{
  const au=require('../modules/auth-users');
  const mod={exports:{}};
  vm.runInNewContext(fs.readFileSync(require.resolve('../auth'),'utf8'),{
    require:id=>id==='./modules/auth-users'?{...au,verifySession:req=>req.testUser}:require(id),
    module:mod,exports:mod.exports,process,console,Buffer
  });
  for(const roles of [['owner'],['owner','admin'],['admin'],['accounting'],['sales']]){
    let allowed=false,redirect;
    mod.exports.gate({path:'/rentals.html',method:'GET',headers:{},testUser:{username:'rental-test',roles,role:roles[0]}},
      {redirect:(status,url)=>{redirect={status,url}},status(){return this},json(){},send(){}},()=>{allowed=true});
    assert.equal(allowed,roles.includes('owner'));
    if(!allowed)assert.equal(redirect.status,302);
  }
});
