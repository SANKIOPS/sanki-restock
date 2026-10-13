'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm');
const {visibleFor}=require('../modules/module-registry');
test('rental navigation is visible only to owners and Prashant',()=>{
  for(const [username,roles] of [['rental-test',['owner']],['rental-test',['owner','admin']],['rental-test',['admin']],['rental-test',['accounting']],['rental-test',['sales']],['rental-test',['admin','accounting']],['prashant',['admin']]]){
    const item=visibleFor({username,roles,role:roles[0]}).find(m=>m.key==='rentals');
    const expected=roles.includes('owner')||username==='prashant';
    assert.equal(Boolean(item),expected);
    if(item) assert.equal(item.href,'/rentals.html');
  }
});
test('direct rental page access rejects nonowners except Prashant before admin bypass',()=>{
  const au=require('../modules/auth-users');
  const mod={exports:{}};
  vm.runInNewContext(fs.readFileSync(require.resolve('../auth'),'utf8'),{
    require:id=>id==='./modules/auth-users'?{...au,verifySession:req=>req.testUser}:require(id),
    module:mod,exports:mod.exports,process,console,Buffer
  });
  for(const [username,roles] of [['rental-test',['owner']],['rental-test',['owner','admin']],['rental-test',['admin']],['rental-test',['accounting']],['rental-test',['sales']],['prashant',['admin']]]){
    let allowed=false,redirect;
    mod.exports.gate({path:'/rentals.html',method:'GET',headers:{},testUser:{username,roles,role:roles[0]}},
      {redirect:(status,url)=>{redirect={status,url}},status(){return this},json(){},send(){}},()=>{allowed=true});
    assert.equal(allowed,roles.includes('owner')||username==='prashant');
    if(!allowed)assert.equal(redirect.status,302);
  }
});
