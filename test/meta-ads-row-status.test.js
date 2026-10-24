'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const context={window:{}};vm.runInNewContext(fs.readFileSync(require.resolve('../public/meta-linked-workspace.js'),'utf8'),context);const {statusFor}=context.window.MetaLinkedWorkspace;
const on={status:'reported_running'},off={status:'paused'};
test('active campaign does not make a paused ad set or ad appear on',()=>{assert.equal(statusFor(on,off,on).label,'Off');assert.equal(statusFor(on,on,off).label,'Off');assert.equal(statusFor(off,on,on).label,'Off');assert.equal(statusFor(on,on,on).on,true);});
test('three ad sets count two on and one off under an active campaign',()=>{const states=[on,off,on].map(set=>statusFor(on,set));assert.equal(states.filter(s=>s.on).length,2);assert.equal(states.filter(s=>s.label==='Off').length,1);});
test('missing statuses are unconfirmed and archived parents keep children off',()=>{assert.equal(statusFor(on,{status:'unknown'},on).label,'Unconfirmed');assert.equal(statusFor({...on,archived:true},on,on).label,'Off');});
test('unknown status has an explicit badge instead of an off switch',()=>{const {switchFor,setCounts}=context.window.MetaLinkedWorkspace;assert.match(switchFor({status:'unknown'}),/Unconfirmed/);assert.doesNotMatch(switchFor({status:'unknown'}),/state-switch/);assert.match(switchFor(off),/state-switch/);assert.match(setCounts({status:'unknown'},[on,off]),/1 on · 1 off/);});
