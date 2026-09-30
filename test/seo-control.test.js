'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('fs'),path=require('path');
test('playbook seeds all 55 tasks and completion has evidence',()=>{const {seed}=require('../modules/seo-control');const s=seed();assert.equal(s.tasks.length,55);assert.ok(s.tasks.filter(x=>x.status==='Completed').every(x=>x.evidence));});
test('SEO module is visible only to the owner',()=>{const {visibleFor}=require('../modules/module-registry');assert.ok(visibleFor({roles:['owner']}).some(x=>x.key==='seo-control'));assert.ok(!visibleFor({roles:['admin']}).some(x=>x.key==='seo-control'));});
test('SEO page stays private, index-blocked and uses the shared sidebar',()=>{const html=fs.readFileSync(path.join(__dirname,'../public/seo-control.html'),'utf8');assert.match(html,/noindex,nofollow/);assert.match(html,/sidebar\.js/);assert.match(html,/OWNER ONLY/);});
