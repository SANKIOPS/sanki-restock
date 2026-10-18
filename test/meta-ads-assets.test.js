'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const root=path.join(__dirname,'..','public');
test('Meta page references the current content versions of scripts and styles',()=>{const html=fs.readFileSync(path.join(root,'meta-ads.html'),'utf8');for(const name of ['meta-ads.js','meta-ads.css','meta-linked-workspace.js','meta-insight-controls.js']){const hash=crypto.createHash('sha256').update(fs.readFileSync(path.join(root,name),'utf8').replace(/\r\n/g,'\n')).digest('hex').slice(0,12);assert.ok(html.includes('/'+name+'?v='+hash),name+' must reference its current content hash');}});
