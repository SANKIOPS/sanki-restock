'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {mergeSetupFiles,setupCoverage}=require('../modules/meta-ads-setup-bundle'),{parseSetup}=require('../modules/meta-ads-import');
const file=s=>({buffer:Buffer.from(s)});
const campaign=file('Campaign ID,Campaign name,Campaign status\n123,C,OFF');
const sets=file('Campaign ID,Campaign name,Ad set ID,Ad set name,Ad set status\n123,C,234,S,ON');
const ads=file('Campaign ID,Campaign name,Ad set ID,Ad set name,Ad ID,Ad name,Ad status\n123,C,234,S,345,A,PAUSED');
test('linked campaign/ad-set/ad tables merge hierarchy and exact on/off values',()=>{const r=mergeSetupFiles([campaign,sets,ads]);assert.equal(r.records.campaigns.length,1);assert.equal(r.records.adSets.length,1);assert.equal(r.records.ads.length,1);assert.equal(r.records.campaigns[0].status,'paused');assert.equal(r.records.adSets[0].status,'reported_running');assert.equal(r.records.ads[0].status,'paused');assert.equal(r.records.ads[0].adSetId,r.records.adSets[0].id);assert.equal(r.level,'bundle');});
test('conflicting explicit statuses require consistent exports',()=>{assert.throws(()=>mergeSetupFiles([campaign,file('Campaign ID,Campaign name,Campaign status\n123,C,ON')]),/Conflicting status/);});
test('campaign-only and table-only hierarchy remain clearly incomplete',()=>{const r=mergeSetupFiles([campaign,sets,ads]);const c=setupCoverage(r.records)[0];assert.equal(c.complete,false);assert.ok(c.missing.includes('locations'));assert.ok(c.missing.includes('creative URLs'));const only=setupCoverage({campaigns:r.records.campaigns,adSets:[],ads:[]})[0];assert.ok(only.missing.includes('ad sets'));});
test('blank configured status is not an explicit on or off update',()=>{const r=parseSetup(Buffer.from('Campaign ID,Campaign Name,Campaign Status,Ad Set ID,Ad Set Name,Ad Set Run Status,Ad ID,Ad Name,Ad Status\n123,C,,234,S,,345,A,'));assert.equal(Object.hasOwn(r.records.campaigns[0],'status'),false);assert.equal(Object.hasOwn(r.records.adSets[0],'status'),false);assert.equal(Object.hasOwn(r.records.ads[0],'status'),false);});
test('missing detail checklist treats broad targeting and Advantage+ placements as confirmed and excludes archived records',()=>{
 const {missingDetails}=require('../modules/meta-ads-setup-bundle');
 const state={campaigns:[{id:'c',name:'C',objective:'Traffic',status:'paused'}],adSets:[{id:'s',campaignId:'c',name:'S',status:'paused',budgetDaily:0,destination:'WhatsApp',performanceGoal:'LINK_CLICKS',locations:[{name:'India'}],ageMin:18,ageMax:65,gender:'all',interestsConfirmed:true,interests:[],behaviorsConfirmed:true,behaviors:[],placementsConfirmed:true,placements:['Advantage+ placements']}],ads:[{id:'a',adSetId:'s',name:'A',status:'paused',collection:'Femme',creativeTypes:['UGC'],model:'Not applicable',mediaUrls:['https://example.com']},{id:'old',adSetId:'s',archived:true}]};
 assert.deepEqual(missingDetails(state),[{type:'adSets',id:'s',name:'S',keys:['whatsappNumber']}]);state.adSets[0].whatsappNumber='+919999999999';assert.deepEqual(missingDetails(state),[]);
});
