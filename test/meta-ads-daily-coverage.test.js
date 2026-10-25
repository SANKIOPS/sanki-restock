'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {insights}=require('../modules/meta-ads-insights');
const state={campaigns:[{id:'c'}],adSets:[{id:'s',campaignId:'c',performanceGoal:'PROFILE_VISIT',destination:'Instagram profile'}],ads:[{id:'a',adSetId:'s'}],results:[]};
const empty={adId:'a',adSetId:'s',periodStart:'2026-10-22',periodEnd:'2026-10-22',spend:0,impressions:0,reach:0,resultIndicator:'',reportGoal:'PROFILE_VISIT',reportDestination:'Instagram profile',attributionSetting:'1-day click'};
const active={...empty,periodStart:'2026-10-23',periodEnd:'2026-10-23',spend:20,impressions:100,profileVisits:10,resultIndicator:'profile_visit_view'};
test('explicit zero-delivery day without result indicator combines with active daily report',()=>{const r=insights({...state,results:[empty,active]},'2026-10-22~2026-10-23');assert.equal(r.creatives.length,1);assert.equal(r.creatives[0].count,10);assert.equal(r.creatives[0].spend,20);assert.equal(r.creatives[0].reach,null);});
test('missing days, nonzero unknown delivery and different attribution do not fill coverage',()=>{for(const first of [null,{...empty,spend:1},{...empty,attributionSetting:'7-day click'}]){const r=insights({...state,results:[...(first?[first]:[]),active]},'2026-10-22~2026-10-23');assert.equal(r.creatives.length,0);}});
