'use strict';
// One-time theme connection. Publishing later updates product metadata, never theme code.
const fs=require('fs'),path=require('path');
async function main(){
 const [envFile,outDir]=process.argv.slice(2);if(!envFile||!outDir)throw Error('Usage: node tools/deploy-image-review-card.js ENV_FILE BACKUP_DIRECTORY [--deploy]');
 const env=Object.fromEntries(fs.readFileSync(envFile,'utf8').split(/\r?\n/).filter(l=>l.includes('=')&&!l.startsWith('#')).map(l=>{const n=l.indexOf('=');return[l.slice(0,n),l.slice(n+1)];}));
 const auth=await fetch('https://'+env.SHOPIFY_STORE+'/admin/oauth/access_token',{method:'POST',body:new URLSearchParams({grant_type:'client_credentials',client_id:env.SHOPIFY_CLIENT_ID,client_secret:env.SHOPIFY_CLIENT_SECRET})});if(!auth.ok)throw Error('Shopify authentication failed: '+auth.status);const token=(await auth.json()).access_token;
 async function gql(query,variables){const r=await fetch('https://'+env.SHOPIFY_STORE+'/admin/api/2026-07/graphql.json',{method:'POST',headers:{'content-type':'application/json','x-shopify-access-token':token},body:JSON.stringify({query,variables})});const b=await r.json();if(!r.ok||b.errors)throw Error(JSON.stringify(b.errors||r.status));return b.data;}
 const theme=(await gql('query{themes(first:100){nodes{id name role}}}')).themes.nodes.find(t=>t.role==='MAIN');if(!theme)throw Error('No main theme');
 const names=['snippets/product-card.liquid','templates/product.sanki-review-verify.liquid'];
 async function read(){return(await gql('query($id:ID!,$names:[String!]!){theme(id:$id){files(filenames:$names,first:10){nodes{filename body{... on OnlineStoreThemeFileBodyText{content}}}}}}',{id:theme.id,names})).theme.files.nodes;}
 const before=await read(),card=before.find(f=>f.filename===names[0])?.body.content;if(!card)throw Error('Product card missing');
 const block=fs.readFileSync(path.join(__dirname,'../theme/image-review-card.liquid'),'utf8');
 let after=card;
 if(!card.includes('SANKI_APPROVED_IMAGE_REVIEW_V1')){
  const needle='  assign has_secondary = false';if(card.split(needle).length!==2||card.split('  data-sk-card\n').length!==2)throw Error('Theme structure changed; no write');
  after=card.replace(needle,block+needle).replace('  data-sk-card\n','  data-sk-card\n  data-sanki-review-proof="{% if sk_review_verified_job != blank %}SANKI_REVIEW_CARD:{{ sk_review_verified_job }}:{{ v.id }}{% endif %}"\n');
 }
 const files=[{filename:names[0],body:{type:'TEXT',value:after}},{filename:names[1],body:{type:'TEXT',value:fs.readFileSync(path.join(__dirname,'../theme/product.sanki-review-verify.liquid'),'utf8')}}];
 fs.mkdirSync(outDir,{recursive:true});fs.writeFileSync(path.join(outDir,'theme-before.json'),JSON.stringify({theme,files:before},null,2));fs.writeFileSync(path.join(outDir,'theme-plan.json'),JSON.stringify({theme,files},null,2));
 if(!process.argv.includes('--deploy')){console.log('Theme connection prepared. No live writes.');return;}
 if(JSON.stringify(await read())!==JSON.stringify(before))throw Error('Concurrent theme change; no write');
 const result=await gql('mutation($id:ID!,$files:[OnlineStoreThemeFilesUpsertFileInput!]!){themeFilesUpsert(themeId:$id,files:$files){upsertedThemeFiles{filename} userErrors{field message}}}',{id:theme.id,files});
 if(result.themeFilesUpsert.userErrors.length)throw Error(JSON.stringify(result.themeFilesUpsert.userErrors));
 const saved=await read();for(const f of files)if(saved.find(s=>s.filename===f.filename)?.body.content!==f.body.value)throw Error('Theme readback mismatch');
 fs.writeFileSync(path.join(outDir,'theme-deployment.json'),JSON.stringify({theme,result,verified:true},null,2));console.log('Main theme connection deployed and read back. No product images published.');
}
main().catch(e=>{console.error(e.message);process.exitCode=1;});
