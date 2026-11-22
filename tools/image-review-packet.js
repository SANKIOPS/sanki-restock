#!/usr/bin/env node
'use strict';
// Unpack an owner-authenticated browser download. No credentials or network access needed.
const fs=require('fs'),path=require('path');
const [packetPath,outputPath]=process.argv.slice(2);if(!packetPath||!outputPath){console.error('Usage: node tools/image-review-packet.js <downloaded-packet.json> <output-directory>');process.exit(1);}
const packet=JSON.parse(fs.readFileSync(packetPath,'utf8'));if(packet.version!==1||!Array.isArray(packet.originals)||!packet.job?.id)throw Error('Not an Image Review work packet.');
const folder=path.resolve(outputPath);fs.mkdirSync(folder,{recursive:true});
const originals=packet.originals.map(photo=>{if(!['front','back','detail'].includes(photo.view))throw Error('Invalid view.');const ext=path.extname(photo.file);if(!['.jpg','.png','.webp'].includes(ext))throw Error('Unsupported original format.');const buffer=Buffer.from(photo.base64,'base64');if(buffer.length>20*1024*1024)throw Error('Original exceeds limit.');const digest=require('crypto').createHash('sha256').update(buffer).digest('hex');if(digest!==photo.sha256)throw Error('Original hash mismatch.');const file=path.join(folder,photo.view+ext);fs.writeFileSync(file,buffer);const {base64,...metadata}=photo;return{...metadata,localPath:file};});
const manifest={...packet,originals};fs.writeFileSync(path.join(folder,'job.json'),JSON.stringify(manifest,null,2)+'\n');console.log(JSON.stringify({jobId:packet.job.id,product:packet.item.title,sku:packet.item.variants.map(v=>v.sku),originals:originals.map(f=>f.localPath),manifest:path.join(folder,'job.json')},null,2));
