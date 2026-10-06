const test=require('node:test'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
for(const file of ['procurement.html','fresh-procurement.html','size-tracker.html'])test(file+' compiles all inline browser scripts',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public',file),'utf8');
  for(const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))if(script[1].trim())new vm.Script(script[1],{filename:file});
});
