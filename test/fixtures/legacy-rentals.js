const seeds=()=>[
  ['fraganote-3','Fraganote','3rd',79800,'2026-08-31'],['fraganote-1','Fraganote','1st',105000,'2026-08-31'],
  ['suraagna-top','Suraagna','Top',60000,''],['lujo-2','Lujo Fab','2nd',89250,'2026-10-12'],
  ['lujo-basement','Lujo Fab','Basement',69500,''],['amty-4','AMTY','4th',83790,'']
].map(([id,tenant,floor,baseRent,endDate])=>({id,tenant,floor,baseRent,endDate,property:'Kirti Nagar',startMonth:'2026-09',dueDay:null,cgst:9,sgst:9,tds:10,history:[],note:id==='suraagna-top'?'Status after April needs confirmation.':id==='amty-4'?'₹79,800 before July 2026; ₹83,790 from July.':''}));
module.exports=seeds;
