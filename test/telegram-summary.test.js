const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-telegram-summary-'));process.env.DATA_PATH=path.join(dir,'data.json');fs.writeFileSync(process.env.DATA_PATH,'{}');
const expenses=require('../modules/expenses'),telegram=require('../modules/telegram');

test('daily Telegram summary separates activity, pending work and employee settlement',()=>{
  const store={openingBalances:{'Prashant Axis 3645':-1049.08,'Prashant Cash':-523},expenses:{
    old:{id:'EX-OLD',date:'2026-09-01',amount:5000,nature:'SANKI',status:'paid',approvedAt:'x',vendor:'The Printing Solutions',payments:[{id:'PAY-1',date:'2026-09-05',amount:2000,account:'Prashant Axis 3645'}]},
    paid:{id:'EX-PAID',date:'2026-09-05',amount:473,nature:'SANKI',status:'paid',approvedAt:'x',paidAmount:473,vendor:'Zepto',payments:[{id:'PAY-1',date:'2026-09-05',amount:473,account:'Prashant Cash'}]},
    approved:{id:'EX-A',date:'2026-09-05',amount:190,nature:'SANKI',status:'approved',approvedAt:'x',paidAmount:0,vendor:'Kalu Flower',payments:[]},
    personal:{id:'EX-P',date:'2026-09-05',amount:100,nature:'PERSONAL',status:'pending',paidAmount:0,vendor:'Personal item',payments:[]},
    claimant:{id:'EX-C',date:'2026-09-04',amount:500,nature:'SANKI',status:'paid',approvedAt:'2026-09-04',paidAlready:true,personalPaidAmount:500,reimbursementAmount:200,claimant:'prashant',vendor:'Claimed item',payments:[],reimbursementPayments:[]}
  },transfers:[],adjustments:[],receipts:[],bankTruthMovements:[]};
  const out=expenses.telegramAccountingSummary('2026-09-05','2026-09-05',store);
  assert.equal(out.recorded.total,763);assert.equal(out.recorded.business,663);assert.equal(out.recorded.personal,100);assert.equal(out.totalPayments,2473);assert.equal(out.approvedPending[0].amount,190);assert.equal(out.awaitingApproval[0].amount,100);assert.deepEqual(out.settlements,[{employee:'Prashant',amount:300,count:1}]);
  const text=telegram.formatAccountingSummary(out,'5 September 2026');
  assert.match(text,/The Printing Solutions ₹2,000 — Prashant Axis 3645/);assert.match(text,/Company owes Prashant ₹300/);assert.doesNotMatch(text,/Company owes Arshpreet|Company owes Pradeep|₹4,045\.08/);assert.match(text,/Action:<\/b> pay 1 expense ₹190 · review 1 ₹100/);assert.doesNotMatch(text,/proof|bill photo/i);
});

test('summary date controls use India time and accept both date formats',()=>{
  assert.deepEqual(telegram.summaryRange('yesterday',new Date('2026-09-05T18:00:00Z')),{from:'2026-09-04',to:'2026-09-04',label:'Yesterday · 2026-09-04'});
  assert.equal(telegram.parseSummaryDate('05-09-2026'),'2026-09-05');assert.equal(telegram.parseSummaryDate('summary 2026-09-05'),'2026-09-05');
});
