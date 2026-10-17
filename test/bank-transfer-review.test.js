const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
test('review exposes Record transfer only for an unmatched bank transaction',()=>{
  const html=fs.readFileSync(require('node:path').join(__dirname,'../public/expenses.html'),'utf8');
  // Exercise only this transfer-action wrapper. Later reversal wrappers have
  // their own dependencies and are covered by the reconciliation route tests.
  const start=html.indexOf('var openBankReviewWithMatches='),end=html.indexOf('window.uploadVelocityRemittanceForBank=',start);
  assert.ok(start>=0&&end>start);
  let row,buttons='';
  const context={window:{openBankReview:()=>{}},bankReconView:null,bankReconRow:()=>row,esc:String,el:()=>({insertAdjacentHTML:(where,text)=>buttons+=text})};
  vm.runInNewContext(html.slice(start,end),context);
  for(const bank of [{debit:5000},{credit:5000}]){
    buttons='';row={id:'bank-1',status:'missing_in_app',bank};context.window.openBankReview(row.id);
    assert.match(buttons,/Record transfer/);assert.match(buttons,/internal_transfer/);
  }
  for(const status of ['resolved','missing_in_bank','possible_match']){
    buttons='';row={id:'bank-1',status,bank:{debit:5000}};context.window.openBankReview(row.id);assert.equal(buttons,'');
  }
});
