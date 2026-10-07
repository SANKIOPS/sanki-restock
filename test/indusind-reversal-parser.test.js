'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const os=require('node:os');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-reversal-parser-'));
process.env.DATA_PATH=path.join(dir,'data.json');
const {parseBankStatementText,bankReversalPairCandidates}=require('../modules/expenses');
test.after(()=>fs.rmSync(dir,{recursive:true,force:true}));

function statement(reference='12345678\n9012'){
  return `Account Statement\n000000008181\nINDUS PRIVILEGE\nStatement Period: 07 Oct 2026 - 07 Oct 2026\nTransaction History\nDateParticularsChq No/Ref NoWithdrawalDepositBalance\n07 Oct 2026 UPI/123456789099/DR/PRAD/SBIN/merchant S6338577665.000.00361.67\n07 Oct 2026 REVERSED : UPI/${reference}/DR/PRAD/ICIC/merchant S41889090.00665.001026.67\n07 Oct 2026 UPI/123456789012/DR/PRAD/ICIC/merchant S4188311665.000.00361.67\n07 Oct 2026 IMPS/P2A/123456789088/UTIB/merchant S971234510.002000.001026.67\nThis is a computer generated statement`;
}
test('IndusInd wrapped reversal reference remains the original exact UPI reference',()=>{
  const rows=parseBankStatementText(statement());
  assert.equal(rows.length,4);
  assert.equal(rows[1].reference,'123456789012');
  assert.equal(rows[1].bankReference,'S4188909');
  assert.equal(rows[1].reversal,true);
  assert.equal(rows[1].credit,665);
  assert.equal(rows[1].debit,0,'DR inside reversal narration is not the reversal money direction');
  assert.deepEqual(bankReversalPairCandidates(rows)[0].bankRowIds.slice().sort(),['bank-1','bank-2']);
  assert.equal(rows[0].reference,'123456789099','separate successful payment stays separate');
  assert.equal(rows.statementSummary.openingBalance,-973.33);
  assert.equal(rows.statementSummary.closingBalance,361.67);
});
test('IndusInd never manufactures a twelve-digit reference from an invalid digit count',()=>{
  const rows=parseBankStatementText(statement('1234567\n9012'));
  assert.notEqual(rows[1].reference,'123456789012');
  assert.equal(bankReversalPairCandidates(rows).length,0);
});
