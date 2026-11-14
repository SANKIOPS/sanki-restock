'use strict';

const crypto = require('node:crypto');
const creditCardAccounting = require('./credit-card-accounting');
const expenseRefunds = require('./expense-refunds');

const ENTITIES = ['SANKI', 'SAMAST', 'PERSONAL'];
const REVIEWABLE_STATUSES = new Set(['pending', 'approved', 'partially_paid', 'paid']);
const SINGLE_KEYS = new Set(['sourceType', 'entity', 'nature', 'category', 'ledger', 'vendor', 'narration', 'particulars', 'reason', 'expectedRevision', 'fingerprint']);
const CHANGE_KEYS = new Set(['entity', 'nature', 'category', 'ledger', 'vendor', 'narration', 'particulars']);
const BULK_CHANGE_KEYS = new Set(['entity', 'nature', 'category', 'ledger']);
const BULK_KEYS = new Set(['records', 'expenseIds', 'changes', 'reason']);
const RECORD_KEYS = new Set(['sourceType', 'recordId', 'id', 'expectedRevision', 'fingerprint']);
const SOURCE_TYPES = new Set(['expense', 'reconciliation_expense']);

function money(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.round(parsed * 100) / 100 : 0;
}

function revision(expense) {
  const value = Number(expense && expense.classificationRevision);
  return Number.isInteger(value) && value >= 0 ? value : 0;
}

function recordFingerprint(expense, sourceType) {
  return crypto.createHash('sha256').update(JSON.stringify({sourceType:SOURCE_TYPES.has(sourceType) ? sourceType : 'expense',record:expense || null})).digest('hex');
}

function versionError(expense, sourceType, expectedRevision, fingerprint) {
  const expected = Number(expectedRevision), supplied = String(fingerprint || '').trim();
  if (!Number.isInteger(expected) || expected < 0 || !/^[a-f0-9]{64}$/i.test(supplied)) return {status:400,error:'Reload this classification row before saving; its revision and fingerprint are required.'};
  if (expected !== revision(expense) || supplied !== recordFingerprint(expense, sourceType)) return {status:409,error:'This expense changed after it was loaded. Reload Review Classification and apply the correction again; nothing was saved.'};
  return null;
}

function entity(value) {
  const normalized = String(value || '').trim().toUpperCase();
  return ENTITIES.includes(normalized) ? normalized : '';
}

function validDate(value) {
  const text = String(value || '').trim();
  if (!text) return '';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const date = new Date(text + 'T00:00:00.000Z');
  return Number.isNaN(date.getTime()) || date.toISOString().slice(0, 10) !== text ? null : text;
}

function requestedEntities(query) {
  const values = [].concat(query && (query.entities != null ? query.entities : (query.entity != null ? query.entity : (query.natures != null ? query.natures : query.nature))) || []);
  return Array.from(new Set(values.flatMap(value => String(value || '').split(',')).map(value => value.trim().toUpperCase()).filter(Boolean)));
}

function authorizedEntities(req, deps) {
  const approved = new Set([].concat(deps.approvalNatures(req) || []).map(entity).filter(Boolean));
  return ENTITIES.filter(value => approved.has(value) && (value !== 'PERSONAL' || deps.canManagePersonalAccounting(req)));
}

function categoryChoices(store, selectedEntities, deps) {
  return Object.fromEntries(selectedEntities.map(selectedEntity => {
    const seen = new Set();
    const choices = [].concat(deps.pickableLedgers(store, selectedEntity) || []).map(choice => ({
      name: String(choice && choice.name || '').trim(),
      group: String(choice && choice.group || '').trim(),
      type: String(choice && choice.type || '').trim()
    })).filter(choice => choice.name && !seen.has(choice.name.toLocaleLowerCase()) && seen.add(choice.name.toLocaleLowerCase())).sort((a, b) => a.name.localeCompare(b.name));
    return [selectedEntity, choices];
  }));
}

function refundLinked(store, expense) {
  if (!expense) return false;
  if (expense.expenseRefundReceiptId) return true;
  return [].concat(store && store.expenseRefunds || []).some(refund => refund && refund.status === 'received' && [].concat(refund.components || []).some(component => component && component.externalMovementId === expense.id));
}

function activeReconciliationDraft(store, expense) {
  const drafts = store && store.bankReconciliationDrafts || {};
  const payments = [].concat(expense && expense.payments || []);
  const reimbursementPayments = [].concat(expense && expense.reimbursementPayments || []);
  const directIds = [
    expense && expense.reconciliationDraft,
    expense && expense.bankReconciliationDraft,
    expense && expense.reconciliationSource && expense.reconciliationSource.draftId,
    expense && expense.reconciliationSource && expense.reconciliationSource.reconciliationDraft,
    ...payments.flatMap(payment => [payment && payment.bankReconciliationDraft, payment && payment.reconciliationDraft]),
    ...reimbursementPayments.flatMap(payment => [payment && payment.bankReconciliationDraft, payment && payment.reconciliationDraft])
  ].map(value => String(value || '').trim()).filter(Boolean);
  if (directIds.some(id => !!drafts[id])) return true;
  const expenseId = String(expense && expense.id || '').trim();
  const movementIds = new Set([expenseId]
    .concat(payments.flatMap(payment => {
      const batchPaymentId = String(payment && payment.batchPaymentId || '').trim();
      const linkedAdvances = batchPaymentId ? [].concat(store && store.vendorAdvances || []).filter(advance => String(advance && advance.batchPaymentId || '').trim() === batchPaymentId) : [];
      return [
        payment && payment.id ? expenseId + '/' + payment.id : '',
        batchPaymentId,
        ...linkedAdvances.flatMap(advance => [advance && advance.paymentReference, advance && advance.id])
      ];
    }))
    .concat(reimbursementPayments.flatMap(payment => [
      payment && payment.id ? expenseId + '/' + payment.id : '',
      payment && payment.batchId
    ]))
    .map(value => String(value || '').trim()).filter(Boolean));
  const linkedToActiveDraft = map => Object.entries(map || {}).some(([movementId, link]) => movementIds.has(String(movementId)) && link && !!drafts[String(link.reconciliationDraft || '')]);
  if (linkedToActiveDraft(store && store.bankDateOverrides) || linkedToActiveDraft(store && store.bankReconciliationLinks)) return true;
  return Object.values(drafts).some(draft => Object.values(draft && draft.resolutions || {}).some(resolution => {
    const linkedIds = [resolution && resolution.appId].concat(resolution && resolution.appIds || []).map(value => String(value || '').trim()).filter(Boolean);
    return linkedIds.some(id => movementIds.has(id));
  }));
}

function reviewableExpense(expense, sourceType, store = {}) {
  if (!expense) return false;
  if (activeReconciliationDraft(store, expense)) return false;
  if (sourceType === 'reconciliation_expense') {
    return !expense.accountingExcluded && money(expense.amount) > 0 && !refundLinked(store, expense);
  }
  return !expense.accountingExcluded && REVIEWABLE_STATUSES.has(String(expense.status || 'pending')) && !expense.rentalFeeId && !expense.nonExpenseReimbursement && !expense.salaryAdvanceId;
}

function hotelTagged(expense) {
  return ['reportingUnit', 'sourceReportingUnit', 'reportingTreatment', 'hotelName', 'hotelLocation', 'incomeSource'].some(key => String(expense && expense[key] || '').trim());
}

function vendorAdvanceLocked(expense) {
  return [].concat(expense && expense.vendorAdvanceApplications || []).some(application => application && !application.accountingExcluded && !application.voided && money(application.amount) > 0);
}

function refundDependency(store, expense) {
  return !!(expense && expense.id && (expenseRefunds.hasSource(store, expense.id) || expenseRefunds.hasCreditApplication(store, expense.id)));
}

function entityLockReason(store, expense, sourceType) {
  if (sourceType === 'reconciliation_expense') return 'Entity is locked because this line is owned by finalized bank or card reconciliation evidence.';
  if (hotelTagged(expense)) return 'Entity is locked because Sewak Park Hotel reporting metadata must remain inside PERSONAL accounting.';
  if (reconciled(store, expense, sourceType)) return 'Entity is locked because this expense is connected to bank reconciliation evidence.';
  if ([].concat(expense.payments || []).length || money(expense.paidAmount) > 0 || expense.paidAlready) return 'Entity is locked because a payment has already been recorded against this expense.';
  if (vendorAdvanceLocked(expense)) return 'Entity is locked because this expense uses a vendor-advance allocation.';
  if ([].concat(expense.reimbursementPayments || []).length || money(expense.reimbursementAmount) > 0 || !['', 'not_applicable'].includes(String(expense.reimbursementStatus || ''))) return 'Entity is locked because this expense participates in a reimbursement workflow.';
  if (refundDependency(store, expense)) return 'Entity is locked because this expense has a linked return, refund, or refund-credit application.';
  const sourceFields = ['reconciliationSource', 'bankReconciliationDraft', 'bankReconciliationEvidence', 'creditCardStatementId', 'creditCardId', 'procurementPoId', 'purchaseOrderId', 'inventoryPurchase', 'batchPaymentId', 'batchId', 'manualSaleId', 'orderId', 'shopifyOrderId', 'refundRecoveryExpenseId', 'expenseRefundReceiptId', 'expenseRefundId', 'refundId', 'loanId'];
  if (sourceFields.some(key => expense[key])) return 'Entity is locked because this expense belongs to another accounting source workflow.';
  if (String(expense.status || 'pending') !== 'pending') return 'Entity is locked after an expense is approved; use its source correction workflow for an entity change.';
  return '';
}

function editPolicy(store, expense, sourceType) {
  const fieldLocks = {}, entityReason = entityLockReason(store, expense, sourceType);
  if (entityReason) fieldLocks.entity = entityReason;
  if (vendorAdvanceLocked(expense)) fieldLocks.vendor = 'Vendor is locked because this expense uses a vendor-advance allocation. Correct or merge the vendor ledger first.';
  if (refundDependency(store, expense)) fieldLocks.vendor = 'Vendor is locked because this expense has a linked return, refund, or refund-credit application.';
  const editableFields = ['entity', 'category', 'vendor', 'narration'].filter(field => !fieldLocks[field]);
  return {editable:editableFields.length > 0, editableFields, fieldLocks, readOnlyReason:Array.from(new Set(Object.values(fieldLocks))).join(' ')};
}

function proofCount(expense) {
  const values = [];
  const add = value => {
    [].concat(Array.isArray(value) ? value : (value ? [value] : [])).forEach(item => {
      const text = String(item || '').trim();
      if (text) values.push(text);
    });
  };
  ['billPhotos', 'billPhoto', 'purchasePaymentProofs', 'purchasePaymentProof', 'paymentProofs', 'paymentProof', 'qrPhoto', 'exceptionEvidence'].forEach(key => add(expense[key]));
  [].concat(expense.payments || [], expense.reimbursementPayments || []).forEach(payment => {
    add(payment && payment.proofs); add(payment && payment.proof);
  });
  return new Set(values).size;
}

function reconciliationReference(store, expense, sourceType) {
  if (sourceType === 'reconciliation_expense') return String(expense.bankTransactionId || expense.adjustmentId || expense.settlementId || expense.reconciliationDraft || expense.id || '');
  const direct = expense.reconciliationSource && (expense.reconciliationSource.bankReference || expense.reconciliationSource.reference);
  if (direct) return String(direct);
  const payment = [].concat(expense.payments || []).find(item => item && (item.bankReference || item.bankReconciliationRow || item.bankReconciliationDraft));
  if (payment) return String(payment.bankReference || payment.bankReconciliationRow || payment.bankReconciliationDraft);
  const overrideKey = Object.keys(store.bankDateOverrides || {}).find(key => key === expense.id || key.startsWith(expense.id + '/'));
  if (overrideKey) {
    const override = store.bankDateOverrides[overrideKey] || {};
    return String(override.bankReference || override.reference || overrideKey);
  }
  return String(expense.id || '');
}

function reconciled(store, expense, sourceType) {
  if (sourceType === 'reconciliation_expense') return true;
  if (expense.reconciliationSource) return true;
  if ([].concat(expense.payments || []).some(payment => payment && (payment.bankReconciliationDraft || payment.bankReconciliationRow || payment.reconciliationDraft))) return true;
  return [store.bankDateOverrides, store.bankReconciliationLinks].some(map => Object.keys(map || {}).some(key => key === expense.id || key.startsWith(expense.id + '/')));
}

function row(store, expense, sourceType) {
  const source = SOURCE_TYPES.has(sourceType) ? sourceType : 'expense';
  const companyPayment = [].concat(expense.payments || []).filter(payment => payment && !payment.accountingExcluded).at(-1);
  const policy = editPolicy(store, expense, source);
  return {
    id: String(expense.id || ''),
    recordId: String(expense.id || ''),
    sourceType: source,
    revision: revision(expense),
    fingerprint: recordFingerprint(expense, source),
    date: String(expense.date || '').slice(0, 10),
    entity: entity(expense.nature) || 'SANKI',
    category: String(source === 'reconciliation_expense' ? expense.category : expense.ledger || '').trim(),
    categoryGroup: String(expense.categoryGroup || '').trim(),
    categoryType: String(expense.type || '').trim(),
    vendor: String(expense.vendor || '').trim(),
    narration: String(expense.particulars || '').trim(),
    amount: money(expense.amount),
    spendAmount: money(expense.amount),
    paidAmount: source === 'reconciliation_expense' ? money(expense.amount) : money(expense.paidAmount),
    status: String(expense.status || (source === 'reconciliation_expense' ? 'posted' : '')),
    account: String(companyPayment && companyPayment.account || expense.account || '').trim(),
    paymentMethod: String(companyPayment && companyPayment.paymentType || expense.paymentType || (source === 'reconciliation_expense' ? 'Bank reconciliation' : '')).trim(),
    reference: reconciliationReference(store, expense, source),
    proofCount: proofCount(expense),
    reconciled: reconciled(store, expense, source),
    ...policy
  };
}

function displayedCategory(expense, sourceType) {
  return String(sourceType === 'reconciliation_expense' ? expense.category : expense.ledger || '').trim() || 'Unclassified';
}

function canonicalCategory(store, targetEntity, requested, deps) {
  const value = String(requested || '').trim();
  if (!value) return null;
  const match = [].concat(deps.pickableLedgers(store, targetEntity) || []).find(choice => String(choice && choice.name || '').trim().toLocaleLowerCase() === value.toLocaleLowerCase());
  return match ? {name:String(match.name).trim(), group:String(match.group || '').trim(), type:String(match.type || '').trim()} : null;
}

function sameAlias(body, first, second) {
  if (body[first] == null || body[second] == null) return true;
  return String(body[first]).trim() === String(body[second]).trim();
}

function normalizedChanges(body) {
  if (!sameAlias(body, 'entity', 'nature')) return { error:'Entity and nature must identify the same accounting entity.' };
  if (!sameAlias(body, 'category', 'ledger')) return { error:'Category and ledger must identify the same expense category.' };
  if (!sameAlias(body, 'narration', 'particulars')) return { error:'Narration and particulars must contain the same text.' };
  const result = {};
  if (body.entity != null || body.nature != null) result.entity = entity(body.entity != null ? body.entity : body.nature);
  if (body.category != null || body.ledger != null) result.category = String(body.category != null ? body.category : body.ledger).trim();
  if (body.vendor != null) result.vendor = String(body.vendor).trim();
  if (body.narration != null || body.particulars != null) result.narration = String(body.narration != null ? body.narration : body.particulars).trim();
  return { changes: result };
}

function assertOnlyKeys(value, allowed, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return label + ' must be an object.';
  const unexpected = Object.keys(value).filter(key => !allowed.has(key));
  return unexpected.length ? 'Classification review cannot change ' + unexpected.join(', ') + '. Amount, date, payment, account, proof and reconciliation fields are locked.' : '';
}

function classificationSnapshot(expense, sourceType) {
  return { entity:entity(expense.nature) || 'SANKI', category:String(sourceType === 'reconciliation_expense' ? expense.category : expense.ledger || ''), vendor:String(expense.vendor || ''), narration:String(expense.particulars || ''), type:String(expense.type || ''), categoryGroup:String(expense.categoryGroup || '') };
}

function canAccessExpense(req, expense, deps) {
  return deps.canViewExpense(req, expense) && (!deps.canAccessExpense || deps.canAccessExpense(req, expense));
}

function applyClassification(store, req, expense, sourceType, requested, reason, deps) {
  const source = SOURCE_TYPES.has(sourceType) ? sourceType : 'expense';
  if (!reviewableExpense(expense, source, store)) return { status:409, error:'This is not an editable expense classification. Correct generated, pending-reconciliation or refund records in their source workflow.' };
  if (!canAccessExpense(req, expense, deps)) return { status:403, error:'This expense is restricted to the Owner or an authorized PERSONAL manager.' };
  const allowed = authorizedEntities(req, deps), previous = classificationSnapshot(expense, source);
  if (!allowed.includes(previous.entity)) return { status:403, error:'You do not have access to this accounting entity.' };
  const targetEntity = requested.entity === undefined ? previous.entity : requested.entity;
  if (!targetEntity || !allowed.includes(targetEntity)) return { status:403, error:'You cannot move this expense to the selected accounting entity.' };
  const policy = editPolicy(store, expense, source);
  if (targetEntity !== previous.entity && !policy.editableFields.includes('entity')) return { status:409, error:policy.fieldLocks.entity || 'This expense entity is locked by its accounting source.' };
  if (requested.vendor !== undefined && (!requested.vendor || requested.vendor.length > 160)) return { status:400, error:'Enter a vendor name (up to 160 characters).' };
  if (requested.narration !== undefined && (!requested.narration || requested.narration.length > 1000)) return { status:400, error:'Enter a narration (up to 1,000 characters).' };
  const requestedCategory = requested.category === undefined ? previous.category : requested.category;
  const categoryScopeChanged = targetEntity !== previous.entity || (requested.category !== undefined && requested.category !== previous.category);
  const categoryDefinition = categoryScopeChanged
    ? canonicalCategory(store, targetEntity, requestedCategory, deps)
    : (canonicalCategory(store, targetEntity, requestedCategory, deps) || {
        name:previous.category,
        group:previous.categoryGroup,
        type:previous.type
      });
  if (!categoryDefinition) return { status:400, error:'Select an active category assigned to the selected accounting entity.' };
  if (requested.vendor !== undefined && requested.vendor !== previous.vendor && !policy.editableFields.includes('vendor')) return { status:409, error:policy.fieldLocks.vendor || 'This expense vendor is locked by its accounting source.' };
  const shouldRegisterVendor = targetEntity !== previous.entity || (requested.vendor !== undefined && requested.vendor !== previous.vendor);
  const proposedVendor = requested.vendor === undefined ? previous.vendor : requested.vendor;
  const canonicalVendor = shouldRegisterVendor ? (deps.registerVendorForNature(store, targetEntity, proposedVendor, {}) || proposedVendor) : proposedVendor;
  const next = {
    entity: targetEntity,
    category: categoryDefinition.name,
    vendor: canonicalVendor,
    narration: requested.narration === undefined ? previous.narration : requested.narration,
    type: categoryDefinition.type,
    categoryGroup: categoryDefinition.group
  };
  const changes = Object.keys(next).filter(key => next[key] !== previous[key]).map(field => ({field,before:previous[field],after:next[field],derived:['type','categoryGroup'].includes(field)}));
  if (!changes.length) return { changed:false, changes:[], expense };
  const before = JSON.parse(JSON.stringify(expense));
  expense.nature = next.entity;
  if (source === 'reconciliation_expense') expense.category = next.category;
  else expense.ledger = next.category;
  if (next.type) expense.type = next.type; else delete expense.type;
  if (next.categoryGroup) expense.categoryGroup = next.categoryGroup; else delete expense.categoryGroup;
  if (shouldRegisterVendor) expense.vendor = next.vendor;
  expense.particulars = next.narration;
  expense.classificationRevision = revision(expense) + 1;
  const finalClassification = classificationSnapshot(expense, source);
  changes.forEach(change => { change.after = finalClassification[change.field]; });
  expense.auditHistory = Array.isArray(expense.auditHistory) ? expense.auditHistory : [];
  expense.auditHistory.push({
    id:'CLASS-'+String(expense.auditHistory.length + 1).padStart(3, '0'),
    reason,
    changes:JSON.parse(JSON.stringify(changes)),
    editedBy:String(req.user && req.user.username || 'system'),
    editedAt:new Date().toISOString(),
    classificationOnly:true,
    sourceType:source
  });
  deps.audit(store, req, 'EXPENSE_CLASSIFICATION_CHANGED', 'expense', expense.id, {
    nature:next.entity,
    before,
    after:JSON.parse(JSON.stringify(expense)),
    note:reason + ' · ' + source + ' classification-only correction; amount, date, account, payment method, proofs and reconciliation evidence unchanged.'
  });
  if (source === 'reconciliation_expense' && expense.creditCardStatementId) {
    store.expenseClassificationOverrides = store.expenseClassificationOverrides || {};
    store.expenseClassificationOverrides[expense.id] = {
      sourceType:source,
      sourceFingerprint:creditCardAccounting.postingFingerprint(expense),
      category:expense.category,
      type:expense.type || '',
      categoryGroup:expense.categoryGroup || '',
      vendor:expense.vendor || '',
      particulars:expense.particulars || '',
      classificationRevision:expense.classificationRevision,
      auditHistory:JSON.parse(JSON.stringify(expense.auditHistory || [])),
      updatedBy:String(req.user && req.user.username || 'system'),
      updatedAt:new Date().toISOString()
    };
  }
  return { changed:true, changes, expense };
}

function register(router, deps) {
  const manager = (req, res) => {
    if (deps.claimantOnly(req) || !deps.canManageCategoriesAndVendors(req)) {
      res.status(403).json({success:false,error:'Only an authorized category manager can review expense classifications.'});
      return false;
    }
    return true;
  };

  router.get('/api/expenses/classification-review', (req, res) => {
    if (!manager(req, res)) return;
    const allowed = authorizedEntities(req, deps), requested = requestedEntities(req.query || {});
    const invalid = requested.filter(value => !ENTITIES.includes(value));
    if (invalid.length) return res.status(400).json({success:false,error:'Unknown accounting entity: ' + invalid.join(', ') + '.'});
    const unauthorized = requested.filter(value => !allowed.includes(value));
    if (unauthorized.length) return res.status(403).json({success:false,error:'You do not have access to: ' + unauthorized.join(', ') + '.'});
    const selected = requested.length ? requested : allowed;
    const from = validDate(req.query && req.query.from), to = validDate(req.query && req.query.to);
    if (from === null || to === null) return res.status(400).json({success:false,error:'Dates must use YYYY-MM-DD.'});
    if (from && to && from > to) return res.status(400).json({success:false,error:'From date cannot be after To date.'});
    const page = Math.max(1, Math.floor(Number(req.query && req.query.page) || 1));
    const limit = Math.max(1, Math.min(250, Math.floor(Number(req.query && req.query.limit) || 100)));
    const categoryFilter = String(req.query && req.query.category || '').trim();
    const search = String(req.query && req.query.search || '').trim().toLocaleLowerCase();
    const store = deps.loadStore(), choices = categoryChoices(store, allowed, deps);
    const records = Object.values(store.expenses || {}).map(expense => ({sourceType:'expense',expense})).concat([].concat(store.reconciliationExpenses || []).map(expense => ({sourceType:'reconciliation_expense',expense}))).filter(record => {
      const {expense,sourceType} = record, expenseEntity = entity(expense && expense.nature) || 'SANKI';
      if (!selected.includes(expenseEntity) || !reviewableExpense(expense, sourceType, store) || !canAccessExpense(req, expense, deps)) return false;
      const date = String(expense.date || '').slice(0, 10);
      if (from && date < from || to && date > to) return false;
      return true;
    });
    const matchesSearch = ({expense,sourceType}) => {
      if (!search) return true;
      const safe = row(store, expense, sourceType);
      return [safe.id, safe.vendor, safe.narration, safe.category, safe.account, safe.reference].some(value => String(value || '').toLocaleLowerCase().includes(search));
    };
    const searchedRecords = search ? records.filter(matchesSearch) : records;
    const summary = new Map();
    if (!search) selected.forEach(selectedEntity => (choices[selectedEntity] || []).forEach(choice => summary.set(selectedEntity + '\u0000' + choice.name.toLocaleLowerCase(), {entity:selectedEntity,name:choice.name,group:choice.group,type:choice.type,count:0,total:0,valid:true})));
    searchedRecords.forEach(({expense,sourceType}) => {
      const selectedEntity = entity(expense.nature) || 'SANKI';
      const categoryName = displayedCategory(expense, sourceType);
      const categoryKey = selectedEntity + '\u0000' + categoryName.toLocaleLowerCase();
      const current = summary.get(categoryKey) || {entity:selectedEntity,name:categoryName,group:'',type:String(expense.type || ''),count:0,total:0,valid:false};
      current.count += 1; current.total = money(current.total + money(expense.amount)); summary.set(categoryKey, current);
    });
    const categories = Array.from(summary.values()).sort((a, b) => a.entity.localeCompare(b.entity) || a.name.localeCompare(b.name));
    const matching = categoryFilter ? searchedRecords.filter(({expense,sourceType}) => displayedCategory(expense, sourceType).toLocaleLowerCase() === categoryFilter.toLocaleLowerCase()) : [];
    matching.sort((a, b) => money(b.expense.amount) - money(a.expense.amount) || String(b.expense.date || '').localeCompare(String(a.expense.date || '')) || String(a.expense.id || '').localeCompare(String(b.expense.id || '')) || a.sourceType.localeCompare(b.sourceType));
    const total = matching.length, start = (page - 1) * limit;
    res.setHeader('Cache-Control', 'no-store');
    return res.json({
      success:true,
      allowedEntities:allowed,
      selectedEntities:selected,
      selectedFrom:from || '',
      selectedTo:to || '',
      categoryChoices:choices,
      categories,
      expenses:matching.slice(start, start + limit).map(record => row(store, record.expense, record.sourceType)),
      pagination:{page,limit,total,pages:Math.max(1, Math.ceil(total / limit))}
    });
  });

  router.patch('/api/expenses/classification-review/:id', (req, res) => {
    if (!manager(req, res)) return;
    const body = req.body || {}, keyError = assertOnlyKeys(body, SINGLE_KEYS, 'Classification change');
    if (keyError) return res.status(400).json({success:false,error:keyError});
    const reason = String(body.reason || '').trim();
    if (!reason) return res.status(400).json({success:false,error:'Enter a reason for this classification correction.'});
    if (reason.length > 1000) return res.status(400).json({success:false,error:'Keep the correction reason within 1,000 characters.'});
    const normalized = normalizedChanges(body);
    if (normalized.error) return res.status(400).json({success:false,error:normalized.error});
    if (!Object.keys(normalized.changes).length) return res.status(400).json({success:false,error:'Choose at least one classification field to change.'});
    const sourceType = String(body.sourceType || 'expense'), store = deps.loadStore();
    if (!SOURCE_TYPES.has(sourceType)) return res.status(400).json({success:false,error:'Select a valid classification source type.'});
    const expense = sourceType === 'reconciliation_expense' ? [].concat(store.reconciliationExpenses || []).find(record => String(record.id || '') === String(req.params.id || '')) : (store.expenses || {})[String(req.params.id || '')];
    if (!expense) return res.status(404).json({success:false,error:'Expense not found.'});
    const stale = versionError(expense, sourceType, body.expectedRevision, body.fingerprint);
    if (stale) return res.status(stale.status).json({success:false,error:stale.error});
    const result = applyClassification(store, req, expense, sourceType, normalized.changes, reason, deps);
    if (result.error) return res.status(result.status || 400).json({success:false,error:result.error});
    if (result.changed) deps.saveStore(store, {skipCategoryNormalization:true});
    return res.json({success:true,expense:row(store, expense, sourceType),changes:result.changes,changed:result.changed});
  });

  router.post('/api/expenses/classification-review/bulk', (req, res) => {
    if (!manager(req, res)) return;
    const body = req.body || {}, keyError = assertOnlyKeys(body, BULK_KEYS, 'Bulk classification change');
    if (keyError) return res.status(400).json({success:false,error:keyError});
    const changeError = assertOnlyKeys(body.changes, BULK_CHANGE_KEYS, 'Bulk changes');
    if (changeError) return res.status(400).json({success:false,error:changeError});
    const reason = String(body.reason || '').trim(), requestedRecords = Array.isArray(body.records) ? body.records : [].concat(body.expenseIds || []).map(recordId => ({sourceType:'expense',recordId}));
    const descriptorError = requestedRecords.map(record => assertOnlyKeys(record, RECORD_KEYS, 'Bulk record')).find(Boolean);
    if (descriptorError) return res.status(400).json({success:false,error:descriptorError});
    const records = Array.from(new Map(requestedRecords.map(record => {
      const sourceType = String(record && record.sourceType || 'expense'), recordId = String(record && (record.recordId || record.id) || '').trim();
      return [sourceType + '\u0000' + recordId, {sourceType,recordId,expectedRevision:record.expectedRevision,fingerprint:String(record.fingerprint || '').trim()}];
    })).values()).filter(record => record.recordId);
    if (!reason) return res.status(400).json({success:false,error:'Enter a reason for this bulk classification correction.'});
    if (reason.length > 1000) return res.status(400).json({success:false,error:'Keep the correction reason within 1,000 characters.'});
    if (!records.length || records.length > 100) return res.status(400).json({success:false,error:'Select between 1 and 100 expenses.'});
    if (records.some(record => !SOURCE_TYPES.has(record.sourceType))) return res.status(400).json({success:false,error:'Select valid classification source types.'});
    const normalized = normalizedChanges(body.changes || {});
    if (normalized.error) return res.status(400).json({success:false,error:normalized.error});
    if (!Object.keys(normalized.changes).length) return res.status(400).json({success:false,error:'Choose at least one classification field to change.'});
    const original = deps.loadStore(), store = JSON.parse(JSON.stringify(original)), updated = [], allChanges = [];
    for (const record of records) {
      const expense = record.sourceType === 'reconciliation_expense' ? [].concat(store.reconciliationExpenses || []).find(value => String(value.id || '') === record.recordId) : (store.expenses || {})[record.recordId];
      if (!expense) return res.status(404).json({success:false,error:'Expense not found: ' + record.recordId + '. No changes were saved.'});
      const stale = versionError(expense, record.sourceType, record.expectedRevision, record.fingerprint);
      if (stale) return res.status(stale.status).json({success:false,error:record.recordId + ': ' + stale.error});
      const result = applyClassification(store, req, expense, record.sourceType, normalized.changes, reason, deps);
      if (result.error) return res.status(result.status || 400).json({success:false,error:record.recordId + ': ' + result.error + ' No changes were saved.'});
      updated.push(row(store, expense, record.sourceType)); allChanges.push({...record,changes:result.changes});
    }
    if (allChanges.some(item => item.changes.length)) {
      const entities = Array.from(new Set(updated.map(item => item.entity)));
      deps.audit(store, req, 'EXPENSE_CLASSIFICATION_BULK_CHANGED', 'expense_batch', records.map(record => record.sourceType + ':' + record.recordId).join(','), {nature:entities.length === 1 ? entities[0] : 'SANKI',before:null,after:allChanges,note:reason + ' · Atomic classification-only batch across ' + entities.join(', ') + '.'});
      deps.saveStore(store, {skipCategoryNormalization:true});
    }
    return res.json({success:true,updated,count:updated.length,changes:allChanges});
  });
}

module.exports = { register, row, applyClassification, requestedEntities, validDate, reviewableExpense, activeReconciliationDraft, recordFingerprint };
