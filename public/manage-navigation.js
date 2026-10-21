(function (root, factory) {
  'use strict';
  var api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.SankiManageNavigation = api;
})(typeof window !== 'undefined' ? window : this, function () {
  'use strict';

  // A navigation catalogue, not another authorization layer. Every destination
  // still enforces its existing server permissions and transaction locks.
  var HEADS = [
    { key: 'classification', title: 'Vendors & classification', description: 'Organize shared names, expense categories and groups.' },
    { key: 'money', title: 'Expenses & money', description: 'Use the existing expense, payment and account workflows.' },
    { key: 'sales', title: 'Sales & orders', description: 'Review orders, customer payments and delivery evidence.' },
    { key: 'inventory', title: 'Inventory & purchases', description: 'Work with products, purchasing and received stock.' },
    { key: 'operations', title: 'People & operations', description: 'Open the existing payroll, incentives and scheduling tools.' },
    { key: 'administration', title: 'Administration & marketing', description: 'Manage permitted users and specialist tools.' }
  ];

  function rolesOf(user) {
    return user && Array.isArray(user.roles) ? user.roles : user && user.role ? [user.role] : [];
  }

  function buildCatalog(options) {
    options = options || {};
    var modules = Array.isArray(options.modules) ? options.modules : [];
    var user = options.user || {};
    var roles = rolesOf(user);
    var cfg = options.expenseConfig;
    var heads = HEADS.map(function (head) {
      return { key: head.key, title: head.title, description: head.description, expanded: false, items: [] };
    });
    var live = {};
    modules.forEach(function (item) {
      // Exact known hrefs below prevent arbitrary supplied links, hidden or
      // retired modules from appearing in the universal menu.
      if (item && item.status === 'live') live[item.key] = item.href;
    });
    function has(key, href) { return live[key] === href; }
    function add(head, key, title, description, href, actions) {
      heads.filter(function (item) { return item.key === head; })[0].items.push({
        key: key, title: title, description: description, href: href, actions: actions.slice()
      });
    }
    // Financial maintenance fails closed when config is missing/unavailable.
    // No role/name fallback may manufacture category or payment capabilities.
    var expenses = has('expenses', '/expenses.html') && cfg && cfg.claimantOnly === false;
    if (expenses && cfg.canManageVendors === true) {
      add('classification', 'vendors', 'Manage vendors', 'Add approved vendors, rename shared names or merge duplicate vendor ledgers.', '/expenses.html?tab=vendors', ['Add vendor', 'Rename vendor', 'Merge vendors']);
    }
    if (expenses && cfg.canManageCategories === true) {
      add('classification', 'categories', 'Manage categories', 'View current categories and add a category under an existing or newly organized group.', '/manage.html?section=categories', ['View categories', 'Add category', 'Assign group when adding']);
      add('classification', 'groups', 'Manage groups', 'Add groups, rename a group or merge duplicates into one final group.', '/manage.html?section=groups', ['Add group', 'Rename group', 'Merge groups']);
    }
    if (expenses && cfg.canApprove === true) {
      add('money', 'expenses', 'Manage expenses', 'Find expenses and use the existing correction form; reconciled amount and date locks remain enforced.', '/expenses.html?tab=list', ['Find expense', 'Edit permitted details']);
    }
    if (expenses && (cfg.isAdmin === true || cfg.canVerifyCounterCash === true)) {
      add('money', 'transactions', cfg.isAdmin === true ? 'Manage transactions' : 'Counter Cash verification', cfg.isAdmin === true ? 'Review payments, transfers and reconciliations through their original account-ledger workflow.' : 'Open your existing Counter Cash verification screen.', '/expenses.html?tab=cash', cfg.isAdmin === true ? ['Account ledgers', 'Review transactions', 'Reconcile account'] : ['Verify Counter Cash']);
    }
    if (expenses && cfg.isAdmin === true) {
      add('money', 'opening-balances', 'Manage opening balances', 'Review account-ledger openings, stage corrections without posting, and use the existing reconciliation for active statements.', '/manage.html?section=opening-balances', ['Review recorded openings', 'Stage opening balance', 'Owner-only apply']);
      add('money', 'payables', 'Manage payables', 'Open existing payment forms and their account and proof checks.', '/expenses.html?tab=pending', ['Review payables', 'Record permitted payment']);
      add('money', 'reimbursements', 'Manage reimbursements', 'Review pending and recorded employee reimbursements.', '/expenses.html?tab=reimbursements', ['Review reimbursements', 'Record permitted reimbursement']);
      add('money', 'receivables', 'Receivables, refunds & returns', 'Open the current receivables and refund-recovery workflows.', '/expenses.html?tab=receivables', ['Review receivables', 'Review refunds and returns']);
      add('money', 'audit', 'Audit history', 'Review existing expense and accounting change history.', '/expenses.html?tab=audit', ['Review changes']);
    }
    if (expenses && cfg.isOwner === true) {
      add('money', 'creditcards', 'Manage credit cards', 'Open the existing Owner-only card management and statement tools.', '/expenses.html?tab=creditcards', ['Manage cards', 'Review statements']);
    }
    // The current Rental page still mounts its tools only for the Owner,
    // even though the registry also retains Prashant's existing page entry.
    if (has('rentals', '/rentals.html') && roles.indexOf('owner') !== -1) {
      add('money', 'rentals', 'Manage rental income', 'Open tenant, rent receipt and deposit workflows already available to you.', '/rentals.html', ['Manage tenants', 'Record rent receipts', 'Review deposits']);
    }
    if (has('sales-orders-logistics', '/sales-orders.html')) {
      add('sales', 'sales-orders', 'Sales orders & logistics', 'Review sales, payment batches, shipments and settlement evidence.', '/sales-orders.html', ['Review orders', 'Review payment evidence', 'Review shipments']);
    }
    if (has('orders', '/orders.html')) {
      add('sales', 'orders', 'Manage orders', 'Use the current Shopify order and dispatch-tracking screen.', '/orders.html', ['Find orders', 'Review order details', 'Track dispatch']);
    }
    // Stylists have catalogue browsing rights, not stock-maintenance rights.
    var inventoryOperator = roles.some(function (role) { return ['owner', 'admin', 'inventory', 'warehouse'].indexOf(role) !== -1; });
    if (has('inventory-dashboard', '/inventory.html') && inventoryOperator) {
      add('inventory', 'inventory', 'Manage inventory', 'Open the current inventory dashboard and its permitted product tools.', '/inventory.html', ['Find products', 'Review stock']);
    }
    if (has('inventory-care', '/inventory-care.html')) {
      add('inventory', 'inventory-care', 'Garment care & miscellaneous', 'Manage cleaning batches, partial returns and pieces not for sale.', '/inventory-care.html', ['Manage cleaning batches', 'Review returns']);
    }
    if (has('procurement', '/procurement.html')) {
      var procurementOperator = roles.some(function (role) { return ['owner', 'admin', 'procurement', 'inventory'].indexOf(role) !== -1; });
      if (procurementOperator) add('inventory', 'purchases', 'Manage purchases', 'Create and review purchase orders using the existing purchasing workflow and permissions.', '/procurement.html?tab=advance', ['Open purchasing', 'Review purchase orders']);
      add('inventory', 'received-stock', procurementOperator ? 'Received stock & corrections' : 'Review received stock', procurementOperator ? 'Open the existing received-stock audit and corrections screen.' : 'Review received purchase records without changing stock or purchase costs.', '/procurement.html?tab=audit', procurementOperator ? ['Review received stock', 'Open permitted corrections'] : ['Review received stock']);
      add('inventory', 'purchase-history', 'Purchase history & invoices', 'Review purchase summaries and existing invoice evidence.', '/procurement.html?tab=summary', ['Review purchase summaries', 'Review invoices']);
    }
    if (has('fresh-proc', '/fresh-procurement.html')) {
      add('inventory', 'fresh-procurement', 'Fresh procurement', 'Use the existing sourcing and vendor PO tools.', '/fresh-procurement.html', ['Manage sourcing batches', 'Review vendor POs']);
    }
    if (has('size-tracker', '/size-tracker.html')) {
      add('inventory', 'size-tracker', 'Manage size charts', 'Match vendor size charts to SANKI target sizes.', '/size-tracker.html', ['Review vendor charts', 'Manage size matching']);
    }
    if (has('salary', '/salary.html')) {
      add('operations', 'salary', 'Manage salary', 'Open the existing payroll and attendance tools; original access checks apply.', '/salary.html', ['Review attendance', 'Open payroll']);
    }
    if (has('incentives', '/incentives.html')) {
      add('operations', 'incentives', 'Manage sales incentives', 'Open the current incentive calculations and approval workflow.', '/incentives.html', ['Review incentives', 'Open existing approvals']);
    }
    if (has('model-calendar', '/model-calendar.html')) {
      add('operations', 'model-calendar', 'Manage model calendar', 'Schedule models and review package and payment links.', '/model-calendar.html', ['Manage schedule', 'Review linked packages']);
    }
    if (has('users', '/admin-users.html')) {
      add('administration', 'users', 'Manage users & roles', 'Use existing user and role-page permission tools.', '/admin-users.html', ['Manage users', 'Manage role permissions']);
    }
    if (has('seo-control', '/seo-control.html')) {
      add('administration', 'seo', 'Manage SEO', 'Open current SEO, AEO and GEO tasks and evidence.', '/seo-control.html', ['Manage SEO tasks', 'Review evidence']);
    }
    if (has('meta-ads', '/meta-ads.html')) {
      add('administration', 'meta-ads', 'Manage Meta Ads', 'Use the existing campaign, creative and change-history tools.', '/meta-ads.html', ['Review campaigns', 'Review change history']);
    }
    if (has('catalogue-images', '/catalogue-images.html')) {
      add('administration', 'catalogue-images', 'Manage catalogue images', 'Use the existing image staging, verification and publishing controls.', '/catalogue-images.html', ['Stage images', 'Review galleries']);
    }
    return heads.filter(function (head) { return head.items.length > 0; });
  }

  return { buildCatalog: buildCatalog };
});
