const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-proc-overview-'));
process.env.PROCUREMENT_PATH = path.join(dir, 'procurement.json');
process.env.CASUALS_PATH = path.join(dir, 'casuals.json');
process.env.DATA_PATH = path.join(dir, 'orders.json');

const { casualsOverview, settingsWithDefaults, router } = require('../modules/casuals');

test('Casuals overview nets only advance Purchases linked to the named batch', () => {
  const settings = settingsWithDefaults({ settings: {} });
  settings.categories.Trouser.sizeMode = 'designs';
  settings.categories.Trouser.designs = 10;
  settings.categories.Trouser.avgCost = 800;
  settings.categories.Trouser.budgetOverride = 90000;
  settings.categories.Trouser.sizeSystem = 'numeric';
  settings.categories.Trouser.sizes = { 26:1, 28:1, 30:1, 32:1, 34:1, 36:1 };
  settings.categories.Trouser.colours = { Black:40, White:20, Brown:15, Beige:15, Olive:10 };

  fs.writeFileSync(process.env.PROCUREMENT_PATH, JSON.stringify({ settings: { exRate: 15, freightPerGram: 0.42 }, pos: {
    'PO-0001': { id:'PO-0001', status:'advance', line:'casuals', sourceBatchId:'b-wide', origin:'china', exRate:15,
      vendor:'VENDOR A', lines:[
        { designName:'6910', productType:'Trouser', audience:'Women', fit:'Wide Leg', colour:'Black', sizeLabel:'M', qty:12, perPcsYuan:50 },
        { designName:'6910', productType:'Trouser', audience:'Women', fit:'Wide Leg', colour:'White', sizeLabel:'L', qty:8, perPcsYuan:50 }
      ] },
    'PO-0002': { id:'PO-0002', status:'received', line:'casuals', sourceBatchId:'b-wide', origin:'china', exRate:15,
      vendor:'VENDOR A', lines:[{ productType:'Trouser', audience:'Women', fit:'Wide Leg', colour:'Black', qty:100, perPcsYuan:50 }] },
    'PO-0003': { id:'PO-0003', status:'advance', line:'funky', origin:'china', exRate:15,
      vendor:'VENDOR B', lines:[{ productType:'Trouser', qty:100, perPcsYuan:50 }] }
  }}));

  const overview = casualsOverview({
    settings, activeBatch:'b-wide',
    batches:[{ id:'b-wide', num:1, name:'Women Wide-Leg Trousers', audience:'Women', type:'Wide Leg', categories:['Trouser'], category:'Trouser', planSettings:settings }],
    candidates:[{ id:'c1', batch:'b-wide', category:'Trouser', colour:'Black', vendor:'VENDOR A', designName:'6910' }]
  });
  assert.equal(overview.rows[0].name, 'Women Wide-Leg Trousers');
  assert.equal(overview.rows[0].designs, 10);
  assert.equal(overview.rows[0].pieces, 60);
  assert.equal(overview.rows[0].budget, 90000);
  assert.equal(overview.rows[0].onWayCost, 15000);
  assert.equal(overview.rows[0].onWayDesigns, 1);
  assert.equal(overview.rows[0].remainingDesigns, 9);
  assert.equal(overview.rows[0].remainingPieces, 40);
  assert.equal(overview.rows[0].colours, 5);
  assert.equal(overview.rows[0].remaining, 75000);
  assert.equal(overview.totals.onWayPieces, 20);
  assert.equal(overview.categories[0].designs, 10);
  assert.equal(overview.categories[0].pieces, 60);
  assert.equal(overview.categories[0].onWayDesigns, 1);
  assert.equal(overview.categories[0].remainingDesigns, 9);
  assert.equal(overview.categories[0].remainingPieces, 40);
});

test('Funky uses the same named-batch overview while remaining data-separated', () => {
  const settings = settingsWithDefaults({ settings: {} });
  settings.categories.Shirt.enabled = true;
  settings.categories.Shirt.sizeMode = 'designs';
  settings.categories.Shirt.designs = 5;
  const common = { settings, planSettings:settings, categories:['Shirt'], category:'Shirt', audience:'Women', type:'Oversized' };
  const overview = casualsOverview({
    settings, activeBatch:'f1',
    batches:[
      { ...common, id:'f1', num:1, name:'Funky Oversized', categoryName:'Tops', line:'funky' },
      { ...common, id:'c1', num:2, name:'Casual Oversized', categoryName:'Shirts', line:'casuals' }
    ],
    candidates:[{ id:'x', batch:'f1', category:'Shirt', colour:'Black', designName:'F1' }]
  }, 'funky');
  assert.equal(overview.line, 'funky');
  assert.equal(overview.rows.length, 1);
  assert.equal(overview.rows[0].name, 'Funky Oversized');
  assert.equal(overview.rows[0].category, 'Tops');
});

test('Fresh Procurement UI has the summary, named batch fields and exact PO bridge', () => {
  const fresh = fs.readFileSync(path.join(__dirname, '..', 'public', 'fresh-procurement.html'), 'utf8');
  const purchases = fs.readFileSync(path.join(__dirname, '..', 'public', 'procurement.html'), 'utf8');
  const procurement = fs.readFileSync(path.join(__dirname, '..', 'modules', 'procurement.js'), 'utf8');
  const casuals = fs.readFileSync(path.join(__dirname, '..', 'modules', 'casuals.js'), 'utf8');
  assert.match(fresh, /id="procOverview"/);
  assert.match(fresh, /id="czBatchName"/);
  assert.match(fresh, /id="czBatchAudience"/);
  assert.match(fresh, /id="czBatchType"/);
  assert.match(fresh, /\/api\/casuals\/overview/);
  assert.match(fresh, /Received and posted purchases are excluded/);
  assert.match(purchases, /id="b_sourceBatch"/);
  assert.match(purchases, /sourceBatchId:el\('b_sourceBatch'\)\.value/);
  assert.match(procurement, /sourceBatchId: normLine\(b\.line\) \?/);
  assert.match(fresh, /\/api\/casuals\/overview\?line=/);
  assert.match(fresh, /categoryName:categoryName/);
  assert.match(fresh, /line:czLine/);
  assert.match(fresh, /data-ov-budget/);
  assert.match(fresh, /remainingDesigns/);
  assert.match(fresh, /ovFilteredUnlinked/);
  assert.match(casuals, /budgetOverride = share/);
  const routes = router.stack.filter(layer => layer.route).map(layer => ({ path:layer.route.path, methods:layer.route.methods }));
  assert.ok(routes.some(r => r.path === '/api/casuals/overview' && r.methods.get));
  assert.ok(routes.some(r => r.path === '/api/casuals/batches/:id' && r.methods.patch));
});
