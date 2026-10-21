'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { commit, recover, JOURNAL } = require('../modules/bank-authority-transaction');

const EXPENSES = 'expenses.json', CARDS = 'credit-cards.json';
const originalExpenses = JSON.stringify({ transfers: [{ id: 'TR-00136', amount: 26701 }], history: ['preserved'] });
const updatedExpenses = JSON.stringify({ transfers: [{ id: 'TR-00136', amount: 26700.63 }], history: ['preserved'] });
const originalCards = JSON.stringify({ payments: [{ id: 'CCP-1', amount: 26701 }], statements: { finalized: true } });
const updatedCards = JSON.stringify({ payments: [{ id: 'CCP-1', amount: 26700.63 }], statements: { finalized: true } });

function fixture(t, { cards = true } = {}) {
  // The Windows managed sandbox virtualizes os.tmpdir() with restricted rename
  // rights; use the explicitly writable checkout for real filesystem testing.
  const directory = fs.mkdtempSync(path.join(__dirname, '..', '.bank-authority-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  fs.writeFileSync(path.join(directory, EXPENSES), originalExpenses);
  if (cards) fs.writeFileSync(path.join(directory, CARDS), originalCards);
  return directory;
}
function entries({ cardsBefore = originalCards } = {}) {
  return [
    { fileName: EXPENSES, before: originalExpenses, after: updatedExpenses },
    { fileName: CARDS, before: cardsBefore, after: updatedCards }
  ];
}
function read(directory, file) { return fs.readFileSync(path.join(directory, file), 'utf8'); }
function journal(directory) { return JSON.parse(read(directory, JOURNAL)); }
function files(directory) { return fs.readdirSync(directory).sort(); }
function crash(phase, fileName) {
  return { onPhase(current, detail) {
    if (current === phase && (!fileName || detail === fileName)) {
      const failure = new Error('Simulated process interruption'); failure.simulateCrash = true; throw failure;
    }
  } };
}
function interrupted(directory, phase = 'renamed_file', fileName = EXPENSES, input = entries()) {
  assert.throws(() => commit(directory, input, crash(phase, phase === 'renamed_file' ? fileName : undefined)), /Simulated process interruption/);
  assert.ok(fs.existsSync(path.join(directory, JOURNAL)));
}
function renameIO(callback) {
  return { ...fs, renameSync(from, to) { return callback(from, to, fs.renameSync); } };
}
function assertOriginal(directory, { cards = true } = {}) {
  assert.equal(read(directory, EXPENSES), originalExpenses);
  if (cards) assert.equal(read(directory, CARDS), originalCards);
  else assert.equal(fs.existsSync(path.join(directory, CARDS)), false);
}
function assertUpdated(directory) {
  assert.equal(read(directory, EXPENSES), updatedExpenses);
  assert.equal(read(directory, CARDS), updatedCards);
}

test('commits both linked accounting stores and removes all private transaction artifacts', t => {
  const directory = fixture(t);
  const result = commit(directory, entries());
  assert.deepEqual(result, { committed: true, files: [EXPENSES, CARDS] });
  assertUpdated(directory);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
  assert.deepEqual(recover(directory), { recovered: false });
});

test('single-store updates are allowed without touching the other store', t => {
  const directory = fixture(t);
  commit(directory, entries().slice(0, 1));
  assert.equal(read(directory, EXPENSES), updatedExpenses);
  assert.equal(read(directory, CARDS), originalCards);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('unchanged snapshots are checked for staleness but create no journal or writes', t => {
  const directory = fixture(t);
  const result = commit(directory, [{ fileName: EXPENSES, before: originalExpenses, after: originalExpenses }]);
  assert.deepEqual(result, { committed: false, unchanged: true, files: [] });
  assertOriginal(directory);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('filters unchanged members out of a two-store bundle', t => {
  const directory = fixture(t);
  const input = entries(); input[0].after = input[0].before;
  const result = commit(directory, input);
  assert.deepEqual(result.files, [CARDS]);
  assert.equal(read(directory, EXPENSES), originalExpenses);
  assert.equal(read(directory, CARDS), updatedCards);
});

test('a stale second snapshot leaves both stores and all financial history untouched', t => {
  const directory = fixture(t), changed = JSON.stringify({ payments: [], reason: 'independent write' });
  fs.writeFileSync(path.join(directory, CARDS), changed);
  assert.throws(() => commit(directory, entries()), /changed after review/);
  assert.equal(read(directory, EXPENSES), originalExpenses);
  assert.equal(read(directory, CARDS), changed);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('raw snapshots include formatting: logically equal but different bytes are stale', t => {
  const directory = fixture(t);
  fs.writeFileSync(path.join(directory, EXPENSES), JSON.stringify(JSON.parse(originalExpenses), null, 2));
  assert.throws(() => commit(directory, entries()), /changed after review/);
  assert.equal(read(directory, CARDS), originalCards);
});

for (const fileName of ['../expenses.json', '/expenses.json', 'C:\\expenses.json', 'audit.json', 'credit-cards.json/../expenses.json']) {
  test('rejects unapproved target path ' + fileName, t => {
    const directory = fixture(t);
    assert.throws(() => commit(directory, [{ fileName, before: originalExpenses, after: updatedExpenses }]), /Only unique/);
    assertOriginal(directory);
    assert.deepEqual(files(directory), [CARDS, EXPENSES]);
  });
}

test('rejects relative directories, duplicate targets and invalid bundle sizes', t => {
  const directory = fixture(t);
  assert.throws(() => commit('.', entries()), /exact absolute/);
  assert.throws(() => recover('.'), /exact absolute/);
  assert.throws(() => commit(directory, [entries()[0], entries()[0]]), /Only unique/);
  for (const input of [[], null, [...entries(), entries()[0]]]) assert.throws(() => commit(directory, input), /one or both/);
  assertOriginal(directory);
});

for (const invalid of ['not JSON', '[]', 'null', '42']) {
  test('rejects invalid accounting object ' + invalid + ' without writes', t => {
    const directory = fixture(t), input = entries(); input[1].after = invalid;
    assert.throws(() => commit(directory, input), /JSON/);
    assertOriginal(directory);
    assert.deepEqual(files(directory), [CARDS, EXPENSES]);
  });
}

test('all snapshots and backups are durable and complete before the first financial rename', t => {
  const directory = fixture(t);
  commit(directory, entries(), { onPhase(phase, data) {
    if (phase !== 'prepared') return;
    assertOriginal(directory);
    assert.equal(data.entries.length, 2);
    for (const row of data.entries) {
      assert.equal(read(directory, row.stage), row.fileName === EXPENSES ? updatedExpenses : updatedCards);
      assert.equal(read(directory, row.backup), row.fileName === EXPENSES ? originalExpenses : originalCards);
    }
  } });
  assertUpdated(directory);
});

test('exclusive journal fallback supports hosts which forbid hard links', t => {
  const directory = fixture(t);
  const io = { ...fs, linkSync() { const failure = new Error('Hard links unavailable'); failure.code = 'EPERM'; throw failure; } };
  commit(directory, entries(), { io });
  assertUpdated(directory);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('a truncated fallback journal fails closed while both original financial stores stay unchanged', t => {
  const directory = fixture(t), descriptors = new Map();
  const io = { ...fs,
    linkSync() { const failure = new Error('Hard links unavailable'); failure.code = 'EPERM'; throw failure; },
    openSync(file, ...args) { const descriptor = fs.openSync(file, ...args); descriptors.set(descriptor, file); return descriptor; },
    writeFileSync(file, contents, ...args) {
      if (typeof file === 'number' && path.basename(descriptors.get(file) || '') === JOURNAL) {
        fs.writeFileSync(file, contents.slice(0, 20), ...args);
        const failure = new Error('Process stopped during journal preparation'); failure.simulateCrash = true; throw failure;
      }
      return fs.writeFileSync(file, contents, ...args);
    }
  };
  assert.throws(() => commit(directory, entries(), { io }), /stopped during journal preparation/);
  assertOriginal(directory);
  assert.throws(() => recover(directory), /valid JSON/);
  assertOriginal(directory);
  assert.ok(fs.existsSync(path.join(directory, JOURNAL)));
});

test('exclusive publication never overwrites a competing recovery journal', t => {
  const directory = fixture(t), competing = '{"external":"transaction"}';
  const io = { ...fs, linkSync(from, to) {
    fs.writeFileSync(to, competing, { flag: 'wx' });
    const failure = new Error('Journal already exists'); failure.code = 'EEXIST'; throw failure;
  } };
  assert.throws(() => commit(directory, entries(), { io }), /no partial correction/);
  assert.equal(read(directory, JOURNAL), competing);
  assertOriginal(directory);
  assert.deepEqual(files(directory), [JOURNAL, CARDS, EXPENSES].sort());
});

test('a financial file changed after preparation is never overwritten', t => {
  const directory = fixture(t), changed = JSON.stringify({ payments: ['changed during prepare'] });
  assert.throws(() => commit(directory, entries(), { onPhase(phase) {
    if (phase === 'prepared') fs.writeFileSync(path.join(directory, CARDS), changed);
  } }), /needs recovery/);
  assert.equal(read(directory, EXPENSES), originalExpenses);
  assert.equal(read(directory, CARDS), changed);
  assert.ok(fs.existsSync(path.join(directory, JOURNAL)));
});

test('a backup preparation failure occurs before either financial file is renamed', t => {
  const directory = fixture(t);
  const io = { ...fs, openSync(file, ...args) {
    if (typeof file === 'string' && file.endsWith('-credit-cards.json.before')) throw new Error('Backup creation refused');
    return fs.openSync(file, ...args);
  } };
  assert.throws(() => commit(directory, entries(), { io }), /no partial correction/);
  assertOriginal(directory);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('a second financial rename failure rolls the first store back exactly', t => {
  const directory = fixture(t);
  const io = renameIO((from, to, native) => {
    if (path.basename(to) === CARDS && from.endsWith('.after')) throw new Error('Injected second rename failure');
    return native(from, to);
  });
  assert.throws(() => commit(directory, entries(), { io }), /no partial correction/);
  assertOriginal(directory);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('a first financial rename failure does not alter either store', t => {
  const directory = fixture(t);
  const io = renameIO((from, to, native) => {
    if (from.endsWith('.after')) throw new Error('Injected first rename failure');
    return native(from, to);
  });
  assert.throws(() => commit(directory, entries(), { io }), /no partial correction/);
  assertOriginal(directory);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

for (const [phase, fileName] of [['prepared', undefined], ['renamed_file', EXPENSES], ['renamed_file', CARDS], ['committed', undefined]]) {
  test('recovers an interruption after ' + phase + (fileName ? ' ' + fileName : '') + ' exactly once', t => {
    const directory = fixture(t);
    interrupted(directory, phase, fileName);
    assert.deepEqual(recover(directory), { recovered: true, outcome: 'committed', files: [EXPENSES, CARDS] });
    assertUpdated(directory);
    assert.deepEqual(files(directory), [CARDS, EXPENSES]);
    assert.deepEqual(recover(directory), { recovered: false });
  });
}

test('recovering itself can be interrupted between files without losing recoverability', t => {
  const directory = fixture(t);
  interrupted(directory, 'prepared', undefined);
  assert.throws(() => recover(directory, crash('recovered_file', EXPENSES)), /Simulated process interruption/);
  assert.equal(read(directory, EXPENSES), updatedExpenses);
  assert.equal(read(directory, CARDS), originalCards);
  assert.equal(recover(directory).outcome, 'committed');
  assertUpdated(directory);
});

test('recover rolls back both stores if a remaining after-snapshot is missing', t => {
  const directory = fixture(t);
  interrupted(directory);
  fs.unlinkSync(path.join(directory, journal(directory).entries.find(row => row.fileName === CARDS).stage));
  assert.equal(recover(directory).outcome, 'rolled_back');
  assertOriginal(directory);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('recover does not complete or roll back over an independent financial write', t => {
  const directory = fixture(t);
  interrupted(directory);
  const changed = JSON.stringify({ payments: [{ id: 'NEW' }] });
  fs.writeFileSync(path.join(directory, CARDS), changed);
  assert.throws(() => recover(directory), /changed outside/);
  assert.equal(read(directory, EXPENSES), updatedExpenses);
  assert.equal(read(directory, CARDS), changed);
  assert.ok(fs.existsSync(path.join(directory, JOURNAL)));
});

test('an independent write between the two renames is preserved and cannot be silently loaded as blank', t => {
  const directory = fixture(t), changed = JSON.stringify({ payments: [{ id: 'CCP-INDEPENDENT', amount: 700 }], statements: { finalized: true } });
  assert.throws(() => commit(directory, entries(), { onPhase(phase, fileName) {
    if (phase === 'renamed_file' && fileName === EXPENSES) fs.writeFileSync(path.join(directory, CARDS), changed);
  } }), /needs recovery/);
  assert.throws(() => recover(directory), /changed outside/);
  assert.deepEqual(JSON.parse(read(directory, EXPENSES)), JSON.parse(updatedExpenses));
  assert.deepEqual(JSON.parse(read(directory, CARDS)), JSON.parse(changed));
  assert.ok(fs.existsSync(path.join(directory, JOURNAL)));
});

for (const artifact of ['stage', 'backup']) {
  test('recover fails closed on a corrupted ' + artifact + ' snapshot', t => {
    const directory = fixture(t);
    interrupted(directory, 'prepared', undefined);
    fs.writeFileSync(path.join(directory, journal(directory).entries[0][artifact]), '{"corrupted":true}');
    assert.throws(() => recover(directory), /snapshot changed/);
    assertOriginal(directory);
    assert.ok(fs.existsSync(path.join(directory, JOURNAL)));
  });
}

test('recover refuses rollback if the needed original snapshot was lost', t => {
  const directory = fixture(t);
  interrupted(directory);
  const rows = journal(directory).entries;
  fs.unlinkSync(path.join(directory, rows.find(row => row.fileName === CARDS).stage));
  fs.unlinkSync(path.join(directory, rows.find(row => row.fileName === EXPENSES).backup));
  assert.throws(() => recover(directory), /original accounting snapshot is missing/);
  assert.equal(read(directory, EXPENSES), updatedExpenses);
  assert.equal(read(directory, CARDS), originalCards);
});

for (const mutation of [
  value => { value.directory = path.dirname(value.directory); },
  value => { value.entries[0].fileName = '../expenses.json'; },
  value => { value.entries[0].stage = '../outside.after'; },
  value => { value.entries[0].backup = path.join(os.tmpdir(), 'outside.before'); },
  value => { value.entries[1].fileName = EXPENSES; },
  value => { value.entries[0].afterHash = 'invalid'; },
  value => { value.id = '../unsafe'; }
]) {
  test('rejects a tampered recovery journal without altering financial stores: ' + mutation.toString(), t => {
    const directory = fixture(t);
    interrupted(directory, 'prepared', undefined);
    const value = journal(directory); mutation(value);
    fs.writeFileSync(path.join(directory, JOURNAL), JSON.stringify(value));
    assert.throws(() => recover(directory), /journal/);
    assertOriginal(directory);
    assert.ok(fs.existsSync(path.join(directory, JOURNAL)));
  });
}

test('invalid journal JSON is never swallowed or replaced by a fresh commit', t => {
  const directory = fixture(t);
  fs.writeFileSync(path.join(directory, JOURNAL), '{unreadable');
  assert.throws(() => recover(directory), /valid JSON/);
  assert.throws(() => commit(directory, entries()), /valid JSON/);
  assert.equal(read(directory, JOURNAL), '{unreadable');
  assertOriginal(directory);
});

test('a recovery rename failure rolls back the entire bundle', t => {
  const directory = fixture(t);
  interrupted(directory, 'prepared', undefined);
  const io = renameIO((from, to, native) => {
    if (path.basename(to) === CARDS && from.endsWith('.after')) throw new Error('Injected recovery rename failure');
    return native(from, to);
  });
  assert.throws(() => recover(directory, { io }), /failed and was rolled back/);
  assertOriginal(directory);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('a failed rollback leaves a recoverable journal rather than pretending success', t => {
  const directory = fixture(t);
  let refusedRollback = false;
  const io = renameIO((from, to, native) => {
    if (path.basename(to) === CARDS && from.endsWith('.after')) throw new Error('Commit rename failed');
    if (path.basename(to) === EXPENSES && from.endsWith('.before')) { refusedRollback = true; throw new Error('Rollback rename failed'); }
    return native(from, to);
  });
  assert.throws(() => commit(directory, entries(), { io }), /needs recovery/);
  assert.equal(refusedRollback, true);
  assert.ok(fs.existsSync(path.join(directory, JOURNAL)));
  assert.equal(recover(directory).outcome, 'committed');
  assertUpdated(directory);
});

test('creating a previously absent approved store can commit and recover', t => {
  const directory = fixture(t, { cards: false });
  interrupted(directory, 'renamed_file', EXPENSES, entries({ cardsBefore: null }));
  assert.equal(recover(directory).outcome, 'committed');
  assertUpdated(directory);
});

test('rollback removes a newly created store only when it still equals the verified after-snapshot', t => {
  const directory = fixture(t, { cards: false });
  const input = entries({ cardsBefore: null }).reverse();
  const io = renameIO((from, to, native) => {
    if (path.basename(to) === EXPENSES && from.endsWith('.after')) throw new Error('Injected failure after new store creation');
    return native(from, to);
  });
  assert.throws(() => commit(directory, input, { io }), /no partial correction/);
  assertOriginal(directory, { cards: false });
  assert.deepEqual(files(directory), [EXPENSES]);
});

test('before=null does not overwrite an existing store', t => {
  const directory = fixture(t);
  assert.throws(() => commit(directory, entries({ cardsBefore: null })), /changed after review/);
  assertOriginal(directory);
});

test('cleanup failure retains a fully committed, recoverable journal without unsafe rollback', t => {
  const directory = fixture(t);
  const io = { ...fs, unlinkSync(file) {
    if (file.endsWith('.before')) throw new Error('Injected backup cleanup failure');
    return fs.unlinkSync(file);
  } };
  const result = commit(directory, entries(), { io });
  assert.equal(result.committed, true);
  assert.equal(result.cleanupPending, true);
  assertUpdated(directory);
  assert.ok(fs.existsSync(path.join(directory, JOURNAL)));
  assert.equal(recover(directory).outcome, 'committed');
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('a stale new commit cannot undo a recovered previous commit', t => {
  const directory = fixture(t);
  interrupted(directory);
  assert.throws(() => commit(directory, entries()), /changed after review/);
  assertUpdated(directory);
  assert.deepEqual(files(directory), [CARDS, EXPENSES]);
});

test('rejects non-regular financial targets and non-regular journal paths', t => {
  const directory = fixture(t);
  fs.unlinkSync(path.join(directory, CARDS)); fs.mkdirSync(path.join(directory, CARDS));
  assert.throws(() => commit(directory, entries()), /not a regular file/);
  assert.equal(read(directory, EXPENSES), originalExpenses);
  fs.rmdirSync(path.join(directory, CARDS)); fs.mkdirSync(path.join(directory, JOURNAL));
  assert.throws(() => recover(directory), /not a regular file/);
});

test('rejects symbolic-link financial targets without writing their referents', t => {
  const directory = fixture(t), referent = path.join(directory, 'outside.json');
  fs.writeFileSync(referent, originalCards); fs.unlinkSync(path.join(directory, CARDS));
  let io = fs;
  try { fs.symlinkSync(referent, path.join(directory, CARDS), 'file'); }
  catch (cause) {
    if (!['EPERM', 'EACCES', 'ENOTSUP'].includes(cause.code)) throw cause;
    // Exercise the security check even when this Windows host cannot create
    // links. No mock writes are permitted; only lstat's link marker is injected.
    io = { ...fs, lstatSync(file) {
      if (path.basename(file) === CARDS) return { isSymbolicLink: () => true, isFile: () => true };
      return fs.lstatSync(file);
    } };
  }
  assert.throws(() => commit(directory, entries(), { io }), /not a regular file/);
  assert.equal(fs.readFileSync(referent, 'utf8'), originalCards);
  assert.equal(read(directory, EXPENSES), originalExpenses);
});
