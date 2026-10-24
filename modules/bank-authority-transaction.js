'use strict';

// Synchronous crash-recoverable bundle for the two private accounting stores.
// Readers must call recover() before loading either store. There is no import
// side effect, network access, generic path API or recursive filesystem action.
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const JOURNAL = '.bank-authority-transaction.json';
const ALLOWED = new Set(['expenses.json', 'credit-cards.json']);
const sha = value => crypto.createHash('sha256').update(value).digest('hex');
function error(message, cause) { const result = new Error(message, cause ? { cause } : undefined); result.code = 'BANK_AUTHORITY_TRANSACTION'; result.recoveryRequired = true; return result; }
function ioFor(options) { return options.io || fs; }
function directoryFor(directory, io) {
  if (typeof directory !== 'string' || !path.isAbsolute(directory)) throw error('Use the exact absolute accounting directory.');
  const resolved = io.realpathSync(path.resolve(directory));
  if (!io.statSync(resolved).isDirectory()) throw error('The accounting directory is unavailable.');
  return resolved;
}
function regular(io, file) {
  try { const stat = io.lstatSync(file); if (stat.isSymbolicLink() || !stat.isFile()) throw error('An accounting transaction path is not a regular file.'); return true; }
  catch (cause) { if (cause.code === 'ENOENT') return false; throw cause; }
}
function raw(io, file) { return regular(io, file) ? io.readFileSync(file, 'utf8') : null; }
function digest(io, file) { const value = raw(io, file); return value == null ? null : sha(value); }
function jsonObject(value, label) {
  let parsed; try { parsed = JSON.parse(value); } catch (cause) { throw error(label + ' must contain valid JSON; no financial file was changed.', cause); }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw error(label + ' must contain a JSON object.');
  return parsed;
}
function durableWrite(io, file, contents, opened) {
  const descriptor = io.openSync(file, 'wx', 0o600);
  try { if (opened) opened(); io.writeFileSync(descriptor, contents, 'utf8'); io.fsyncSync(descriptor); } finally { io.closeSync(descriptor); }
}
function syncDirectory(io, directory) {
  let descriptor;
  try { descriptor = io.openSync(directory, 'r'); io.fsyncSync(descriptor); }
  catch (cause) { if (!['EISDIR', 'EINVAL', 'EPERM', 'EACCES', 'ENOTSUP', 'EBADF'].includes(cause.code)) throw cause; }
  finally { if (descriptor !== undefined) io.closeSync(descriptor); }
}
function artifactNames(id, fileName) { return { stage: '.bank-authority-' + id + '-' + fileName + '.after', backup: '.bank-authority-' + id + '-' + fileName + '.before' }; }
function validateJournal(journal, directory) {
  if (!journal || journal.version !== 1 || !/^[a-f0-9]{24}$/.test(journal.id) || journal.directory !== directory || !Array.isArray(journal.entries) || !journal.entries.length || journal.entries.length > 2) throw error('The bank accounting recovery journal is invalid. Do not overwrite it.');
  const used = new Set();
  for (const entry of journal.entries) {
    if (!entry || !ALLOWED.has(entry.fileName) || used.has(entry.fileName) || entry.beforeHash !== null && !/^[a-f0-9]{64}$/.test(entry.beforeHash) || !/^[a-f0-9]{64}$/.test(entry.afterHash)) throw error('The bank accounting recovery journal has an invalid target or fingerprint.');
    const names = artifactNames(journal.id, entry.fileName);
    if (entry.stage !== names.stage || entry.backup !== names.backup) throw error('The bank accounting recovery journal contains an unsafe artifact path.');
    used.add(entry.fileName);
  }
  return journal;
}
function inspect(io, directory, journal) {
  return journal.entries.map(entry => {
    const target = path.join(directory, entry.fileName), stage = path.join(directory, entry.stage), backup = path.join(directory, entry.backup);
    const actual = digest(io, target), staged = digest(io, stage), backed = digest(io, backup);
    if (actual !== entry.beforeHash && actual !== entry.afterHash) throw error(entry.fileName + ' changed outside the pending bank transaction. Recovery stopped without overwriting it.');
    if (staged !== null && staged !== entry.afterHash || backed !== null && backed !== entry.beforeHash) throw error('A staged accounting snapshot changed. Recovery stopped without overwriting financial files.');
    return { ...entry, target, stage, backup, actual, staged, backed, state: actual === entry.afterHash ? 'after' : 'before' };
  });
}
function removeVerified(io, file, expected) {
  const actual = digest(io, file);
  if (actual === null) return;
  if (actual !== expected) throw error('An accounting transaction artifact changed; it has been preserved for review.');
  io.unlinkSync(file);
}
function cleanup(io, directory, journal) {
  for (const entry of journal.entries) {
    removeVerified(io, path.join(directory, entry.stage), entry.afterHash);
    removeVerified(io, path.join(directory, entry.backup), entry.beforeHash);
  }
  // The journal is removed last: a cleanup interruption remains recoverable.
  const journalPath = path.join(directory, JOURNAL), current = raw(io, journalPath);
  if (current !== null && sha(current) !== sha(JSON.stringify(journal))) throw error('The accounting recovery journal changed during cleanup.');
  if (current !== null) io.unlinkSync(journalPath);
  syncDirectory(io, directory);
}
function rollback(io, directory, journal) {
  const inspected = inspect(io, directory, journal);
  for (const entry of inspected) if (entry.state === 'after' && entry.beforeHash !== null && entry.backed !== entry.beforeHash) throw error('An original accounting snapshot is missing. Automatic rollback is unsafe.');
  for (const entry of inspected.slice().reverse()) {
    if (entry.state !== 'after') continue;
    if (digest(io, entry.target) !== entry.afterHash) throw error('An accounting file changed during rollback. Nothing was overwritten.');
    if (entry.beforeHash === null) io.unlinkSync(entry.target);
    else io.renameSync(entry.backup, entry.target);
  }
  if (inspect(io, directory, journal).some(entry => entry.actual !== entry.beforeHash)) throw error('The accounting rollback could not be verified.');
  cleanup(io, directory, journal);
}
function checkpoint(options, phase, detail) { if (typeof options.onPhase === 'function') options.onPhase(phase, detail); }
function recover(directory, options = {}) {
  const io = ioFor(options), root = directoryFor(directory, io), journalPath = path.join(root, JOURNAL), contents = raw(io, journalPath);
  if (contents === null) return { recovered: false };
  const journal = validateJournal(jsonObject(contents, 'The accounting recovery journal'), root), entries = inspect(io, root, journal);
  if (entries.every(entry => entry.state === 'after')) { cleanup(io, root, journal); return { recovered: true, outcome: 'committed', files: journal.entries.map(entry => entry.fileName) }; }
  const canComplete = entries.filter(entry => entry.state === 'before').every(entry => entry.staged === entry.afterHash);
  if (!canComplete) { rollback(io, root, journal); return { recovered: true, outcome: 'rolled_back', files: journal.entries.map(entry => entry.fileName) }; }
  try {
    for (const entry of entries) {
      if (entry.state !== 'before') continue;
      if (digest(io, entry.target) !== entry.beforeHash) throw error('An accounting file changed while recovering the bank transaction.');
      io.renameSync(entry.stage, entry.target); checkpoint(options, 'recovered_file', entry.fileName);
    }
    if (inspect(io, root, journal).some(entry => entry.state !== 'after')) throw error('The recovered accounting transaction could not be verified.');
    syncDirectory(io, root);
  } catch (cause) {
    if (cause.simulateCrash) throw cause;
    try { rollback(io, root, journal); } catch (rollbackCause) { throw error('Accounting recovery needs review; the journal and verified snapshots were retained.', rollbackCause); }
    throw error('Accounting recovery failed and was rolled back.', cause);
  }
  cleanup(io, root, journal);
  return { recovered: true, outcome: 'committed', files: journal.entries.map(entry => entry.fileName) };
}
function commit(directory, supplied, options = {}) {
  const io = ioFor(options), root = directoryFor(directory, io);
  recover(root, options);
  if (!Array.isArray(supplied) || !supplied.length || supplied.length > 2) throw error('Supply one or both approved accounting files.');
  const used = new Set(), entries = supplied.map(entry => {
    if (!entry || !ALLOWED.has(entry.fileName) || used.has(entry.fileName) || entry.before !== null && typeof entry.before !== 'string' || typeof entry.after !== 'string') throw error('Only unique expenses.json and credit-cards.json snapshots are allowed.');
    used.add(entry.fileName); if (entry.before !== null) jsonObject(entry.before, 'The original ' + entry.fileName); jsonObject(entry.after, 'The updated ' + entry.fileName);
    if (raw(io, path.join(root, entry.fileName)) !== entry.before) throw error(entry.fileName + ' changed after review. Reload it before applying the bank correction.');
    return { fileName: entry.fileName, before: entry.before, after: entry.after, beforeHash: entry.before === null ? null : sha(entry.before), afterHash: sha(entry.after) };
  }).filter(entry => entry.before !== entry.after);
  if (!entries.length) return { committed: false, unchanged: true, files: [] };
  const id = crypto.randomBytes(12).toString('hex'), journal = { version: 1, id, directory: root, entries: entries.map(entry => ({ fileName: entry.fileName, beforeHash: entry.beforeHash, afterHash: entry.afterHash, ...artifactNames(id, entry.fileName) })) };
  const journalPath = path.join(root, JOURNAL), journalStage = path.join(root, '.bank-authority-' + id + '.journal');
  let published = false, allAfter = false;
  try {
    for (let index = 0; index < entries.length; index++) {
      const entry = entries[index], names = journal.entries[index];
      durableWrite(io, path.join(root, names.stage), entry.after);
      if (entry.before !== null) durableWrite(io, path.join(root, names.backup), entry.before);
    }
    durableWrite(io, journalStage, JSON.stringify(journal));
    // Prefer atomic exclusive publication. Some Windows hosts prohibit links;
    // their exclusive durable-write fallback still publishes the ENTIRE journal
    // before touching financial files. An interruption while writing it fails
    // closed on recovery, with both original financial stores untouched.
    try { io.linkSync(journalStage, journalPath); published = true; }
    catch (cause) {
      if (!['EPERM', 'EACCES', 'ENOTSUP', 'EOPNOTSUPP', 'EXDEV'].includes(cause.code)) throw cause;
      durableWrite(io, journalPath, JSON.stringify(journal), () => { published = true; });
    }
    io.unlinkSync(journalStage); syncDirectory(io, root);
    checkpoint(options, 'prepared', journal);
    const inspected = inspect(io, root, journal);
    for (const entry of inspected) {
      if (digest(io, entry.target) !== entry.beforeHash) throw error('An accounting file changed before bank transaction commit.');
      io.renameSync(entry.stage, entry.target); checkpoint(options, 'renamed_file', entry.fileName);
    }
    if (inspect(io, root, journal).some(entry => entry.state !== 'after')) throw error('The accounting transaction could not be verified.');
    syncDirectory(io, root); allAfter = true; checkpoint(options, 'committed', journal);
  } catch (cause) {
    if (cause.simulateCrash) throw cause;
    if (published && !allAfter) {
      try { rollback(io, root, journal); } catch (rollbackCause) { throw error('Accounting commit needs recovery; the journal and snapshots were retained without overwriting changed data.', rollbackCause); }
    } else if (!published) {
      // Only our verified, uniquely named artifacts may be removed here.
      for (const entry of journal.entries) { removeVerified(io, path.join(root, entry.stage), entry.afterHash); removeVerified(io, path.join(root, entry.backup), entry.beforeHash); }
      removeVerified(io, journalStage, sha(JSON.stringify(journal)));
    }
    if (allAfter) return { committed: true, cleanupPending: true, files: entries.map(entry => entry.fileName) };
    throw error('Accounting commit failed; no partial correction was retained.', cause);
  }
  try { cleanup(io, root, journal); }
  catch { return { committed: true, cleanupPending: true, files: entries.map(entry => entry.fileName) }; }
  return { committed: true, files: entries.map(entry => entry.fileName) };
}

module.exports = { commit, recover, JOURNAL };
