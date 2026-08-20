'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const publicDir = path.join(__dirname, '..', 'public');

test('PWA manifest is valid and all declared icons exist', () => {
  const manifest = JSON.parse(fs.readFileSync(path.join(publicDir, 'manifest.webmanifest'), 'utf8'));
  assert.equal(manifest.name, 'SANKI Business OS');
  assert.equal(manifest.display, 'standalone');
  assert.equal(manifest.start_url, '/dashboard.html');
  assert.ok(Array.isArray(manifest.icons) && manifest.icons.length >= 2);
  for (const icon of manifest.icons) {
    assert.equal(fs.existsSync(path.join(publicDir, icon.src.replace(/^\//, ''))), true, icon.src);
  }
});

test('service worker explicitly bypasses every API request', () => {
  const source = fs.readFileSync(path.join(publicDir, 'sw.js'), 'utf8');
  assert.match(source, /url\.pathname\.startsWith\('\/api\/'\)/);
  assert.doesNotMatch(source, /caches\.put\([^\n]*\/api\//);
});

test('service worker never precaches protected HTML or caches redirected login responses', () => {
  const source = fs.readFileSync(path.join(publicDir, 'sw.js'), 'utf8');
  const shell = source.slice(source.indexOf('const SHELL'), source.indexOf('];', source.indexOf('const SHELL')) + 2);
  assert.doesNotMatch(shell, /\.html/);
  assert.match(source, /!res\.redirected/);
});

test('login page cannot automatically loop back to a protected page', () => {
  const source = fs.readFileSync(path.join(publicDir, 'login.html'), 'utf8');
  assert.doesNotMatch(source, /fetch\('\/api\/auth\/me'/);
  assert.match(source, /location\.href = d\.home/);
  assert.match(source, /serviceWorker\.getRegistrations\(\)/);
  assert.match(source, /key\.indexOf\('sanki-os-'\)/);
  assert.match(source, /fetch\('\/api\/auth\/diagnostic'/);
});

test('PWA pages register the manifest, Apple icon, and service worker', () => {
  for (const page of ['dashboard.html', 'expenses.html', 'accounting.html']) {
    const source = fs.readFileSync(path.join(publicDir, page), 'utf8');
    assert.match(source, /rel="manifest" href="\/manifest\.webmanifest"/);
    assert.match(source, /rel="apple-touch-icon"/);
    assert.match(source, /serviceWorker\.register\('\/sw\.js'\)/);
  }
});
