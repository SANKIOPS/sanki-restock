// SANKI Business OS — service worker.
// Goal: fast repeat loads (cache the app shell) WITHOUT ever caching live data.
//  • /api/*            → never touched (always straight to network, always fresh)
//  • HTML navigations  → network-first; fall back to cached page only when offline
//  • static assets     → stale-while-revalidate (instant from cache, refresh in bg)
// Only same-origin, status-200, credentialed ("basic") responses are cached, so a
// login redirect or a cross-origin response never poisons the cache.
const CACHE = 'sanki-os-v3';
const SHELL = [
  '/sidebar.js',
  '/manifest.webmanifest',
  '/icon-192.png',
  '/icon-512.png'
];

self.addEventListener('install', (e) => {
  self.skipWaiting();
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL).catch(() => {})));
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

function cacheable(res) {
  // A protected page may redirect to login and finish as a 200 response.
  // Never store that redirected login HTML under the protected page's URL.
  return res && res.status === 200 && res.type === 'basic' && !res.redirected;
}

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== location.origin) return;      // let the browser handle cross-origin
  if (url.pathname.startsWith('/api/')) return;     // NEVER cache live data / APIs

  // HTML navigations: network-first so pages are always fresh; cache is the offline safety net.
  if (req.mode === 'navigate') {
    e.respondWith(
      fetch(req)
        .then((res) => { if (cacheable(res)) { const cp = res.clone(); caches.open(CACHE).then((c) => c.put(req, cp)); } return res; })
        .catch(() => caches.match(req).then((r) => r || caches.match('/dashboard.html')))
    );
    return;
  }

  // Static assets: serve from cache immediately, refresh in the background.
  e.respondWith(
    caches.match(req).then((cached) => {
      const net = fetch(req)
        .then((res) => { if (cacheable(res)) { const cp = res.clone(); caches.open(CACHE).then((c) => c.put(req, cp)); } return res; })
        .catch(() => cached);
      return cached || net;
    })
  );
});
