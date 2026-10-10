const CACHE = 'ayugram-v2';

// Установка SW
self.addEventListener('install', e => {
  self.skipWaiting();
  e.waitUntil(
    caches.open(CACHE).then(cache =>
      cache.addAll(['./', './index.html', './manifest.json']).catch(() => {})
    )
  );
});

// Активация: чистим старые кэши
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

// Кэширование запросов: network-first, fallback в кэш
self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  if (e.request.url.includes('unsplash.com')) return;    // обои не кэшируем
  if (e.request.url.includes('onrender.com')) return;    // серверное не кэшируем
  if (e.request.url.includes('icons8.com')) return;      // иконки всегда свежие
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});

// Сообщение от страницы — показать уведомление
self.addEventListener('message', e => {
  if (e.data && e.data.type === 'SHOW_NOTIFICATION') {
    const { title, body, icon, tag, data } = e.data;
    e.waitUntil(
      self.registration.showNotification(title, {
        body,
        icon: icon || 'https://img.icons8.com/color/192/telegram-app.png',
        badge: 'https://img.icons8.com/color/192/telegram-app.png',
        tag,
        data,
        vibrate: [200, 100, 200, 100, 200],
        requireInteraction: false,
        actions: [
          { action: 'reply', title: '↩ Ответить' },
          { action: 'close', title: '✕ Закрыть' }
        ]
      })
    );
  }
});

// Клик по уведомлению — открыть приложение
self.addEventListener('notificationclick', e => {
  e.notification.close();
  if (e.action === 'close') return;
  const from = e.notification.data ? e.notification.data.from : null;
  e.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clients => {
      for (const client of clients) {
        if (client.url.includes('ayugram') || client.url.includes('github.io') || client.url.includes('HyperGram')) {
          client.focus();
          if (from) client.postMessage({ type: 'OPEN_CHAT', from });
          return;
        }
      }
      return self.clients.openWindow('./');
    })
  );
});

// Фоновая синхронизация (placeholder для Android)
self.addEventListener('sync', e => {
  if (e.tag === 'sync-messages') {
    // placeholder
  }
});
