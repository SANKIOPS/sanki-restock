'use strict';

const fetch = require('node-fetch');

function parseRetryAfter(value) {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const at = Date.parse(value);
  return Number.isFinite(at) ? Math.max(0, at - Date.now()) : null;
}

class ShopifyClient {
  constructor(options = {}) {
    this.store = options.store || process.env.SHOPIFY_STORE || '';
    this.token = options.token || process.env.SHOPIFY_ACCESS_TOKEN || '';
    this.fetch = options.fetchImpl || fetch;
    this.sleep = options.sleep || (ms => new Promise(resolve => setTimeout(resolve, ms)));
    this.now = options.now || (() => Date.now());
    this.minIntervalMs = options.minIntervalMs == null ? 550 : options.minIntervalMs;
    this.maxRetries = options.maxRetries == null ? 4 : options.maxRetries;
    this._tail = Promise.resolve();
    this._lastStart = 0;
  }

  _schedule(task) {
    const run = this._tail.then(task, task);
    this._tail = run.catch(() => undefined);
    return run;
  }

  _validateUrl(url) {
    if (!this.store || !this.token) throw new Error('Shopify env not configured');
    const parsed = new URL(url, `https://${this.store}`);
    if (parsed.hostname !== this.store) throw new Error('Refusing to send Shopify credentials to an unexpected host');
    return parsed.toString();
  }

  async request(url, options = {}) {
    const target = this._validateUrl(url);
    return this._schedule(async () => {
      let lastError;
      for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
        const spacing = Math.max(0, this.minIntervalMs - (this.now() - this._lastStart));
        if (spacing) await this.sleep(spacing);
        this._lastStart = this.now();

        try {
          const response = await this.fetch(target, {
            ...options,
            headers: {
              'Content-Type': 'application/json',
              ...(options.headers || {}),
              'X-Shopify-Access-Token': this.token
            }
          });
          const retryable = response.status === 429 || response.status === 500 || response.status === 502 || response.status === 503 || response.status === 504;
          if (!retryable || attempt === this.maxRetries) return response;

          const fromHeader = parseRetryAfter(response.headers && response.headers.get('Retry-After'));
          const backoff = fromHeader == null ? Math.min(1000 * (2 ** attempt), 10000) : fromHeader;
          await this.sleep(backoff + Math.floor(Math.random() * 200));
        } catch (error) {
          lastError = error;
          if (attempt === this.maxRetries) throw error;
          await this.sleep(Math.min(1000 * (2 ** attempt), 10000));
        }
      }
      throw lastError || new Error('Shopify request failed');
    });
  }
}

const shopifyClient = new ShopifyClient();

module.exports = { ShopifyClient, shopifyClient, parseRetryAfter };
