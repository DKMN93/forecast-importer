const fetch = require('node-fetch');

class ShopifyClient {
  constructor(domain, accessToken) {
    this.baseUrl = `https://${domain}/admin/api/2024-01`;
    this.headers = {
      'X-Shopify-Access-Token': accessToken,
      'Content-Type': 'application/json'
    };
  }

  async _get(path) {
    const res = await fetch(`${this.baseUrl}${path}`, { headers: this.headers });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Shopify ${res.status}: ${text}`);
    }
    return res.json();
  }

  // Alle Orders der letzten N Tage holen (paginiert)
  async getOrders(days = 84) {
    const since = new Date();
    since.setDate(since.getDate() - days);
    const sinceISO = since.toISOString();

    let orders = [];
    let url = `/orders.json?status=any&financial_status=paid&limit=250&created_at_min=${sinceISO}&fields=id,name,created_at,cancelled_at,line_items`;

    while (url) {
      const res = await fetch(`${this.baseUrl}${url}`, { headers: this.headers });
      if (!res.ok) throw new Error(`Shopify ${res.status}`);
      const data = await res.json();
      orders = orders.concat(data.orders || []);

      // Pagination via Link header
      const link = res.headers.get('link') || '';
      const next = link.match(/<([^>]+)>;\s*rel="next"/);
      if (next) {
        // Link header enthält die volle URL
        url = next[1].replace(this.baseUrl, '');
      } else {
        url = null;
      }

      // Kurze Pause um Rate-Limit zu respektieren
      await new Promise(r => setTimeout(r, 250));
    }

    return orders;
  }

  // Bundles auflösen: ERYTH-GR-5.3 → baseSku=ERYTH-GR-5, factor=3
  _resolveBundle(sku) {
    const match = sku.match(/^(.+)\.(\d+)$/);
    if (match) return { baseSku: match[1], factor: parseInt(match[2]) };
    return { baseSku: sku, factor: 1 };
  }

  // Line Items aus Orders extrahieren (nur gültige, nicht storniert)
  async getLineItems(days = 84) {
    const orders = await this.getOrders(days);
    const items = [];

    for (const order of orders) {
      if (order.cancelled_at) continue; // Stornierte überspringen

      for (const line of order.line_items || []) {
        if (!line.sku) continue;

        const { baseSku, factor } = this._resolveBundle(line.sku);
        const resolvedQty   = line.quantity * factor;
        const gramsPerUnit  = factor > 1 ? line.grams / factor : line.grams;

        items.push({
          orderId:     order.id,
          orderName:   order.name,
          date:        order.created_at,
          sku:         baseSku,
          originalSku: line.sku,
          isBundle:    factor > 1,
          bundleFactor: factor,
          title:       line.title,
          qty:         resolvedQty,
          grams:       gramsPerUnit,
          kg:          (line.quantity * line.grams) / 1000, // Gesamt-KG bleibt gleich
          price:       parseFloat(line.price) * line.quantity
        });
      }
    }

    return items;
  }

  // Verbindungstest
  async testConnection() {
    const data = await this._get('/shop.json');
    return { ok: true, shop: data.shop?.name, domain: data.shop?.domain };
  }
}

module.exports = ShopifyClient;
