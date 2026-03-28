const fetch = require('node-fetch');

class MRPeasyClient {
  constructor(apiKey, apiSecret) {
    this.baseUrl = 'https://app.mrpeasy.com/rest/v1';
    this.auth = 'Basic ' + Buffer.from(`${apiKey}:${apiSecret}`).toString('base64');
    this.headers = {
      'Authorization': this.auth,
      'Content-Type': 'application/json'
    };
  }

  async _request(method, path, body = null) {
    const opts = { method, headers: this.headers };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(`${this.baseUrl}${path}`, opts);
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`MRPeasy ${res.status} ${path}: ${text}`);
    }
    if (res.status === 204) return null;
    return res.json();
  }

  // Alle Items laden (max 1000 pro Request, paginiert)
  async getItems() {
    let items = [];
    let offset = 0;
    const limit = 1000;

    while (true) {
      const res = await fetch(
        `${this.baseUrl}/items?limit=${limit}&offset=${offset}`,
        { headers: this.headers }
      );
      if (!res.ok) throw new Error(`MRPeasy ${res.status}`);
      const data = await res.json();
      const batch = Array.isArray(data) ? data : (data.items || []);
      items = items.concat(batch);

      if (batch.length < limit) break;
      offset += limit;
      await new Promise(r => setTimeout(r, 200));
    }

    return items;
  }

  // Item per SKU/Code suchen
  async getItemByCode(code) {
    const data = await this._request('GET', `/items?code=${encodeURIComponent(code)}`);
    const arr = Array.isArray(data) ? data : (data.items || []);
    return arr.find(i => i.code === code) || null;
  }

  // Mindestbestand eines Items aktualisieren
  async updateMinQuantity(articleId, minQty) {
    return this._request('PUT', `/items/${articleId}`, { min_quantity: String(minQty) });
  }

  // Alle BOMs laden (für Rohstoff-Mapping)
  async getBoms() {
    let boms = [];
    let offset = 0;
    const limit = 1000;

    while (true) {
      const res = await fetch(
        `${this.baseUrl}/boms?limit=${limit}&offset=${offset}`,
        { headers: this.headers }
      );
      if (!res.ok) {
        // BOMs könnten leer sein - kein Fehler
        if (res.status === 404) break;
        throw new Error(`MRPeasy ${res.status}`);
      }
      const data = await res.json();
      const batch = Array.isArray(data) ? data : (data.boms || []);
      boms = boms.concat(batch);
      if (batch.length < limit) break;
      offset += limit;
      await new Promise(r => setTimeout(r, 200));
    }

    return boms;
  }

  // Verbindungstest
  async testConnection() {
    const res = await fetch(`${this.baseUrl}/items?limit=1`, { headers: this.headers });
    if (!res.ok) throw new Error(`MRPeasy ${res.status}`);
    return { ok: true };
  }
}

module.exports = MRPeasyClient;
