require('dotenv').config();

const express  = require('express');
const fs       = require('fs');
const path     = require('path');
const crypto   = require('crypto');
const fetch    = require('node-fetch');
const helmet   = require('helmet');

const xlsx           = require('xlsx');
const multer         = require('multer');
const upload         = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } }); // max 10 MB
const ShopifyClient  = require('./lib/shopify');
const { calculateForecast, aggregateByBaseSku, getWeeklySummary, getBaseSku } = require('./lib/forecast');

const app = express();

// ─── Security Headers ─────────────────────────────────────────────────────────
app.use(helmet({ contentSecurityPolicy: false }));

// ─── Authentifizierung ────────────────────────────────────────────────────────
const APP_PASSWORD = process.env.APP_PASSWORD;
const APP_USERNAME = process.env.APP_USERNAME || 'admin';

function requireAuth(req, res, next) {
  if (!APP_PASSWORD) return next(); // Kein Passwort gesetzt → lokal ohne Auth
  const auth = req.headers['authorization'];
  if (!auth || !auth.startsWith('Basic ')) {
    res.set('WWW-Authenticate', 'Basic realm="Forecast Tool"');
    return res.status(401).send('Authentifizierung erforderlich');
  }
  const decoded = Buffer.from(auth.slice(6), 'base64').toString();
  const colonIdx = decoded.indexOf(':');
  const user = decoded.slice(0, colonIdx);
  const pass = decoded.slice(colonIdx + 1);
  if (user !== APP_USERNAME || pass !== APP_PASSWORD) {
    res.set('WWW-Authenticate', 'Basic realm="Forecast Tool"');
    return res.status(401).send('Falscher Benutzername oder Passwort');
  }
  next();
}

app.use(requireAuth);
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Basis-URL für OAuth Callbacks ────────────────────────────────────────────
const BASE_URL = (process.env.BASE_URL || 'http://localhost:3000').replace(/\/$/, '');

// Config-Datei (API-Keys werden lokal gespeichert)
const CONFIG_FILE = path.join(__dirname, 'config.json');
const LOG_FILE    = path.join(__dirname, 'update-log.json');

function loadConfig() {
  let cfg = {};
  try { cfg = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8')); } catch {}
  // Env vars überschreiben gespeicherte Werte (für Railway Deployment)
  if (process.env.SHOPIFY_DOMAIN) cfg.shopifyDomain = process.env.SHOPIFY_DOMAIN;
  if (process.env.SHOPIFY_TOKEN)  cfg.shopifyToken  = process.env.SHOPIFY_TOKEN;
  if (process.env.SHOPIFY_CLIENT_ID)     cfg.shopifyClientId     = process.env.SHOPIFY_CLIENT_ID;
  if (process.env.SHOPIFY_CLIENT_SECRET) cfg.shopifyClientSecret = process.env.SHOPIFY_CLIENT_SECRET;
  return cfg;
}

function saveConfig(cfg) {
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(cfg, null, 2));
}

function loadLog() {
  try { return JSON.parse(fs.readFileSync(LOG_FILE, 'utf8')); }
  catch { return []; }
}

function appendLog(entry) {
  const log = loadLog();
  log.unshift({ ...entry, timestamp: new Date().toISOString() });
  fs.writeFileSync(LOG_FILE, JSON.stringify(log.slice(0, 50), null, 2)); // max 50 Einträge
}

// ─── Shopify OAuth ────────────────────────────────────────────────────────────

// In-Memory State-Speicher (verhindert CSRF, TTL 10 Min)
const pendingOAuthStates = new Map();

// Schritt 1: User zu Shopify weiterleiten
app.get('/auth/shopify', (req, res) => {
  const cfg = loadConfig();
  if (!cfg.shopifyDomain || !cfg.shopifyClientId) {
    return res.redirect('/?error=Bitte+zuerst+Shop-Domain+und+Client-ID+eintragen');
  }
  const state = crypto.randomBytes(16).toString('hex');
  pendingOAuthStates.set(state, Date.now());
  // Alte States aufräumen (älter als 10 Minuten)
  for (const [s, t] of pendingOAuthStates) {
    if (Date.now() - t > 10 * 60 * 1000) pendingOAuthStates.delete(s);
  }
  const params = new URLSearchParams({
    client_id:    cfg.shopifyClientId,
    scope:        'read_orders',
    redirect_uri: `${BASE_URL}/auth/callback`,
    state
  });
  res.redirect(`https://${cfg.shopifyDomain}/admin/oauth/authorize?${params}`);
});

// Schritt 2: Shopify leitet zurück mit Code → Token holen
app.get('/auth/callback', async (req, res) => {
  const { code, shop, state } = req.query;
  if (!code || !shop) return res.redirect('/?error=OAuth+fehlgeschlagen');

  // State validieren (CSRF-Schutz)
  if (!state || !pendingOAuthStates.has(state)) {
    return res.redirect('/?error=OAuth+Sicherheitsfehler:+Ungültiger+State');
  }
  pendingOAuthStates.delete(state);

  const cfg = loadConfig();
  try {
    const tokenRes = await fetch(`https://${shop}/admin/oauth/access_token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        client_id:     cfg.shopifyClientId,
        client_secret: cfg.shopifyClientSecret,
        code
      })
    });
    const data = await tokenRes.json();
    if (!data.access_token) throw new Error(JSON.stringify(data));

    cfg.shopifyDomain = shop;
    cfg.shopifyToken  = data.access_token;
    saveConfig(cfg);
    res.redirect('/?connected=shopify');
  } catch (e) {
    res.redirect('/?error=Token+Fehler:+' + encodeURIComponent(e.message));
  }
});

// ─── Config Endpoints ────────────────────────────────────────────────────────

app.get('/api/config', (req, res) => {
  const cfg = loadConfig();
  res.json({
    shopifyDomain:  cfg.shopifyDomain || '',
    shopifyToken:     cfg.shopifyToken ? '***' : '',
    targetMonths:     cfg.targetMonths  || cfg.targetWeeks || 2,
    forecastDays:     cfg.forecastDays  || 90,
    lieferzeitWochen: cfg.lieferzeitWochen || 2,
    configured:       !!(cfg.shopifyDomain && cfg.shopifyToken)
  });
});

app.post('/api/config', (req, res) => {
  const cfg = loadConfig();
  const { shopifyDomain, shopifyToken, shopifyClientId, shopifyClientSecret,
          targetMonths, forecastDays, lieferzeitWochen } = req.body;

  if (shopifyDomain)       cfg.shopifyDomain       = shopifyDomain.replace(/^https?:\/\//, '').replace(/\/$/, '');
  if (shopifyClientId)     cfg.shopifyClientId     = shopifyClientId;
  if (shopifyClientSecret && shopifyClientSecret !== '***') cfg.shopifyClientSecret = shopifyClientSecret;
  if (shopifyToken && shopifyToken !== '***')       cfg.shopifyToken        = shopifyToken;
  if (targetMonths)        cfg.targetMonths        = parseFloat(targetMonths);
  if (forecastDays)        cfg.forecastDays        = parseInt(forecastDays);
  if (lieferzeitWochen)    cfg.lieferzeitWochen    = parseFloat(lieferzeitWochen);

  saveConfig(cfg);
  res.json({ ok: true });
});

// ─── Connection Test ──────────────────────────────────────────────────────────

app.get('/api/test', async (req, res) => {
  try {
    const cfg = loadConfig();
    if (!cfg.shopifyDomain || !cfg.shopifyToken) throw new Error('Shopify nicht konfiguriert');
    const shopify = new ShopifyClient(cfg.shopifyDomain, cfg.shopifyToken);
    const result = await shopify.testConnection();
    res.json({ shopify: { ok: true, ...result } });
  } catch (e) {
    res.json({ shopify: { ok: false, error: e.message } });
  }
});

// ─── Dashboard Data ───────────────────────────────────────────────────────────

app.get('/api/dashboard', async (req, res) => {
  try {
    const cfg = loadConfig();
    if (!cfg.shopifyDomain || !cfg.shopifyToken) throw new Error('Shopify nicht konfiguriert');
    const shopify     = new ShopifyClient(cfg.shopifyDomain, cfg.shopifyToken);
    const days        = parseInt(req.query.days) || cfg.forecastDays || 90;
    const targetMonths = cfg.targetMonths || 2;
    const months      = days / 30;

    const lineItems = await shopify.getLineItems(days);
    const weeklySummary = getWeeklySummary(lineItems);

    const artData  = loadArticles();
    const artItems = artData.items || {};

    // Aggregation per SKU
    const skuMap = {};
    for (const item of lineItems) {
      if (!skuMap[item.sku]) skuMap[item.sku] = { sku: item.sku, title: item.title, kg: 0, qty: 0, revenue: 0 };
      skuMap[item.sku].kg      += item.kg;
      skuMap[item.sku].qty     += item.qty;
      skuMap[item.sku].revenue += item.price;
    }

    const totalKg      = +lineItems.reduce((a, b) => a + b.kg, 0).toFixed(1);
    const totalRevenue = +lineItems.reduce((a, b) => a + b.price, 0).toFixed(2);
    const totalBeutel  = lineItems.reduce((a, b) => a + b.qty, 0);
    const avgKgMonth   = +(totalKg / months).toFixed(1);
    const avgRevMonth  = +(totalRevenue / months).toFixed(2);
    const avgBeutelDay = +(totalBeutel / days).toFixed(1);

    // Lager-Alerts: Artikel unter Mindestbestand (based on available, not inStock)
    const stockAlerts = Object.values(artItems)
      .filter(a => a.minQty > 0 && (a.available || 0) < a.minQty)
      .map(a => ({
        nr: a.nr, name: a.name, group: a.group, unit: a.unit,
        inStock: a.inStock, available: a.available, minQty: a.minQty,
        deficit: +(a.minQty - (a.available || 0)).toFixed(1)
      }))
      .sort((a, b) => (b.deficit / b.minQty) - (a.deficit / a.minQty));

    // Reichweite-Alerts: unter Ziel (based on available)
    const rwAlerts = Object.values(skuMap)
      .filter(s => {
        const art = artItems[s.sku];
        if (!art) return false;
        const avgPM = s.qty / months;
        const rw = avgPM > 0 ? (art.available || 0) / avgPM : 99;
        return rw < targetMonths;
      }).length;

    // Top SKUs
    const topByKg  = Object.values(skuMap).sort((a,b) => b.kg - a.kg).slice(0, 10)
      .map(s => ({ ...s, kg: +s.kg.toFixed(1), revenue: +s.revenue.toFixed(2), avgKgPM: +(s.kg/months).toFixed(1) }));
    const topByRev = Object.values(skuMap).sort((a,b) => b.revenue - a.revenue).slice(0, 10)
      .map(s => ({ ...s, kg: +s.kg.toFixed(1), revenue: +s.revenue.toFixed(2) }));

    // Langsamdreher: nur Fertigware mit Bestand, wo Reichweite nicht berechenbar
    // oder > 2× Ziel-Reichweite. Rohstoffe + Verpackung werden ausgeschlossen.
    const deadStock = Object.values(artItems)
      .filter(a => a.group === 'Fertigware' && (a.available || 0) > 0)
      .map(a => {
        const s      = skuMap[a.nr];
        const avgPM  = s ? s.qty / months : 0;
        const rw     = avgPM > 0 ? +((a.available || 0) / avgPM).toFixed(1) : null;
        return { nr: a.nr, name: a.name, inStock: a.inStock, available: a.available, unit: a.unit, avgPM: +avgPM.toFixed(1), reichweite: rw };
      })
      .filter(a => a.reichweite === null || a.reichweite > targetMonths * 2)
      .sort((a, b) => (b.reichweite ?? 9999) - (a.reichweite ?? 9999))
      .slice(0, 30);

    // Monatlicher Verlauf (wöchentlich aus weeklySummary)
    const byBase = aggregateByBaseSku(lineItems, days / 7);

    res.json({
      period: { days, months: +months.toFixed(1), targetMonths },
      summary: {
        totalKg, totalRevenue, avgKgMonth, avgRevMonth, totalBeutel, avgBeutelDay,
        uniqueSkus:    new Set(lineItems.map(l => l.sku)).size,
        totalOrders:   lineItems.length,
        stockAlerts:   stockAlerts.length,
        rwAlerts,
        deadStockCount: deadStock.length, deadStockTarget: targetMonths * 2,
        articlesLoaded: Object.keys(artItems).length > 0,
        lastFetch:     new Date().toISOString(),
      },
      weeklySummary,
      topByKg,
      topByRev,
      stockAlerts: stockAlerts.slice(0, 20),
      deadStock,
      rohstoffe: byBase.slice(0, 20),
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});



// ─── MRPeasy CSV Parser (shared) ─────────────────────────────────────────────

function parseMrpCsv(buffer) {
  const text = buffer.toString('utf8').replace(/^\uFEFF/, '');
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  if (!lines.length) throw new Error('Leere Datei');
  const parseRow = l => l.split(';').map(v => v.replace(/^"|"$/g, '').trim());
  const headers = parseRow(lines[0]);
  return { headers, rows: lines.slice(1).map(parseRow) };
}

// ─── MRPeasy CSV Upload ───────────────────────────────────────────────────────

const STOCK_FILE = path.join(__dirname, 'stock.json');

function loadStock() {
  try { return JSON.parse(fs.readFileSync(STOCK_FILE, 'utf8')); }
  catch { return {}; }
}

app.post('/api/upload-stock', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Keine Datei' });

    const text = req.file.buffer.toString('utf8').replace(/^\uFEFF/, ''); // BOM entfernen
    const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
    if (!lines.length) return res.status(400).json({ error: 'Leere Datei' });

    // Header parsen (Semikolon-getrennt, mit Anführungszeichen)
    const parseRow = l => l.split(';').map(v => v.replace(/^"|"$/g, '').trim());
    const headers = parseRow(lines[0]);

    // Sprache erkennen (DE oder EN)
    const isDE = headers.includes('Artikelnr.');
    const skuCol   = isDE ? 'Artikelnr.'       : 'Part No.';
    const stockCol = isDE ? 'Auf Lager'         : 'In stock';
    const availCol = isDE ? 'Verfügbar'         : 'Available';
    const nameCol  = isDE ? 'Artikelbezeichnung': 'Part description';
    const unitCol  = isDE ? 'Maßeinheit'        : 'UoM';
    const weightCol = 'Gewicht in kg';

    const idx = name => headers.indexOf(name);
    const iSku    = idx(skuCol);
    const iStock  = idx(stockCol);
    const iAvail  = idx(availCol);
    const iName   = idx(nameCol);
    const iUnit   = idx(unitCol);
    const iWeight = idx(weightCol);

    if (iSku === -1 || iStock === -1) return res.status(400).json({ error: 'Unbekanntes CSV-Format' });

    // Pro SKU aggregieren (mehrere Chargen/Lots addieren)
    const stock = {};
    for (let i = 1; i < lines.length; i++) {
      const cols = parseRow(lines[i]);
      const sku = cols[iSku];
      if (!sku) continue;
      const inStock = parseFloat((cols[iStock] || '0').replace(',', '.')) || 0;
      const avail   = parseFloat((cols[iAvail]  || '0').replace(',', '.')) || 0;
      const weight  = parseFloat((cols[iWeight] || '0').replace(',', '.')) || 0;
      if (!stock[sku]) stock[sku] = { sku, name: cols[iName] || '', unit: cols[iUnit] || 'Stk.', inStock: 0, available: 0, weightKg: weight };
      stock[sku].inStock   += inStock;
      stock[sku].available += avail;
    }

    fs.writeFileSync(STOCK_FILE, JSON.stringify({ updatedAt: new Date().toISOString(), items: stock }, null, 2));
    res.json({ ok: true, skuCount: Object.keys(stock).length, language: isDE ? 'DE' : 'EN' });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/stock', (req, res) => {
  const s = loadStock();
  res.json(s);
});

// ─── MRPeasy Articles Upload ──────────────────────────────────────────────────

const ARTICLES_FILE = path.join(__dirname, 'articles.json');

function loadArticles() {
  try { return JSON.parse(fs.readFileSync(ARTICLES_FILE, 'utf8')); }
  catch { return {}; }
}

app.post('/api/upload-articles', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Keine Datei' });
    const { headers, rows } = parseMrpCsv(req.file.buffer);

    // DE-Spalten (aktuell nur DE-Format vorhanden)
    const idx = name => headers.indexOf(name);
    const iNr       = idx('Artikelnr.');
    const iName     = idx('Artikelbezeichnung');
    const iGroup    = idx('Gruppenname');
    const iStock    = idx('Auf Lager');
    const iAvail    = idx('Verfügbar');
    const iMin      = idx('Mindestbestand');
    const iUnit     = idx('Maßeinheit');
    const iWeight   = idx('Gewicht in kg');
    const iLead     = idx('Lieferzeit');
    const iMinProd  = idx('Minimale Herstellmenge');
    const iPurchase = idx('beschaffter Artikel');

    if (iNr === -1 || iMin === -1) return res.status(400).json({ error: 'Unbekanntes Format – Artikelnr. oder Mindestbestand nicht gefunden' });

    const pf = v => parseFloat((v || '0').replace(',', '.')) || 0;
    const articles = {};
    for (const cols of rows) {
      const nr = cols[iNr];
      if (!nr) continue;
      articles[nr] = {
        nr,
        name:        cols[iName]    || '',
        group:       cols[iGroup]   || '',
        inStock:     pf(cols[iStock]),
        available:   pf(cols[iAvail]),
        minQty:      pf(cols[iMin]),
        unit:        cols[iUnit]    || '',
        weightKg:    pf(cols[iWeight]),
        leadTimeDays: pf(cols[iLead]),
        minProdQty:  pf(cols[iMinProd]),
        isPurchased: cols[iPurchase] === '1',
      };
    }

    fs.writeFileSync(ARTICLES_FILE, JSON.stringify({ updatedAt: new Date().toISOString(), items: articles }, null, 2));
    const groups = {};
    Object.values(articles).forEach(a => { groups[a.group] = (groups[a.group] || 0) + 1; });
    res.json({ ok: true, total: Object.keys(articles).length, groups });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/articles', (req, res) => res.json(loadArticles()));

// ─── MRPeasy Reorder-CSV Export ───────────────────────────────────────────────
// Erzeugt eine CSV mit vorgeschlagenen Mindestbeständen für alle Artikel.
// Fertigware: Ø/Monat × Ziel-Monate (Stk)
// Rohware:    Ø/Tag × Lieferzeit (kg), aufgerundet auf Einkaufseinheit
app.get('/api/export-reorder-csv', async (req, res) => {
  try {
    const cfg = loadConfig();
    if (!cfg.shopifyDomain || !cfg.shopifyToken) return res.status(400).json({ error: 'Shopify nicht konfiguriert' });

    const shopify      = new ShopifyClient(cfg.shopifyDomain, cfg.shopifyToken);
    const days         = parseInt(req.query.days) || 120; // immer 4 Monate für Rohware-Berechnung
    const targetMonths = parseFloat(req.query.months) || cfg.targetMonths || 2;
    const months       = days / 30;

    const lineItems    = await shopify.getLineItems(days);
    const artData      = loadArticles();
    const artItems     = artData.items || {};
    const partsData    = loadParts();
    const partsMap     = partsData.mapping || {};

    // Gesammelter Output: pro Artikelnr. nur ein Eintrag (kein Duplikat)
    const output = {}; // nr → { name, group, proposed, unit, basis }

    // ── Fertigware: pro Shopify-SKU (nur nicht-Rohstoffe) ────────────────────
    const skuSales = {};
    for (const item of lineItems) {
      if (!skuSales[item.sku]) skuSales[item.sku] = { title: item.title, qty: 0 };
      skuSales[item.sku].qty += item.qty;
    }
    for (const [sku, s] of Object.entries(skuSales)) {
      const art   = artItems[sku];
      const group = art ? art.group : 'Fertigware';
      if (group === 'Rohstoffe') continue; // Rohstoffe nur über BOM-Pfad
      const avgPerMonth = s.qty / months;
      const proposed    = Math.ceil(avgPerMonth * targetMonths);
      output[sku] = { name: art ? art.name : s.title, group, proposed, unit: 'Stk.', basis: `Ø ${avgPerMonth.toFixed(1)} Stk/Mo × ${targetMonths} Mo` };
    }

    // ── Rohware: über Stücklisten-Mapping (kg-basiert, dedupliziert) ─────────
    const rohwareSales = {};
    for (const item of lineItems) {
      const prefix = item.sku.replace(/-\d+$/, '');
      const part   = partsMap[prefix];
      if (!part) continue;
      const nr = part.rohwareNr;
      if (!rohwareSales[nr]) rohwareSales[nr] = { name: part.rohwareName, totalKg: 0 };
      rohwareSales[nr].totalKg += item.kg;
    }
    for (const [nr, r] of Object.entries(rohwareSales)) {
      const art         = artItems[nr];
      const avgKgPerDay = r.totalKg / days;
      const leadTime    = art && art.leadTimeDays > 0 ? art.leadTimeDays : 1;
      const bagSize     = art && art.weightKg > 0 ? art.weightKg : 25; // Sackgröße (20 oder 25 kg)
      const rawKg       = avgKgPerDay * leadTime * 1.5;
      // Aufrunden auf ganze Säcke, mindestens 1 Sack
      const proposed    = Math.max(bagSize, Math.ceil(rawKg / bagSize) * bagSize);
      const einheit     = art ? art.unit || 'kg' : 'kg';
      output[nr] = { name: r.name, group: 'Rohstoffe', proposed, unit: einheit, basis: `Ø ${avgKgPerDay.toFixed(3)} kg/Tag × ${leadTime} Tage LZ × 1,5 → ${Math.ceil(rawKg/bagSize) || 1} Sack(${bagSize}kg)` };
    }

    const rows = [['Artikelnr.', 'Artikelbezeichnung', 'Gruppe', 'Vorgeschlagener Mindestbestand', 'Einheit', 'Berechnungsgrundlage']];
    for (const [nr, o] of Object.entries(output)) {
      rows.push([nr, o.name, o.group, o.proposed, o.unit, o.basis]);
    }

    // CSV ausgeben (Semikolon-getrennt, UTF-8 BOM für Excel)
    const csv = '\uFEFF' + rows.map(r => r.map(v => `"${String(v).replace(/"/g, '""')}"`).join(';')).join('\r\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="mrpeasy_mindestbestand_${new Date().toISOString().slice(0,10)}.csv"`);
    res.send(csv);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Excel Export ─────────────────────────────────────────────────────────────

app.get('/api/export', async (req, res) => {
  try {
    const cfg = loadConfig();
    if (!cfg.shopifyDomain || !cfg.shopifyToken) return res.status(400).json({ error: 'Shopify nicht konfiguriert' });

    const shopify      = new ShopifyClient(cfg.shopifyDomain, cfg.shopifyToken);
    const days         = parseInt(req.query.days) || cfg.forecastDays || 90;
    const targetMonths = cfg.targetMonths || 2;
    const months       = days / 30;
    const lineItems    = await shopify.getLineItems(days);

    // Artikel- und Bestandsdaten laden
    const articlesData = loadArticles();
    const articleItems = articlesData.items || {};
    const hasArticles  = Object.keys(articleItems).length > 0;

    // Sheet 1: Mindestbestand pro SKU (Fertigware, in Stk)
    const skuMap = {};
    for (const item of lineItems) {
      if (!skuMap[item.sku]) skuMap[item.sku] = { sku: item.sku, title: item.title, qty: 0, kg: 0 };
      skuMap[item.sku].qty += item.qty;
      skuMap[item.sku].kg  += item.kg;
    }
    const skuHeaders = ['Artikelnr.', 'Produktname', `Verkauf ${days} Tage (Stk)`, 'Ø Stk/Monat', 'Ziel-Monate', 'Vorgeschlagener Mindestbestand'];
    if (hasArticles) skuHeaders.push('Aktueller Mindestbestand', 'Änderung', 'Auf Lager', 'Verfügbar', 'Reichweite (Mo.)');
    const skuRows = [skuHeaders];
    for (const s of Object.values(skuMap).sort((a,b) => b.qty - a.qty)) {
      const avgPM       = s.qty / months;
      const proposedMin = Math.ceil(avgPM * targetMonths);
      const row = [s.sku, s.title, s.qty, +avgPM.toFixed(1), targetMonths, proposedMin];
      if (hasArticles) {
        const art        = articleItems[s.sku];
        const currentMin = art ? art.minQty : '';
        const delta      = art ? proposedMin - art.minQty : '';
        const inStock    = art ? art.inStock : '';
        const avail      = art ? art.available : '';
        const rw         = art && avgPM > 0 ? +((art.available || 0) / avgPM).toFixed(1) : '';
        row.push(currentMin, delta, inStock, avail, rw);
      }
      skuRows.push(row);
    }

    // Sheet 2: Rohstoffe nach Base-SKU (in KG)
    const baseMap = {};
    for (const item of lineItems) {
      const base = getBaseSku(item.sku);
      if (!baseMap[base]) baseMap[base] = { base, kg: 0, qty: 0 };
      baseMap[base].kg  += item.kg;
      baseMap[base].qty += item.qty;
    }
    const baseHeaders = ['Base-SKU (Rohstoff)', `Verbrauch ${days} Tage (KG)`, 'Ø KG/Monat', `Zielbestand (${targetMonths} Mo.) KG`, 'Einheit'];
    if (hasArticles) baseHeaders.push('Aktueller Mindestbestand', 'Auf Lager (KG)', 'Verfügbar (KG)', 'Änderung', 'Reichweite (Mo.)');
    const baseRows = [baseHeaders];
    for (const b of Object.values(baseMap).sort((a,b) => b.kg - a.kg)) {
      const avgKgPM       = b.kg / months;
      const proposedMinKg = +(avgKgPM * targetMonths).toFixed(2);
      const row = [b.base, +b.kg.toFixed(2), +avgKgPM.toFixed(2), proposedMinKg, 'KG'];
      if (hasArticles) {
        const art        = articleItems[b.base];
        const currentMin = art ? art.minQty : '';
        const inStock    = art ? art.inStock : '';
        const avail      = art ? art.available : '';
        const delta      = art ? +(proposedMinKg - art.minQty).toFixed(2) : '';
        const rw         = art && avgKgPM > 0 ? +((art.available || 0) / avgKgPM).toFixed(1) : '';
        row.push(currentMin, inStock, avail, delta, rw);
      }
      baseRows.push(row);
    }

    // Sheet 3: MRPeasy Import (Artikelnr. + neuer Mindestbestand)
    const importRows = [['Artikelnr.', 'Mindestbestand']];
    for (const s of Object.values(skuMap)) {
      const proposedMin = Math.ceil((s.qty / months) * targetMonths);
      importRows.push([s.sku, proposedMin]);
    }

    // Sheet 4: Rohware-Bedarf (Prefix-basiert via -5 Stücklisten-Mapping)
    const partsData = loadParts();
    const partsMap = partsData.mapping || {}; // { 'ERYTH-GR': { rohwareNr, rohwareName } }
    const articlesItems = (loadArticles().items) || {};
    const rohwareMap = {};

    for (const item of lineItems) {
      // Prefix ableiten: ERYTH-GR-5 → ERYTH-GR, ERYTH-GR-1 → ERYTH-GR
      const prefix = item.sku.replace(/-\d+$/, '');
      const part = partsMap[prefix];
      if (!part) continue; // kein Mapping für diesen Prefix

      const nr = part.rohwareNr;
      if (!rohwareMap[nr]) {
        const artRef = articlesItems[nr];
        const einheitKg = artRef ? (artRef.weightKg || 25) : 25;
        rohwareMap[nr] = { nr, name: part.rohwareName, einheitKg, totalKg: 0, prefixes: new Set() };
      }
      // KG direkt aus Line Item (qty × grams bereits korrekt berechnet)
      rohwareMap[nr].totalKg += item.kg;
      rohwareMap[nr].prefixes.add(prefix);
    }

    const hasParts = Object.keys(partsMap).length > 0;
    const rohwareHeaders = ['Rohware Artikelnr.', 'Rohware Bezeichnung', `Verbrauch ${days} Tage (KG)`, 'Ø KG/Monat', `Mindestbestand (${targetMonths} Mo.) KG`, 'Einkaufseinheit (KG)', 'Benötigte Einheiten'];
    const rohwareRows = [rohwareHeaders];
    for (const r of Object.values(rohwareMap).sort((a,b) => b.totalKg - a.totalKg)) {
      const avgKgMonth = r.totalKg / months;
      const mindestbestand = +(avgKgMonth * targetMonths).toFixed(2);
      const einheiten = Math.ceil(mindestbestand / r.einheitKg);
      rohwareRows.push([r.nr, r.name, +r.totalKg.toFixed(2), +avgKgMonth.toFixed(2), mindestbestand, r.einheitKg, einheiten]);
    }
    if (!hasParts) rohwareRows.push(['', '→ Bitte zuerst Stücklisten (parts CSV) hochladen', '', '', '', '', '']);

    const wb = xlsx.utils.book_new();
    const ws1 = xlsx.utils.aoa_to_sheet(skuRows);
    ws1['!cols'] = [{wch:20},{wch:40},{wch:20},{wch:14},{wch:14},{wch:26},{wch:24},{wch:12},{wch:14},{wch:14}];
    const ws2 = xlsx.utils.aoa_to_sheet(baseRows);
    ws2['!cols'] = [{wch:22},{wch:22},{wch:14},{wch:22},{wch:10},{wch:24},{wch:16},{wch:16},{wch:12}];
    const ws3 = xlsx.utils.aoa_to_sheet(importRows);
    ws3['!cols'] = [{wch:20},{wch:20}];
    const ws4 = xlsx.utils.aoa_to_sheet(rohwareRows);
    ws4['!cols'] = [{wch:18},{wch:40},{wch:16},{wch:22},{wch:14},{wch:22},{wch:20},{wch:18}];
    xlsx.utils.book_append_sheet(wb, ws1, 'Fertigware_Mindestbestand');
    xlsx.utils.book_append_sheet(wb, ws2, 'Rohstoffe_KG');
    xlsx.utils.book_append_sheet(wb, ws3, 'MRPeasy_Import');
    xlsx.utils.book_append_sheet(wb, ws4, 'Rohware_Bedarf');

    // Sheet 5: Planung (Rohware → Fertigware Cluster)
    const planPfxSales = {};
    for (const item of lineItems) {
      const prefix = item.sku.replace(/-\d+$/, '');
      if (!planPfxSales[prefix]) planPfxSales[prefix] = {};
      if (!planPfxSales[prefix][item.sku]) planPfxSales[prefix][item.sku] = { title: item.title, qty: 0, kg: 0 };
      planPfxSales[prefix][item.sku].qty += item.qty;
      planPfxSales[prefix][item.sku].kg  += item.kg;
    }
    const planBlocks = {};
    for (const [prefix, rohInfo] of Object.entries(partsMap)) {
      const sales = planPfxSales[prefix];
      if (!sales) continue;
      const nr = rohInfo.rohwareNr;
      if (!planBlocks[nr]) {
        const rawArt = articlesItems[nr];
        planBlocks[nr] = { rohwareNr: nr, rohwareName: rohInfo.rohwareName, bestandKg: rawArt ? (rawArt.available || 0) : 0, fw: [] };
      }
      for (const [sku, s] of Object.entries(sales)) {
        const art = articlesItems[sku];
        const wKg = art ? (art.weightKg || 0) : 0;
        const bst = art ? (art.available || 0) : 0;
        planBlocks[nr].fw.push({ sku, title: s.title, bst, bestandKgEquiv: bst * wKg, kgPM: s.kg / months, totalKg: s.kg });
      }
    }
    // Pass 2 Excel: Artikel mit Bestand ohne Verkäufe (z.B. -9 Großgebinde)
    for (const [artNr, art] of Object.entries(articlesItems)) {
      if (!art.group || art.group === 'Rohstoffe') continue;
      const artPrefix = artNr.replace(/-\d+$/, '');
      const rohInfo = partsMap[artPrefix];
      if (!rohInfo) continue;
      const nr = rohInfo.rohwareNr;
      if (!planBlocks[nr]) {
        const rawArt = articlesItems[nr];
        planBlocks[nr] = { rohwareNr: nr, rohwareName: rohInfo.rohwareName, bestandKg: rawArt ? (rawArt.available || 0) : 0, fw: [] };
      }
      if (!planBlocks[nr].fw.some(f => f.sku === artNr) && (art.available || 0) > 0) {
        const wKg = art.weightKg || 0;
        planBlocks[nr].fw.push({ sku: artNr, title: art.name, bst: art.available, bestandKgEquiv: (art.available || 0) * wKg, kgPM: 0, totalKg: 0 });
      }
    }

    const planResult = Object.values(planBlocks).map(b => {
      const vkgPM = b.fw.reduce((s, f) => s + f.kgPM, 0);
      const fwKg  = b.fw.reduce((s, f) => s + f.bestandKgEquiv, 0);
      const totKg = b.bestandKg + fwKg;
      const rw    = vkgPM > 0 ? totKg / vkgPM : null;
      const zKg   = vkgPM * targetMonths;
      const best  = Math.max(0, zKg - b.bestandKg);  // Einkauf via Rohwarenbestand
      const st    = rw === null ? 'OK' : rw < targetMonths * 0.5 ? 'KRITISCH' : rw < targetMonths ? 'WARNUNG' : 'OK';
      b.fw.sort((a, c) => c.kgPM - a.kgPM);
      return { ...b, vkgPM, fwKg, totKg, rw, zKg, best, st };
    });
    const planStOrd = { KRITISCH: 0, WARNUNG: 1, OK: 2 };
    planResult.sort((a, b) => (planStOrd[a.st] - planStOrd[b.st]) || (b.vkgPM - a.vkgPM));

    const planHdr = ['Typ', 'Nr', 'Bezeichnung', 'Rohware Bestand KG', 'FW Bestand KG-Äquiv', 'Total Bestand KG', 'Ø KG/Monat', 'Reichweite (Mo.)', `Ziel-KG (${targetMonths} Mo.)`, 'Bestellempfehlung KG', 'Status'];
    const planRows = [planHdr];
    for (const b of planResult) {
      planRows.push(['ROHWARE', b.rohwareNr, b.rohwareName, +b.bestandKg.toFixed(2), +b.fwKg.toFixed(2), +b.totKg.toFixed(2), +b.vkgPM.toFixed(2), b.rw !== null ? +b.rw.toFixed(1) : 'n/a', +b.zKg.toFixed(2), +b.best.toFixed(2), b.st]);
      const totAll = b.fw.reduce((s, f) => s + f.totalKg, 0);
      for (const f of b.fw) {
        const anteil = totAll > 0 ? +(f.totalKg / totAll * 100).toFixed(1) : 0;
        planRows.push(['  Fertigware', f.sku, f.title, '', +f.bestandKgEquiv.toFixed(2), '', +f.kgPM.toFixed(2), '', '', '', anteil + '%']);
      }
    }
    if (!planResult.length) planRows.push(['', '→ Bitte zuerst Stücklisten hochladen', '', '', '', '', '', '', '', '', '']);

    const ws5 = xlsx.utils.aoa_to_sheet(planRows);
    ws5['!cols'] = [{wch:12},{wch:18},{wch:40},{wch:20},{wch:20},{wch:16},{wch:14},{wch:16},{wch:18},{wch:20},{wch:10}];
    xlsx.utils.book_append_sheet(wb, ws5, 'Planung');

    const buf = xlsx.write(wb, { type: 'buffer', bookType: 'xlsx' });
    const filename = `Forecast_${new Date().toISOString().slice(0,10)}.xlsx`;
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.send(buf);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Stücklisten (Parts) Upload ───────────────────────────────────────────────

const PARTS_FILE = path.join(__dirname, 'parts-mapping.json');

function loadParts() {
  try { return JSON.parse(fs.readFileSync(PARTS_FILE, 'utf8')); }
  catch { return {}; }
}

app.post('/api/upload-parts', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Keine Datei' });
    const { headers, rows } = parseMrpCsv(req.file.buffer);

    const iSku     = headers.indexOf('Artikelnummer');
    const iGroup   = headers.indexOf('Gruppenname');
    const iRohNr   = headers.indexOf('Artikelnr.');
    const iRohName = headers.indexOf('Artikelbezeichnung');

    if (iSku === -1 || iRohNr === -1) return res.status(400).json({ error: 'Unbekanntes Format' });

    // Für jeden Fertigware-Prefix (ERYTH-GR, FRUKT-PL, ...) die Rohware finden.
    // Quelle: jede aktive SKU mit Rohstoff-Eintrag (nicht -del).
    // Prefix = SKU ohne letzten -N Suffix (ERYTH-GR-5 → ERYTH-GR, ERYTH-GR-9 → ERYTH-GR)
    const mapping = {};
    for (const cols of rows) {
      if (cols[iGroup] !== 'Rohstoffe') continue;
      const sku = cols[iSku];
      if (!sku || sku.includes('-del')) continue;
      const prefix = sku.replace(/-\d+$/, '');
      if (!mapping[prefix]) {
        mapping[prefix] = {
          rohwareNr:   cols[iRohNr],
          rohwareName: cols[iRohName],
        };
      }
    }

    fs.writeFileSync(PARTS_FILE, JSON.stringify({ updatedAt: new Date().toISOString(), mapping }, null, 2));
    res.json({ ok: true, count: Object.keys(mapping).length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});


// ─── Planung (Rohware → Fertigware Cluster) ───────────────────────────────────
// Logik:
//   Rohwarenpool  = Rohware-Artikel (z.B. A00099) + alle -9 SKUs (25kg-Beutel)
//                 → können in kleinere Packungen umgewandelt werden
//                 → Einkauf läuft nur über Rohware-Artikelnummer
//   Fertigwarenbestand = -1 bis -5 + Bundles → fixiert, fließt nicht zurück
//   Bestellempfehlung  = max(0, Ziel-KG − Total aus Rohwarenpool)

app.get('/api/planung', async (req, res) => {
  try {
    const cfg = loadConfig();
    if (!cfg.shopifyDomain || !cfg.shopifyToken) throw new Error('Shopify nicht konfiguriert');

    const shopify      = new ShopifyClient(cfg.shopifyDomain, cfg.shopifyToken);
    const days         = parseInt(req.query.days) || cfg.forecastDays || 90;
    const targetMonths = cfg.targetMonths || 2;
    const months       = days / 30;

    const lineItems = await shopify.getLineItems(days);
    const partsData = loadParts();
    const partsMap  = partsData.mapping || {};
    const artData   = loadArticles();
    const artItems  = artData.items || {};

    // Hilfs-Funktion: ist eine SKU ein Großgebinde (-9)?
    const isGrossgebinde = sku => /\-9$/.test(sku);

    // Per-prefix Verkauf aggregieren
    const prefixSales = {};
    for (const item of lineItems) {
      const prefix = item.sku.replace(/-\d+$/, '');
      if (!prefixSales[prefix]) prefixSales[prefix] = {};
      if (!prefixSales[prefix][item.sku])
        prefixSales[prefix][item.sku] = { title: item.title, qty: 0, kg: 0 };
      prefixSales[prefix][item.sku].qty += item.qty;
      prefixSales[prefix][item.sku].kg  += item.kg;
    }

    // Rohware-Blöcke aufbauen (getrennt nach Rohwarenpool / Fertigware)
    const blocks = {};
    const initBlock = (nr, rohInfo) => {
      if (blocks[nr]) return;
      const rawArt = artItems[nr];
      blocks[nr] = {
        rohwareNr: nr, rohwareName: rohInfo.rohwareName,
        rohwareArtikel: { nr, name: rawArt ? rawArt.name : rohInfo.rohwareName, bestandKg: rawArt ? (rawArt.available || 0) : 0 },
        grossgebinde: [], fertigware: [],
      };
    };

    // Pass 1: Aus Shopify-Verkäufen
    for (const [prefix, rohInfo] of Object.entries(partsMap)) {
      const sales = prefixSales[prefix];
      if (!sales) continue;
      const nr = rohInfo.rohwareNr;
      initBlock(nr, rohInfo);
      for (const [sku, s] of Object.entries(sales)) {
        const art = artItems[sku];
        const wKg = art ? (art.weightKg || 0) : 0;
        const bst = art ? (art.available || 0) : 0;
        const entry = {
          sku, title: s.title, bestand: bst, weightKg: wKg,
          bestandKgEquiv:   +(bst * wKg).toFixed(2),
          verkaufProMonat:  +(s.qty / months).toFixed(2),
          kgProMonat:       +(s.kg  / months).toFixed(2),
          totalKg:          +s.kg.toFixed(2),
        };
        if (isGrossgebinde(sku)) blocks[nr].grossgebinde.push(entry);
        else                     blocks[nr].fertigware.push(entry);
      }
    }

    // Pass 2: Artikel ohne Verkäufe im Zeitraum (Bestand trotzdem mitzählen)
    for (const [artNr, art] of Object.entries(artItems)) {
      if (!art.group || art.group === 'Rohstoffe') continue;
      const artPrefix = artNr.replace(/-\d+$/, '');
      const rohInfo = partsMap[artPrefix];
      if (!rohInfo) continue;
      const nr = rohInfo.rohwareNr;
      initBlock(nr, rohInfo);
      const pool = isGrossgebinde(artNr) ? blocks[nr].grossgebinde : blocks[nr].fertigware;
      if (!pool.some(f => f.sku === artNr) && (art.available || 0) > 0) {
        const wKg = art.weightKg || 0;
        pool.push({ sku: artNr, title: art.name, bestand: art.available, weightKg: wKg,
          bestandKgEquiv: +((art.available || 0) * wKg).toFixed(2), verkaufProMonat: 0, kgProMonat: 0, totalKg: 0 });
      }
    }

    // Reichweite & Status berechnen (in Monaten)
    const result = [];
    for (const block of Object.values(blocks)) {
      const allSkus  = [...block.grossgebinde, ...block.fertigware];
      const vkgPM    = allSkus.reduce((s, f) => s + f.kgProMonat, 0);  // KG/Monat
      const ggKg     = block.grossgebinde.reduce((s, f) => s + f.bestandKgEquiv, 0);
      const poolKg   = block.rohwareArtikel.bestandKg + ggKg;
      const fwKg     = block.fertigware.reduce((s, f) => s + f.bestandKgEquiv, 0);
      const totKg    = poolKg + fwKg;
      const rw       = vkgPM > 0 ? +(totKg / vkgPM).toFixed(1) : null;  // Monate
      const zielKg   = +(vkgPM * targetMonths).toFixed(2);
      const best     = +(Math.max(0, zielKg - poolKg)).toFixed(2);  // Einkauf via Rohwarenpool
      const status   = rw === null ? 'ok'
        : rw < targetMonths * 0.5 ? 'kritisch'
        : rw < targetMonths       ? 'warn' : 'ok';

      const totKgAll = allSkus.reduce((s, f) => s + f.totalKg, 0);
      for (const f of allSkus)
        f.anteilProzent = totKgAll > 0 ? +(f.totalKg / totKgAll * 100).toFixed(1) : 0;
      block.grossgebinde.sort((a, b) => b.kgProMonat - a.kgProMonat);
      block.fertigware.sort((a, b) => b.kgProMonat - a.kgProMonat);

      result.push({
        rohwareNr: block.rohwareNr, rohwareName: block.rohwareName,
        rohwareArtikel: block.rohwareArtikel,
        grossgebinde: block.grossgebinde,
        rohwarenPoolKg: +poolKg.toFixed(2),
        fertigware: block.fertigware,
        fertigwarenBestandKg: +fwKg.toFixed(2),
        totalBestandKg: +totKg.toFixed(2),
        verbrauchKgProMonat: +vkgPM.toFixed(2),
        reichweiteMonate: rw,
        zielKg, bestellempfehlungKg: best, status,
      });
    }

    const stOrd = { kritisch: 0, warn: 1, ok: 2 };
    result.sort((a, b) => (stOrd[a.status] - stOrd[b.status]) || (b.verbrauchKgProMonat - a.verbrauchKgProMonat));

    res.json({
      period:      { days, months: +(days/30).toFixed(1), targetMonths },
      importDates: { articles: artData.updatedAt || null, parts: partsData.updatedAt || null },
      blocks:      result,
      summary: {
        total:    result.length,
        kritisch: result.filter(r => r.status === 'kritisch').length,
        warn:     result.filter(r => r.status === 'warn').length,
        ok:       result.filter(r => r.status === 'ok').length,
      },
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── SKU-Ziele (per-SKU Ziel-Monate Überschreibung) ──────────────────────────

const SKU_TARGETS_FILE = path.join(__dirname, 'sku-targets.json');

function loadSkuTargets() {
  try {
    const raw = JSON.parse(fs.readFileSync(SKU_TARGETS_FILE, 'utf8'));
    // Migration: altes Format { "SKU": 2.5 } → neues { "SKU": { targetMonths: 2.5 } }
    const out = {};
    for (const [k, v] of Object.entries(raw)) {
      out[k] = typeof v === 'number' ? { targetMonths: v } : v;
    }
    return out;
  } catch { return {}; }
}

app.get('/api/sku-targets', (req, res) => res.json(loadSkuTargets()));

// field = 'targetMonths' | 'lieferzeitWochen' | 'purchasePrice'
app.post('/api/sku-targets', (req, res) => {
  const { nr, field, value } = req.body;
  if (!nr || !field) return res.status(400).json({ error: 'nr und field erforderlich' });
  const targets = loadSkuTargets();
  if (!targets[nr]) targets[nr] = {};
  if (value === null || value === '') {
    delete targets[nr][field];
    if (Object.keys(targets[nr]).length === 0) delete targets[nr];
  } else {
    targets[nr][field] = parseFloat(value);
  }
  fs.writeFileSync(SKU_TARGETS_FILE, JSON.stringify(targets, null, 2));
  res.json({ ok: true });
});

// ─── MRPeasy Import CSV ───────────────────────────────────────────────────────

// Schützt gegen CSV/Excel-Formel-Injection
function safeCsvStr(v) {
  const s = String(v);
  if (/^[=+\-@\t\r]/.test(s)) return `'${s}`;
  return s;
}

app.get('/api/export/csv', async (req, res) => {
  try {
    const cfg = loadConfig();
    if (!cfg.shopifyDomain || !cfg.shopifyToken) return res.status(400).json({ error: 'Shopify nicht konfiguriert' });

    const shopify        = new ShopifyClient(cfg.shopifyDomain, cfg.shopifyToken);
    const days           = parseInt(req.query.days) || cfg.forecastDays || 90;
    const globalTarget   = cfg.targetMonths || 2;
    const months         = days / 30;
    const lineItems      = await shopify.getLineItems(days);
    const skuTargets     = loadSkuTargets();

    const skuMap = {};
    for (const item of lineItems) {
      if (!skuMap[item.sku]) skuMap[item.sku] = { qty: 0 };
      skuMap[item.sku].qty += item.qty;
    }

    const rows = ['"Artikelnr.";"Mindestbestand"'];
    for (const [sku, s] of Object.entries(skuMap)) {
      const target      = skuTargets[sku] ?? globalTarget;
      const proposedMin = Math.ceil((s.qty / months) * target);
      rows.push(`"${safeCsvStr(sku)}";${proposedMin}`);
    }

    const csv = rows.join('\r\n');
    const filename = `MRPeasy_Import_${new Date().toISOString().slice(0,10)}.csv`;
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.send('\uFEFF' + csv);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Artikel-Übersicht ────────────────────────────────────────────────────────

app.get('/api/artikel', async (req, res) => {
  try {
    const cfg             = loadConfig();
    const days            = parseInt(req.query.days) || cfg.forecastDays || 90;
    const globalTarget    = cfg.targetMonths || 2;
    const globalLieferzeit = cfg.lieferzeitWochen || 2;
    const months          = days / 30;

    const artData    = loadArticles();
    const artItems   = artData.items || {};
    const skuTargets = loadSkuTargets();

    let salesBySku = {};
    if (cfg.shopifyDomain && cfg.shopifyToken) {
      const shopify   = new ShopifyClient(cfg.shopifyDomain, cfg.shopifyToken);
      const lineItems = await shopify.getLineItems(days);
      for (const item of lineItems) {
        if (!salesBySku[item.sku]) salesBySku[item.sku] = { qty: 0, kg: 0 };
        salesBySku[item.sku].qty += item.qty;
        salesBySku[item.sku].kg  += item.kg;
      }
    }

    const result = Object.values(artItems).map(art => {
      const artSku      = skuTargets[art.nr] || {};
      const s           = salesBySku[art.nr] || { qty: 0, kg: 0 };
      const isKg        = (art.unit || '').toLowerCase() === 'kg';
      const verbrauch   = isKg ? s.kg : s.qty;
      const verbrauchPM = verbrauch / months;
      const verbrauchPW = verbrauchPM / 4.33;
      const bestand     = art.available || 0;

      // Per-SKU Overrides oder Global-Default
      const target        = artSku.targetMonths     ?? globalTarget;
      const lieferzeit    = artSku.lieferzeitWochen ?? globalLieferzeit;
      const purchasePrice = artSku.purchasePrice    ?? null;

      // Reichweite
      const reichweiteMonate = verbrauchPM > 0 ? +(bestand / verbrauchPM).toFixed(1) : null;
      const reichweiteWochen = verbrauchPW > 0 ? +(bestand / verbrauchPW).toFixed(1) : null;

      // Stockout Risk: wie viele Wochen Puffer nach Lieferankunft
      const stockoutRisk = reichweiteWochen !== null ? +(reichweiteWochen - lieferzeit).toFixed(1) : null;

      // Slow Mover: Reichweite > 2× Ziel
      const slowMover = reichweiteMonate !== null && reichweiteMonate > 2 * target;

      // Gebundenes Kapital
      const gebundenesKapital = purchasePrice !== null ? +(bestand * purchasePrice).toFixed(2) : null;

      // Vorschlag Mindestbestand
      const vorschlag = Math.ceil(verbrauchPM * target);

      // Handlungsempfehlung (Priorität: Bestell-Dringlichkeit > Slow Mover > OK)
      let handlung;
      if (verbrauchPM === 0) {
        handlung = 'kein-bedarf';
      } else if (stockoutRisk !== null && stockoutRisk < 0) {
        handlung = 'jetzt-bestellen';  // Lager läuft vor Lieferung leer
      } else if (stockoutRisk !== null && stockoutRisk <= 1) {
        handlung = 'beobachten';       // weniger als 1 Woche Puffer
      } else if (slowMover) {
        handlung = 'ueberbestand';
      } else {
        handlung = 'ok';
      }

      // Status (für Farb-Logik Reichweite vs. Ziel)
      const status = reichweiteMonate === null ? 'kein-bedarf'
        : reichweiteMonate < target * 0.5 ? 'kritisch'
        : reichweiteMonate < target       ? 'warn'
        : 'ok';

      const isFertigware = (art.group || '') !== 'Rohstoffe';
      const beutelPW     = isFertigware ? +(s.qty / (days / 7)).toFixed(1) : null;
      const zuProduzieren = isFertigware && art.minQty > 0 && bestand < art.minQty
        ? Math.ceil(art.minQty - bestand) : 0;

      return {
        nr: art.nr, name: art.name, group: art.group || '',
        unit: art.unit || '', weightKg: art.weightKg || 0,
        bestand, available: art.available || 0, minQty: art.minQty || 0,
        verbrauchPM:       +verbrauchPM.toFixed(2),
        verbrauchKgPM:     +(s.kg / months).toFixed(2),
        beutelPW,
        zuProduzieren,
        reichweiteMonate,
        reichweiteWochen,
        stockoutRisk,
        slowMover,
        gebundenesKapital,
        targetMonths:      target,
        targetCustom:      artSku.targetMonths !== undefined,
        lieferzeitWochen:  lieferzeit,
        lieferzeitCustom:  artSku.lieferzeitWochen !== undefined,
        purchasePrice,
        vorschlag,
        deltaMin:  art.minQty > 0 ? vorschlag - art.minQty : null,
        unterMin:  art.minQty > 0 && bestand < art.minQty,
        hasSales:  verbrauch > 0,
        handlung,
        status,
      };
    });

    // Sortierung: jetzt-bestellen → beobachten → ok → ueberbestand → kein-bedarf
    const handlungOrder = { 'jetzt-bestellen': 0, 'beobachten': 1, 'ok': 2, 'ueberbestand': 3, 'kein-bedarf': 4 };
    result.sort((a, b) => (handlungOrder[a.handlung] ?? 5) - (handlungOrder[b.handlung] ?? 5));

    res.json({
      period:      { days, months: +months.toFixed(1), globalTarget, globalLieferzeit },
      updatedAt:   artData.updatedAt || null,
      items:       result,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Update-Log ───────────────────────────────────────────────────────────────

app.get('/api/log', (req, res) => {
  res.json(loadLog());
});

// ─── Server starten ───────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n✓ Forecast Tool läuft auf http://localhost:${PORT}`);
  console.log('  Öffne den Link im Browser um zu starten.\n');
});
