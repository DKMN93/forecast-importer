require('dotenv').config();

// Persistenter Datenpfad — auf Railway: DATA_DIR=/data (Railway Volume), lokal: __dirname
const DATA_DIR = process.env.DATA_DIR || __dirname;
if (DATA_DIR !== __dirname && !require('fs').existsSync(DATA_DIR))
  require('fs').mkdirSync(DATA_DIR, { recursive: true });

const express  = require('express');
const fs       = require('fs');
const path     = require('path');
const crypto   = require('crypto');

const SERVER_START = new Date().toISOString();
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
const CONFIG_FILE = path.join(DATA_DIR, 'config.json');
const LOG_FILE    = path.join(DATA_DIR, 'update-log.json');

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
    shopifyDomain:     cfg.shopifyDomain || '',
    shopifyToken:      cfg.shopifyToken ? '***' : '',
    targetMonths:      cfg.targetMonths  || cfg.targetWeeks || 2,
    forecastDays:      cfg.forecastDays  || 90,
    lieferzeitWochen:  cfg.lieferzeitWochen || 2,
    // Lager-Zielreichweiten (Tage)
    rohwareTargetDays: cfg.rohwareTargetDays || 28,
    fbmTargetDays:     cfg.fbmTargetDays     || 30,
    transitTargetDays: cfg.transitTargetDays || 7,
    fbaTargetDays:     cfg.fbaTargetDays     || 35,
    configured:        !!(cfg.shopifyDomain && cfg.shopifyToken),
    deployedAt:        SERVER_START,
  });
});

app.post('/api/config', (req, res) => {
  const cfg = loadConfig();
  const { shopifyDomain, shopifyToken, shopifyClientId, shopifyClientSecret,
          targetMonths, forecastDays, lieferzeitWochen,
          rohwareTargetDays, fbmTargetDays, transitTargetDays, fbaTargetDays } = req.body;

  if (shopifyDomain)       cfg.shopifyDomain       = shopifyDomain.replace(/^https?:\/\//, '').replace(/\/$/, '');
  if (shopifyClientId)     cfg.shopifyClientId     = shopifyClientId;
  if (shopifyClientSecret && shopifyClientSecret !== '***') cfg.shopifyClientSecret = shopifyClientSecret;
  if (shopifyToken && shopifyToken !== '***')       cfg.shopifyToken        = shopifyToken;
  if (targetMonths)        cfg.targetMonths        = parseFloat(targetMonths);
  if (forecastDays)        cfg.forecastDays        = parseInt(forecastDays);
  if (lieferzeitWochen)    cfg.lieferzeitWochen    = parseFloat(lieferzeitWochen);
  if (rohwareTargetDays)   cfg.rohwareTargetDays   = parseInt(rohwareTargetDays);
  if (fbmTargetDays)       cfg.fbmTargetDays       = parseInt(fbmTargetDays);
  if (transitTargetDays)   cfg.transitTargetDays   = parseInt(transitTargetDays);
  if (fbaTargetDays)       cfg.fbaTargetDays       = parseInt(fbaTargetDays);

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

    // Produktionsklassen: verkaufte Beutel + dynamische Produktionszeit pro Abfüllklasse
    // zuProduzieren = MAX(0, Zielbestand − verfügbar), wobei Zielbestand = Ø/Monat × targetMonths
    const klassenMap = {};
    for (const [sku, s] of Object.entries(skuMap)) {
      const art = artItems[sku];
      if (!art || !art.abfuellklasse) continue;
      const kl = art.abfuellklasse;
      if (!klassenMap[kl]) klassenMap[kl] = { klasse: kl, fuellrate: art.fuellrateProStunde || 0, beutelVerkauft: 0, zuProduzieren: 0, zielbestand: 0 };
      klassenMap[kl].beutelVerkauft += s.qty;
      const avgPerMonth = s.qty / months;
      const sollBestand = avgPerMonth * targetMonths;
      const deficit = Math.max(0, Math.ceil(sollBestand - (art.available || 0)));
      klassenMap[kl].zuProduzieren += deficit;
      klassenMap[kl].zielbestand   += Math.ceil(sollBestand);
    }
    const produktionsklassen = Object.values(klassenMap)
      .filter(k => k.beutelVerkauft > 0 || k.zuProduzieren > 0)
      .map(k => ({
        klasse:             k.klasse,
        fuellrate:          k.fuellrate,
        beutelVerkauft:     k.beutelVerkauft,
        beutelProTag:       +(k.beutelVerkauft / days).toFixed(1),
        zielbestand:        k.zielbestand,
        zuProduzieren:      k.zuProduzieren,
        produktionsstunden: k.fuellrate > 0 ? +(k.zuProduzieren / k.fuellrate).toFixed(1) : null,
        targetMonths,
      }))
      .sort((a, b) => a.klasse.localeCompare(b.klasse));

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
      produktionsklassen,
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

const STOCK_FILE         = path.join(DATA_DIR, 'stock.json');
const TRANSIT_STOCK_FILE = path.join(DATA_DIR, 'stock-transit.json');
const STOCK_FULL_FILE    = path.join(DATA_DIR, 'stock-full.json');
const FBA_STOCK_FILE     = path.join(DATA_DIR, 'fba-stock.json');
const FBA_SHIPMENTS_FILE = path.join(DATA_DIR, 'fba-shipments.json');
const PO_FILE            = path.join(DATA_DIR, 'purchase-orders.json');

function loadStock() {
  try { return JSON.parse(fs.readFileSync(STOCK_FILE, 'utf8')); }
  catch { return {}; }
}
function loadStockFull() {
  try { return JSON.parse(fs.readFileSync(STOCK_FULL_FILE, 'utf8')); }
  catch { return { items: [] }; }
}
function loadTransitStock() {
  try { return JSON.parse(fs.readFileSync(TRANSIT_STOCK_FILE, 'utf8')); }
  catch { return {}; }
}
function loadFbaStock() {
  try { return JSON.parse(fs.readFileSync(FBA_STOCK_FILE, 'utf8')); }
  catch { return {}; }
}
function loadFbaShipments() {
  try { return JSON.parse(fs.readFileSync(FBA_SHIPMENTS_FILE, 'utf8')); }
  catch { return { shipments: [], inTransit: {} }; }
}
function loadPurchaseOrders() {
  try { return JSON.parse(fs.readFileSync(PO_FILE, 'utf8')); }
  catch { return { incoming: {} }; }
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
    const idx = name => headers.indexOf(name);
    const iSku      = idx(isDE ? 'Artikelnr.'        : 'Part No.');
    const iStock    = idx(isDE ? 'Auf Lager'          : 'In stock');
    const iAvail    = idx(isDE ? 'Verfügbar'          : 'Available');
    const iName     = idx(isDE ? 'Artikelbezeichnung' : 'Part description');
    const iUnit     = idx(isDE ? 'Maßeinheit'         : 'UoM');
    const iWeight   = idx('Gewicht in kg');
    const iStandort = idx('Standort');
    const iGruppe   = idx('Gruppenname');        // nur Lots-Export
    const iCost     = idx('Kosten pro Einheit'); // nur Lots-Export

    if (iSku === -1 || iStock === -1) return res.status(400).json({ error: 'Unbekanntes CSV-Format' });

    const IGNORIERTE_GRUPPEN = new Set(['Beutel', 'Etiketten', 'Kartons']);
    const pf = v => parseFloat((v || '0').replace(',', '.')) || 0;

    // stockMain / stockTransit: gefiltert für Planungsberechnungen
    const stockMain    = {};
    const stockTransit = {};
    // stockFull: vollständig inkl. Packmittel + gewichtete Ø-Kosten für Inventur-Export
    const stockFull    = {}; // key = "standort|SKU"

    for (let i = 1; i < lines.length; i++) {
      const cols    = parseRow(lines[i]);
      const sku     = (cols[iSku] || '').toUpperCase();
      if (!sku) continue;
      const standort = iStandort >= 0 ? cols[iStandort] : 'Main site';
      if (standort === 'Amazon FBA') continue; // ERP-Umbuchung, immer ignorieren

      const gruppe   = iGruppe >= 0 ? cols[iGruppe] : '';
      const inStock  = pf(cols[iStock]);
      const avail    = pf(cols[iAvail]);
      const weight   = pf(cols[iWeight]);
      const cost     = iCost >= 0 ? pf(cols[iCost]) : 0;

      // ── stock-full: alle Standorte, alle Gruppen (inkl. Packmittel) ──
      const fullKey = standort + '|' + sku;
      if (!stockFull[fullKey]) {
        stockFull[fullKey] = {
          sku, standort, name: cols[iName] || '', gruppe,
          unit: cols[iUnit] || 'Stk.', weightKg: weight,
          inStock: 0, available: 0, costSum: 0, costQty: 0,
        };
      }
      stockFull[fullKey].inStock   += inStock;
      stockFull[fullKey].available += avail;
      if (cost > 0 && inStock > 0) {
        stockFull[fullKey].costSum += cost * inStock;
        stockFull[fullKey].costQty += inStock;
      }

      // ── stockMain / stockTransit: Packmittel ausfiltern, für Planung ──
      if (IGNORIERTE_GRUPPEN.has(gruppe)) continue;
      const target = standort === 'Transit Amazon' ? stockTransit : stockMain;
      if (!target[sku]) target[sku] = { sku, name: cols[iName] || '', unit: cols[iUnit] || 'Stk.', inStock: 0, available: 0, weightKg: weight };
      target[sku].inStock   += inStock;
      target[sku].available += avail;
    }

    const now = new Date().toISOString();
    fs.writeFileSync(STOCK_FILE,         JSON.stringify({ updatedAt: now, items: stockMain    }, null, 2));
    fs.writeFileSync(TRANSIT_STOCK_FILE, JSON.stringify({ updatedAt: now, items: stockTransit }, null, 2));

    // stock-full: gewichteten Ø-Einheitspreis berechnen und speichern
    const fullItemsArr = Object.values(stockFull).map(it => {
      const avgCost = it.costQty > 0 ? +(it.costSum / it.costQty).toFixed(6) : 0;
      return {
        sku: it.sku, standort: it.standort, name: it.name, gruppe: it.gruppe,
        unit: it.unit, weightKg: it.weightKg,
        available: it.available, inStock: it.inStock,
        avgCostPerUnit: avgCost,
        totalValue: +(it.available * avgCost).toFixed(2),
        costSum: it.costSum,  // für globalen Ø-Preis im Inventur-Export
        costQty: it.costQty,
      };
    });
    fs.writeFileSync(STOCK_FULL_FILE, JSON.stringify({ updatedAt: now, items: fullItemsArr }, null, 2));

    res.json({
      ok: true,
      skuCount:        Object.keys(stockMain).length,
      mainSkuCount:    Object.keys(stockMain).length,
      transitSkuCount: Object.keys(stockTransit).length,
      transitSkus:     Object.keys(stockTransit),
      language: isDE ? 'DE' : 'EN',
    });
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

const ARTICLES_FILE = path.join(DATA_DIR, 'articles.json');

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
    const iAbfuell  = idx('Abfüllklasse');
    const iFuell    = idx('Füllrate Plan pro Stunde');
    const iActive   = idx('ACTIVE');

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
        abfuellklasse:     iAbfuell >= 0 ? (cols[iAbfuell] || '') : '',
        fuellrateProStunde: iFuell   >= 0 ? pf(cols[iFuell])      : 0,
        active:      iActive >= 0 ? (cols[iActive] || '').trim().toLowerCase() === 'ja' : false,
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

// ─── Inventur-Export (Steuerberater) ─────────────────────────────────────────
// Stichtags-Inventur: alle Lagerplätze mit Mengen + Einstandskosten.
// Amazon FBA: verfügbar + reserviert (FC-Transfers + Kundenbestellungen) = Gesamteigentum bei Amazon.

app.get('/api/export/inventory', (req, res) => {
  try {
    const dateStr = new Date().toISOString().slice(0, 10);

    const fullData  = loadStockFull();
    const fullItems = fullData.items || [];
    const fbaData   = loadFbaStock();
    const fbaItems  = fbaData.items || {};
    const shipData  = loadFbaShipments();
    const inTransit = shipData.inTransit || {};

    // Kosten-Lookup: globaler gewichteter Ø-Einstandspreis über alle Standorte pro SKU.
    // Verhindert, dass die Reihenfolge im CSV (Transit vor Main) den Preis verfälscht.
    const globalCostAgg = {};
    for (const it of fullItems) {
      if (!globalCostAgg[it.sku]) globalCostAgg[it.sku] = { costSum: 0, costQty: 0 };
      globalCostAgg[it.sku].costSum += it.costSum || 0;
      globalCostAgg[it.sku].costQty += it.costQty || 0;
    }
    const getCost = sku => {
      const base = sku.replace(/-FBA$/i, '');
      const g = globalCostAgg[base] || globalCostAgg[sku];
      if (g && g.costQty > 0) return +(g.costSum / g.costQty).toFixed(6);
      return 0;
    };

    // Gruppen-Etiketten für Steuerberater
    const gruppeLabel = g => {
      if (['Beutel', 'Etiketten', 'Kartons'].includes(g)) return 'Packmittel / Hilfsstoffe';
      return g || 'Fertigware';
    };
    const lagerBeschreibung = (standort, gruppe, sku) => {
      if (standort === 'Transit Amazon') return 'FBA-Transit Lager (bei uns, versandbereit)';
      if (gruppe === 'Rohstoffe') return 'Hauptlager – Rohstoffe';
      if (['Beutel', 'Etiketten', 'Kartons'].includes(gruppe)) return 'Hauptlager – Packmittel';
      if (sku.endsWith('-9')) return 'Hauptlager – Herstellergebinde (Rohware in Gebinde)';
      return 'Hauptlager – Fertigware';
    };

    const rows = [];
    const headers = [
      'Lagerplatz', 'Lagerplatz Beschreibung',
      'Artikelnummer', 'Artikelname', 'Produktgruppe',
      'Verfügbare Menge', 'Maßeinheit',
      'Einheitspreis Ø (€)', 'Gesamtwert (€)',
    ];

    // ── 1. Main Lager + Transit Lager (aus stock-full.json) ──
    const GRUPPEN_ORDER = { 'Rohstoffe': 0, 'Fertigware': 1, 'Beutel': 2, 'Etiketten': 2, 'Kartons': 2 };
    const mainSorted = fullItems
      .filter(it => it.available > 0 && it.standort !== 'Amazon FBA')
      .sort((a, b) => {
        if (a.standort !== b.standort) return a.standort === 'Transit Amazon' ? 1 : -1;
        return (GRUPPEN_ORDER[a.gruppe] ?? 9) - (GRUPPEN_ORDER[b.gruppe] ?? 9) || a.sku.localeCompare(b.sku);
      });

    for (const it of mainSorted) {
      const standortLabel = it.standort === 'Transit Amazon' ? 'Transit Lager' : 'Hauptlager';
      rows.push([
        standortLabel,
        lagerBeschreibung(it.standort, it.gruppe, it.sku),
        it.sku,
        it.name,
        gruppeLabel(it.gruppe),
        it.available,
        it.unit,
        it.avgCostPerUnit > 0 ? it.avgCostPerUnit : '',
        it.totalValue    > 0 ? it.totalValue     : '',
      ]);
    }

    // ── 2. Ware unterwegs zu Amazon (Sellerboard Shipments) ──
    for (const [skuFba, trData] of Object.entries(inTransit)) {
      const enRoute = Math.max(0, (trData.shipped || 0) - (trData.received || 0));
      if (enRoute <= 0) continue;
      const baseSku = skuFba.replace(/-FBA$/i, '');
      const fba     = fbaItems[skuFba];
      const cost    = getCost(baseSku);
      rows.push([
        'Amazon FBA – Unterwegs',
        'Versendet, auf dem Weg zum Amazon Warehouse (noch nicht eingebucht)',
        baseSku,
        fba ? fba.title.substring(0, 100) : baseSku,
        'Fertigware',
        enRoute,
        'Stk.',
        cost > 0 ? cost : '',
        cost > 0 ? +(enRoute * cost).toFixed(2) : '',
      ]);
    }

    // ── 3. Amazon FBA Lager (Sellerboard) ──
    // Verfügbar + Reserviert (FC-Transfers & Kundenbestellungen) = Gesamteigentum bei Amazon
    for (const [skuFba, fba] of Object.entries(fbaItems)) {
      const avail    = fba.fbaStock    || 0;
      const reserved = fba.fbaReserved || 0;
      if (avail <= 0 && reserved <= 0) continue;
      const baseSku = skuFba.replace(/-FBA$/i, '');
      const cost    = getCost(baseSku);
      const title   = (fba.title || baseSku).substring(0, 100);
      if (avail > 0) {
        rows.push([
          'Amazon FBA Lager',
          'Im Amazon Warehouse – Verfügbar zum Kauf',
          baseSku,
          title,
          'Fertigware',
          avail,
          'Stk.',
          cost > 0 ? cost : '',
          cost > 0 ? +(avail * cost).toFixed(2) : '',
        ]);
      }
      if (reserved > 0) {
        rows.push([
          'Amazon FBA Lager',
          'Im Amazon Warehouse – Reserviert (FC-Transfer & Kundenbestellungen)',
          baseSku,
          title,
          'Fertigware',
          reserved,
          'Stk.',
          cost > 0 ? cost : '',
          cost > 0 ? +(reserved * cost).toFixed(2) : '',
        ]);
      }
    }

    // ── Summenzeile ──
    const totalVal = rows.reduce((s, r) => s + (typeof r[8] === 'number' ? r[8] : 0), 0);
    rows.push(['', '', '', '', '', '', '', 'GESAMT', +totalVal.toFixed(2)]);

    // ── Excel bauen ──
    const sheetData = [headers, ...rows];
    const wb = xlsx.utils.book_new();
    const ws = xlsx.utils.aoa_to_sheet(sheetData);
    ws['!cols'] = [
      {wch:24}, {wch:48}, {wch:18}, {wch:55}, {wch:22},
      {wch:16}, {wch:10}, {wch:18}, {wch:16},
    ];

    // Datenquellen-Info als zweites Sheet
    const infoRows = [
      ['Inventur-Export', `Stichtag: ${dateStr}`],
      [],
      ['Datenquelle', 'Datei', 'Stand'],
      ['Hauptlager & Transit', 'MRPeasy Chargen-Export (stock_lots)', fullData.updatedAt || '—'],
      ['Amazon FBA Lager', 'Sellerboard Lager-Export', fbaData.updatedAt || '—'],
      ['Amazon FBA unterwegs', 'Sellerboard Sendungen-Export', shipData.updatedAt || '—'],
      [],
      ['Hinweis:', 'Es werden nur verfügbare Mengen ("Verfügbar") gewertet.'],
      ['', 'Reservierte Ware (Gebucht) und Ware im Zulauf sind nicht enthalten.'],
    ];
    const wsInfo = xlsx.utils.aoa_to_sheet(infoRows);
    wsInfo['!cols'] = [{wch:30}, {wch:50}, {wch:20}];

    xlsx.utils.book_append_sheet(wb, ws,     `Inventur_${dateStr}`);
    xlsx.utils.book_append_sheet(wb, wsInfo, 'Datenquellen');

    const buf = xlsx.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Disposition', `attachment; filename="Inventur_${dateStr}.xlsx"`);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.send(buf);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Stücklisten (Parts) Upload ───────────────────────────────────────────────

const PARTS_FILE = path.join(DATA_DIR, 'parts-mapping.json');

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

const SKU_TARGETS_FILE = path.join(DATA_DIR, 'sku-targets.json');

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

// ─── Transit Lager Upload ─────────────────────────────────────────────────────

app.post('/api/upload-stock-transit', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Keine Datei' });
    const text = req.file.buffer.toString('utf8').replace(/^﻿/, '');
    const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
    if (!lines.length) return res.status(400).json({ error: 'Leere Datei' });
    const parseRow = l => l.split(';').map(v => v.replace(/^"|"$/g, '').trim());
    const headers = parseRow(lines[0]);
    const idx = name => headers.indexOf(name);
    const isDE = headers.includes('Artikelnr.');
    const iSku    = idx(isDE ? 'Artikelnr.'        : 'Part No.');
    const iStock  = idx(isDE ? 'Auf Lager'          : 'In stock');
    const iAvail  = idx(isDE ? 'Verfügbar'          : 'Available');
    const iName   = idx(isDE ? 'Artikelbezeichnung' : 'Part description');
    const iUnit   = idx(isDE ? 'Maßeinheit'         : 'UoM');
    const iWeight = idx('Gewicht in kg');
    if (iSku === -1 || iStock === -1) return res.status(400).json({ error: 'Unbekanntes Format' });
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
    fs.writeFileSync(TRANSIT_STOCK_FILE, JSON.stringify({ updatedAt: new Date().toISOString(), items: stock }, null, 2));
    res.json({ ok: true, skuCount: Object.keys(stock).length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/stock-transit', (req, res) => res.json(loadTransitStock()));

// ─── Sellerboard FBA Lager Upload (XLSX) ─────────────────────────────────────

app.post('/api/upload-sellerboard-lager', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Keine Datei' });
    const wb = xlsx.read(req.file.buffer, { type: 'buffer' });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    const rows = xlsx.utils.sheet_to_json(sheet, { header: 1, defval: '' });
    if (rows.length < 2) return res.status(400).json({ error: 'Datei leer oder ungültig' });

    // Normalize headers: strip newlines/extra whitespace for fuzzy matching
    const normH = h => String(h).toLowerCase().replace(/[\n\r\s]+/g, ' ').trim();
    const headers = rows[0].map(h => normH(String(h)));
    const findCol = (...kws) => headers.findIndex(h => kws.every(kw => h.includes(kw.toLowerCase())));

    const iSku        = findCol('sku');
    const iAsin       = findCol('asin');
    const iTitle      = findCol('title');
    const iStock      = findCol('fba/fbm stock');
    const iReserved   = findCol('reserved');
    const iVelocity   = findCol('estimated', 'velocity');
    const iDaysLeft   = findCol('days', 'stock', 'left');
    const iRecommended = findCol('recommended', 'quantity', 'reorder');
    const iSentToFba  = findCol('sent', 'fba');
    const iShipIn     = findCol('recommended', 'ship-in');

    if (iSku === -1) return res.status(400).json({ error: 'SKU-Spalte nicht gefunden' });
    if (iStock === -1) return res.status(400).json({ error: 'FBA/FBM Stock-Spalte nicht gefunden' });

    const pf = v => { const n = parseFloat(String(v).replace(',', '.')); return isNaN(n) ? 0 : n; };

    const items = {};
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const sku = String(row[iSku] || '').trim();
      if (!sku || !sku.toUpperCase().endsWith('-FBA')) continue;
      items[sku] = {
        sku,
        asin:              String(row[iAsin]  || '').trim(),
        title:             String(row[iTitle] || '').trim().substring(0, 80),
        fbaStock:          pf(row[iStock]),
        fbaReserved:       iReserved >= 0 ? pf(row[iReserved]) : 0,
        velocity:          iVelocity    >= 0 ? pf(row[iVelocity])    : 0,
        daysLeft:          iDaysLeft    >= 0 ? pf(row[iDaysLeft])    : 0,
        recommendedReorder: iRecommended >= 0 ? pf(row[iRecommended]) : 0,
        sentToFba:         iSentToFba   >= 0 ? pf(row[iSentToFba])   : 0,
        recommendedShipIn: iShipIn      >= 0 ? pf(row[iShipIn])      : 0,
      };
    }

    fs.writeFileSync(FBA_STOCK_FILE, JSON.stringify({ updatedAt: new Date().toISOString(), items }, null, 2));
    res.json({ ok: true, skuCount: Object.keys(items).length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Sellerboard Sendungen Upload (XLSX) ─────────────────────────────────────

app.post('/api/upload-sellerboard-shipments', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Keine Datei' });
    const wb = xlsx.read(req.file.buffer, { type: 'buffer' });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    const rows = xlsx.utils.sheet_to_json(sheet, { header: 1, defval: '' });
    if (rows.length < 2) return res.status(400).json({ error: 'Datei leer' });

    const normH = h => String(h).toLowerCase().replace(/[\n\r\s]+/g, ' ').trim();
    const headers = rows[0].map(h => normH(String(h)));
    const findCol = (...kws) => headers.findIndex(h => kws.every(kw => h.includes(kw)));

    const iShipId   = findCol('shipment id');
    const iProducts = findCol('products');
    const iShipped  = findCol('units shipped');
    const iReceived = headers.indexOf('units received'); // exact match — "% of units received" must not match
    const iStatus   = findCol('status');
    const iDate     = findCol('date');

    if (iShipId === -1 || iProducts === -1) return res.status(400).json({ error: 'Unbekanntes Shipments-Format' });

    const pf = v => { const n = parseFloat(String(v).replace(',', '.')); return isNaN(n) ? 0 : n; };
    const ACTIVE_STATUSES = ['RECEIVING', 'WORKING', 'SHIPPED', 'IN_TRANSIT'];

    const shipments = [];
    let current = null;

    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const shipId   = String(row[iShipId]   || '').trim();
      const products = String(row[iProducts] || '').trim();
      const status   = String(row[iStatus]   || '').trim().toUpperCase();

      if (shipId.startsWith('FBA')) {
        current = { shipmentId: shipId, status: status || '', date: String(row[iDate] || '').trim(), products: [] };
        shipments.push(current);
      }

      if (!current) continue;

      // Update status if found on a later row for same shipment
      if (!shipId && status && !current.status) current.status = status;
      if (!shipId && status && ACTIVE_STATUSES.includes(status)) current.status = status;

      // Product line: "Title/ASIN/SKU" — SKU is after the last "/"
      if (products.includes('/')) {
        const parts = products.split('/');
        const sku = parts[parts.length - 1].trim();
        if (sku && sku.length > 3 && !sku.startsWith('Selected')) {
          current.products.push({ sku, shipped: iShipped >= 0 ? pf(row[iShipped]) : 0, received: iReceived >= 0 ? pf(row[iReceived]) : 0 });
        }
      }
    }

    // Build per-SKU inTransit index for active shipments
    const inTransit = {};
    for (const s of shipments) {
      if (!ACTIVE_STATUSES.includes(s.status)) continue;
      for (const p of s.products) {
        if (!inTransit[p.sku]) inTransit[p.sku] = { shipped: 0, received: 0 };
        inTransit[p.sku].shipped  += p.shipped;
        inTransit[p.sku].received += p.received;
      }
    }

    fs.writeFileSync(FBA_SHIPMENTS_FILE, JSON.stringify({ updatedAt: new Date().toISOString(), shipments, inTransit }, null, 2));
    res.json({ ok: true, shipmentCount: shipments.length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Bestellungen (Purchase Orders) Upload ───────────────────────────────────
// Importiert offene Einkaufsbestellungen aus MRPeasy CSV.
// Relevante Zeilen: Status (erste Spalte) = "Verschickt" → im Zulauf.
// Menge = kg (ERP speichert Rohware in kg).

app.post('/api/upload-purchase-orders', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Keine Datei' });
    const text = req.file.buffer.toString('utf8').replace(/^﻿/, '');
    const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
    if (!lines.length) return res.status(400).json({ error: 'Leere Datei' });

    const parseRow = l => l.split(';').map(v => v.replace(/^"|"$/g, '').trim());
    const headers = parseRow(lines[0]);
    const idx = name => headers.indexOf(name);

    const iNr       = idx('Nummer');
    const iSku      = idx('Artikelnr.');
    const iName     = idx('Artikelbezeichnung');
    const iMenge    = idx('Menge');
    const iLiefdat  = idx('Erw. Lieferdatum');
    const iLieferant = idx('Lieferant');
    // Zwei "Status"-Spalten — erste ist der Zeilenstatus
    let iStatus = -1;
    for (let i = 0; i < headers.length; i++) {
      if (headers[i] === 'Status') { iStatus = i; break; }
    }

    if (iSku === -1 || iMenge === -1) return res.status(400).json({ error: 'Unbekanntes Format' });

    const pf = v => parseFloat((v || '0').replace(',', '.')) || 0;
    const incoming = {}; // axyNr → { sku, name, totalKg, lieferant, lines[] }

    for (let i = 1; i < lines.length; i++) {
      const cols   = parseRow(lines[i]);
      const status = iStatus >= 0 ? cols[iStatus] : '';
      // Nur "Verschickt" = unterwegs, noch nicht angekommen
      if (status !== 'Verschickt') continue;

      const sku   = cols[iSku];
      const menge = pf(cols[iMenge]); // kg
      if (!sku || menge <= 0) continue;

      if (!incoming[sku]) incoming[sku] = {
        sku, name: cols[iName] || '',
        lieferant: iLieferant >= 0 ? cols[iLieferant] : '',
        totalKg: 0, orders: [],
      };
      incoming[sku].totalKg += menge;
      incoming[sku].orders.push({
        poNr:    cols[iNr]      || '',
        menge,
        liefdat: iLiefdat >= 0 ? cols[iLiefdat] : '',
      });
    }

    fs.writeFileSync(PO_FILE, JSON.stringify({ updatedAt: new Date().toISOString(), incoming }, null, 2));
    res.json({ ok: true, inTransitCount: Object.keys(incoming).length });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/purchase-orders', (req, res) => res.json(loadPurchaseOrders()));

// ─── FBA Wochenplanung ────────────────────────────────────────────────────────
// Lead times: 7 Tage Produktion + 14 Tage Amazon Einlagerung = 21 Tage gesamt

const FBA_PROD_DAYS   = 7;
const FBA_AMAZON_DAYS = 14;
const FBA_LEAD_TOTAL  = FBA_PROD_DAYS + FBA_AMAZON_DAYS;

app.get('/api/fba-planung', (req, res) => {
  try {
    const cfg       = loadConfig();
    const fbaTargetDays     = cfg.fbaTargetDays     || 35;
    const transitTargetDays = cfg.transitTargetDays || 7;

    const fbaData      = loadFbaStock();
    const fbaItems     = fbaData.items || {};
    const mainData     = loadStock();
    const mainItems    = mainData.items || {};
    const transitData  = loadTransitStock();
    const transitItems = transitData.items || {};
    const artData      = loadArticles();
    const artItems     = artData.items || {};
    const shipData     = loadFbaShipments();
    const inTransit    = shipData.inTransit || {};

    // Datenbasis (eine Quelle pro Lager):
    // MRPeasy "Main site"      → mainItems (FBM-Fertigware + Rohware)
    // MRPeasy "Transit Amazon" → transitItems (FBA-Sticker drauf, noch bei uns, noch nicht versendet)
    // MRPeasy "Amazon FBA"     → ignoriert (Umbuchung damit Ware aus ERP verschwindet)
    // Sellerboard Shipments    → inTransit (versendet, auf dem Weg zu Amazon)
    // Sellerboard Lager        → fbaStock (aktuell im Amazon Warehouse verfügbar)

    const result = [];

    for (const [skuFba, fba] of Object.entries(fbaItems)) {
      const skuBase    = skuFba.replace(/-FBA$/i, '');
      const main       = mainItems[skuBase]    || { available: 0, inStock: 0 };
      const transit    = transitItems[skuBase] || { available: 0 };
      const art        = artItems[skuBase]     || null;

      const transitStock = transit.available;   // MRPeasy Transit Amazon: bei uns, FBA-Sticker drauf
      const trData       = inTransit[skuFba] || { shipped: 0, received: 0 };
      const enRoute      = Math.max(0, (trData.shipped || 0) - (trData.received || 0)); // Sellerboard Shipments: auf dem Weg
      const velocity     = fba.velocity > 0 ? fba.velocity
                         : (fba.daysLeft > 0 && fba.fbaStock > 0 ? fba.fbaStock / fba.daysLeft : 0);

      // Pipeline = FBA Transit (MRPeasy) + unterwegs zu Amazon (Shipments) + im Amazon Warehouse (Sellerboard)
      const pipelineTotal = fba.fbaStock + enRoute + transitStock;
      const pipelineDays  = velocity > 0 ? pipelineTotal / velocity : 999;

      // Was noch aus MAIN vorbereitet + versendet werden muss
      const recommendation   = fba.recommendedReorder || 0;
      const nochZuSenden     = Math.max(0, recommendation - enRoute - transitStock);

      // Aus FBM-Überbestand entnehmen?
      const fbmMinQty        = art ? (art.minQty || 0) : 0;
      const mainOverstock    = Math.max(0, main.available - fbmMinQty);
      const ausMainEntnehmen = Math.min(nochZuSenden, mainOverstock);
      const zuProduzierenFba = Math.max(0, nochZuSenden - ausMainEntnehmen);

      // Kritisch = unter Lead Time (kann nicht rechtzeitig nachfüllen)
      // Warn = unter Zielreichweite (35 Tage)
      const status = pipelineDays < FBA_LEAD_TOTAL ? 'kritisch'
                   : pipelineDays < fbaTargetDays  ? 'warn' : 'ok';

      result.push({
        skuFba, skuBase,
        name:     fba.title || (art ? art.name : skuBase),
        asin:     fba.asin || '',
        fbaStock:      fba.fbaStock,
        velocity:      +velocity.toFixed(2),
        daysLeft:      fba.daysLeft,
        transitStock,                        // MRPeasy Transit Amazon (bei uns, noch nicht versendet)
        enRoute,                             // Sellerboard Shipments (versendet, unterwegs)
        mainStock:     main.available,
        fbmMinQty,
        mainOverstock,
        pipelineTotal: Math.round(pipelineTotal),
        pipelineDays:  pipelineDays < 999 ? +pipelineDays.toFixed(1) : 999,
        recommendation: Math.round(recommendation),
        nochZuSenden:   Math.round(nochZuSenden),
        ausMainEntnehmen: Math.round(ausMainEntnehmen),
        zuProduzierenFba: Math.round(zuProduzierenFba),
        status,
      });
    }

    const stOrd = { kritisch: 0, warn: 1, ok: 2 };
    result.sort((a, b) => (stOrd[a.status] - stOrd[b.status]) || (a.pipelineDays - b.pipelineDays));

    res.json({
      updatedAt: { fbaStock: fbaData.updatedAt || null, main: mainData.updatedAt || null, shipments: shipData.updatedAt || null },
      leadTimes: { prodDays: FBA_PROD_DAYS, amazonDays: FBA_AMAZON_DAYS, totalDays: FBA_LEAD_TOTAL, fbaTargetDays, transitTargetDays },
      summary: {
        total:    result.length,
        kritisch: result.filter(r => r.status === 'kritisch').length,
        warn:     result.filter(r => r.status === 'warn').length,
        ok:       result.filter(r => r.status === 'ok').length,
        zuVersenden:   result.reduce((s, r) => s + r.nochZuSenden, 0),
        zuProduzieren: result.reduce((s, r) => s + r.zuProduzierenFba, 0),
      },
      items: result,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Aktive Artikel (ACTIVE = ja) ────────────────────────────────────────────

app.get('/api/active-articles', (req, res) => {
  try {
    const artData      = loadArticles();
    const artItems     = artData.items || {};
    const mainData     = loadStock();
    const mainItems    = mainData.items || {};
    const transitData  = loadTransitStock();
    const transitItems = transitData.items || {};
    const fbaData      = loadFbaStock();
    const fbaItems     = fbaData.items || {};

    const result = [];
    for (const [nr, art] of Object.entries(artItems)) {
      if (!art.active) continue;
      const skuFba      = nr + '-FBA';
      const main        = mainItems[nr]      || { available: 0 };
      const transit     = transitItems[nr]   || { available: 0 };
      const fba         = fbaItems[skuFba]   || null;
      const mainStock   = main.available    || 0;
      const transitStock = transit.available || 0;
      const fbaStock    = fba ? fba.fbaStock : null;
      const totalStock  = mainStock + transitStock + (fbaStock || 0);
      const unterMin    = art.minQty > 0 && mainStock < art.minQty;

      result.push({
        nr, name: art.name, group: art.group || '', unit: art.unit || '',
        mainStock, transitStock, fbaStock, hasFba: !!fba, totalStock,
        minQty: art.minQty || 0, unterMin,
        status: totalStock === 0 ? 'nullbestand' : 'ok',
      });
    }

    result.sort((a, b) => {
      if (a.status !== b.status) return a.status === 'nullbestand' ? -1 : 1;
      if (a.unterMin !== b.unterMin) return a.unterMin ? -1 : 1;
      return a.nr.localeCompare(b.nr);
    });

    res.json({
      total:       result.length,
      nullbestand: result.filter(r => r.status === 'nullbestand').length,
      unterMin:    result.filter(r => r.unterMin).length,
      updatedAt:   { articles: artData.updatedAt || null, main: mainData.updatedAt || null, transit: transitData.updatedAt || null, fba: fbaData.updatedAt || null },
      items: result,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Lagerübersicht (Multichannel, Demand-Matrix) ────────────────────────────
//
// Gruppiert alle Artikel nach Produktfamilie (Prefix, z.B. CREAT-FP).
// Pro Familie: 4 Lager + Demand-Matrix (FBM direkt + Bundles + FBA).

const getFamilyKey = sku =>
  sku.replace(/-FBA$/i, '').replace(/\.\d+$/, '').replace(/-\d+$/, '');

app.get('/api/lagerbestand', async (req, res) => {
  try {
    const cfg       = loadConfig();
    const days      = parseInt(req.query.days) || cfg.forecastDays || 90;
    const weeks     = days / 7;
    const fbmTargetDays    = cfg.fbmTargetDays  || 30;
    const fbaTargetDays    = cfg.fbaTargetDays  || 35;
    const FBM_TARGET_WEEKS = fbmTargetDays / 7;
    const FBA_TARGET_WEEKS = fbaTargetDays / 7;

    const artData      = loadArticles();
    const artItems     = artData.items || {};
    const mainData     = loadStock();
    const mainItems    = mainData.items || {};
    const transitData  = loadTransitStock();
    const transitItems = transitData.items || {};
    const fbaData      = loadFbaStock();
    const fbaItems     = fbaData.items || {};
    const partsData    = loadParts();
    const partsMap     = partsData.mapping || {};
    const skuTargets   = loadSkuTargets();

    // Shopify Demand (optional — graceful wenn nicht verbunden)
    let lineItems = [];
    if (cfg.shopifyDomain && cfg.shopifyToken) {
      const shopify = new ShopifyClient(cfg.shopifyDomain, cfg.shopifyToken);
      lineItems = await shopify.getLineItems(days);
    }

    // Demand-Aggregation pro Basis-SKU aus Shopify
    // shopify.js löst Bundles bereits auf: isBundle, bundleFactor, originalSku, qty=resolvedQty
    const shopifyDemand = {};
    for (const item of lineItems) {
      const sku = item.sku; // immer die Basis-SKU
      if (!shopifyDemand[sku]) shopifyDemand[sku] = { direct: 0, bundles: {} };
      if (!item.isBundle) {
        shopifyDemand[sku].direct += item.qty;
      } else {
        const f = item.bundleFactor;
        if (!shopifyDemand[sku].bundles[f])
          shopifyDemand[sku].bundles[f] = { originalSku: item.originalSku, factor: f, bundleSales: 0, baseUnits: 0 };
        shopifyDemand[sku].bundles[f].bundleSales += item.qty / f;
        shopifyDemand[sku].bundles[f].baseUnits   += item.qty;
      }
    }

    // Produktfamilien aufbauen
    const families = {};
    for (const [nr, art] of Object.entries(artItems)) {
      if ((art.group || '') === 'Rohstoffe') continue;
      const fk = getFamilyKey(nr);
      if (!families[fk]) families[fk] = {
        familyKey: fk, familyName: '', active: false,
        skus: {}, fbaSku: null, rohwareNr: null, rohwareName: null,
      };
      const fam = families[fk];
      // Größen-Suffix extrahieren (5, 5.3, 5-FBA, 9, …)
      const suffixMatch = nr.match(new RegExp('^' + fk.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '-(.+)$', 'i'));
      const suffix = suffixMatch ? suffixMatch[1] : nr;
      fam.skus[suffix] = { sku: nr, name: art.name, active: art.active,
        weightKg: art.weightKg || 0, unit: art.unit || 'Stk.', minQty: art.minQty || 0 };
      if (art.active) fam.active = true;
      if (!fam.familyName) {
        // Produktname: Größenangabe am Ende abschneiden
        fam.familyName = art.name.replace(/\s*[\-–]\s*\d+[\.,]?\d*\s*(kg|g|ml|l)\b.*/i, '').trim();
      }
      if (nr.toUpperCase().endsWith('-FBA')) fam.fbaSku = nr;
      if (!fam.rohwareNr && partsMap[fk]) {
        fam.rohwareNr   = partsMap[fk].rohwareNr;
        fam.rohwareName = partsMap[fk].rohwareName;
      }
    }

    // Ergebnis aufbauen
    const result = [];
    for (const fam of Object.values(families)) {
      const primarySku = fam.familyKey + '-5';  // 1kg = Basiseinheit
      const fba        = fam.fbaSku ? fbaItems[fam.fbaSku] : null;
      const main       = mainItems[primarySku]    || { available: 0, inStock: 0 };
      const transit    = transitItems[primarySku] || { available: 0 };
      const rawArt     = fam.rohwareNr ? artItems[fam.rohwareNr] : null;
      const artTarget  = skuTargets[primarySku] || {};
      const fbmTargetWeeks = artTarget.targetMonths ? artTarget.targetMonths * 4.33 : FBM_TARGET_WEEKS;

      // Demand-Matrix aus Shopify
      const sd = shopifyDemand[primarySku] || { direct: 0, bundles: {} };
      const directPW = sd.direct / weeks;
      const bundleRows = Object.values(sd.bundles)
        .map(b => ({
          originalSku:     b.originalSku,
          factor:          b.factor,
          bundleSalesPW:   +(b.bundleSales / weeks).toFixed(1),
          baseUnitsPW:     +(b.baseUnits   / weeks).toFixed(1),
        }))
        .sort((a, b) => a.factor - b.factor);
      const fbmBundleTotal = bundleRows.reduce((s, b) => s + b.baseUnitsPW, 0);
      const fbmTotal   = directPW + fbmBundleTotal;
      const fbaPerWeek = fba ? (fba.velocity || 0) * 7 : 0;
      const totalPW    = fbmTotal + fbaPerWeek;

      // Reichweiten
      const mainRwWeeks = fbmTotal > 0 ? main.available / fbmTotal : null;
      const fbaDays     = fba ? fba.daysLeft : null;

      // Status (schlechtester Kanal bestimmt Gesamtstatus)
      let status = 'ok';
      const fbmCrit  = mainRwWeeks !== null && mainRwWeeks < fbmTargetWeeks * 0.5;
      const fbmWarn  = mainRwWeeks !== null && mainRwWeeks < fbmTargetWeeks;
      const fbaCrit  = fba && fbaDays !== null && fbaDays < 21;
      const fbaWarn  = fba && fbaDays !== null && fbaDays < 42;
      if (fbmCrit || fbaCrit) status = 'kritisch';
      else if (fbmWarn || fbaWarn) status = 'warn';

      // Andere Größen aus der Familie (nicht -5 und nicht -FBA und nicht -9)
      const otherSizes = Object.entries(fam.skus)
        .filter(([suffix]) => suffix !== '5' && !suffix.toUpperCase().includes('FBA') && suffix !== '9')
        .map(([suffix, info]) => ({
          suffix, sku: info.sku, name: info.name, active: info.active,
          available: (mainItems[info.sku] || {}).available || 0,
          unit: info.unit,
        }));

      result.push({
        familyKey:   fam.familyKey,
        familyName:  fam.familyName || fam.familyKey,
        active:      fam.active,
        primarySku,
        fbaSku:      fam.fbaSku || null,
        status,
        stock: {
          rohware:  rawArt ? { sku: fam.rohwareNr, name: rawArt.name, available: rawArt.available, unit: rawArt.unit } : null,
          main:     { available: main.available, inStock: main.inStock },
          transit:  { available: transit.available },
          fba:      fba ? { available: fba.fbaStock, daysLeft: fba.daysLeft, velocity: fba.velocity || 0 } : null,
        },
        demand: {
          fbmDirect:     +directPW.toFixed(1),
          fbmBundles:    bundleRows,
          fbmBundleTotal: +fbmBundleTotal.toFixed(1),
          fbmTotal:      +fbmTotal.toFixed(1),
          fbaPerWeek:    +fbaPerWeek.toFixed(1),
          totalPerWeek:  +totalPW.toFixed(1),
          fbmShare: totalPW > 0 ? Math.round(fbmTotal / totalPW * 100) : 0,
          fbaShare: totalPW > 0 ? Math.round(fbaPerWeek / totalPW * 100) : 0,
          hasDemand: totalPW > 0,
        },
        reichweite: {
          mainWeeks:      mainRwWeeks !== null ? +mainRwWeeks.toFixed(1) : null,
          fbaDays,
          fbmTargetWeeks: +fbmTargetWeeks.toFixed(1),
          fbaTargetWeeks: FBA_TARGET_WEEKS,
        },
        otherSizes,
      });
    }

    const stOrd = { kritisch: 0, warn: 1, ok: 2 };
    result.sort((a, b) => {
      if (a.status !== b.status) return stOrd[a.status] - stOrd[b.status];
      if (a.active !== b.active) return a.active ? -1 : 1;
      return b.demand.totalPerWeek - a.demand.totalPerWeek;
    });

    res.json({
      period:    { days, weeks: +weeks.toFixed(1) },
      updatedAt: { articles: artData.updatedAt || null, main: mainData.updatedAt || null, fba: fbaData.updatedAt || null },
      summary: {
        total:    result.length,
        active:   result.filter(r => r.active).length,
        kritisch: result.filter(r => r.status === 'kritisch').length,
        warn:     result.filter(r => r.status === 'warn').length,
      },
      families: result,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Wochenplanung ────────────────────────────────────────────────────────────
// Beantwortet 5 Planungsfragen pro Produktfamilie:
//   1. Was muss zu Amazon gesendet werden?
//   2. Was für FBA-Transit produzieren?
//   3. Was für FBM produzieren?
//   4. Was einkaufen (Rohware)?
//   5. Reichweiten-Übersicht

app.get('/api/wochenplanung', async (req, res) => {
  try {
    const cfg               = loadConfig();
    const days              = parseInt(req.query.days) || cfg.forecastDays || 90;
    const weeks             = days / 7;
    const fbmTargetDays     = cfg.fbmTargetDays     || 30;
    const rohwareTargetDays = cfg.rohwareTargetDays || 28;
    const transitTargetDays = cfg.transitTargetDays || 7;
    const fbaTargetDays     = cfg.fbaTargetDays     || 35;
    const targetWeeks       = fbmTargetDays / 7;

    const artData      = loadArticles();
    const artItems     = artData.items || {};
    const mainData     = loadStock();
    const mainItems    = mainData.items || {};
    const transitData  = loadTransitStock();
    const transitItems = transitData.items || {};
    const fbaData      = loadFbaStock();
    const fbaItems     = fbaData.items || {};
    const shipData     = loadFbaShipments();
    const inTransit    = shipData.inTransit || {};
    const partsData    = loadParts();
    const partsMap     = partsData.mapping || {};
    const poData       = loadPurchaseOrders();
    const poIncoming   = poData.incoming || {};

    // Shopify Demand aggregieren (einmalig vor der Familienloop)
    let lineItems = [];
    if (cfg.shopifyDomain && cfg.shopifyToken) {
      try {
        const shopify = new ShopifyClient(cfg.shopifyDomain, cfg.shopifyToken);
        lineItems = await shopify.getLineItems(days);
      } catch { /* graceful degradation: kein Shopify → velocity = 0 */ }
    }

    // Demand pro Basis-SKU (Shopify-Bundles bereits aufgelöst)
    const shopifyDemand = {};
    for (const item of lineItems) {
      const sku = item.sku;
      if (!shopifyDemand[sku]) shopifyDemand[sku] = { direct: 0, bundles: {} };
      if (!item.isBundle) {
        shopifyDemand[sku].direct += item.qty;
      } else {
        const f = item.bundleFactor;
        if (!shopifyDemand[sku].bundles[f])
          shopifyDemand[sku].bundles[f] = { baseUnits: 0 };
        shopifyDemand[sku].bundles[f].baseUnits += item.qty;
      }
    }

    const families = [];

    for (const [prefix, rohInfo] of Object.entries(partsMap)) {
      const sack9Sku  = prefix + '-9';
      const sack9Art  = artItems[sack9Sku];
      const sackKg    = sack9Art ? (sack9Art.weightKg || 25) : 25;
      const pool9Stk  = (mainItems[sack9Sku] || {}).available || 0;
      const poolKg    = pool9Stk * sackKg;

      // FBM Produktion: alle Größen -1 bis -5 die aktiv sind
      const sizes = ['1','2','3','4','5'];
      const fbmProduction = [];
      for (const sz of sizes) {
        const sku = prefix + '-' + sz;
        const art = artItems[sku];
        if (!art || !art.active) continue;

        const sd         = shopifyDemand[sku] || { direct: 0, bundles: {} };
        const bundleUnits = Object.values(sd.bundles).reduce((s, b) => s + b.baseUnits, 0);
        const totalUnits  = sd.direct + bundleUnits;
        const velocityPW  = weeks > 0 ? totalUnits / weeks : 0;
        const targetStk   = Math.ceil(velocityPW * targetWeeks);
        const current     = (mainItems[sku] || {}).available || 0;
        const rawNeed     = Math.max(0, targetStk - current);
        const lotSize     = sackKg > 0 && art.weightKg > 0
          ? Math.ceil(sackKg * 1000 / (art.weightKg * 1000))
          : 1;
        const prodNeed    = rawNeed > 0 ? Math.ceil(rawNeed / lotSize) * lotSize : 0;
        const reichweiteWochen = velocityPW > 0 ? +(current / velocityPW).toFixed(1) : null;

        fbmProduction.push({
          sku,
          name:            art.name,
          active:          art.active,
          abfuellklasse:   art.abfuellklasse || '',
          weightKg:        art.weightKg || 0,
          velocityPW:      +velocityPW.toFixed(2),
          current,
          target:          targetStk,
          rawNeed,
          lotSize,
          prodNeed,
          reichweiteWochen,
        });
      }

      const totalFbmKg = fbmProduction.reduce((s, p) => {
        const art = artItems[p.sku];
        return s + p.prodNeed * (art ? (art.weightKg || 0) : 0);
      }, 0);

      // FBA
      const fbaSku  = prefix + '-5-FBA';
      const fbaItem = fbaItems[fbaSku];
      let fbaResult = null;
      let totalFbaKg = 0;

      if (fbaItem) {
        const primarySku  = prefix + '-5';
        const art5        = artItems[primarySku];
        const transitAvail = (transitItems[primarySku] || {}).available || 0;
        const mainAvail    = (mainItems[primarySku]    || {}).available || 0;
        const fbmMinQty    = art5 ? (art5.minQty || 0) : 0;
        const fbmOverstock = Math.max(0, mainAvail - fbmMinQty);
        const recommendation = fbaItem.recommendedReorder || 0;
        const trData       = inTransit[fbaSku] || { shipped: 0, received: 0 };
        const enRoute      = Math.max(0, (trData.shipped || 0) - (trData.received || 0));
        const effRec       = Math.max(0, recommendation - enRoute);
        const shortfall    = Math.max(0, effRec - transitAvail);
        const fromFbm      = Math.min(shortfall, fbmOverstock);
        const newProd      = Math.max(0, shortfall - fromFbm);
        const sendNow      = Math.min(transitAvail, effRec);
        const stillMissing = Math.max(0, effRec - sendNow);

        totalFbaKg = newProd * (art5 ? (art5.weightKg || 1) : 1);

        fbaResult = {
          skuFba:         fbaSku,
          skuBase:        primarySku,
          fbaStock:       fbaItem.fbaStock || 0,
          fbaDaysLeft:    fbaItem.daysLeft || 0,
          fbaVelocity:    fbaItem.velocity || 0,
          transitAvail,
          enRoute,
          recommendation,
          effRec,
          shortfall,
          sendNow,
          stillMissing,
          fromFbm,
          newProd,
        };
      }

      // Einkauf — offene Bestellungen (Verschickt) abziehen
      const totalProdKg  = totalFbmKg + totalFbaKg;
      const poEntry      = poIncoming[rohInfo.rohwareNr] || null;
      const incomingKg   = poEntry ? poEntry.totalKg : 0;
      const buyKg        = Math.max(0, totalProdKg - poolKg);
      const stillToBuyKg = Math.max(0, buyKg - incomingKg);
      const buySacks     = buyKg       > 0 ? Math.ceil(buyKg       / sackKg) : 0;
      const stillBuySacks = stillToBuyKg > 0 ? Math.ceil(stillToBuyKg / sackKg) : 0;

      families.push({
        familyKey:   prefix,
        rohwareNr:   rohInfo.rohwareNr,
        rohwareName: rohInfo.rohwareName,
        sack9Sku,
        sack9Stock:  pool9Stk,
        sackKg,
        poolKg:      +poolKg.toFixed(1),
        fbmProduction,
        fba:         fbaResult,
        einkauf: {
          totalFbmKg:   +totalFbmKg.toFixed(1),
          totalFbaKg:   +totalFbaKg.toFixed(1),
          totalProdKg:  +totalProdKg.toFixed(1),
          poolKg:       +poolKg.toFixed(1),
          buyKg:        +buyKg.toFixed(1),
          buySacks,
          incomingKg:   +incomingKg.toFixed(1),
          incomingOrders: poEntry ? poEntry.orders : [],
          stillToBuyKg: +stillToBuyKg.toFixed(1),
          stillBuySacks,
        },
      });
    }

    // Sortierung: FBA zu senden zuerst, dann Einkaufsbedarf, dann FBM Produktion
    families.sort((a, b) => {
      const aFbaSend = a.fba && a.fba.sendNow > 0 ? 1 : 0;
      const bFbaSend = b.fba && b.fba.sendNow > 0 ? 1 : 0;
      if (bFbaSend !== aFbaSend) return bFbaSend - aFbaSend;
      if (b.einkauf.buySacks !== a.einkauf.buySacks) return b.einkauf.buySacks - a.einkauf.buySacks;
      const aTotalProd = a.fbmProduction.reduce((s, p) => s + p.prodNeed, 0);
      const bTotalProd = b.fbmProduction.reduce((s, p) => s + p.prodNeed, 0);
      return bTotalProd - aTotalProd;
    });

    // Summary
    const summary = {
      zuVersenden:       families.reduce((s, f) => s + (f.fba ? f.fba.sendNow : 0), 0),
      zuProduzierenFba:  families.reduce((s, f) => s + (f.fba ? f.fba.newProd : 0), 0),
      zuProduzierenFbm:  families.reduce((s, f) => f.fbmProduction.reduce((ss, p) => ss + p.prodNeed, 0) + s, 0),
      zuEinkaufenKg:     +families.reduce((s, f) => s + f.einkauf.buyKg, 0).toFixed(1),
      zuEinkaufenSaecke: families.reduce((s, f) => s + f.einkauf.buySacks, 0),
      imZulaufKg:        +families.reduce((s, f) => s + f.einkauf.incomingKg, 0).toFixed(1),
      nochKaufenKg:      +families.reduce((s, f) => s + f.einkauf.stillToBuyKg, 0).toFixed(1),
      nochKaufenSaecke:  families.reduce((s, f) => s + f.einkauf.stillBuySacks, 0),
      familienMitBedarf: families.filter(f =>
        f.einkauf.buySacks > 0 ||
        f.fbmProduction.some(p => p.prodNeed > 0) ||
        (f.fba && (f.fba.sendNow > 0 || f.fba.newProd > 0))
      ).length,
    };

    res.json({
      period:    { days, weeks: +weeks.toFixed(2), targetWeeks: +targetWeeks.toFixed(2) },
      updatedAt: {
        articles: artData.updatedAt || null,
        main:     mainData.updatedAt || null,
        fba:      fbaData.updatedAt || null,
      },
      families,
      summary,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Server starten ───────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n✓ Forecast Tool läuft auf http://localhost:${PORT}`);
  console.log('  Öffne den Link im Browser um zu starten.\n');
});
