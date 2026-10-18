/**
 * Monatsinventur (Steuerberater) – Stichtagsbericht aus 5 Quellen, 4 Lagerstufen:
 *   1. Hauptlager (MRPeasy-Export, Location "Main Lager")
 *   2. Transit-Amazon-Lager (MRPeasy-Export, Location "Transit Amazon")
 *   3. Unterwegs zu Amazon: je Sendung aus Seller-Central-Sendungsübersicht (xlsx)
 *      + Ledger-Detailansicht (CSV, Event "Receipts" mit Reference ID = Sendungs-ID)
 *   4. Amazon FBA sellable: Ledger-Zusammenfassung (CSV, monatlich aggregiert)
 *
 * Gleiche Logik wie der Claude-Skill "ingreo-monatsinventur" (scripts/build_inventur.js) –
 * bei Methodik-Änderungen beide anpassen.
 */
const XLSX = require('xlsx');

function round2(n) { return Math.round(n * 100) / 100; }

function parseCsv(text, delim) {
  text = text.replace(/^﻿/, '');
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else { inQuotes = false; }
      } else field += c;
    } else {
      if (c === '"') inQuotes = true;
      else if (c === delim) { row.push(field); field = ''; }
      else if (c === '\r') { /* skip */ }
      else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
      else field += c;
    }
  }
  if (field !== '' || row.length) { row.push(field); rows.push(row); }
  return rows.filter(r => r.length > 1 || (r.length === 1 && r[0] !== ''));
}

// "09/29/2026" -> "2026-09-29"
function usDateToIso(s) {
  const m = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(String(s).trim());
  return m ? `${m[3]}-${m[1]}-${m[2]}` : null;
}

function requireColumns(header, cols, label) {
  const missing = cols.filter(c => header.indexOf(c) === -1);
  if (missing.length) throw new Error(`${label}: Spalte(n) fehlen: ${missing.join(', ')} – falsche Datei?`);
}

function parseMrpeasy(text, label) {
  const rows = parseCsv(text, ';');
  const header = rows[0] || [];
  requireColumns(header, ['Artikelnr.', 'Artikelbezeichnung', 'Menge', 'Kosten'], label);
  const iSku = header.indexOf('Artikelnr.');
  const iName = header.indexOf('Artikelbezeichnung');
  const iQty = header.indexOf('Menge');
  const iKosten = header.indexOf('Kosten');
  const items = {};
  const rowsOut = [];
  let total = 0;
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    if (!row || row.length < header.length) continue;
    const qty = parseFloat(row[iQty]) || 0;
    // "Kosten" = bereits Menge × Ø-EK (Gesamtwert der Position), nicht neu multiplizieren
    const kosten = parseFloat(row[iKosten]) || 0;
    items[row[iSku]] = { qty, value: kosten };
    total += kosten;
    if (kosten !== 0 || qty !== 0) rowsOut.push({ Artikelnr: row[iSku], Bezeichnung: row[iName], Menge: qty, Wert_EUR: round2(kosten) });
  }
  rowsOut.sort((a, b) => b.Wert_EUR - a.Wert_EUR);
  const activeSkus = rowsOut.filter(r => r.Menge !== 0).length;
  return { items, total, rowsOut, activeSkus };
}

// Ø-EK je Basis-SKU, gewichtet über Hauptlager + Transit kombiniert
function combineAvgCost(main, transit) {
  const avg = {};
  for (const sku of new Set([...Object.keys(main.items), ...Object.keys(transit.items)])) {
    const m = main.items[sku] || { qty: 0, value: 0 };
    const t = transit.items[sku] || { qty: 0, value: 0 };
    const qty = m.qty + t.qty;
    avg[sku] = qty > 0 ? (m.value + t.value) / qty : 0;
  }
  return avg;
}

const baseSkuFromFba = (s) => s.replace(/-FBA$/, '');
const looksLikeInternalSku = (s) => /^[A-Z0-9]+(-[A-Z0-9]+)+-FBA$/.test(s);

function parseLedgerSummary(text) {
  const rows = parseCsv(text, ',');
  const header = rows[0] || [];
  requireColumns(header, ['MSKU', 'Disposition', 'Ending Warehouse Balance', 'In Transit Between Warehouses', 'Date'], 'Ledger-Zusammenfassung');
  const iMsku = header.indexOf('MSKU');
  const iDisp = header.indexOf('Disposition');
  const iEnding = header.indexOf('Ending Warehouse Balance');
  const iInterTransit = header.indexOf('In Transit Between Warehouses');
  const iDate = header.indexOf('Date');
  const byMsku = {};
  const months = new Set();
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    if (!row || row.length < header.length) continue;
    // In Transit Between Warehouses = amazon-interne FC-Umbuchung bereits empfangener Ware,
    // in keinem Ending Warehouse Balance enthalten -> gehört trotzdem zum Amazon-Bestand
    const qty = (parseFloat(row[iEnding]) || 0) + (parseFloat(row[iInterTransit]) || 0);
    months.add(row[iDate]);
    const msku = row[iMsku], disp = row[iDisp];
    if (!byMsku[msku]) byMsku[msku] = {};
    byMsku[msku][disp] = (byMsku[msku][disp] || 0) + qty;
  }
  return { byMsku, months: [...months] };
}

// Seller Central "Sendungen an Amazon"-Export: Kopfzeile je Sendung (ID, Datum, Status,
// Last update), darunter Produktzeilen "<Titel> / <ASIN> / <MSKU>" mit Units shipped/received
function parseShipments(buffer) {
  const wb = XLSX.read(buffer, { type: 'buffer' });
  const rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], { header: 1, defval: '' });
  const header = (rows[0] || []).map(h => String(h).trim());
  requireColumns(header, ['Shipment plan/Shipment ID', 'Date', 'Products', 'Status', 'Last update', 'Units shipped', 'Units received'], 'Sendungen an Amazon');
  const idx = (name) => header.indexOf(name);
  const iId = idx('Shipment plan/Shipment ID'), iDate = idx('Date'), iProd = idx('Products');
  const iStatus = idx('Status'), iLast = idx('Last update');
  const iShipped = idx('Units shipped'), iRecv = idx('Units received');
  const shipments = [];
  let cur = null;
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const idCell = String(row[iId] || '').trim();
    if (idCell) {
      const m = /(FBA[0-9A-Z]+)\s*$/.exec(idCell);
      cur = { id: m ? m[1] : idCell, date: usDateToIso(row[iDate]), status: String(row[iStatus] || '').trim(), lastUpdate: usDateToIso(row[iLast]), lines: [] };
      shipments.push(cur);
    } else if (cur && row[iProd]) {
      const parts = String(row[iProd]).split(' / ');
      cur.lines.push({ msku: parts[parts.length - 1].trim(), shipped: parseFloat(row[iShipped]) || 0, received: parseFloat(row[iRecv]) || 0 });
    }
  }
  return shipments;
}

// Ledger-Detailansicht: Wareneingänge je Sendungs-ID × MSKU bis einschl. Stichtag
function parseLedgerDetail(text, stichtag) {
  const rows = parseCsv(text, ',');
  const header = rows[0] || [];
  requireColumns(header, ['Date', 'MSKU', 'Event Type', 'Reference ID', 'Quantity'], 'Ledger-Detailansicht');
  const iDate = header.indexOf('Date'), iMsku = header.indexOf('MSKU'), iType = header.indexOf('Event Type');
  const iRef = header.indexOf('Reference ID'), iQty = header.indexOf('Quantity');
  const receipts = {};
  let minDate = null, maxDate = null;
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const d = usDateToIso(row[iDate]);
    if (!d) continue;
    if (!minDate || d < minDate) minDate = d;
    if (!maxDate || d > maxDate) maxDate = d;
    if (row[iType] !== 'Receipts' || d > stichtag) continue;
    const key = row[iRef] + '|' + row[iMsku];
    receipts[key] = (receipts[key] || 0) + (parseFloat(row[iQty]) || 0);
  }
  return { receipts, minDate, maxDate };
}

// Je Sendung × MSKU: unterwegs = erwartete Menge − bis Stichtag empfangen. Erwartet = bei
// geschlossenen Sendungen final empfangene Menge (Verlorenes ist kein Bestand), sonst versendet.
// Nach Stichtag angelegt oder bis Stichtag geschlossen -> nichts unterwegs.
function unterwegsFromShipments(shipments, detail, stichtag) {
  const lines = [];
  const hinweise = [];
  for (const s of shipments) {
    if (!s.date || s.date > stichtag) continue;
    const closed = s.status === 'Closed';
    if (closed && s.lastUpdate && s.lastUpdate <= stichtag) continue;
    if (detail.minDate && s.date < detail.minDate) {
      hinweise.push(`${s.id}: angelegt ${s.date}, Ledger-Detail beginnt erst ${detail.minDate} – frühere Eingänge nicht sichtbar, Menge prüfen`);
    }
    for (const l of s.lines) {
      const expected = closed ? l.received : l.shipped;
      const qty = Math.max(0, expected - (detail.receipts[s.id + '|' + l.msku] || 0));
      if (!closed && l.received < l.shipped) {
        hinweise.push(`${s.id} ${l.msku}: Status ${s.status}, bisher ${l.received} von ${l.shipped} empfangen – voll als unterwegs gezählt, ggf. Fall bei Amazon öffnen`);
      }
      if (qty > 0) lines.push({ shipmentId: s.id, angelegt: s.date, status: s.status, msku: l.msku, qty });
    }
  }
  if (detail.maxDate && detail.maxDate < stichtag) {
    hinweise.push(`Ledger-Detail endet ${detail.maxDate}, vor dem Stichtag ${stichtag} – Eingänge danach fehlen`);
  }
  return { lines, hinweise };
}

/**
 * @param files { main, transit, shipments, ledgerDetail, ledger } je { buffer, name }
 * @param stichtag 'YYYY-MM-DD'
 * @param prior { stichtag, gesamt } | null – letzter Lauf vor dem Stichtag
 * @returns { summary, buffer } – summary für Historie/Log, buffer = fertige xlsx
 */
function buildInventur(files, stichtag, prior) {
  const text = (f) => f.buffer.toString('utf8');
  const mainLager = parseMrpeasy(text(files.main), 'Hauptlager');
  const transitLager = parseMrpeasy(text(files.transit), 'Transit Amazon');
  const warnungen = [];
  // Plausibilität Main vs. Transit: Main = volles Sortiment / hoher Wert
  if (transitLager.total > mainLager.total) {
    warnungen.push(`Transit-Datei (${round2(transitLager.total)} €) ist wertvoller als Hauptlager-Datei (${round2(mainLager.total)} €) – Dateien vertauscht?`);
  }
  const avgCost = combineAvgCost(mainLager, transitLager);
  const ledger = parseLedgerSummary(text(files.ledger));
  const detail = parseLedgerDetail(text(files.ledgerDetail), stichtag);
  const unterwegs = unterwegsFromShipments(parseShipments(files.shipments.buffer), detail, stichtag);

  const stichMonth = `${stichtag.slice(5, 7)}/${stichtag.slice(0, 4)}`;
  if (!ledger.months.includes(stichMonth)) {
    warnungen.push(`Ledger-Zusammenfassung enthält Monat(e) ${ledger.months.join(', ')}, erwartet ${stichMonth}`);
  }

  // Stufe 3
  let unterwegsValue = 0;
  const unterwegsRows = [];
  const unmatched = [];
  for (const l of unterwegs.lines) {
    const base = baseSkuFromFba(l.msku);
    const cost = avgCost[base];
    if (cost !== undefined) {
      const value = l.qty * cost;
      unterwegsValue += value;
      unterwegsRows.push({ Sendung: l.shipmentId, Angelegt: l.angelegt, Status: l.status, SKU: l.msku, Basis_SKU: base, Menge: l.qty, Oe_EK_EUR: round2(cost), Wert_EUR: round2(value) });
    } else {
      unmatched.push({ SKU: l.msku, Menge: l.qty, Grund: 'kein Ø-EK gefunden (unterwegs)' });
    }
  }
  unterwegsRows.sort((a, b) => b.Wert_EUR - a.Wert_EUR);

  // Stufe 4: nur SELLABLE; nicht verkäuflich + nicht zuordenbar separat, nie in GESAMT
  let fbaValue = 0;
  const fbaRows = [];
  const nonSellable = {};
  for (const [msku, disps] of Object.entries(ledger.byMsku)) {
    for (const [disp, qty] of Object.entries(disps)) {
      if (qty === 0) continue;
      if (disp !== 'SELLABLE') { nonSellable[disp] = (nonSellable[disp] || 0) + qty; continue; }
      if (!looksLikeInternalSku(msku)) { unmatched.push({ SKU: msku, Menge: qty, Grund: 'altes Namensschema, keine SKU-Zuordnung' }); continue; }
      const base = baseSkuFromFba(msku);
      const cost = avgCost[base];
      if (cost === undefined) { unmatched.push({ SKU: msku, Menge: qty, Grund: 'kein Ø-EK gefunden' }); continue; }
      fbaValue += qty * cost;
      fbaRows.push({ MSKU: msku, Basis_SKU: base, Menge: qty, Oe_EK_EUR: round2(cost), Wert_EUR: round2(qty * cost) });
    }
  }
  fbaRows.sort((a, b) => b.Wert_EUR - a.Wert_EUR);

  const gesamt = mainLager.total + transitLager.total + unterwegsValue + fbaValue;

  // ── Excel ──
  const wb = XLSX.utils.book_new();
  const summaryData = [
    ['Suppstainable / Ingreo – Inventurbericht zum Stichtag ' + stichtag],
    [],
    ['Position', 'Wert (EUR)'],
    ['1. Hauptlager (MRPeasy)', round2(mainLager.total)],
    ['2. Transit-Amazon-Lager (MRPeasy, physisch)', round2(transitLager.total)],
    ['3. Unterwegs zu Amazon (DHL/DPD, Sendungen an Amazon)', round2(unterwegsValue)],
    ['4. Amazon FBA sellable (Ledger)', round2(fbaValue)],
    ['GESAMT', round2(gesamt)],
  ];
  if (prior) {
    summaryData.push([], ['Vergleich Vormonat (' + prior.stichtag + ')', ''], ['Vormonat GESAMT', round2(prior.gesamt)], ['Differenz', round2(gesamt - prior.gesamt)]);
  }
  summaryData.push([], ['Nicht-verkäuflicher Amazon-Bestand (separat, NICHT in GESAMT enthalten)'], ['Disposition', 'Menge']);
  for (const [disp, qty] of Object.entries(nonSellable)) summaryData.push([disp, qty]);
  if (unmatched.length) {
    summaryData.push([], ['Nicht bewertbare Mengen (separat auszuweisen, NICHT in GESAMT)']);
    for (const u of unmatched) summaryData.push([u.SKU, u.Menge, u.Grund]);
  }
  if (unterwegs.hinweise.length) {
    summaryData.push([], ['Prüfhinweise Position 3 (Sendungen an Amazon)']);
    for (const h of unterwegs.hinweise) summaryData.push([h]);
  }
  const wsSummary = XLSX.utils.aoa_to_sheet(summaryData);
  wsSummary['!cols'] = [{ wch: 60 }, { wch: 16 }, { wch: 30 }];
  XLSX.utils.book_append_sheet(wb, wsSummary, 'Zusammenfassung');
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(mainLager.rowsOut), 'Hauptlager-Detail');
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(transitLager.rowsOut), 'Transit-Amazon-Detail');
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(unterwegsRows), 'Unterwegs-DHL-DPD-Detail');
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(fbaRows), 'FBA-sellable-Detail');

  const methodikData = [
    ['Datenquellen'],
    ['1. Hauptlager', files.main.name],
    ['2. Transit-Amazon-Lager', files.transit.name],
    ['3. Sendungen an Amazon', files.shipments.name],
    ['3. Ledger-Detailansicht', files.ledgerDetail.name + ` (${detail.minDate || '?'} bis ${detail.maxDate || '?'})`],
    ['4. Ledger-Zusammenfassung', files.ledger.name + ` (Monat ${ledger.months.join(', ')})`],
    [],
    ['Methodik'],
    ['Hauptlager/Transit', '"Kosten"-Spalte aus MRPeasy-Export = bereits Menge × Ø-EK (nicht neu multiplizieren)'],
    ['Ø-EK je Basis-SKU', 'gewichtet über Hauptlager + Transit kombiniert (Menge/Wert), ordnungsunabhängig'],
    ['Unterwegs DHL/DPD', 'je Sendung (angelegt bis Stichtag, nicht bis Stichtag geschlossen): versendete Menge (bei geschlossenen Sendungen final empfangene Menge) minus Wareneingänge bis Stichtag lt. Ledger-Detail (Event "Receipts", Reference ID = Sendungs-ID), × Ø-EK der Basis-SKU'],
    ['Amazon FBA sellable', 'Ledger: nur Disposition SELLABLE, "Ending Warehouse Balance" + "In Transit Between Warehouses" (amazon-interne FC-Umbuchung), × Ø-EK der Basis-SKU (MSKU minus "-FBA")'],
    ['Nicht-verkäuflicher Bestand', 'alle Ledger-Dispositionen außer SELLABLE (defekt/beschädigt/abgelaufen) — separat ausgewiesen, nie in GESAMT, Bewertung/Teilwertabschreibung entscheidet der Steuerberater'],
  ];
  const wsMethodik = XLSX.utils.aoa_to_sheet(methodikData);
  wsMethodik['!cols'] = [{ wch: 30 }, { wch: 70 }];
  XLSX.utils.book_append_sheet(wb, wsMethodik, 'Datenquellen & Methodik');

  const summary = {
    stichtag,
    hauptlager: round2(mainLager.total),
    transitAmazon: round2(transitLager.total),
    unterwegsDhlDpd: round2(unterwegsValue),
    fbaSellable: round2(fbaValue),
    gesamt: round2(gesamt),
    priorMonth: prior ? { stichtag: prior.stichtag, gesamt: prior.gesamt, differenz: round2(gesamt - prior.gesamt) } : null,
    nichtVerkaeuflich: nonSellable,
    nichtBewertbar: unmatched,
    unterwegsDetail: unterwegsRows,
    pruefhinweise: unterwegs.hinweise,
    warnungen,
    skus: { hauptlager: mainLager.activeSkus, transit: transitLager.activeSkus },
  };
  return { summary, buffer: XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' }) };
}

module.exports = { buildInventur };
