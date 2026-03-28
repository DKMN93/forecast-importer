// Forecast-Berechnung: Shopify Sales → Mindestbestand-Vorschläge

// Base-SKU aus Varianten-SKU ableiten
// Bundles (.3/.5/.10) wurden bereits in shopify.js aufgelöst.
// Hier leiten wir den Rohstoff ab: ERYTH-GR-5 → ERYTH-GR-5 (Root = Rohstoff)
function getBaseSku(sku) {
  return sku; // Nach Bundle-Auflösung in shopify.js ist jede SKU bereits eine Root SKU
}

// Wochennummer aus Datum
function getWeekKey(dateStr) {
  const d = new Date(dateStr);
  const startOfYear = new Date(d.getFullYear(), 0, 1);
  const week = Math.ceil(((d - startOfYear) / 86400000 + startOfYear.getDay() + 1) / 7);
  return `${d.getFullYear()}-W${String(week).padStart(2,'0')}`;
}

function round(n, d = 2) { return +n.toFixed(d); }

// Haupt-Berechnung
function calculateForecast(lineItems, mrpeasyItems, targetWeeksDefault = 3) {
  if (!lineItems.length) return [];

  // Zeitraum berechnen
  const dates = lineItems.map(l => new Date(l.date)).sort((a,b) => a-b);
  const daysDiff = (dates[dates.length-1] - dates[0]) / (1000*60*60*24) || 1;
  const weeks = Math.max(daysDiff / 7, 1);

  // Sales aggregieren pro SKU (Fertigware)
  const skuStats = {};
  for (const item of lineItems) {
    const sku = item.sku;
    if (!skuStats[sku]) {
      skuStats[sku] = { sku, title: item.title, totalQty: 0, totalKg: 0, totalRevenue: 0, weeklyQty: {}, weeklyKg: {} };
    }
    skuStats[sku].totalQty += item.qty;
    skuStats[sku].totalKg  += item.kg;
    skuStats[sku].totalRevenue += item.price;

    const wk = getWeekKey(item.date);
    skuStats[sku].weeklyQty[wk] = (skuStats[sku].weeklyQty[wk] || 0) + item.qty;
    skuStats[sku].weeklyKg[wk]  = (skuStats[sku].weeklyKg[wk]  || 0) + item.kg;
  }

  // MRPeasy Items als Map (code → item)
  const mrpMap = {};
  for (const item of mrpeasyItems) {
    if (item.code) mrpMap[item.code] = item;
  }

  // Forecast-Vorschläge pro SKU berechnen
  const proposals = [];
  for (const [sku, stats] of Object.entries(skuStats)) {
    const mrpItem = mrpMap[sku];
    const avgQtyPerWeek = stats.totalQty / weeks;
    const avgKgPerWeek  = stats.totalKg  / weeks;
    const targetWeeks = targetWeeksDefault;
    const proposedMin = Math.ceil(avgQtyPerWeek * targetWeeks);

    // Wöchentliche Daten für Chart
    const weekKeys = Object.keys(stats.weeklyQty).sort();

    proposals.push({
      sku,
      title:          stats.title,
      totalQty:       stats.totalQty,
      totalKg:        round(stats.totalKg),
      totalRevenue:   round(stats.totalRevenue),
      avgQtyPerWeek:  round(avgQtyPerWeek, 1),
      avgKgPerWeek:   round(avgKgPerWeek, 1),
      targetWeeks,
      proposedMin,

      // Aus MRPeasy (falls vorhanden)
      articleId:      mrpItem?.id || null,
      currentMin:     mrpItem ? parseFloat(mrpItem.min_quantity || 0) : null,
      currentStock:   mrpItem ? parseFloat(mrpItem.in_stock || 0) : null,
      available:      mrpItem ? parseFloat(mrpItem.available || 0) : null,
      unit:           mrpItem?.measurement_unit || 'Stk.',
      inMrp:          !!mrpItem,

      // Änderung
      delta:          mrpItem ? proposedMin - parseFloat(mrpItem.min_quantity || 0) : null,

      // Herleitung
      reasoning: `Ø ${round(avgQtyPerWeek, 1)} Stk./Woche × ${targetWeeks} Wochen = ${proposedMin} Stk. Mindestbestand`,

      // Wochendaten für Charts
      weeks: weekKeys,
      weeklyQty: weekKeys.map(w => stats.weeklyQty[w] || 0),
      weeklyKg:  weekKeys.map(w => round(stats.weeklyKg[w]  || 0)),
    });
  }

  // Sortiert nach KG (höchster Umsatz zuerst)
  return proposals.sort((a, b) => b.totalKg - a.totalKg);
}

// Sales nach Base-SKU (Rohstoff) aggregieren
function aggregateByBaseSku(lineItems, weeks) {
  const baseStats = {};
  for (const item of lineItems) {
    const base = getBaseSku(item.sku);
    if (!baseStats[base]) baseStats[base] = { base, variants: new Set(), totalKg: 0, totalQty: 0, totalRevenue: 0 };
    baseStats[base].totalKg      += item.kg;
    baseStats[base].totalQty     += item.qty;
    baseStats[base].totalRevenue += item.price;
    baseStats[base].variants.add(item.sku);
  }

  return Object.values(baseStats).map(b => ({
    base:         b.base,
    variants:     Array.from(b.variants),
    totalKg:      round(b.totalKg),
    totalQty:     b.totalQty,
    totalRevenue: round(b.totalRevenue),
    avgKgPerWeek: round(b.totalKg / weeks, 1),
  })).sort((a, b) => b.totalKg - a.totalKg);
}

// Wöchentliche Gesamtübersicht (für Dashboard-Chart)
function getWeeklySummary(lineItems) {
  const weekly = {};
  for (const item of lineItems) {
    const wk = getWeekKey(item.date);
    if (!weekly[wk]) weekly[wk] = { week: wk, totalKg: 0, totalQty: 0, totalRevenue: 0 };
    weekly[wk].totalKg      += item.kg;
    weekly[wk].totalQty     += item.qty;
    weekly[wk].totalRevenue += item.price;
  }
  return Object.values(weekly).sort((a,b) => a.week.localeCompare(b.week))
    .map(w => ({ ...w, totalKg: round(w.totalKg), totalRevenue: round(w.totalRevenue) }));
}

module.exports = { calculateForecast, aggregateByBaseSku, getWeeklySummary, getBaseSku };
