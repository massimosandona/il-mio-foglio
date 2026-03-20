import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

// Alt tickers for ETF/ETC not found by ISIN on Yahoo Finance
const ALT_TICKERS: Record<string, string[]> = {
  "IE00B3VTML14": ["CSBGE7.MI", "IBGM.L", "CSBGE7.SW"],
  "IE00B4K48X80": ["SMEA.MI", "IMAE.L", "SMEA.DE"],
  "IE00B53L3W79": ["CSSX5E.MI", "SX5S.DE", "CSX5E.L"],
  "IE00BHZPJ239": ["SAEM.L", "EMCE.MI", "EMCE.DE"],
  "IE00BHZPJ908": ["SUSC.MI", "SUUS.L", "SAUS.DE"],
  "IE00BX7RRJ27": ["UBSQ.MI", "UBSQ.DE"],
  "JE00B1VS3770": ["PHAU.MI", "PHAU.L", "PHAU.DE"],
  "LU0290357929": ["XGIN.MI", "XGIN.DE", "DBXH.L"],
};

interface PerfResult {
  isin: string;
  price: number | null;
  date: string | null;
  perf_1m: number | null;
  perf_3m: number | null;
  perf_6m: number | null;
  perf_1y: number | null;
  perf_3y: number | null;
  perf_5y: number | null;
  perf_10y: number | null;
  perf_ytd: number | null;
  min_1y: number | null;
  max_1y: number | null;
  sparkline: Record<string, number[]> | number[];
  source?: string;
  ticker?: string;
  error?: string;
}

function calcPerf(prices: number[], days: number): number | null {
  if (prices.length < 2) return null;
  let targetIdx = Math.max(0, prices.length - days);
  if (targetIdx >= prices.length - 1) targetIdx = 0;
  const old = prices[targetIdx];
  const cur = prices[prices.length - 1];
  if (old && old > 0) return Math.round(((cur / old - 1) * 100) * 100) / 100;
  return null;
}

function calcYtd(timestamps: number[], prices: number[]): number | null {
  if (prices.length < 2) return null;
  const now = new Date();
  const yearStart = new Date(now.getFullYear(), 0, 1).getTime() / 1000;
  for (let i = 0; i < timestamps.length; i++) {
    if (timestamps[i] >= yearStart) {
      if (prices[i] && prices[i] > 0) {
        return Math.round(((prices[prices.length - 1] / prices[i] - 1) * 100) * 100) / 100;
      }
      break;
    }
  }
  return null;
}

function sliceYtd(timestamps: number[], prices: number[]): number[] {
  const yearStart = new Date(new Date().getFullYear(), 0, 1).getTime() / 1000;
  for (let i = 0; i < timestamps.length; i++) {
    if (timestamps[i] >= yearStart) return prices.slice(i);
  }
  return [];
}

function buildSparkline(prices: number[], points = 30): number[] {
  if (prices.length <= points) return prices.map((p) => Math.round(p * 100) / 100);
  const step = Math.floor(prices.length / points);
  const result: number[] = [];
  for (let i = 0; i < prices.length && result.length < points; i += step) {
    result.push(Math.round(prices[i] * 100) / 100);
  }
  return result;
}

async function searchYahooTicker(isin: string): Promise<string | null> {
  try {
    const url = `https://query2.finance.yahoo.com/v1/finance/search?q=${encodeURIComponent(isin)}&quotesCount=5&newsCount=0`;
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" },
    });
    if (!resp.ok) {
      console.log(`[Yahoo Search] ${isin} -> HTTP ${resp.status}`);
      return null;
    }
    const data = await resp.json();
    const quotes = data?.quotes || [];
    if (quotes.length === 0) {
      console.log(`[Yahoo Search] ${isin} -> no results`);
      return null;
    }
    // Prefer .MI (Milan) exchange, then any
    const milan = quotes.find((q: { symbol: string }) => q.symbol?.endsWith(".MI"));
    const ticker = milan?.symbol || quotes[0]?.symbol;
    console.log(`[Yahoo Search] ${isin} -> ${ticker}`);
    return ticker || null;
  } catch (e) {
    console.log(`[Yahoo Search] ${isin} -> error: ${e}`);
    return null;
  }
}

async function fetchYahoo(symbol: string): Promise<{ timestamps: number[]; prices: number[] } | null> {
  try {
    const url = `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=10y&interval=1d`;
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" },
    });
    if (!resp.ok) {
      console.log(`[Yahoo Chart] ${symbol} -> HTTP ${resp.status}`);
      return null;
    }
    const data = await resp.json();
    const result = data?.chart?.result?.[0];
    if (!result) return null;
    const timestamps: number[] = result.timestamp || [];
    const prices: number[] = result.indicators?.quote?.[0]?.close || [];
    // Filter out nulls
    const clean: { ts: number[]; px: number[] } = { ts: [], px: [] };
    for (let i = 0; i < prices.length; i++) {
      if (prices[i] != null && timestamps[i] != null) {
        clean.ts.push(timestamps[i]);
        clean.px.push(prices[i]);
      }
    }
    if (clean.px.length < 5) return null;
    return { timestamps: clean.ts, prices: clean.px };
  } catch (e) {
    console.log(`[Yahoo Chart] ${symbol} -> error: ${e}`);
    return null;
  }
}

function buildResultFromYahoo(
  isin: string,
  timestamps: number[],
  prices: number[],
  sourceTicker?: string
): PerfResult {
  const lastTs = timestamps[timestamps.length - 1];
  const lastDate = new Date(lastTs * 1000);
  const dateStr = lastDate.toISOString().slice(0, 10);

  // For 1y calculations, use min(252, prices.length)
  const len1y = Math.min(252, prices.length);
  const prices1y = prices.slice(-len1y);

  return {
    isin,
    price: Math.round(prices[prices.length - 1] * 10000) / 10000,
    date: dateStr,
    perf_1m: calcPerf(prices, 21),
    perf_3m: calcPerf(prices, 63),
    perf_6m: calcPerf(prices, 126),
    perf_1y: calcPerf(prices, 252),
    perf_3y: calcPerf(prices, 756),
    perf_5y: calcPerf(prices, 1260),
    perf_10y: calcPerf(prices, 2520),
    perf_ytd: calcYtd(timestamps, prices),
    min_1y: prices1y.length ? Math.round(Math.min(...prices1y) * 10000) / 10000 : null,
    max_1y: prices1y.length ? Math.round(Math.max(...prices1y) * 10000) / 10000 : null,
    sparkline: {
      "ytd": buildSparkline(sliceYtd(timestamps, prices)),
      "1y": buildSparkline(prices.slice(-252)),
      "3y": buildSparkline(prices.slice(-756)),
      "5y": buildSparkline(prices.slice(-1260)),
      "10y": buildSparkline(prices),
    },
    source: "yahoo",
    ticker: sourceTicker,
  };
}

async function fetchYahooForIsin(isin: string): Promise<PerfResult | null> {
  // Try alt tickers first (known mappings)
  const alts = ALT_TICKERS[isin];
  if (alts) {
    for (const ticker of alts) {
      const alt = await fetchYahoo(ticker);
      if (alt) return buildResultFromYahoo(isin, alt.timestamps, alt.prices, ticker);
    }
  }

  // Search for ticker by ISIN via Yahoo Search API
  const ticker = await searchYahooTicker(isin);
  if (ticker) {
    const result = await fetchYahoo(ticker);
    if (result) return buildResultFromYahoo(isin, result.timestamps, result.prices, ticker);
  }

  return null;
}

async function fetchMorningstar(isin: string): Promise<PerfResult | null> {
  const universes = ["FOESP$$ALL", "FOALL$$ALL", "ETALL$$ALL"];
  const headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    Accept: "application/json",
  };

  for (const universe of universes) {
    try {
      const url =
        `https://lt.morningstar.com/api/rest.svc/klr5zyak8x/security/screener` +
        `?outputType=json&version=1&languageId=it-IT&currencyId=EUR` +
        `&universeIds=${universe}` +
        `&securityDataPoints=SecId,Name,ClosePrice,TrailingDate,ReturnM1,ReturnM3,ReturnM6,ReturnM12,ReturnM36,ReturnM60,ReturnM120` +
        `&filters=ISIN:EQ:${isin}&rows=10`;
      const resp = await fetch(url, { headers });
      if (!resp.ok) {
        console.log(`[Morningstar] ${isin} ${universe} -> HTTP ${resp.status}`);
        continue;
      }
      const data = await resp.json();
      if (data?.total > 0) {
        const r = data.rows[0];
        console.log(`[Morningstar] ${isin} -> OK (${universe}), price=${r.ClosePrice}`);
        return {
          isin,
          price: r.ClosePrice ?? null,
          date: r.TrailingDate || null,
          perf_1m: r.ReturnM1 ?? null,
          perf_3m: r.ReturnM3 ?? null,
          perf_6m: r.ReturnM6 ?? null,
          perf_1y: r.ReturnM12 ?? null,
          perf_3y: r.ReturnM36 ?? null,
          perf_5y: r.ReturnM60 ?? null,
          perf_10y: r.ReturnM120 ?? null,
          perf_ytd: null,
          min_1y: null,
          max_1y: null,
          sparkline: [],
          source: "morningstar",
        };
      }
    } catch (e) {
      console.log(`[Morningstar] ${isin} ${universe} -> error: ${e}`);
    }
  }
  console.log(`[Morningstar] ${isin} -> not found in any universe`);
  return null;
}

async function fetchSingle(
  isin: string,
  source: string
): Promise<PerfResult> {
  if (source === "yahoo") {
    const r = await fetchYahooForIsin(isin);
    if (r) return r;
    return { isin, price: null, date: null, perf_1m: null, perf_3m: null, perf_6m: null, perf_1y: null, perf_3y: null, perf_5y: null, perf_10y: null, perf_ytd: null, min_1y: null, max_1y: null, sparkline: [], error: "no_data_yahoo" };
  }
  if (source === "morningstar") {
    const r = await fetchMorningstar(isin);
    if (r) return r;
    return { isin, price: null, date: null, perf_1m: null, perf_3m: null, perf_6m: null, perf_1y: null, perf_3y: null, perf_5y: null, perf_10y: null, perf_ytd: null, min_1y: null, max_1y: null, sparkline: [], error: "no_data_morningstar" };
  }
  // cascade: try Yahoo first (has sparkline/min/max/ytd), then Morningstar as fallback
  console.log(`[Cascade] ${isin} -> trying Yahoo first`);
  const yahoo = await fetchYahooForIsin(isin);
  if (yahoo) return yahoo;
  console.log(`[Cascade] ${isin} -> Yahoo failed, trying Morningstar`);
  const ms = await fetchMorningstar(isin);
  if (ms) return ms;
  console.log(`[Cascade] ${isin} -> all sources failed`);
  return { isin, price: null, date: null, perf_1m: null, perf_3m: null, perf_6m: null, perf_1y: null, perf_3y: null, perf_5y: null, perf_10y: null, perf_ytd: null, min_1y: null, max_1y: null, sparkline: [], error: "no_data_all_sources" };
}

// Process a batch of ISINs with concurrency limit
async function processBatch(
  isins: string[],
  source: string,
  skipToday: boolean,
  existingDates: Record<string, string>
): Promise<{ updated: PerfResult[]; skipped: string[]; failed: PerfResult[] }> {
  const today = new Date().toISOString().slice(0, 10);
  const toProcess: string[] = [];
  const skipped: string[] = [];

  for (const isin of isins) {
    if (skipToday && existingDates[isin] === today) {
      skipped.push(isin);
    } else {
      toProcess.push(isin);
    }
  }

  const updated: PerfResult[] = [];
  const failed: PerfResult[] = [];

  // Process with concurrency of 5
  const CONCURRENCY = 5;
  for (let i = 0; i < toProcess.length; i += CONCURRENCY) {
    const chunk = toProcess.slice(i, i + CONCURRENCY);
    const results = await Promise.all(chunk.map((isin) => fetchSingle(isin, source)));
    for (const r of results) {
      if (r.error) failed.push(r);
      else updated.push(r);
    }
  }

  return { updated, skipped, failed };
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const supabase = createClient(supabaseUrl, serviceKey);

    const body = await req.json();
    const action = body.action;

    // ==================== UPDATE-BATCH ====================
    if (action === "update-batch") {
      const isins: string[] = body.isins || [];
      const source: string = body.source || "cascade";
      const skipToday: boolean = body.skipToday ?? true;

      if (!isins.length) {
        return new Response(JSON.stringify({ error: "No ISINs provided" }), {
          status: 400,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }

      // Get existing dates for skipToday logic
      let existingDates: Record<string, string> = {};
      if (skipToday) {
        const { data } = await supabase
          .from("performance")
          .select("isin,date")
          .in("isin", isins);
        if (data) {
          for (const row of data) {
            if (row.date) existingDates[row.isin] = row.date;
          }
        }
      }

      const result = await processBatch(isins, source, skipToday, existingDates);

      // Upsert updated results
      if (result.updated.length > 0) {
        const rows = result.updated.map((r) => ({
          isin: r.isin,
          price: r.price,
          date: r.date,
          perf_1m: r.perf_1m,
          perf_3m: r.perf_3m,
          perf_6m: r.perf_6m,
          perf_1y: r.perf_1y,
          perf_3y: r.perf_3y,
          perf_5y: r.perf_5y,
          perf_10y: r.perf_10y,
          perf_ytd: r.perf_ytd,
          min_1y: r.min_1y,
          max_1y: r.max_1y,
          sparkline: r.sparkline,
          source: r.source || null,
          ticker: r.ticker || null,
        }));
        const { error: upsertErr } = await supabase
          .from("performance")
          .upsert(rows, { onConflict: "isin" });
        if (upsertErr) {
          return new Response(JSON.stringify({ error: "Upsert failed: " + upsertErr.message }), {
            status: 500,
            headers: { ...corsHeaders, "Content-Type": "application/json" },
          });
        }
      }

      // Also upsert failed results (to record the error)
      if (result.failed.length > 0) {
        const failRows = result.failed.map((r) => ({
          isin: r.isin,
          error: r.error,
        }));
        await supabase.from("performance").upsert(failRows, { onConflict: "isin" });
      }

      return new Response(
        JSON.stringify({
          updated: result.updated.map((r) => r.isin),
          skipped: result.skipped,
          failed: result.failed.map((r) => ({ isin: r.isin, error: r.error })),
        }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // ==================== MERGE-CSV ====================
    if (action === "merge-csv") {
      const instruments: Array<{
        nome: string; isin: string; categoria: string; rating: string;
        quotazione: string; dataAgg: string; valuta: string; tipo: string;
        macro: string; geo: string; politica: string;
      }> = body.instruments || [];

      if (!instruments.length) {
        return new Response(JSON.stringify({ error: "No instruments provided" }), {
          status: 400,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }

      // Get existing ISINs from DB
      const { data: existing } = await supabase
        .from("strumenti")
        .select("isin");
      const existingIsins = new Set((existing || []).map((r: { isin: string }) => r.isin));
      const newIsins = new Set(instruments.map((i) => i.isin));

      // Calculate diff
      const toAdd = instruments.filter((i) => !existingIsins.has(i.isin));
      const toRemove = [...existingIsins].filter((isin) => !newIsins.has(isin));
      const kept = [...existingIsins].filter((isin) => newIsins.has(isin));

      // INSERT new instruments
      if (toAdd.length > 0) {
        for (let i = 0; i < toAdd.length; i += 500) {
          const batch = toAdd.slice(i, i + 500).map((s) => ({
            nome: s.nome,
            isin: s.isin,
            categoria_morningstar: s.categoria,
            rating_overall: s.rating,
            quotazione: s.quotazione,
            data_agg: s.dataAgg,
            valuta: s.valuta,
            tipo: s.tipo,
            macro_classe: s.macro,
            zona_geografica: s.geo,
            politica: s.politica,
          }));
          await supabase.from("strumenti").insert(batch);
        }
      }

      // DELETE removed instruments and their performance
      if (toRemove.length > 0) {
        // Delete performance first
        await supabase.from("performance").delete().in("isin", toRemove);
        // Delete instruments
        await supabase.from("strumenti").delete().in("isin", toRemove);
      }

      return new Response(
        JSON.stringify({
          added: toAdd.length,
          removed: toRemove.length,
          kept: kept.length,
          addedIsins: toAdd.map((i) => i.isin),
          removedIsins: toRemove,
        }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    return new Response(JSON.stringify({ error: "Unknown action: " + action }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
