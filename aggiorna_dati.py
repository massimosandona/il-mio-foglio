"""
Aggiorna dati storici per tutti gli strumenti in UNIFICATO_v2.csv
Scarica performance 1M, 3M, 6M, 1Y, YTD e sparkline (ultimi 30 gg)
Salva in performance_data.json

Strategia di ricerca (fallback):
  1. Yahoo Finance per ISIN
  2. Yahoo Finance per ticker alternativo (ETF/ETC noti)
  3. Morningstar Screener (fondi istituzionali)
"""
import csv
import json
import time
import sys
import urllib.request
from datetime import datetime, timedelta

try:
    import yfinance as yf
except ImportError:
    print("Installare yfinance: pip install yfinance")
    sys.exit(1)

# Config
CSV_PATH = "C:/Progetti/IL MIO FOGLIO/UNIFICATO_v2.csv"
OUTPUT_PATH = "C:/Progetti/IL MIO FOGLIO/performance_data.json"
BATCH_SIZE = 10  # quanti ticker scaricare per batch
SLEEP_BETWEEN = 0.5  # pausa tra batch (secondi)

# Ticker alternativi per ETF/ETC non trovati per ISIN su Yahoo Finance
# Mappatura manuale ISIN -> ticker di borsa
ALT_TICKERS = {
    'IE00B3VTML14': ['CSBGE7.MI', 'IBGM.L', 'CSBGE7.SW'],
    'IE00B4K48X80': ['SMEA.MI', 'IMAE.L', 'SMEA.DE'],
    'IE00B53L3W79': ['CSSX5E.MI', 'SX5S.DE', 'CSX5E.L'],
    'IE00BHZPJ239': ['SAEM.L', 'EMCE.MI', 'EMCE.DE'],
    'IE00BHZPJ908': ['SUSC.MI', 'SUUS.L', 'SAUS.DE'],
    'IE00BX7RRJ27': ['UBSQ.MI', 'UBSQ.DE'],
    'JE00B1VS3770': ['PHAU.MI', 'PHAU.L', 'PHAU.DE'],
    'LU0290357929': ['XGIN.MI', 'XGIN.DE', 'DBXH.L'],
}


def load_isins():
    """Carica gli ISIN unici dal CSV"""
    isins = set()
    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        for row in reader:
            if len(row) > 1 and row[1].strip():
                isins.add(row[1].strip())
    return sorted(isins)


def calc_perf(prices, days):
    """Calcola performance % su N giorni"""
    if len(prices) < 2:
        return None
    target_idx = max(0, len(prices) - days)
    if target_idx >= len(prices) - 1:
        target_idx = 0
    old = prices[target_idx]
    new = prices[-1]
    if old and old > 0:
        return round((new / old - 1) * 100, 2)
    return None


def calc_ytd(dates, prices):
    """Performance Year-to-Date"""
    if len(prices) < 2:
        return None
    year_start = datetime(datetime.now().year, 1, 1)
    for i, d in enumerate(dates):
        if d >= year_start:
            if prices[i] and prices[i] > 0:
                return round((prices[-1] / prices[i] - 1) * 100, 2)
            break
    return None


def build_result_from_yf(hist, source_ticker=None):
    """Costruisce il dict risultato da dati yfinance"""
    closes = hist['Close'].tolist()
    dates = [d.to_pydatetime().replace(tzinfo=None) for d in hist.index]

    # Sparkline: ultimi 30 punti (campionati)
    if len(closes) > 30:
        step = len(closes) // 30
        sparkline = [round(closes[i], 2) for i in range(0, len(closes), step)][:30]
    else:
        sparkline = [round(c, 2) for c in closes]

    result = {
        'price': round(closes[-1], 4) if closes else None,
        'date': dates[-1].strftime('%Y-%m-%d') if dates else None,
        'perf_1m': calc_perf(closes, 21),
        'perf_3m': calc_perf(closes, 63),
        'perf_6m': calc_perf(closes, 126),
        'perf_1y': calc_perf(closes, len(closes)),
        'perf_ytd': calc_ytd(dates, closes),
        'min_1y': round(min(closes), 4) if closes else None,
        'max_1y': round(max(closes), 4) if closes else None,
        'sparkline': sparkline,
    }
    if source_ticker:
        result['ticker'] = source_ticker
    return result


def fetch_yfinance_isin(isin):
    """Step 1: Prova Yahoo Finance con ISIN diretto"""
    try:
        t = yf.Ticker(isin)
        hist = t.history(period='1y')
        if not hist.empty and len(hist) > 5:
            return build_result_from_yf(hist)
    except Exception:
        pass
    return None


def fetch_yfinance_alt_ticker(isin):
    """Step 2: Prova Yahoo Finance con ticker alternativi (ETF/ETC)"""
    if isin not in ALT_TICKERS:
        return None
    for ticker in ALT_TICKERS[isin]:
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period='1y')
            if not hist.empty and len(hist) > 5:
                return build_result_from_yf(hist, source_ticker=ticker)
        except Exception:
            pass
    return None


def fetch_morningstar(isin):
    """Step 3: Prova Morningstar Screener (fondi istituzionali)"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/json',
    }
    universes = ['FOESP$$ALL', 'FOALL$$ALL', 'ETALL$$ALL']

    for universe in universes:
        url = (
            f'https://lt.morningstar.com/api/rest.svc/klr5zyak8x/security/screener'
            f'?outputType=json&version=1&languageId=it-IT&currencyId=EUR'
            f'&universeIds={universe}'
            f'&securityDataPoints=SecId,Name,ClosePrice,TrailingDate,ReturnM1,ReturnM3,ReturnM6,ReturnM12,ReturnM36'
            f'&filters=ISIN:EQ:{isin}&rows=10'
        )
        try:
            req = urllib.request.Request(url, headers=headers)
            resp = urllib.request.urlopen(req, timeout=10)
            data = json.loads(resp.read().decode('utf-8'))
            if data.get('total', 0) > 0:
                r = data['rows'][0]
                return {
                    'price': r.get('ClosePrice'),
                    'date': r.get('TrailingDate', ''),
                    'perf_1m': r.get('ReturnM1'),
                    'perf_3m': r.get('ReturnM3'),
                    'perf_6m': r.get('ReturnM6'),
                    'perf_1y': r.get('ReturnM12'),
                    'perf_ytd': None,
                    'min_1y': None,
                    'max_1y': None,
                    'sparkline': [],
                    'source': 'morningstar',
                }
        except Exception:
            pass
    return None


def fetch_single(isin):
    """Scarica dati per un singolo ISIN con fallback a cascata"""
    # Step 1: Yahoo Finance ISIN
    result = fetch_yfinance_isin(isin)
    if result:
        return result

    # Step 2: Yahoo Finance ticker alternativo
    result = fetch_yfinance_alt_ticker(isin)
    if result:
        return result

    # Step 3: Morningstar Screener
    result = fetch_morningstar(isin)
    if result:
        return result

    return {'error': 'no_data_all_sources'}


def main():
    print("=" * 60)
    print("AGGIORNAMENTO DATI PERFORMANCE")
    print("=" * 60)

    isins = load_isins()
    print(f"ISIN unici da scaricare: {len(isins)}")

    # Carica dati esistenti per non riscaricare tutto
    existing = {}
    try:
        with open(OUTPUT_PATH, 'r', encoding='utf-8') as f:
            existing = json.load(f)
        print(f"Dati esistenti: {len(existing)} ISIN")
    except Exception:
        print("Nessun dato esistente, scarico tutto")

    # Aggiorna solo quelli senza dati o vecchi di > 1 giorno
    today = datetime.now().strftime('%Y-%m-%d')
    to_fetch = []
    for isin in isins:
        if isin in existing and existing[isin].get('date') == today and 'error' not in existing[isin]:
            continue
        to_fetch.append(isin)

    print(f"Da aggiornare: {len(to_fetch)} ISIN")
    if not to_fetch:
        print("Tutto aggiornato!")
        return

    # Scarica
    all_data = dict(existing)
    total = len(to_fetch)
    ok_count = 0
    err_count = 0
    yf_count = 0
    alt_count = 0
    ms_count = 0

    for i, isin in enumerate(to_fetch):
        batch_num = i + 1
        print(f"\r[{batch_num}/{total}] {isin}...", end='', flush=True)

        result = fetch_single(isin)
        all_data[isin] = result

        if 'error' in result:
            err_count += 1
        else:
            ok_count += 1
            if result.get('source') == 'morningstar':
                ms_count += 1
            elif result.get('ticker'):
                alt_count += 1
            else:
                yf_count += 1

        # Salva periodicamente
        if batch_num % 50 == 0 or batch_num == total:
            with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False)

        if i < total - 1:
            time.sleep(0.3)

    # Salvataggio finale
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False)

    print(f"\n\n{'=' * 60}")
    print(f"COMPLETATO!")
    print(f"{'=' * 60}")
    print(f"  OK: {ok_count}")
    print(f"    - Yahoo Finance (ISIN): {yf_count}")
    print(f"    - Yahoo Finance (ticker): {alt_count}")
    print(f"    - Morningstar: {ms_count}")
    print(f"  Errori: {err_count}")
    print(f"  Totale in archivio: {len(all_data)}")
    print(f"  Salvato in: {OUTPUT_PATH}")

    # Mostra errori
    if err_count > 0:
        print(f"\nISIN non trovati:")
        for isin in sorted(all_data.keys()):
            if 'error' in all_data[isin]:
                print(f"  {isin}")

    # Mostra qualche esempio
    print("\nEsempi:")
    for isin in list(all_data.keys())[:5]:
        d = all_data[isin]
        if 'error' not in d:
            src = d.get('source', d.get('ticker', 'yfinance'))
            print(f"  {isin}: {d.get('price','-')} | 1M:{d.get('perf_1m','-')}% | 3M:{d.get('perf_3m','-')}% | 1Y:{d.get('perf_1y','-')}% | [{src}]")


if __name__ == '__main__':
    main()
