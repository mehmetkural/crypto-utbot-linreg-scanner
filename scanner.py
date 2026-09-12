#!/usr/bin/env python3
"""
UT Bot + LinReg Candle Crypto Scanner
--------------------------------------
Binance USDT spot piyasasini 15dk ve 1 saatlik zaman diliminde tarar.
UT Bot (ATR trailing stop) sinyali + LinReg Candle trend yonu + 1 saatlik
trend onayi uc'u ayni yonde ise "GUCLU AL/SAT" sinyali uretir.

Kullanim:
    python3 scanner.py                 # taramayi calistir, sonucu yazdir ve JSON'a kaydet
    python3 scanner.py --json-only     # sadece JSON ciktisi (stdout'a), log basma

Cikti dosyasi: latest_signals.json (bu script ile ayni klasorde)
"""

import os
import sys
import json
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

# data-api.binance.vision Binance'in genel (herkese acik) piyasa verisi aynasidir;
# api.binance.com bazi bulut saglayici IP araliklarini (or. GitHub Actions) engelleyebiliyor,
# bu yuzden once mirror'i deniyoruz, olmazsa ana API'ye dusuyoruz.
BINANCE_BASES = ["https://data-api.binance.vision", "https://api.binance.com"]

# ---------------------------------------------------------------------------
# Ayarlar (varsayilanlar)
# ---------------------------------------------------------------------------
MIN_24H_QUOTE_VOLUME_USDT = 500_000     # bu hacmin altindaki pariteler elenir
UT_KEY_VALUE = 1.0                       # UT Bot "Key Value" (sensitivity)
UT_ATR_PERIOD = 10                       # UT Bot ATR periyodu
LINREG_LENGTH = 7                        # LinReg Candle uzunlugu (sinyal yumusatma)
KLINES_LIMIT = 150                       # her sembol/timeframe icin cekilen mum sayisi (warmup icin)
TIMEFRAME_ENTRY = "15m"
TIMEFRAME_CONFIRM = "1h"
MAX_WORKERS = 8                          # es zamanli istek sayisi
REQUEST_TIMEOUT = 10
NOTIFY_COOLDOWN_SECONDS = 2 * 60 * 60    # guclu sinyal bildirimleri arasinda en az bu kadar bekle (spam onleme)

# Leveraged token / stablecoin-stablecoin gibi anlamsiz pariteleri disla
EXCLUDE_SUFFIXES = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
EXCLUDE_SYMBOLS = {
    "USDCUSDT", "BUSDUSDT", "FDUSDUSDT", "TUSDUSDT", "DAIUSDT",
    "USDPUSDT", "EURUSDT", "GBPUSDT", "AEURUSDT", "USTUSDT",
}


def log(msg):
    print(f"[{datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')}] {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Bildirim (ntfy.sh)
# ---------------------------------------------------------------------------
def tradingview_url(symbol):
    """Binance sembolu icin TradingView grafik linki (or. BTCUSDT -> BINANCE:BTCUSDT)."""
    return f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}"


def notify_state_path():
    return __file__.rsplit("/", 1)[0] + "/notify_state.json"


def load_last_notified_at():
    """En son GUCLU sinyal bildiriminin ne zaman gonderildigini okur (throttle icin). Yoksa None doner."""
    try:
        with open(notify_state_path()) as f:
            data = json.load(f)
        return datetime.datetime.fromisoformat(data["last_notified_at"])
    except Exception:
        return None


def save_last_notified_at(dt):
    """En son bildirim zamanini notify_state.json'a yazar (repo'ya commitlenip calisma boyunca kalici olur)."""
    with open(notify_state_path(), "w") as f:
        json.dump({"last_notified_at": dt.isoformat()}, f)


def send_ntfy(message, title=None, priority="default", click_url=None):
    """NTFY_TOPIC ortam degiskeni tanimliysa ntfy.sh uzerinden push bildirimi gonderir."""
    topic = os.environ.get("NTFY_TOPIC")
    if not topic:
        log("NTFY_TOPIC tanimli degil, bildirim gonderilmiyor.")
        return
    headers = {"Priority": priority}
    if title:
        headers["Title"] = title.encode("utf-8")
    if click_url:
        # Bildirime tiklaninca dogrudan bu linki acar (tek sinyal varsa TradingView grafigi)
        headers["Click"] = click_url
    try:
        requests.post(f"https://ntfy.sh/{topic}", data=message.encode("utf-8"), headers=headers, timeout=10)
        log("ntfy bildirimi gonderildi.")
    except Exception as e:
        log(f"ntfy gonderim hatasi: {e}")


# ---------------------------------------------------------------------------
# Binance veri cekme
# ---------------------------------------------------------------------------
def binance_get(path, params=None):
    """BINANCE_BASES icindeki adresleri sirayla dener (biri engellenmis/erisilemezse digerine gecer)."""
    last_err = None
    for base in BINANCE_BASES:
        try:
            r = requests.get(f"{base}{path}", params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                time.sleep(2)
                r = requests.get(f"{base}{path}", params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            # Binance hata yanitlari {"code": ..., "msg": ...} seklinde gelir (ornegin
            # bulut IP'lerinin engellenmesi durumunda) -- bunu erken yakala.
            if isinstance(data, dict) and "code" in data and "msg" in data:
                raise RuntimeError(f"{base} hata dondurdu: {data.get('msg')}")
            return data
        except Exception as e:
            last_err = e
            log(f"{base}{path} basarisiz ({e}), sonraki adres deneniyor...")
            continue
    raise RuntimeError(f"Tum Binance adresleri basarisiz: {last_err}")


def get_usdt_symbols(min_volume=MIN_24H_QUOTE_VOLUME_USDT):
    """Binance spot USDT pariteleri, hacim filtresi uygulanmis, TRADING durumunda."""
    info = binance_get("/api/v3/exchangeInfo")
    tradable = set()
    for s in info["symbols"]:
        if (
            s["quoteAsset"] == "USDT"
            and s["status"] == "TRADING"
            and s["isSpotTradingAllowed"]
            and not s["symbol"].endswith(EXCLUDE_SUFFIXES)
            and s["symbol"] not in EXCLUDE_SYMBOLS
        ):
            tradable.add(s["symbol"])

    tickers = binance_get("/api/v3/ticker/24hr")
    qualified = []
    for t in tickers:
        sym = t["symbol"]
        if sym in tradable:
            try:
                qv = float(t["quoteVolume"])
            except (KeyError, ValueError):
                continue
            if qv >= min_volume:
                qualified.append((sym, qv))

    qualified.sort(key=lambda x: x[1], reverse=True)
    return [sym for sym, _ in qualified]


def get_klines(symbol, interval, limit=KLINES_LIMIT):
    data = binance_get("/api/v3/klines", params={"symbol": symbol, "interval": interval, "limit": limit})
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_base", "taker_quote", "ignore",
    ])
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype(float)
    return df


# ---------------------------------------------------------------------------
# Indikator mantigi
# ---------------------------------------------------------------------------
def wilder_atr(df, period):
    high, low, close = df["high"].values, df["low"].values, df["close"].values
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    atr = np.empty_like(tr)
    atr[:period] = np.nan
    if len(tr) >= period:
        atr[period - 1] = tr[:period].mean()
        for i in range(period, len(tr)):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def ut_bot_signal(df, key_value=UT_KEY_VALUE, atr_period=UT_ATR_PERIOD):
    """
    UT Bot Alerts (QuantNomad) mantigi.
    Donus: ('buy' | 'sell' | None, trend) -- trend: 'up' | 'down' | None
    Sinyal, SON KAPANAN mum icin hesaplanir (df'in son satiri).
    """
    close = df["close"].values
    atr = wilder_atr(df, atr_period)
    n_loss = key_value * atr

    start = atr_period  # ATR stabilize olana kadar bekle
    if len(close) <= start + 2:
        return None, None

    stop = np.full(len(close), np.nan)
    stop[start] = close[start]  # baslangic degeri

    for i in range(start + 1, len(close)):
        prev_stop = stop[i - 1]
        c, c_prev = close[i], close[i - 1]
        nl = n_loss[i]
        if np.isnan(nl):
            stop[i] = prev_stop
            continue
        if c > prev_stop and c_prev > prev_stop:
            stop[i] = max(prev_stop, c - nl)
        elif c < prev_stop and c_prev < prev_stop:
            stop[i] = min(prev_stop, c + nl)
        elif c > prev_stop:
            stop[i] = c - nl
        else:
            stop[i] = c + nl

    last, prev = len(close) - 1, len(close) - 2
    if np.isnan(stop[last]) or np.isnan(stop[prev]):
        return None, None

    trend = "up" if close[last] > stop[last] else "down"

    signal = None
    if close[prev] <= stop[prev] and close[last] > stop[last]:
        signal = "buy"
    elif close[prev] >= stop[prev] and close[last] < stop[last]:
        signal = "sell"

    return signal, trend


def _linreg_endpoint(y):
    """Verilen pencere (y) icin OLS dogrusunun son noktadaki degeri (ta.linreg offset=0 ile ayni)."""
    n = len(y)
    x = np.arange(n)
    x_mean, y_mean = x.mean(), y.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return y_mean
    slope = ((x - x_mean) * (y - y_mean)).sum() / denom
    intercept = y_mean - slope * x_mean
    return intercept + slope * (n - 1)


def linreg_trend(df, length=LINREG_LENGTH):
    """
    LinReg Candle trend yonu: son kapanan mum icin smoothed close vs smoothed open.
    Donus: 'green' | 'red' | None
    """
    if len(df) < length:
        return None
    window_close = df["close"].values[-length:]
    window_open = df["open"].values[-length:]
    lr_close = _linreg_endpoint(window_close)
    lr_open = _linreg_endpoint(window_open)
    return "green" if lr_close >= lr_open else "red"


# ---------------------------------------------------------------------------
# Tek sembol degerlendirme
# ---------------------------------------------------------------------------
def evaluate_symbol(symbol):
    try:
        df_entry = get_klines(symbol, TIMEFRAME_ENTRY)
        df_confirm = get_klines(symbol, TIMEFRAME_CONFIRM)
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

    signal, trend_entry = ut_bot_signal(df_entry)
    lr_entry = linreg_trend(df_entry)
    lr_confirm = linreg_trend(df_confirm)

    result = {
        "symbol": symbol,
        "signal": signal,
        "ut_trend_15m": trend_entry,
        "linreg_15m": lr_entry,
        "linreg_1h": lr_confirm,
        "last_close": float(df_entry["close"].iloc[-1]),
        "bar_time_15m": int(df_entry["open_time"].iloc[-1]),
    }

    strong = None
    if signal == "buy" and lr_entry == "green" and lr_confirm == "green":
        strong = "GUCLU_AL"
    elif signal == "sell" and lr_entry == "red" and lr_confirm == "red":
        strong = "GUCLU_SAT"
    result["strong_signal"] = strong
    return result


# ---------------------------------------------------------------------------
# Ana tarama
# ---------------------------------------------------------------------------
def run_scan(min_volume=MIN_24H_QUOTE_VOLUME_USDT, verbose=True):
    if verbose:
        log("Binance USDT pariteleri cekiliyor...")
    symbols = get_usdt_symbols(min_volume)
    if verbose:
        log(f"{len(symbols)} parite hacim filtresini gecti, taraniyor...")

    results = []
    errors = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(evaluate_symbol, s): s for s in symbols}
        for fut in as_completed(futures):
            res = fut.result()
            if "error" in res:
                errors.append(res)
            else:
                results.append(res)

    strong_signals = [r for r in results if r["strong_signal"]]

    summary = {
        "scanned_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "symbols_scanned": len(results),
        "symbols_errored": len(errors),
        "strong_signal_count": len(strong_signals),
        "strong_signals": strong_signals,
        "all_results": results,
    }

    if verbose:
        log(f"Tarama bitti: {len(results)} sembol basarili, {len(errors)} hata, {len(strong_signals)} guclu sinyal.")
        for s in strong_signals:
            log(f"  {s['strong_signal']}: {s['symbol']} @ {s['last_close']}")

    return summary


if __name__ == "__main__":
    json_only = "--json-only" in sys.argv
    summary = run_scan(verbose=not json_only)
    out_path = __file__.rsplit("/", 1)[0] + "/latest_signals.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)
    if json_only:
        print(json.dumps(summary))
    else:
        log(f"Sonuclar kaydedildi: {out_path}")

    if os.environ.get("FORCE_TEST_NOTIFY"):
        send_ntfy(
            "Kurulum basarili calisiyor. Gercek GUCLU AL/SAT sinyalleri geldikce burada bildirim alacaksin.",
            title="Crypto Scanner - Test Bildirimi",
        )
    elif summary["strong_signals"]:
        signals = summary["strong_signals"]
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        last_notified = load_last_notified_at()
        seconds_since_last = (now_utc - last_notified).total_seconds() if last_notified else None
        if seconds_since_last is not None and seconds_since_last < NOTIFY_COOLDOWN_SECONDS:
            remaining_min = int((NOTIFY_COOLDOWN_SECONDS - seconds_since_last) / 60)
            log(f"Guclu sinyal var ama bildirim 2 saatlik bekleme suresinde ({remaining_min} dk kaldi), bildirim atlaniyor.")
        else:
            lines = [
                f"{s['strong_signal'].replace('_', ' ')}: {s['symbol']} @ {s['last_close']}\n{tradingview_url(s['symbol'])}"
                for s in signals
            ]
            # Tek sinyal varsa bildirime tiklaninca dogrudan o coinin TradingView grafigi acilsin
            click_url = tradingview_url(signals[0]["symbol"]) if len(signals) == 1 else None
            send_ntfy(
                "\n\n".join(lines),
                title=f"{len(signals)} Guclu Sinyal (15m/1h UT Bot + LinReg)",
                priority="high",
                click_url=click_url,
            )
            save_last_notified_at(now_utc)
