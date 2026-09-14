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
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

import matplotlib
matplotlib.use("Agg")  # GitHub Actions gibi ekransiz ortamlarda calismak icin
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

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

# Bildirime eklenen grafik gorseli icin ayarlar
CHART_TIMEFRAME = "4h"                   # bildirime eklenen grafigin zaman dilimi
CHART_KLINES_LIMIT = 60                  # grafikte gosterilen mum sayisi (60 x 4h ~ 10 gun)
CHART_FILENAME = "latest_chart.png"
GITHUB_REPO_RAW_BASE = "https://raw.githubusercontent.com/mehmetkural/crypto-utbot-linreg-scanner/main"

# GitHub Pages paneli icin sinyal gecmisi ayarlari
HISTORY_FILENAME = "signals_history.json"
HISTORY_MAX_ENTRIES = 300                # gecmiste tutulan en fazla sinyal sayisi
HISTORY_EVAL_HORIZON_SECONDS = 4 * 60 * 60   # sinyalden bu kadar sure sonra isabet/kacirma degerlendirmesi yapilir

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


# ---------------------------------------------------------------------------
# GitHub Pages paneli icin sinyal gecmisi (signals_history.json)
# ---------------------------------------------------------------------------
def history_path():
    return __file__.rsplit("/", 1)[0] + "/" + HISTORY_FILENAME


def load_history():
    """signals_history.json'daki gecmis GUCLU sinyal kayitlarini okur. Yoksa bos liste doner."""
    try:
        with open(history_path()) as f:
            return json.load(f)
    except Exception:
        return []


def save_history(history):
    """Sinyal gecmisini signals_history.json'a yazar (repo'ya commitlenip GitHub Pages panelinden okunur)."""
    with open(history_path(), "w") as f:
        json.dump(history, f, indent=2)


def update_history_with_new_signals(history, strong_signals, now_utc):
    """Yeni GUCLU sinyalleri gecmise ekler (ayni sembol + ayni 15dk mumu tekrar eklenmez)."""
    existing_keys = {(h["symbol"], h["bar_time_15m"]) for h in history}
    for s in strong_signals:
        key = (s["symbol"], s["bar_time_15m"])
        if key in existing_keys:
            continue
        history.append({
            "symbol": s["symbol"],
            "direction": s["strong_signal"],          # "GUCLU_AL" | "GUCLU_SAT"
            "bar_time_15m": s["bar_time_15m"],
            "detected_at_utc": now_utc.isoformat(timespec="seconds"),
            "price_at_signal": s["last_close"],
            "evaluated": False,
            "outcome": None,                            # "hit" | "miss" (evaluated=True olunca dolar)
            "price_after": None,
            "evaluated_at": None,
        })
        existing_keys.add(key)
    # En yeni kayit en basta olacak sekilde sirala, listeyi HISTORY_MAX_ENTRIES ile sinirla
    history.sort(key=lambda h: h["detected_at_utc"], reverse=True)
    return history[:HISTORY_MAX_ENTRIES]


def evaluate_pending_history(history, results_lookup, now_utc):
    """
    Henuz degerlendirilmemis kayitlar icin, sinyal uzerinden HISTORY_EVAL_HORIZON_SECONDS
    kadar sure gectiyse ve sembol bu taramada tekrar cekildiyse (run_scan sonuclarindan,
    EKSTRA API cagrisi yapmadan) isabet/kacirma (hit/miss) degerlendirmesi yapar.
    """
    for h in history:
        if h["evaluated"]:
            continue
        try:
            detected_at = datetime.datetime.fromisoformat(h["detected_at_utc"])
        except Exception:
            continue
        if (now_utc - detected_at).total_seconds() < HISTORY_EVAL_HORIZON_SECONDS:
            continue
        current_price = results_lookup.get(h["symbol"])
        if current_price is None:
            continue  # bu sembol bu taramada yok (hacim filtresi vb.), sonraki taramada tekrar denenir
        if h["direction"] == "GUCLU_AL":
            outcome = "hit" if current_price > h["price_at_signal"] else "miss"
        else:
            outcome = "hit" if current_price < h["price_at_signal"] else "miss"
        h["evaluated"] = True
        h["outcome"] = outcome
        h["price_after"] = current_price
        h["evaluated_at"] = now_utc.isoformat(timespec="seconds")
    return history


# ---------------------------------------------------------------------------
# Bildirime eklenen grafik gorseli
# ---------------------------------------------------------------------------
def heikin_ashi(df):
    """
    Verilen OHLC DataFrame'inden Heikin Ashi mumlarini hesaplar.
    Donus: open_time, open, high, low, close kolonlarini iceren yeni bir DataFrame.
    """
    o = df["open"].values
    h = df["high"].values
    l = df["low"].values
    c = df["close"].values
    n = len(df)

    ha_close = (o + h + l + c) / 4
    ha_open = np.empty(n)
    ha_open[0] = (o[0] + c[0]) / 2
    for i in range(1, n):
        ha_open[i] = (ha_open[i - 1] + ha_close[i - 1]) / 2
    ha_high = np.maximum.reduce([h, ha_open, ha_close])
    ha_low = np.minimum.reduce([l, ha_open, ha_close])

    return pd.DataFrame({
        "open_time": df["open_time"].values,
        "open": ha_open,
        "high": ha_high,
        "low": ha_low,
        "close": ha_close,
    })


def _draw_candlestick_panel(ax, symbol, df, timeframe_label):
    """
    Verilen eksene (ax) Heikin Ashi mumlarini, UT Bot ATR trailing-stop cizgisini
    (al/sat ok isaretleriyle) ve LinReg Candle trend seridini birlikte cizer.
    """
    ha = heikin_ashi(df)
    opens = ha["open"].values
    highs = ha["high"].values
    lows = ha["low"].values
    closes = ha["close"].values
    n = len(ha)

    close_raw = df["close"].values
    stop = _ut_bot_stop_series(df, UT_KEY_VALUE, UT_ATR_PERIOD)
    linreg_colors = _linreg_color_series(df, LINREG_LENGTH)

    ax.set_facecolor("#0d1117")

    # --- Heikin Ashi mumlari ---
    for i in range(n):
        up = closes[i] >= opens[i]
        color = "#26a69a" if up else "#ef5350"
        ax.plot([i, i], [lows[i], highs[i]], color=color, linewidth=1, zorder=2)
        body_bottom = min(opens[i], closes[i])
        body_height = abs(closes[i] - opens[i])
        if body_height <= 0:
            body_height = (highs[i] - lows[i]) * 0.01 or 0.0001
        ax.add_patch(Rectangle((i - 0.3, body_bottom), 0.6, body_height, color=color, zorder=2))

    # --- UT Bot trailing-stop cizgisi (trend yonune gore renkli) ---
    xs = np.arange(n)
    valid = ~np.isnan(stop)
    up_line = np.where(valid & (close_raw > stop), stop, np.nan)
    down_line = np.where(valid & (close_raw <= stop), stop, np.nan)
    ax.plot(xs, up_line, color="#4fc3f7", linewidth=1.4, alpha=0.9, zorder=3)
    ax.plot(xs, down_line, color="#ffb74d", linewidth=1.4, alpha=0.9, zorder=3)

    # --- Al/Sat ok isaretleri (UT Bot stop cizgisinin kesildigi noktalar) ---
    for i in range(1, n):
        if not valid[i] or not valid[i - 1]:
            continue
        prev_up = close_raw[i - 1] > stop[i - 1]
        cur_up = close_raw[i] > stop[i]
        if not prev_up and cur_up:
            ax.scatter([i], [lows[i]], marker="^", color="#26a69a", s=50, zorder=5, edgecolors="white", linewidths=0.4)
        elif prev_up and not cur_up:
            ax.scatter([i], [highs[i]], marker="v", color="#ef5350", s=50, zorder=5, edgecolors="white", linewidths=0.4)

    # --- Eksen limitleri (stop cizgisi dahil) + LinReg seridi icin alt bosluk ---
    y_candidates = [highs, lows]
    if valid.any():
        y_candidates.append(stop[valid])
    y_max = max(np.nanmax(a) for a in y_candidates)
    y_min = min(np.nanmin(a) for a in y_candidates)
    y_range = (y_max - y_min) or (y_max * 0.01) or 1.0
    band_h = y_range * 0.05
    band_gap = y_range * 0.03
    band_y = y_min - band_gap - band_h

    # --- LinReg Candle trend seridi (alt kisimda renkli serit) ---
    for i in range(n):
        c = linreg_colors[i]
        if c is None:
            continue
        ax.add_patch(Rectangle((i - 0.5, band_y), 1.0, band_h, color=("#26a69a" if c == "green" else "#ef5350"), linewidth=0, zorder=2))

    ax.set_ylim(band_y - band_gap, y_max + y_range * 0.05)
    ax.set_xlim(-1, n)
    ax.set_title(f"{symbol}  ({timeframe_label}) - HA + UT Bot + LinReg", color="white", fontsize=11)
    ax.tick_params(colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color("#333333")
    ax.grid(color="#222222", linewidth=0.5)

    step = max(n // 5, 1)
    tick_positions = list(range(0, n, step))
    tick_labels = [
        datetime.datetime.fromtimestamp(int(df["open_time"].iloc[p]) / 1000, tz=datetime.timezone.utc).strftime("%d/%m %H:%M")
        for p in tick_positions
    ]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=30, ha="right", fontsize=7)


def generate_signals_chart(symbol_dfs, out_path, timeframe_label):
    """
    Bir veya birden fazla sinyal sembolunun mum grafigini tek bir PNG'de izgara
    (grid) halinde birlestirir; ntfy bildirimine tek gorsel olarak eklenir.
    symbol_dfs: [(symbol, df), ...]
    """
    n = len(symbol_dfs)
    cols = 1 if n == 1 else (2 if n <= 4 else 3)
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(cols * 6, rows * 4), dpi=110, squeeze=False)
    fig.patch.set_facecolor("#0d1117")

    for idx, (symbol, df) in enumerate(symbol_dfs):
        ax = axes[idx // cols][idx % cols]
        _draw_candlestick_panel(ax, symbol, df, timeframe_label)

    # Kullanilmayan izgara hucrelerini gizle (grid tam dolmadiysa)
    for idx in range(n, rows * cols):
        axes[idx // cols][idx % cols].axis("off")

    fig.tight_layout()
    fig.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close(fig)


# ---------------------------------------------------------------------------
# Repo'ya commit/push (notify_state.json ve grafik gorseli icin)
# ---------------------------------------------------------------------------
def run_git(*args):
    repo_dir = __file__.rsplit("/", 1)[0]
    return subprocess.run(["git", *args], cwd=repo_dir, capture_output=True, text=True)


def commit_and_push_files(paths, message):
    """Verilen dosyalari (degisiklik varsa) commit'leyip push eder. Basarili/gereksizse True doner."""
    run_git("add", *paths)
    status_res = run_git("status", "--porcelain", *paths)
    if not status_res.stdout.strip():
        log("Commit edilecek degisiklik yok.")
        return True

    run_git("config", "user.name", "github-actions[bot]")
    run_git("config", "user.email", "github-actions[bot]@users.noreply.github.com")

    commit_res = run_git("commit", "-m", message)
    if commit_res.returncode != 0:
        log(f"git commit hatasi: {commit_res.stderr.strip()}")
        return False

    push_res = run_git("push")
    if push_res.returncode != 0:
        log(f"git push basarisiz, pull --rebase deneniyor: {push_res.stderr.strip()}")
        run_git("pull", "--rebase")
        push_res = run_git("push")
        if push_res.returncode != 0:
            log(f"git push tekrar basarisiz: {push_res.stderr.strip()}")
            return False
    return True


def send_ntfy(message, title=None, priority="default", click_url=None, attach_url=None):
    """NTFY_TOPIC ortam degiskeni tanimliysa ntfy.sh uzerinden push bildirimi gonderir."""
    topic = os.environ.get("NTFY_TOPIC")
    if not topic:
        log("NTFY_TOPIC tanimli degil, bildirim gonderilmiyor.")
        return
    headers = {"Priority": priority}
    if title:
        headers["Title"] = title.encode("utf-8")
    if attach_url:
        # Bildirime bir gorsel ekler (ornegin sinyal coininin grafigi)
        headers["Attach"] = attach_url
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


def _ut_bot_stop_series(df, key_value=UT_KEY_VALUE, atr_period=UT_ATR_PERIOD):
    """
    UT Bot Alerts (QuantNomad) ATR trailing-stop cizgisini TUM barlar icin hesaplar
    (grafik cizimi icin de kullanilir). ut_bot_signal() bu dizinin sadece son iki
    degerini kullanarak sinyal karari verir; hesaplama mantigi birebir aynidir.
    Donus: stop degerlerini iceren numpy dizisi (henuz stabil olmayan barlarda NaN).
    """
    close = df["close"].values
    atr = wilder_atr(df, atr_period)
    n_loss = key_value * atr

    start = atr_period  # ATR stabilize olana kadar bekle
    stop = np.full(len(close), np.nan)
    if len(close) <= start:
        return stop
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

    return stop


def ut_bot_signal(df, key_value=UT_KEY_VALUE, atr_period=UT_ATR_PERIOD):
    """
    UT Bot Alerts (QuantNomad) mantigi.
    Donus: ('buy' | 'sell' | None, trend) -- trend: 'up' | 'down' | None
    Sinyal, SON KAPANAN mum icin hesaplanir (df'in son satiri).
    """
    close = df["close"].values
    if len(close) <= atr_period + 2:
        return None, None

    stop = _ut_bot_stop_series(df, key_value, atr_period)

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


def _linreg_color_series(df, length=LINREG_LENGTH):
    """
    LinReg Candle trend rengini HER bar icin hesaplar (grafikte alt serit olarak
    gosterilir). linreg_trend() ile ayni mantigi kullanir, tum barlar icin tekrarlar.
    Donus: uzunlugu len(df) olan liste; 'green' | 'red' | None (pencere dolmadiysa).
    """
    closes = df["close"].values
    opens = df["open"].values
    n = len(df)
    colors = [None] * n
    for i in range(length - 1, n):
        window_close = closes[i - length + 1: i + 1]
        window_open = opens[i - length + 1: i + 1]
        lr_close = _linreg_endpoint(window_close)
        lr_open = _linreg_endpoint(window_open)
        colors[i] = "green" if lr_close >= lr_open else "red"
    return colors


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

    # --- GitHub Pages paneli icin: sinyal gecmisini guncelle/degerlendir ve
    # son tarama sonuclarini + gecmisi her calismada repo'ya commitle
    # (bildirim gonderilsin ya da gonderilmesin, panel her zaman guncel kalsin) ---
    try:
        history = load_history()
        now_hist = datetime.datetime.now(datetime.timezone.utc)
        results_lookup = {r["symbol"]: r["last_close"] for r in summary["all_results"]}
        history = evaluate_pending_history(history, results_lookup, now_hist)
        history = update_history_with_new_signals(history, summary["strong_signals"], now_hist)
        save_history(history)
    except Exception as e:
        log(f"Sinyal gecmisi guncellenirken hata: {e}")
    if not commit_and_push_files(
        ["latest_signals.json", HISTORY_FILENAME],
        "Panel verileri guncellendi (son tarama + sinyal gecmisi)",
    ):
        log("Panel verileri (latest_signals.json / signals_history.json) push edilemedi.")

    if os.environ.get("FORCE_TEST_NOTIFY"):
        # Test bildirimine ornek bir grafik de ekleyerek gorsel ozelligin
        # gercek bir sinyal beklemeden calistigini gostermis oluyoruz.
        test_attach_url = None
        try:
            demo_symbols = ["BTCUSDT", "ETHUSDT"]
            symbol_dfs = [
                (sym, get_klines(sym, CHART_TIMEFRAME, limit=CHART_KLINES_LIMIT))
                for sym in demo_symbols
            ]
            chart_path = __file__.rsplit("/", 1)[0] + "/" + CHART_FILENAME
            generate_signals_chart(symbol_dfs, chart_path, CHART_TIMEFRAME)
            if commit_and_push_files([CHART_FILENAME], "Test bildirimi icin ornek grafik guncellendi"):
                test_attach_url = f"{GITHUB_REPO_RAW_BASE}/{CHART_FILENAME}"
            else:
                log("Test grafigi push edilemedi, test bildirimi gorselsiz gonderilecek.")
        except Exception as e:
            log(f"Test grafigi olusturulamadi: {e}")
        send_ntfy(
            "Kurulum basarili calisiyor. Gercek GUCLU AL/SAT sinyalleri geldikce burada bildirim alacaksin.",
            title="Crypto Scanner - Test Bildirimi",
            attach_url=test_attach_url,
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
            save_last_notified_at(now_utc)

            # Sinyal sayisi ne olursa olsun, tum sinyal coinlerinin 4 saatlik mum
            # grafigini tek bir gorselde (izgara halinde) birlestirip bildirime ekle
            attach_url = None
            try:
                symbol_dfs = [
                    (s["symbol"], get_klines(s["symbol"], CHART_TIMEFRAME, limit=CHART_KLINES_LIMIT))
                    for s in signals
                ]
                chart_path = __file__.rsplit("/", 1)[0] + "/" + CHART_FILENAME
                generate_signals_chart(symbol_dfs, chart_path, CHART_TIMEFRAME)
                pushed = commit_and_push_files(
                    [CHART_FILENAME, "notify_state.json"],
                    "Guclu sinyal grafigi ve bildirim durumu guncellendi",
                )
                if pushed:
                    attach_url = f"{GITHUB_REPO_RAW_BASE}/{CHART_FILENAME}"
                else:
                    log("Grafik push edilemedi, bildirim gorselsiz gonderilecek.")
            except Exception as e:
                log(f"Grafik olusturma hatasi, bildirim gorselsiz gonderilecek: {e}")
                commit_and_push_files(["notify_state.json"], "Bildirim zaman damgasi guncellendi")

            send_ntfy(
                "\n\n".join(lines),
                title=f"{len(signals)} Guclu Sinyal (15m/1h UT Bot + LinReg)",
                priority="high",
                click_url=click_url,
                attach_url=attach_url,
            )
