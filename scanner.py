#!/usr/bin/env python3
"""
Valid Highs & Lows (Structure Break) + UT Bot/LinReg Crypto Scanner
----------------------------------------------------------------------
Binance USDT spot piyasasini 1 saatlik (1h) zaman diliminde tarar.
Ana tarama kriteri kullanicinin sagladigi "Valid Highs & Lows (Structure
Break)" Pine Script v6 indikatorudur: bir swing yuksek/dusuk, fiyat son
onayli pivot dusugu/yuksegi kirinca (VALID_HL_CONFIRM_ON_CLOSE'a gore kapanis
ya da fitille) "valid" sayilir. Bu kirilim SON barda gerceklesmisse VE
kirilan ekstrem nokta son VALID_HL_FRESHNESS_BARS bar icinde olusmussa
"GUCLU AL/SAT" sinyali uretilir.
UT Bot (ATR trailing stop), LinReg Candle trend yonu ve MACD/hacim panelleri
artik sinyal kapisi degil; arkaplanda hesaplanmaya ve grafikte referans
olarak gosterilmeye devam eder.

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
from matplotlib.gridspec import GridSpec

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
LINREG_LENGTH = 11                       # LinReg Candle uzunlugu (sinyal yumusatma)
KLINES_LIMIT = 150                       # her sembol/timeframe icin cekilen mum sayisi (warmup icin)
TIMEFRAME_ENTRY = "1h"                   # giris (sinyal arama) zaman dilimi
TIMEFRAME_CONFIRM = "1h"                 # onay zaman dilimi (tum parametreler 1 saatlik)
MAX_WORKERS = 8                          # es zamanli istek sayisi
REQUEST_TIMEOUT = 10
NOTIFY_COOLDOWN_SECONDS = 2 * 60 * 60    # guclu sinyal bildirimleri arasinda en az bu kadar bekle (spam onleme)

# Bildirime eklenen grafik gorseli icin ayarlar
CHART_TIMEFRAME = "1h"                   # bildirime eklenen grafigin zaman dilimi
CHART_KLINES_LIMIT = 100                 # grafikte GOSTERILEN (ekranda gorunen) mum sayisi
CHART_STRUCT_WARMUP_BARS = 150           # gosterilen pencereden ONCE, sadece hesaplama (Valid H/L/MACD/hacim)
                                          # icin cekilen gizli "isinma" mumu -- gercek TradingView indikatoru
                                          # gibi, grafigin ilk barinda state'in sifirdan degil onceden
                                          # kurulmus halde baslamasini saglar (bkz. asagidaki display_bars)

# MACD paneli ve uyumsuzluk (divergence) tespiti icin ayarlar
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
MACD_DIVERGENCE_ORDER = 3                # swing (tepe/dip) noktasi icin +/- mum penceresi
VOLUME_MA_LENGTH = 20                    # hacim panelindeki hareketli ortalama periyodu

# "Valid Highs & Lows (Structure Break)" indikatoru (Pine Script v6, kullanici tarafindan
# saglandi) - artik ANA tarama kriteri. UT Bot/LinReg/MACD/Hacim arkaplanda hesaplanmaya devam eder.
VALID_HL_PIV_BARS = 5                    # ta.pivothigh/pivotlow pencere genisligi (her iki yanda)
VALID_HL_CONFIRM_ON_CLOSE = True         # True: kirilim kapanisla onaylanir, False: fitil (wick) yeterli
VALID_HL_FRESHNESS_BARS = 5              # onaylanan ekstrem nokta, onay barina gore en fazla bu kadar bar once olustuysa "taze" sayilir

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
    """Yeni GUCLU sinyalleri gecmise ekler (ayni sembol + ayni giris mumu tekrar eklenmez).

    Not: eski kayitlarda (timeframe degisikliginden once) bar zamani "bar_time_15m"
    anahtariyla tutuluyordu; geriye donuk uyumluluk icin okurken ikisini de destekler.
    """
    existing_keys = {(h["symbol"], h.get("bar_time_entry", h.get("bar_time_15m"))) for h in history}
    for s in strong_signals:
        key = (s["symbol"], s["bar_time_entry"])
        if key in existing_keys:
            continue
        history.append({
            "symbol": s["symbol"],
            "direction": s["strong_signal"],          # "GUCLU_AL" | "GUCLU_SAT"
            "bar_time_entry": s["bar_time_entry"],
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


def macd_series(df, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL):
    """
    Standart MACD hesaplamasi (EMA farki + sinyal cizgisi + histogram).
    Donus: (macd_line, signal_line, histogram) - uc numpy dizisi.
    """
    close = df["close"]
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line.values, signal_line.values, hist.values


def _find_swing_points(values, order=MACD_DIVERGENCE_ORDER):
    """
    values dizisindeki yerel tepe (max) ve dip (min) noktalarinin indekslerini bulur.
    Bir nokta, kendisinden +/- order kadar uzaklikta ki pencere icinde tek (unique)
    en yuksek/en dusuk deger ise swing noktasi sayilir.
    Donus: (tepe_indeksleri, dip_indeksleri)
    """
    n = len(values)
    highs, lows = [], []
    for i in range(order, n - order):
        window = values[i - order: i + order + 1]
        if values[i] == np.max(window) and np.argmax(window) == order:
            highs.append(i)
        if values[i] == np.min(window) and np.argmin(window) == order:
            lows.append(i)
    return highs, lows


def detect_macd_divergence(df, macd_line, order=MACD_DIVERGENCE_ORDER):
    """
    Fiyatin (close) son iki swing tepe/dip noktasini, ayni indekslerdeki MACD
    degerleriyle karsilastirarak klasik (regular) pozitif/negatif uyumsuzlugu tespit eder:
      - Pozitif (bullish) uyumsuzluk: fiyat daha DUSUK dip yaparken MACD daha YUKSEK dip yapar
        (asagi yonlu momentum zayifliyor -> olasi yukselis donusu).
      - Negatif (bearish) uyumsuzluk: fiyat daha YUKSEK tepe yaparken MACD daha DUSUK tepe yapar
        (yukari yonlu momentum zayifliyor -> olasi dusus donusu).
    Donus: {"bullish": bool, "bullish_points": (i1, i2) | None,
            "bearish": bool, "bearish_points": (i1, i2) | None}
    """
    close = df["close"].values
    price_highs, price_lows = _find_swing_points(close, order)

    result = {"bullish": False, "bullish_points": None, "bearish": False, "bearish_points": None}

    if len(price_highs) >= 2:
        i1, i2 = price_highs[-2], price_highs[-1]
        if close[i2] > close[i1] and macd_line[i2] < macd_line[i1]:
            result["bearish"] = True
            result["bearish_points"] = (i1, i2)

    if len(price_lows) >= 2:
        j1, j2 = price_lows[-2], price_lows[-1]
        if close[j2] < close[j1] and macd_line[j2] > macd_line[j1]:
            result["bullish"] = True
            result["bullish_points"] = (j1, j2)

    return result


def _draw_candlestick_panel(ax, symbol, df, timeframe_label, divergence=None, display_bars=None):
    """
    Verilen eksene (ax) Heikin Ashi mumlarini ve UT Bot ATR trailing-stop cizgisini
    (al/sat ok isaretleriyle) cizer.
    divergence verilirse (bkz. detect_macd_divergence), pozitif/negatif MACD
    uyumsuzlugunu fiyat grafigi uzerinde de kesikli cizgi + etiketle isaretler.
    display_bars verilirse, hesaplamalar (Valid H/L) df'in TAMAMI uzerinde
    yapilir (gizli "isinma" gecmisi dahil) ama sadece SON display_bars mum cizilir/
    gosterilir -- boylece gorunen pencerenin basinda state sifirdan baslamaz.
    """
    ha = heikin_ashi(df)
    opens = ha["open"].values
    highs = ha["high"].values
    lows = ha["low"].values
    closes = ha["close"].values
    n_full = len(ha)
    offset = max(n_full - display_bars, 0) if display_bars else 0

    # Valid H/L artik HA mumlari uzerinden hesaplaniyor (bkz. evaluate_symbol) --
    # overlay'in de ayni HA serisi uzerinden hesaplanmasi gerekiyor, yoksa cizilen
    # HA mumlariyla H/L kirilim seviyeleri/etiketleri uyusmaz.
    struct = detect_valid_high_low(ha)

    ax.set_facecolor("#0d1117")

    # --- Heikin Ashi mumlari (sadece gorunen pencere cizilir) ---
    for i in range(offset, n_full):
        up = closes[i] >= opens[i]
        color = "#26a69a" if up else "#ef5350"
        ax.plot([i, i], [lows[i], highs[i]], color=color, linewidth=1, zorder=2)
        body_bottom = min(opens[i], closes[i])
        body_height = abs(closes[i] - opens[i])
        if body_height <= 0:
            body_height = (highs[i] - lows[i]) * 0.01 or 0.0001
        ax.add_patch(Rectangle((i - 0.3, body_bottom), 0.6, body_height, color=color, zorder=2))

    # Not: UT Bot artik tamamen arkaplan gostergesi -- stop cizgisi (ortadaki beyaz
    # cizgi) ve Buy/Sell etiketleri kafa karistirmamasi icin grafikten kaldirildi;
    # tek gorunur etiketler asagidaki Valid High/Low pinleri.

    # --- Valid Highs & Lows (Structure Break) - ana tarama kriteri, grafik uzerinde de gosterilir ---
    _draw_valid_hl_overlay(ax, df, struct=struct, offset=offset)

    # --- MACD uyumsuzlugu (varsa) fiyat grafiginde de isaretlenir ---
    if divergence:
        if divergence.get("bullish"):
            j1, j2 = divergence["bullish_points"]
            if j2 >= offset:
                ax.plot([max(j1, offset), j2], [lows[j1], lows[j2]], color="#69f0ae", linewidth=1.6, linestyle="--", zorder=4)
                ax.annotate("POZ UYUMSUZLUK", xy=(j2, lows[j2]), xytext=(0, -24), textcoords="offset points",
                            color="#69f0ae", fontsize=7, fontweight="bold", ha="center", va="top")
        if divergence.get("bearish"):
            i1, i2 = divergence["bearish_points"]
            if i2 >= offset:
                ax.plot([max(i1, offset), i2], [highs[i1], highs[i2]], color="#ff5252", linewidth=1.6, linestyle="--", zorder=4)
                ax.annotate("NEG UYUMSUZLUK", xy=(i2, highs[i2]), xytext=(0, 24), textcoords="offset points",
                            color="#ff5252", fontsize=7, fontweight="bold", ha="center", va="bottom")

    # --- Eksen limitleri ---
    y_candidates = [highs[offset:], lows[offset:]]
    y_max = max(np.nanmax(a) for a in y_candidates)
    y_min = min(np.nanmin(a) for a in y_candidates)
    y_range = (y_max - y_min) or (y_max * 0.01) or 1.0

    # Alt/ust bosluklar: Buy/Sell etiket kutulari, Valid H/L etiketleri ve (varsa)
    # uyumsuzluk etiketleri icin yeterli yer birakilir.
    top_margin = 0.28 if divergence and divergence.get("bearish") else 0.16
    bottom_margin = 0.16
    ax.set_ylim(y_min - y_range * bottom_margin, y_max + y_range * top_margin)
    ax.set_xlim(offset - 1, n_full)
    ax.set_title(f"{symbol}  ({timeframe_label}) - Valid H/L", color="white", fontsize=11)
    ax.tick_params(colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color("#333333")
    ax.grid(color="#222222", linewidth=0.5)

    visible_n = n_full - offset
    step = max(visible_n // 5, 1)
    tick_positions = list(range(offset, n_full, step))
    tick_labels = [
        datetime.datetime.fromtimestamp(int(df["open_time"].iloc[p]) / 1000, tz=datetime.timezone.utc).strftime("%d/%m %H:%M")
        for p in tick_positions
    ]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=30, ha="right", fontsize=7)


def _draw_macd_panel(ax, df, macd_line, signal_line, hist, divergence=None, display_bars=None):
    """
    Verilen eksene MACD cizgisini, sinyal cizgisini ve histogram cubuklarini cizer.
    divergence verilirse (bkz. detect_macd_divergence), pozitif/negatif uyumsuzlugu
    MACD cizgisi uzerinde kesikli baglanti + etiketle isaretler.
    display_bars verilirse, MACD tam seri (isinma dahil) uzerinden hesaplanmis olarak
    gelir; burada sadece son display_bars mum cizilir (bkz. _draw_candlestick_panel).
    """
    n = len(macd_line)
    offset = max(n - display_bars, 0) if display_bars else 0
    xs = np.arange(offset, n)

    ax.set_facecolor("#0d1117")

    hist_colors = ["#26a69a" if v >= 0 else "#ef5350" for v in hist[offset:]]
    ax.bar(xs, hist[offset:], color=hist_colors, width=0.8, zorder=2, alpha=0.7)
    ax.plot(xs, macd_line[offset:], color="#4fc3f7", linewidth=1.2, zorder=3, label="MACD")
    ax.plot(xs, signal_line[offset:], color="#ffb74d", linewidth=1.2, zorder=3, label="Sinyal")
    ax.axhline(0, color="#555555", linewidth=0.7, zorder=1)

    if divergence and divergence.get("bullish"):
        j1, j2 = divergence["bullish_points"]
        if j2 >= offset:
            j1c = max(j1, offset)
            ax.plot([j1c, j2], [macd_line[j1c], macd_line[j2]], color="#69f0ae", linewidth=1.8, linestyle="--", zorder=4)
            ax.scatter([j1c, j2], [macd_line[j1c], macd_line[j2]], color="#69f0ae", s=22, zorder=5)
            ax.annotate("POZ UYUMSUZLUK", xy=(j2, macd_line[j2]), xytext=(0, -12), textcoords="offset points",
                        color="#69f0ae", fontsize=7, fontweight="bold", ha="center", va="top")

    if divergence and divergence.get("bearish"):
        i1, i2 = divergence["bearish_points"]
        if i2 >= offset:
            i1c = max(i1, offset)
            ax.plot([i1c, i2], [macd_line[i1c], macd_line[i2]], color="#ff5252", linewidth=1.8, linestyle="--", zorder=4)
            ax.scatter([i1c, i2], [macd_line[i1c], macd_line[i2]], color="#ff5252", s=22, zorder=5)
            ax.annotate("NEG UYUMSUZLUK", xy=(i2, macd_line[i2]), xytext=(0, 12), textcoords="offset points",
                        color="#ff5252", fontsize=7, fontweight="bold", ha="center", va="bottom")

    ax.set_xlim(offset - 1, n)
    ax.tick_params(colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color("#333333")
    ax.grid(color="#222222", linewidth=0.5)
    ax.set_ylabel("MACD", color="#999999", fontsize=8)
    ax.legend(loc="upper left", fontsize=6, facecolor="#0d1117", edgecolor="#333333",
              labelcolor="white", framealpha=0.6)

    visible_n = n - offset
    step = max(visible_n // 5, 1)
    tick_positions = list(range(offset, n, step))
    tick_labels = [
        datetime.datetime.fromtimestamp(int(df["open_time"].iloc[p]) / 1000, tz=datetime.timezone.utc).strftime("%d/%m %H:%M")
        for p in tick_positions
    ]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=30, ha="right", fontsize=7)


def _draw_volume_panel(ax, df, length=VOLUME_MA_LENGTH, display_bars=None):
    """
    Verilen eksene hacim (volume) cubuklarini (mum yonune gore yesil/kirmizi) ve
    hacmin VOLUME_MA_LENGTH periyotluk hareketli ortalamasini cizgi olarak cizer.
    display_bars verilirse, hareketli ortalama tam seri (isinma dahil) uzerinden
    hesaplanir; sadece son display_bars mum cizilir (bkz. _draw_candlestick_panel).
    """
    ha = heikin_ashi(df)
    opens = ha["open"].values
    closes = ha["close"].values
    volumes = df["volume"].values
    n = len(volumes)
    offset = max(n - display_bars, 0) if display_bars else 0
    xs = np.arange(offset, n)

    vol_colors = ["#26a69a" if closes[i] >= opens[i] else "#ef5350" for i in range(offset, n)]
    vol_ma = pd.Series(volumes).rolling(length).mean().values

    ax.set_facecolor("#0d1117")
    ax.bar(xs, volumes[offset:], color=vol_colors, width=0.8, zorder=2, alpha=0.7)
    ax.plot(xs, vol_ma[offset:], color="#ffca28", linewidth=1.3, zorder=3, label=f"Hacim MA{length}")

    ax.set_xlim(offset - 1, n)
    ax.tick_params(colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_color("#333333")
    ax.grid(color="#222222", linewidth=0.5)
    ax.set_ylabel("Hacim", color="#999999", fontsize=8)
    ax.legend(loc="upper left", fontsize=6, facecolor="#0d1117", edgecolor="#333333",
              labelcolor="white", framealpha=0.6)

    visible_n = n - offset
    step = max(visible_n // 5, 1)
    tick_positions = list(range(offset, n, step))
    tick_labels = [
        datetime.datetime.fromtimestamp(int(df["open_time"].iloc[p]) / 1000, tz=datetime.timezone.utc).strftime("%d/%m %H:%M")
        for p in tick_positions
    ]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=30, ha="right", fontsize=7)


def _draw_valid_hl_overlay(ax, df, struct=None, offset=0):
    """
    "Valid Highs & Lows (Structure Break)" indikatorunu fiyat panelinin uzerine cizer:
      - Onaylanmis her Valid High/Low noktasina kirmizi "H" / yesil "L" etiketi
        (Pine'daki label.new ile ayni: noktanin OLUSTUGU barda, yani point_bar'da).
      - En son onaylanmis VH/VL seviyesinden, o noktadan grafigin sonuna kadar uzanan
        kalici bir yatay cizgi (Pine'daki "var line" + extend.right'in statik grafikteki
        karsiligi -- yeni bir onay gelene kadar ayni cizgi kalir).
      - Henuz onaylanmamis "bekleyen aday" (running high/low), gri kesikli cizgi ve
        "h?" / "l?" etiketiyle -- sadece grafigin SON barina gore (Pine'daki barstate.islast).
    struct, df'in TAMAMI (gizli isinma gecmisi dahil) uzerinden hesaplanmis olabilir;
    offset, gorunen pencerenin df icindeki baslangic indeksidir. offset'ten ONCE
    olusan noktalar icin etiket cizilmez (ekran disina tasip komsu panellere
    bulasmasin diye -- ax.annotate cizgiler gibi otomatik kirpilmiyor), ama en son
    onaylanmis seviye/cizgi state'i yine de dogru sekilde takip edilir ve gorunen
    pencereye kadar uzatilir.
    """
    if struct is None:
        struct = detect_valid_high_low(df)
    n = len(df)
    if n == 0:
        return

    last_vh_point = None
    last_vl_point = None
    for i in range(n):
        rec = struct[i]
        if rec["vh_new"] and rec["vh_point_bar"] is not None:
            pb, level = rec["vh_point_bar"], rec["vh"]
            if pb >= offset:
                ax.annotate(
                    "H", xy=(pb, level), xytext=(0, 16), textcoords="offset points",
                    ha="center", va="bottom", fontsize=8, fontweight="bold", color="white",
                    bbox=dict(boxstyle="round,pad=0.32", fc="#ef5350", ec="none"),
                    arrowprops=dict(arrowstyle="-", color="#ef5350", lw=1.4, shrinkA=0, shrinkB=3),
                    zorder=7,
                )
            last_vh_point = (pb, level)
        if rec["vl_new"] and rec["vl_point_bar"] is not None:
            pb, level = rec["vl_point_bar"], rec["vl"]
            if pb >= offset:
                ax.annotate(
                    "L", xy=(pb, level), xytext=(0, -16), textcoords="offset points",
                    ha="center", va="top", fontsize=8, fontweight="bold", color="white",
                    bbox=dict(boxstyle="round,pad=0.32", fc="#26a69a", ec="none"),
                    arrowprops=dict(arrowstyle="-", color="#26a69a", lw=1.4, shrinkA=0, shrinkB=3),
                    zorder=7,
                )
            last_vl_point = (pb, level)

    if last_vh_point is not None:
        pb, level = last_vh_point
        ax.plot([max(pb, offset), n - 1], [level, level], color="#ef5350", linewidth=1.1, alpha=0.85, zorder=4)
    if last_vl_point is not None:
        pb, level = last_vl_point
        ax.plot([max(pb, offset), n - 1], [level, level], color="#26a69a", linewidth=1.1, alpha=0.85, zorder=4)

    # Bekleyen (henuz onaylanmamis) aday -- sadece grafigin son barina gore
    last_rec = struct[-1]
    if last_rec["mode"] == 1 and last_rec["run_high_bar"] is not None:
        pb, level = last_rec["run_high_bar"], last_rec["run_high"]
        ax.plot([max(pb, offset), n - 1], [level, level], color="#9e9e9e", linewidth=1.0, linestyle="--", alpha=0.8, zorder=4)
        if pb >= offset:
            ax.annotate(
                "h?", xy=(pb, level), xytext=(0, 16), textcoords="offset points",
                ha="center", va="bottom", fontsize=8, fontweight="bold", color="white",
                bbox=dict(boxstyle="round,pad=0.32", fc="#9e9e9e", ec="none"),
                arrowprops=dict(arrowstyle="-", color="#9e9e9e", lw=1.2, shrinkA=0, shrinkB=3),
                zorder=7,
            )
    elif last_rec["mode"] == 2 and last_rec["run_low_bar"] is not None:
        pb, level = last_rec["run_low_bar"], last_rec["run_low"]
        ax.plot([max(pb, offset), n - 1], [level, level], color="#9e9e9e", linewidth=1.0, linestyle="--", alpha=0.8, zorder=4)
        if pb >= offset:
            ax.annotate(
                "l?", xy=(pb, level), xytext=(0, -16), textcoords="offset points",
                ha="center", va="top", fontsize=8, fontweight="bold", color="white",
                bbox=dict(boxstyle="round,pad=0.32", fc="#9e9e9e", ec="none"),
                arrowprops=dict(arrowstyle="-", color="#9e9e9e", lw=1.2, shrinkA=0, shrinkB=3),
                zorder=7,
            )


def generate_signals_chart(symbol_dfs, out_path, timeframe_label, display_bars=CHART_KLINES_LIMIT):
    """
    Bir veya birden fazla sinyal sembolunun mum grafigini, altinda MACD paneliyle
    (pozitif/negatif uyumsuzluk isaretli) ve hacim (volume + MA20) paneliyle
    birlikte tek bir PNG'de izgara (grid) halinde birlestirir; ntfy bildirimine
    tek gorsel olarak eklenir.
    symbol_dfs: [(symbol, df), ...] -- df'ler CHART_KLINES_LIMIT + CHART_STRUCT_WARMUP_BARS
    kadar mum icerebilir (bkz. cagiran taraf); tum hesaplamalar (Valid H/L, MACD,
    hacim MA) bu TAM seri uzerinden yapilir, ama sadece son display_bars mum cizilir.
    """
    n = len(symbol_dfs)
    cols = 1 if n == 1 else (2 if n <= 4 else 3)
    rows = (n + cols - 1) // cols

    fig = plt.figure(figsize=(cols * 6, rows * 7.2), dpi=110, constrained_layout=True)
    fig.patch.set_facecolor("#0d1117")
    gs = GridSpec(rows * 3, cols, figure=fig, height_ratios=[3, 1.3, 1.1] * rows, hspace=0.08, wspace=0.22)

    for idx, (symbol, df) in enumerate(symbol_dfs):
        r, c = idx // cols, idx % cols
        ax_price = fig.add_subplot(gs[3 * r, c])
        ax_macd = fig.add_subplot(gs[3 * r + 1, c])
        ax_volume = fig.add_subplot(gs[3 * r + 2, c])

        macd_line, signal_line, hist = macd_series(df)
        divergence = detect_macd_divergence(df, macd_line)

        _draw_candlestick_panel(ax_price, symbol, df, timeframe_label, divergence=divergence, display_bars=display_bars)
        ax_price.set_xticklabels([])  # tarih etiketleri sadece en alttaki hacim panelinde gosterilsin
        _draw_macd_panel(ax_macd, df, macd_line, signal_line, hist, divergence=divergence, display_bars=display_bars)
        ax_macd.set_xticklabels([])  # tarih etiketleri sadece en alttaki hacim panelinde gosterilsin
        _draw_volume_panel(ax_volume, df, display_bars=display_bars)

    # Kullanilmayan izgara hucrelerini gizle (grid tam dolmadiysa)
    for idx in range(n, rows * cols):
        r, c = idx // cols, idx % cols
        fig.add_subplot(gs[3 * r, c]).axis("off")
        fig.add_subplot(gs[3 * r + 1, c]).axis("off")
        fig.add_subplot(gs[3 * r + 2, c]).axis("off")

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


def _pivot_confirmed_series(values, bars, is_high):
    """
    Pine Script'teki ta.pivothigh(values, bars, bars) / ta.pivotlow(values, bars, bars)
    mantiginin birebir portu: merkezdeki bar (center), kendisinden 'bars' kadar once ve
    sonraki barlari da iceren pencerede TEK (unique) en yuksek/en dusuk deger ise pivot
    sayilir. Pine'daki gibi onay, merkez bardan 'bars' bar SONRA gerceklesir (confirmation
    lag) -- yani pivot degeri, donen dizide (center + bars) pozisyonunda gorunur.
    is_high=True ise pivot high (ta.pivothigh), False ise pivot low (ta.pivotlow) davranisi.
    Donus: values ile ayni uzunlukta numpy dizisi; pivot yoksa NaN.
    """
    n = len(values)
    out = np.full(n, np.nan)
    for center in range(bars, n - bars):
        window = values[center - bars: center + bars + 1]
        if is_high:
            if values[center] == np.max(window) and np.argmax(window) == bars:
                out[center + bars] = values[center]
        else:
            if values[center] == np.min(window) and np.argmin(window) == bars:
                out[center + bars] = values[center]
    return out


def detect_valid_high_low(df, piv_bars=VALID_HL_PIV_BARS, use_close=VALID_HL_CONFIRM_ON_CLOSE):
    """
    Kullanicinin sagladigi "Valid Highs & Lows (Structure Break)" Pine Script v6
    indikatorunun birebir Python portu.

    Mantik: bir swing yuksek (pivot high), fiyat daha sonra son onaylanmis pivot
    dusugun ALTINA kirilinca (use_close'a gore kapanisla ya da fitille) "valid"
    (gecerli) sayilir; o valid noktanin degeri, bir onceki valid noktadan bu yana
    ulasilan en yuksek high'tir (running extreme). Simetrik olarak bir swing dusuk
    de, fiyat son onaylanmis pivot yuksegin USTUNE kirilinca valid sayilir.

    mode: 1 = bir YUKSEGIN onaylanmasi bekleniyor, 2 = bir DUSUGUN onaylanmasi bekleniyor.
    Pine Script'te bu iki kontrol ELIF DEGIL, ardisik iki ayri "if" blogudur ve mode
    degiskenini paylasirlar; bu yuzden ayni barda ONCE yuksek onaylanip mode 2'ye
    gectikten hemen sonra (ayni bar icinde, run_low bu barin low'una resetlendigi icin)
    dusuk de onaylanabilir. Bu "ayni barda cift gecis" davranisi burada da birebir
    korunuyor (iki if de sirali calisir, elif kullanilmiyor).

    Donus: len(df) uzunlugunda, HER bar icin bir dict iceren liste:
        {"mode": 1|2, "vh": float|nan, "vl": float|nan,
         "vh_new": bool, "vl_new": bool,
         "vh_point_bar": int|None, "vl_point_bar": int|None,
         "run_high": float, "run_high_bar": int|None,
         "run_low": float, "run_low_bar": int|None}
    "vh_new"/"vl_new" True ise, o TAM O BARDA yeni bir Valid High/Low onaylandigi
    anlamina gelir (Pine'daki label.new/line.new'in tetiklendigi bar).
    """
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    n = len(df)

    ph = _pivot_confirmed_series(high, piv_bars, is_high=True)
    pl = _pivot_confirmed_series(low, piv_bars, is_high=False)

    last_ph = np.nan
    last_pl = np.nan

    run_high = np.nan
    run_high_bar = None
    run_low = np.nan
    run_low_bar = None

    vh = np.nan
    vl = np.nan
    mode = 1

    records = []

    for i in range(n):
        if not np.isnan(ph[i]):
            last_ph = ph[i]
        if not np.isnan(pl[i]):
            last_pl = pl[i]

        if np.isnan(run_high) or high[i] > run_high:
            run_high = high[i]
            run_high_bar = i
        if np.isnan(run_low) or low[i] < run_low:
            run_low = low[i]
            run_low_bar = i

        vh_new = False
        vl_new = False
        vh_point_bar = None
        vl_point_bar = None

        # Fiyat son onaylanmis dusugun altina kirdi -> mevcut running high VALID olur.
        if mode == 1 and not np.isnan(last_pl) and not np.isnan(run_high):
            broke = (close[i] < last_pl) if use_close else (low[i] < last_pl)
            if broke:
                vh = run_high
                vh_point_bar = run_high_bar
                mode = 2
                vh_new = True
                run_low = low[i]
                run_low_bar = i

        # Fiyat son onaylanmis yuksegin ustune kirdi -> mevcut running low VALID olur.
        # (elif DEGIL -- yukaridaki blok mode'u 2 yaptiysa bu blok da AYNI barda calisabilir)
        if mode == 2 and not np.isnan(last_ph) and not np.isnan(run_low):
            broke = (close[i] > last_ph) if use_close else (high[i] > last_ph)
            if broke:
                vl = run_low
                vl_point_bar = run_low_bar
                mode = 1
                vl_new = True
                run_high = high[i]
                run_high_bar = i

        records.append({
            "mode": mode,
            "vh": vh, "vl": vl,
            "vh_new": vh_new, "vl_new": vl_new,
            "vh_point_bar": vh_point_bar,
            "vl_point_bar": vl_point_bar,
            "run_high": run_high, "run_high_bar": run_high_bar,
            "run_low": run_low, "run_low_bar": run_low_bar,
        })

    return records


# ---------------------------------------------------------------------------
# Tek sembol degerlendirme
# ---------------------------------------------------------------------------
def evaluate_symbol(symbol):
    try:
        df_entry = get_klines(symbol, TIMEFRAME_ENTRY)
        # Giris ve onay zaman dilimi ayniysa (varsayilan: ikisi de 1h) ayni veriyi
        # iki kez cekmeye gerek yok -- gereksiz Binance API cagrisini onler.
        if TIMEFRAME_CONFIRM == TIMEFRAME_ENTRY:
            df_confirm = df_entry
        else:
            df_confirm = get_klines(symbol, TIMEFRAME_CONFIRM)
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

    # Kullanici TradingView'da Heikin Ashi kullaniyor -- Pine Script indikatorleri (Valid H/L,
    # UT Bot, LinReg Candle) TradingView'da HA grafigi uzerinde calistiginda `close`/`high`/`low`
    # degiskenleri HA degerlerine karsilik gelir. Ayni sonucu burada da elde etmek icin ham
    # Binance mumlarini once Heikin Ashi'ye ceviriyoruz ve TUM sinyal hesaplamalarini bu HA
    # seri uzerinden yapiyoruz -- boylece struct_level/H-L kirilimlari ve UT Bot/LinReg trendi
    # kullanicinin gordugu HA grafigiyle birebir eslesir.
    ha_entry = heikin_ashi(df_entry)
    ha_confirm = ha_entry if TIMEFRAME_CONFIRM == TIMEFRAME_ENTRY else heikin_ashi(df_confirm)

    # Arkaplanda tutulan eski gostergeler: artik sinyal kapisi degil, sadece bilgi amacli.
    signal, trend_entry = ut_bot_signal(ha_entry)
    lr_entry = linreg_trend(ha_entry)
    lr_confirm = linreg_trend(ha_confirm)

    # --- Ana tarama kriteri: Valid Highs & Lows (Structure Break) ---
    # Sadece SON barda yeni bir Valid High/Low onaylandiysa VE onaylanan ekstrem nokta
    # o onay barina gore en fazla VALID_HL_FRESHNESS_BARS bar once olustuysa "taze" sayilir.
    struct_records = detect_valid_high_low(ha_entry)
    last_struct = struct_records[-1]
    last_bar_idx = len(ha_entry) - 1

    struct_signal = None
    struct_point_bar = None
    struct_level = None
    # Sira, Pine Script'teki blok sirasiyla ayni (once H onayi, sonra L onayi) --
    # ayni barda cift gecis olursa (nadir), en son gerceklesen (L) kazanir.
    if (
        last_struct["vh_new"]
        and last_struct["vh_point_bar"] is not None
        and (last_bar_idx - last_struct["vh_point_bar"]) <= VALID_HL_FRESHNESS_BARS
    ):
        struct_signal = "sell"
        struct_point_bar = last_struct["vh_point_bar"]
        struct_level = float(last_struct["vh"])
    if (
        last_struct["vl_new"]
        and last_struct["vl_point_bar"] is not None
        and (last_bar_idx - last_struct["vl_point_bar"]) <= VALID_HL_FRESHNESS_BARS
    ):
        struct_signal = "buy"
        struct_point_bar = last_struct["vl_point_bar"]
        struct_level = float(last_struct["vl"])

    result = {
        "symbol": symbol,
        "signal": signal,
        "ut_trend_entry": trend_entry,
        "linreg_entry": lr_entry,
        "linreg_confirm": lr_confirm,
        "struct_signal": struct_signal,
        "struct_level": struct_level,
        "struct_point_bar": struct_point_bar,
        "struct_bars_ago": (last_bar_idx - struct_point_bar) if struct_point_bar is not None else None,
        "last_close": float(df_entry["close"].iloc[-1]),
        "bar_time_entry": int(df_entry["open_time"].iloc[-1]),
    }

    strong = None
    if struct_signal == "buy":
        strong = "GUCLU_AL"
    elif struct_signal == "sell":
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
                (sym, get_klines(sym, CHART_TIMEFRAME, limit=CHART_KLINES_LIMIT + CHART_STRUCT_WARMUP_BARS))
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
            "Kurulum basarili calisiyor. Gercek GUCLU AL sinyalleri geldikce burada bildirim alacaksin.",
            title="Crypto Scanner - Test Bildirimi",
            attach_url=test_attach_url,
        )
    elif [s for s in summary["strong_signals"] if s["strong_signal"] == "GUCLU_AL"]:
        # Tarama hem AL hem SAT guclu sinyallerini tespit edip gecmise kaydetmeye
        # devam eder; ancak kullanici sadece AL sinyalleri icin bildirim almak
        # istedigi icin burada SAT sinyalleri bildirimden filtrelenir.
        signals = [s for s in summary["strong_signals"] if s["strong_signal"] == "GUCLU_AL"]
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

            # Sinyal sayisi ne olursa olsun, tum sinyal coinlerinin 1 saatlik mum
            # grafigini tek bir gorselde (izgara halinde) birlestirip bildirime ekle
            attach_url = None
            try:
                symbol_dfs = [
                    (s["symbol"], get_klines(s["symbol"], CHART_TIMEFRAME, limit=CHART_KLINES_LIMIT + CHART_STRUCT_WARMUP_BARS))
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
                title=f"{len(signals)} Guclu AL Sinyal (1h Valid H/L Break)",
                priority="high",
                click_url=click_url,
                attach_url=attach_url,
            )
