#!/usr/bin/env python3
"""
YATAY FILTRELI UT BOT Taramasi - 2 saatlik, bagimsiz tarama
------------------------------------------------------------
Ana taramadan (scanner.py), Memo taramasindan (memo_scanner.py) ve BTC kagit
alim-satimdan (btc_paper_trader.py) TAMAMEN BAGIMSIZ, ayri bir GitHub Actions
workflow'u (sideways.yml) tarafindan HER 2 SAATTE BIR calistirilan tarama.

Fikir: UT Bot Alerts, yatay (sideways / range) piyasada fiyat trailing stop'un
ustune/altina surekli gecip durdugu icin en cok yanlis sinyal ureten gostergelerden
biridir. Bu tarama 2 saatlik UT Bot AL sinyallerini bulur, ardindan coinin o an
yatayda olup olmadigini kontrol eder:
  - Yatayda DEGILSE  -> "gecen" sinyal, ntfy bildirimi gonderilir.
  - Yataydaysa       -> "elenen" sinyal, bildirim GONDERILMEZ (panelde yine gorunur).
  - Yataydaydi ama sinyal bari kanalin ustunde kapattiysa -> KIRILIM, sinyal gecer.
Olculer sinyal barindan ONCEKI barlarda hesaplanir (bkz. sideways_metrics).

Yatay tespiti (HAM mumlar uzerinden -- piyasanin gercek durumunu olcer), 3 olcuden
EN AZ 2'si saglanirsa coin "yatay" sayilir (bkz. SIDEWAYS_MIN_VOTES):
  1. ADX(14) < 20                         -> trend gucu zayif
  2. Choppiness Index(14) > 61.8          -> fiyat yonsuz/dalgali
  3. Bollinger bant genisligi (20, 2), son 100 barin en dar %20'lik diliminde -> sikisma

UT Bot sinyali, ana taramayla ayni sekilde Heikin Ashi mumlari uzerinden hesaplanir
(kullanici TradingView'da HA kullaniyor). Fiyatlar her zaman ham (gercek) kapanistir.

scanner.py sadece ortak yardimci fonksiyonlar icin import edilir; import etmek ana
taramayi tetiklemez. Bu tarama kendi dosyalarina yazar:
  sideways_latest_signals.json, sideways_signals_history.json,
  sideways_notify_state.json, sideways_chart.png
"""

import os
import sys
import json
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

import scanner  # SADECE ortak yardimci fonksiyonlar icin (get_klines, heikin_ashi,
                 # ut_bot_signal, wilder_atr, send_ntfy, commit_and_push_files, ...)

# ---------------------------------------------------------------------------
# Ayarlar (bu taramaya ozel, diger taramalardan bagimsiz)
# ---------------------------------------------------------------------------
SW_TIMEFRAME = "2h"
SW_KLINES_LIMIT = 200                 # ADX/BB yuzdelik warmup'i icin yeterli gecmis

SW_ADX_PERIOD = 14
SW_ADX_THRESHOLD = 20.0               # ADX bunun altindaysa trend zayif
SW_CHOP_PERIOD = 14
SW_CHOP_THRESHOLD = 61.8              # CHOP bunun ustundeyse piyasa yonsuz
SW_BB_LENGTH = 20
SW_BB_MULT = 2.0
SW_BB_LOOKBACK = 100                  # bant genisligi yuzdeligi bu kadar bar icinde olculur
SW_BB_SQUEEZE_PCTL = 20.0             # son 100 barin en dar %20'si -> sikisma
SW_RANGE_BARS = 30                    # bilgi amacli: son 30 barin kanal genisligi %
SW_MIN_VOTES = 2                      # 3 olcuden en az kaci saglanirsa "yatay"

SW_LATEST_FILENAME = "sideways_latest_signals.json"
SW_HISTORY_FILENAME = "sideways_signals_history.json"
SW_STATE_FILENAME = "sideways_notify_state.json"
SW_CHART_FILENAME = "sideways_chart.png"
SW_HISTORY_MAX_ENTRIES = 300


def log(msg):
    scanner.log(f"[YATAY] {msg}")


def _path(name):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), name)


def _load_json(name, default):
    try:
        with open(_path(name)) as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(name, data):
    with open(_path(name), "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Yatay piyasa gostergeleri
# ---------------------------------------------------------------------------
def _wilder_smooth(values, period):
    """Wilder (RMA) yumusatmasi; ilk "period" deger basit ortalamayla baslatilir."""
    out = np.full(len(values), np.nan)
    if len(values) < period:
        return out
    out[period - 1] = np.nanmean(values[:period])
    for i in range(period, len(values)):
        out[i] = (out[i - 1] * (period - 1) + values[i]) / period
    return out


def adx_series(df, period=SW_ADX_PERIOD):
    high, low, close = df["high"].values, df["low"].values, df["close"].values
    up = np.diff(high, prepend=high[0])
    down = -np.diff(low, prepend=low[0])
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    # ilk bar anlamsiz (onceki bar yok), yumusatmaya 1. bardan basla
    atr = _wilder_smooth(tr[1:], period)
    pdi = 100 * _wilder_smooth(plus_dm[1:], period) / atr
    mdi = 100 * _wilder_smooth(minus_dm[1:], period) / atr
    with np.errstate(divide="ignore", invalid="ignore"):
        dx = 100 * np.abs(pdi - mdi) / (pdi + mdi)
    dx = np.nan_to_num(dx, nan=0.0)
    valid = ~np.isnan(atr)
    adx = np.full(len(dx), np.nan)
    first = np.argmax(valid)
    if valid.any():
        adx[first:] = _wilder_smooth(dx[first:], period)
    return np.concatenate([[np.nan], adx])


def choppiness_series(df, period=SW_CHOP_PERIOD):
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    tr_sum = tr.rolling(period).sum()
    rng = high.rolling(period).max() - low.rolling(period).min()
    chop = 100 * np.log10(tr_sum / rng) / np.log10(period)
    return chop.values


def bb_width_series(df, length=SW_BB_LENGTH, mult=SW_BB_MULT):
    close = df["close"]
    ma = close.rolling(length).mean()
    sd = close.rolling(length).std(ddof=0)
    return ((2 * mult * sd) / ma).values


def sideways_metrics(df_full):
    """
    Yatay piyasa olculerini ve karari dondurur.
    ONEMLI: Olculer sinyal barinin kendisi DAHIL EDILMEDEN (bir onceki bara kadar) hesaplanir.
    Sebep: UT Bot sinyali genelde ortalamadan buyuk bir mumla tetiklenir; o mum dahil edilirse
    oynaklik birden artmis gorunur ve yatay coinler filtreden kacar (sentetik testte goruldu).
    Istisna -- KIRILIM: sinyal barinin kapanisi, onceki SW_RANGE_BARS barin en yuksek
    seviyesinin UZERINDEYSE bu bir yatay kanal kirilimidir, filtre uygulanmaz (sinyal gecer).
    """
    df = df_full.iloc[:-1]
    adx = adx_series(df)[-1]
    chop = choppiness_series(df)[-1]
    bbw = bb_width_series(df)
    window = bbw[-SW_BB_LOOKBACK:]
    window = window[~np.isnan(window)]
    bbw_last = bbw[-1]
    bbw_pctl = float((window <= bbw_last).mean() * 100) if len(window) and not np.isnan(bbw_last) else None

    recent = df.iloc[-SW_RANGE_BARS:]
    range_pct = float((recent["high"].max() - recent["low"].min()) / recent["close"].iloc[-1] * 100)

    votes = {
        "adx_low": bool(not np.isnan(adx) and adx < SW_ADX_THRESHOLD),
        "chop_high": bool(not np.isnan(chop) and chop > SW_CHOP_THRESHOLD),
        "bb_squeeze": bool(bbw_pctl is not None and bbw_pctl <= SW_BB_SQUEEZE_PCTL),
    }
    vote_count = sum(votes.values())
    range_high = float(recent["high"].max())
    breakout = bool(float(df_full["close"].iloc[-1]) > range_high)
    return {
        "adx": None if np.isnan(adx) else round(float(adx), 2),
        "chop": None if np.isnan(chop) else round(float(chop), 2),
        "bb_width_pctl": None if bbw_pctl is None else round(bbw_pctl, 1),
        "range_pct": round(range_pct, 2),
        "votes": votes,
        "vote_count": vote_count,
        "range_high": range_high,
        "breakout": breakout,
        "was_sideways": vote_count >= SW_MIN_VOTES,   # sinyalden onceki durum
        "is_sideways": vote_count >= SW_MIN_VOTES and not breakout,  # nihai karar
    }


# ---------------------------------------------------------------------------
# Tek sembol degerlendirme
# ---------------------------------------------------------------------------
def evaluate_sideways_symbol(symbol):
    try:
        df = scanner.get_klines(symbol, SW_TIMEFRAME, limit=SW_KLINES_LIMIT)
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}
    if len(df) < SW_BB_LENGTH + SW_ADX_PERIOD * 2 + 5:
        return {"symbol": symbol, "error": "yetersiz veri"}

    ha = scanner.heikin_ashi(df)
    signal, trend = scanner.ut_bot_signal(ha)

    res = {
        "symbol": symbol,
        "ut_signal": signal,
        "ut_trend": trend,
        "last_close": float(df["close"].iloc[-1]),
        "bar_time": int(df["open_time"].iloc[-1]),
    }
    # Yatay olculeri sadece AL sinyali olan semboller icin gerekli (gereksiz hesaplama yok)
    if signal == "buy":
        res.update(sideways_metrics(df))
    return res


# ---------------------------------------------------------------------------
# Ana tarama
# ---------------------------------------------------------------------------
def run_sideways_scan(min_volume=scanner.MIN_24H_QUOTE_VOLUME_USDT, verbose=True):
    if verbose:
        log("Binance USDT pariteleri cekiliyor...")
    symbols = scanner.get_usdt_symbols(min_volume)
    if verbose:
        log(f"{len(symbols)} parite taraniyor ({SW_TIMEFRAME})...")

    results, errors = [], []
    with ThreadPoolExecutor(max_workers=scanner.MAX_WORKERS) as ex:
        futures = {ex.submit(evaluate_sideways_symbol, s): s for s in symbols}
        for fut in as_completed(futures):
            r = fut.result()
            (errors if "error" in r else results).append(r)

    buys = [r for r in results if r["ut_signal"] == "buy"]
    passed = sorted([r for r in buys if not r["is_sideways"]], key=lambda r: r["symbol"])
    filtered = sorted([r for r in buys if r["is_sideways"]], key=lambda r: r["symbol"])
    price_lookup = {r["symbol"]: r["last_close"] for r in results}

    summary = {
        "scanned_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "timeframe": SW_TIMEFRAME,
        "symbols_scanned": len(results),
        "symbols_errored": len(errors),
        "ut_buy_count": len(buys),
        "passed_count": len(passed),
        "filtered_count": len(filtered),
        "passed_signals": passed,
        "filtered_signals": filtered,
        "criteria": {
            "adx_below": SW_ADX_THRESHOLD,
            "chop_above": SW_CHOP_THRESHOLD,
            "bb_squeeze_pctl": SW_BB_SQUEEZE_PCTL,
            "min_votes": SW_MIN_VOTES,
        },
    }
    if verbose:
        log(f"Bitti: {len(results)} sembol, {len(errors)} hata, {len(buys)} UT Bot AL "
            f"-> {len(passed)} gecti, {len(filtered)} yatay oldugu icin elendi.")
    return summary, price_lookup


# ---------------------------------------------------------------------------
# Gecmis (hem gecen hem elenen sinyaller -- filtrenin ise yarayip yaramadigi
# panelde karsilastirilabilsin diye ikisi de fiyat takibine alinir)
# ---------------------------------------------------------------------------
def update_history(history, summary, price_lookup, now_utc):
    now_iso = now_utc.isoformat(timespec="seconds")
    keys = {(h["symbol"], h.get("bar_time")) for h in history}
    for status, items in (("passed", summary["passed_signals"]), ("filtered", summary["filtered_signals"])):
        for s in items:
            if (s["symbol"], s["bar_time"]) in keys:
                continue
            history.append({
                "symbol": s["symbol"],
                "bar_time": s["bar_time"],
                "status": status,
                "detected_at_utc": now_iso,
                "price_at_signal": s["last_close"],
                "last_price": s["last_close"],
                "last_price_updated_at": now_iso,
                "adx": s.get("adx"),
                "chop": s.get("chop"),
                "bb_width_pctl": s.get("bb_width_pctl"),
                "vote_count": s.get("vote_count"),
                "was_sideways": s.get("was_sideways"),
                "breakout": s.get("breakout"),
            })
            keys.add((s["symbol"], s["bar_time"]))
    for h in history:
        p = price_lookup.get(h["symbol"])
        if p is not None:
            h["last_price"] = p
            h["last_price_updated_at"] = now_iso
    history.sort(key=lambda h: h["detected_at_utc"], reverse=True)
    return history[:SW_HISTORY_MAX_ENTRIES]


def _fmt_line(s):
    tag = "  [YATAYDAN KIRILIM]" if s.get("breakout") and s.get("was_sideways") else ""
    return (f"{s['symbol']} @ {s['last_close']}{tag}\n"
            f"ADX {s['adx']} | CHOP {s['chop']} | BB %{s['bb_width_pctl']}\n"
            f"{scanner.tradingview_url(s['symbol'])}")


if __name__ == "__main__":
    json_only = "--json-only" in sys.argv
    summary, price_lookup = run_sideways_scan(verbose=not json_only)
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    _save_json(SW_LATEST_FILENAME, summary)
    if json_only:
        print(json.dumps(summary))

    history = update_history(_load_json(SW_HISTORY_FILENAME, []), summary, price_lookup, now_utc)
    _save_json(SW_HISTORY_FILENAME, history)

    if not scanner.commit_and_push_files(
        [SW_LATEST_FILENAME, SW_HISTORY_FILENAME],
        "Yatay filtreli UT Bot tarama sonuclari guncellendi",
    ):
        log("Panel verileri push edilemedi.")

    if os.environ.get("FORCE_TEST_NOTIFY"):
        scanner.send_ntfy(
            "Yatay filtreli UT Bot taramasi kurulumu basarili. 2 saatlik UT Bot AL sinyallerinden "
            "yatayda OLMAYAN coinler geldikce burada bildirim alacaksin.",
            title="Yatay Filtre - Test Bildirimi",
        )
        sys.exit(0)

    state = _load_json(SW_STATE_FILENAME, {})
    to_notify = [s for s in summary["passed_signals"]
                 if state.get(s["symbol"], {}).get("last_notified_bar_time") != s["bar_time"]]
    if not to_notify:
        log("Bildirilecek yeni (yatayda olmayan) AL sinyali yok.")
        sys.exit(0)

    for s in to_notify:
        state[s["symbol"]] = {"last_notified_bar_time": s["bar_time"],
                              "last_notified_at": now_utc.isoformat(timespec="seconds")}
    _save_json(SW_STATE_FILENAME, state)

    attach_url = None
    files = [SW_STATE_FILENAME]
    try:
        symbol_dfs = [
            (s["symbol"], scanner.get_klines(s["symbol"], SW_TIMEFRAME,
                                             limit=scanner.CHART_KLINES_LIMIT + scanner.CHART_STRUCT_WARMUP_BARS))
            for s in to_notify[:9]  # grafik okunabilir kalsin diye en fazla 9 coin
        ]
        scanner.generate_signals_chart(symbol_dfs, _path(SW_CHART_FILENAME), SW_TIMEFRAME)
        files.append(SW_CHART_FILENAME)
    except Exception as e:
        log(f"Grafik olusturulamadi, bildirim gorselsiz gidecek: {e}")
    pushed = scanner.commit_and_push_files(files, "Yatay filtre bildirim grafigi/durumu guncellendi")
    if pushed and SW_CHART_FILENAME in files:
        attach_url = f"{scanner.GITHUB_REPO_RAW_BASE}/{SW_CHART_FILENAME}"

    if scanner.notifications_enabled():
        scanner.send_ntfy(
            "\n\n".join(_fmt_line(s) for s in to_notify)
            + f"\n\n({summary['filtered_count']} AL sinyali yatay oldugu icin elendi)",
            title=f"{len(to_notify)} Filtreli AL Sinyali (2h UT Bot, yatay degil)",
            priority="high",
            click_url=scanner.tradingview_url(to_notify[0]["symbol"]) if len(to_notify) == 1 else None,
            attach_url=attach_url,
        )
    else:
        log("Bildirimler panel togglesiyle KAPALI, bildirim gonderilmiyor.")
