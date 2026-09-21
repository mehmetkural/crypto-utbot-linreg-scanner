#!/usr/bin/env python3
"""
MEMO Taramasi (MACD sifir ustu kesisim + Hacim MA20 ustu) - 4 saatlik, bagimsiz tarama
----------------------------------------------------------------------------------------
Bu, ana "Valid Highs & Lows" taramasindan (scanner.py) TAMAMEN BAGIMSIZ, ayri bir
GitHub Actions workflow'u (memo.yml) tarafindan HER 4 SAATTE BIR calistirilan ikinci
bir taramadir. scanner.py'deki Binance/yardimci fonksiyonlari (get_klines, macd_series,
send_ntfy, vb.) import ederek yeniden kullanir, ama:
  - KENDI durum dosyasina yazar (memo_notify_state.json) -- ana taramanin notify_state.json
    dosyasina veya 2 saatlik bekleme suresine HICBIR sekilde dokunmaz.
  - KENDI JSON ciktisina yazar (memo_latest_signals.json) -- latest_signals.json'a dokunmaz.
  - KENDI ntfy basligiyla bildirim gonderir, ana taramanin sinyallerinden tamamen ayridir.
  - scanner.py import edilirken hicbir tarama calismaz (ana script'in tum tarama mantigi
    `if __name__ == "__main__":` icinde oldugu icin import etmek yan etkisizdir).

Kriter (MEMO sinyali, 4 saatlik mumlarda, SON KAPANAN bar icin):
  1. MACD cizgisi SIFIRIN UZERINDE VE sifira YAKIN bir seviyede (bkz. MEMO_MACD_NEAR_ZERO_PCT)
     VE sinyal cizgisini bir onceki bara gore YUKARI dogru YENI kesmis olmali (bullish crossover).
  2. Son mumun hacmi, MEMO_VOLUME_MA_LENGTH (20) periyotluk hacim hareketli ortalamasinin
     UZERINDE olmali.

Kullanim:
    python3 memo_scanner.py                 # taramayi calistir, sonucu yazdir ve JSON'a kaydet
    python3 memo_scanner.py --json-only      # sadece JSON ciktisi (stdout'a), log basma

Cikti dosyasi: memo_latest_signals.json (bu script ile ayni klasorde)
"""

import os
import sys
import json
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

import scanner  # ana tarama scripti -- SADECE ortak yardimci fonksiyonlar/sabitler icin
                 # import edilir (get_klines, get_usdt_symbols, macd_series, send_ntfy, vb.).
                 # scanner.py'nin kendi tarama akisi `if __name__ == "__main__":` icinde
                 # oldugundan bu import ana taramayi TETIKLEMEZ ve ona hicbir sekilde
                 # dokunmaz.

# ---------------------------------------------------------------------------
# Ayarlar (MEMO taramasina ozel, ana taramadan bagimsiz)
# ---------------------------------------------------------------------------
MEMO_TIMEFRAME = "4h"                     # bu tarama 4 saatlik grafikte calisir
MEMO_KLINES_LIMIT = 150                   # MACD + hacim MA20 icin yeterli warmup gecmisi
MEMO_VOLUME_MA_LENGTH = 20                # hacim hareketli ortalama periyodu (4h mum bazinda)
MEMO_MACD_NEAR_ZERO_PCT = 0.004           # |MACD| <= kapanis fiyatinin bu orani ise "sifira yakin" sayilir
                                           # (coin fiyat olceklerine gore normalize edilmis esik; ayarlanabilir)

MEMO_STATE_FILENAME = "memo_notify_state.json"

# GitHub Pages paneli icin memo sinyal gecmisi ayarlari (ana taramanin signals_history.json
# mekanizmasindan bagimsiz, kendi dosyasina yazar). Ana taramadan farkli olarak burada sabit
# bir degerlendirme ufku (isabet/kacirma) YOK -- kullanici talebiyle sadece "bildirim geldigi
# fiyat" ile "son fiyat" surekli karsilastirilir (bkz. update_memo_history_prices): bir sinyal
# MACD kesisiminden sonra guncel memo_signals listesinden dusse bile (ör. bar artik "yeni
# kesisim" olmadigi icin), gecmisteki kaydi fiyati izlenmeye devam eder.
MEMO_HISTORY_FILENAME = "memo_signals_history.json"
MEMO_HISTORY_MAX_ENTRIES = 200            # gecmiste tutulan en fazla memo sinyali sayisi


def log(msg):
    scanner.log(f"[MEMO] {msg}")


def memo_state_path():
    return __file__.rsplit("/", 1)[0] + "/" + MEMO_STATE_FILENAME


def load_memo_state():
    """Sembol basina en son bildirim gonderilen bar zamanini okur (ayni bar icin tekrar
    bildirim gonderilmesini onlemek icin). Yoksa bos dict doner."""
    try:
        with open(memo_state_path()) as f:
            return json.load(f)
    except Exception:
        return {}


def save_memo_state(state):
    with open(memo_state_path(), "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# GitHub Pages paneli icin memo sinyal gecmisi (memo_signals_history.json)
# ---------------------------------------------------------------------------
def memo_history_path():
    return __file__.rsplit("/", 1)[0] + "/" + MEMO_HISTORY_FILENAME


def load_memo_history():
    """memo_signals_history.json'daki gecmis MEMO sinyal kayitlarini okur. Yoksa bos liste doner."""
    try:
        with open(memo_history_path()) as f:
            return json.load(f)
    except Exception:
        return []


def save_memo_history(history):
    """Memo sinyal gecmisini memo_signals_history.json'a yazar (repo'ya commitlenip GitHub Pages panelinden okunur)."""
    with open(memo_history_path(), "w") as f:
        json.dump(history, f, indent=2)


def update_memo_history_with_new_signals(history, memo_signals, now_utc):
    """Yeni MEMO sinyallerini gecmise ekler (ayni sembol + ayni bar tekrar eklenmez)."""
    existing_keys = {(h["symbol"], h.get("bar_time")) for h in history}
    for s in memo_signals:
        key = (s["symbol"], s["bar_time"])
        if key in existing_keys:
            continue
        history.append({
            "symbol": s["symbol"],
            "bar_time": s["bar_time"],
            "detected_at_utc": now_utc.isoformat(timespec="seconds"),
            "price_at_signal": s["last_close"],
            "last_price": s["last_close"],
            "last_price_updated_at": now_utc.isoformat(timespec="seconds"),
        })
        existing_keys.add(key)
    # En yeni kayit en basta olacak sekilde sirala, listeyi MEMO_HISTORY_MAX_ENTRIES ile sinirla
    history.sort(key=lambda h: h["detected_at_utc"], reverse=True)
    return history[:MEMO_HISTORY_MAX_ENTRIES]


def update_memo_history_prices(history, price_lookup, now_utc):
    """
    Gecmisteki HER kayit icin (MEMO sinyali artik guncel listede olmasa bile -- ör. OPUSDT
    gibi bir kesisim gecmiste kalmis olsa da), sembol bu taramada tekrar cekildiyse
    (run_memo_scan'in bu calismadaki sonuclarindan, EKSTRA API cagrisi yapmadan) son fiyatini
    gunceller. Boylece panelde "bildirim geldigi fiyat" ile "son fiyat" surekli izlenebilir.
    """
    now_iso = now_utc.isoformat(timespec="seconds")
    for h in history:
        current_price = price_lookup.get(h["symbol"])
        if current_price is None:
            continue  # bu sembol bu taramada yok (hacim filtresi disina dustu vb.), sonraki taramada tekrar denenir
        h["last_price"] = current_price
        h["last_price_updated_at"] = now_iso
    return history


# ---------------------------------------------------------------------------
# Tek sembol degerlendirme
# ---------------------------------------------------------------------------
def evaluate_memo_symbol(symbol):
    try:
        df = scanner.get_klines(symbol, MEMO_TIMEFRAME, limit=MEMO_KLINES_LIMIT)
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

    if len(df) < MEMO_VOLUME_MA_LENGTH + 2:
        return {"symbol": symbol, "error": "yetersiz veri"}

    macd_line, signal_line, _hist = scanner.macd_series(df)
    close = df["close"].values
    volume = df["volume"].values
    vol_ma = pd.Series(volume).rolling(MEMO_VOLUME_MA_LENGTH).mean().values

    last, prev = len(df) - 1, len(df) - 2

    macd_last, macd_prev = macd_line[last], macd_line[prev]
    signal_last, signal_prev = signal_line[last], signal_line[prev]
    vol_ma_last = vol_ma[last]

    crossed_up = macd_prev <= signal_prev and macd_last > signal_last
    above_zero = macd_last > 0
    near_zero = abs(macd_last) <= close[last] * MEMO_MACD_NEAR_ZERO_PCT
    volume_above_ma = (not np.isnan(vol_ma_last)) and volume[last] > vol_ma_last

    memo_signal = bool(crossed_up and above_zero and near_zero and volume_above_ma)

    return {
        "symbol": symbol,
        "memo_signal": memo_signal,
        "macd": float(macd_last),
        "macd_signal": float(signal_last),
        "crossed_up": bool(crossed_up),
        "above_zero": bool(above_zero),
        "near_zero": bool(near_zero),
        "volume": float(volume[last]),
        "volume_ma": float(vol_ma_last) if not np.isnan(vol_ma_last) else None,
        "volume_above_ma": bool(volume_above_ma),
        "last_close": float(close[last]),
        "bar_time": int(df["open_time"].iloc[-1]),
    }


# ---------------------------------------------------------------------------
# Ana tarama
# ---------------------------------------------------------------------------
def run_memo_scan(min_volume=scanner.MIN_24H_QUOTE_VOLUME_USDT, verbose=True):
    if verbose:
        log("Binance USDT pariteleri cekiliyor...")
    symbols = scanner.get_usdt_symbols(min_volume)
    if verbose:
        log(f"{len(symbols)} parite hacim filtresini gecti, taraniyor (4h)...")

    results = []
    errors = []
    with ThreadPoolExecutor(max_workers=scanner.MAX_WORKERS) as ex:
        futures = {ex.submit(evaluate_memo_symbol, s): s for s in symbols}
        for fut in as_completed(futures):
            res = fut.result()
            if "error" in res:
                errors.append(res)
            else:
                results.append(res)

    memo_signals = [r for r in results if r["memo_signal"]]

    # Sadece aktif memo sinyalleri degil, bu taramada basariyla degerlendirilen TUM
    # sembollerin son fiyati -- gecmis kayitlarin (memo_signals_history.json) "son fiyat"
    # alanini, ekstra API cagrisi yapmadan guncellemek icin kullanilir (bkz. __main__ ve
    # update_memo_history_prices). Bu, memo_latest_signals.json ciktisina YAZILMAZ.
    price_lookup = {r["symbol"]: r["last_close"] for r in results}

    summary = {
        "scanned_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "timeframe": MEMO_TIMEFRAME,
        "symbols_scanned": len(results),
        "symbols_errored": len(errors),
        "memo_signal_count": len(memo_signals),
        "memo_signals": memo_signals,
    }

    if verbose:
        log(f"Tarama bitti: {len(results)} sembol basarili, {len(errors)} hata, {len(memo_signals)} memo sinyali.")
        for s in memo_signals:
            log(f"  MEMO: {s['symbol']} @ {s['last_close']} (MACD={s['macd']:.6f})")

    return summary, price_lookup


if __name__ == "__main__":
    json_only = "--json-only" in sys.argv
    summary, price_lookup = run_memo_scan(verbose=not json_only)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    out_path = __file__.rsplit("/", 1)[0] + "/memo_latest_signals.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)
    if json_only:
        print(json.dumps(summary))
    else:
        log(f"Sonuclar kaydedildi: {out_path}")

    # GitHub Pages paneli icin memo sinyal gecmisi (memo_signals_history.json): yeni
    # sinyalleri gecmise ekler VE gecmisteki TUM kayitlarin (artik aktif sinyal olmasalar
    # bile) son fiyatini bu taramanin sonuclariyla gunceller -- boylece "bildirim geldigi
    # fiyat" ile "son fiyat" panelde surekli takip edilebilir (ör. bir sinyal MACD
    # kesisiminden sonra guncel listeden dusse bile fiyati izlenmeye devam eder).
    memo_history = load_memo_history()
    memo_history = update_memo_history_with_new_signals(memo_history, summary["memo_signals"], now_utc)
    memo_history = update_memo_history_prices(memo_history, price_lookup, now_utc)
    save_memo_history(memo_history)

    # Panelde (index.html "Memo" sekmesi) her zaman guncel veri gorunsun diye, bildirim
    # gonderilsin ya da gonderilmesin, her calismada memo_latest_signals.json VE
    # memo_signals_history.json commit'lenir -- ana taramanin latest_signals.json /
    # signals_history.json dosyalarina dokunmaz.
    if not scanner.commit_and_push_files(
        ["memo_latest_signals.json", MEMO_HISTORY_FILENAME],
        "Memo tarama sonuclari guncellendi",
    ):
        log("memo_latest_signals.json / memo_signals_history.json push edilemedi.")

    if os.environ.get("FORCE_TEST_NOTIFY"):
        scanner.send_ntfy(
            "Memo taramasi kurulumu basarili calisiyor. MACD (sifir ustu, sifira yakin, "
            "yukari kesisim) + hacim MA20 ustu kriterine uyan sinyaller geldikce burada "
            "bildirim alacaksin. (4 saatlik grafik, ana taramadan bagimsiz)",
            title="Memo Taramasi - Test Bildirimi",
        )
    elif summary["memo_signals"]:
        state = load_memo_state()

        # Ayni bar icin tekrar bildirim gonderilmesini onle (zaman bazli cooldown yerine
        # bar bazli dedup -- her calisma zaten yeni bir 4h barini degerlendirdigi icin
        # yapay bir bekleme suresine gerek yok, sadece ayni barin tekrarini engellemek yeterli).
        signals_to_notify = [
            s for s in summary["memo_signals"]
            if state.get(s["symbol"], {}).get("last_notified_bar_time") != s["bar_time"]
        ]

        if signals_to_notify:
            lines = [
                f"{s['symbol']} @ {s['last_close']}\n"
                f"MACD: {s['macd']:.6f}  |  Sinyal: {s['macd_signal']:.6f}\n"
                f"{scanner.tradingview_url(s['symbol'])}"
                for s in signals_to_notify
            ]
            for s in signals_to_notify:
                state[s["symbol"]] = {
                    "last_notified_bar_time": s["bar_time"],
                    "last_notified_at": now_utc.isoformat(timespec="seconds"),
                }
            save_memo_state(state)
            scanner.commit_and_push_files(
                [MEMO_STATE_FILENAME],
                "Memo tarama bildirim durumu guncellendi",
            )

            click_url = scanner.tradingview_url(signals_to_notify[0]["symbol"]) if len(signals_to_notify) == 1 else None
            if scanner.notifications_enabled():
                scanner.send_ntfy(
                    "\n\n".join(lines),
                    title=f"{len(signals_to_notify)} MEMO Sinyali (4h MACD+Hacim)",
                    priority="high",
                    click_url=click_url,
                )
            else:
                log("Bildirimler panel togglesiyle KAPALI, MEMO bildirimi gonderilmiyor "
                    "(gecmis/panel verileri yine de guncellendi).")
        else:
            log("Sinyal var ama hepsi bu bar icin daha once bildirildi, bildirim atlaniyor.")
    else:
        log("Bu taramada MEMO sinyali yok.")
