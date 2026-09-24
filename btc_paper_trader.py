#!/usr/bin/env python3
"""
BTC Kagit Alim-Satim Simulasyonu (UT Bot Alerts, 100 EUR baslangic butcesi)
----------------------------------------------------------------------------
Bu, ana taramadan (scanner.py) ve Memo taramasindan (memo_scanner.py) TAMAMEN
BAGIMSIZ, ucuncu bir "sanki gercekten trade yapiyormus gibi" takip scripti'dir.
Gercek para/emir YOK -- sadece BTCUSDT'nin 4 saatlik grafiginde, ana taramada
zaten kullanilan AYNI UT Bot Alerts (ATR Trailing Stop) indikatoru "buy"
dedigi anda 100 EUR'luk (basitlestirme: 1 birim = 1 USDT, EUR/USD kuru
uygulanmaz) hayali butcenin TAMAMIYLA BTC alindigini, "sell" dedigi anda
o BTC'nin TAMAMIYLA satilip tekrar nakde donuldugunu varsayip defter tutar.

Ana taramadan farki:
  - scanner.py'nin GUCLU AL/SAT sinyali coklu kriter + coklu sembol taramasidir;
    burada TEK sembol (BTCUSDT) ve TEK kriter (UT Bot Alerts) var, sinyal
    dogrudan al/sat EMRI olarak yorumlanir (GUCLU AL/SAT esiginden BAGIMSIZ).
  - Bildirim (ntfy) GONDERMEZ -- sadece GitHub Pages panelinde ayri bir
    sekmede (bkz. index.html "btcPanel") takip edilmesi icin JSON ciktisi
    (btc_paper_trade.json) uretir.
  - scan.yml icinde ana taramayla AYNI adimda (her 15 dakikada bir) calisir --
    "buy"/"sell" sinyali (bkz. scanner.ut_bot_signal) zaten sadece kesisimin
    TAM O ANKI barda gerceklestigi taramada donduruldugu icin (bir onceki/son
    iki bar karsilastirmasi degismedikce ayni sonuc tekrar tekrar gelir), asil
    tekrarli islem korumasi pozisyon durumundan (position: "flat"/"long")
    gelir: "buy" sinyali sadece pozisyon "flat" iken islenir, "sell" sinyali
    sadece pozisyon "long" iken islenir -- ayni sinyal 4 saatlik pencere
    boyunca onlarca kez tekrar gelse bile ikinci bir alim/satim tetiklenmez.

Kullanim:
    python3 btc_paper_trader.py

Cikti dosyasi: btc_paper_trade.json (bu script ile ayni klasorde)
Durum dosyasi: btc_paper_trade_state.json (cash/BTC bakiyesi + islem gecmisi,
    repo'ya commit'lenip calisma boyunca kalici olur -- her calismada okunup
    guncellenir, sifirdan baslamaz).
"""

import sys
import json
import datetime

import scanner  # ana tarama scripti -- SADECE ortak yardimci fonksiyonlar/sabitler
                 # icin import edilir (get_klines, heikin_ashi, ut_bot_signal,
                 # commit_and_push_files, log, vb.). scanner.py'nin kendi tarama
                 # akisi `if __name__ == "__main__":` icinde oldugundan bu import
                 # ana taramayi TETIKLEMEZ ve ona hicbir sekilde dokunmaz.

# ---------------------------------------------------------------------------
# Ayarlar (bu simulasyona ozel, ana taramadan bagimsiz)
# ---------------------------------------------------------------------------
BTC_SYMBOL = "BTCUSDT"
BTC_TIMEFRAME = "4h"                      # kendi sabit ayari -- ana taramanin (scanner.py)
                                           # zaman dilimi degisikliklerinden BAGIMSIZ, kasitli olarak
                                           # ayri tutuluyor (bkz. modul basi aciklama)
BTC_KLINES_LIMIT = 150                    # UT Bot ATR'i icin yeterli warmup gecmisi
BTC_INITIAL_BUDGET_EUR = 100.0            # baslangic butcesi (basitlestirme: 1 birim = 1 USDT)
BTC_TRADE_MAX_TRADES = 500                # islem gecmisinde tutulan en fazla kayit (guvenlik siniri)

BTC_TRADE_STATE_FILENAME = "btc_paper_trade_state.json"
BTC_TRADE_OUTPUT_FILENAME = "btc_paper_trade.json"


def log(msg):
    scanner.log(f"[BTC-PAPER] {msg}")


def btc_trade_state_path():
    return __file__.rsplit("/", 1)[0] + "/" + BTC_TRADE_STATE_FILENAME


def btc_trade_output_path():
    return __file__.rsplit("/", 1)[0] + "/" + BTC_TRADE_OUTPUT_FILENAME


def _initial_state():
    return {
        "initial_budget_eur": BTC_INITIAL_BUDGET_EUR,
        "position": "flat",          # "flat" | "long"
        "cash_eur": BTC_INITIAL_BUDGET_EUR,
        "btc_amount": 0.0,
        "entry_price": None,
        "entry_at_utc": None,
        "trades": [],
    }


def load_btc_trade_state():
    """btc_paper_trade_state.json'i okur. Dosya yoksa/bozuksa 100 EUR nakit,
    pozisyon 'flat' ile sifirdan baslar (ilk calisma davranisi)."""
    try:
        with open(btc_trade_state_path()) as f:
            state = json.load(f)
            if isinstance(state, dict) and "position" in state:
                return state
    except Exception:
        pass
    return _initial_state()


def save_btc_trade_state(state):
    with open(btc_trade_state_path(), "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Simulasyon
# ---------------------------------------------------------------------------
def run_btc_paper_trade(verbose=True):
    df = scanner.get_klines(BTC_SYMBOL, BTC_TIMEFRAME, limit=BTC_KLINES_LIMIT)
    ha = scanner.heikin_ashi(df)
    signal, trend = scanner.ut_bot_signal(ha)

    # Islemler HER ZAMAN gercek (ham) kapanis fiyatindan yapilir -- sinyal HA
    # mumlarindan hesaplansa da, "sanki gercekten alip sattik" simulasyonunun
    # gercek piyasada uygulanabilir bir fiyat kullanmasi icin (ayni mantik
    # scanner.py'deki result["last_close"] icin de gecerli, bkz. proje notlari).
    price = float(df["close"].iloc[-1])
    bar_time = int(df["open_time"].iloc[-1])
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_iso = now_utc.isoformat(timespec="seconds")

    state = load_btc_trade_state()
    action = None

    if signal == "buy" and state["position"] == "flat":
        btc_amount = state["cash_eur"] / price
        state["trades"].append({
            "type": "buy",
            "time_utc": now_iso,
            "bar_time": bar_time,
            "price": price,
            "btc_amount": btc_amount,
            "cash_used_eur": state["cash_eur"],
        })
        state["btc_amount"] = btc_amount
        state["cash_eur"] = 0.0
        state["position"] = "long"
        state["entry_price"] = price
        state["entry_at_utc"] = now_iso
        action = "buy"
        if verbose:
            log(f"AL: {btc_amount:.8f} BTC @ {price} (butce: {state['trades'][-1]['cash_used_eur']:.2f} EUR)")

    elif signal == "sell" and state["position"] == "long":
        cash_eur = state["btc_amount"] * price
        cash_used = state["trades"][-1]["cash_used_eur"] if state["trades"] else BTC_INITIAL_BUDGET_EUR
        pnl_eur = cash_eur - cash_used
        pnl_pct = ((price - state["entry_price"]) / state["entry_price"] * 100) if state["entry_price"] else 0.0
        state["trades"].append({
            "type": "sell",
            "time_utc": now_iso,
            "bar_time": bar_time,
            "price": price,
            "btc_amount": state["btc_amount"],
            "cash_after_eur": cash_eur,
            "pnl_eur": pnl_eur,
            "pnl_pct": pnl_pct,
        })
        state["cash_eur"] = cash_eur
        state["btc_amount"] = 0.0
        state["position"] = "flat"
        state["entry_price"] = None
        state["entry_at_utc"] = None
        action = "sell"
        if verbose:
            log(f"SAT: {price} -> {cash_eur:.2f} EUR (kazanc: {pnl_pct:+.2f}%)")

    # Guvenlik siniri: islem gecmisi cok uzamasin diye en eski kayitlar budanir
    # (en yeni islemler korunur).
    if len(state["trades"]) > BTC_TRADE_MAX_TRADES:
        state["trades"] = state["trades"][-BTC_TRADE_MAX_TRADES:]

    save_btc_trade_state(state)

    # Islem olsun olmasin HER calismada guncel (mark-to-market) equity hesaplanir --
    # boylece panelde "su an ne kadar deger" surekli taze kalir, sadece alim/satim
    # anlarinda degil.
    equity_eur = state["cash_eur"] if state["position"] == "flat" else state["btc_amount"] * price
    total_return_pct = (equity_eur - state["initial_budget_eur"]) / state["initial_budget_eur"] * 100

    summary = {
        "updated_at_utc": now_iso,
        "symbol": BTC_SYMBOL,
        "timeframe": BTC_TIMEFRAME,
        "initial_budget_eur": state["initial_budget_eur"],
        "position": state["position"],
        "cash_eur": state["cash_eur"],
        "btc_amount": state["btc_amount"],
        "entry_price": state["entry_price"],
        "entry_at_utc": state["entry_at_utc"],
        "current_price": price,
        "trend": trend,
        "equity_eur": equity_eur,
        "total_return_pct": total_return_pct,
        "last_action": action,
        "trade_count": len(state["trades"]),
        "trades": state["trades"],
    }

    if verbose:
        log(f"Guncel durum: pozisyon={state['position']}, fiyat={price}, "
            f"equity={equity_eur:.2f} EUR ({total_return_pct:+.2f}%)")

    return summary


if __name__ == "__main__":
    json_only = "--json-only" in sys.argv
    summary = run_btc_paper_trade(verbose=not json_only)

    out_path = btc_trade_output_path()
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)

    if json_only:
        print(json.dumps(summary))
    else:
        log(f"Sonuclar kaydedildi: {out_path}")

    # Panelde (index.html "BTC Kagit Al-Sat" sekmesi) her zaman guncel veri
    # gorunsun diye, islem olsun olmasin her calismada hem durum hem cikti
    # dosyasi commit'lenip push'lanir -- ana taramanin ya da memo taramasinin
    # dosyalarina HICBIR sekilde dokunmaz.
    commit_msg = "BTC kagit alim-satim guncellendi"
    if summary["last_action"]:
        commit_msg += f" ({summary['last_action'].upper()})"
    if not scanner.commit_and_push_files(
        [BTC_TRADE_OUTPUT_FILENAME, BTC_TRADE_STATE_FILENAME],
        commit_msg,
    ):
        log("btc_paper_trade.json / btc_paper_trade_state.json push edilemedi.")
