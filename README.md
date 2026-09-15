# Crypto Scanner — UT Bot + LinReg Candle (1h)

Binance USDT spot piyasasini tarayan, UT Bot (ATR trailing stop) ve LinReg
Candle indikatorlerini 1 saatlik (1h) zaman diliminde birlestiren bir sinyal
tarayicisi. GitHub Actions uzerinde her 15 dakikada bir otomatik calisir ve
"GUCLU AL/SAT" sinyali bulundugunda [ntfy.sh](https://ntfy.sh) uzerinden
telefona push bildirim gonderir. Son taramalari, sinyal gecmisini ve isabet
istatistiklerini tarayicidan gormek icin bir [GitHub Pages
paneli](https://mehmetkural.github.io/crypto-utbot-linreg-scanner/) de mevcut.

## Sinyal mantigi

Bir parite icin "GUCLU AL" sinyali su ucu ayni anda saglandiginda uretilir:

1. 1 saatlik grafikte UT Bot buy sinyali (trailing stop crossover)
2. 1 saatlik LinReg Candle trendi yesil (yukselis)
3. 1 saatlik LinReg Candle trendi (onay) de yesil

"GUCLU SAT" icin ucu de ters yonde (sell / kirmizi / kirmizi) olmali.
Giris ve onay zaman dilimi su an ikisi de 1 saat (`TIMEFRAME_ENTRY` /
`TIMEFRAME_CONFIRM`, scanner.py icinde) — istenirse farkli zaman dilimlerine
ayarlanabilir.

## Panel (GitHub Pages)

Repo koku `index.html`, ayni klasordeki `latest_signals.json` (son tarama),
`signals_history.json` (gecmis sinyaller + isabet/kacirma durumu) ve
`latest_chart.png` dosyalarini okuyarak tarayicida canli bir panel gosterir.
30 saniyede bir otomatik yenilenir.

## Parametreler (scanner.py icinde degistirilebilir)

- `MIN_24H_QUOTE_VOLUME_USDT` = 500,000 — bu hacmin altindaki pariteler elenir
- `UT_KEY_VALUE` = 1.0, `UT_ATR_PERIOD` = 10 — UT Bot ayarlari
- `LINREG_LENGTH` = 7 — LinReg Candle uzunlugu
- `TIMEFRAME_ENTRY` / `TIMEFRAME_CONFIRM` = "1h" — sinyal arama / onay zaman dilimi
- `CHART_TIMEFRAME` = "1h" — bildirime ve panele eklenen grafigin zaman dilimi

## Kurulum

1. Bu repo icindeki dosyalari GitHub'a yukle (scanner.py, .github/workflows/scan.yml)
2. Repo Settings > Secrets and variables > Actions altina `NTFY_TOPIC` adinda
   bir secret ekle (rastgele, tahmin edilemez bir metin — bu senin ozel bildirim kanalin)
3. Telefonuna [ntfy uygulamasini](https://ntfy.sh) kur, ayni topic adina abone ol
4. Actions sekmesinden workflow'u manuel calistirarak (`Run workflow`) test bildirimini dogrula

## Onemli not

Bu script yatirim tavsiyesi degildir, sinyal uretir ama kar garanti etmez.
Kripto piyasalarinda yalanci sinyal (whipsaw) riski yuksektir. Gercek parayla
kullanmadan once sinyalleri bir sure gozlemlemen onerilir.
