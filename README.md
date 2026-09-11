# Crypto Scanner — UT Bot + LinReg Candle (15m/1h)

Binance USDT spot piyasasini tarayan, UT Bot (ATR trailing stop) ve LinReg
Candle indikatorlerini 15 dakikalik ve 1 saatlik zaman dilimlerinde
birlestiren bir sinyal tarayicisi. GitHub Actions uzerinde her 15 dakikada
bir otomatik calisir ve "GUCLU AL/SAT" sinyali bulundugunda
[ntfy.sh](https://ntfy.sh) uzerinden telefona push bildirim gonderir.

## Sinyal mantigi

Bir parite icin "GUCLU AL" sinyali su ucu ayni anda saglandiginda uretilir:

1. 15 dakikalik grafikte UT Bot buy sinyali (trailing stop crossover)
2. 15 dakikalik LinReg Candle trendi yesil (yukselis)
3. 1 saatlik LinReg Candle trendi de yesil (ust zaman dilimi onayi)

"GUCLU SAT" icin ucu de ters yonde (sell / kirmizi / kirmizi) olmali.

## Parametreler (scanner.py icinde degistirilebilir)

- `MIN_24H_QUOTE_VOLUME_USDT` = 500,000 — bu hacmin altindaki pariteler elenir
- `UT_KEY_VALUE` = 1.0, `UT_ATR_PERIOD` = 10 — UT Bot ayarlari
- `LINREG_LENGTH` = 11 — LinReg Candle uzunlugu

## Kurulum

1. Bu repo icindeki dosyalar zaten yuklu (scanner.py, .github/workflows/scan.yml)
2. Repo Settings > Secrets and variables > Actions altina `NTFY_TOPIC` adinda
   bir secret ekle (rastgele, tahmin edilemez bir metin — bu senin ozel bildirim kanalin)
3. Telefonuna [ntfy uygulamasini](https://ntfy.sh) kur, ayni topic adina abone ol
4. Actions sekmesinden workflow'u manuel calistirarak (`Run workflow`) test bildirimini dogrula

## Onemli not

Bu script yatirim tavsiyesi degildir, sinyal uretir ama kar garanti etmez.
Kripto piyasalarinda yalanci sinyal (whipsaw) riski yuksektir. Gercek parayla
kullanmadan once sinyalleri bir sure gozlemlemen onerilir.
# crypto-utbot-linreg-scanner
