# Amazon Product Recommendation Using Spark

Bu depo, SNAP Amazon ürün meta verisini yerel Apache Spark üzerinde işleyen,
zamansal olarak ayrılmış öneri adayları üreten ve beş bağımsız model ile iki
önceden tanımlı hibriti karşılaştıran BİL401 dönem projesidir.

## Veri kaynağı ve yorum sayılarının anlamı

Kaynak dosya varsayılan olarak `Dataset/amazon-meta.txt` yolundadır. Bu sürümün
kimliği şöyledir:

- boyut: `977.506.331` bayt;
- fiziksel satır: `15.010.574`;
- SHA-256: `600135116a05b7ce2dcb7e842e892d663c6190a0567d00373e0c5c4f3c908f02`;
- kodlama ve satır sonu: UTF-8, CRLF;
- kayıt ayırıcısı: boş satırın bayt gösterimi olan `0d0a0d0a`.

Kaynak özet alanlarındaki yorum toplamı `7.781.990`, gerçekten indirilen ve
fiziksel alt kayıt olarak bulunan yorum sayısı `7.593.244`'tür. Bu iki sayı aynı
metriği ifade etmez. `reviews_raw` fiziksel yorumları korur;
`reviews_deduplicated` tam anahtar tekilleştirmesinden sonraki tablo,
`user_item_interactions` ise kullanıcı-ürün-gün birleşiminden sonraki modelleme
tablosudur. Resmî sayımlar G4 ve G5 manifestolarından okunmalıdır.

## Kilitli çalışma ortamı

Doğrulanan çalışma yığını:

| Bileşen | Sürüm / yol |
|---|---|
| Python | `3.13.1`, pyenv ortamı `bil401_env_1` |
| Java | OpenJDK `21.0.11`, `/usr/lib/jvm/java-21-openjdk-amd64` |
| Spark | `4.0.0` |
| Scala | `2.13.16` |
| GraphFrames | `0.12.1` |
| PyArrow | `25.0.0` |
| DuckDB | `1.5.4` |
| Streamlit | `1.59.1` |

Java 23 veya sistem Python'u ile karışık çalıştırma desteklenmez. Kurulum:

```bash
cd Amazon-Product-Recommendation-Using-Spark
pyenv install -s 3.13.1
pyenv prefix bil401_env_1 >/dev/null 2>&1 || \
  pyenv virtualenv 3.13.1 bil401_env_1
export PYENV_VERSION=bil401_env_1
export PY=$(pyenv which python)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
test "$(java -XshowSettings:properties -version 2>&1 | \
  sed -n 's/^ *java.version = //p')" = '21.0.11'
"$PY" -m pip install -r requirements.lock -e .
```

Ubuntu üzerinde Java 21 yoksa dağıtımın OpenJDK 21 paketi kurulmalı; kurulum
preflight'i ve G0/G12 kabulü tam `java.version=21.0.11` ile
`JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64` kilidini doğrular.

GraphFrames JVM JAR'ları `.cache/ivy/jars/` altında bulunmalıdır. Çevrim içi ilk
kurulumda kilitli Maven koordinatı ve geçişli bağımlılıkları Spark ile indirip
yerel proje önbelleğine kopyalayın:

```bash
mkdir -p .cache/ivy/jars
PYSPARK_PYTHON="$PY" PYSPARK_DRIVER_PYTHON="$PY" \
PYSPARK_SUBMIT_ARGS='--packages io.graphframes:graphframes-spark4_2.13:0.12.1 pyspark-shell' \
"$PY" -c 'from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().stop()'
find "$HOME/.ivy2/jars" -maxdepth 1 -type f \
  \( -name 'io.graphframes_*.jar' -o -name 'org.apache.datasketches_*.jar' \) \
  -exec cp -n {} .cache/ivy/jars/ \;
```

Çevrim dışı çalışmada aynı kilitli JAR dosyaları doğrudan bu dizine konur.
Kullanılan JAR'ların SHA-256 değerleri G0 manifestosunda saklanır.
`bin/amazon-rec` önce isteğe bağlı `AMAZON_REC_PYTHON` yolunu, yoksa
`PYENV_VERSION=bil401_env_1 pyenv which python` sonucunu kullanır. Bulduğu gerçek
yorumlayıcı yolunun `bil401_env_1` öneki altında ve tam Python 3.13.1 olduğunu
doğrular; hem sürücü hem Spark işçisine aynı yolu verir ve eksik JAR durumunda
başlamaz.

## Donanım ve disk beklentisi

G0 ortam kanıtı ile başarılı G4–G6 tam-veri kapıları; 12 mantıksal çekirdek,
yaklaşık 16 GiB fiziksel RAM ve 8 GiB Spark sürücü heap'i olan yerel makinede
çalıştırılmıştır. Büyük shuffle aşamalarında tarayıcı, Docker sanal makinesi veya
başka JVM'ler bellek baskısı yaratabilir.

Kaynak yaklaşık 1 GiB'dir; geliştirme sırasındaki bir G7 ara-çıktı ölçümünde
kanonik ve geçici artefaktlar birlikte yaklaşık 5 GiB düzeyine ulaşmıştır. Ölçüm,
model ve atomik geçici yayınlar için en az 20 GiB boş disk önerilir. Bu 20 GiB bir
kapasite payıdır; tamamlanmış bir koşumun gerçek tablo boyutları ilgili
manifestolardaki `size_bytes` alanlarından hesaplanmalıdır.

## Faz kapıları ve güvenli çalışma sırası

G0–G12 sıralıdır. Bir geçit başarısızsa sonraki geçit çalıştırılmaz. G4'ten
itibaren tam veri zorunludur; eksik veriyle başarılı tam koşum veya metrik
üretilmez.

Önce test kanıtı oluşturun:

```bash
export SOURCE8=$(sha256sum Dataset/amazon-meta.txt | cut -c1-8)
export RUN_ID="run-$(date -u +%Y%m%dT%H%M%SZ)-$SOURCE8"
export PYENV_VERSION=bil401_env_1
export PY=$(pyenv which python)
mkdir -p "artifacts/runs/$RUN_ID/test-results"
make test RUN_ID="$RUN_ID" \
  TEST_JUNIT="artifacts/runs/$RUN_ID/test-results/preflight.xml" \
  TEST_SHARD_DIR="artifacts/runs/$RUN_ID/test-results/shards"
export JUNIT="artifacts/runs/$RUN_ID/test-results/preflight.xml"
```

`make test`, bellek birikmesini önlemek için testleri ardışık ve ayrı
Python/Spark JVM süreçlerinde çalıştırır, aktif bir proje Spark koşumu varsa
başlamaz ve shard JUnit dosyalarını tek kanıtta birleştirir. Bütün testleri tek
uzun JVM'de doğrudan `pytest` ile çalıştırmak önerilmez.

G0, Spark/GraphFrames ortam kanıtıdır. Yerel JAR listesi Spark classpath'ine
verilerek ayrı çalıştırılır:

```bash
export JARS=$(find .cache/ivy/jars -type f -name '*.jar' | sort | paste -sd, -)
export PYSPARK_SUBMIT_ARGS="--master local[2] --driver-memory 2g \
  --conf spark.ui.enabled=false --jars $JARS pyspark-shell"
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
PYSPARK_PYTHON="$PY" PYSPARK_DRIVER_PYTHON="$PY" \
"$PY" scripts/g0_smoke.py \
  --output "artifacts/runs/$RUN_ID/manifests/G0.json"
```

Sonraki kapılar aynı koşum kimliği ve geçen JUnit kanıtıyla sırayla yürütülür:

```bash
./bin/amazon-rec --run-id "$RUN_ID" gate G1  --evidence-file "$JUNIT"
./bin/amazon-rec --run-id "$RUN_ID" gate G2  --evidence-file "$JUNIT" # parser
./bin/amazon-rec --run-id "$RUN_ID" gate G3  --evidence-file "$JUNIT" # smoke ETL
./bin/amazon-rec --run-id "$RUN_ID" gate G4  --evidence-file "$JUNIT" # full ETL
./bin/amazon-rec --run-id "$RUN_ID" gate G5  --evidence-file "$JUNIT" # cleaning
./bin/amazon-rec --run-id "$RUN_ID" gate G6  --evidence-file "$JUNIT" # split
./bin/amazon-rec --run-id "$RUN_ID" gate G7  --evidence-file "$JUNIT" # train
./bin/amazon-rec --run-id "$RUN_ID" gate G8  --evidence-file "$JUNIT" # hybrids
./bin/amazon-rec --run-id "$RUN_ID" gate G9  --evidence-file "$JUNIT" # evaluate
./bin/amazon-rec --run-id "$RUN_ID" gate G10 --evidence-file "$JUNIT" # UI data
./bin/amazon-rec --run-id "$RUN_ID" gate G11 --evidence-file "$JUNIT" # performance
./bin/amazon-rec --run-id "$RUN_ID" gate G12 --evidence-file "$JUNIT" # delivery
```

Durum denetimi:

```bash
./bin/amazon-rec --run-id "$RUN_ID" status
```

Streamlit yalnız G10 başarılı olduktan sonra, önceden üretilmiş Gold Parquet
tablolarını DuckDB ile salt okunur biçimde sorgular. Sayfa açılışı Spark oturumu
başlatmaz:

```bash
STREAMLIT_SERVER_HEADLESS=true \
STREAMLIT_SERVER_ADDRESS=127.0.0.1 \
STREAMLIT_SERVER_PORT=8501 \
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
./bin/amazon-rec --run-id "$RUN_ID" dashboard
```

Bu komut `http://127.0.0.1:8501` adresindeki uzun ömürlü Streamlit sunucusunu
**foreground** olarak çalıştırır. Sunucuyu ayrı bir
terminal/PTY oturumunda açık bırakın ve yalnız işiniz bittiğinde aynı oturumda
`Ctrl-C` ile kapatın. Geniş kapsamlı `pkill` kullanmayın; eğitim veya başka
kullanıcı süreçlerini yanlışlıkla sonlandırabilir.

## Model özeti ve değiştirilemez deney bütçesi

Beş bağımsız liste yalnız eğitim bölümünden bir kez üretilir:

- popülerlik: `m=20` Bayes skoru, grup geri dönüşü ve 100 aday;
- açık ALS: rank 20, regParam 0,10, 10 iterasyon, seed 42, ham 200 ve filtreli
  100 aday;
- FP-Growth: `max(0,001; 200/B)` destek, güven 0,05, lift en az 1,10 ve 50
  aday;
- ürün grafı: en fazla 20 olumlu tohum, doğrudan/karşılıklı/iki-adım ağırlıkları
  `1,0/0,25/0,50`, PageRank yalnız eşitlik bozucu ve 50 aday;
- kategori: kosinüs/grup/popülerlik ağırlıkları `0,80/0,10/0,10`, en fazla
  5.000 aday havuzu ve 50 sonuç.

RRF sabiti `c=60`'tır. Yalnız şu iki hibrit vardır; üçüncü bir ağırlık deneyi
yoktur:

| Hibrit | ALS | Graf | Kategori | FP | Popülerlik |
|---|---:|---:|---:|---:|---:|
| H-A | 0,35 | 0,20 | 0,20 | 0,15 | 0,10 |
| H-B | 0,50 | 0,20 | 0,10 | 0,15 | 0,05 |

Kazanan, yalnız validation/common-warm/overall NDCG@10 ile seçilir. Mutlak fark
0,001'den küçükse aynı kohortun kullanıcı kapsamı, o da eşitse H-A kullanılır.
Seçim test sonuçları hesaplanmadan önce dondurulur. Test tablosunda beş bağımsız
model ve yalnız seçilen hibrit resmîdir; kaybeden hibrit testte değerlendirilmez.

## Sonuçları yeniden üretme ve teslim paketi

Çalıştırılmamış değerler README'ye elle yazılmaz. Resmî metriklerin tek kaynağı:

- `artifacts/runs/$RUN_ID/data/g9/official_test_comparison/`;
- seçim kanıtı için `data/g9/selected_hybrid/` ve
  `_selection_frozen_before_test.json`;
- yerel paralellik deneyi için `performance/summary.json` ve sekiz denemenin
  Spark olay günlükleri.

Kanonik manifest yolları
`artifacts/runs/$RUN_ID/manifests/G0.json`–`G12.json`; seçim freeze kanıtı
`artifacts/runs/$RUN_ID/data/g9/_selection_frozen_before_test.json`; performans
özeti ise `artifacts/runs/$RUN_ID/performance/summary.json` yolundadır.

G12 bu girdilerin fingerprintlerini yeniden hesaplar, bütün G0–G11 manifest
zincirini doğrular ve aşağıdaki atomik paketi üretir:

```text
artifacts/runs/$RUN_ID/delivery/
├── README.md
├── final-results.md
├── official-test-comparison.csv
├── acceptance-report.json
├── artifact-inventory.json
├── manifest-index.json
├── source-identity.json
├── test-summary.json
├── manifests/
└── test-results/
```

G12 handler'ı teslimi önce manifest-finalizasyonu bekleyen durumda yayımlar.
CLI kanonik `manifests/G12.json` dosyasını atomik yazdıktan sonra aynı manifesti
teslim paketine kopyalar ve `_SUCCESS.json` işaretini en son oluşturur. Kesinti
iki adımın arasında olursa aynı G12 komutu mevcut kanonik manifestten
finalizasyonu tamamlar. Metrik tablosu ile Markdown gösterimi çelişirse
fingerprinti doğrulanmış Parquet ve G9 manifestosu esastır.

## Bilinen sınırlamalar

- Veri 2006 tarihli Amazon meta verisidir; güncel ürün kataloğu veya çevrim içi
  kullanıcı davranışı değildir.
- Common-warm, modellerin çıktı verip vermemesine bakılarak sonradan seçilmez;
  ALS eğitim evreni ve train geçmişiyle önceden tanımlanır.
- ALS RMSE/MAE yalnız ham, kırpılmamış ve cold-start sonrası tahmini bulunan
  puanlara aittir; diğer modeller için bu metrikler tanımlı değildir.
- G11 yatay ölçekleme deneyi değildir. Aynı iş yükünde `local[1]` ile
  `local[min(4, mantıksal çekirdek)]` yerel paralelliğini karşılaştırır; koşul
  başına bir ısınma ve üç ölçümün medyanını raporlar.

