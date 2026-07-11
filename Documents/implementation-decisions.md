# Uygulama Karar Kaydı

Bu belge, `project-implementation-details.md` içindeki serbest bırakılmış mimari
kararları ve uygulama sırasında açıkça çözülen normatif belirsizlikleri kaydeder.
Bağlayıcı teori, veri sözleşmesi, split ve deney bütçesinin yerine geçmez.

## ADR-001 — Kaynak ortalama puan yuvarlaması

- **Tarih:** 2026-07-11
- **Durum:** Kabul edildi
- **Karar:** Fiziksel yorumlardan yeniden hesaplanan ortalama, en yakın `0,5`
  değerine `ROUND_HALF_UP` ile yuvarlanarak kaynak `avg rating` alanıyla
  karşılaştırılır.
- **Tam veri regresyonu:** Tam indirilen uygun 533.938 kayıt içinde 487
  uyuşmazlık.
- **Gerekçe:** Kaynak özet değerlerinin tamamı 0,5 ızgarasındadır. Tam kaynak
  taramasında en yakın 0,5 kuralı 487; bir ondalık HALF_UP kuralı 174.276
  uyuşmazlık üretmiştir. Kullanıcı, planlama sırasında `0,5 ve 487` seçeneğini
  bağlayıcı yorum olarak onaylamıştır.
- **Etkisi:** Şartnamedeki aynı 487 sayısını “bir ondalık” ile ilişkilendiren
  ifade uygulanmaz. Bu tek açıklama dışında matematiksel parametre değişikliği
  yapılmaz.

## ADR-002 — Hibrit seçimi doğrulama kohortu

- **Tarih:** 2026-07-11
- **Durum:** Kabul edildi
- **Karar:** H-A/H-B kazananı ortak sıcak başlangıç validation kohortunun
  NDCG@10 değeriyle seçilir. Mutlak fark 0,001'den küçükse aynı kohortun
  UserCoverage değeri; o da eşitse H-A tie-break'i uygulanır.
- **Etkisi:** Operasyonel kohort sonuçları eksiksiz raporlanır fakat hibrit
  ağırlık seçimini belirlemez.

## ADR-003 — Yerel çalışma yığını

- **Tarih:** 2026-07-11
- **Durum:** G0 ile doğrulandı
- **Karar:** `bil401_env_1` Python 3.13.1, Java 21.0.11, Spark 4.0.0,
  Scala 2.13.16 ve GraphFrames 0.12.1 kullanılır. Java 23 kullanılmaz.
- **Kanıt:** G0 Snappy Parquet, PageRank, WCC ve kalıcı checkpoint testleri
  `artifacts/runs/run-20260711T030500Z-60013511/manifests/G0.json` içinde
  `passed` durumundadır.

## ADR-004 — Kararlı hash

- **Durum:** Kabul edildi
- **Kohort hash'i:** UTF-8 `customer_id + U+001F + "42"` metninin SHA-256
  hex değeri; artan hash ve ardından artan `customer_id`.
- **G3 ürün örneği:** Aynı biçimde ürün kimliği ve seed üzerinden SHA-256'nın
  ilk 16 bitinin 328'den küçük olması; zorunlu veri sınıfı temsilcileri union
  edilerek eklenirse neden alanı saklanır.

## ADR-005 — Bayesçi örneğin aritmetik düzeltmesi

- **Tarih:** 2026-07-11
- **Durum:** Formül lehine çözüldü
- **Karar:** Şartnamedeki `m=20` ve
  `WR=(v/(v+20))R+(20/(v+20))C` aynen korunur. `C=4`, `R=5`, `v=10`
  girdisinin test sonucu `4,3333333333` olur.
- **Gerekçe:** Bölüm 19.3'te yazan `4,1667`, aynı formül ve `m=20` ile
  üretilemez; bu değer `m=50` gerektirir ve bağlayıcı model parametresini
  değiştirirdi. Bu nedenle test örneği aritmetik yazım hatasıdır; model
  parametresi veya formül değiştirilmemiştir.

## ADR-006 — Fiziksel satıra taşmış 10 başlık

- **Tarih:** 2026-07-11
- **Durum:** Tam G4 taramasında doğrulandı
- **Karar:** `title:` satırından sonra `group:` satırına kadar görülen girintisiz
  fiziksel satırlar başlık devamı kabul edilir ve aradaki gerçek satır sonu `\n`
  olarak korunur. Girintili, tanınmayan alan bu kurala girmez ve karantinaya
  gider.
- **Kanıt:** İlk tam envelope alımında tam 10 kayıt
  `missing_or_reordered_group` olarak durdu. Ham blokların doğrudan
  incelenmesinde hepsinin başlığın fiziksel satıra taşması olduğu görüldü;
  örnek ürün kimlikleri `2658`, `14253`, `397289` ve `518614`.
- **Etkisi:** Kayıtlar atılmaz veya başlık metni kesilmez; ürün yapısı ve alan
  sırası `group:` sonrasında değişmeden uygulanır. Bu kayıtlar
  `multiline_title` kalite olayı üretir.

## ADR-007 — G4 kesinti kurtarma ve disk önbelleği

- **Tarih:** 2026-07-11
- **Durum:** Kabul edildi
- **Karar:** Tam kaynak geçişinin tamamlanmış ingestion envelope'u ve `_SUCCESS`
  içeren atomik tablo yayınları G4 yeniden başlatmalarında korunur. Yalnız
  `.<tablo>.<uuid>.tmp` ara dizinleri temizlenir. Büyük ve tekrar kullanılan
  G4 ara DataFrame'leri `DISK_ONLY` ile tutulur ve son tüketicisinden hemen
  sonra `unpersist` edilir. `master=local[*]` korunurken her Spark görevi dört
  CPU slotu ister; bu makinede aynı anda üç görev çalışır. Unified memory
  fraction `0,35`, storage fraction `0,30` seçilerek shuffle daha erken diske
  taşınır.
- **Gerekçe:** Kullanıcı terminali kapandığında tam kaynak ayrıştırması bitmiş,
  ilk kalıcı tablo yazımı yarım kalmıştı. Aynı anda makine swap alanı doluydu;
  1 GiB test JVM'i 129 MiB sınır kaydı üzerinde OOM üretti. Disk önbelleği
  matematiksel davranışı, veri sözleşmesini, split'i veya deney bütçesini
  değiştirmeden heap baskısını sınırlar.
- **Kanıt:** Resume birim testleri tamamlanan envelope ve tabloların byte/mtime
  değerlerini değiştirmeden yeniden kullanıldığını ve yalnız yarım atomik
  dizinlerin temizlendiğini doğrular. G4 manifestosu yeniden kullanılan
  checkpointleri ve seçilen storage level'ı kaydeder.
- **OOM kanıtı:** İlk resume denemesinde kernel günlüğü `java` PID 524832'yi
  5.558.772 KiB anon RSS düzeyinde global OOM ile öldürdüğünü kaydetti. Bu
  nedenle kaynak eşzamanlılığı sınırlaması varsayımsal değil, gözlenen makine
  baskısına dayalı koşullu bir teknik sapmadır.

## ADR-008 — G7 yerel bellek profili ve WCC yürütücüsü

- **Tarih:** 2026-07-11
- **Durum:** Kabul edildi
- **Karar:** Beş modelin matematiksel parametreleri değiştirilmeden G7 ağır
  graf/kategori bölümleri `master=local[*]` altında tek eşzamanlı Spark görevi
  (`spark.task.cpus=12`) ile yürütülür. Kategori aday puanlamasında unified
  memory fraction `0,20` kullanılarak daha erken disk spill tercih edilir.
  Tam iç katalog zayıf bağlı bileşen hesabı GraphFrames API'sinin `graphx`
  yürütücüsüyle yapılır; yönlü kenarların zayıf bileşen semantiği değişmez.
- **Gerekçe:** İki eşzamanlı görev temiz JVM'de yaklaşık 5,03 GiB RSS'ye çıkıp
  kullanılabilir sistem belleğini 775 MiB'ye düşürdü. Tek görevli kategori
  reduce/sort geçişi `0,35` memory fraction ile yaklaşık 4,61 GiB RSS'ye
  ulaştı; `0,20` ayarı aynı planı daha fazla disk spill ile güvenli baş boşlukta
  tutar. Bunlar model eşiği, aday derinliği, skor veya deney bütçesi değildir.
- **WCC kanıtı:** `connectedComponents(algorithm="two_phase")` tam
  548.552-düğümlü graf üzerinde `Py4JError` ile başarısız oldu. Aynı GraphFrames
  çağrısının `graphx` yürütücüsü iç PageRank/derece/karşılıklılık hesaplarından
  bağımsız ve checkpoint'li olarak tamamlandı; böylece bir yapısal algoritma
  hatası diğer atomik graf çıktılarını geçersiz kılmaz.
- **Kesinti kurtarma:** Popülerlik, ALS, FP, graf ve kategori çıktıları ayrı
  `_SUCCESS` sözleşmeleriyle yayımlanır. ALS/FP model dizinlerinde Spark'ın
  `metadata/_SUCCESS` işareti esas alınır. Aynı kaynak/yapılandırma imzasında
  tamamlanan model veya tablo yeniden eğitilmez ve yeniden yazılmaz.

## ADR-009 — RRF kayan nokta toplama determinismi

- **Tarih:** 2026-07-11
- **Durum:** Tam G8 doğrulamasında çözüldü
- **Bulgu:** İlk tam H-A yayını, ortak aday tablosundan bağımsız yeniden
  hesaplanan top-100 sözleşmesinde `446` eksik ve `446` fazla sıra satırı
  üretti. Çıktı skorunu aynı satırın kanonik model sıralarından yeniden
  hesaplama kontrolünde yalnız `582 / 4.672.900` satır birebir bit düzeyinde
  farklıydı; azami mutlak fark `3,469446951953614e-18` ve `1e-15` üstü fark
  sayısı sıfırdı.
- **Kök neden:** Dağıtık `groupBy().sum(double)` aynı RRF katkılarını bölümleme
  sırasına göre farklı sırada topluyordu. Matematiksel eşitliklerde bir ULP'den
  küçük fark bile top-100 sınırındaki sıra eşitliğini Bayes/product_id
  zincirinden önce bozabiliyordu.
- **Karar:** RRF skoru, alfabetik olarak kararlı `model_ranks` haritası üzerinde
  tek deterministik `aggregate` ifadesiyle hesaplanır. Model katkı haritası da
  aynı kanonik sıradan türetilir. H-A ve H-B, beş kaynak listesinin ayrı
  lineage'larını yeniden birleştirmek yerine aynı atomik `hybrid_candidates`
  tablosundan puanlanır.
- **Etkisi:** `c=60`, H-A/H-B ağırlıkları, kullanıcı düzeyi ağırlık
  normalizasyonu, aday derinlikleri ve eşitlik bozma zinciri değişmez; yalnız
  cebirsel olarak aynı toplamın yürütüm sırası sabitlenir. Başarısız ilk G8
  denemesi koşumun `manifests/attempts/` dizininde korunur.

## ADR-010 — Değerlendirme hedefi grup doğrulamasının kapsamı

- **Tarih:** 2026-07-11
- **Durum:** Tam G9 koşumunda çözüldü
- **Bulgu:** İlk G9 validation materializasyonu, `every evaluation target must
  have one catalog product group` kontrolünde durdu. Tam DuckDB uzlaştırması
  `80.000` değerlendirme satırındaki `23.356` tekil hedefin tamamının katalogda
  bulunduğunu ve hiçbirinin null/boş gruba sahip olmadığını gösterdi. Katalogda
  grup değeri olmayan kayıt sayısı ise kaynakla uyumlu olarak tam `5.868`
  durdurulmuş üründü.
- **Kök neden:** Grup geçerlilik ifadesi held-out hedeflerle join edilmeden önce
  bütün ürün tablosuna uygulanıyordu. Spark planı `raise_error` ifadesini
  yalnız join sonucunda gereken hedeflerle sınırlamadan değerlendirdiği için,
  değerlendirme evrenine giremeyen meşru durdurulmuş kayıtlar da hatayı
  tetikledi.
- **Karar:** Ürün-grup tekillik özeti önce hedeflerle left join edilir;
  null/eksik/çelişkili grup kontrolü yalnız gerçek değerlendirme hedefi
  satırlarında uygulanır. Hedef dışı katalog ürünü dilim sınıflandırmasını
  engellemez; gerçek bir hedefte grup yoksa geçit yine fail-closed durur.
- **Etkisi:** Split, kohort, metrik paydaları ve Book/non-Book tanımı değişmez.
  İlgisiz null grubu kabul eden ve null-gruplu gerçek hedefi reddeden iki yönlü
  regresyon testi eklendi; başarısız ilk G9 denemesi attempts dizininde korunur.

## ADR-011 — G10 çevrimdışı DuckDB Gold derleme belleği

- **Tarih:** 2026-07-11
- **Durum:** Tam G10 koşumunda çözüldü
- **Bulgu:** İlk gerçek G10 koşumu, altı küçük toplulaştırmayı yayımladıktan
  sonra `product_search_index` sorgusunda 384 MB DuckDB sınırının
  `365,9 / 366,2 MiB` kullanımında 4 KiB ek blok ayıramamasıyla fail-closed
  durdu. Bu sorgu 2.509.699 kategori yolunu ürün düzeyinde sıralı liste ve
  arama metnine toplar. Tek iş parçacıklı 1 GB ve 2 GB denemeleri de sırasıyla
  `950,7 / 953,6 MiB` ve `1,8 / 1,8 GiB` sınırlarında durarak, aynı yaklaşık
  245,9 milyon kaynak karakterini eşzamanlı `list` ile `string_agg` durumlarında
  tutan planın bellek artışıyla güvenilir biçimde çözülmediğini gösterdi.
- **Karar:** Yalnız çevrimdışı G10 Gold derleme bağlantısı tek iş parçacığı ve
  sabit 2 GB bellek sınırıyla çalışır; mevcut disk spill dizini korunur.
  Kategori yolları yalnız bir kez kanonik `path_ordinal` sırasıyla listeye
  toplanır; aynı içerikli arama metni dış projeksiyonda bu listeden türetilir.
  Streamlit sunum bağlantısının iki iş parçacıklı 384 MB sınırı değişmez,
  çünkü arayüz yalnız önceden oluşturulmuş kompakt tabloları ve sınırlandırılmış
  sorguları okur.
- **Etkisi:** Alanlar, satırlar, sıralama, veri sözleşmesi, split, model
  matematiği ve deney bütçesi değişmez. Değişiklik yalnız gözlenen çevrimdışı
  hash/list aggregate bellek ihtiyacına verilen operasyonel kaynak ayarıdır;
  ilk başarısız deneme `manifests/attempts/` altında korunur.
