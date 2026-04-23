# kelompok-8-ets-bigdata

## Keanggotaan Kelompok 08
| NRP | Nama | Kontribusi |
| --- | --- | --- |
| 5027231007 | Billy | | 
| 5027241026 | Evan | | 
| 5027241039 | Rayka  | |
| 5027241044 | Akbar | |

## Timeline
| Hari | Target |
| --- | --- |
| 1–2 |	Daftar topik ke dosen, setup Docker (Hadoop + Kafka), buat 2 topic Kafka |
| 3–4 |	producer_api.py berjalan — event masuk ke Kafka |
| 5–6 |	producer_rss.py berjalan — artikel RSS masuk ke Kafka |
| 7 |	Checkpoint Minggu 1: Kafka menerima dari kedua sumber, verifikasi via console consumer |
| 8–9 |	consumer_to_hdfs.py berjalan — file JSON muncul di HDFS |
| 10–11 |	Spark analysis.ipynb — 3 analisis berjalan dari HDFS |
| 12 |	dashboard/app.py — dashboard menampilkan data nyata |
| 13 |	Testing end-to-end, perbaikan, finalisasi README |
| 14 |	Deadline: Submit link GitHub ke LMS, persiapan demo |

## Checklist Demo
#### KAFKA:
- [ ] docker compose (Kafka) berjalan — kafka-broker aktif
- [ ] kafka-topics.sh --list menampilkan 2 topic [tema]-api dan [tema]-rss
- [ ] producer_api.py berjalan dan output event terlihat di terminal
- [ ] producer_rss.py berjalan dan output artikel terlihat di terminal
- [ ] consumer_to_hdfs.py berjalan
- [ ] kafka-consumer-groups.sh --describe menampilkan consumer group

#### HDFS:
- [ ] docker compose (Hadoop) berjalan — 4 container aktif
- [ ] hdfs dfs -ls /data/[tema]/api/ menampilkan file JSON
- [ ] hdfs dfs -ls /data/[tema]/rss/ menampilkan file JSON
- [ ] Screenshot HDFS Web UI (localhost:9870) ada di README

#### SPARK:
- [ ] Analisis 1 berjalan tanpa error dari HDFS
- [ ] Analisis 2 berjalan (Spark SQL)
- [ ] Analisis 3 berjalan
- [ ] hdfs dfs -ls /data/[tema]/hasil/ menampilkan output Spark
- [ ] dashboard/data/spark_results.json ada

#### DASHBOARD:
- [ ] python dashboard/app.py berjalan
- [ ] localhost:5000 bisa dibuka di browser
- [ ] Panel data Spark menampilkan data nyata (bukan placeholder)
- [ ] Panel data live menampilkan event terbaru
- [ ] Panel berita menampilkan artikel terbaru
- [ ] Auto-refresh terbukti berjalan

#### REPOSITORY:
- [X] GitHub repo public
- [ ] Semua file kode ada (tidak ada file yang "lupa di-push")
- [ ] README berisi nama anggota + kontribusi + cara menjalankan + screenshot
- [ ] Link repository sudah dikirim ke LMS sebelum deadline

----

# Topik 08 - 🛒 HargaPangan: Monitor Harga Komoditas Bahan Pokok 
*<sub>( Terpilih karena nomor topik serupa dengan nomor kelompok XD )</sub>*

## Diagram Arsitektur

## Step-by-Step

__Screenshot: HDFS Web UI + Kafka consumer output + Dashboard berjalan__

## Tantangan Terbesar dan Mitigasinya
