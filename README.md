# 🌾 HargaPangan Monitor — Big Data Pipeline

> **ETS Mata Kuliah Big Data** | Sistem monitoring harga komoditas bahan pokok Indonesia menggunakan arsitektur Big Data end-to-end.

---

## 👥 Anggota Tim
| NRP | Nama | Peran |
| --- |------|-------| 
| 5027231007 | **Thio Billy** | Producer API |
| 5027241026 | **Evan Christian Nainggolan** | Spark Analysis |
| 5027241039 | **Rayka Dharma Pranandita** | DevOps + Dashboard |
| 5027241044 | **Rizqi Akbar** | Producer RSS + Consumer |


---

## 📋 Topik & Justifikasi

**Topik:** HargaPangan — Monitor Harga Komoditas Bahan Pokok Indonesia

**Pertanyaan Bisnis:**
> *"Komoditas mana yang paling bergejolak harganya hari ini, dan apakah ada berita ekonomi yang menjelaskan penyebabnya?"*

**Justifikasi sumber data:**
- Producer API menggunakan **simulator realistis** dengan model *random walk* berbasis harga pasar riil Indonesia (April 2026), karena API harga pangan publik tidak menyediakan endpoint yang stabil dan terdokumentasi untuk akses programatik.
- Producer RSS menggunakan feed **Bisnis.com** dan **Kompas.com** (ekonomi/money) sebagai sumber berita pangan real-time.

---

## 🏗️ Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────┐
│                      PRODUCERS                          │
│  producer_api.py          producer_rss.py               │
│  (Simulator Harga)        (RSS Bisnis.com)               │
│       │                        │                        │
│       ▼ topic: pangan-api      ▼ topic: pangan-rss      │
└───────────────────┬────────────┴────────────────────────┘
                    │
          ┌─────────▼──────────┐
          │   Apache Kafka     │
          │   (KRaft Mode)     │
          │   localhost:9092   │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │ consumer_to_hdfs   │
          │  Buffer 2 menit    │
          └──────┬──────┬──────┘
                 │      │
        ┌────────▼┐    ┌▼─────────────┐
        │  HDFS   │    │  Local JSON  │
        │/data/   │    │ dashboard/   │
        │pangan/  │    │ data/*.json  │
        └────────┬┘    └─────────┬────┘
                 │               │
        ┌────────▼──────┐        │
        │ Spark Analysis│        │
        │ analysis.py   │        │
        └────────┬──────┘        │
                 │               │
        ┌────────▼───────────────▼────┐
        │     Flask Dashboard         │
        │     localhost:5000          │
        └─────────────────────────────┘
```

---

## 🚀 Cara Menjalankan

### Prasyarat

- Docker Desktop (min. RAM 6GB dialokasikan)
- Python 3.9+
- pip

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Hadoop Cluster

```bash
docker compose -f docker-compose-hadoop.yml up -d
```

Verifikasi:
```bash
# HDFS Web UI
http://localhost:9870

# Cek namenode running
docker ps | grep namenode
```

Buat direktori HDFS untuk data:
```bash
docker exec namenode hdfs dfs -mkdir -p /data/pangan/api
docker exec namenode hdfs dfs -mkdir -p /data/pangan/rss
docker exec namenode hdfs dfs -mkdir -p /data/pangan/hasil
docker exec namenode hdfs dfs -chmod 777 /data/pangan
```

### 3. Start Kafka

```bash
docker compose -f docker-compose-kafka.yml up -d
```

Verifikasi:
```bash
# Kafka UI
http://localhost:8080

# Cek broker running
docker ps | grep kafka-broker

# Cek topics (setelah producer jalan)
docker exec kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 4. Jalankan Producers (di terminal terpisah)

**Terminal 1 — Producer API (Billy):**
```bash
python kafka/producer_api.py
```

**Terminal 2 — Producer RSS (Akbar):**
```bash
python kafka/producer_rss.py
```

### 5. Jalankan Consumer HDFS (Akbar)

**Terminal 3:**
```bash
python kafka/consumer_to_hdfs.py
```

### 6. Jalankan Spark Analysis (Evan)

Tunggu beberapa menit hingga data terkumpul di HDFS, lalu:
```bash
# Jika PySpark terinstall di host
python spark/analysis.py

# Atau via spark-submit
spark-submit --master local[*] spark/analysis.py
```

### 7. Jalankan Flask Dashboard (Rayka)

**Terminal 4:**
```bash
python dashboard/app.py
```

Buka browser: **http://localhost:5000**

---

## 📁 Struktur Proyek

```
ets/
├── hadoop.env                    # Konfigurasi Hadoop environment
├── docker-compose-hadoop.yml     # Hadoop HDFS/YARN (4 container)
├── docker-compose-kafka.yml      # Kafka KRaft + Kafka UI
├── requirements.txt              # Python dependencies
├── .gitignore
│
├── kafka/
│   ├── producer_api.py           # Simulator harga komoditas (Billy)
│   ├── producer_rss.py           # RSS feed reader (Akbar)
│   └── consumer_to_hdfs.py      # Consumer → HDFS (Akbar)
│
├── spark/
│   └── analysis.py              # 3 analisis + MLlib bonus (Evan)
│
└── dashboard/
    ├── app.py                   # Flask backend (Rayka)
    ├── templates/index.html     # Dashboard UI
    ├── static/style.css         # Dark theme CSS
    └── data/                   # Runtime data (di-gitignore)
        ├── live_api.json        # 50 harga terakhir dari Kafka
        ├── live_rss.json        # 20 berita terakhir dari Kafka
        └── spark_results.json  # Hasil analisis Spark
```

---

## 🔌 Port & Services

| Service | URL | Keterangan |
|---------|-----|------------|
| HDFS Web UI | http://localhost:9870 | Monitor HDFS, browse files |
| YARN ResourceManager | http://localhost:8088 | Monitor jobs YARN |
| Kafka UI | http://localhost:8080 | Monitor topics & consumer groups |
| Flask Dashboard | http://localhost:5000 | Dashboard utama |
| Kafka Broker | localhost:9092 | Endpoint untuk producer/consumer |
| HDFS RPC | localhost:8020 | Endpoint untuk koneksi Spark/Python |

---

## 📊 Kafka Topics

| Topic | Producer | Consumer | Isi |
|-------|----------|----------|-----|
| `pangan-api` | `producer_api.py` | `consumer_to_hdfs.py` | Data harga komoditas (JSON per event) |
| `pangan-rss` | `producer_rss.py` | `consumer_to_hdfs.py` | Artikel berita ekonomi (JSON per artikel) |

**Contoh payload `pangan-api`:**
```json
{
  "commodity": "beras",
  "price": 13500,
  "unit": "kg",
  "region": "Jakarta",
  "timestamp": "2026-04-27T14:00:00+07:00",
  "source": "simulator",
  "change": 100,
  "change_pct": 0.75,
  "trend": "naik"
}
```

**Contoh payload `pangan-rss`:**
```json
{
  "title": "Harga Beras Naik Jelang Ramadan",
  "link": "https://bisnis.com/...",
  "summary": "Kenaikan harga beras...",
  "published": "2026-04-27T10:00:00",
  "source": "bisnis.com",
  "timestamp": "2026-04-27T14:05:00+07:00"
}
```

---

## 🔥 Analisis Spark (3 Wajib + 1 Bonus)

| # | Analisis | API Spark | Output |
|---|----------|-----------|--------|
| 1 | **Volatilitas Harga** — ranking komoditas paling bergejolak | DataFrame API (`groupBy`, `agg`) | `volatilitas_pct` per komoditas |
| 2 | **Tren Harga per Periode** — rata-rata harga per jam | Spark SQL (`CREATE TEMP VIEW`, `SELECT AVG`) | Time-series per komoditas |
| 3 | **Korelasi Berita-Harga** — frekuensi sebutan komoditas di RSS vs perubahan harga | DataFrame + cross-reference | Tabel korelasi |
| 🎁 | **Prediksi Tren** — Linear Regression harga beras | MLlib Pipeline (`VectorAssembler`, `LinearRegression`) | Koefisien, R², RMSE |

---

## 🖥️ Dashboard — Panel

| Panel | Data Sumber | Fitur |
|-------|------------|-------|
| 💹 Harga Terkini | `live_api.json` | 8 komoditas, indikator ▲/▼, flash animasi saat update |
| 📈 Tren Harga | `spark_results.json` | Line chart Chart.js, multi-komoditas |
| ⚡ Volatilitas | `spark_results.json` | Bar horizontal, ranking bergejolak |
| 📰 Berita RSS | `live_rss.json` | 20 artikel terbaru, link ke sumber |
| 🔗 Korelasi | `spark_results.json` | Tabel frekuensi berita vs perubahan harga |
| 🤖 MLlib | `spark_results.json` | Koefisien, R², RMSE, interpretasi |

Auto-refresh: **setiap 30 detik**

---

## 🛠️ Troubleshooting

**Kafka tidak bisa diakses dari Python:**
```bash
# Pastikan broker sudah healthy
docker ps | grep kafka-broker
# Cek log
docker logs kafka-broker --tail 50
```

**HDFS permission denied:**
```bash
docker exec namenode hdfs dfs -chmod -R 777 /data
```

**Dashboard panel kosong:**
- Pastikan `consumer_to_hdfs.py` sudah jalan dan ada data di `dashboard/data/`
- Jalankan `spark/analysis.py` untuk generate `spark_results.json`
- Cek endpoint: `curl http://localhost:5000/api/status`

**RAM tidak cukup (Docker):**
- Hadoop butuh minimal 4GB
- Kafka butuh minimal 1GB
- Total rekomendasi: **≥ 6GB** untuk Docker Desktop

---

## 📸 Screenshots

> *Tambahkan screenshot berikut setelah sistem berjalan:*
> - [ ] HDFS Web UI — file di `/data/pangan/`
> - [ ] Kafka UI — topics `pangan-api` dan `pangan-rss`
> - [ ] Terminal producer berjalan
> - [ ] Flask Dashboard — semua panel

---

## ⚠️ Catatan Penting

- File `dashboard/data/*.json` tidak di-commit ke Git (ada di `.gitignore`) — dibuat secara otomatis saat sistem berjalan
- Consumer menggunakan `group_id="pangan-consumer-group"` — restart consumer akan lanjut dari offset terakhir (tidak ada data yang terlewat)
- Spark analysis bisa dijalankan berkali-kali — hasilnya akan di-overwrite
