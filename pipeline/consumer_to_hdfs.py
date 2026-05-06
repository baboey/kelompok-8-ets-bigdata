"""
consumer_to_hdfs.py — Consumer Kafka → HDFS (Parquet) + Local JSON
==================================================================
Anggota : Akbar
Topics  : pangan-api, pangan-rss
Group   : pangan-consumer-group

Alur data:
  Kafka → Consumer (real-time) → local JSON (dashboard langsung baca)
                               → Preprocessing (TextBlob Sentiment Analysis)
                               → HDFS (Parquet format, partitioned by Date)
"""

import json, os, threading, time
from datetime import datetime, timezone, timedelta
from collections import deque
from kafka import KafkaConsumer
import pandas as pd
from textblob import TextBlob
from tenacity import retry, wait_exponential, stop_after_attempt

# ─── Konfigurasi ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = "localhost:9092"
GROUP_ID         = "pangan-consumer-group"
HDFS_URL         = "http://localhost:9870"      # NameNode WebHDFS
HDFS_USER        = "root"
HDFS_BATCH_SIZE  = 24     # Flush ke HDFS setiap 24 pesan
MAX_LOCAL_EVENTS = 50     # Simpan 50 event terakhir di local JSON

# Path HDFS
HDFS_PATH_API = "/data/pangan/api"
HDFS_PATH_RSS = "/data/pangan/rss"

# Path lokal (untuk dashboard)
LOCAL_DIR      = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data")
LOCAL_API_FILE = os.path.join(LOCAL_DIR, "live_api.json")
LOCAL_RSS_FILE = os.path.join(LOCAL_DIR, "live_rss.json")

# Buffer in-memory untuk HDFS batch
buffer_api: list = []
buffer_rss: list = []
buffer_lock       = threading.Lock()

# Deque untuk local JSON (dashboard)
local_api = deque(maxlen=MAX_LOCAL_EVENTS)
local_rss = deque(maxlen=20)

TZ_WIB = timezone(timedelta(hours=7))

# ─── Preprocessing & ML ──────────────────────────────────────────────────────

def preprocess_and_analyze(data: list, label: str) -> pd.DataFrame:
    """Preprocess data and apply Text Classification/Sentiment Analysis for RSS."""
    df = pd.DataFrame(data)
    
    if label == "RSS" and not df.empty:
        # Preprocessing text: lowercasing and basic cleaning
        df["cleaned_title"] = df["title"].astype(str).str.lower().str.strip()
        df["cleaned_summary"] = df["summary"].astype(str).str.lower().str.strip()
        
        # Machine Learning Component: Sentiment Analysis using TextBlob
        def get_sentiment(text):
            analysis = TextBlob(text)
            polarity = analysis.sentiment.polarity
            if polarity > 0.1:
                return 'positive'
            elif polarity < -0.1:
                return 'negative'
            else:
                return 'neutral'
                
        df["sentiment"] = df["cleaned_title"].apply(get_sentiment)

    # Convert timestamp column to datetime for partitioning
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["year"] = df["timestamp"].dt.year
        df["month"] = df["timestamp"].dt.month
        df["day"] = df["timestamp"].dt.day
        
    return df

# ─── Fungsi HDFS ─────────────────────────────────────────────────────────────

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def simpan_ke_hdfs(data: list, hdfs_path: str, label: str):
    """Simpan batch data ke HDFS dalam format Parquet dengan partisi."""
    if not data:
        return

    # Preprocessing and Sentiment Analysis
    df = preprocess_and_analyze(data, label)
    
    timestamp_str = datetime.now(TZ_WIB).strftime("%Y-%m-%d_%H-%M-%S")
    filename  = f"{timestamp_str}.parquet"

    # Save to local temporary Parquet file first
    try:
        import tempfile, subprocess
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_parquet_path = os.path.join(tmp_dir, filename)
            # Simpan tanpa partisi di lokal untuk dikirim ke folder partisi HDFS
            df.to_parquet(tmp_parquet_path, engine="pyarrow", index=False)
            
            # Kita tentukan folder partisi dari data record pertama
            year = df["year"].iloc[0] if "year" in df.columns else datetime.now(TZ_WIB).year
            month = df["month"].iloc[0] if "month" in df.columns else datetime.now(TZ_WIB).month
            day = df["day"].iloc[0] if "day" in df.columns else datetime.now(TZ_WIB).day
            
            partitioned_hdfs_path = f"{hdfs_path}/year={year}/month={month}/day={day}"
            full_path = f"{partitioned_hdfs_path}/{filename}"
            
            # Buat direktori partisi di HDFS
            subprocess.run(
                ["docker", "exec", "-i", "namenode",
                 "hdfs", "dfs", "-mkdir", "-p", partitioned_hdfs_path],
                check=True, capture_output=True
            )
            # Copy Parquet ke docker container (namenode)
            subprocess.run(
                ["docker", "cp", tmp_parquet_path, f"namenode:/tmp/{filename}"],
                check=True, capture_output=True
            )
            # Pindahkan ke path tujuan di HDFS
            subprocess.run(
                ["docker", "exec", "namenode",
                 "hdfs", "dfs", "-put", "-f", f"/tmp/{filename}", full_path],
                check=True, capture_output=True
            )
            print(f"  [HDFS] [{label}] → {full_path} ({len(data)} records) - Parquet Partitioned")
    except Exception as e:
        print(f"  [HDFS] Gagal simpan ke HDFS: {e}")
        raise e


def update_local_file(filepath: str, data: deque):
    """Update file JSON lokal untuk dashboard (real-time)."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(list(data), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  [LOCAL] Gagal update {os.path.basename(filepath)}: {e}")


# ─── Thread Consumer API ──────────────────────────────────────────────────────

def consume_api():
    """Thread consumer untuk topic pangan-api."""
    consumer = KafkaConsumer(
        "pangan-api",
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=GROUP_ID,
        auto_offset_reset="latest",   # hanya pesan baru (production mode)
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    print("  [OK] Consumer API aktif (topic: pangan-api)")

    msg_count = 0
    try:
        for msg in consumer:
            data = msg.value

            # 1. Update local JSON LANGSUNG (real-time untuk dashboard)
            with buffer_lock:
                local_api.append(data)
                buffer_api.append(data)
                msg_count += 1
                current_count = len(buffer_api)

            update_local_file(LOCAL_API_FILE, local_api)

            # Log ringkas
            kom  = data.get("komoditas", "?")
            harga = data.get("harga", 0)
            pct  = data.get("perubahan_persen", 0)
            arrow = "+" if pct > 0 else ("-" if pct < 0 else "=")
            print(f"  [API] #{msg_count} {kom:<15} Rp {harga:>9,.0f} ({arrow}{abs(pct):.2f}%) "
                  f"| local={len(local_api)}/{MAX_LOCAL_EVENTS} | hdfs_batch={current_count}/{HDFS_BATCH_SIZE}")

            # 2. Flush ke HDFS setelah batch penuh
            if current_count >= HDFS_BATCH_SIZE:
                with buffer_lock:
                    batch = buffer_api.copy()
                    buffer_api.clear()
                try:
                    simpan_ke_hdfs(batch, HDFS_PATH_API, "API")
                except Exception as e:
                    print(f"  [ERROR] Gagal simpan HDFS setelah retries: {e}")
    except Exception as e:
        print(f"  [CRITICAL] Error di consumer API: {e}")


# ─── Thread Consumer RSS ──────────────────────────────────────────────────────

def consume_rss():
    """Thread consumer untuk topic pangan-rss."""
    consumer = KafkaConsumer(
        "pangan-rss",
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    print("  [OK] Consumer RSS aktif (topic: pangan-rss)")

    rss_count = 0
    try:
        for msg in consumer:
            data = msg.value
            rss_count += 1

            with buffer_lock:
                local_rss.append(data)
                buffer_rss.append(data)
                current_rss = len(buffer_rss)

            update_local_file(LOCAL_RSS_FILE, local_rss)
            print(f"  [RSS] #{rss_count} {data.get('title','?')[:60]}...")

            if current_rss >= 10:
                with buffer_lock:
                    batch = buffer_rss.copy()
                    buffer_rss.clear()
                try:
                    simpan_ke_hdfs(batch, HDFS_PATH_RSS, "RSS")
                except Exception as e:
                    print(f"  [ERROR] Gagal simpan HDFS setelah retries: {e}")
    except Exception as e:
        print(f"  [CRITICAL] Error di consumer RSS: {e}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  HargaPangan Consumer — Kafka → Preprocessing → HDFS (Parquet)")
    print(f"  Group      : {GROUP_ID}")
    print(f"  HDFS       : {HDFS_URL}")
    print(f"  Local JSON : {LOCAL_API_FILE}")
    print(f"  HDFS batch : setiap {HDFS_BATCH_SIZE} pesan")
    print(f"  Mode       : PRODUCTION (auto_offset_reset=latest)")
    print("=" * 60)

    os.makedirs(LOCAL_DIR, exist_ok=True)

    # Threads
    t_api  = threading.Thread(target=consume_api,  name="consumer-api",  daemon=True)
    t_rss  = threading.Thread(target=consume_rss,  name="consumer-rss",  daemon=True)

    t_api.start()
    t_rss.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n  Consumer dihentikan.")


if __name__ == "__main__":
    main()
