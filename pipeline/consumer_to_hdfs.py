"""
consumer_to_hdfs.py — Consumer Kafka → HDFS (JSON) + Local JSON
==================================================================
Anggota : Akbar
Topics  : pangan-api, pangan-rss
Group   : pangan-consumer-group

Alur data:
  Kafka → Consumer (real-time) → local JSON (dashboard langsung baca)
                               → Preprocessing (ML Pipeline)
                               → HDFS (JSON format, diberi nama timestamp)
"""

import json, os, threading, time
from datetime import datetime, timezone, timedelta
from collections import deque
from kafka import KafkaConsumer
from tenacity import retry, wait_exponential, stop_after_attempt
import subprocess
import pandas as pd
from textblob import TextBlob

from ml_pipeline import rss_classifier, api_regressor

# ─── Konfigurasi ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = "localhost:9092"
GROUP_ID         = "pangan-consumer-group"
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
        
        # Rule-based Component: Sentiment Analysis using TextBlob (serves as synthetic target label)
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
        
        # ── ML Component: Retrain and Predict ──
        acc = rss_classifier.retrain(df)
        if acc is not None:
            print(f"  [ML] RSS Classifier retrained. Accuracy: {acc:.2f}")
        df = rss_classifier.predict(df)

    elif label == "API" and not df.empty:
        # ── ML Component: Retrain and Predict ──
        # Ensure we have the necessary columns before predicting/training
        if "komoditas" in df.columns and "harga" in df.columns:
            # We don't want to convert "harga" permanently to string if it's already int, but ensure we use a clean float copy inside ML
            rmse = api_regressor.retrain(df)
            if rmse is not None:
                print(f"  [ML] API Price Regressor retrained. RMSE: {rmse:.2f}")
            df = api_regressor.predict(df)

    return df

# ─── Fungsi HDFS ─────────────────────────────────────────────────────────────

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def simpan_ke_hdfs(data: list, hdfs_path: str, label: str):
    """
    Simpan batch data ke HDFS dalam format JSON bernama timestamp.
    Menggunakan subprocess (docker exec) — Opsi A dari hint soal.
    Nama file: YYYY-MM-DD_HH-MM-SS.json
    """
    if not data:
        return

    # Preprocessing and ML evaluation
    df = preprocess_and_analyze(data, label)

    timestamp_str = datetime.now(TZ_WIB).strftime("%Y-%m-%d_%H-%M-%S")
    filename  = f"{timestamp_str}.json"

    try:
        import tempfile
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file_path = os.path.join(tmp_dir, filename)

            # Simpan ke file lokal sementara sebagai JSON lines
            # Ensure timestamps are serialized correctly
            if "timestamp" in df.columns:
                df["timestamp"] = df["timestamp"].astype(str)
                
            with open(tmp_file_path, "w", encoding="utf-8") as f:
                for record in df.to_dict('records'):
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")

            hdfs_full_path = f"{hdfs_path}/{filename}"

            # Copy file ke dalam container namenode
            subprocess.run(
                ["docker", "cp", tmp_file_path, f"namenode:/tmp/{filename}"],
                check=True, capture_output=True
            )
            # Upload dari container ke HDFS
            subprocess.run(
                ["docker", "exec", "namenode",
                 "hdfs", "dfs", "-put", "-f", f"/tmp/{filename}", hdfs_full_path],
                check=True, capture_output=True
            )
            print(f"  [HDFS] [{label}] → {hdfs_full_path} ({len(data)} records) - JSON")
    except subprocess.CalledProcessError as e:
        err_msg = e.stderr.decode('utf-8') if e.stderr else str(e)
        print(f"  [HDFS] Docker command failed: {err_msg}")
        raise e
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
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    print("  [OK] Consumer API aktif (topic: pangan-api)")

    msg_count = 0
    try:
        for msg in consumer:
            data = msg.value

            # -- ML Component: Predict Real-time for Dashboard --
            try:
                single_df = pd.DataFrame([data])
                if "komoditas" in single_df.columns and not single_df.empty:
                    single_df = api_regressor.predict(single_df)
                    if 'ml_harga_pred' in single_df.columns:
                        data['ml_harga_pred'] = float(single_df['ml_harga_pred'].iloc[0])
            except Exception as e:
                pass # Fail silently for prediction

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
        auto_offset_reset="earliest",
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

            # -- ML Component: Predict Real-time for Dashboard --
            try:
                single_df = pd.DataFrame([data])
                if "title" in single_df.columns and not single_df.empty:
                    single_df["cleaned_title"] = single_df["title"].astype(str).str.lower().str.strip()
                    single_df = rss_classifier.predict(single_df)
                    if 'ml_sentiment_pred' in single_df.columns:
                        data['ml_sentiment_pred'] = str(single_df['ml_sentiment_pred'].iloc[0])
            except Exception as e:
                pass # Fail silently for prediction

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
    print("  HargaPangan Consumer — Kafka → Preprocessing → HDFS (JSON)")
    print(f"  Group      : {GROUP_ID}")
    print(f"  Local JSON : {LOCAL_API_FILE}")
    print(f"  HDFS batch : setiap {HDFS_BATCH_SIZE} pesan")
    print(f"  Mode       : auto_offset_reset=earliest")
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
