"""
consumer_to_hdfs.py — Consumer Kafka → HDFS + Local JSON
=========================================================
Anggota : Akbar
Topics  : pangan-api, pangan-rss
Group   : pangan-consumer-group
Output  : HDFS /data/pangan/api/ dan /data/pangan/rss/
          + salinan lokal dashboard/data/

Cara menjalankan:
    pip install kafka-python hdfs
    python consumer_to_hdfs.py
"""

import json, os, threading
from datetime import datetime, timezone, timedelta
from collections import deque
from kafka import KafkaConsumer

# ─── Konfigurasi ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = "localhost:9092"
GROUP_ID         = "pangan-consumer-group"
HDFS_URL         = "http://localhost:9870"      # NameNode WebHDFS
HDFS_USER        = "root"
BUFFER_INTERVAL  = 120    # Flush ke HDFS setiap 2 menit
MAX_LOCAL_EVENTS = 50     # Simpan 50 event terakhir di local JSON

# Path HDFS
HDFS_PATH_API = "/data/pangan/api"
HDFS_PATH_RSS = "/data/pangan/rss"

# Path lokal (untuk dashboard)
LOCAL_DIR     = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data")
LOCAL_API_FILE = os.path.join(LOCAL_DIR, "live_api.json")
LOCAL_RSS_FILE = os.path.join(LOCAL_DIR, "live_rss.json")

# Buffer in-memory
buffer_api: list = []
buffer_rss: list = []
buffer_lock       = threading.Lock()

# Deque untuk local files (max last N)
local_api  = deque(maxlen=MAX_LOCAL_EVENTS)
local_rss  = deque(maxlen=20)


# ─── Fungsi HDFS ─────────────────────────────────────────────────────────────

def simpan_ke_hdfs(data: list, hdfs_path: str, label: str):
    """Simpan batch data ke HDFS menggunakan library hdfs atau fallback docker exec."""
    if not data:
        return

    tz_wib     = timezone(timedelta(hours=7))
    timestamp  = datetime.now(tz_wib).strftime("%Y-%m-%d_%H-%M")
    filename   = f"{timestamp}.json"
    konten     = "\n".join(json.dumps(d, ensure_ascii=False) for d in data)

    # Coba hdfs Python library
    try:
        from hdfs import InsecureClient
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        full_path = f"{hdfs_path}/{filename}"
        client.makedirs(hdfs_path)
        with client.write(full_path, encoding="utf-8", overwrite=True) as writer:
            writer.write(konten)
        print(f"  📂 [{label}] → HDFS {full_path} ({len(data)} records)")
        return
    except ImportError:
        pass
    except Exception as e:
        print(f"  ⚠️  HDFS library error: {e} — coba docker exec fallback")

    # Fallback: docker exec namenode
    try:
        import subprocess, tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json",
                                         delete=False, encoding="utf-8") as f:
            f.write(konten)
            tmp_path = f.name

        full_path = f"{hdfs_path}/{filename}"
        subprocess.run(
            ["docker", "exec", "-i", "namenode",
             "hdfs", "dfs", "-mkdir", "-p", hdfs_path],
            check=True, capture_output=True
        )
        subprocess.run(
            ["docker", "cp", tmp_path, f"namenode:/tmp/{filename}"],
            check=True, capture_output=True
        )
        subprocess.run(
            ["docker", "exec", "namenode",
             "hdfs", "dfs", "-put", "-f", f"/tmp/{filename}", full_path],
            check=True, capture_output=True
        )
        os.unlink(tmp_path)
        print(f"  📂 [{label}] → HDFS {full_path} ({len(data)} records) via docker exec")
    except Exception as e:
        print(f"  ❌ Gagal simpan ke HDFS: {e}")


def update_local_file(filepath: str, data: deque):
    """Update file JSON lokal untuk dashboard."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(list(data), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  ⚠️  Gagal update local file {filepath}: {e}")


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
        consumer_timeout_ms=BUFFER_INTERVAL * 1000,
    )
    print("  🟢 Consumer API aktif (topic: pangan-api)")

    while True:
        try:
            for msg in consumer:
                data = msg.value
                with buffer_lock:
                    buffer_api.append(data)
                    local_api.append(data)

                print(f"    🌾 [API] {data.get('commodity','?'):<15} "
                      f"Rp {data.get('price',0):>8,} — {data.get('trend','?')}")
        except Exception:
            # Timeout → flush ke HDFS
            with buffer_lock:
                if buffer_api:
                    simpan_ke_hdfs(buffer_api.copy(), HDFS_PATH_API, "API")
                    buffer_api.clear()
                update_local_file(LOCAL_API_FILE, local_api)
            continue


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
        consumer_timeout_ms=BUFFER_INTERVAL * 1000,
    )
    print("  🟢 Consumer RSS aktif (topic: pangan-rss)")

    while True:
        try:
            for msg in consumer:
                data = msg.value
                with buffer_lock:
                    buffer_rss.append(data)
                    local_rss.append(data)

                print(f"    📰 [RSS] {data.get('title','?')[:60]}...")
        except Exception:
            with buffer_lock:
                if buffer_rss:
                    simpan_ke_hdfs(buffer_rss.copy(), HDFS_PATH_RSS, "RSS")
                    buffer_rss.clear()
                update_local_file(LOCAL_RSS_FILE, local_rss)
            continue


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  📥 HargaPangan Consumer — Kafka → HDFS")
    print(f"  Group   : {GROUP_ID}")
    print(f"  HDFS    : {HDFS_URL}")
    print(f"  Flush   : setiap {BUFFER_INTERVAL//60} menit")
    print("=" * 55)

    os.makedirs(LOCAL_DIR, exist_ok=True)

    # Jalankan dua thread consumer secara paralel
    t_api = threading.Thread(target=consume_api, name="consumer-api", daemon=True)
    t_rss = threading.Thread(target=consume_rss, name="consumer-rss", daemon=True)

    t_api.start()
    t_rss.start()

    try:
        t_api.join()
        t_rss.join()
    except KeyboardInterrupt:
        print("\n✋ Consumer dihentikan.")


if __name__ == "__main__":
    main()
