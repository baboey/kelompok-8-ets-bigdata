"""
consumer_to_hdfs.py — Consumer Kafka → HDFS + Local JSON
=========================================================
Anggota : Akbar
Topics  : pangan-api, pangan-rss
Group   : pangan-consumer-group

Alur data (arsitektur benar):
  Kafka → Consumer (real-time) → local JSON (dashboard langsung baca)
                               → HDFS (batch setiap HDFS_BATCH_SIZE pesan)

Cara menjalankan:
    pip install kafka-python-ng hdfs
    python consumer_to_hdfs.py
"""

import json, os, threading, time
from datetime import datetime, timezone, timedelta
from collections import deque
from kafka import KafkaConsumer

# ─── Konfigurasi ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = "localhost:9092"
GROUP_ID         = "pangan-consumer-group"
HDFS_URL         = "http://localhost:9870"      # NameNode WebHDFS
HDFS_USER        = "root"
HDFS_BATCH_SIZE  = 24     # Flush ke HDFS setiap 24 pesan (3 iterasi producer)
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


# ─── Fungsi HDFS ─────────────────────────────────────────────────────────────

def simpan_ke_hdfs(data: list, hdfs_path: str, label: str):
    """Simpan batch data ke HDFS via library hdfs atau fallback docker exec."""
    if not data:
        return

    timestamp = datetime.now(TZ_WIB).strftime("%Y-%m-%d_%H-%M-%S")
    filename  = f"{timestamp}.json"
    konten    = "\n".join(json.dumps(d, ensure_ascii=False) for d in data)

    # Coba hdfs Python library
    try:
        from hdfs import InsecureClient
        client    = InsecureClient(HDFS_URL, user=HDFS_USER)
        full_path = f"{hdfs_path}/{filename}"
        client.makedirs(hdfs_path)
        with client.write(full_path, encoding="utf-8", overwrite=True) as writer:
            writer.write(konten)
        print(f"  [HDFS] [{label}] → {full_path} ({len(data)} records)")
        return
    except ImportError:
        pass
    except Exception as e:
        print(f"  [HDFS] Peringatan: {e} — mencoba fallback docker exec")

    # Fallback: docker exec ke namenode
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
        print(f"  [HDFS] [{label}] → {full_path} ({len(data)} records) via docker exec")
    except Exception as e:
        print(f"  [HDFS] Gagal simpan ke HDFS: {e}")


def update_local_file(filepath: str, data: deque):
    """Update file JSON lokal untuk dashboard (real-time)."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(list(data), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  [LOCAL] Gagal update {os.path.basename(filepath)}: {e}")


def cek_hdfs_files(hdfs_path: str) -> list:
    """Cek file yang ada di HDFS path (untuk verifikasi)."""
    try:
        from hdfs import InsecureClient
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        files = client.list(hdfs_path, status=True)
        return [(f, s['length']) for f, s in files]
    except Exception:
        pass
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "namenode",
             "hdfs", "dfs", "-ls", hdfs_path],
            capture_output=True, text=True
        )
        return result.stdout.strip().split("\n")[1:]  # skip header
    except Exception:
        return []


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
            simpan_ke_hdfs(batch, HDFS_PATH_API, "API")


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
            simpan_ke_hdfs(batch, HDFS_PATH_RSS, "RSS")


# ─── Thread Verifikasi HDFS ───────────────────────────────────────────────────

def monitor_hdfs():
    """Thread background: cetak isi HDFS setiap 5 menit sebagai verifikasi."""
    while True:
        time.sleep(300)  # setiap 5 menit
        print("\n  ====== VERIFIKASI HDFS ======")
        api_files = cek_hdfs_files(HDFS_PATH_API)
        rss_files = cek_hdfs_files(HDFS_PATH_RSS)
        print(f"  [HDFS] /data/pangan/api/ → {len(api_files)} file(s)")
        for f in api_files[-5:]:  # tampilkan 5 terbaru
            print(f"         {f}")
        print(f"  [HDFS] /data/pangan/rss/ → {len(rss_files)} file(s)")
        print("  ==============================\n")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  HargaPangan Consumer — Kafka → Local JSON + HDFS")
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
    t_hdfs = threading.Thread(target=monitor_hdfs, name="hdfs-monitor",  daemon=True)

    t_api.start()
    t_rss.start()
    t_hdfs.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n  Consumer dihentikan.")


if __name__ == "__main__":
    main()
