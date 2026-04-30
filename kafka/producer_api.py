# [Anggota2]: Producer Simulator Harga Komoditas Pangan

import os
import json
import time
import random
import math
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ─── Konfigurasi ────────────────────────────────────────────────────────────

# Gunakan 127.0.0.1 untuk stabilitas di Windows host
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
TOPIC           = "pangan-api"
INTERVAL_DETIK  = 30       # Kirim data setiap 30 detik
REGION_LIST     = [
    "Jakarta", "Surabaya", "Bandung", "Medan",
    "Makassar", "Semarang", "Yogyakarta", "Palembang",
]

# ─── Data Komoditas — Harga Base (Rp/kg, per April 2026) ────────────────────

KOMODITAS = {
    "beras": {
        "base_price": 13_500,
        "unit": "kg",
        "volatility": 0.02,
        "min_price": 11_000,
        "max_price": 17_000,
    },
    "jagung": {
        "base_price": 5_200,
        "unit": "kg",
        "volatility": 0.04,
        "min_price": 4_000,
        "max_price": 7_500,
    },
    "kedelai": {
        "base_price": 11_000,
        "unit": "kg",
        "volatility": 0.03,
        "min_price": 8_500,
        "max_price": 14_000,
    },
    "gula_pasir": {
        "base_price": 17_500,
        "unit": "kg",
        "volatility": 0.025,
        "min_price": 14_000,
        "max_price": 22_000,
    },
    "minyak_goreng": {
        "base_price": 15_800,
        "unit": "liter",
        "volatility": 0.03,
        "min_price": 12_000,
        "max_price": 22_000,
    },
    "cabai_merah": {
        "base_price": 35_000,
        "unit": "kg",
        "volatility": 0.12,
        "min_price": 15_000,
        "max_price": 100_000,
    },
    "bawang_merah": {
        "base_price": 28_000,
        "unit": "kg",
        "volatility": 0.10,
        "min_price": 12_000,
        "max_price": 65_000,
    },
    "telur_ayam": {
        "base_price": 28_500,
        "unit": "kg",
        "volatility": 0.04,
        "min_price": 22_000,
        "max_price": 38_000,
    },
}

# State: simpan harga terakhir per komoditas (random walk)
harga_state: dict[str, float] = {k: v["base_price"] for k, v in KOMODITAS.items()}
harga_prev:  dict[str, float] = {k: v["base_price"] for k, v in KOMODITAS.items()}

# ─── Fungsi Simulasi Harga ───────────────────────────────────────────────────

def simulasi_harga(commodity: str, jam_sekarang: int) -> float:
    meta      = KOMODITAS[commodity]
    base      = meta["base_price"]
    vol       = meta["volatility"]
    prev      = harga_state[commodity]

    # Siklus harian (sinusoidal)
    siklus    = 0.015 * math.sin((jam_sekarang - 9) * math.pi / 12)

    # Random walk dengan mean reversion
    noise     = random.gauss(0, vol * base * 0.3)
    reversion = (base - prev) * 0.1

    harga_baru = prev + noise + reversion + (siklus * base)
    harga_baru = max(meta["min_price"], min(meta["max_price"], harga_baru))
    harga_baru = round(harga_baru / 100) * 100

    harga_state[commodity] = harga_baru
    return harga_baru

def buat_payload(commodity: str, harga: float, region: str) -> dict:
    tz_wib    = timezone(timedelta(hours=7))
    now       = datetime.now(tz_wib)
    prev      = harga_prev[commodity]

    change      = harga - prev
    change_pct  = (change / prev * 100) if prev > 0 else 0.0
    trend       = "naik" if change > 0 else ("turun" if change < 0 else "stabil")

    harga_prev[commodity] = harga

    return {
        "commodity":    commodity,
        "price":        int(harga),
        "unit":         KOMODITAS[commodity]["unit"],
        "region":       region,
        "timestamp":    now.isoformat(),
        "source":       "simulator",
        "change":       int(change),
        "change_pct":   round(change_pct, 2),
        "trend":        trend,
    }

# ─── Main Producer ────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  🌾 HargaPangan Producer — Simulator Harga Komoditas")
    print(f"  Topic   : {TOPIC}")
    print(f"  Broker  : {KAFKA_BOOTSTRAP}")
    print(f"  Interval: {INTERVAL_DETIK} detik")
    print("=" * 60)

    # Loop retry untuk koneksi awal ke Kafka
    producer = None
    max_retries = 5
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,
                compression_type="gzip",
            )
            print(f"  ✅ Terhubung ke Kafka Broker: {KAFKA_BOOTSTRAP}")
            break
        except NoBrokersAvailable:
            print(f"  ⚠️  Broker {KAFKA_BOOTSTRAP} belum tersedia (mencoba lagi {i+1}/{max_retries})...")
            time.sleep(5)
        except Exception as e:
            print(f"  ❌ Error saat inisialisasi: {e}")
            time.sleep(5)
    
    if not producer:
        print(f"  ❌ Gagal terhubung ke Kafka setelah {max_retries} percobaan.")
        return

    siklus = 0
    try:
        while True:
            siklus += 1
            jam = datetime.now().hour
            print(f"\n[Siklus #{siklus} — {datetime.now().strftime('%H:%M:%S')}]")

            for commodity in KOMODITAS:
                region  = random.choice(REGION_LIST)
                harga   = simulasi_harga(commodity, jam)
                payload = buat_payload(commodity, harga, region)

                producer.send(
                    topic=TOPIC,
                    key=commodity,
                    value=payload,
                )

                print(
                    f"  ✅ {commodity:<15} Rp {payload['price']:>8,}  "
                    f"{payload['trend']:>6}  {payload['change_pct']:+.2f}%  "
                    f"[{region}]"
                )

            producer.flush()
            print(f"  ↑ {len(KOMODITAS)} records dikirim ke '{TOPIC}'")
            time.sleep(INTERVAL_DETIK)

    except KeyboardInterrupt:
        print(f"\n✋ Producer dihentikan. Total siklus: {siklus}")
    except KafkaError as e:
        print(f"\n❌ Kafka Error: {e}")
    finally:
        if producer:
            producer.flush()
            producer.close()
            print("🔌 Producer ditutup.")

if __name__ == "__main__":
    main()
