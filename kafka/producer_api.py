"""
producer_api.py — Simulator Harga Komoditas Pangan Indonesia
============================================================
Anggota : Billy
Tugas   : ETS Big Data — HargaPangan Pipeline
Topic   : pangan-api
Update  : setiap ~30 detik (simulasi polling data harga)

Deskripsi:
    Generator simulator harga 8 komoditas bahan pokok Indonesia
    menggunakan model random walk realistis. Harga berfluktuasi
    berdasarkan base price riil + noise siklus harian + random walk.

Komoditas:
    beras, jagung, kedelai, gula_pasir, minyak_goreng,
    cabai_merah, bawang_merah, telur_ayam

Format JSON per record:
    {
        "commodity": "beras",
        "price": 13500,
        "unit": "kg",
        "region": "Jakarta",
        "timestamp": "2026-04-27T14:00:00+07:00",
        "source": "simulator",
        "change_pct": 0.35,
        "trend": "naik"
    }

Cara menjalankan:
    pip install kafka-python
    python producer_api.py
"""

import json
import time
import random
import math
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── Konfigurasi ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC           = "pangan-api"
INTERVAL_DETIK  = 30       # Kirim data setiap 30 detik
REGION_LIST     = [
    "Jakarta", "Surabaya", "Bandung", "Medan",
    "Makassar", "Semarang", "Yogyakarta", "Palembang",
]

# ─── Data Komoditas — Harga Base (Rp/kg, per April 2026) ────────────────────
#
# Base price diambil dari referensi harga pasar Indonesia:
# - Beras medium: ~Rp 13.500/kg
# - Jagung pipilan: ~Rp 5.200/kg
# - Kedelai impor: ~Rp 11.000/kg
# - Gula pasir lokal: ~Rp 17.500/kg
# - Minyak goreng curah: ~Rp 15.800/kg
# - Cabai merah besar: ~Rp 35.000/kg (bergejolak tinggi)
# - Bawang merah: ~Rp 28.000/kg (bergejolak tinggi)
# - Telur ayam ras: ~Rp 28.500/kg

KOMODITAS = {
    "beras": {
        "base_price": 13_500,
        "unit": "kg",
        "volatility": 0.02,    # ±2% — relatif stabil
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
        "volatility": 0.12,    # ±12% — sangat bergejolak (musiman)
        "min_price": 15_000,
        "max_price": 100_000,
    },
    "bawang_merah": {
        "base_price": 28_000,
        "unit": "kg",
        "volatility": 0.10,    # ±10% — bergejolak
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
    """
    Model harga menggunakan random walk + siklus harian.

    - Random walk: perubahan kecil dari harga sebelumnya
    - Siklus harian: harga cenderung lebih tinggi jam 8-11 (pasar pagi)
      dan sedikit turun sore hari
    - Reversion to mean: jika harga terlalu jauh dari base, ditarik kembali
    """
    meta      = KOMODITAS[commodity]
    base      = meta["base_price"]
    vol       = meta["volatility"]
    prev      = harga_state[commodity]

    # Siklus harian (sinusoidal) — lebih tinggi jam 9 pagi
    siklus    = 0.015 * math.sin((jam_sekarang - 9) * math.pi / 12)

    # Random walk dengan mean reversion
    noise     = random.gauss(0, vol * base * 0.3)
    reversion = (base - prev) * 0.1    # Tarik kembali ke harga base

    harga_baru = prev + noise + reversion + (siklus * base)

    # Clamp ke batas min/max
    harga_baru = max(meta["min_price"], min(meta["max_price"], harga_baru))

    # Bulatkan ke ratusan terdekat (realistis)
    harga_baru = round(harga_baru / 100) * 100

    # Update state
    harga_state[commodity] = harga_baru
    return harga_baru


def buat_payload(commodity: str, harga: float, region: str) -> dict:
    """Buat payload JSON untuk satu record harga komoditas."""
    tz_wib    = timezone(timedelta(hours=7))
    now       = datetime.now(tz_wib)
    prev      = harga_prev[commodity]

    change      = harga - prev
    change_pct  = (change / prev * 100) if prev > 0 else 0.0
    trend       = "naik" if change > 0 else ("turun" if change < 0 else "stabil")

    # Update history
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

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        enable_idempotence=True,
        retries=3,
        linger_ms=10,
        compression_type="lz4",
    )

    siklus = 0

    try:
        while True:
            siklus += 1
            jam = datetime.now().hour

            print(f"\n[Siklus #{siklus} — {datetime.now().strftime('%H:%M:%S')}]")

            for commodity in KOMODITAS:
                # Kirim 1 event per komoditas per siklus, region acak
                region  = random.choice(REGION_LIST)
                harga   = simulasi_harga(commodity, jam)
                payload = buat_payload(commodity, harga, region)

                future = producer.send(
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
        producer.flush()
        producer.close()
        print("🔌 Producer ditutup.")


if __name__ == "__main__":
    main()
