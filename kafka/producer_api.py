# [Anggota2]: Producer Simulator Harga Komoditas Pangan

import json
import time
import random
import math
from datetime import datetime
from kafka import KafkaProducer

# ============================================================
# KONFIGURASI KOMODITAS
# Harga baseline berdasarkan data BULOG / BPS terkini
# ============================================================
KOMODITAS_CONFIG = {
    "Beras Medium":     {"harga_dasar": 13500, "std": 200,  "unit": "kg"},
    "Jagung":           {"harga_dasar": 6500,  "std": 150,  "unit": "kg"},
    "Kedelai":          {"harga_dasar": 14000, "std": 300,  "unit": "kg"},
    "Gula Pasir":       {"harga_dasar": 16500, "std": 250,  "unit": "kg"},
    "Minyak Goreng":    {"harga_dasar": 19000, "std": 400,  "unit": "liter"},
    "Cabai Merah":      {"harga_dasar": 45000, "std": 3000, "unit": "kg"},
    "Bawang Merah":     {"harga_dasar": 38000, "std": 2500, "unit": "kg"},
    "Telur Ayam":       {"harga_dasar": 29000, "std": 500,  "unit": "kg"},
}

# State harga saat ini (untuk random walk)
current_prices = {k: v["harga_dasar"] for k, v in KOMODITAS_CONFIG.items()}
prev_prices = dict(current_prices)

def simulate_price_movement(komoditas):
    """
    Simulasi pergerakan harga menggunakan Random Walk dengan Mean Reversion.
    Harga bergerak acak tapi selalu balik ke baseline (tidak lari ke infinity).
    """
    config = KOMODITAS_CONFIG[komoditas]
    harga_dasar = config["harga_dasar"]
    std = config["std"]
    
    # Random walk component
    random_change = random.gauss(0, std * 0.1)
    
    # Mean reversion: tarik balik ke harga dasar
    current = current_prices[komoditas]
    reversion = (harga_dasar - current) * 0.05
    
    # Update harga
    new_price = current + random_change + reversion
    
    # Pastikan tidak negatif dan tidak terlalu jauh dari baseline
    min_price = harga_dasar * 0.7
    max_price = harga_dasar * 1.5
    new_price = max(min_price, min(max_price, new_price))
    
    return round(new_price, 0)

def calculate_change_pct(old_price, new_price):
    """Hitung persentase perubahan harga"""
    if old_price == 0:
        return 0.0
    return round((new_price - old_price) / old_price * 100, 4)

def build_event(komoditas, harga_baru):
    """Build JSON event untuk dikirim ke Kafka"""
    config = KOMODITAS_CONFIG[komoditas]
    harga_lama = prev_prices[komoditas]
    
    return {
        "komoditas": komoditas,
        "harga": harga_baru,
        "harga_sebelumnya": harga_lama,
        "perubahan_persen": calculate_change_pct(harga_lama, harga_baru),
        "harga_baseline": config["harga_dasar"],
        "unit": config["unit"],
        "timestamp": datetime.utcnow().isoformat(),
        "sumber": "simulator_bulog_baseline",
        "kota": "Nasional"
    }

def main():
    print("=" * 60)
    print("🚀 Producer HargaPangan started")
    print(f"📦 Memantau {len(KOMODITAS_CONFIG)} komoditas")
    print(f"⏱️  Interval: 30 detik")
    print("=" * 60)
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8'),
        enable_idempotence=True,
        acks='all',
        retries=5
    )
    
    iteration = 0
    
    while True:
        iteration += 1
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Iteration #{iteration}")
        
        for komoditas in KOMODITAS_CONFIG.keys():
            # Hitung harga baru
            harga_baru = simulate_price_movement(komoditas)
            
            # Build event
            event = build_event(komoditas, harga_baru)
            
            # Update state
            prev_prices[komoditas] = current_prices[komoditas]
            current_prices[komoditas] = harga_baru
            
            # Kirim ke Kafka
            producer.send(
                topic='pangan-api',
                key=komoditas,
                value=event
            )
            
            # Log
            change = event['perubahan_persen']
            arrow = "▲" if change > 0 else "▼" if change < 0 else "—"
            print(f"  {arrow} {komoditas:20s}: Rp {harga_baru:>8,.0f} ({change:+.2f}%)")
        
        producer.flush()
        print(f"✅ {len(KOMODITAS_CONFIG)} events sent to pangan-api")
        
        # Tunggu 30 detik sebelum update berikutnya
        time.sleep(30)

if __name__ == "__main__":
    main()
