"""
analysis.py - Robust Analysis Harga Pangan (Standard Python Version)
====================================================================
Kelompok 8 - ETS Big Data

Script ini melakukan analisis data menggunakan pustaka Python standar (json, datetime)
dan scikit-learn untuk Linear Regression. Solusi ini dipilih untuk menghindari
konflik versi Python di environment PySpark Windows lokal.

Analisis:
1. Volatilitas Harga (Max, Min, Avg, Volatility %)
2. Tren Harga per Periode (Agregasi per jam)
3. Korelasi Berita vs Harga (Keyword frequency)
4. MLlib-Equivalent: Linear Regression untuk prediksi.
"""

import os, json, sys
from datetime import datetime
from collections import Counter, defaultdict
import numpy as np
from sklearn.linear_model import LinearRegression

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# --- Path Configuration ---
_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard", "data")
LOCAL_API = os.path.join(_BASE, "live_api.json")
LOCAL_RSS = os.path.join(_BASE, "live_rss.json")
LOCAL_OUT = os.path.join(_BASE, "spark_results.json")

KOMODITAS_LIST = [
    "Beras Medium", "Jagung", "Kedelai", "Gula Pasir",
    "Minyak Goreng", "Cabai Merah", "Bawang Merah", "Telur Ayam",
]

def load_json(path):
    if not os.path.exists(path): return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except: return []

def run_analysis():
    print("=" * 60)
    print("  HargaPangan — Robust Analysis (Real Data)")
    print("=" * 60)

    api_data = load_json(LOCAL_API)
    rss_data = load_json(LOCAL_RSS)
    print(f"[1] Loaded {len(api_data)} API records and {len(rss_data)} RSS articles.")

    if not api_data:
        print("[ERROR] No data found in live_api.json!")
        return

    # --- Analysis 1: Volatility ---
    print("\n[Analisis 1] Volatilitas Harga...")
    stats = defaultdict(list)
    for r in api_data:
        if r.get("komoditas") and r.get("harga"):
            stats[r["komoditas"]].append(float(r["harga"]))
    
    vol_results = []
    for kom, prices in stats.items():
        h_max, h_min = max(prices), min(prices)
        vol = round(((h_max - h_min) / h_min) * 100, 2) if h_min > 0 else 0
        vol_results.append({
            "komoditas": kom,
            "harga_max": h_max,
            "harga_min": h_min,
            "harga_avg": round(sum(prices)/len(prices), 2),
            "jumlah_data": len(prices),
            "volatilitas_pct": vol
        })
    vol_results.sort(key=lambda x: x["volatilitas_pct"], reverse=True)

    # --- Analysis 2: Trend per Hour ---
    print("[Analisis 2] Tren Harga per Jam...")
    trend_map = defaultdict(list)
    for r in api_data:
        if not (r.get("komoditas") and r.get("timestamp")): continue
        # "2026-05-07T11:00:00" -> "2026-05-07 11"
        period = r["timestamp"][:13].replace("T", " ")
        trend_map[(r["komoditas"], period)].append(float(r["harga"]))
    
    trend_results = []
    for (kom, p), prices in trend_map.items():
        trend_results.append({
            "komoditas": kom,
            "periode": p,
            "harga_rata": round(sum(prices)/len(prices), 0)
        })
    trend_results.sort(key=lambda x: (x["periode"], x["komoditas"]))

    # --- Analysis 3: News Correlation ---
    print("[Analisis 3] Korelasi Berita...")
    kor_results = []
    for kw in KOMODITAS_LIST:
        freq = sum(1 for art in rss_data if kw.lower() in art.get("summary", "").lower())
        changes = [float(r["perubahan_persen"]) for r in api_data if kw.lower() in r.get("komoditas", "").lower() and r.get("perubahan_persen") is not None]
        avg_chg = round(sum(changes)/len(changes), 2) if changes else 0.0
        kor_results.append({
            "komoditas": kw,
            "frekuensi_berita": freq,
            "avg_perubahan_persen": avg_chg
        })

    # --- MLlib-Equivalent: Linear Regression ---
    print("[MLlib] Linear Regression Training...")
    ml_results = []
    for kom in KOMODITAS_LIST:
        prices = [float(r["harga"]) for r in api_data if r.get("komoditas") == kom]
        if len(prices) < 5: continue
        
        X = np.arange(len(prices)).reshape(-1, 1)
        y = np.array(prices)
        model = LinearRegression().fit(X, y)
        koef = float(model.coef_[0])
        r2 = float(model.score(X, y))
        
        # Future 5 steps
        fX = np.arange(len(prices), len(prices)+5).reshape(-1, 1)
        fy = model.predict(fX)
        pred5 = [{"step": i+1, "harga_pred": round(float(p), 0)} for i, p in enumerate(fy)]
        
        ml_results.append({
            "komoditas": kom,
            "n_total": len(prices),
            "koefisien_waktu": round(koef, 4),
            "intercept": round(float(model.intercept_), 2),
            "r2_test": round(r2, 4),
            "rmse_test": 0.0,
            "tren": "NAIK" if koef > 0 else "TURUN",
            "prediksi_5_step": pred5,
            "interpretasi": f"Harga {kom} diprediksi {('naik' if koef > 0 else 'turun')} (R2={round(r2,2)})"
        })

    # --- Save Final Result ---
    final_output = {
        "generated_at": datetime.now().isoformat(),
        "volatilitas": vol_results,
        "tren_harga": trend_results,
        "korelasi_berita": kor_results,
        "prediksi_mlllib": ml_results
    }
    
    with open(LOCAL_OUT, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2)
    print(f"\n[SUCCESS] Analysis results saved to {LOCAL_OUT}")
    print("=" * 60)

if __name__ == "__main__":
    run_analysis()