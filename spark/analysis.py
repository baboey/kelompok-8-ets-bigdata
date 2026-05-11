"""
analysis.py - Robust Analysis Harga Pangan (Standard Python Version)
====================================================================
Kelompok 8 - ETS Big Data

Script ini melakukan analisis data menggunakan pustaka Python standar (json, datetime)
dan scikit-learn untuk Linear Regression. Solusi ini dipilih untuk menghindari
konflik versi Python di environment PySpark Windows lokal.

Analisis:
1. Volatilitas Harga (Max, Min, Avg, Volatility %)
2. Tren Harga per Periode (Agregasi per menit)
3. Korelasi Berita vs Harga (Keyword frequency)
4. MLlib-Equivalent: Linear Regression untuk prediksi.
"""

import os, json, sys, math, shutil, subprocess
from datetime import datetime
from collections import Counter, defaultdict
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# --- Path Configuration ---
_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard", "data")
LOCAL_API = os.path.join(_BASE, "live_api.json")
LOCAL_RSS = os.path.join(_BASE, "live_rss.json")
LOCAL_OUT = os.path.join(_BASE, "spark_results.json")

KOMODITAS_LIST = [
    "Beras", "Jagung", "Kedelai", "Gula Pasir",
    "Minyak Goreng", "Cabai Merah", "Bawang Merah", "Telur Ayam",
]

KOMODITAS_BASELINE = {
    "Beras": 13500,
    "Jagung": 6500,
    "Kedelai": 14000,
    "Gula Pasir": 16500,
    "Minyak Goreng": 19000,
    "Cabai Merah": 45000,
    "Bawang Merah": 38000,
    "Telur Ayam": 29000,
}

# Untuk korelasi berita, gunakan keyword yang lebih umum agar match di artikel.
KORELASI_NEWS_KEYWORDS = {
    "Beras": ["beras"],
    "Jagung": ["jagung"],
    "Kedelai": ["kedelai"],
    "Gula Pasir": ["gula", "gula pasir"],
    "Minyak Goreng": ["minyak goreng", "minyak"],
    "Cabai Merah": ["cabai", "cabai merah"],
    "Bawang Merah": ["bawang merah", "bawang"],
    "Telur Ayam": ["telur", "telur ayam"],
}


def _normalize_komoditas(name: str) -> str:
    if not name:
        return ""
    key = str(name).strip().lower()
    key = " ".join(key.split())
    if key == "beras medium":
        return "Beras"
    if key == "beras":
        return "Beras"
    # Untuk komoditas lain, pertahankan Title Case yang sudah ada
    return str(name).strip()

def load_json(path):
    if not os.path.exists(path): return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except: return []


def _parse_hour(ts: str) -> int:
    if not ts:
        return 0
    try:
        # producer_api.py memakai datetime.utcnow().isoformat() (tanpa timezone)
        dt = datetime.fromisoformat(ts)
        return int(dt.hour)
    except Exception:
        return 0


def _train_mllib_equivalent(api_data: list) -> list:
    """Linear Regression (sklearn) dengan split 80/20 urut waktu.

    Output mengikuti schema yang dibutuhkan dashboard (lihat dashboard/templates/index.html).
    """
    by_kom = {k: [] for k in KOMODITAS_LIST}
    for r in api_data:
        kom = _normalize_komoditas(r.get("komoditas"))
        ts = r.get("timestamp")
        harga = r.get("harga")
        if kom in by_kom and ts and harga is not None:
            by_kom[kom].append((str(ts), float(harga)))

    # Urutkan by timestamp agar sequential split benar
    for kom in by_kom:
        by_kom[kom].sort(key=lambda x: x[0])

    hasil = []
    for kom, rows in by_kom.items():
        n = len(rows)
        if n < 5:
            continue

        hours = np.array([_parse_hour(ts) for ts, _ in rows], dtype=float)
        prices = np.array([h for _, h in rows], dtype=float)

        X = np.column_stack([np.arange(n, dtype=float), hours])
        y = prices

        n_train = max(int(n * 0.8), 3)
        n_test = n - n_train
        if n_test < 1:
            n_train = n - 1
            n_test = 1

        X_train, y_train = X[:n_train], y[:n_train]
        X_test, y_test = X[n_train:], y[n_train:]

        # Pipeline: StandardScaler(with_mean=True, with_std=True) -> Ridge(alpha=0.01)
        model = Pipeline([
            ("scaler", StandardScaler(with_mean=True, with_std=True)),
            ("lr", Ridge(alpha=0.01)),
        ])
        model.fit(X_train, y_train)

        pred_train = model.predict(X_train)
        pred_test = model.predict(X_test)

        r2_train = float(r2_score(y_train, pred_train))
        r2_test = float(r2_score(y_test, pred_test))
        rmse_train = float(math.sqrt(mean_squared_error(y_train, pred_train)))
        rmse_test = float(math.sqrt(mean_squared_error(y_test, pred_test)))
        mae_train = float(mean_absolute_error(y_train, pred_train))
        mae_test = float(mean_absolute_error(y_test, pred_test))

        # Koefisien setelah scaling (ekuivalen MLlib script)
        lr = model.named_steps["lr"]
        koef_time = float(lr.coef_[0])
        koef_hour = float(lr.coef_[1])
        intercept = float(lr.intercept_)

        arah = "NAIK" if koef_time > 0 else "TURUN"
        kualitas = (
            "baik - model mampu menjelaskan tren dengan andal"
            if r2_test >= 0.7 else (
                "sedang - ada pola linier namun noise cukup tinggi"
                if r2_test >= 0.4 else
                "rendah - harga sangat fluktuatif dan sulit diprediksi secara linier"
            )
        )
        rekomendasi = (
            "Pantau potensi kenaikan; pertimbangkan pembelian stok lebih awal."
            if arah == "NAIK" else
            "Harga cenderung turun; peluang pembelian stok dengan harga lebih murah."
        )
        interpretasi = (
            f"Harga {kom} cenderung {arah} (koef_waktu={koef_time:.4f}). "
            f"Akurasi model {kualitas}: R2_test={r2_test:.4f}, RMSE_test=Rp{rmse_test:,.0f}. "
            f"{rekomendasi}"
        )

        # Prediksi 5 langkah ke depan
        last_idx = float(n - 1)
        jam_now = int(datetime.now().hour)
        future_X = np.array(
            [[last_idx + step, float((jam_now + step) % 24)] for step in range(1, 6)],
            dtype=float,
        )
        future_pred = model.predict(future_X)
        pred5 = []
        for i, p in enumerate(future_pred, start=1):
            pred5.append({
                "step": i,
                "time_idx": int(last_idx + i),
                "jam": int((jam_now + i) % 24),
                "harga_pred": round(float(p), 0),
            })

        hasil.append({
            "komoditas": kom,
            "n_total": n,
            "n_train": n_train,
            "n_test": n_test,
            "koefisien_waktu": round(koef_time, 6),
            "koefisien_jam": round(koef_hour, 6),
            "intercept": round(intercept, 2),
            "r2_train": round(r2_train, 4),
            "rmse_train": round(rmse_train, 2),
            "mae_train": round(mae_train, 2),
            "r2_test": round(r2_test, 4),
            "rmse_test": round(rmse_test, 2),
            "mae_test": round(mae_test, 2),
            "tren": arah,
            "prediksi_5_step": pred5,
            "interpretasi": interpretasi,
        })

    return hasil


def _upload_results_to_hdfs(local_file: str) -> None:
    """Upload spark_results.json ke HDFS /data/pangan/hasil/spark_results.json.

    Menggunakan docker cp + docker exec ke container namenode.
    """
    if not os.path.exists(local_file):
        return
    if shutil.which("docker") is None:
        print("[HDFS] docker tidak ditemukan, skip upload hasil.")
        return

    try:
        # Pastikan namenode container ada
        chk = subprocess.run(
            ["docker", "ps", "-q", "-f", "name=namenode"],
            capture_output=True, text=True, check=False, timeout=10
        )
        if not (chk.stdout or "").strip():
            print("[HDFS] Container 'namenode' tidak running, skip upload hasil.")
            return

        # Pastikan folder HDFS ada
        subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data/pangan/hasil"],
            check=False, capture_output=True, timeout=15
        )

        # Copy file ke container dan put ke HDFS
        subprocess.run(
            ["docker", "cp", local_file, "namenode:/tmp/spark_results.json"],
            check=True, capture_output=True, timeout=20
        )
        subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", "/tmp/spark_results.json", "/data/pangan/hasil/spark_results.json"],
            check=True, capture_output=True, timeout=20
        )
        print("[HDFS] Hasil analisis diupload ke /data/pangan/hasil/spark_results.json")
    except subprocess.TimeoutExpired as e:
        print(f"[HDFS] Timeout saat upload hasil analisis ke HDFS: {e}")
    except subprocess.CalledProcessError as e:
        stderr = ""
        try:
            stderr = (e.stderr or b"").decode("utf-8", errors="replace").strip()
        except Exception:
            stderr = ""
        if stderr:
            print(f"[HDFS] Gagal upload hasil analisis (stderr): {stderr}")
        print(f"[HDFS] Gagal upload hasil analisis ke HDFS: {e}")
    except Exception as e:
        print(f"[HDFS] Gagal upload hasil analisis ke HDFS: {e}")

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
        kom = _normalize_komoditas(r.get("komoditas"))
        if kom in KOMODITAS_LIST and r.get("harga") is not None:
            stats[kom].append(float(r["harga"]))
    
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

    # --- Analysis 2: Trend per Periode (per menit) ---
    # NOTE: Agar grafik dashboard cepat terlihat saat demo singkat,
    # agregasi dibuat per menit (bukan per jam).
    print("[Analisis 2] Tren Harga per Menit...")
    trend_map = defaultdict(list)
    periods_set = set()
    for r in api_data:
        kom = _normalize_komoditas(r.get("komoditas"))
        if not (kom and r.get("timestamp")):
            continue
        if kom not in KOMODITAS_LIST:
            continue
        # "2026-05-07T11:36:12" -> "2026-05-07 11:36"
        period = r["timestamp"][:16].replace("T", " ")
        periods_set.add(period)
        trend_map[(kom, period)].append(float(r["harga"]))
    
    trend_results = []
    # Forward-fill agar semua komoditas muncul di semua periode yang ada.
    periods_sorted = sorted(periods_set)
    for kom in KOMODITAS_LIST:
        last_price = float(KOMODITAS_BASELINE.get(kom, 0))
        for p in periods_sorted:
            prices = trend_map.get((kom, p))
            if prices:
                last_price = float(sum(prices) / len(prices))
            trend_results.append({
                "komoditas": kom,
                "periode": p,
                "harga_rata": round(last_price, 0)
            })
    trend_results.sort(key=lambda x: (x["periode"], x["komoditas"]))

    # --- Analysis 3: News Correlation ---
    print("[Analisis 3] Korelasi Berita...")
    kor_results = []
    for label, keys in KORELASI_NEWS_KEYWORDS.items():
        keys_lc = [k.lower() for k in (keys or [])]
        freq = sum(
            1
            for art in rss_data
            if any(k in (art.get("summary", "").lower()) for k in keys_lc)
        )
        # Untuk perubahan harga, match komoditas by substring (misal: "beras" match "Beras").
        changes = [
            float(r["perubahan_persen"])
            for r in api_data
            if any(k in r.get("komoditas", "").lower() for k in keys_lc)
            and r.get("perubahan_persen") is not None
        ]
        avg_chg = round(sum(changes)/len(changes), 2) if changes else 0.0
        kor_results.append({
            "komoditas": label,
            "frekuensi_berita": freq,
            "avg_perubahan_persen": avg_chg
        })

    # --- MLlib-Equivalent: Linear Regression (schema lengkap untuk dashboard) ---
    print("[MLlib] Linear Regression (sklearn, split 80/20 urut waktu)...")
    ml_results = _train_mllib_equivalent(api_data)

    # --- Save Final Result ---
    now_iso = datetime.now().isoformat()
    final_output = {
        "generated_at": now_iso,
        "volatilitas": vol_results,
        "tren_harga": trend_results,
        "korelasi_berita": kor_results,
        "prediksi_mlllib": ml_results,
        "mllib_generated_at": now_iso,
    }
    
    with open(LOCAL_OUT, "w", encoding="utf-8") as f:
        json.dump(final_output, f, ensure_ascii=False, indent=2)
    print(f"\n[SUCCESS] Analysis results saved to {LOCAL_OUT}")
    print("=" * 60)

    # Upload hasil ke HDFS (/data/pangan/hasil)
    _upload_results_to_hdfs(LOCAL_OUT)

if __name__ == "__main__":
    run_analysis()