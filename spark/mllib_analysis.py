"""
mllib_analysis.py - MLlib: Prediksi Tren Harga Pangan
=================================================================
Kelompok 8 - ETS Big Data

Implementasi Linear Regression untuk prediksi harga komoditas pangan.
Menggunakan PySpark MLlib (pyspark.ml)

Algoritma:
  - VectorAssembler -> StandardScaler (withMean=True, withStd=True) -> LinearRegression
  - Fitur            : [time_idx, hour_of_day]
  - Label            : harga (Rp)
  - Split            : 80% train / 20% test (urut waktu / sequential)
  - Evaluasi         : R2, RMSE, MAE (train + test set)
  - Output           : koefisien, intercept, prediksi 5 langkah ke depan, interpretasi bisnis

Output : dashboard/data/spark_results.json  (field: prediksi_mlllib)

Cara menjalankan:
    python spark/mllib_analysis.py
    atau:
    spark-submit --master local[*] spark/mllib_analysis.py
"""

import sys, os, json
from datetime import datetime

# FIX: Force Spark to use the same Python version for Driver and Workers
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import numpy as np

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Path ──────────────────────────────────────────────────────────────────────
_BASE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard", "data")
LOCAL_API = os.path.join(_BASE, "live_api.json")
LOCAL_OUT = os.path.join(_BASE, "spark_results.json")

KOMODITAS_LIST = [
    "Beras", "Jagung", "Kedelai", "Gula Pasir",
    "Minyak Goreng", "Cabai Merah", "Bawang Merah", "Telur Ayam",
]
MIN_ROWS = 5


# ── Inisialisasi Spark ────────────────────────────────────────────────────────
def buat_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HargaPangan-MLlib")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


# ── Load Data ─────────────────────────────────────────────────────────────────
def load_data():
    """
    Gabungkan data dari:
    1. live_api.json       - data streaming terkini
    2. spark_results.json  - data historis agregat per jam (tren_harga)
    """
    records = {}  # key: (komoditas, timestamp) -> harga

    # --- live_api.json ---
    if os.path.exists(LOCAL_API):
        try:
            with open(LOCAL_API, "r", encoding="utf-8") as f:
                live = json.load(f)
            for r in (live if isinstance(live, list) else []):
                kom = r.get("komoditas", "")
                ts  = r.get("timestamp", "")
                h   = r.get("harga")
                if kom and ts and h is not None:
                    records[(kom, ts)] = float(h)
            print(f"  [OK] live_api.json: {len(live)} records")
        except Exception as e:
            print(f"  [WARN] live_api.json: {e}")

    # --- spark_results.json: tren_harga ---
    if os.path.exists(LOCAL_OUT):
        try:
            with open(LOCAL_OUT, "r", encoding="utf-8") as f:
                blob = json.load(f)
            tren = blob.get("tren_harga", [])
            n_hist = 0
            for r in tren:
                kom    = r.get("komoditas", "")
                harga  = r.get("harga_rata") or r.get("harga_avg")
                periode = r.get("periode", "")
                if not (kom and harga and periode):
                    continue
                # parse "2026-04-30 10" -> "2026-04-30T10:00:00"
                try:
                    ts = datetime.strptime(periode.strip(), "%Y-%m-%d %H").isoformat()
                except ValueError:
                    ts = periode
                key = (kom, ts)
                if key not in records:  # jangan overwrite live data
                    records[key] = float(harga)
                    n_hist += 1
            print(f"  [OK] spark_results.json tren_harga: {n_hist} records historis")
        except Exception as e:
            print(f"  [WARN] spark_results.json: {e}")

    # --- Susun menjadi dict per komoditas ---
    by_kom = {}
    for (kom, ts), harga in records.items():
        by_kom.setdefault(kom, []).append((ts, harga))

    # Urutkan per komoditas by timestamp
    for kom in by_kom:
        by_kom[kom].sort(key=lambda x: x[0])

    total = sum(len(v) for v in by_kom.values())
    print(f"  [OK] Total gabungan: {total} records dari {len(by_kom)} komoditas")
    return by_kom


# ── Feature Engineering ───────────────────────────────────────────────────────
def buat_rows(data_list):
    """
    Input: [(timestamp_str, harga), ...]  sudah terurut by time
    Output: list of Row(time_idx, hour_of_day, harga)
    """
    rows = []
    for i, (ts, harga) in enumerate(data_list):
        try:
            dt  = datetime.fromisoformat(ts)
            jam = float(dt.hour)
        except Exception:
            jam = 0.0
        rows.append(Row(time_idx=float(i), hour_of_day=jam, harga=float(harga)))
    return rows


# ── Training ──────────────────────────────────────────────────────────────────
def latih(spark: SparkSession, nama: str, data_list: list):
    """
    PySpark MLlib Linear Regression:
      VectorAssembler -> StandardScaler (withMean=True, withStd=True) -> LinearRegression
    Sequential 80/20 split (bukan random) untuk menjaga urutan waktu.
    """
    n = len(data_list)
    print(f"\n  [TRAIN] {nama} — n={n}")

    # Buat Spark DataFrame dari list of Row
    schema = StructType([
        StructField("time_idx",    DoubleType(), False),
        StructField("hour_of_day", DoubleType(), False),
        StructField("harga",       DoubleType(), False),
    ])
    rows = buat_rows(data_list)
    df   = spark.createDataFrame(rows, schema=schema)

    # Sequential split 80/20 (urutan waktu dipertahankan via time_idx)
    n_train = max(int(n * 0.8), 3)
    n_test  = n - n_train
    if n_test < 1:
        n_train = n - 1
        n_test  = 1

    df_train = df.filter(F.col("time_idx") < float(n_train))
    df_test  = df.filter(F.col("time_idx") >= float(n_train))

    # ── Pipeline MLlib ────────────────────────────────────────────────────────
    # Tahap 1: VectorAssembler — gabungkan fitur menjadi satu kolom vektor
    assembler = VectorAssembler(
        inputCols=["time_idx", "hour_of_day"],
        outputCol="features_raw"
    )

    # Tahap 2: StandardScaler — normalisasi (withMean=True, withStd=True)
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    # Tahap 3: LinearRegression (ekuivalen Ridge alpha=0.01 -> regParam=0.01, elasticNetParam=0.0)
    lr = LinearRegression(
        featuresCol="features",
        labelCol="harga",
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.0,   # 0.0 = Ridge (L2), 1.0 = Lasso (L1)
    )

    pipeline = Pipeline(stages=[assembler, scaler, lr])
    model    = pipeline.fit(df_train)

    # Ambil model LinearRegression dari pipeline
    lr_model   = model.stages[-1]
    koef       = lr_model.coefficients  # DenseVector([koef_time, koef_hour])
    koef_time  = float(koef[0])
    koef_hour  = float(koef[1])
    intercept  = float(lr_model.intercept)

    # ── Evaluasi ──────────────────────────────────────────────────────────────
    evaluator_r2   = RegressionEvaluator(labelCol="harga", predictionCol="prediction", metricName="r2")
    evaluator_rmse = RegressionEvaluator(labelCol="harga", predictionCol="prediction", metricName="rmse")
    evaluator_mae  = RegressionEvaluator(labelCol="harga", predictionCol="prediction", metricName="mae")

    # Metrik training
    pred_train = model.transform(df_train)
    r2_train   = float(evaluator_r2.evaluate(pred_train))
    rmse_train = float(evaluator_rmse.evaluate(pred_train))
    mae_train  = float(evaluator_mae.evaluate(pred_train))

    # Metrik test
    pred_test  = model.transform(df_test)
    r2_test    = float(evaluator_r2.evaluate(pred_test))
    rmse_test  = float(evaluator_rmse.evaluate(pred_test))
    mae_test   = float(evaluator_mae.evaluate(pred_test))

    print(f"    Train  R2={r2_train:.4f}  RMSE=Rp{rmse_train:,.0f}  MAE=Rp{mae_train:,.0f}")
    print(f"    Test   R2={r2_test:.4f}  RMSE=Rp{rmse_test:,.0f}  MAE=Rp{mae_test:,.0f}")

    # ── Prediksi 5 langkah ke depan ───────────────────────────────────────────
    last_idx = float(n - 1)
    jam_now  = float(datetime.now().hour)

    future_rows = [
        Row(time_idx=last_idx + float(step), hour_of_day=float((int(jam_now) + step) % 24), harga=0.0)
        for step in range(1, 6)
    ]
    df_future = spark.createDataFrame(future_rows, schema=schema)
    pred_future = model.transform(df_future).select(
        "time_idx", "hour_of_day", "prediction"
    ).collect()

    pred5 = []
    for step, row in enumerate(pred_future, start=1):
        pred5.append({
            "step":       step,
            "time_idx":   int(row["time_idx"]),
            "jam":        int(row["hour_of_day"]),
            "harga_pred": round(float(row["prediction"]), 0),
        })

    # ── Interpretasi bisnis ───────────────────────────────────────────────────
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
        f"Harga {nama} cenderung {arah} (koef_waktu={koef_time:.4f}). "
        f"Akurasi model {kualitas}: R2_test={r2_test:.4f}, RMSE_test=Rp{rmse_test:,.0f}. "
        f"{rekomendasi}"
    )
    print(f"    Tren={arah} | {interpretasi[:90]}...")

    return {
        "komoditas":       nama,
        "n_total":         n,
        "n_train":         n_train,
        "n_test":          n_test,
        # Koefisien & Intercept
        "koefisien_waktu": round(koef_time, 6),
        "koefisien_jam":   round(koef_hour, 6),
        "intercept":       round(intercept, 2),
        # Metrik Training
        "r2_train":        round(r2_train, 4),
        "rmse_train":      round(rmse_train, 2),
        "mae_train":       round(mae_train, 2),
        # Metrik Test
        "r2_test":         round(r2_test, 4),
        "rmse_test":       round(rmse_test, 2),
        "mae_test":        round(mae_test, 2),
        # Prediksi & Interpretasi
        "tren":            arah,
        "prediksi_5_step": pred5,
        "interpretasi":    interpretasi,
    }


# ── Update spark_results.json ─────────────────────────────────────────────────
def update_spark_results(hasil_list):
    existing = {}
    if os.path.exists(LOCAL_OUT):
        try:
            with open(LOCAL_OUT, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    existing["prediksi_mlllib"]    = hasil_list
    existing["mllib_generated_at"] = datetime.now().isoformat()

    os.makedirs(os.path.dirname(LOCAL_OUT), exist_ok=True)
    with open(LOCAL_OUT, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    print(f"\n  [OK] spark_results.json diperbarui: {LOCAL_OUT}")


# ── Ringkasan ─────────────────────────────────────────────────────────────────
def ringkasan(hasil_list):
    print("\n" + "=" * 72)
    print("  RINGKASAN HASIL MLlib - HargaPangan Linear Regression")
    print("=" * 72)
    print(f"  {'Komoditas':<18} {'n':>5} {'R2_train':>9} {'R2_test':>8} {'RMSE_test':>12} {'Tren':>6}")
    print("  " + "-" * 62)
    for h in hasil_list:
        print(
            f"  {h['komoditas']:<18} {h['n_total']:>5} "
            f"{h['r2_train']:>9.4f} {h['r2_test']:>8.4f} "
            f"Rp{h['rmse_test']:>10,.0f} {h['tren']:>6}"
        )
    print("=" * 72)

    best  = max(hasil_list, key=lambda x: x["r2_test"])
    worst = min(hasil_list, key=lambda x: x["r2_test"])
    print(f"  [Terbaik]  {best['komoditas']}  (R2={best['r2_test']})")
    print(f"  [Tersulit] {worst['komoditas']}  (R2={worst['r2_test']})")

    naik  = [h["komoditas"] for h in hasil_list if h["tren"] == "NAIK"]
    turun = [h["komoditas"] for h in hasil_list if h["tren"] == "TURUN"]
    print(f"  Tren NAIK  : {', '.join(naik)  or '-'}")
    print(f"  Tren TURUN : {', '.join(turun) or '-'}")
    print()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 72)
    print("  [MLlib] HargaPangan - Linear Regression Prediksi Tren Harga")
    print("  Metode: VectorAssembler -> StandardScaler -> LinearRegression")
    print("=" * 72)

    spark = buat_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n[1] Load & Gabungkan Data...")
    by_kom = load_data()

    print("\n[2] Training Model per Komoditas...")
    hasil_list = []
    gagal_list = []

    for nama in KOMODITAS_LIST:
        data = by_kom.get(nama, [])
        n    = len(data)
        if n < MIN_ROWS:
            print(f"  [SKIP] [{nama}] data={n} < {MIN_ROWS}, dilewati")
            gagal_list.append(nama)
            continue

        hasil = latih(spark, nama, data)
        if hasil:
            hasil_list.append(hasil)
        else:
            gagal_list.append(nama)

    if not hasil_list:
        print("\n  [ERROR] Tidak ada model berhasil dilatih!")
        spark.stop()
        return

    ringkasan(hasil_list)

    print("[3] Menyimpan Hasil ke spark_results.json...")
    update_spark_results(hasil_list)

    if gagal_list:
        print(f"  [WARN] Dilewati (data kurang): {', '.join(gagal_list)}")

    spark.stop()
    print("\n[DONE] MLlib Analysis selesai!\n")


if __name__ == "__main__":
    main()