"""
analysis.py — Spark Analysis untuk HargaPangan Pipeline
========================================================
Anggota : Evan
Input   : HDFS /data/pangan/api/*.json dan /data/pangan/rss/*.json
Output  : HDFS /data/pangan/hasil/ + dashboard/data/spark_results.json

3 Analisis Wajib:
    1. Volatilitas Harga per Komoditas (DataFrame API)
    2. Rata-rata Harga per Periode (Spark SQL)
    3. Sebutan Komoditas di Berita RSS (cross-reference)

Bonus (+5 poin):
    4. Linear Regression prediksi tren harga (MLlib)

Cara menjalankan:
    spark-submit --master local[*] spark/analysis.py
    atau: python spark/analysis.py  (jika PySpark ter-install)
"""

import os, json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

# ─── Konfigurasi ─────────────────────────────────────────────────────────────

HDFS_BASE       = "hdfs://namenode:8020"
HDFS_API_PATH   = f"{HDFS_BASE}/data/pangan/api"
HDFS_RSS_PATH   = f"{HDFS_BASE}/data/pangan/rss"
HDFS_OUT_PATH   = f"{HDFS_BASE}/data/pangan/hasil"

# Fallback path lokal (jika HDFS tidak tersedia)
LOCAL_BASE      = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data")
LOCAL_API_FILE  = os.path.join(LOCAL_BASE, "live_api.json")
LOCAL_RSS_FILE  = os.path.join(LOCAL_BASE, "live_rss.json")
LOCAL_OUT_FILE  = os.path.join(LOCAL_BASE, "spark_results.json")

# Daftar komoditas untuk cross-reference dengan RSS
KOMODITAS_LIST = [
    "beras", "jagung", "kedelai", "gula", "minyak",
    "cabai", "bawang", "telur", "daging",
]


# ─── Inisialisasi Spark ───────────────────────────────────────────────────────

def buat_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HargaPangan-Analysis")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ─── Load Data ────────────────────────────────────────────────────────────────

def load_data_api(spark: SparkSession):
    """Load data harga dari HDFS atau fallback file lokal."""
    schema = StructType([
        StructField("komoditas",   StringType(),  True),
        StructField("harga",       IntegerType(), True),
        StructField("harga_sebelumnya", IntegerType(), True),
        StructField("perubahan_persen",  DoubleType(),  True),
        StructField("harga_baseline", IntegerType(), True),
        StructField("unit",        StringType(),  True),
        StructField("timestamp",   StringType(),  True),
        StructField("sumber",      StringType(),  True),
        StructField("kota",      StringType(),  True),
    ])

    # Coba HDFS dulu
    try:
        df = spark.read.option("multiLine", False).json(HDFS_API_PATH, schema=schema)
        if df.count() > 0:
            print(f"  ✅ Data API dimuat dari HDFS: {df.count()} records")
            return df
    except Exception as e:
        print(f"  ⚠️  HDFS tidak tersedia: {e}")

    # Fallback: baca local JSON
    print(f"  📁 Fallback: membaca {LOCAL_API_FILE}")
    df = spark.read.json(LOCAL_API_FILE, schema=schema)
    print(f"  ✅ Data API lokal: {df.count()} records")
    return df


def load_data_rss(spark: SparkSession):
    """Load data berita RSS dari HDFS atau fallback file lokal."""
    try:
        df = spark.read.option("multiLine", False).json(HDFS_RSS_PATH)
        if df.count() > 0:
            return df
    except Exception:
        pass
    return spark.read.option("multiLine", False).json(LOCAL_RSS_FILE)


# ─── Analisis 1: Volatilitas Harga ───────────────────────────────────────────

def analisis_volatilitas(df_api):
    """
    Analisis 1 (DataFrame API): Hitung volatilitas harga per komoditas.
    Volatilitas = (max_price - min_price) / avg_price * 100
    """
    print("\n[Analisis 1] Volatilitas Harga per Komoditas")

    hasil = (
        df_api
        .groupBy("komoditas")
        .agg(
            F.max("harga").alias("harga_max"),
            F.min("harga").alias("harga_min"),
            F.avg("harga").alias("harga_avg"),
            F.count("harga").alias("jumlah_data"),
        )
        .withColumn(
            "volatilitas_pct",
            F.round(
                (F.col("harga_max") - F.col("harga_min")) / F.col("harga_avg") * 100,
                2
            )
        )
        .orderBy(F.desc("volatilitas_pct"))
    )

    hasil.show(truncate=False)
    return hasil


# ─── Analisis 2: Rata-rata Harga per Periode ─────────────────────────────────

def analisis_tren_harga(spark: SparkSession, df_api):
    """
    Analisis 2 (Spark SQL): Rata-rata harga per komoditas per jam.
    Menunjukkan tren harga sepanjang hari.
    """
    print("\n[Analisis 2] Rata-rata Harga per Komoditas per Jam")

    df_api.createOrReplaceTempView("harga_pangan")

    hasil = spark.sql("""
        SELECT
            komoditas,
            SUBSTRING(timestamp, 1, 13) AS periode,
            CAST(AVG(harga) AS INT)      AS harga_rata,
            MIN(harga)                   AS harga_min,
            MAX(harga)                   AS harga_max,
            COUNT(*)                     AS jumlah_data
        FROM harga_pangan
        WHERE harga IS NOT NULL
        GROUP BY komoditas, SUBSTRING(timestamp, 1, 13)
        ORDER BY komoditas, periode
    """)

    hasil.show(50, truncate=False)
    return hasil


# ─── Analisis 3: Sebutan Komoditas di Berita ──────────────────────────────────

def analisis_korelasi_berita(spark: SparkSession, df_api, df_rss):
    """
    Analisis 3: Hitung kemunculan nama komoditas dalam judul berita RSS.
    Cross-reference dengan rata-rata perubahan harga di periode yang sama.
    """
    print("\n[Analisis 3] Sebutan Komoditas di Berita RSS")

    # Hitung frekuensi sebutan per komoditas
    rows = []
    total_berita = df_rss.count()

    for kw in KOMODITAS_LIST:
        jumlah = df_rss.filter(
            F.lower(F.col("title")).contains(kw) |
            F.lower(F.col("summary")).contains(kw)
        ).count()

        # Rata-rata change_pct untuk komoditas ini
        avg_change = df_api.filter(F.col("komoditas").contains(kw)) \
                           .agg(F.avg("perubahan_persen")) \
                           .collect()[0][0]

        rows.append({
            "komoditas":    kw,
            "frekuensi_berita": jumlah,
            "avg_perubahan_persen":   round(avg_change, 2) if avg_change else 0.0,
        })

    from pyspark.sql import Row
    hasil = spark.createDataFrame([Row(**r) for r in rows]) \
                 .orderBy(F.desc("frekuensi_berita"))

    hasil.show(truncate=False)
    return hasil, rows


# ─── Bonus: MLlib Linear Regression ──────────────────────────────────────────

def analisis_prediksi_mlllib(spark: SparkSession, df_api):
    """
    Bonus (+5 poin): Prediksi tren harga menggunakan Linear Regression MLlib.
    Feature: index waktu (urutan pengamatan)
    Target : harga
    """
    print("\n[Bonus MLlib] Linear Regression — Prediksi Tren Harga Beras")

    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml import Pipeline

    # Filter beras saja sebagai contoh
    df_beras = (
        df_api
        .filter(F.lower(F.col("komoditas")).contains("beras"))
        .orderBy("timestamp")
        .withColumn("idx", F.monotonically_increasing_id().cast("double"))
        .select("idx", F.col("harga").cast("double").alias("price"))
        .na.drop()
    )

    if df_beras.count() < 5:
        print("  ⚠️  Data beras tidak cukup untuk MLlib (min 5 records)")
        return None

    assembler = VectorAssembler(inputCols=["idx"], outputCol="features")
    lr        = LinearRegression(
        featuresCol="features",
        labelCol="price",
        maxIter=50,
        regParam=0.1,
    )

    pipeline = Pipeline(stages=[assembler, lr])
    model    = pipeline.fit(df_beras)
    lr_model = model.stages[-1]

    print(f"  📈 Koefisien: {lr_model.coefficients[0]:.4f}")
    print(f"  📈 Intercept: {lr_model.intercept:.2f}")
    print(f"  📈 R²       : {lr_model.summary.r2:.4f}")
    print(f"  📈 RMSE     : {lr_model.summary.rootMeanSquaredError:.2f}")

    return {
        "komoditas":    "beras",
        "koefisien":    round(lr_model.coefficients[0], 4),
        "intercept":    round(lr_model.intercept, 2),
        "r2":           round(lr_model.summary.r2, 4),
        "rmse":         round(lr_model.summary.rootMeanSquaredError, 2),
        "interpretasi": "Koefisien > 0 berarti tren harga naik per satuan waktu",
    }


# ─── Simpan Hasil ─────────────────────────────────────────────────────────────

def simpan_hasil(spark, df_volatilitas, df_tren, korelasi_rows, prediksi):
    """Simpan semua hasil ke HDFS dan local JSON untuk dashboard."""
    print("\n[Simpan Hasil]")

    # Simpan ke HDFS (Parquet + JSON)
    for df, nama in [
        (df_volatilitas, "volatilitas"),
        (df_tren,        "tren_harga"),
    ]:
        try:
            path = f"{HDFS_OUT_PATH}/{nama}"
            df.coalesce(1).write.mode("overwrite").json(path)
            print(f"  ✅ HDFS: {path}")
        except Exception as e:
            print(f"  ⚠️  Gagal simpan {nama} ke HDFS: {e}")

    # Kumpulkan hasil ke satu dict untuk dashboard
    hasil_dashboard = {
        "generated_at":  datetime.now().isoformat(),
        "volatilitas":   [
            {
                "komoditas":      r["komoditas"],
                "harga_max":      r["harga_max"],
                "harga_min":      r["harga_min"],
                "harga_avg":      round(r["harga_avg"]),
                "volatilitas_pct": r["volatilitas_pct"],
                "jumlah_data":    r["jumlah_data"],
            }
            for r in df_volatilitas.collect()
        ],
        "tren_harga":    [row.asDict() for row in df_tren.collect()],
        "korelasi_berita": korelasi_rows,
        "prediksi_mlllib": prediksi,
    }

    # Simpan local JSON
    os.makedirs(os.path.dirname(LOCAL_OUT_FILE), exist_ok=True)
    with open(LOCAL_OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(hasil_dashboard, f, ensure_ascii=False, indent=2)
    print(f"  ✅ Lokal: {LOCAL_OUT_FILE}")

    return hasil_dashboard


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  🔥 HargaPangan — Spark Analysis")
    print("  3 Analisis Wajib + Bonus MLlib")
    print("=" * 60)

    spark = buat_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Load data
    df_api = load_data_api(spark)
    df_rss = load_data_rss(spark)

    # Jalankan analisis
    df_volatilitas         = analisis_volatilitas(df_api)
    df_tren                = analisis_tren_harga(spark, df_api)
    df_korelasi, kor_rows  = analisis_korelasi_berita(spark, df_api, df_rss)
    prediksi               = analisis_prediksi_mlllib(spark, df_api)

    # Simpan hasil
    simpan_hasil(spark, df_volatilitas, df_tren, kor_rows, prediksi)

    spark.stop()
    print("\n✅ Spark Analysis selesai!")


if __name__ == "__main__":
    main()
