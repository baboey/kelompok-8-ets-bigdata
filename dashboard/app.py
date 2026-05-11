"""
app.py — Flask Dashboard Backend
=================================
Anggota : Rayka
Port    : 5000
Data    : dashboard/data/*.json

Endpoints:
    GET /           → render index.html
    GET /api/prices → live_api.json (harga terkini)
    GET /api/news   → live_rss.json (berita terbaru)
    GET /api/spark  → spark_results.json (hasil analisis)
    GET /api/data   → gabungan semua data (untuk polling)
    GET /api/status → status sistem
"""

import json, os
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, render_template
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Izinkan CORS untuk development

# Path file data
DATA_DIR      = os.path.join(os.path.dirname(__file__), "data")
FILE_API      = os.path.join(DATA_DIR, "live_api.json")
FILE_RSS      = os.path.join(DATA_DIR, "live_rss.json")
FILE_SPARK    = os.path.join(DATA_DIR, "spark_results.json")


# Baseline komoditas (untuk memastikan 8 komoditas selalu tampil sejak awal)
_KOMODITAS_ORDER = [
    "Bawang Merah",
    "Beras",
    "Cabai Merah",
    "Gula Pasir",
    "Jagung",
    "Kedelai",
    "Minyak Goreng",
    "Telur Ayam",
]

_KOMODITAS_BASELINE = {
    "Beras": 13500,
    "Jagung": 6500,
    "Kedelai": 14000,
    "Gula Pasir": 16500,
    "Minyak Goreng": 19000,
    "Cabai Merah": 45000,
    "Bawang Merah": 38000,
    "Telur Ayam": 29000,
}


_KOMODITAS_CANONICAL = {c.lower(): c for c in _KOMODITAS_ORDER}
_KOMODITAS_ALIASES = {
    "beras medium": "Beras",
    "beras": "Beras",
}


def _normalize_komoditas(name: str) -> str:
    if not name:
        return ""
    key = str(name).strip().lower()
    key = " ".join(key.split())
    key = _KOMODITAS_ALIASES.get(key, key)
    canonical = _KOMODITAS_CANONICAL.get(str(key).lower())
    return canonical if canonical else str(name).strip()


def baca_json(filepath: str, default=None):
    """Baca file JSON dengan fallback jika file belum ada."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default if default is not None else []


def harga_terkini(data_list: list) -> dict:
    """
    Dari list events harga, ambil harga terkini per komoditas.
    Return dict: {komoditas: {harga, perubahan_persen, kota, timestamp}}
    """
    # Seed default agar 8 komoditas selalu muncul di UI.
    # Akan ditimpa ketika data real dari Kafka consumer sudah masuk.
    terkini = {}
    for com in _KOMODITAS_ORDER:
        baseline = _KOMODITAS_BASELINE.get(com, 0)
        terkini[com] = {
            "harga": baseline,
            "perubahan_persen": 0.0,
            "trend": "➡️ stabil",
            "kota": "Nasional",
            "timestamp": "",
            "harga_baseline": baseline,
        }

    for item in data_list:
        com = _normalize_komoditas(item.get("komoditas", ""))
        if com:
            # Tentukan trend berdasarkan perubahan_persen
            pct = item.get("perubahan_persen", 0)
            trend = "📈 naik" if pct > 0.3 else ("📉 turun" if pct < -0.3 else "➡️ stabil")
            
            terkini[com] = {
                "harga":                item.get("harga", 0),
                "perubahan_persen":     item.get("perubahan_persen", 0.0),
                "trend":                trend,
                "kota":                 item.get("kota", ""),
                "timestamp":            item.get("timestamp", ""),
                "harga_baseline":       item.get("harga_baseline", _KOMODITAS_BASELINE.get(com, 0)),
            }
    return terkini


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Halaman utama dashboard."""
    return render_template("index.html")


@app.route("/api/prices")
def api_prices():
    """Data harga live dari Kafka consumer (50 event terakhir)."""
    data = baca_json(FILE_API, default=[])
    data_norm = [
        {
            **item,
            "komoditas": _normalize_komoditas(item.get("komoditas", "")) or item.get("komoditas", ""),
        }
        for item in (data or [])
        if isinstance(item, dict)
    ]
    return jsonify({
        "status":   "ok",
        "count":    len(data_norm),
        "data":     data_norm,
        "terkini":  harga_terkini(data_norm),
        "updated":  datetime.now(timezone(timedelta(hours=7))).isoformat(),
    })


@app.route("/api/news")
def api_news():
    """Data berita RSS terbaru (20 artikel terakhir)."""
    data = baca_json(FILE_RSS, default=[])
    return jsonify({
        "status":   "ok",
        "count":    len(data),
        "articles": data,
        "updated":  datetime.now(timezone(timedelta(hours=7))).isoformat(),
    })


@app.route("/api/spark")
def api_spark():
    """Hasil analisis Spark (volatilitas, tren, korelasi, prediksi)."""
    data = baca_json(FILE_SPARK, default={})
    return jsonify({
        "status": "ok",
        "data":   data,
    })


@app.route("/api/data")
def api_data():
    """Endpoint gabungan — dipanggil setiap 30 detik oleh dashboard."""
    prices_raw  = baca_json(FILE_API,   default=[])
    news_raw    = baca_json(FILE_RSS,   default=[])
    spark_raw   = baca_json(FILE_SPARK, default={})

    prices_norm = [
        {
            **item,
            "komoditas": _normalize_komoditas(item.get("komoditas", "")) or item.get("komoditas", ""),
        }
        for item in (prices_raw or [])
        if isinstance(item, dict)
    ]

    return jsonify({
        "status":       "ok",
        "updated":      datetime.now(timezone(timedelta(hours=7))).isoformat(),
        "prices": {
            "live":     prices_norm[-50:] if prices_norm else [],
            "terkini":  harga_terkini(prices_norm),
        },
        "news":         news_raw[-20:] if news_raw else [],
        "spark":        spark_raw,
    })


@app.route("/api/status")
def api_status():
    """Status ketersediaan file data."""
    tz_wib = timezone(timedelta(hours=7))

    def file_info(path: str) -> dict:
        if os.path.exists(path):
            mtime = os.path.getmtime(path)
            size  = os.path.getsize(path)
            return {
                "exists":   True,
                "size_kb":  round(size / 1024, 1),
                "modified": datetime.fromtimestamp(mtime, tz_wib).isoformat(),
            }
        return {"exists": False}

    return jsonify({
        "status":   "running",
        "server":   datetime.now(tz_wib).isoformat(),
        "files": {
            "live_api":      file_info(FILE_API),
            "live_rss":      file_info(FILE_RSS),
            "spark_results": file_info(FILE_SPARK),
        }
    })


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    os.makedirs(DATA_DIR, exist_ok=True)
    print("=" * 50)
    print("  HargaPangan Dashboard")
    print("  http://localhost:5000")
    print("=" * 50)
    app.run(debug=True, host="0.0.0.0", port=5000)
