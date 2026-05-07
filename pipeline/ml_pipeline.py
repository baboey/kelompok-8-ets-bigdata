import pandas as pd
import numpy as np
import os
import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import accuracy_score, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
import warnings
import json
from datetime import datetime

# Suppress sklearn warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning)

ML_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
os.makedirs(ML_DIR, exist_ok=True)

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data")
ML_STATS_FILE = os.path.join(DASHBOARD_DIR, "ml_stats.json")
os.makedirs(DASHBOARD_DIR, exist_ok=True)

ml_global_stats = {
    "api_rmse": 0,
    "api_r2": 0,
    "api_samples": 0,
    "rss_acc": 0,
    "rss_samples": 0,
    "last_updated": ""
}

def update_ml_stats():
    try:
        ml_global_stats["last_updated"] = datetime.now().isoformat()
        with open(ML_STATS_FILE, "w") as f:
            json.dump(ml_global_stats, f)
    except Exception:
        pass

class RSSNewsClassifier:
    """
    Classification model for RSS News Topics/Sentiment.
    Uses TF-IDF for text preprocessing and Logistic Regression.
    """
    def __init__(self):
        self.vectorizer = TfidfVectorizer(stop_words='english', max_features=500)
        self.model = LogisticRegression(random_state=42)
        self.is_trained = False
        self.model_path = os.path.join(ML_DIR, "rss_classifier.pkl")
        self.vectorizer_path = os.path.join(ML_DIR, "rss_vectorizer.pkl")
        self.data_buffer = pd.DataFrame()
        self.load_model()

    def load_model(self):
        if os.path.exists(self.model_path) and os.path.exists(self.vectorizer_path):
            self.model = joblib.load(self.model_path)
            self.vectorizer = joblib.load(self.vectorizer_path)
            self.is_trained = True

    def save_model(self):
        joblib.dump(self.model, self.model_path)
        joblib.dump(self.vectorizer, self.vectorizer_path)

    def retrain(self, df_new):
        """Accumulate new streaming data and retrain the model."""
        if df_new.empty or 'cleaned_title' not in df_new.columns or 'sentiment' not in df_new.columns:
            return None
            
        self.data_buffer = pd.concat([self.data_buffer, df_new]).drop_duplicates().tail(1000)
        
        # Need a minimum amount of data to train
        if len(self.data_buffer) < 20: 
            return None
            
        X_text = self.data_buffer['cleaned_title'].fillna('')
        y = self.data_buffer['sentiment']
        
        # Ensure we have at least 2 classes to perform classification
        if len(y.unique()) < 2:
            return None
            
        # Feature Extraction: TF-IDF Vectorization
        X = self.vectorizer.fit_transform(X_text)
        
        # Train-test split for evaluation metrics
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train Model
        self.model.fit(X_train, y_train)
        self.is_trained = True
        self.save_model()
        
        # Evaluate Model
        y_pred = self.model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        
        ml_global_stats["rss_acc"] = float(acc)
        ml_global_stats["rss_samples"] = len(self.data_buffer)
        update_ml_stats()
        
        return acc

    def predict(self, df):
        if not self.is_trained or df.empty or 'cleaned_title' not in df.columns:
            return df
        
        X_text = df['cleaned_title'].fillna('')
        # Transform data into numerical features
        X = self.vectorizer.transform(X_text)
        # Add prediction to DataFrame
        df['ml_sentiment_pred'] = self.model.predict(X)
        return df


class APIPriceRegressor:
    """
    Regression model for API Price trends.
    Uses One-Hot Encoding for categorical commodities and Ridge Regression.
    """
    def __init__(self):
        self.encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
        self.model = Ridge(random_state=42)
        self.is_trained = False
        self.model_path = os.path.join(ML_DIR, "api_regressor.pkl")
        self.encoder_path = os.path.join(ML_DIR, "api_encoder.pkl")
        self.data_buffer = pd.DataFrame()
        self.load_model()

    def load_model(self):
        if os.path.exists(self.model_path) and os.path.exists(self.encoder_path):
            self.model = joblib.load(self.model_path)
            self.encoder = joblib.load(self.encoder_path)
            self.is_trained = True

    def save_model(self):
        joblib.dump(self.model, self.model_path)
        joblib.dump(self.encoder, self.encoder_path)

    def retrain(self, df_new):
        """Accumulate new streaming data and retrain the regression model."""
        if df_new.empty or 'komoditas' not in df_new.columns or 'harga' not in df_new.columns:
            return None

        self.data_buffer = pd.concat([self.data_buffer, df_new]).drop_duplicates().tail(1000)
        
        df_clean = self.data_buffer.dropna(subset=['komoditas', 'harga'])
        # Need a minimum amount of data to train
        if len(df_clean) < 20:
            return None
            
        X_cat = df_clean[['komoditas']]
        y = pd.to_numeric(df_clean['harga'], errors='coerce').fillna(0)
        
        # Preprocessing: Convert categorical variables to numerical using OneHotEncoding
        X = self.encoder.fit_transform(X_cat)
        
        # Train-test split for evaluation metrics
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train Model
        self.model.fit(X_train, y_train)
        self.is_trained = True
        self.save_model()
        
        # Evaluate Model using RMSE and R2
        y_pred = self.model.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)
        
        ml_global_stats["api_rmse"] = float(rmse)
        ml_global_stats["api_r2"] = float(r2)
        ml_global_stats["api_samples"] = len(self.data_buffer)
        update_ml_stats()
        
        return rmse

    def predict(self, df):
        if not self.is_trained or df.empty or 'komoditas' not in df.columns:
            return df
            
        X_cat = df[['komoditas']].fillna('Unknown')
        # Transform categorical feature
        X = self.encoder.transform(X_cat)
        # Add prediction to DataFrame
        df['ml_harga_pred'] = self.model.predict(X)
        return df

# Initialize models
rss_classifier = RSSNewsClassifier()
api_regressor = APIPriceRegressor()
