import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from utils import download_data_from_s3, upload_model_to_s3
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configs
BUCKET_NAME = "nycc-taxi-processed-data"
INPUT_KEY = "cleaned/yellow_tripdata_combined.csv"
OUTPUT_MODEL_KEY = "models/yellow_trip_model.pkl"

def preprocess_data(df):
    # rename columns for easier usage (optional)
    df = df.rename(columns={
        'tpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime'
    })
    
    df = df.dropna()
    df['trip_duration'] = (pd.to_datetime(df['dropoff_datetime']) - pd.to_datetime(df['pickup_datetime'])).dt.total_seconds() / 60.0
    df = df[df['trip_duration'] < 180]  # remove trips longer than 3 hours

    feature_cols = ['passenger_count', 'trip_distance', 'fare_amount']
    target_col = 'trip_duration'

    return df[feature_cols], df[target_col]

def train():
    logger.info("🔹 Step 1: Load data from S3")
    df = download_data_from_s3(BUCKET_NAME, INPUT_KEY)

    logger.info("🔹 Step 2: Preprocess")
    X, y = preprocess_data(df)

    logger.info("🔹 Step 3: Train/Test Split")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    logger.info("🔹 Step 4: Model Training")
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    logger.info("🔹 Step 5: Evaluate")
    preds = model.predict(X_test)
    mse = mean_squared_error(y_test, preds)  # no squared param
    rmse = np.sqrt(mse)
    logger.info(f"✅ RMSE: {rmse:.2f}")

    logger.info("🔹 Step 6: Upload model to S3")
    upload_model_to_s3(model, BUCKET_NAME, OUTPUT_MODEL_KEY)

    # load_data_from_athena()

if __name__ == "__main__":
    train()
