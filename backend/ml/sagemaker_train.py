import argparse
import pandas as pd
import joblib
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error

def preprocess_data(df):
    df = df.dropna()
    df['trip_duration'] = (pd.to_datetime(df['dropoff_datetime']) - pd.to_datetime(df['pickup_datetime'])).dt.total_seconds() / 60.0
    df = df[df['trip_duration'] < 180]
    feature_cols = ['passenger_count', 'trip_distance', 'fare_amount']
    target_col = 'trip_duration'
    return df[feature_cols], df[target_col]

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--train_data', type=str, default='/opt/ml/input/data/train/train.csv')
    parser.add_argument('--model_dir', type=str, default=os.environ.get('SM_MODEL_DIR'))

    args = parser.parse_args()

    df = pd.read_csv(args.train_data)
    X, y = preprocess_data(df)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    rmse = root_mean_squared_error(y_test, preds, squared=False)
    print(f"✅ SageMaker RMSE: {rmse:.2f}")

    # Save model
    joblib.dump(model, os.path.join(args.model_dir, "model.joblib"))

if __name__ == '__main__':
    main()
