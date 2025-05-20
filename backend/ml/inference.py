# inference.py
import json
import joblib
import pandas as pd
import os

def model_fn(model_dir):
    print("🔍 Loading model from:", model_dir)
    model_path = os.path.join(model_dir, "yellow_trip_model.pkl")
    return joblib.load(model_path)

def input_fn(request_body, content_type='application/json'):
    if content_type == 'application/json':
        return pd.read_json(request_body, orient='records')
    raise ValueError(f"Unsupported content type: {content_type}")

def predict_fn(input_data, model):
    return model.predict(input_data)

def output_fn(prediction, content_type='application/json'):
    return json.dumps(prediction.tolist())
