import pandas as pd
import pytest

from train import validate_training_data, add_time_features
from predict import validate_inference_data, predict


def test_training_data_validation():
    df = pd.DataFrame({
        "tpep_pickup_datetime": ["2024-01-01 10:00:00"],
        "trip_distance": [3.0],
        "passenger_count": [1],
        "fare_amount": [10.0],
        "extra": [1.0],
        "mta_tax": [0.5],
        "tip_amount": [2.0],
        "tolls_amount": [0.0],
        "congestion_surcharge": [2.5],
        "airport_fee": [0.0],
        "total_amount": [16.0]
    })

    df = add_time_features(df)
    validate_training_data(df)


def test_training_data_missing():
    df = pd.DataFrame({
        "trip_distance": [3.0],
        "total_amount": [10.0]
    })

    with pytest.raises(ValueError):
        validate_training_data(df)


def test_inference_data_validation():
    df = pd.DataFrame({
        "tpep_pickup_datetime": ["2024-01-01 10:00:00"],
        "trip_distance": [2.0],
        "passenger_count": [1],
        "fare_amount": [8.0],
        "extra": [1.0],
        "mta_tax": [0.5],
        "tip_amount": [1.0],
        "tolls_amount": [0.0],
        "congestion_surcharge": [2.5],
        "airport_fee": [0.0]
    })

    df = add_time_features(df)
    validate_inference_data(df)


def test_prediction_pipeline():
    df = pd.DataFrame({
        "tpep_pickup_datetime": ["2024-01-01 10:00:00"],
        "trip_distance": [4.0],
        "passenger_count": [1],
        "fare_amount": [15.0],
        "extra": [1.0],
        "mta_tax": [0.5],
        "tip_amount": [2.0],
        "tolls_amount": [0.0],
        "congestion_surcharge": [2.5],
        "airport_fee": [0.0]
    })

    preds = predict(df)
    assert preds[0] > 0
