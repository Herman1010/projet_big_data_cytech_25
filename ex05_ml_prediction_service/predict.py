import pandas as pd
import joblib
from typing import Union


FEATURES = [
    "trip_distance",
    "passenger_count",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "congestion_surcharge",
    "airport_fee",
    "hour",
    "weekday",
    "month",
]


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add temporal features from pickup datetime.

    :param df: Input dataframe.
    :type df: pd.DataFrame
    :param df: pd.DataFrame: 

    
    """
    df = df.copy()
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["hour"] = df["tpep_pickup_datetime"].dt.hour
    df["weekday"] = df["tpep_pickup_datetime"].dt.weekday
    df["month"] = df["tpep_pickup_datetime"].dt.month
    return df


def validate_inference_data(df: pd.DataFrame) -> None:
    """Validate input data before inference.

    :param df: 
    :type df: pd.DataFrame
    :param df: pd.DataFrame: 

    
    """
    if df.empty:
        raise ValueError("Input data is empty")

    for col in FEATURES:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")


def predict(df: Union[pd.DataFrame, dict]):
    """Predict taxi price from input data.

    :param df: Input data.
    :type df: pd.DataFrame or dict
    :param df: Union[pd.DataFrame: 
    :param dict]: 

    
    """
    if isinstance(df, dict):
        df = pd.DataFrame([df])

    
    df = add_time_features(df)

    
    validate_inference_data(df)

    model = joblib.load("model.pkl")

    df[FEATURES] = df[FEATURES].fillna(0)

    return model.predict(df[FEATURES])
