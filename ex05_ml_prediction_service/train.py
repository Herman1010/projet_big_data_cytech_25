import pandas as pd
import numpy as np
import joblib
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.ensemble import RandomForestRegressor



# Chargement de notre table 

def load_data_from_db(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    table_name: str
) -> pd.DataFrame:

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )

    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)

# Connexion à notre base de données qui contient toutes nos données
df = load_data_from_db(  
    host="localhost",
    port=5432,
    database="nyc_taxi_dw",
    user="dw_user",
    password="Big_data",
    table_name="nyc_taxi.t_taxi_jaune"
)

print(df)

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

TARGET = "total_amount"




# FEATURE ENGINEERING


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add temporal features from pickup datetime.
    """
    df = df.copy()
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["hour"] = df["tpep_pickup_datetime"].dt.hour
    df["weekday"] = df["tpep_pickup_datetime"].dt.weekday
    df["month"] = df["tpep_pickup_datetime"].dt.month
    return df


def clean_training_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values in training data.
    """
    df = df.dropna(subset=[TARGET])
    df[FEATURES] = df[FEATURES].fillna(0)
    return df


def validate_training_data(df: pd.DataFrame) -> None:
    """
    Validate training input data.
    """
    for col in FEATURES + [TARGET]:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    if df[TARGET].isna().any():
        raise ValueError("Target contains NaN values")



# VISUALIZATION


def plot_correlation_matrix(df: pd.DataFrame) -> None:
    """
    Plot correlation matrix.
    """
    corr = df[FEATURES + [TARGET]].corr()

    plt.figure(figsize=(10, 8))
    im = plt.imshow(corr, cmap="coolwarm", interpolation="nearest")
    plt.colorbar(im)

    plt.xticks(range(len(corr.columns)), corr.columns, rotation=90)
    plt.yticks(range(len(corr.columns)), corr.columns)

    for i in range(len(corr.columns)):
        for j in range(len(corr.columns)):
            value = corr.iloc[i, j]
            plt.text(
                j,
                i,
                f"{value:.2f}",
                ha="center",
                va="center",
                color="black" if abs(value) < 0.6 else "white",
                fontsize=8,
            )

    plt.title("Matrice de corrélation")
    plt.tight_layout()
    plt.show()



# MAIN 

def main():
    """Main training pipeline."""
    # Load data 
    df = load_data_from_db(  
    host="localhost",
    port=5432,
    database="nyc_taxi_dw",
    user="dw_user",
    password="Big_data",
    table_name="nyc_taxi.t_taxi_jaune"
)

    # Feature engineering & cleaning
    df = add_time_features(df)
    df = clean_training_data(df)
    validate_training_data(df)

    # Correlation analysis
    print("\nCorrelation with total_amount:")
    plot_correlation_matrix(df)

    # Split data
    X = df[FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Model
    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=15,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
    )

    model.fit(X_train, y_train)

    # Predictions
    preds = model.predict(X_test)

    # Metrics
    mse = mean_squared_error(y_test, preds)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_test, preds)

    print(f"\nRMSE (Random Forest Regressor) : {rmse:.2f}")
    print(f"MAE  (Random Forest Regressor) : {mae:.2f}")

    # Save model
    joblib.dump(model, "model.pkl")
    print("Model saved as model.pkl")


if __name__ == "__main__":
    main()
