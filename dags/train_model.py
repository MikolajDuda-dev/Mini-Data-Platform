import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from mlflow.tracking import MlflowClient
import sys
import os
import time

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"

STORAGE_OPTS = {
    "key": "minioadmin",
    "secret": "minioadmin",
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("Product_Demand_Prediction")

def train():
    print(">>> [Training] Starting the training process...")

    print(">>> Downloading data from Gold Layer (MinIO)...")
    try:

        df = pd.read_parquet("s3://gold/data/product_stats/", storage_options=STORAGE_OPTS)
        
        if df.empty:
            print("!!! ERROR: Gold DataFrame is empty. Check your Spark Gold job.")
            sys.exit(1)
            
    except Exception as e:
        print(f"!!! Data read error: {e}")
        print("💡 Hint: Check if 's3fs' and 'pyarrow' are installed in the Airflow container.")
        sys.exit(1)

    print(f">>> Data loaded successfully: {len(df)} rows.")


    try:
        X = df[["avg_price"]]
        y = df["total_quantity"]
    except KeyError as e:
        print(f"!!! Column error: {e}. Check if Spark Gold job output matches these names.")
        sys.exit(1)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


    with mlflow.start_run():
        print(">>> Training Linear Regression model...")
        model = LinearRegression()
        model.fit(X_train, y_train)

        predictions = model.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        r2 = r2_score(y_test, predictions) if len(y_test) > 1 else 0.0

        print(f">>> Metrics: MSE={round(mse, 4)}, R2={round(r2, 4)}")

 
        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)

        signature = mlflow.models.infer_signature(X_train, model.predict(X_train))

        model_name = "ProductDemandModel"
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=model_name,
            signature=signature
        )
        print(f">>> Model logged and registered as '{model_name}'.")


        print(">>> Transitioning model to Production...")
        client = MlflowClient()

        latest_version = None
        for attempt in range(1, 6):
            versions = client.get_latest_versions(model_name, stages=["None"])
            if versions:
                latest_version = versions[0].version
                break
            print(f"    (Attempt {attempt}/5) Waiting for model version to appear in Registry...")
            time.sleep(2)

        if latest_version:
            print(f">>> Promoting version {latest_version} to Production...")
            client.transition_model_version_stage(
                name=model_name,
                version=latest_version,
                stage="Production",
                archive_existing_versions=True
            )
            print(">>> SUCCESS! Model is now in Production stage.")
        else:
            print("!!! ERROR: Could not find model version to promote.")
            sys.exit(1)

if __name__ == "__main__":
    train()