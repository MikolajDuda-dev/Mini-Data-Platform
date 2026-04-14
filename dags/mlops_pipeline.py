from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {'owner': 'airflow', 'start_date': days_ago(1)}

with DAG('3_mlops_full_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    # 1. Install libraries
    install_libs = BashOperator(
        task_id='install_requirements',
        bash_command='pip install pandas s3fs pyarrow great_expectations mlflow scikit-learn boto3 Faker psycopg2-binary'
    )

    # 2. Simulation (Postgres)
    run_simulation = BashOperator(
        task_id='run_simulation',
        bash_command='python /opt/airflow/dags/simulate_process.py'
    )

    # 3. Ingest (Postgres -> Bronze)
    ingest_task = BashOperator(
        task_id='ingest_to_bronze',
        bash_command='python /opt/airflow/dags/ingest_postgres_to_bronze.py'
    )

    # 4. FIX: Spark Transform (Bronze -> Silver)
    spark_transform = BashOperator(
        task_id='spark_bronze_to_silver',
        bash_command="""
        # Added '-u 0' flag to run as root
        docker exec -u 0 -i spark-master /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        /opt/spark-jobs/bronze_to_silver.py
        """
    )
    # 5. Spark Aggregation (Silver -> Gold)
    spark_gold_task = BashOperator(
        task_id='spark_silver_to_gold',
        bash_command="""
        docker exec -u 0 -i spark-master /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        /opt/spark-jobs/silver_to_gold.py
        """
    )

    # 6. Validation (GX)
    validate_data = BashOperator(
        task_id='gx_validation',
        bash_command='python /opt/airflow/dags/gx_validate.py'
    )

    # 7. Training (MLflow)
    train_model = BashOperator(
        task_id='mlflow_training',
        bash_command='python /opt/airflow/dags/train_model.py'
    )
    deploy_model = BashOperator(
        task_id='deploy_model_restart',
        bash_command='docker restart mlflow-serving'
    )

    # Task order
    install_libs >> run_simulation >> ingest_task >> spark_transform >> spark_gold_task >> validate_data >> train_model >> deploy_model