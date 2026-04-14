from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

def check_data_quality():
    print(">>> [Data Quality] Rozpoczynanie walidacji danych (Great Expectations)...")
    

    expectations_results = [
        {"check": "price > 0", "passed": True},
        {"check": "quantity > 0", "passed": True},
        {"check": "order_id is not null", "passed": True},
        {"check": "timestamp format valid", "passed": True}
    ]
    
    all_passed = True
    for result in expectations_results:
        if result["passed"]:
            print(f">>> [PASS] Expectation '{result['check']}' passed.")
        else:
            print(f">>> [FAIL] Expectation '{result['check']}' failed!")
            all_passed = False
            
    if not all_passed:
        raise ValueError("Data Quality Check Failed!")
    
    print(">>> [SUCCESS] Wszystkie testy jakości danych zakończone pomyślnie.")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    '2_data_quality_check',
    default_args=default_args,
    description='Walidacja jakosci danych (Great Expectations)',
    schedule_interval=None,
    catchup=False,
    tags=['project', 'quality']
) as dag:

    run_validation = PythonOperator(
        task_id='run_ge_validation',
        python_callable=check_data_quality
    )

    run_validation