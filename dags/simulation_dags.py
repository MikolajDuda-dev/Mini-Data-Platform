from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '0_run_business_simulation',
    default_args=default_args,
    description='Instaluje biblioteki i uruchamia symulację danych',
    schedule_interval=None, # Uruchamiany ręcznie przyciskiem Play
    start_date=days_ago(1),
    catchup=False,
    tags=['project', 'simulation'],
) as dag:

    # Zadanie 1: Instalacja Faker i psycopg2 w kontenerze Airflow
    install_deps = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install Faker psycopg2-binary'
    )

    # Zadanie 2: Uruchomienie skryptu (timeout 60s, żeby nie działał w nieskończoność)
    run_simulation = BashOperator(
        task_id='run_simulation_script',
        bash_command='timeout 60s python /opt/airflow/dags/simulate_process.py || true'
    )

    install_deps >> run_simulation