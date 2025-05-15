from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def load_csv_to_postgres():
    # Connexion PostgreSQL
    host = "epsi_dataops-postgres-1"
    port = "5432"
    dbname = "airflow_db"
    user = "airflow"
    password = "airflow"

    # Chemin statique vers le fichier CSV dans le conteneur
    csv_path = "/opt/airflow/dags/data/sales_2.csv"

    # Connexion SQLAlchemy à PostgreSQL
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")

    # Chargement du CSV
    df = pd.read_csv(csv_path)

    # Chargement dans PostgreSQL
    df.to_sql("raw_sales", engine, if_exists="replace", index=False)

    print("✅ Données CSV chargées avec succès dans PostgreSQL.")

# Définition du DAG
with DAG(
    dag_id='loading_data',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=['dataops'],
) as dag:

    load_task = PythonOperator(
        task_id='load_sales_csv',
        python_callable=load_csv_to_postgres
    )
    run_stg_sales_model = BashOperator(
        task_id='run_stg_sales_model',
        bash_command="cd /opt/airflow/dbt && dbt run --select stg_sales"
    )
    run_daily_revenue_model = BashOperator(
        task_id='run_daily_revenue_model',
        bash_command="cd /opt/airflow/dbt && dbt run --select fct_daily_revenue"
    )
        
load_task >> run_stg_sales_model >> run_daily_revenue_model
