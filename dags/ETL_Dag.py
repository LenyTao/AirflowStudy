from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from tasks.Extractor import extract_data
from tasks.Transformer import transform_data
from tasks.Loader import load_data
from tasks.Sender import send_metrics_to_mlflow


args = {
    'owner': 'User',
    'start_date': days_ago(1),
    'provide_context': True
}

with DAG(
        'my_etl_dag',
        description='This is my first DAG for testing ETL workflow in AirFlow',
        tags=['ETL_PROCESS_EXAMPLE'],
        schedule_interval=timedelta(minutes=1),
        catchup=False,
        default_args=args
) as etl_dag:
    # Закрепляем операцию извлечения в etl_dag
    extract = PythonOperator(
        python_callable=extract_data,
        dag=etl_dag,
        task_id='extract'
    )
    # Закрепляем операцию трансформации в etl_dag
    transform = PythonOperator(
        python_callable=transform_data,
        dag=etl_dag,
        task_id='transform'
    )
    # Закрепляем операцию загрузки в etl_dag
    load = PythonOperator(
        python_callable=load_data,
        dag=etl_dag,
        task_id='load'
    )
    # Закрепляем операцию отправки метрик в MlFlow в etl_dag
    send_metric = PythonOperator(
        python_callable=send_metrics_to_mlflow,
        dag=etl_dag,
        task_id='send'
    )

# Строим пайплайн для создания полноценного etl процесса в нашем DAG
extract >> transform >> load >> send_metric
