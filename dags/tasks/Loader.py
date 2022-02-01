from airflow.models import Variable
from airflow.models import Connection
from minio import Minio
import mlflow
from datetime import datetime

# Операция загрузки обработанных данных
def load_data():
    # Цепляемся за подключение созданное нами внутри среды AirFlow
    connection = Connection().get_connection_from_secrets(Variable.get("minio_connection"))
    # Cоздаём подключение к minio
    client = Minio(
        f"{connection.host}:{connection.port}",
        access_key=connection.login,
        secret_key=connection.password,
        secure=False
    )
    # Отправляем файл в minio
    client.fput_object(
        bucket_name=Variable.get("minio_bucket_name"),
        object_name="output.csv",
        file_path="./output.csv",
        content_type='application/csv',
    )
    # mlflow_uri = "http://mlflow:5000/"
    # mlflow.set_tracking_uri(mlflow_uri)
    # expname = "My_ETL_DAG"
    # mlflow.set_experiment(expname)
    # mlflow.log_param("Finish_Time", datetime.today())
    # mlflow.log_artifact("output.csv",artifact_path="s3://mlflow/")

    
