from airflow.models import Variable
from airflow.models import Connection
from minio import Minio
import mlflow
from datetime import datetime
from mlflow.pyfunc import PythonModel

# Операция извлечения данных
def extract_data():
    # Цепляемся за подключение созданное нами внутри среды AirFlow
    connection = Connection().get_connection_from_secrets(Variable.get("minio_connection"))
    # Создаём клиента для подключения к Minio
    client = Minio(
        f"{connection.host}:{connection.port}",
        access_key=connection.login,
        secret_key=connection.password,
        secure=False
    )
    # Выгружаем оттуда объект
    client.fget_object(Variable.get("minio_bucket_name"), "input.csv", "input.csv")

    # mlflow_uri = "http://mlflow:5000/"
    # mlflow.set_tracking_uri(mlflow_uri)
    # expname = "My_ETL_DAG"
    #
    #
    #
    # try:
    #     mlflow.create_experiment(expname)
    # except:
    #     pass
    # finally:
    #     mlflow.set_experiment(expname)
    #     mlflow.log_param("Start_Time", datetime.today())



