import pandas as pd
import os
import mlflow
from pandas import DataFrame

from models.etl_model import ETL_Model
from airflow.models import Variable
from airflow.models import Connection


# Создаём переменные окружения для подключения к Minio

def create_endpoints():
    connection = Connection().get_connection_from_secrets(Variable.get("minio_connection"))
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = f'http://{connection.host}:{connection.port}/'
    os.environ['AWS_ACCESS_KEY_ID'] = connection.login
    os.environ['AWS_SECRET_ACCESS_KEY'] = connection.password


# Осуществляем оценку корректности обработки

def make_assessment(data_frame: DataFrame, model: ETL_Model):
    prediction = model.predict(None, data_frame)
    return "SUCCESS" if prediction else "FILED"


# Главный метод для осуществления отправки всех необходимых данных в MlFlow

def send_metrics_to_mlflow():
    create_endpoints()

    model_name = "ETL_Model"

    df = pd.read_csv("output.csv", delimiter=';')

    mlflow_uri = Variable.get("mlflow_uri")
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("FIRST_ETL")
    model = ETL_Model()

    with mlflow.start_run():
        mlflow.set_tag("etl", "My_First_ETL_Process")
        mlflow.log_param('model_name', model_name)
        mlflow.log_param("processing_csv_file", make_assessment(df, model))
        mlflow.log_metric("total_rows", df.shape[0])
        mlflow.log_artifact("output.csv")
        mlflow.pyfunc.log_model(model_name, python_model=model)
