from datetime import timedelta

from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago
from minio import Minio
from airflow.models import Variable
from airflow.models import Connection


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


# Операция трансформации данных
def transform_data():
    # Считываем наш csv файл в DataFrame
    df = pd.read_csv("./input.csv", delimiter=';')

    # Задаём для него схему
    df.columns = [
        'sign_date',
        'blank',
        'creation_date',
        'name',
        'index',
        'type_of_organization',
        'sector',
        'payout type',
        'amount',
        'period_of_date',
        'blank2'
    ]

    # Избавляемся от ненужных колонок:
    df.drop(["blank"], axis=1, inplace=True)
    df.drop(["blank2"], axis=1, inplace=True)

    # Очищаем DataFrame от пустых строк, за основу берётся поле, в котором всегда есть значение
    df = df.dropna(subset=['sign_date'])

    # Трансформация 1: "1 от 17.09.2018" -> 17.09.2018

    df['sign_date'] = df['sign_date'].str.extract(r'от (.*)', expand=False)

    # Трансформация 2: "ООО ""Континент ЭТС""" -> "ООО" | "Континент ЭТС"

    df['legal_form'] = df['name'].str.split(' ').str[0]
    df['organization_name'] = df['name'].str.extract(r' (.*)', expand=False).str.replace('\"', '')
    df.drop(["name"], axis=1, inplace=True)

    # Трансформация 3: “1 014 430,57” -> 1014430,57

    df['amount'] = df['amount'].str.replace(",", ".").str.replace(" ", "")

    # Трансформация 4: 2018-2020 -> 2018 | 2020

    df['beginning_contract'] = df['period_of_date'].str[0:4]
    df['ending_contract'] = df['period_of_date'].str[-4:]
    df.drop(["period_of_date"], axis=1, inplace=True)

    # Приводим данные из некоторых столбцов к необходимым типам
    df['sign_date'] = df['sign_date'].astype('datetime64')
    df['creation_date'] = df['creation_date'].astype('datetime64')
    df['index'] = df['index'].astype('int64')
    df['amount'] = df['amount'].astype('float64')

    # Записываем готовый фрейм в файл
    df.to_csv("./output.csv", index=False, sep=';')


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

# Строим пайплайн для создания полноценного etl процесса в нашем DAG
extract >> transform >> load
