from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# DAG định nghĩa
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'mysql_to_bigquery_with_gcs_1',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Danh sách các bảng
tables = ['addresses', 'customers', 'games','inventory_buy','inventory_rent','purchases','rentals','staff','tournament','tournament_results']

# Hàm truy xuất dữ liệu từ một bảng
def extract_table_from_mysql(table_name):
    # Kết nối MySQL
    mysql_hook = MySqlHook(mysql_conn_id='Son_MySQL')
    query = f"SELECT * FROM {table_name};"
    df = mysql_hook.get_pandas_df(sql=query)

    # Lưu dữ liệu vào file CSV tạm thời
    temp_file = f'/tmp/{table_name}.csv'
    df.to_csv(temp_file, index=False)
    return temp_file

# Task 1: Truy xuất dữ liệu từ từng bảng
def extract_all_tables(**kwargs):
    file_paths = {}
    for table in tables:
        file_paths[table] = extract_table_from_mysql(table)
    kwargs['ti'].xcom_push(key='file_paths', value=file_paths)

extract_task = PythonOperator(
    task_id='extract_all_tables',
    python_callable=extract_all_tables,
    provide_context=True,
    dag=dag,
)

# Task 2: Upload file CSV lên Google Cloud Storage
for table in tables:
    upload_task = LocalFilesystemToGCSOperator(
        task_id=f'upload_{table}_to_gcs',
        src=f'/tmp/{table}.csv',
        dst=f'mysql_data/{table}.csv',
        bucket='us-central1-gamestore-f36b3a3c-bucket',  # Thay bằng tên bucket thực tế
        mime_type='text/csv',
        dag=dag,
    )

    # Task 3: Load dữ liệu từ GCS lên BigQuery
    load_task = BigQueryInsertJobOperator(
        task_id=f"load_{table}_to_bigquery",
        configuration={
            "load": {
                "sourceUris": [f"gs://us-central1-gamestore-f36b3a3c-bucket/mysql_data/{table}.csv"],
                "destinationTable": {
                    "projectId": "dtwh-final",
                    "datasetId": "son_test",
                    "tableId": table,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "sourceFormat": "CSV",
                "fieldDelimiter": ",",
                "skipLeadingRows": 1,
                "autodetect": True,
            }
        },
        dag=dag,
    )

    extract_task >> upload_task >> load_task
