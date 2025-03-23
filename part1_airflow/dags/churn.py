import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator # импортируем класс оператора 
from steps.churn import extract, transform, load, create_table # type: ignore
from steps.messages import send_telegram_success_message, send_telegram_failure_message # type: ignore

with DAG(
    dag_id='flats_churn',
    schedule='@once',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message) as dag:

        # инициализируем задачи DAG, указывая параметр python_callable
        create_table = PythonOperator(task_id='create_table', python_callable=create_table)
        extract_step = PythonOperator(task_id='extract', python_callable=extract)
        transform_step = PythonOperator(task_id='transform', python_callable=transform)
        load_step = PythonOperator(task_id='load', python_callable=load) 

        # описываем последовательность
        create_table >> extract_step
        extract_step >> transform_step
        transform_step >> load_step