import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator # импортируем класс оператора 
from steps.clean_data import transform, load # type: ignore
from steps.messages import send_telegram_success_message, send_telegram_failure_message # type: ignore


with DAG(
    dag_id='clean_flats_data',
    schedule='@once',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message) as dag:

        # инициализируем задачи DAG, указывая параметр python_callable
        transform_step = PythonOperator(task_id='transform', python_callable=transform)
        load_step = PythonOperator(task_id='load', python_callable=load)
        
        transform_step >> load_step

