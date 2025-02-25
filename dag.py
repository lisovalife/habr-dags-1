import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.sql import SqlSensor


default_args = {
    'owner': 'tlisova',
    'depends_on_past': False,
    'email': ['tlisova@neoflex.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'max_active_runs': 1
}

def hello():
    return 'Hello'

with DAG(
    dag_id = 'hello',
    default_args =default_args,
    catchup = False,
    schedule_interval=None,
    tags = ['habr']
) as dag:
    task_1 = PythonOperator(
        task_id = 'task1',
        python_callable = hello
    )

    # Email = EmailOperator(
    #     task_id='send_email',
    #     to='tlisova@neoflex.ru',
    #     subject='Airflow Alert',
    #     html_content='<p>Your Airflow job has finished.</p>'
    # )


    record_exists_sensor = SqlSensor(
        task_id='check_record_exists',
        conn_id='my_db_conn',
        sql="""SELECT count(*)  FROM employees t WHERE date_of_recording > now()- INTERVAL '60 Minutes'""",
    )

    telegram = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="telegram_conn",
        chat_id="1169795947",
        text="Hello from Airflow!",
    )

task_1 >> record_exists_sensor >> telegram