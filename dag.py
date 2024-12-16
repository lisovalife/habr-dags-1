import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator


default_args = {
    'owner': 'tlisova',
    'depends_on_past': False,
    'retries': 0,
    'max_active_runs': 1
}

def hello():
    return 'Hello'

with DAG(
    dag_id = 'hello',
    default_args =default_args,
    catchup = False,
    tags = ['habr']
) as dag:
    task_1 = PythonOperator(
        task_id = 'task1',
        python_callable = hello
    )

    Email = EmailOperator(
        task_id='send_email',
        to='tlisova@neoflex.ru',
        subject='Airflow Alert',
        html_content='<p>Your Airflow job has finished.</p>'
    )

task_1 -> Email