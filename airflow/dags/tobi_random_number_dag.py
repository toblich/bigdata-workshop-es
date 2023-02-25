"""Random number dag modified by Tobi."""
from datetime import datetime
from pathlib import Path

import logging

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

STORE_DIR = Path(__file__).resolve().parent / 'tmp-files' / 'random-num'
Path.mkdir(STORE_DIR, exist_ok=True, parents=True)
bash_cmd = f"echo $(( ( RANDOM % 10 )  + 1 )) > {str(STORE_DIR / 'random_number_{{ ds_nodash }}.txt')}"
cleanup_cmd = f"rm {str(STORE_DIR / 'random_number_{{ ds_nodash }}.txt')}"

def _read_number_and_square(store_dir, **context):
    fn = str(store_dir / f'random_number_{context["execution_date"]:%Y%m%d}.txt')
    logging.info(f"Reading file: {fn}")
    with open(fn, 'r') as f:
        n = int(f.readline())
    logging.info(f"Read number: {n}")
    logging.info(f"Squared result: {n ** 2}")
    return 'print_low' if n <= 5 else 'print_high'



default_args = {'owner': 'pedro', 'retries': 0, 'start_date': datetime(2022, 12, 11)}
with DAG(
    'random_number_tobi', default_args=default_args, schedule_interval='0 4 * * *'
) as dag:
    dummy_start_task = DummyOperator(task_id=f'dummy_start')
    generate_random_number = BashOperator( task_id='generate_random_number', bash_command=bash_cmd )
    cleanup = BashOperator( task_id='cleanup', bash_command=cleanup_cmd, trigger_rule = 'one_success' )
    print_high = BashOperator(task_id='print_high', bash_command='echo High!')
    print_low = BashOperator(task_id='print_low', bash_command='echo Low!')
    read_num_and_square = BranchPythonOperator(
        task_id='read_number_and_square_it',
        python_callable=_read_number_and_square,
        op_args=[STORE_DIR],
        provide_context=True,
    )

    dummy_start_task >> generate_random_number >> read_num_and_square >> [print_high, print_low] >> cleanup
