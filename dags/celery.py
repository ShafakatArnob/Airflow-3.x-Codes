from airflow.sdk import dag, task
from time import sleep

@dag
def celery_dag():
    @task
    def a():
        sleep(5)

    @task
    def b():
        sleep(5)

    @task
    def c():
        sleep(5)

    @task
    def d():
        sleep(5)

    a() >> [b(), c()] >> d()  # This will run b and c in parallel after a, then d after both are complete


celery_dag()