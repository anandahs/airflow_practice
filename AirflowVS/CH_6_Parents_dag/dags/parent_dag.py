from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(dag_id="parent_dag", schedule=None, start_date=None, catchup=False)
def parent_dag():

    trigger_first_dag = TriggerDagRunOperator(
        task_id="child_dag_first_trigger",
        trigger_dag_id="child_dag_first"
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id="child_dag_second_trigger",
        trigger_dag_id="child_dag_second"
    )

    trigger_first_dag >> trigger_second_dag

parent_dag_dag = parent_dag()