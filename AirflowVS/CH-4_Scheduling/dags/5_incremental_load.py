from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum 


@dag(dag_id="incremental_load_dag_1", schedule=CronDataIntervalTimetable("0 0 * * *", timezone="America/New_York")
, start_date=pendulum.datetime(year=2026, month=6, day=15), catchup=True, is_paused_upon_creation=False)
def incremental_load_dag1():
    
    @task.python
    def extract_data(**kwargs):
        
        from_date = kwargs['data_interval_start']
        to_date = kwargs['data_interval_end']

        print(f"Extracting data from {from_date} to {to_date}")
        print(f"select * from source_table where date >= {from_date} and date < {to_date}")

    @task.bash
    def load_data():
        return """
        echo "Loading data from {{data_interval_start | ds }} to {{data_interval_end | ds}}"
        """

    extract_data() >> load_data()

incremental_load_dag_instance = incremental_load_dag1()
