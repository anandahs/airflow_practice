from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests
import pandas as pd
import os
from sqlalchemy import create_engine, text
from airflow.providers.postgres.hooks.postgres import PostgresHook  


@dag(dag_id="etl_pipeline", schedule=None, start_date=None, catchup=False)
def etl_pipeline():

    @task.python
    def get_timestamp():
        from datetime import datetime
        return datetime.now().isoformat()

    @task.python
    def extract(ti):

        # fetching timestamp from previous task
        timestamp = ti.xcom_pull(task_ids="get_timestamp", key="return_value")
        print(f"Data extracted at {timestamp}")

        url = "http://fastapi:8000/fetch_data"

        response = requests.get(url)
        data = response.json().get("data", [])

        # writing data into staging layer

        # creating directory if not exists
        os.makedirs("/tmp/raw", exist_ok=True)

        with open(f"/tmp/raw/data_{timestamp}.csv", "w") as f:
            f.write("id,name,age\n")
            for item in data:
                f.write(f"{item['id']},{item['name']},{item['age']}\n")
        
        return "Data is extracted and stored in staging layer"
    
    @task.python
    def transform(ti):
        # fetching timestamp from previous task
        timestamp = ti.xcom_pull(task_ids="get_timestamp", key="return_value")
        print(f"Data transformed at {timestamp}")

        # reading data from staging layer
        df = pd.read_csv(f"/tmp/raw/data_{timestamp}.csv")


        # adding age group column
        df["age_group"] = df["age"].apply(lambda x: "young" if x < 30 else "old")
        # simple transformation: filtering out age > 30

        # writing transformed data into another directory
        os.makedirs("/tmp/transformed", exist_ok=True)
        df.to_csv(f"/tmp/transformed/data__transformed_{timestamp}.csv", index=False)

    @task.python
    def create_tables():
       
       query="""
       CREATE TABLE IF NOT EXISTS employees (
           id INT PRIMARY KEY,
           name VARCHAR(255),
           age INT,
           age_group VARCHAR(50)
       );
       """
       connec = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
       
       with connec.begin() as transaction:
           try:
               transaction.execute(text(query))
           except Exception as e:
               transaction.rollback()
               print(f"Error creating tables: {e}")
               raise
           print("Tables created successfully")

    @task.python
    def load_data(ti):
        timestamp = ti.xcom_pull(task_ids="get_timestamp", key="return_value")
        print(f"Data loaded at {timestamp}")

        # reading transformed data
        df = pd.read_csv(f"/tmp/transformed/data__transformed_{timestamp}.csv")

        connec = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

        with connec.begin() as transaction:
            transaction.execute(text("TRUNCATE TABLE employees"))

        df.to_sql("employees", connec, if_exists="append", index=False)
        print("Data loaded into the database successfully")

        connec.dispose()

    create_new_table = SQLExecuteQueryOperator(
        task_id="create_students_table",
        sql="""
        CREATE TABLE IF NOT EXISTS students (
            id INT,
            name VARCHAR(255),
            age INT,
            age_group VARCHAR(50)
        );
        """,
        conn_id="my_postgres_sql"
    )

    # task to write data using postgres hook
    @task.python
    def write_to_new_table(ti):
        timestamp = ti.xcom_pull(task_ids="get_timestamp", key="return_value")
        print(f"Data written using hook at {timestamp}")

        # reading transformed data
        #df = pd.read_csv(f"/tmp/transformed/data__transformed_{timestamp}.csv")

        hook = PostgresHook(postgres_conn_id="my_postgres_sql")
        hook.copy_expert(sql="COPY students(id, name, age, age_group) FROM STDIN WITH CSV HEADER", filename=f"/tmp/transformed/data__transformed_{timestamp}.csv")
        print("Data written to new table using PostgresHook successfully")
 
    # write data into staging layer
    time_stamp = get_timestamp()
    exctrated_data = extract()
    transformed_data = transform()
    create_tables = create_tables()
    write_data = write_to_new_table()
    load_data = load_data()



    time_stamp >> exctrated_data >> transformed_data >> [create_tables, create_new_table] 
    create_tables >> load_data
    create_new_table >> write_data



etl_pipeline_instance = etl_pipeline()
