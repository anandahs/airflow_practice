import os
from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.vcs import Github
from diagrams.onprem.analytics import Databricks
from diagrams.custom import Custom
from diagrams.aws.storage import SimpleStorageServiceS3
from diagrams.aws.analytics import Glue, GlueCrawlers, Athena

graph_attr = {
    "fontsize": "20",
    "bgcolor": "white",
    "splines": "polyline",
    "nodesep": "0.6",
    "ranksep": "0.8",
}

docs_dir = os.path.join(os.path.dirname(__file__), "..", "docs")
os.makedirs(docs_dir, exist_ok=True)
output_path = os.path.join(docs_dir, "architecture")
delta_lake_icon = os.path.join(docs_dir, "databricks_deltalake.jpeg")

with Diagram("AWS Pipeline Architecture", filename=output_path, outformat="png", show=False, graph_attr=graph_attr, direction="LR"):

    github = Github("Raw CSVs\n(GitHub)")

    with Cluster("Self-hosted (outside AWS)"):
        airflow = Airflow("Airflow\n(docker-compose)")

    with Cluster("AWS"):
        s3_bronze = SimpleStorageServiceS3("S3\nbronze/")

        with Cluster("Glue ETL"):
            glue_delta = Glue("silver_layer.py\n(Delta)")
            glue_parquet = Glue("silver_layer_athena.py\n(Parquet)")

        s3_silver_delta = SimpleStorageServiceS3("S3\nsilver/obt\n(Delta)")
        s3_silver_parquet = SimpleStorageServiceS3("S3\nsilver/obt_parquet\n(Parquet)")

        crawler = GlueCrawlers("airflow_s3_crawler_silver")
        athena = Athena("Athena\n(queries silver_db)")

    with Cluster("Databricks"):
        gold_job = Databricks("Gold layer job")
        gold_schema = Custom("airflow_gold.gold_schema", delta_lake_icon)

    github >> Edge(label="extract_load_to_s3") >> s3_bronze

    airflow >> Edge(label="extract_load_to_s3", style="dashed") >> github
    airflow >> Edge(label="transform_load_s3", style="dashed") >> glue_delta
    airflow >> Edge(label="transform_load_s3_parquet", style="dashed") >> glue_parquet
    airflow >> Edge(label="trigger_glue_crawler", style="dashed") >> crawler
    airflow >> Edge(label="trigger_databricks_job", style="dashed") >> gold_job

    s3_bronze >> glue_delta >> s3_silver_delta >> gold_job
    s3_bronze >> glue_parquet >> s3_silver_parquet >> crawler
    crawler >> Edge(label="catalogs into\nsilver_db") >> athena

    gold_job >> gold_schema
