# URL for Api 
# repo URL: https://github.com/anandahs/airflow_practice/

class BronzeLayer:

    def __init___(self):
        pass

    def ingest_data_api(self, url):
        import pandas as pd
        import requests
        from io import StringIO
 
        response = requests.get(url)

        if response.status_code == 200:
           data = response.text

           df = pd.read_csv(StringIO(data))

           # converting dataframe into csv in memory using StringIO

           csv_buffer = StringIO()

           df.to_csv(csv_buffer, index=False)

           #print csv content
           return csv_buffer.getvalue()

        else:

            print(f"Failed to fetch the data data. status code: {response.status_code}")


    
    def put_data_s3(self, bucket_name, object_key, data):

        import os
        from dotenv import load_dotenv
        import boto3

        load_dotenv()

        aws_access_key_id= os.getenv("aws_access_key_id")
        aws_secret_access_key = os.getenv("aws_secret_access_key")
        aws_region = 'us-east-1'

        s3_client = boto3.client("s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=data
        )

    
object = BronzeLayer()
data = object.ingest_data_api("https://raw.githubusercontent.com/anandahs/airflow_practice/refs/heads/main/airflow_ingest/bookings.csv")
object.put_data_s3('airflow-aws-ananda', 'bronze/bookings.csv', data=data)




