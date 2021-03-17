from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import pandas as pd
import boto3
import json
from io import StringIO
from datetime import datetime

class CleanAndUploadDataOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        input_path="",
        aws_credentials_id="aws_credentials",
        s3_bucket="",
        s3_key="",
        *args, **kwargs):

        super(CleanAndUploadDataOperator, self).__init__(*args, **kwargs)
        self.input_path = input_path
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        s3 = boto3.resource(
            's3',
            region_name="us-east-1",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key
        )

        self.log.info("Cleaning JSON data")
        with open(self.input_path, "r") as f:
            data = json.load(f)

        cleaned_data = []

        cleaned_city_names = {
            "bengaluru": 'bangalore',
            'asanol': 'asansol',
            'gurugram': 'gurgaon',
            'rajamahendravaram': 'rajamahandevaram',
            'thiruvananthapuram': 'trivandrum',
            'vijayawada': 'vijaywada'
        }

        invalid_city_names = ['rajasthan', 'sector']

        for key in data:
            city = key.split("_")[0]
            metric = "_".join(key.split("_")[2:])
            value = data[key]
            if value is not None and city not in invalid_city_names:
                for item in value:
                    dt = datetime.fromtimestamp(item[0]).isoformat().replace("T", " ")
                    val = item[1]
                    cleaned_city_name = cleaned_city_names.get(city, city)
                    if val >= 0 and val < 2000: #discard implausible values
                        cleaned_data.append({"city": cleaned_city_name, "metric": metric, "dt": dt, "value": val})
        
        self.log.info("Converting JSON data into CSV")
        csv_buffer = StringIO()
        pd.DataFrame(cleaned_data).to_csv(csv_buffer, index=False)

        self.log.info("Uploading CSV to S3")
        s3.Object(self.s3_bucket, self.s3_key).put(Body=csv_buffer.getvalue())