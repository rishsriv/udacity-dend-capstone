from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import boto3

class UploadDataOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        input_path="",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="",
        s3_key="",
        table="",
        columns="",
        *args, **kwargs):

        super(UploadDataOperator, self).__init__(*args, **kwargs)
        self.input_path = input_path
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.columns = columns

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        s3 = boto3.resource(
            's3',
            region_name="us-east-1",
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key
        )

        self.log.info("Reading CSV file as StringIO object")
        with open(self.input_path, "r") as f:
            contents = f.read()

        self.log.info("Uploading CSV to S3")
        s3.Object(self.s3_bucket, self.s3_key).put(Body=contents)

        sql_to_run = f"COPY {self.table} ({self.columns}) FROM '{self.s3_path}' ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}' CSV IGNOREHEADER 1;"
        self.log.info(f"Executing {sql_to_run} ...")
        redshift.run(sql_to_run)