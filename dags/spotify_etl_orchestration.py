from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime( 2026, 5, 30),
}


dag = DAG(
    dag_id='spotify_lambda_trigger',
    default_args=default_args,
    description='DAG to trigger Lambda Function and check S3 upload',
    schedule=timedelta(days=1),
    catchup=False,
)

# Step 1: Trigger Lambda to extract Spotify data
trigger_extract_lambda = LambdaInvokeFunctionOperator(
    task_id='trigger_extract_lambda',
    function_name='spotify_api_data_extract',
    aws_conn_id='aws_s3_spotify',
    region_name="us-east-1",
    dag=dag,
)

# Step 2: Wait for data to appear in S3
check_s3 = S3KeySensor(
    task_id='check_s3_upload',
    bucket_key='s3://spotify-etl-project-204537390950-us-east-1-an/raw_data/to_processed/*',
    wildcard_match=True,
    aws_conn_id='aws_s3_spotify',
    timeout=60 * 60, #wait for one hour
    poke_interval=60, #check every 60 sec
    dag=dag,
)

# Step 3: Trigger Glue job for Spark transformation
trigger_glue = GlueJobOperator(
    task_id='trigger_glue_job',
    job_name='spotify_transformation_job',
    script_location='s3://aws-glue-assets-204537390950-us-east-1/scripts/spotify_transformation_job.py', #find this in Glue - job details
    aws_conn_id='aws_s3_spotify',
    region_name='us-east-1',
    iam_role_name='spotify_glue_iam_role',
    s3_bucket='aws-glue-assets-204537390950-us-east-1',
    dag=dag,
)
#
trigger_extract_lambda >> check_s3>> trigger_glue