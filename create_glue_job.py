# create_glue_job.py
import boto3

glue_client = boto3.client('glue', region_name='us-east-1')
BUCKET_NAME = 'your-glue-lab-bucket'

def create_etl_job():
    """Create Glue ETL job"""
    
    # First, upload the script to S3
    s3_client = boto3.client('s3')
    with open('glue_etl_job.py', 'rb') as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, 'scripts/glue_etl_job.py')
    print(f"Uploaded script to s3://{BUCKET_NAME}/scripts/glue_etl_job.py")
    
    # Create the job
    try:
        glue_client.create_job(
            Name='transactions-etl-job',
            Role='AWSGlueServiceRole',
            Command={
                'Name': 'glueetl',
                'ScriptLocation': f's3://{BUCKET_NAME}/scripts/glue_etl_job.py',
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--SOURCE_BUCKET': BUCKET_NAME,
                '--TARGET_BUCKET': BUCKET_NAME,
                '--enable-metrics': '',
                '--enable-continuous-cloudwatch-log': 'true'
            },
            GlueVersion='4.0',
            WorkerType='G.1X',
            NumberOfWorkers=2,
            Timeout=60
        )
        print("Created job: transactions-etl-job")
    except glue_client.exceptions.AlreadyExistsException:
        print("Job already exists")

def run_etl_job():
    """Start the ETL job"""
    response = glue_client.start_job_run(
        JobName='transactions-etl-job',
        Arguments={
            '--SOURCE_BUCKET': BUCKET_NAME,
            '--TARGET_BUCKET': BUCKET_NAME
        }
    )
    print(f"Started job run: {response['JobRunId']}")
    return response['JobRunId']

def check_job_status(job_run_id):
    """Check job status"""
    response = glue_client.get_job_run(
        JobName='transactions-etl-job',
        RunId=job_run_id
    )
    status = response['JobRun']['JobRunState']
    print(f"Job status: {status}")
    return status

if __name__ == "__main__":
    create_etl_job()
    job_run_id = run_etl_job()
    
    import time
    while True:
        time.sleep(30)
        status = check_job_status(job_run_id)
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
