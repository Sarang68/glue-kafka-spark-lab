# MIT License
#
# Copyright (c) 2026 Sarang68
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
