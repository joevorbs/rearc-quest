# Set up logging
import json
import os
import logging
import time
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
import boto3
client = boto3.client('glue')

# Variables for the job: 
#glueJobName = "bls_gov_update_and_check"
job_list = ['bls_gov_update_and_check', 'usa_population_data_pull']

# Define Lambda function
def lambda_handler(event, context):
    logger.info('## INITIATED BY EVENT: ')
    
    response_bls = client.start_job_run(JobName = job_list[0])
    logger.info('## STARTED GLUE JOB: ' + job_list[0])
    logger.info('## GLUE JOB RUN ID: ' + response_bls['JobRunId'])
    
    response_usa = client.start_job_run(JobName = job_list[1])
    logger.info('## STARTED GLUE JOB: ' + job_list[1])
    logger.info('## GLUE JOB RUN ID: ' + response_usa['JobRunId'])
    
    job_run = client.get_job_run(JobName=job_list[0], RunId = response_bls.get("JobRunId"))
    job_status = job_run.get("JobRun").get("JobRunState")
    while job_status == "RUNNING" :
        job_run = client.get_job_run(JobName=job_list[0], RunId = response_bls.get("JobRunId"))
        job_status = job_run.get("JobRun").get("JobRunState")
        print("not yet")
        if job_status == "SUCCEEDED":
            time.sleep(10)
            response_report = client.start_job_run(JobName = 'task_3_analytics')
            print("Report Generation in Progress")
        
    return [response_bls, response_usa, response_report]
