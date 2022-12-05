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
job_list = ['bls_gov_update_and_check', 'usa_population_data_pull', 'task_3_analytics']

# Define Lambda function
def lambda_handler(event, context):
    logger.info('## INITIATED BY EVENT: ')
    
    #Start BLS data pull
    response_bls = client.start_job_run(JobName = job_list[0])
    logger.info('## STARTED GLUE JOB: ' + job_list[0])
    logger.info('## GLUE JOB RUN ID: ' + response_bls['JobRunId'])
    
    #Start USA data pull
    response_usa = client.start_job_run(JobName = job_list[1])
    logger.info('## STARTED GLUE JOB: ' + job_list[1])
    logger.info('## GLUE JOB RUN ID: ' + response_usa['JobRunId'])
    
    #Get progress of BLS data pull - once completed the report script will start
    job_run = client.get_job_run(JobName=job_list[0], RunId = response_bls.get("JobRunId"))
    job_status = job_run.get("JobRun").get("JobRunState")
    #Once job status switches from running to succeeded task_3_analytics will trigger
    while job_status == "RUNNING" :
        job_run = client.get_job_run(JobName=job_list[0], RunId = response_bls.get("JobRunId"))
        job_status = job_run.get("JobRun").get("JobRunState")
        print("not yet")
        if job_status == "SUCCEEDED":
            time.sleep(10)
            response_report = client.start_job_run(JobName = job_list[2])
            print("Report Generation in Progress")
            break
        
    return [response_bls, response_usa, response_report]
