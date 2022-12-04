import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import requests as re
import pandas as pd
from datetime import datetime

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

#Current date for partitioning files
today = datetime.today().strftime('%Y-%m-%d')

#Specify bucket to write files to
write_bucket = 'bls-timeseries-data'

#Initialize boto3 client for s3 writes
s3 = boto3.resource('s3')

#List of datasets to loop through - in the case that these ever need to change in the future without editing reptitive code
datasets_suffix = ["pr.data.0.Current", "pr.data.1.AllData", "pr.series"] #appear to be the 3 main datasets

#Loop through all items in the BLS directory, extract each dataset and store as an item in a list
dataset_list = []
for i in datasets_suffix:
    site_data_download = re.get("https://download.bls.gov/pub/time.series/pr/" + i).text
    dataset_list.append(site_data_download)    

#For each dataset in the list, write to its respective folder in s3
iterator = 0 
for j in datasets_suffix:
    s3.Object(write_bucket, j + "/" + today + "/" + 'data.txt').put(Body=dataset_list[iterator])
    iterator += 1
    
#Initialize target bucket - will retreive most recent down
target_bucket = s3.Bucket(write_bucket)

# Bucket to use
bucket = s3.Bucket(write_bucket)

most_recent = []
previous = []
#For each dataset obtain the most recent file to check against current day's run
for i in datasets_suffix:
        all_obj_in_folder = list(s3.Bucket(write_bucket).objects.filter(Prefix=i))
        all_obj_in_folder.sort(key=lambda o: o.last_modified)
        print(all_obj_in_folder)
        try:
            most_recent.append("s3://" + write_bucket + "/" + all_obj_in_folder[-1].key)
            previous.append("s3://" + write_bucket + "/" + all_obj_in_folder[-2].key)
        except:
            print(i + " has no previous data")

#Compare previously downloaded dataset to current day's run
for i,j in zip(most_recent, previous):
    for x in datasets_suffix:
        curr = pd.read_fwf(i)
        prev = pd.read_fwf(j)
        combine_dfs = pd.concat([curr,prev], axis = 1)
        if len(combine_dfs) == len(curr):
            print("no changes to files")
        elif len(combine_dfs) != curr:
            combine_dfs.to_csv("s3://" + write_bucket + "/" + x + "/" + today + "/" + "data.csv")
            
job.commit()
