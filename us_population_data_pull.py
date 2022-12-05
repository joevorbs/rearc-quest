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
write_bucket = 'data-usa'

#Obtain data from datausa api
pop_data = re.get("https://datausa.io/api/data?drilldowns=Nation&measures=Population")

#Turn json from api call into a df
pop_data_df = pd.DataFrame(pop_data.json()['data'])

#Write data to s3
pop_data_df.to_json("s3://" + write_bucket + "/" + "pop_data.json")
