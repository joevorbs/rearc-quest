# Rearc Data Quest
### This repo contains the source code and information required to complete the take home data quest
----
#### Part 1: AWS S3 & Sourcing Datasets
The code for part 1 works as so:
- Download the data for the start day (in order to start the pipeline some initial data has to be saved)
- Store each dataset in its own respective folder in the bucket "bls-timeseries-data', which are partitioned by the run date
- The script then downloads the current day's data and compares it against to the previously downloaded data
- If there are differences in the file it will be saved and stored as the current file for that day, if not the original downloaded file will be saved
----
#### Part 2: APIs
This is a relatively straight forward python script that extracts us population data from datausa using their api

----
#### Part 3: Data Analytics
This notebook displays some analytics performed on the collected data as well as a few spot checks to ensure the proper report output was achieved

----
#### Part 4: Infrastructure as Code & Data Pipeline with AWS CDK
The code here contains a lambda function that runs both data pulls from part 1 and 2, as well as the cloudformation template (YAML) that triggers these to run daily





----
#### Notes
- Glue tables could have been used as the data store, if this was more of a data lake setup I think that might be a better approach
- Using bucket versioning and removing date partitions may be best approach if we can get away with a single file per dataset
