# Rearc Data Quest
### This repo contains the source code and information required to complete the take home data quest
----
#### Part 1: AWS S3 & Sourcing Datasets
The code for part 1 works as so:
- Download the data for the start day (in order to start the pipeline some initial data has to be saved, in this approach)
- Store each dataset in its own respective folder in the bucket "bls-timeseries-data', which are partitioned by the run date
- The script then downloads the current day's data and compares it against the previous day's data
- If there are differences in the file it will be saved and stored as the current file for that day, if not the original downloaded file will remain
- All the buckets/files used in this project can be found here: https://s3.console.aws.amazon.com/s3/buckets?region=us-east-1&region=us-east-1
----
#### Part 2: APIs
This is a relatively straight forward python script that extracts US population data from datausa using their api

----
#### Part 3: Data Analytics
This notebook displays some analytics performed on the collected data as well as a few spot checks to ensure the proper report output was achieved

----
#### Part 4: Infrastructure as Code & Data Pipeline with AWS CDK
The code for this section contains a lambda function that runs both data pulls from part 1 and 2, as well as the cloudformation template (YAML) that creates the lambda that runs on a daily schedule
- The lambda function first triggers the 2 data pulls
- Once the BLS data pull is complete, the lambda function then triggers the script to generate the report from task 3
----
#### Notes
- Glue tables could have been used as the data store, if this was more of a data lake setup I think that might be a better approach
- Using bucket versioning and removing date partitions may be best approach if we can get away with a single file per dataset for the 2 extracts
- I decided to hard code the 3 main datasets in the BLS data pull because the only tool I thought to use was beautifulsoup/parsing the HTML on the page which didn't see like the appropriate method to download the data.
- With more time, I would make the analytics job trigger more elegant in the lambda function (I included a 10 second implicit sleep, would build out the conditional statement more, etc.)
- With more time I would also like to build out the SQS queue section of the architecture.
- This project was a lot of fun, provided a challenge, and I'm eager to hear back from the team and learn more.
----
#### Resources
- CF: https://www.sentiatechblog.com/defining-a-scheduled-lambda-in-cloudformation
- Lambda: https://aws.amazon.com/premiumsupport/knowledge-center/start-glue-job-crawler-completes-lambda/
