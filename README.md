# ETL Pipeline for Apache Access logs

### Overview
This project was created to load the daily apache access logs to AWS s3, cleanse, Transform and perform analysis to get the total users count, how many requests created and total number of requests.

### Project Goals
1. Data Load — Loaded the log files using aws s3 cli commands.
2. ETL System — We are getting data in raw log format, transforming this data into the proper format.
3. Staging — We will partition the data based on the current day and load them to glue catalog tables.
4. Target —Extracted the data from glue catalog tables, transform and load it to oracle db.
5. Cloud — We can’t process vast amounts of data on our local computer so we need to use the cloud, in this case, we will use AWS.
6. Reporting — Data scientists and other stake holders can directly fetch the statistics data from the oracle db.

### Architecture
![Architecture diagram](https://github.com/sreedharchalavadi/tdk-apache-log-etl/blob/main/TDK_ETL_ARCHITECTURE.png)

### Services used

1. **AWS S3 (Simple Storage Service) :** Its an object storage service that provides manufacturing scalability, data availability, security, and performance. Its a highly scalable object storage service that can store and retrieve any amount of data from anywhere on the web.It is commonly used to store and distribute large media files, data backups and static website files.
2. **AWS IAM :** This is nothing but identity and access management which enables us to manage access to AWS services and resources securely.
3. **AWS Glue :** A serverless data integration service that makes it easy to discover, prepare, and combine data for analytics, machine learning, and application development.
4. **AWS Glue Data Catalog :** AWS Glue Data Catalog is a fully managed metadata repository that makes it easy to discover and manage data in AWS. We can use the Glue Data Catalog with other AWS services, such as Athena
5. **AWS Lambda :** Lambda is a computing service that allows programmers to run code without creating or managing servers.We can use lambda to run code in response to events like changes in s3, DynamoDB, or other AWS services
6. **AWS Athena :** Athena is an interactive query service that makes it easy to analyze data in amazon s3 using standard SQL. We can use Athena to analyze data in Glue Data catalog or in other s3 buckets
7. **AWS Cloud Watch :** Amazon cloudwatch is a monitoring service for AWS resources and the applications that run on them. We can use CloudWatch to collect and track metrics, collect and monitor log files and set alarms.
8. **AWS Glue Studio ETL (Extract, Transform, Load) :** These jobs are a visual interface within AWS Glue that enables users to design and build data transformation workflows without writing code. It simplifies the process of data extraction, transformation, and loading by providing a drag-and-drop interface for creating and managing ETL pipelines.
9. **AWS RDS :** Created a database in oracle DB to store the analytical data so that Data scientists can access.
10.**SQL Developer :** To read the analytical data stored in oracle DB.

### Dataset Used
Apache HTTP server access logs in YYYY-MM-DD.log format.

## Project Execution Flow

#### Goal is to create an ETL pipeline to Load, Extract and Transform the appache http server access logs. So that we can create analytical reports on the requests.  

#### Steps Executed

1. created s3 buckets for the raw layer and cleansed layers, Below is the buckets and folder structure.
   raw layer : Bucket : sree-etl-tdk-raw-data
   sree-etl-tdk-raw-data/processed/
   sree-etl-tdk-raw-data/to_be_processed/

   cleansed_layer : Bucket : sree-etl-tdk-cleaned-data
   sree-etl-tdk-cleaned-data/cleaned_log/
   
2. loaded the data into the below folders using aws cli commands.
raw layer having to_be_processed folder contains the raw logs data loaded daily in YYYY-MM-DDD.log format.

3. Create the glue catalog database : sree_etl_tdk_clean_db . To store the cleansed data in staging.

4. Created the Cloudwatch trigger 1, This will trigger Lambda function daily at 11:45pm, whenever there is a new log file in the to_be_processed folder.

5. Created the lambda function :sree-etl-tdk-lambda. This lambda function will do the following tasks.
  -Extract the data from to_be_processed folder.
  -cleanse the data and creates the pandas dataframe in the desired format.
  -Creates the glue catalog tables : sree-etl-tdk-cleaned-data if not exists and loads the dataframes into the glue catalog table on the current date partition.
  -Converts the data into parquet format and stores in the s3 path : s3://sree-etl-tdk-cleaned-data/cleaned_log/
  -Removes the log file from the to_be_processed_folder to processed folder
   
6. In athena Query the glue catalog tables sree_etl_tdk_clean_db.sree-etl-tdk-cleaned-data to check how the data has been loaded in staging.

   SELECT count(*) FROM "sree_etl_tdk_clean_db"."sree_tdk_log_clean_table" where user_id is not null limit 10;
   SELECT user_id,count(*) FROM "sree_etl_tdk_clean_db"."sree_tdk_log_clean_table" group by user_id;
   SELECT count(*) FROM "sree_etl_tdk_clean_db"."sree_tdk_log_clean_table" where status_code=200;

8. Created Amazon rds oracle database, noted the oracle jdbc url.
   
9. Once data is in cleansed layer, created one more cloud watch trigger to initiate the glue studio etl.
    
11. Created a glue studio etl job :sree-etl-tdk-glue-job to transform the parquet files in the cleansed layer by accessing the glue catalog tables with latest partition.
   Perform the aggregation using pyspark scripts and store the final results to oracle db : sree-tdk-oracle-db

12. In pyspark code created two tables in oracle db and stored the data for the following.
  -tdk_user_requests_table : Contains the http requests counts per user
  -tdk_total_requests_table : Contains current date, total users, total successful requests. 

13. Connect to the oracle db using sql develop to query the analytical tables.
    
15. All the above process will be completed before 3:00AM CET, so that visualization application can connect to oracle db and access latest data.

