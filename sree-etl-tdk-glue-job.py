import sys
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.conf import SparkConf
from datetime import datetime

# Initialize Spark context and session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue job
job = Job(glueContext)
job.init("sree-etl-tdk-glue-job") 

# create the Oracle connection string from aws end point
oracle_connection_string = "sree-tdk-oracle-db.dummy.us-east-1.rds.amazonaws.com"

# Define Oracle database connection properties
oracle_properties = {
    "user": "sreedhar",
    "password": "test1234",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Get current date in the format 'YYYY-MM-DD'
current_date = datetime.now().strftime('%Y-%m-%d')

# Define Glue Catalog table database and name
glue_database = "sree_etl_tdk_clean_db"
glue_table_name = "sree_tdk_log_clean_table"

# Define the push_down_predicate to filter the Glue Catalog partition for the current day
push_down_predicate = f"date = '{current_date}'"

# create a dynamic frame to Read the Glue Catalog table partition for the current day
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=glue_database,
    table_name=glue_table_name,
    push_down_predicate=push_down_predicate
)

# Convert DynamicFrame to a Spark DataFrame
source_df = dynamic_frame.toDF()

# To get the total number of users
user_count = source_df.select('user_id').distinct().count()

# create an aggregate function to get the requests count by user id
requests_per_user = (
    source_df
    .groupBy('user_id')
    .agg({'client_requested_resource': 'count'})
    .withColumnRenamed('count(client_requested_resource)', 'requests_count')
)

#Filter the total successful request in the df
total_successful_requests = source_df.filter(source_df['status_code'] == 200).count()

# Display results
print("Total number of users:", user_count)
print("Requests per user:")
requests_per_user.show()
print("Total number of successful requests:", total_successful_requests)

# Write the requests_per_user dataframe to Oracle database
requests_per_user.write.jdbc(
    url=oracle_connection_string,
    table="tdk_user_requests_table",
    mode="overwrite",
    properties=oracle_properties
)

# Create a Row with the data for the request table dataframe
data_row = Row(current_date=current_date, user_count=user_count, total_successful_requests=total_successful_requests)

# Define the schema for the request table dataframe
schema = ["current_date STRING", "user_count INT", "total_successful_requests INT"]

# Create total request DataFrame from the calculated values
total_results_df = spark.createDataFrame([data_row], schema=schema)

# Write the results DataFrame to Oracle database
total_results_df.write.jdbc(
    url=oracle_connection_string,
    table="tdk_total_requests_table",
    mode="append",  
    properties=oracle_properties
)

# Stop the Spark session (this is necessary in a Glue job)
spark.stop()

# Commit the job (final step)
job.commit()
