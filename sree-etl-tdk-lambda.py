#import awswrangler as wr
import pandas as pd
import urllib.parse
import os
import boto3
import re
import pytz
from io import StringIO
from datetime import datetime
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

#Temporary hard-coded AWS Settings; i.e. to be set as OS variable in Lambda
os_input_s3_raw_bucket = os.environ['s3_raw_bucket']
os_input_s3_clean_bucket = os.environ['s3_clean_bucket']
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']
os_input_source_folder = os.environ['source_folder']
os_input_destination_folder = os.environ['destination_folder']

#create s3 client, glue client and athena client using boto3 
s3=boto3.client('s3')
glue_client = boto3.client('glue')
athena_client = boto3.client('athena')

# function to split the string enclosed in two characters
def parse_str(x):
    """
    Returns the string delimited by two characters.

    Example:
        `>>> parse_str('[my string]')`
        `'my string'`
    """
    return x[1:-1]

# function to get the datetime formatted string
def parse_datetime(x):
    '''
    Parses datetime with timezone formatted as:
        `[day/month/year:hour:minute:second zone]`

    Example:
        `>>> parse_datetime('13/Nov/2015:11:45:42 +0000')`
        `datetime.datetime(2015, 11, 3, 11, 45, 4, tzinfo=<UTC>)`

    Due to problems parsing the timezone (`%z`) with `datetime.strptime`, the
    timezone will be obtained using the `pytz` library.
    '''
    dt = datetime.strptime(x[1:-7], '%d/%b/%Y:%H:%M:%S')
    dt_tz = int(x[-6:-3])*60+int(x[-3:-1])
    return dt.replace(tzinfo=pytz.FixedOffset(dt_tz))


# function to create the glue catalog table
def create_glue_table(database, table, s3_location):
   
    # Define the Glue catalog table schema 
    table_schema = [
        {'Name': 'ip_address', 'Type': 'string'},
        {'Name': 'rfc_1413_identity', 'Type': 'double'},
        {'Name': 'user_id', 'Type': 'bigint'},
        {'Name': 'time', 'Type': 'timestamp'},
        {'Name': 'status_code', 'Type': 'bigint'},
        {'Name': 'size', 'Type': 'bigint'},
        {'Name': 'referer', 'Type': 'string'},
        {'Name': 'user_agent', 'Type': 'string'},
        {'Name': 'request_method', 'Type': 'string'},
        {'Name': 'request_resource', 'Type': 'string'},
        {'Name': 'request_protocol', 'Type': 'string'}
        # Add more columns as needed
    ]

    # Define the partition key (date in this case)
    partition_key = [{'Name': 'date', 'Type': 'string'}]

    # Create or update the Glue table
    glue_client.create_table(
        DatabaseName=database,
        TableInput={
            'Name': table,
            'StorageDescriptor': {
                'Columns': table_schema,
                'Location': s3_location,
            },
            'PartitionKeys': partition_key,
        }
    )

# Checks if the glue catalog table already exists
def table_exists(database, table):
    
    try:
        glue_client.get_table(DatabaseName=database, Name=table)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False

# Insert the dataframes into the glue catalog table and store the data into s3
def write_to_glue_catalog(df, database, table_name, s3_bucket, s3_path):
    # Create a Spark context
    sc = SparkContext()
    glueContext = GlueContext(sc)

    # Convert Pandas DataFrame to Glue DynamicFrame
    dynamic_frame = glueContext.create_dynamic_frame.from_pandas_frame(df, glueContext, "dynamic_frame")

    # Write the DynamicFrame to Glue Catalog with partitioning by current_date
    glueContext.write_dynamic_frame.from_catalog(frame=dynamic_frame,
                                                  database=database,
                                                  table_name=table_name,
                                                  transformation_ctx="datasink",
                                                  format="parquet",
                                                  format_options={"compression": "SNAPPY"},
                                                  partition_keys=["current_date"])

    # Write the Parquet files to S3
    spark = SparkSession.builder.getOrCreate()
    df.write.parquet(f"s3://{s3_bucket}/{s3_path}/", compression="snappy", mode="overwrite")

    # Stop the Spark context
    sc.stop()


# copy the log files to the transformed folder and remove from to_be_processed_folder
def move_file_within_s3(bucket_name, source_folder, destination_folder, file_name):

    # Construct source and destination object keys
    source_key = f"{source_folder}/{file_name}"
    destination_key = f"{destination_folder}/{file_name}"

    # Copy the object within the S3 bucket
    s3.copy_object(Bucket=bucket_name,
                   CopySource={'Bucket': bucket_name, 'Key': source_key},
                   Key=destination_key,
                   ACL='private')

    # Delete the object from the source folder
    s3.delete_object(Bucket=bucket_name, Key=source_key)


def lambda_handler(event, context):

    Bucket=os_input_s3_raw_bucket
    Key="to_be_processed/"
    
    for file in s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
        print(file)
        file_key=file['Key']
        
        #process only .log files 
        if file_key.split('.')[-1] == "log":
        #if file['Key'] == "logs/2013-09-15.log":
            
            print("inside if : The file name is found ")
            try:
                s3_Bucket_Name = event["Records"][0]["s3"]["bucket"]["name"]
                s3_File_Name = event["Records"][0]["s3"]["object"]["key"]
                object = s3.get_object(Bucket=s3_Bucket_Name, Key=s3_File_Name)
                body = object['Body']
                csv_string = body.read().decode('utf-8')

                # Convert text data to a Pandas DataFrame
                data = pd.read_csv(
                StringIO(csv_string) ,
                sep=r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])',
                engine='python',
                na_values='-',
                header=None,
                usecols=[0, 1 , 2 ,3, 4, 5, 6, 7, 8],
                names=['IP_address', 'RFC_1413_identity', 'user_id', 'time', 'request', 'status_code', 'size', 'referer', 'user_agent'],
                converters={'time': parse_datetime,
                           'request': parse_str,
                           'status': int,
                           'size': int,
                           'referer': parse_str,
                           'user_agent': str})
                
                #split client request string to client requested method, requested resource and requested protocol
                request = data.pop('request').str.split()
                data['request_method'] = request.str[0]
                data['request_resource'] = request.str[1]
                data['request_protocol'] = request.str[2]
                
                data['user_agent'] = data['user_agent'].astype('string')
            
                #df=data.head(5)
                print("Dataframe read successfully")
                
                #store the data frame into df variable
                df=data
                
                # Create glue catalog table if not exists
                if table_exists(os_input_glue_catalog_db_name, os_input_glue_catalog_table_name):
                    print(f"Table {os_input_glue_catalog_db_name}.{os_input_glue_catalog_table_name} already exists.")
                else:
                    create_glue_table(os_input_glue_catalog_db_name,os_input_glue_catalog_table_name,os_input_s3_cleansed_layer)
    
                print(os_input_glue_catalog_db_name, os_input_glue_catalog_table_name, os_input_s3_cleansed_layer, df,os_input_write_data_operation)
                
                # Add a 'current_date' column
                df['current_date'] = datetime.now().strftime("%Y-%m-%d")

                # Write the DataFrame to Glue Catalog and S3
                write_to_glue_catalog(df, os_input_glue_catalog_db_name, os_input_glue_catalog_table_name, os_input_s3_clean_bucket, os_input_s3_cleansed_layer)
                
                # copy the processed file and delete from the source folder
                move_file_within_s3(os_input_s3_raw_bucket, os_input_source_folder, os_input_destination_folder, s3_File_Name)
            
                
            except Exception as e:
                
                print(e)
                print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(Key, Bucket))
                raise e   
                
        else:
            print("current dates log not found")
            