
import boto3
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError
import json


# AWS Configuration
INPUT_BUCKET = "dag--bucket"
INPUT_FILE = "customers_data.csv"
STAGE_BUCKET = "dag-output-bucket"

# Function to establish a connection to AWS S3
def connect_to_s3():
    s3_client = boto3.client('s3') 
    return s3_client


# Function to fetch a CSV file from an S3 bucket and load it into a DataFrame
def fetch_csv_from_s3(s3_client, bucket_name, file_name):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    return df

# Function to upload a DataFrame as a gzipped CSV file to the S3 staging bucket
def upload_in_stage(bucket_name,stagefile,df,s3_client):
    df.to_csv(stagefile, index=False, compression="gzip")
    print(f"{stagefile} created")
    s3_client.upload_file(Bucket=bucket_name, Key=stagefile, Filename=stagefile)
    print(f"{stagefile} file uploaded in stage")
 
# Establish connections to S3 and Snowflake
s3_client = connect_to_s3()

# Fetch input data from S3 and load it into a DataFrame
df_data = fetch_csv_from_s3(s3_client, INPUT_BUCKET, INPUT_FILE)
print(df_data.shape)

# Prepare the data for staging and upload it to the staging S3 bucket
stage_details = [
    (df_data, "customers_data_k8.csv.gz")]
for df, stagefile in stage_details:
    upload_in_stage(STAGE_BUCKET,stagefile, df,s3_client)

# Load data from the staging S3 bucket
print('data loaded successfully')
