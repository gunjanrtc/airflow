import boto3
import pandas as pd
from io import StringIO
import snowflake.connector
from botocore.exceptions import ClientError
import json


# AWS Configuration
INPUT_BUCKET = "dag--bucket"
INPUT_FILE = "customers_data.csv"
STAGE_BUCKET = "dag-output-bucket"
DATABASE = "AIRFLOW"
TABLE_NAME = 'AIRFLOW.ETL.CUSTOMERS'

def get_snowflake_secret():
    secret_name = "snowflake-db-creds"
    region_name = "us-east-1"  # specify your region

    # Create a Secrets Manager client
    client = boto3.client(service_name='secretsmanager', region_name=region_name)

    try:
        # Retrieve the secret
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        # Decrypts secret and returns it as a string
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)  # Convert to dictionary if stored in JSON format

    except ClientError as e:
        raise Exception(f"Error retrieving secret: {e}")


# Function to establish a connection to AWS S3
def connect_to_s3():
    s3_client = boto3.client('s3')
    return s3_client

# Function to establish a connection to Snowflake
def connect_to_snowflake(databasename):
    conn = snowflake.connector.connect(
        user=creds["snowflake_user"],
        password=creds["snowflake_password"],
        account=creds["snowflake_account"],
        database=databasename,
        schema="PUBLIC"
    )
    return conn

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
def upload_in_stage(bucket_name,stagefile,df,s3_client):
    df.to_csv(stagefile, index=False, compression="gzip")
    print(f"{stagefile} created")
    s3_client.upload_file(Bucket=bucket_name, Key=stagefile, Filename=stagefile)
    print(f"{stagefile} file uploaded in stage")

# Establish connections to S3 and Snowflake
s3_client = connect_to_s3()
creds = get_snowflake_secret()
snowflake_connect = connect_to_snowflake(DATABASE)

# Fetch input data from S3 and load it into a DataFrame
df_data = fetch_csv_from_s3(s3_client, INPUT_BUCKET, INPUT_FILE)
print(df_data.shape)

# Prepare the data for staging and upload it to the staging S3 bucket
stage_details = [
    (df_data, "customers_data.csv.gz")]
for df, stagefile in stage_details:
    upload_in_stage(STAGE_BUCKET,stagefile, df,s3_client)

# Load data from the staging S3 bucket into Snowflake
cursor = snowflake_connect.cursor()
data_load_query = f""" copy into {TABLE_NAME} from '@LOAD_DATA/{stage_details[0][1]}'
 file_format = (format_name = LOAD_DATA)
 """
cursor.execute(data_load_query)
print('data loaded successfully')