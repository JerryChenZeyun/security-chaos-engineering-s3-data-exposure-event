###################################################################################
# This python file is to create a list of S3 buckets to store mock-up PII data.
# Then store the S3 bucket list in a DynamoDB table as a record for later usage.
# e.g. A clean-up python function can refer to DynamoDB table to delete S3 buckets.
###################################################################################
import logging
import boto3
from botocore.exceptions import ClientError
import random

# Specify the number of s3 buckets to be provisioned. Default value is 10.
NUM_OF_BUCKET = 1
bucket_name_list = []
REGION = "us-east-1"
PII_DATA_FILE = "mock-up-pii-data-500.csv"

# Retrieve the list of existing buckets within the AWS account
def s3_bucket_name_list():
    s3 = boto3.client('s3')
    response = s3.list_buckets()

    # Output the bucket names
    print("\n================== Existing buckets within the selected AWS account: ==================\n")
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')
    print("\n=======================================================================================\n")

# Retrieve the list of existing buckets within the AWS account
def s3_create_buckets(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
            print("s3_bucket has been created in default region")
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
            print("s3_bucket has been created in the specified region")
    except ClientError as e:
        logging.error(e)
        return False
    return True

# Upload the PII mock-up data to those s3 buckets
def s3_upload_pii_data(bucket_name, file_name):
    """
    Upload the mock-up PII csv files to specified s3 buckets
    """
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# Main function part. 
#
# Provision the s3 buckets
for i in range(NUM_OF_BUCKET):
    bucket_name = "my-pii-data-bucket-" + str(random.randint(1000000000, 9999999999))
    bucket_name_list.append(bucket_name)
    s3_create_buckets(bucket_name=bucket_name)

# Upload the PII mock-up data to those s3 buckets
for bucket_name in bucket_name_list:
    s3_upload_pii_data(bucket_name, "mock-up-pii-data-500.csv")
    print("PII data has been uploaded in s3 bucket: " + bucket_name )
    

    







