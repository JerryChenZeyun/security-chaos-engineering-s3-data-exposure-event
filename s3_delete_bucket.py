###################################################################################
# In this code, we first create an S3 client and use it to list all S3 buckets 
#   using the list_buckets() method.
# We then iterate over the list of buckets and check if the bucket name starts with 
#   "test-bucket-" using the startswith() method.
# If the bucket name does start with "test-bucket-", we first delete all objects in 
#   the bucket using the list_objects_v2() and delete_objects()
###################################################################################
import boto3

# Create an S3 client
s3 = boto3.client('s3')

# List all S3 buckets
response = s3.list_buckets()

# Iterate over the list of buckets
for bucket in response['Buckets']:
    bucket_name = bucket['Name']
    # Check if the bucket name starts with "my-pii-data-bucket-"
    if bucket_name.startswith('my-pii-data-bucket-'):
        # Delete all objects in the bucket
        objects = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
            s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
            print(f"Deleted objects in bucket {bucket_name}")
        # Delete the bucket
        s3.delete_bucket(Bucket=bucket_name)
        print(f"Deleted bucket {bucket_name}")




    