import boto3
import logging
from botocore.exceptions import ClientError

# set params
local_file_path = "{FILENAME}"
bucket_name = "{BUCKET NAME}"
s3_file= "{PATH/TO/FILE}"


logging.info("File Upload: STARTED")

# upload the file
s3 = boto3.client('s3')
try:
    s3.upload_file(local_file_path, bucket_name, s3_file)
    logging.info("File Upload: SUCCESS")
except ClientError as e:
    logging.error(e)
except FileNotFoundError:
    logging.error("ERROR: File not found")