import configparser
import boto3
def load_to_s3(local_file_name):
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get("aws_boto_credentials","access_key")
    secret_key = parser.get("aws_boto_credentials","secret_key")
    bucket_name = parser.get("aws_boto_credentials","bucket_name")

    s3_client = boto3.client("s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)

    s3_client.upload_file(local_file_name,bucket_name,local_file_name)



