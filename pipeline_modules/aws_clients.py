"""
AWS Clients setup module.
Creates boto3 clients for S3 and Textract.
"""

import boto3
from pipeline_modules.config import AWS_REGION

# AWS clients
s3_client = boto3.client("s3", region_name=AWS_REGION)
textract_client = boto3.client("textract", region_name=AWS_REGION)
