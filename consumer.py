import boto3
import logging
import json
import time
import argparse
from config import parse_args
from get_widget import  S3RequestRetriever

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

S3_CLIENT = boto3.client('s3', region_name='us-east-1')
DYNAMODB_CLIENT = boto3.resource('dynamodb', region_name='us-east-1')

# --- Configuration ---
S3_BUCKET_NAME = 'jaden-hw6-widgets' # Bucket 3

s3_bucket_requests = "jaden-hw6-requests"
s3_bucket_widgets = "jaden-hw6-widgets"




def main():
    print("Consumer started")
    config = parse_args()

    wiget_retriver = S3RequestRetriever(bucket_name=config.bucket_2_name, region_name=config.region_name)
    loopCondition = True


    while(loopCondition):
        # slow down the loop
        time.sleep(5)

        # grab request
        request = wiget_retriver.get_and_delete_next_request()
        if request is None:
                continue


        print(f'Processing request: {request["requestId"]}')

        







if __name__ == "__main__":
    main()

