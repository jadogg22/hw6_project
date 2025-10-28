import boto3
import logging
import json
import time
import argparse
from config import parse_args
from get_widget import  S3RequestRetriever
from widget_processor import Wiget_Processor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

S3_CLIENT = boto3.client('s3', region_name='us-east-1')
DYNAMODB_CLIENT = boto3.resource('dynamodb', region_name='us-east-1')

# --- Configuration ---
S3_BUCKET_NAME = 'jaden-hw6-widgets' # Bucket 3

s3_bucket_requests = "jaden-hw6-requests"
s3_bucket_widgets = "jaden-hw6-widgets"

def init_consumer():
    config = parse_args()
    wiget_retriver = S3RequestRetriever(bucket_name=config.bucket_2_name, region_name=config.region_name)
    widget_processor = Wiget_Processor(config)

    return wiget_retriver, widget_processor




def main():
    wiget_retriver, widget_processor = init_consumer()

    #main loop
    while(loopContinue := True):
        # slow down the loop
        time.sleep(1)

        # grab request
        request = wiget_retriver.get_and_delete_next_request()
        if request is None:
                continue

        #process request
        widget_processor.process(request)

if __name__ == "__main__":
    main()






if __name__ == "__main__":
    main()

