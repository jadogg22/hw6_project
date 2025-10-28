import boto3
import logging
import time
import os
from datetime import datetime
from config import parse_args
from get_widget import  S3RequestRetriever
from widget_processor import Wiget_Processor

# --- Logging Setup ---
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "consumer.log")

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file_path)
    ]
)

logger = logging.getLogger(__name__)


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
            logger.info("No new requests found. Waiting...")
            continue

        #process request
        widget_processor.process(request)

if __name__ == "__main__":
    main()






if __name__ == "__main__":
    main()

