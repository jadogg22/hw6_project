import boto3
import json
import logging

logger = logging.getLogger(__name__)

class S3RequestRetriever:
    """
    Implements the strategy for retrieving a single Widget Request from S3 (Bucket 2).
    It reads the object with the smallest key, deletes it and returns the JSON payload.
    """
    def __init__(self, bucket_name: str = 'jaden-hw6-requests', region_name: str = 'us-east-1'):
        # Initialize the S3 client using the provided region
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.bucket_name = bucket_name

    def get_and_delete_next_request(self) -> dict or None:
        """
        Periodically tries to read a single Widget Request from Bucket 2.

        Basic s3 object getter from stack overlow modded to fit requirements

        Returns:
            dict: The parsed JSON request if successful.
            None: If no request is available.
        """
        try:
            # list object and max keys 1 to get the smallest key object 
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                MaxKeys=1
            )
        except Exception as e:
            logger.error(f"Error listing objects in bucket: {e}")

        # Check if any objects were found
        if 'Contents' not in response:
            logger.info("No requests found in the bucket.")
            return None  
         # Get the key of the first object
        request_key = response['Contents'][0]['Key']
        logger.info(f"Found and attempting to retrieve request: {request_key}")

        # 2. Read the Request Object
        obj = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key=request_key
        )
 
        # Read and decode the body content
        request_body = obj['Body'].read().decode('utf-8')

            # 3. Process the Request 
        try:
            widget_request = json.loads(request_body)
            logger.info(f"Successfully unfurled (parsed) widget request from {request_key}")
        except json.JSONDecodeError:
            logger.warning(f"Warning: Malformed JSON found in request key: {request_key} returning nothing")
            return None

            #4. Delete the Request Object
        try:
            self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=request_key
            )
            logger.info(f"Successfully deleted request: {request_key}")
        except Exception as delete_e:
                logger.error(f"Failed to delete request {request_key}. Error: {delete_e}")
                return None # Or handle as appropriate if deletion is critical

        return widget_request
            

