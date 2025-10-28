import boto3
import json

class S3RequestRetriever:
    """
    Implements the strategy for retrieving a single Widget Request from S3 (Bucket 2).
    It reads the object with the smallest key, deletes it, and returns the JSON payload.
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

            # Check if any objects were found
            if 'Contents' not in response:
                print("No requests found in the bucket.")
                return None  

            # Get the key of the first object
            request_key = response['Contents'][0]['Key']

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
            except json.JSONDecodeError:
                print(f"Warning: Malformed JSON found in request key: {request_key} returning nothing")
                return None

            

             #4. Delete the Request Object
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=request_key
             )

            print(f"Successfully retrieved and deleted request: {request_key}")
            return widget_request

        except Exception as e:
            # Handle other unexpected errors
            print(f"An unexpected error occurred: {e}")
            return None
