import boto3
import json
import logging
from typing import Protocol, runtime_checkable, Dict, Any
from config import ConsumerConfig

logger = logging.getLogger(__name__)



@runtime_checkable
class StorageStrategy(Protocol):
    """Protocol for all storage mechanisms."""
    def store_widget(self, request_data: Dict[str, Any]) -> None:
        """Stores the widget data in the underlying service."""
        ...


class S3Storage:
    def __init__(self, config: ConsumerConfig):
        self.client = boto3.client('s3', region_name=config.region_name)
        self.bucket_name = config.bucket_3_name

    def store_widget(self, request_data: Dict[str, Any]) -> None:

        """
        Stores widget data in S3 bucket.
        """
        widget_id = request_data.get("widgetId")
        owner = request_data.get("owner")
        request_id = request_data.get("requestId")
        #type = request_data.get("type")

        # make sure the required fields are present
        try: 
            assert widget_id is not None, "widgetId is required in request_data"
            assert owner is not None, "owner is required in request_data"
            assert request_id is not None, "requestId is required in request_data"

        except AssertionError as e:
            logger.error(f"S3 Strategy Error: {e}")
            return None

        
        widget_data = {
                "widgetId": widget_id,
                "owner": request_data["owner"],
                "label": request_data["label"],
                "description": request_data["description"],
                "otherAttributes": request_data.get("otherAttributes", [])
                }
        # strip for spaces and lowercase the owner for the key
        ownerstr = owner.replace(" ", "-").lower()
        widget_key = f"widgets/{ownerstr}/{widget_id}"
        self.client.put_object(
                Bucket=self.bucket_name,
                Key=widget_key,
                Body=json.dumps(widget_data),
                ContentType="application/json"
        )
        logger.info(f"S3 Strategy: Storing widget {widget_id} in {self.bucket_name}")



class DynamoDBStorage:
    def __init__(self, config: ConsumerConfig):
        dyno = boto3.resource('dynamodb', region_name=config.region_name)
        self.table = dyno.Table(config.dynamodb_table_name)

    def store_widget(self, request_data: dict):
        """
        Stores widget data in a DynamoDB table, flattening otherAttributes.
    
        """
    
        # 1. Basic Validation (Same logic as your S3 example)
        widget_id = request_data.get("widgetId")
        owner = request_data.get("owner")
        request_id = request_data.get("requestId")
        type_check = request_data.get("type")

        try:
            assert widget_id is not None, "widgetId is required in request_data"
            assert owner is not None, "owner is required in request_data"
            assert request_id is not None, "requestId is required in request_data"

        except AssertionError as e:
            logger.error(f"DynamoDB Strategy Error: {e}")
            return None

        widget_data = {
                "widgetId": widget_id,
                "owner": request_data["owner"],
                "label": request_data["label"],
                "description": request_data["description"],
                "otherAttributes": request_data.get("otherAttributes", [])
                }
    
        try:
            
            self.table.put_item(Item=widget_data)
            logger.info(f"DynamoDB Strategy Success: Stored widget {widget_id} in table")
        except Exception as e:
            logger.error(f"DynamoDB Put Error: Failed to store widget {widget_id}. Error: {e}")
            raise




class Wiget_Processor:
    def __init__(self, config: ConsumerConfig):
        """
        Initialize the Widget Processor with the specified storage strategy.
        """
        if config.storage_type == 's3':
            self._storage_strategy: StorageStrategy = S3Storage(config)
        elif config.storage_type == 'dynamodb':
            self._storage_strategy: StorageStrategy = DynamoDBStorage(config)
        else:
            raise ValueError(f"Unsupported storage type: {config.storage_type}")
        

    def process(self, widget_data: dict):
        """
        Process the widget data and store it in the specified storage type.
        """

        request_type = widget_data.get('type')
        if request_type is None:
            logger.error("Error: 'type' field missing in payload. Skipping.")
            return
        request_type = request_type.lower()


        if request_type == 'create':
            logger.info("Processing: Widget Create Request...")
            self._storage_strategy.store_widget(widget_data)
        
        elif request_type == 'UPDATE':
            widget_id = widget_data.get("widgetId", "N/A")
            logger.warning(f"Warning: Received UPDATE request for widget {widget_id}. Skipping (Implementation pending for HW7).")
            
        elif request_type == 'DELETE':
            widget_id = widget_data.get("widgetId", "N/A")
            logger.warning(f"Warning: Received DELETE request for widget {widget_id}. Skipping (Implementation pending for HW7).")
            
        else:
            logger.error(f"Error: Unknown request type in payload: {request_type}. Skipping.")
