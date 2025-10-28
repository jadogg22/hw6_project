import boto3
import json
from typing import Protocol, runtime_checkable, Dict, Any
from config import ConsumerConfig



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
            print(f"S3 Strategy Error: {e}")
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
        print(f"S3 Strategy: Storing widget {widget_data.get('widget_id')} in {self.bucket_name}")



class DynamoDBStorage:
    def __init__(self, config: ConsumerConfig):
        self.client = boto3.resource('dynamodb', region_name=config.region_name)
        self.table_name = config.dynamodb_table_name

    def store_widget(self, widget_data: dict):
        # implementing this later
        print(f"DynamoDB Strategy: Storing widget {widget_data.get('widget_id')} in {self.table_name}")



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
            print("Error: 'type' field missing in payload. Skipping.")
            return
        request_type = request_type.lower()


        if request_type == 'create':
            print("Processing: Widget Create Request...")
            self._storage_strategy.store_widget(widget_data)
        
        elif request_type == 'UPDATE':
            print("Warning: Received UPDATE request. Skipping (Implementation pending for HW7).")
            
        elif request_type == 'DELETE':
            print("Warning: Received DELETE request. Skipping (Implementation pending for HW7).")
            
        else:
            print(f"Error: Unknown request type in payload: {request_type}. Skipping.")
