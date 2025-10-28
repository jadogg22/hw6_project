import argparse
import sys
from typing import NamedTuple

# chatGPt designed for configuration parsing based on needed command-line arguments for the comsumer program.

# Use a NamedTuple for strong typing and easy access to configuration settings
class ConsumerConfig(NamedTuple):
    """Configuration settings derived from command-line arguments."""
    storage_type: str
    bucket_2_name: str
    bucket_3_name: str
    dynamodb_table_name: str
    region_name: str
    polling_delay_ms: int

def parse_args() -> ConsumerConfig:
    """
    Parses command-line arguments to configure the Consumer program.
    """
    parser = argparse.ArgumentParser(
        description="Consumer program to process Widget Requests from S3 Bucket 2.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # --- REQUIRED ARGUMENTS ---
    parser.add_argument(
        '--storage-type',
        type=str,
        required=True,
        choices=['s3', 'dynamodb'],
        help="Storage strategy for created Widgets: 's3' (Bucket 3) or 'dynamodb'."
    )

    # --- RESOURCE ARGUMENTS ---
    parser.add_argument(
        '--bucket-2-name',
        type=str,
        required=True,
        default='jaden-hw6-widgets',
        help="Name of the S3 bucket containing Widget Requests (Bucket 2)."
    )

    parser.add_argument(
        '--bucket-3-name',
        type=str,
        required=False,
        default='jaden-hw6-requests',
        help="Name of the S3 bucket for storing created Widgets (Bucket 3). Required if --storage-type is 's3'."
    )

    parser.add_argument(
        '--dynamodb-table-name',
        type=str,
        required=False,
        default='jaden-hw6-widgets', 
        help="Name of the DynamoDB table for storing created Widgets. Required if --storage-type is 'dynamodb'."
    )

    # --- OPTIONAL/ENVIRONMENT ARGUMENTS ---

    parser.add_argument(
        '--region-name',
        type=str,
        default='us-east-1',
        help="AWS region where resources are located (e.g., 'us-east-1')."
    )
    
    parser.add_argument(
        '--polling-delay-ms',
        type=int,
        default=100,
        help="Polling delay in milliseconds when no request is available."
    )

    # Check for conditional requirements
    args = parser.parse_args()
    
    if args.storage_type == 's3' and not args.bucket_3_name:
        parser.error("--bucket-3-name is required when --storage-type is 's3'.")
        
    if args.storage_type == 'dynamodb' and not args.dynamodb_table_name:
        parser.error("--dynamodb-table-name is required when --storage-type is 'dynamodb'.")

    return ConsumerConfig(
        storage_type=args.storage_type,
        bucket_2_name=args.bucket_2_name,
        bucket_3_name=args.bucket_3_name,
        dynamodb_table_name=args.dynamodb_table_name,
        region_name=args.region_name,
        polling_delay_ms=args.polling_delay_ms
    )

if __name__ == '__main__':
    try:
        config = parse_args()
        print("Configuration Loaded Successfully:")
        print(config)
    except SystemExit:
        # argparse handles printing the error message and exiting
        pass
