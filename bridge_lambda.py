import json
import os
from typing import Dict, Any, Tuple, Optional
from dataclasses import dataclass
from functools import partial
import boto3
from botocore.exceptions import ClientError



@dataclass(frozen=True)
class Config:
    """Configuration for the Lambda function."""
    sqs_queue_url: str = os.environ.get('SQS_QUEUE_URL', '')

class QueueError(Exception):
    """Custom exception for SQS queue operations."""
    pass

def parse_query_parameters(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract query string parameters from the ALB event.
    
    Args:
        event: ALB event dictionary
        
    Returns:
        Dictionary containing query parameters
    """
    return event.get('queryStringParameters', {}) or {}

def send_to_sqs(
    sqs_client: boto3.client,
    queue_url: str,
    message: Dict[str, Any]
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Send message to SQS queue.
    
    Args:
        sqs_client: Boto3 SQS client
        queue_url: URL of the target SQS queue
        message: Message to be sent
        
    Returns:
        Tuple containing:
        - Success flag (True/False)
        - Message ID on success or None on failure
        - Error message on failure or None on success
    """
    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        return True , response['MessageId'], None
    except ClientError as e:
        return False, None, f"Failed to send message to SQS: {str(e)}"
        
## temporary
def send_to_sqs_temp(
    sqs_client: boto3.client,
    queue_url: str,
    message: Dict[str, Any]
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Send message to SQS queue.
    
    Args:
        sqs_client: Boto3 SQS client
        queue_url: URL of the target SQS queue
        message: Message to be sent
        
    Returns:
        Tuple containing:
        - Success flag (True/False)
        - Message ID on success or None on failure
        - Error message on failure or None on success
    """
    return True , 'MessageId', None
    # try:
    #     response = sqs_client.send_message(
    #         QueueUrl=queue_url,
    #         MessageBody=json.dumps(message)
    #     )
    #     return True , response['MessageId'], None
    # except ClientError as e:
    #     return False, None, f"Failed to send message to SQS: {str(e)}"

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create ALB response dictionary.
    
    Args:
        status_code: HTTP status code
        body: Response body dictionary
        
    Returns:
        ALB response dictionary
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body)
    }

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler function.
    
    Args:
        event: ALB event dictionary
        context: Lambda context object
        
    Returns:
        ALB response dictionary
    """
    config = Config()
    
    # Validate configuration
    if not config.sqs_queue_url:
        return create_response(500, {
            'message': 'Missing SQS queue URL configuration'
        })
    
    # Process the request
    sqs_client = boto3.client('sqs')
    query_params = parse_query_parameters(event)
    # print(event)
    
    # Send to SQS
    success, message_id, error = send_to_sqs(
        sqs_client,
        config.sqs_queue_url,
        query_params
    )
    # success, message_id, error = send_to_sqs_temp(
    #     sqs_client,
    #     config.sqs_queue_url,
    #     query_params
    # )
    
    if success:
        return create_response(200, {
            'message': "Weekly summary draft and MyChart message draft successfully requested. Please allow 10-15 seconds delay and move to Chart Review and look for the drafts."
            # 'message': str(query_params)
        })
    else:
        return create_response(500, {
            'message': error
        })