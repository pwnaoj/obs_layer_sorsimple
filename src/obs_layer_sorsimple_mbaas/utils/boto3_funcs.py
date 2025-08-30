import boto3
import json
import jmespath

from botocore.exceptions import ClientError
from .settings import (
    BUCKET_NAME, 
    OBJECT_NAME, 
    QUEUE_URL_LIST
)


def from_s3_get_file(bucket_name: str = BUCKET_NAME, object_name: str = OBJECT_NAME):
    """_summary_

    Args:
        bucket_name (str, optional): _description_. Defaults to BUCKET_NAME.
        object_name (str, optional): _description_. Defaults to OBJECT_NAME.

    Raises:
        Exception: _description_
        Exception: _description_

    Returns:
        _type_: _description_
    """
    s3_client = boto3.client('s3')
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        file_content = response['Body'].read().decode('utf-8')
        params = json.loads(file_content)

        return params
    except ClientError as e:
        raise ClientError(f"Error al obtener el archivo JSON: {e}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decodificando el archivo JSON: {e}")

def send_message_to_sqs(message: str, queue_url_list: list = QUEUE_URL_LIST):
    """_summary_

    Args:
        message (str): _description_
        queue_url_list (list, optional): _description_. Defaults to QUEUE_URL_LIST.

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """
    sqs_client = boto3.client('sqs')
    responses = []

    try:
        for url in queue_url_list:
            prefix = url.split("/")[-1].strip()
            response = sqs_client.send_message(QueueUrl=url, MessageBody=json.dumps(message), MessageGroupId=f"{prefix}-{json.loads(message)['jsonPayload.dataObject.consumer.appConsumer.sessionId']}")
            
            responses.append(response)

        return responses
    except ClientError as e:
        raise ClientError(f"Error al enviar el mensaje a la cola SQS '{url}': {e}")
