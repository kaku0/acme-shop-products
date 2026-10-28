"""
TableUpdateFunction
"""


import os
from typing import List
import boto3
from boto3.dynamodb.types import TypeDeserializer
from aws_lambda_powertools.tracing import Tracer
from aws_lambda_powertools.logging.logger import Logger
from ecom.eventbridge import ddb_to_event # pylint: disable=import-error


ENVIRONMENT = os.environ["ENVIRONMENT"]
EVENT_BUS_NAME = os.environ["EVENT_BUS_NAME"]


eventbridge = boto3.client("events") # pylint: disable=invalid-name
type_deserializer = TypeDeserializer() # pylint: disable=invalid-name
logger = Logger() # pylint: disable=invalid-name
tracer = Tracer() # pylint: disable=invalid-name


@tracer.capture_method
def send_events(events: List[dict]):
    """
    Send events to EventBridge

    Each event type is published with its source and detail-type stated
    explicitly, one event per call.
    """

    logger.info("Sending %d events to EventBridge", len(events))
    for event in events:
        if event["DetailType"] == "ProductCreated":
            eventbridge.put_events(Entries=[{
                **event,
                "Source": "ecommerce.products",
                "DetailType": "ProductCreated",
                "EventBusName": EVENT_BUS_NAME
            }])
        elif event["DetailType"] == "ProductModified":
            eventbridge.put_events(Entries=[{
                **event,
                "Source": "ecommerce.products",
                "DetailType": "ProductModified",
                "EventBusName": EVENT_BUS_NAME
            }])
        elif event["DetailType"] == "ProductDeleted":
            eventbridge.put_events(Entries=[{
                **event,
                "Source": "ecommerce.products",
                "DetailType": "ProductDeleted",
                "EventBusName": EVENT_BUS_NAME
            }])


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def handler(event, _):
    """
    Lambda function handler for Products Table stream
    """

    logger.debug({
        "message": "Input event",
        "event": event
    })

    logger.debug({
        "message": "Records received",
        "records": event.get("Records", [])
    })

    events = [
        ddb_to_event(record, EVENT_BUS_NAME, "ecommerce.products", "Product", "productId")
        for record in event.get("Records", [])
    ]

    logger.info("Received %d event(s)", len(events))
    logger.debug({
        "message": "Events processed from records",
        "events": events
    })

    send_events(events)
