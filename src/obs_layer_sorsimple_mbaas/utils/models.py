"""utils/models.py"""

from pydantic import BaseModel, Field
from typing import List, Optional


class Attributes(BaseModel):
    ApproximateReceiveCount: str
    AWSTraceHeader: str
    SentTimestamp: str
    SequenceNumber: str
    MessageGroupId: str
    SenderId: str
    MessageDeduplicationId: str
    ApproximateFirstReceiveTimestamp: str

class Record(BaseModel):
    messageId: str
    receiptHandle: str
    body: str
    attributes: Attributes
    messageAttributes: Optional[dict] = Field(default_factory=dict)
    md5OfBody: str
    eventSource: str
    eventSourceARN: str
    awsRegion: str

class SQSEntry(BaseModel):
    Records: List[Record]
