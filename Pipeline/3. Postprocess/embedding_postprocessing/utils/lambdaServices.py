import os
import json
from boto3 import client as boto3_client

class Lambda:
    def __init__(self):
        pass

    def initiate_scraper(self, url, object_type):
        # Product Page Payload
        payload = {
            'url': url
        }

        # Connect to Lambda
        lambda_client = boto3_client('lambda', region_name="ap-southeast-1",
                                     aws_access_key_id=os.environ['LAMBDA_AWS_ACCESS_KEY'],
                                     aws_secret_access_key=os.environ['LAMBDA_AWS_SECRET_ACCESS_KEY'], )

        try:
            # Invoke Each Prompt
            responseValues = lambda_client.invoke(
                FunctionName=f'arn:aws:lambda:ap-southeast-1:931690842494:function:ph-{object_type}-scraper',
                InvocationType='Event',
                Payload=json.dumps(payload)
            )
        except Exception as e:
            # handle the exception
            print("An exception occurred:", e)