import os
import json
from boto3 import client as boto3_client

class Lambda:
    def __init__(self):
        pass

    def invoke_update_embeddings(self, batch_id,is_last=False):
        # Product Page Payload
        payload = {
            'batch_id': batch_id,
            'is_last':is_last
        }

        # Connect to Lambda
        lambda_client = boto3_client('lambda', region_name="ap-southeast-1",
                                     aws_access_key_id=os.environ['LAMBDA_AWS_ACCESS_KEY'],
                                     aws_secret_access_key=os.environ['LAMBDA_AWS_SECRET_ACCESS_KEY'], )

        try:
            # Invoke Each Prompt
            responseValues = lambda_client.invoke(
                FunctionName=f'arn:aws:lambda:ap-southeast-1:931690842494:function:onlyvectors-update-embeddings',
                InvocationType='Event',
                Payload=json.dumps(payload)
            )
            print(responseValues)
        except Exception as e:
            # handle the exception
            print("An exception occurred:", e)