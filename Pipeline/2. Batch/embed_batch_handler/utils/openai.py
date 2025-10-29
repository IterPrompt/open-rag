import io
import json
import pandas as pd
from openai import OpenAI
from datetime import datetime

class OpenAIUtil:
    def __init__(self):
        # Initiate Values
        self.client = OpenAI()

    def format_embedding_batching(self,value):
        # Format Batch Value
        batch_value = {
            "custom_id": f"request-{value['id']}",
            "method": "POST",
            "url": "/v1/embeddings",
            "body": {
                "model": "text-embedding-3-small",
                "input": value['context']
            }
        }
        return batch_value

    def format_embeddings(self,embeddings_list):
        # Format Embeddings List
        embeddings_batch_list = [self.format_embedding_batching(x) for x in embeddings_list]

        # Format Jsonl
        jsonl_data = "\n".join(json.dumps(record) for record in embeddings_batch_list)

        # List to Bytes
        jsonl_bytes = io.BytesIO(jsonl_data.encode('utf-8'))
        return jsonl_bytes

    def convert_embedding(self,value):
        if type(value) == str:
            return json.loads(value)
        else:
            return value

    def get_batch_status(self,batch_id):
        # Check if Batch has Completed
        batch_status = self.client.batches.retrieve(batch_id)
        return batch_status

    def clean_batch_values(self,value):

        if type(value) == str:
            value = json.loads(value)

        # Get Context
        embedding_id = value['custom_id'].split('-')[-1]

        # Get Json from Response
        extract_response = json.loads(value['response']['body']['choices'][0]['message']['content'])

        return {
            'embedding_id':embedding_id,
            'response':extract_response
        }

    def get_batch_output_as_list(self, file_id):
        print(file_id)
        # Get File
        file = self.client.files.content(file_id)
        print(file)

        # List
        batch_response_list = [self.clean_batch_values(x) for x in file.response.text.split('\n') if x != '']

        return batch_response_list


    def create_openai_batch(self,jsonl_bytes):
        # Create File
        batch_input_file = self.client.files.create(
            file=jsonl_bytes,
            purpose="batch"
        )

        # Create Batch
        batch_response = self.client.batches.create(
            input_file_id=batch_input_file.id,
            endpoint="/v1/embeddings",
            completion_window="24h",
            metadata={
                "description": f"Embedding Batch - Embedder: {datetime.now()}"
            }
        )

        # Get Batch Id
        batch_id = batch_response.id
        return batch_id