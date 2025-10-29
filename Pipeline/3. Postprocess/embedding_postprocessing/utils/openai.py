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

    def format_embeddings(self,embedding_df):
        # Format Embeddings List
        embeddings_batch_list = [self.format_embedding_batching(x) for x in embedding_df.to_dict('records')]

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

    def get_batch_output_as_df(self,file_id):
        # Get File
        file_response = self.client.files.content(file_id)

        # for Embeddings
        full = pd.read_json(path_or_buf=file_response, lines=True)

        # For Context
        full['context'] = full['response'].apply(lambda x: x['body']['data'][0]['embedding'])
        full['context'] = full['context'].apply(lambda x: self.convert_embedding(x))
        full['object_id'] = full['custom_id'].apply(lambda x: x.split('-')[1])

        full = full[['object_id', 'context']]
        full.columns = ['id', 'embedding']
        full['is_complete'] = True
        print(full)

        if len(full['embedding']) > 0:
            print(full['embedding'][0])
            print(type(full['embedding'][0]))

        return full

    def clean_embedding(self,value):

        if type(value) == str:
            value = json.loads(value)

        return {
            'id': value['custom_id'].split('-')[-1],
            'embedding': value['response']['body']['data'][0]['embedding'],
            'status':'complete',
            'updated_at':str(datetime.now())
        }

    def get_batch_output_as_list(self, file_id):
        print(file_id)
        # Get File
        file = self.client.files.content(file_id)
        print(file)
        print(file.response.text)

        # List
        embedding_list = [self.clean_embedding(x) for x in file.response.text.split('\n') if x != '']

        return embedding_list