import itertools
import tiktoken
import json
from datetime import datetime
from collections import defaultdict

class Formatter:
    def __init__(self):
        pass

    def num_tokens_from_string(string: str, encoding_name: str) -> int:
        """Returns the number of tokens in a text string."""
        encoding = tiktoken.get_encoding(encoding_name)
        num_tokens = len(encoding.encode(string))
        return num_tokens
    
    def format_metadata(self, metadata):
        print(metadata)
        metadata_text = ''
        for key, item in metadata.items():
            if type(item) == str:
                if 'http' not in str(item) and item != None and item != '' and key not in ['id', 'context']:
                    metadata_text += f"{key.title()}: {item}; "
            elif type(item) == list:
                if len(item) > 0:
                    if type(item[0]) == dict:
                        for value in item:
                            metadata_text += f"{key}:\n"
                            for key1, item1 in value.items():
                                if 'http' not in str(item1) and item1 != None and item1 != '' and key1 not in ['id', 'context']:
                                    metadata_text += f"{key1.title()}: {item1}; "
                    else:
                        metadata_text += f"{key.title()}: {', '.join(item)}\n\n"
            elif type(item) == dict:
                for key1, item1 in metadata.items():
                    if 'http' not in str(item1) and item1 != None and item1 != '' and key1 not in ['id', 'context']:
                        metadata_text += f"{key1.title()}: {item1}; "
            else:
                if item != None and item != '' and key not in ['id', 'context']:
                    metadata_text += f"{key.title()}: {item}; "
        metadata_text += f"\n\n"
        print(metadata_text)
        return metadata_text

    def clean_meta_lists(self,value):

        if isinstance(value,list):
            return value
        elif isinstance(value,str):
            value = value.replace('?,','?')

            if '? ' in value:
                return [f"{x}?" for x in value.split('? ')]
            if '?' in value:
                return [f"{x}?" for x in value.split('?')]
            return f"{value}"
        else:
            return f"{value}"

    def clean_batch_context(self, batch_response, embeddings_content):

        # Get Context
        embedding_id = int(batch_response['embedding_id'])

        # Get Json from Response
        extract_response = batch_response['response']

        # Format Context
        context = ''
        metadata = {}

        # Format Metadata
        if 'metadata' in embeddings_content[embedding_id].keys():
            # Format Metadata
            metadata = json.loads(embeddings_content[embedding_id]['metadata']) if type(
                embeddings_content[embedding_id]['metadata']) == str else embeddings_content[embedding_id]['metadata']
            metadata_text = self.format_metadata(metadata)

            # Add Question
            if 'question' in metadata.keys():
                context += f"Question: {metadata['question']}\n"

            # Add Content
            context += f"{embeddings_content[embedding_id]['context']}\n\n"

            # Add Metadata
            context += f"Metadata:\n{metadata_text}\n\n"
        else:
            # Add Content
            context += f"{embeddings_content[embedding_id]['context']}\n\n"

        # Add Rephrased Content
        if 'content' in extract_response.keys():
            context += f"Rephrased Content: {extract_response['content']}\n"

        # Add Additional Sample Questions
        if 'questions' in extract_response.keys():
            try:
                context += f"Additional Sample Questions: {' '.join(extract_response['questions'])}\n"
            except:
                context += f"Additional Sample Questions: {extract_response['questions']}\n"

            metadata['questions'] = self.clean_meta_lists(extract_response['questions'])

        # Add Keywords
        if 'keywords' in extract_response.keys():
            context += f"Keywords: {' '.join(extract_response['keywords']) if isinstance(extract_response['keywords'],(dict,list)) else extract_response['keywords']}"
            metadata['keywords'] = self.clean_meta_lists(extract_response['keywords'])

        return {
            'id': embedding_id,
            'context': context,
            'metadata':metadata,
            'updated_at': str(datetime.now())
        }

    def format_embeddings_list(self, batch_embedding_list, embeddings_context):

        # Merge Batch Values with Current Embeddings Context
        return [self.clean_batch_context(batch_response,embeddings_context) for batch_response in batch_embedding_list]

    def chunks(self,iterable, size):
        it = iter(iterable)
        chunk = tuple(itertools.islice(it, size))
        while chunk:
            yield chunk
            chunk = tuple(itertools.islice(it, size))

    def group_metadata_by_content_id(self, data):
        grouped_data = defaultdict(lambda: {'metadata': None})

        for entry in data:
            entry_id = entry['content_id']
            if grouped_data[entry_id]['metadata'] is None:
                # Only assign the metadata once if it hasn't been set
                grouped_data[entry_id]['metadata'] = entry['metadata']
            else:
                metadata = grouped_data[entry_id]['metadata']
                for key, value in entry['metadata'].items():
                    if metadata.get(key,None) is None:
                        metadata[key] = entry['metadata'][key]
                    elif type(metadata[key]) != type(entry['metadata'][key]):
                        metadata[key] = entry['metadata'][key]
                    else:
                        if type(value) == list:
                            metadata[key] = list(set(metadata[key] + entry['metadata'].get(key, [])))
                        elif type(value) == dict:
                            metadata[key].update(entry['metadata'].get(key, {}))
                        else:
                            metadata[key] = entry['metadata'].get(key, '')

        # Convert defaultdict back to a list of dictionaries
        result = [{'id': key, 'metadata': value['metadata']} for key, value in grouped_data.items()]

        # Output result
        return result