import tiktoken
import itertools
from langchain_text_splitters import RecursiveCharacterTextSplitter
from datetime import datetime

class Formatter:
    def __init__(self):
        pass

    def num_tokens_from_string(self, string: str, encoding_name: str) -> int:
        """Returns the number of tokens in a text string."""
        encoding = tiktoken.get_encoding(encoding_name)
        num_tokens = len(encoding.encode(string))
        return num_tokens

    def get_context(self, values, context_columns):
        #
        context_values = []

        # Add Values to Context
        for column in context_columns:
            context_values.append(f"{column.upper()}: {values[column]}; ")

        return '\n'.join(context_values)

    def text_splitter(self, text):

        # Initiate Splitter
        splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            encoding_name="cl100k_base", chunk_size=400, chunk_overlap=200
        )

        # Get Chunks
        texts = splitter.split_text(text)

        return texts

    def format_metadata(self, metadata):
        print(metadata)
        metadata_text = ''
        for key, item in metadata.items():
            if type(item) == str:
                if 'http' not in str(item) and item != None and item != '' and key not in ['id', 'context']:
                    metadata_text += f"{key.title()}: {item}; "
            elif type(item) == list:
                for value in item:
                    if type(value) == dict:
                        metadata_text += f"{key}:\n"
                        for key1, item1 in value.items():
                            if 'http' not in str(item1) and item1 != None and item1 != '' and key1 not in ['id', 'context']:
                                metadata_text += f"{key1.title()}: {item1}; "
                    else:
                        metadata_text += f"{key.title()}: {value} "
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


    def format_embedding_values(self, content_chunk, values):

        final_values = {}

        # Set Context
        final_values['context'] = content_chunk
        final_values['tokens'] = self.num_tokens_from_string(content_chunk, "cl100k_base")
        final_values['metadata'] = values['metadata']
        final_values['content_id_id'] = values['id']
        final_values['type_id_id'] = values['type_id']
        final_values['user_id_id'] = values['user_id_id']
        final_values['application_id_id'] = values['application_id_id']
        final_values['status'] = 'add to batch'
        final_values['posted_at'] = str(datetime.now())
        final_values['updated_at'] = str(datetime.now())
        final_values['created_at'] = str(datetime.now())

        if 'question' in values['metadata'].keys():
            final_values['context'] = f"Question: {values['metadata']['question']}\nAnswer: {final_values['context']}"

        if len(values['metadata'].keys()) > 0:
            # Format Metadata
            metadata_text = self.format_metadata(values['metadata'])

            # Add to Context
            final_values['context'] = f"{final_values['context']}\n\nMetadata:{metadata_text}"

        return final_values

    def format_embedding_update_values(self,values):
        final_values = {}

        # Set Content and Context
        final_values['context'] = values['context']
        final_values['tokens'] = values['tokens']
        final_values['metadata'] = values['metadata']
        final_values['type_id_id'] = values['type_id_id']
        final_values['application_id_id'] = values['application_id_id']
        final_values['status'] = 'add to batch'
        final_values['updated_at'] = str(datetime.now())

        return final_values

    def content_to_embeddings_list(self,values):

        # Initiate Formatter
        formatter = Formatter()

        # Get Content
        content = values['content']

        # Split Content into Embeddings
        content_split = formatter.text_splitter(content)

        # Format for Embeddings
        embeddings_list = [formatter.format_embedding_values(x, values) for x in content_split]

        return embeddings_list

    def chunks(self,iterable, size):
        it = iter(iterable)
        chunk = tuple(itertools.islice(it, size))
        while chunk:
            yield chunk
            chunk = tuple(itertools.islice(it, size))