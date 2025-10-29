import tiktoken
import itertools
from langchain_text_splitters import RecursiveCharacterTextSplitter
from datetime import datetime
import hashlib

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

    def text_splitter(self, text, chunk_size=400, chunk_overlap=200):

        # Initiate Splitter
        splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            encoding_name="cl100k_base", chunk_size=chunk_size, chunk_overlap=chunk_overlap
        )

        # Get Chunks
        texts = splitter.split_text(text)

        return texts
        
    CTRL = dict.fromkeys(c for c in range(32) if c not in (9,10,13))  # drop control chars except \t\n\r

    def normalize_text(s: str) -> str:
        s = unicodedata.normalize("NFC", s)
        s = s.translate(CTRL).replace("\r\n", "\n").replace("\r", "\n")

        # HTML to text (if needed)
        if "<" in s and ">" in s:
            soup = BeautifulSoup(s, "lxml")
            for tag in soup(["script","style","noscript"]): tag.decompose()
            for a in soup.find_all("a"):
                if a.string and a.get("href"):
                    a.replace_with(f"{a.get_text(' ', strip=True)} ({a['href']})")
            for img in soup.find_all("img"):
                alt = img.get("alt")
                if alt: img.replace_with(f"[image: {alt}]")
            s = soup.get_text("\n", strip=False)

        # Fix PDF hyphenation at line breaks: word- \n word → wordword
        s = re.sub(r"(\w)-\n(\w)", r"\1\2", s)

        # Normalize fancy quotes/dashes
        s = s.translate(str.maketrans({
            "“":"\"", "”":"\"", "‘":"'", "’":"'", "—":"-", "–":"-"
        }))

        # Collapse whitespace but keep paragraph breaks
        s = re.sub(r"[ \t]+", " ", s)
        s = re.sub(r"\n{3,}", "\n\n", s)
        s = "\n".join(line.strip() for line in s.split("\n"))
        return s.strip()

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

    def format_content_chunk(self, content_chunk, content_id):

        # Format Content Chunk
        content_chunk_list = []
        for idx, chunk in enumerate(content_chunk):
            content_chunk_list.append({
                'context_version_id': content_id,
                'idx': idx,
                'text': chunk,
                'text_sha256': hashlib.sha256(chunk.encode()).hexdigest(),
                'token_count': self.num_tokens_from_string(chunk, "cl100k_base")
            })
        return content_chunk_list

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

    def format_embedding_update_values(self, values):
        # Copy required keys from input values
        keys_to_copy = ['context', 'tokens', 'metadata', 'type_id_id', 'application_id_id']
        final_values = {key: values[key] for key in keys_to_copy}
        
        # Add/override with update-specific fields
        final_values.update({
            'status': 'add to batch',
            'updated_at': str(datetime.now())
        })
        
        return final_values

    def content_to_embeddings_list(self,values):

        # Get Content
        content = values['content']

        # Split Content into Embeddings
        content_split = self.text_splitter(content)

        # Format for Embeddings
        embeddings_list = [self.format_embedding_values(x, values) for x in content_split] 

        return embeddings_list

    def chunks(self,iterable, size):
        it = iter(iterable)
        chunk = tuple(itertools.islice(it, size))
        while chunk:
            yield chunk
            chunk = tuple(itertools.islice(it, size))