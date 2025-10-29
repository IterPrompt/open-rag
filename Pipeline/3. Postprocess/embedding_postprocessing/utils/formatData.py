import pandas as pd
from psycopg2.extras import Json
from .postgres import PostGres
import tiktoken

class Formatter:
    def __init__(self):
        pass

    def num_tokens_from_string(string: str, encoding_name: str) -> int:
        """Returns the number of tokens in a text string."""
        encoding = tiktoken.get_encoding(encoding_name)
        num_tokens = len(encoding.encode(string))
        return num_tokens

