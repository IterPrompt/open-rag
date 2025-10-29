import os
from utils.postgres import PostGres
from utils.formatData import Formatter
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def handler(event, context):
    # Get Url
    content_id = event.get('id',None)
    priority = event.get('priority', False)
    logger.info(f"Content Id: {content_id}")
    logger.info(event)

    if content_id:
        # Initialize Functions
        psg = PostGres()
        formatter = Formatter()

        # Get Content Values
        content_values = psg.get_db_value_by_id(os.environ['CONTENT_TABLE_NAME'], content_id)
        logger.info(f"Content Values: {content_values}")

        # Check Content Exists
        if content_values['content'] != None and content_values['content'] != '':
            # Normalize Content
            content_text = formatter.normalize_text(content_values['content'])

            # Chunk Content
            chunks = formatter.text_splitter(content_text, chunk_size=400, chunk_overlap=200)
            logger.info(f"Chunks Length: {len(chunks)}")

            # Format Content Chunks
            content_chunks = formatter.format_content_chunks(chunks, content_id)
            logger.info(f"Content Chunks Length: {len(content_chunks)}")

            # Add Content Chunks
            content_chunks_ids = psg.add_content_chunks(content_chunks)
            logger.info(f"Content Chunks Ids: {content_chunks_ids}")

            if priority:
                pass
        else:
            logger.info('Content is empty')
    else:
        logger.info('No content_id found')
