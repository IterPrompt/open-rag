from utils.postgres import PostGres
from utils.openai import OpenAIUtil
from utils.formatData import Formatter

def handler(event, context):
    # Get Url
    batch_id = event['batch_id']
    print(batch_id)

    # Initialize Functions
    psg = PostGres()

    #### Get Batch
    # Get Pending Batches
    batch_list = psg.get_batch(batch_id)
    print(len(batch_list))

    if len(batch_list) > 0:
        # Update Iter
        batch = batch_list[0]
        openai_batch_id = batch['batch_id']
        print(openai_batch_id)

        # Initiate openai
        openai = OpenAIUtil()

        # Check if Batch has Completed
        batch_status = openai.get_batch_status(openai_batch_id)
        print(batch_status)

        if batch_status.status == 'completed':
            # Get File
            file_id = batch_status.output_file_id
            batch_embedding_list = openai.get_batch_output_as_list(file_id)
            print(len(batch_embedding_list))

            #### Get Batch Embeddings
            batch_embeddings = [int(x['embedding_id']) for x in batch_embedding_list]
            embeddings_context = psg.get_embeddings_content(batch_embeddings)
            print(embeddings_context.keys())

            # Initiate Formatter
            formatter = Formatter()

            #### Merge Batch Data with Existing Data
            embeddings_list = formatter.format_embeddings_list(batch_embedding_list,embeddings_context)
            print(embeddings_list[0])
            print(len(embeddings_list))

            #### Update Embedding Content & Metadata
            psg.update_data_many_from_list('content_embedding', embeddings_list)

            ##### Prep Embedding Tags
            embedding_tags_list = [{'embedding_id':values['id'],'keyword':keyword}
                                        for values in embeddings_list for keyword in values['metadata'].get('keywords',[])]
            print(embedding_tags_list[0])

            # Get List of Keywords
            keyword_key_list = [x['keyword'] for x in embedding_tags_list]
            keyword_ids = psg.get_keyword_ids(keyword_key_list)

            # Update Embedding Tags
            embedding_tags_update_list = [{'embedding_id':values['embedding_id'],'tag_id':keyword_ids[values['keyword'].lower().replace(' ','-')]}
                                            for values in embedding_tags_list]
            print(embedding_tags_update_list[0])
            print(f"Embedding Tag Updates: {len(embedding_tags_update_list)}")
            psg.insert_data_many_from_list('content_embedding_tags', embedding_tags_update_list, include_timestamp=False)

            # Add Content_ID to Values
            embeddings_list = [{**embedding,'content_id':embeddings_context[embedding['id']]['content_id_id']} for embedding in embeddings_list]

            #### Format Content Enhanced Metadata
            enhanced_content_metadata_list = formatter.group_metadata_by_content_id(embeddings_list)
            print(enhanced_content_metadata_list[0])

            #### Update Embedding Content & Metadata
            psg.update_content_metadata(enhanced_content_metadata_list)

            #### Get Content Tag and Remove Exising Values
            content_tags_update_list = [{'content_id':value['id'],'tag_id':keyword_ids[keyword.lower().replace(' ','-')]} for value in enhanced_content_metadata_list for keyword in value['metadata']['keywords']]
            content_tags_update_list = psg.remove_existing_pairs(content_tags_update_list)
            print(content_tags_update_list[0])

            # Insert
            print(f"Content Tags Updates: {len(content_tags_update_list)}")
            psg.insert_data_many_from_list('content_content_tags', content_tags_update_list, include_timestamp=False)

            #### Create Embedding Batch
            for embeddings_list_chunk in formatter.chunks(embeddings_list, 1000):
                print(len(embeddings_list_chunk))
                print(embeddings_list_chunk)

                # Format as Batch JsonL
                jsonl_bytes = openai.format_embeddings(embeddings_list_chunk)
                print('Formatted')

                # Create OpenAI Batch Function
                new_openai_batch_id = openai.create_openai_batch(jsonl_bytes)
                print(new_openai_batch_id)

                # Update Batch to Complete
                psg.update_batch_complete(batch_id)

                # Save Batch in DB
                batch_id = psg.create_batch(new_openai_batch_id)

                #### Update Embedding Status
                update_list = [{
                    'id': embedding['id'],
                    'batch_id_id': batch_id,
                    'status': 'embedding content'
                } for embedding in embeddings_list_chunk]

                # Update Content Embeddings
                psg.update_data_many_from_list('content_embedding', update_list)
                print('Batch Successfully Created')
        else:
            print('Batch Not Complete')

