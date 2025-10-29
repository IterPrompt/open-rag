from utils.postgres import PostGres
from utils.openai import OpenAIUtil

def handler(event, context):
    # Get Url
    batch_id = event['batch_id']
    print(batch_id)

    # Initialize Functions
    psg = PostGres()
    openai = OpenAIUtil()

    # Get Pending Batches
    batch_list = psg.get_batch(batch_id)
    print(len(batch_list))

    if len(batch_list) > 0:
        # Update Iter
        batch = batch_list[0]
        openai_batch_id = batch['batch_id']
        print(openai_batch_id)

        # Check if Batch has Completed
        batch_status = openai.get_batch_status(openai_batch_id)
        print(batch_status)

        if batch_status.status == 'completed':
            # Get File
            embedding_list = openai.get_batch_output_as_list(batch_status.output_file_id)
            print(len(embedding_list))

            # Update Embeddings
            table_name = 'content_embedding'
            psg.update_data_many_from_list(table_name, embedding_list)

            # Update Batch to Complete
            psg.update_batch_complete(batch_id)
            print('Successfully Updated Embeddings.')
            print(batch)

            # Get Content Ids
            embedding_ids = [int(x['id']) for x in embedding_list]
            content_ids = psg.get_content_ids_by_embedding_ids(embedding_ids)

            # Format and Update Complete
            content_update_list = [{'id':x,'is_complete':True} for x in content_ids]
            psg.update_data_many_from_list('content_content', content_update_list)

            # Get Attachment Ids
            attachement_ids = psg.get_attachment_ids_by_embedding_ids(embedding_ids)
            print(len(attachement_ids))

            # Update Link Status
            link_update_list = [{'id':x['link_id'],'status': 'Processed'} for x in attachement_ids if x['link_id']]
            if len(link_update_list) > 0:
                psg.update_data_many_from_list('content_link', link_update_list)

            # Update File Status
            file_update_list = [{'id':x['file_id'],'status': 'Processed'} for x in attachement_ids if x['file_id']]
            if len(file_update_list) > 0:
                psg.update_data_many_from_list('content_file', file_update_list)

        else:
            print('Batch Not Completed')

