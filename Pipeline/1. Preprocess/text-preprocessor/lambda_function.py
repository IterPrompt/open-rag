from utils.postgres import PostGres
from utils.formatData import Formatter

def handler(event, context):

    # Get Url
    content_id = event.get('id',None)
    agents = event.get('agents',[])
    priority = event.get('priority', False)
    print(f"Content Id: {content_id}")
    print(f"Agents: {agents}")
    print(event)

    if content_id:
        # Initialize Functions
        psg = PostGres()
        formatter = Formatter()

        # Get Content Values
        content_values = psg.get_content_by_id('content_content', content_id)

        # Check Content Exists
        if content_values['content'] != None and content_values['content'] != '':

            # Get Type
            type_id = psg.get_type('text')
            content_values['type_id'] = type_id

            # Format into Embeddings List
            embeddings_list = formatter.content_to_embeddings_list(content_values)
            print(f"Embeddings List Len: {len(embeddings_list)}")

            # Get Existing Ids
            existing_embedding_ids = psg.get_embedding_ids_by_content(content_id)
            print(f"Existing Embeddings Ids Len: {len(existing_embedding_ids)}")

            if len(existing_embedding_ids) == 0:
                # Create Embeddings from Content Chunks
                embeddings_list, embedding_ids = psg.add_content_embeddings(embeddings_list)

                if len(agents) > 0:
                    # Add Embeddings to Agents
                    psg.add_embeddings_to_agents(embedding_ids, agents)
            else:
                # Get Current Embedding Value Count
                existing_embeddings_count = len(existing_embedding_ids)
                new_embeddings_count = len(embeddings_list)

                # Format Embeddings Lists
                update_list = embeddings_list[:existing_embeddings_count]
                update_ids = existing_embedding_ids[:existing_embeddings_count]
                add_list = embeddings_list[existing_embeddings_count:]
                delete_ids = existing_embedding_ids[new_embeddings_count:]

                # Update Existing Embeddings
                psg.update_content_embeddings(update_list,update_ids)

                if len(add_list) > 0:
                    # Create Embeddings from Content Chunks
                    embeddings_list, embedding_ids = psg.add_content_embeddings(add_list)

                    if len(agents) > 0:
                        psg.add_embeddings_to_agents(embedding_ids, agents) # Add Embeddings to Agents

                if len(delete_ids) > 0:
                    # Delete Values
                    psg.bulk_delete_list('content_embedding', delete_ids)

            if priority:
                pass
        else:
            print('Content is empty')
    else:
        print('No content_id found')
