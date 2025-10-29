import os
import psycopg2
import pandas as pd
from datetime import datetime
from copy import deepcopy
from psycopg2.extras import Json

class PostGres:
    def __init__(self):
        pass

    def connect_to_db(self):
        try:
            DATABASE_URI = os.environ['DATABASE_URI']

            connection = psycopg2.connect(DATABASE_URI)
            return connection
        except psycopg2.Error as e:
            print("Unable to connect to the database:", e)
            return None

    def get_table_columns(self, table_name, table_schema='public'):
        connection = self.connect_to_db()

        cursor = connection.cursor()

        cursor.execute(
            f"SELECT * FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name = '{table_name}'")

        columns = cursor.fetchall()

        cursor.close()
        return columns

    def get_table_values(self,table_name, object_id):
        connection = self.connect_to_db()

        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM {table_name} WHERE id = %s', (object_id,))
        data = cursor.fetchall()
        cursor.close()

        columns_raw = self.get_table_columns(table_name)

        if len(data) > 0:
            # List to DataFrame
            dataframe = pd.DataFrame(data)
            dataframe.columns = [x[3] for x in columns_raw]
            return dataframe.to_dict('records')[0]
        else:
            print('Object does not exist!')
            return False

    def get_table_w_filter(self, table_name, query):
        connection = self.connect_to_db()

        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM {table_name} WHERE {query}')

        data = cursor.fetchall()

        cursor.close()

        columns_raw = self.get_table_columns(table_name)

        if len(data) > 0:
            # List to DataFrame
            dataframe = pd.DataFrame(data)
            dataframe.columns = [x[3] for x in columns_raw]
            return dataframe
        else:
            return pd.DataFrame(columns=[x[3] for x in columns_raw])

    def get_batch(self, batch_id):
        connection = self.connect_to_db()

        cursor = connection.cursor()

        cursor.execute(f'SELECT id, batch_id, is_complete '\
                       f'FROM content_batch '\
                       f'WHERE id=%s AND is_complete=False',[batch_id])

        data = cursor.fetchall()

        cursor.close()

        if len(data) > 0:
            return [{'id':x,'batch_id':y,'is_complete':z} for x,y,z in data]
        else:
            return []

    # Function to insert data into the specified table
    def insert_data(self, table_name, raw_values):

        # Set Update Timestamp
        values = raw_values.copy()
        values['updated_at'] = str(datetime.now())
        values['created_at'] = str(datetime.now())

        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()
                query = """INSERT INTO {table_name} {columns} VALUES ({values}) RETURNING id;
                    """.format(table_name=table_name,
                               columns=f'{tuple(list(values.keys()))}'.replace("'", ''),
                               values=', '.join(['%s' for x in range(len(list(values.keys())))]))
                cursor.execute(query, tuple(list(values.values())))
                connection.commit()
                print("Data inserted successfully into {} table.".format(table_name))

                # Get Posted Value
                value_id = cursor.fetchone()[0]

                if connection:
                    connection.close()

                return value_id
        except psycopg2.Error as e:
            print("Error inserting data into {}: ".format(table_name), e)
        finally:
            if connection:
                connection.close()

    def insert_data_many_from_list(self, table_name, insert_list, include_timestamp=False):
        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()

                values = deepcopy(insert_list)

                for value in values:
                    for key, item in value.items():
                        if isinstance(item, (list, dict)):
                            value[key] = Json(item)

                    if include_timestamp:
                        value['updated_at'] = str(datetime.now())
                        value['created_at'] = str(datetime.now())

                keys = values[0].keys()
                query = cursor.mogrify("INSERT INTO {} ({}) VALUES {} RETURNING {}".format(
                    table_name,
                    ', '.join(keys),
                    ', '.join(['%s'] * len(values)),
                    'id'
                ), [tuple(v.values()) for v in values])

                cursor.execute(query)

                connection.commit()
                print("Data inserted successfully into {} table.".format(table_name))

                # Get Posted Value
                returnValues = cursor.fetchall()

                if connection:
                    connection.close()
                return returnValues
        except psycopg2.Error as e:
            print("Error inserting data into {}: ".format(table_name), e)
        finally:
            if connection:
                connection.close()

    # Function to update data in the specified table
    def update_data(self, table_name, raw_values, value_id):
        # Set Update Timestamp
        update_values = raw_values.copy()
        update_values['updated_at'] = str(datetime.now())

        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()

                variable = ', '.join('{}=%s'.format(k) for k in update_values.keys())
                update_query = ("UPDATE {} SET {} WHERE id={};").format(
                    table_name,
                    variable,
                    value_id
                )
                cursor.execute(update_query, list(update_values.values()))
                connection.commit()
                print("Data updated successfully in {} table.".format(table_name))
        except psycopg2.Error as e:
            print("Error updating data in {}: ".format(table_name), e)
        finally:
            if connection:
                connection.close()

    def update_data_many(self,table_name, df):
        # Set Update Timestamp
        update_df = df.copy()
        update_df['updated_at'] = str(datetime.now())

        # To List of Dicts
        update_values = update_df.to_dict('records')
        print(len(update_values))

        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()

                full_query = ''

                for value in update_values:
                    value_id = value.pop('id')  # Get Id
                    variable = ', '.join('{}=%s'.format(k) for k in value.keys())  # Variable Keys

                    # Format Single Query
                    update_query = ("UPDATE {} SET {} WHERE id={}; ").format(
                        table_name,
                        variable,
                        value_id
                    )

                    # Add to Full Query
                    full_query = full_query + update_query

                print(full_query[:100])

                final_value = tuple([y for x in update_values for y in x.values()])
                print(len(final_value))

                cursor.execute(full_query, final_value)
                connection.commit()
                print("Data updated successfully in {} table.".format(table_name))
        except psycopg2.Error as e:
            print("Error updating data in {}: ".format(table_name), e)
        finally:
            if connection:
                connection.close()

    def update_data_many_from_list(self,table_name, embedding_list):
        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()

                new_embedding_list = deepcopy(embedding_list)

                for value in new_embedding_list:
                    for key, item in value.items():
                        if isinstance(item, (list, dict)):
                            value[key] = Json(item)

                full_query = ''

                for value in new_embedding_list:
                    value_id = value.pop('id')  # Get Id
                    variable = ', '.join('{}=%s'.format(k) for k in value.keys())  # Variable Keys

                    # Format Single Query
                    update_query = ("UPDATE {} SET {} WHERE id={}; ").format(
                        table_name,
                        variable,
                        value_id
                    )

                    # Add to Full Query
                    full_query = full_query + update_query

                print(full_query[:100])

                final_value = tuple([y for x in new_embedding_list for y in x.values()])
                print(len(final_value))

                cursor.execute(full_query, final_value)
                connection.commit()
                print("Data updated successfully in {} table.".format(table_name))
        except psycopg2.Error as e:
            print("Error updating data in {}: ".format(table_name), e)
        finally:
            if connection:
                connection.close()

    def create_batch(self,batch_id):
        # Format Data
        batch_data = {
            'batch_id': batch_id,
            'type':'embeddings',
            'is_complete': False
        }

        # Insert Data
        return self.insert_data('content_batch', batch_data)

    def update_batch_complete(self,object_id):
        values = {
            'is_complete':True
        }
        self.update_data('content_batch', values, object_id)

    def get_content_metadata_by_ids(self, content_ids):

        connection = self.connect_to_db()
        cursor = connection.cursor()
        cursor.execute(f'SELECT id, metadata FROM content_content WHERE id = ANY(%s)',(content_ids,))
        data = cursor.fetchall()
        cursor.close()

        return {x:y for x,y in data}

    def get_embeddings_content(self, embedding_ids):

        connection = self.connect_to_db()
        cursor = connection.cursor()
        cursor.execute(f'SELECT id, content_id_id, context, metadata FROM content_embedding WHERE id = ANY(%s)',(embedding_ids,))
        data = cursor.fetchall()
        cursor.close()

        return {w:{'content_id_id':x,'context':y,'metadata':z} for w,x,y,z in data}

    def update_content_status(self,content_id,status='pending'):
        # Set New Status
        values = {
            'status':status
        }

        # Update Status
        self.update_data('vectors_content',values,content_id)

    def update_content_metadata(self, enhanced_content_metadata_list):
        # Get Ids from Embedding Metdata List
        content_ids = [x['id'] for x in enhanced_content_metadata_list]

        # Get Current Metadata
        current_content_metdata = self.get_content_metadata_by_ids(content_ids)

        # Merge Metadata Values
        content_update_list = [{
            **x,
            'metadata':{
                **current_content_metdata[x['id']],
                **x['metadata']}
            } for x in enhanced_content_metadata_list]

        # Update Content Metadata
        self.update_data_many_from_list('content_content',content_update_list)

        # Success!
        print(f'Successfully Updated Content Metadata: {len(content_update_list)}')

    def get_existing_tags(self,tag_keys):
        connection = self.connect_to_db()

        cursor = connection.cursor()

        cursor.execute(f'''SELECT id, key
                            FROM content_tag
                            WHERE key = ANY(%s)
                            ''', (tag_keys,))

        table = cursor.fetchall()
        cursor.close()

        return {y: x for x, y in table}

    def get_keyword_ids(self, keyword_key_list):

        # Remove Duplicates
        keyword_key_list = [x.lower().replace(' ','-') for x in keyword_key_list]
        unique_keyword_list = [{'key':x.lower().replace(' ','-'), 'name':x.replace('-',' ').title()} for x in list(set(keyword_key_list))]
        print(len(unique_keyword_list))

        # Get Keys and Check Existing Tags
        tag_keys = [x['key'] for x in unique_keyword_list]
        existing_tag_key_ids = self.get_existing_tags(tag_keys)
        print(len(existing_tag_key_ids))

        # Remove Existing Tag Keys
        insert_keyword_list = [x for x in unique_keyword_list if x['key'] not in list(existing_tag_key_ids.keys())]
        print(len(insert_keyword_list))

        if len(insert_keyword_list) > 0:
            # Connect to Hunt
            tag_ids = self.insert_data_many_from_list('content_tag', insert_keyword_list, include_timestamp=True)

            # Tag Key : ID Relations
            new_key_tag_ids = {insert_keyword_list[x]['key']: tag_ids[x][0] for x in list(range(len(insert_keyword_list)))}

            # Add Key Tags to New
            existing_tag_key_ids.update(new_key_tag_ids)

        return existing_tag_key_ids

    def remove_existing_pairs(self, tags_list):

        # Remove Duplicates
        tags_list = pd.DataFrame(tags_list).drop_duplicates().to_dict('records')

        connection = self.connect_to_db()
        cursor = connection.cursor()

        pair_str = ', '.join([f"({value['content_id']}, {value['tag_id']})" for value in tags_list])

        cursor.execute(f'''SELECT content_id, tag_id
                            FROM content_content_tags
                            WHERE (content_id, tag_id) IN ({pair_str})
                            GROUP BY content_id, tag_id;
                            ''', )

        overlaps = cursor.fetchall()
        cursor.close()

        tags_list = [x for x in tags_list if (x['content_id'], x['tag_id']) not in overlaps]
        return tags_list


