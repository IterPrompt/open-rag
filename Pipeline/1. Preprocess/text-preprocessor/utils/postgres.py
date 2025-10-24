import os
import psycopg2
import pandas as pd
from datetime import datetime
from psycopg2.extras import Json
from .formatData import Formatter
from copy import deepcopy

class PostGres:
    def __init__(self):
        pass

    def connect_to_db(self):
        try:
            connection = psycopg2.connect(os.environ['DATABASE_URI'])
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

    def check_object_key(self,key, table_name):

        # Connect to DB
        conn = self.connect_to_db()

        # Create a Cursor
        cursor = conn.cursor()

        # Execute Values
        cursor.execute(f"""SELECT * FROM {table_name} WHERE key=%s""", [key])

        table = cursor.fetchall()

        if len(table) != 0:

            # Get Column Names
            columns = self.get_table_columns(table_name)

            # Get Key and Values
            keys = [x[3] for x in columns]
            values = list(table[0])

            # Object
            objectValues = dict(zip(keys, values))

            return {'exists': True, 'values': objectValues}
        else:
            cursor.close()

            return {'exists': False}

    def get_type(self,value):
        # Type Key
        type_key = '_'.join(value.lower().split(' '))

        # Check Exists
        exists = self.check_object_key(type_key, 'content_type')

        if not exists['exists']:
            # Setup Category Value
            value = {
                'key': type_key,
                'name': value,
                'updated_at': str(datetime.now()),
                'created_at': str(datetime.now())
            }

            # Insert Category
            object_id = self.insert_data('content_type', value)
            return object_id
        else:
            # Get Object Id
            object_id = exists['values']['id']
            return object_id

    def get_embedding_ids_by_content(self,content_id):

        connection = self.connect_to_db()

        cursor = connection.cursor()

        cursor.execute(
            f'SELECT id FROM content_embedding '\
            f'WHERE content_id_id=%s AND file_id_id IS NULL AND ' \
            f'product_id_id IS NULL AND place_id_id IS NULL',(content_id,))

        data = cursor.fetchall()

        cursor.close()

        if len(data) > 0:
            return [x[0] for x,y in data if x[0] != None]
        else:
            return []

    def get_content_by_id(self,table_name, object_id):
        connection = self.connect_to_db()

        cursor = connection.cursor()

        cursor.execute(f'SELECT * FROM {table_name} WHERE id = %s', (object_id,))

        data = cursor.fetchall()

        cursor.close()

        columns_raw = self.get_table_columns(table_name)

        if len(data) > 0:
            # Format Key Values
            keys = [x[3] for x in columns_raw]
            values = data[0]

            return dict(zip(keys, values))
        else:
            print('Object does not exist!')
            return False


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

    def insert_data_many(self, table_name, raw_values):
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
                # Multiply by Number of Rows
                full_query = query * len(values)

                cursor.execute(full_query, tuple(values.stack().tolist()))
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

    def insert_data_many_from_list(self,table_name, insert_list):
        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()

                values = deepcopy(insert_list)

                for value in values:
                    for key, item in value.items():
                        if isinstance(item, (list, dict)):
                            value[key] = Json(item)

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

    def update_data_many_from_list(self,table_name, update_list):

        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()

                # Save Copy
                update_value = deepcopy(update_list)

                for value in update_value:
                    for key, item in value.items():
                        if isinstance(item, (list, dict)):
                            value[key] = Json(item)

                full_query = ''

                for value in update_value:
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

                final_value = tuple([y for x in update_value for y in x.values()])

                cursor.execute(full_query, final_value)
                connection.commit()
                print("Data updated successfully in {} table.".format(table_name))
        except psycopg2.Error as e:
            print("Error updating data in {}: ".format(table_name), e)
        finally:
            if connection:
                connection.close()

    def add_content(self,content, metadata):
        # 
        content_values = {
            'content': content,
            'metadata':Json(metadata),
            'is_active': True
        }

        # Add content
        content_id = self.insert_data('content_content', content_values)
        return content_id

    def add_content_embeddings(self, embeddings_list):
        if len(embeddings_list) > 0:
            # Insert Values Get Id
            embedding_ids = self.insert_data_many_from_list('content_embedding', embeddings_list)

            # Add to List
            for x in range(len(embedding_ids)):
                embeddings_list[x]['id'] = embedding_ids[x][0]

            return embeddings_list, embedding_ids
        else:
            return False

    def add_embeddings_to_agents(self, embedding_ids, agent_ids):
        # Format Values
        values = [{"embedding_id": embedding_id, "agent_id": agent_id} for embedding_id in embedding_ids for agent_id in agent_ids]

        # Insert Values
        self.insert_data_many_from_list('content_embedding_agents', values)

    def update_content_embeddings(self,embeddings_list, update_ids):
        # Initiate Formatter
        formatter = Formatter()

        # Format for Update
        update_list = [formatter.format_embedding_update_values(x) for x in embeddings_list]

        # Add Update Ids
        for x in range(len(update_ids)):
            update_list[x]['id'] = update_ids[x]
            update_list[x]['updated_at'] = str(datetime.now())

        # Update From List
        self.update_data_many_from_list('content_embedding', update_list)

        return update_list

    def bulk_delete_list(self,table_name, delete_ids):

        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()

                # Define the DELETE query using the IN clause and placeholders
                delete_query = f"""
                    DELETE FROM {table_name}
                    WHERE id = ANY(%s);
                """

                # Execute the DELETE query with the list of IDs
                cursor.execute(delete_query, (delete_ids,))

                connection.commit()
                print("Data deleted successfully in {} table.".format(table_name))
        except psycopg2.Error as e:
            print("Error deleting data in {}: ".format(table_name), e)
        finally:
            if connection:
                connection.close()