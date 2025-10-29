import os
from copy import deepcopy
from psycopg2.extras import Json
import psycopg2
import pandas as pd
from datetime import datetime

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

    def get_batch(self, batch_id):
        connection = self.connect_to_db()

        cursor = connection.cursor()

        cursor.execute(f'SELECT id, batch_id, is_complete '\
                       f'FROM content_batch '\
                       f'WHERE id=%s AND type=%s AND is_complete=False',(batch_id,'embeddings',))

        data = cursor.fetchall()

        cursor.close()

        if len(data) > 0:
            return [{'id':x,'batch_id':y,'is_complete':z} for x,y,z in data]
        else:
            return []

    def get_content_ids_by_embedding_ids(self,embedding_ids):

        connection = self.connect_to_db()
        cursor = connection.cursor()

        cursor.execute(f"""
                    SELECT content_id_id
                    FROM content_embedding
                    WHERE id = ANY(%s)
                    GROUP BY content_id_id
                    HAVING COUNT(CASE WHEN status != 'complete' THEN 1 END) = 0;
                    """, (embedding_ids,))

        data = cursor.fetchall()

        cursor.close()
        return [x[0] for x in data if x]

    def get_attachment_ids_by_embedding_ids(self, embedding_ids):
        connection = self.connect_to_db()

        cursor = connection.cursor()

        cursor.execute(f"""
                    SELECT file_id_id, link_id_id, product_id_id, place_id_id
                    FROM content_embedding
                    WHERE id = ANY(%s) AND status = 'complete'
                    """, (embedding_ids,))

        table = cursor.fetchall()

        cursor.close()

        if len(table) > 0:
            keys = ['file_id','link_id', 'product_id', 'place_id']
            return [dict(zip(keys,values)) for values in table]
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

    def update_data_many_from_list(self,table_name, new_embedding_list):
        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()

                # Make Deep Copy
                embedding_list = deepcopy(new_embedding_list)

                # Any dicts or lists to Json Objects
                for value in embedding_list:
                    for key, item in value.items():
                        if isinstance(item, (list, dict)):
                            value[key] = Json(item)

                full_query = ''

                for value in embedding_list:
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

                final_value = tuple([y for x in embedding_list for y in x.values()])
                print(len(final_value))

                cursor.execute(full_query, final_value)
                connection.commit()
                print("Data updated successfully in {} table.".format(table_name))
        except psycopg2.Error as e:
            print("Error updating data in {}: ".format(table_name), e)
        finally:
            if connection:
                connection.close()

    def update_batch_complete(self,object_id):
        values = {
            'is_complete':True
        }
        self.update_data('content_batch', values, object_id)