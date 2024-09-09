import pandas as pd
import numpy as np
import mysql.connector
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def rename_columns_to_uppercase(df):

  return df.rename(columns=lambda x: x.upper())

################################## MySQL ######################################
def read_create_df_mysql(table_name, db_config):
    # Establish a connection
    conn = mysql.connector.connect(**db_config)

    # Create a cursor object
    cursor = conn.cursor()

    # Read table
    cursor.execute(f'SELECT * FROM {table_name}')
    result = cursor.fetchall()
    # print(result)

    # Create df from table
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(result, columns=column_names)

    return df

def convert_numpy_types(value):
    if isinstance(value, np.integer):
        return int(value)
    elif isinstance(value, np.floating):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    else:
        return value


def create_table_and_insert_from_df_mysql(df, table_name, data_types, db_config):
    # Connect to the MySQL database
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        # Convert DataFrame types to ensure compatibility with MySQL
        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].astype(int)
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].astype(float)
            elif pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].astype(str)
            elif pd.api.types.is_bool_dtype(df[col]):
                df[col] = df[col].astype(str)
            # Ensure proper conversion for datetime if needed
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype(str)

        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        # print(result)

        if result:
            # Table exists, append data
            # print('Table exists')
            data_tuples = [tuple(x) for x in df.to_records(index=False)]
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
            data_tuples = [tuple(convert_numpy_types(item) for item in row) for row in data_tuples]
            # print(insert_query)  # For debugging purposes
            # print(data_tuples)  # For debugging purposes

            cursor.executemany(insert_query, data_tuples)
        else:
            # Create the table
            # print('Create the table')
            query = f"CREATE TABLE {table_name} (\n"
            for i, column in enumerate(df.columns):
                column_type = data_types.get(column, 'TEXT')
                column_type = column_type.replace('INTEGER', 'INT')
                query += f"    {column.replace('-', '_')} {column_type}"
                if i < len(df.columns) - 1:
                    query += ",\n"
            query += "\n);"

            # print(query)  # For debugging purposes
            cursor.execute(query)
            # print('self.cursor.execute(query) executed')  # For debugging purposes

            # Insert data into the newly created table
            data_tuples = [tuple(x) for x in df.to_records(index=False)]
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
            data_tuples = [tuple(convert_numpy_types(item) for item in row) for row in data_tuples]
            # print('insert_query')  # For debugging purposes
            # print(insert_query)  # For debugging purposes

            cursor.executemany(insert_query, data_tuples)

        connection.commit()

    except Exception as e:
        print(f"Error creating or inserting into table '{table_name}': {e}")

################################## Snowflake ######################################

def read_snowflake_to_dataframe(username, password, account, database, schema, table):
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account
        )

        # Use a context manager to ensure the connection is closed properly
        with conn.cursor() as cursor:
            # Set the database and schema
            cursor.execute(f"USE DATABASE {database};")
            cursor.execute(f"USE SCHEMA {schema};")

            # Execute the query
            cursor.execute(f"SELECT * FROM {table}")

            # Fetch the results
            rows = cursor.fetchall()

            # Get column names from cursor description
            columns = [col[0] for col in cursor.description]

            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)

        return df

    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise
    finally:
        # Ensure the connection is closed
        conn.close()


def table_exists(conn, table_name):

    query = f"SHOW TABLES LIKE '{table_name}'"
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return len(result) > 0


def create_table_if_not_exists(conn, df, table_name):
    # Dynamically generate SQL for table creation based on DataFrame schema
    columns = []
    for col_name, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            col_type = 'INT'
        elif pd.api.types.is_float_dtype(dtype):
            col_type = 'FLOAT'
        elif pd.api.types.is_string_dtype(dtype):
            col_type = 'STRING'
        elif pd.api.types.is_bool_dtype(dtype):
            col_type = 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            col_type = 'TIMESTAMP'
        else:
            col_type = 'STRING'
        columns.append(f"{col_name} {col_type}")

    create_table_query = f"""
    CREATE TABLE {table_name} (
        {', '.join(columns)}
    );
    """

    cursor = conn.cursor()
    cursor.execute(create_table_query)
    print(f"Table {table_name} created successfully.")

def insert_df_to_snowflake(df, table_name, snowflake_config, batch_id):
    df['BATCH_ID'] = batch_id

    table_name = table_name.upper()
    df = rename_columns_to_uppercase(df)
    # Add the batch_id column
    conn = snowflake.connector.connect(**snowflake_config)
    # Check if the table exists, and create it if necessary
    if not table_exists(conn, table_name):
        create_table_if_not_exists(conn, df, table_name)

    # Ensure proper handling of quotes: only add quotes if table name is lowercase
    if table_name.islower():
        table_name_cleaned = f'"{table_name}"'
    else:
        table_name_cleaned = table_name

    try:
        from snowflake.connector.pandas_tools import write_pandas
        # Use the cleaned table name
        write_pandas(conn, df, table_name_cleaned)
        print(f"Data inserted successfully into {table_name_cleaned}.")
    except Exception as e:
        print(f"Error inserting data into {table_name_cleaned}: {e}")
    finally:
        conn.close()

def insert_df_to_snowflake2(df, table_name, snowflake_config):

    table_name = table_name.upper()
    df = rename_columns_to_uppercase(df)
    # Add the batch_id column
    conn = snowflake.connector.connect(**snowflake_config)
    # Check if the table exists, and create it if necessary
    if not table_exists(conn, table_name):
        create_table_if_not_exists(conn, df, table_name)

    # Ensure proper handling of quotes: only add quotes if table name is lowercase
    if table_name.islower():
        table_name_cleaned = f'"{table_name}"'
    else:
        table_name_cleaned = table_name

    try:
        from snowflake.connector.pandas_tools import write_pandas
        # Use the cleaned table name
        write_pandas(conn, df, table_name_cleaned)
        print(f"Data inserted successfully into {table_name_cleaned}.")
    except Exception as e:
        print(f"Error inserting data into {table_name_cleaned}: {e}")
    finally:
        conn.close()


def insert_df_to_snowflake_chunks(df, table_name, snowflake_config, batch_id, chunk_size):
    # Loop over the DataFrame in chunks
    for start in range(0, len(df), chunk_size):
        end = min(start + chunk_size, len(df))
        chunk = df.iloc[start:end]

        # Process the chunk here
        print(f"Processing rows {start} to {end - 1}")
        # print(len(chunk))
        batch_id += 1

        insert_df_to_snowflake(chunk, table_name, snowflake_config, batch_id)


def get_max_batch_id(snowflake_config, table_name="hired_employees"):
    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(**snowflake_config)

    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Define the SQL query to get the maximum BATCH_ID
        query = f"""
        SELECT COALESCE(MAX(BATCH_ID), 1) AS max_batch_id
        FROM {table_name}
        """

        # Execute the query
        cursor.execute(query)

        # Fetch the result
        result = cursor.fetchone()
        max_batch_id = result[0]

        return max_batch_id
    except Exception as e:
        print(f"Error fetching max BATCH_ID: {e}")
        return 0
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()


def upsert_df_to_snowflake(df, table_name, snowflake_config, primary_key_column):
    table_name = table_name.upper()
    df = rename_columns_to_uppercase(df)
    # Establish connection to Snowflake
    conn = snowflake.connector.connect(**snowflake_config)
    # Check if the table exists, and create it if necessary
    if not table_exists(conn, table_name):
        create_table_if_not_exists(conn, df, table_name)

    # Define stage and temporary table names
    stage_name = f"{table_name}_STAGE"
    temp_table_name = f"{table_name}_TEMP"

    try:
        cursor = conn.cursor()

        # Create a temporary stage if it does not exist
        cursor.execute(f"""
        CREATE OR REPLACE TEMPORARY STAGE {stage_name}
        """)

        # Create a temporary table to hold the DataFrame data
        cursor.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS
        SELECT * FROM {table_name} LIMIT 0
        """)

        # Upload the DataFrame to the temporary stage
        write_pandas(conn, df, temp_table_name)

        # Perform the upsert operation using the MERGE statement
        merge_query = f"""
        MERGE INTO {table_name} AS target
        USING {temp_table_name} AS source
        ON target.{primary_key_column} = source.{primary_key_column}
        WHEN MATCHED THEN
            UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in df.columns if col != primary_key_column])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(df.columns)})
            VALUES ({', '.join([f'source.{col}' for col in df.columns])})
        """

        cursor.execute(merge_query)
        print(f"Upsert operation completed successfully for table {table_name}.")

    except Exception as e:
        print(f"Error during upsert operation: {e}")
    finally:
        cursor.close()
        conn.close()


def drop_table_from_snowflake(table_name, snowflake_config):
    table_name = table_name.upper()

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(**snowflake_config)

    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Define the SQL query to drop the table
        drop_query = f"DROP TABLE IF EXISTS {table_name}"

        # Execute the query
        cursor.execute(drop_query)
        print(f"Table {table_name} has been dropped successfully.")

    except Exception as e:
        print(f"Error dropping table {table_name}: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()



