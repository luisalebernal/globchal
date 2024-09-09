import json
import mysql.connector
import pandas as pd
from functions import *
import time
from flask import Flask, request, jsonify

app = Flask(__name__)

# Original configuration string with single quotes
# configuration = "{'sourceConnection': {'username': 'root', 'password': 'YlorOTGSNylMvjgngZdFCuTdtVvQuZFW', 'host': 'junction.proxy.rlwy.net', 'port': '40287', 'database': 'source', 'connectorName': 'MySQL'}, 'targetConnection': {'username': 'LABVGLOBCHAL', 'password': 'Geo@2021', 'account': 'qedmsnl-fm70895', 'warehouse': 'COMPUTE_WH', 'database': 'TARGET', 'schema': 'TGT', 'databaseRole': 'ACCOUNT_ADMIN', 'connectorName': 'Snowflake'}}"
# chunk_size = 1000

def main(configuration, chunk_size):
    start_time = time.time()
    # Convert the string to valid JSON by replacing single quotes with double quotes
    # json_string = configuration.replace("'", '"')

    # Parse the JSON string
    # configuration = json.loads(json_string)

    # Get source credentials
    sourceConnection = configuration.get('sourceConnection')
    src_user, src_password, src_host, src_port, src_database = [sourceConnection.get(key) for key in
                                                                ['username', 'password', 'host',
                                                                 'port', 'database']]

    # print('src_user:', src_user)
    # print('src_password:', src_password)
    # print('src_host:', src_host)
    # print('src_port:', src_port)
    # print('src_database:', src_database)

    config = {
        'host': src_host,
        'user': src_user,
        'passwd': src_password,
        'db': src_database,
        'port': src_port
    }

    # # Write local hired_employees to mysql database
    # table_name = 'hired_employees'
    # df = pd.read_csv('csv_files/hired_employees.csv')
    # df = df.fillna('') # Replace NaN values with empty strings
    # print(df)
    #
    # data_types = {'id': 'TEXT', 'name': 'TEXT', 'datetime': 'TEXT', 'department_id': 'TEXT', 'job_id': 'TEXT'}
    # create_table_and_insert_from_df_mysql(df, table_name, data_types, config)

    # Read data from mysql source
    table_name_he = "hired_employees"
    df_hired_emp = read_create_df_mysql(table_name_he, config)
    table_name = 'departments'
    df_dep = read_create_df_mysql(table_name, config)
    table_name = 'jobs'
    df_jobs = read_create_df_mysql(table_name, config)

    # print(df_dep)
    # print(df_jobs)
    # print(df_hired_emp)

    # Handle empty values
    df_hired_emp['department_id'] = df_hired_emp['department_id'].replace('', 0).astype(float)
    df_hired_emp['job_id'] = df_hired_emp['job_id'].replace('', 0).astype(float)

    # Write data into snowflake section

    # Get target credentials
    targetConnection = configuration.get('targetConnection')
    tgt_user, tgt_password, tgt_account, tgt_warehouse, tgt_database, tgt_schema = (targetConnection.get(key) for key in
                                                                                    ['username', 'password', 'account',
                                                                                     'warehouse', 'database', 'schema'])

    snowflake_config = {
        'user': tgt_user,
        'password': tgt_password,
        'account': tgt_account,
        'warehouse': tgt_warehouse,
        'database': tgt_database,
        'schema': tgt_schema
    }

    # Replace NaN and empty string in 'datetime' column with '1910-01-01T00:00:00Z'
    df_hired_emp['datetime'] = df_hired_emp['datetime'].replace(
        {np.nan: '1910-01-01T00:00:00Z', '': '1910-01-01T00:00:00Z'})
    # print(df_hired_emp)

    # Write data into snowflake
    batch_id = get_max_batch_id(snowflake_config)
    # chunk_size = 500
    table_name_he = "hired_employees"
    insert_df_to_snowflake_chunks(df_hired_emp, table_name_he, snowflake_config, batch_id, chunk_size)

    table_name_jobs = "jobs"
    primary_key_column = "id"
    upsert_df_to_snowflake(df_jobs, table_name_jobs, snowflake_config, primary_key_column)

    table_name_dep = "departments"
    primary_key_column = "id"
    upsert_df_to_snowflake(df_dep, table_name_dep, snowflake_config, primary_key_column)

    ################################## Section 2 ###################################

    # import data
    # Get target credentials
    targetConnection = configuration.get('targetConnection')
    tgt_user, tgt_password, tgt_account, tgt_warehouse, tgt_database, tgt_schema = (targetConnection.get(key) for key in
                                                                                    ['username', 'password', 'account',
                                                                                     'warehouse', 'database', 'schema'])
    config_tgt = {
        'user': tgt_user,
        'password': tgt_password,
        'account': tgt_account,
        'warehouse': tgt_warehouse,
        'database': tgt_database,
        'schema': tgt_schema
    }

    table = "HIRED_EMPLOYEES"
    df_hired_emp_tgt = read_snowflake_to_dataframe(tgt_user, tgt_password, tgt_account, tgt_database, tgt_schema, table)
    table = "DEPARTMENTS"
    df_dep_tgt = read_snowflake_to_dataframe(tgt_user, tgt_password, tgt_account, tgt_database, tgt_schema, table)
    table = "JOBS"
    df_jobs_tgt = read_snowflake_to_dataframe(tgt_user, tgt_password, tgt_account, tgt_database, tgt_schema, table)

    # Local data
    # df_hired_emp_tgt = pd.read_csv('csv_files/hired_employees.csv')
    # df_dep_tgt = pd.read_csv('csv_files/departments.csv')
    # df_jobs_tgt = pd.read_csv('csv_files/jobs.csv')

    # Handle Null data
    df_hired_emp_tgt['DEPARTMENT_ID'] = df_hired_emp_tgt['DEPARTMENT_ID'].replace('', 0).astype(float)
    df_hired_emp_tgt['JOB_ID'] = df_hired_emp_tgt['JOB_ID'].replace('', 0).astype(float)
    df_hired_emp_tgt['DEPARTMENT_ID'] = df_hired_emp_tgt['DEPARTMENT_ID'].fillna(0)
    df_hired_emp_tgt['JOB_ID'] = df_hired_emp_tgt['JOB_ID'].fillna(0)
    df_hired_emp_tgt['DATETIME'] = df_hired_emp_tgt['DATETIME'].fillna('1920-01-01T00:00:00Z')

    ########################### First Requirement ##################################
    # Convert to string
    df_dep_tgt['ID'] = df_dep_tgt['ID'].astype(int)
    df_hired_emp_tgt['DEPARTMENT_ID'] = df_hired_emp_tgt['DEPARTMENT_ID'].astype(int)

    # Perform the joins
    merged_df_1 = pd.merge(df_dep_tgt, df_hired_emp_tgt, left_on='ID', right_on='DEPARTMENT_ID', how='inner')
    merged_df_2 = pd.merge(df_jobs_tgt, merged_df_1, left_on='ID', right_on='JOB_ID', how='inner')
    merged_df_2 = merged_df_2[['JOB', 'DEPARTMENT', 'NAME', 'DATETIME']]
    # Display the resulting DataFrame
    # print(merged_df_2)

    # Convert 'DATETIME' column to datetime type
    merged_df_2['DATETIME'] = pd.to_datetime(merged_df_2['DATETIME'], format='%Y-%m-%dT%H:%M:%SZ')

    # print(merged_df_2.head())
    # Filter for the year 2021
    df_2021 = merged_df_2[merged_df_2['DATETIME'].dt.year == 2021]

    df_2021['Q1'] = (df_2021['DATETIME'].dt.quarter == 1).astype(int)
    df_2021['Q2'] = (df_2021['DATETIME'].dt.quarter == 2).astype(int)
    df_2021['Q3'] = (df_2021['DATETIME'].dt.quarter == 3).astype(int)
    df_2021['Q4'] = (df_2021['DATETIME'].dt.quarter == 4).astype(int)

    df_2021 = df_2021.sort_values(by=['DEPARTMENT', 'JOB'])

    # print(df_2021.head())
    # print('df_2021')
    # print(df_2021)

    # Group by 'JOB' and 'DEPARTMENT', and count the number of employees in each group
    quarterly_counts = df_2021.groupby(['JOB', 'DEPARTMENT']).agg({
        'Q1': 'sum',
        'Q2': 'sum',
        'Q3': 'sum',
        'Q4': 'sum'
    }).reset_index()
    # Display the result
    # Sort the DataFrame first by 'DEPARTMENT' then by 'JOB'
    quarterly_counts = quarterly_counts.sort_values(by=['DEPARTMENT', 'JOB'])
    quarterly_counts = quarterly_counts[['DEPARTMENT', 'JOB', 'Q1', 'Q2', 'Q3', 'Q4']]

    # print('quarterly_counts')
    # print(quarterly_counts.head())
    # print(quarterly_counts)

    ############################ Second Requirement ##################################
    # df_2021 = merged_df_2[merged_df_2['DATETIME'].dt.year == 2021]
    df_2021['total_hired'] = df_2021['Q1'] + df_2021['Q2'] + df_2021['Q3'] + df_2021['Q4']
    df_2021_Q = df_2021

    # print('df_2021')
    # print(df_2021)
    # Group by 'DEPARTMENT' and sum the 'total_hired'
    department_summary = df_2021.groupby('DEPARTMENT').agg({
        'total_hired': 'sum'
    }).reset_index()

    department_summary = pd.merge(department_summary, df_dep_tgt, how='inner')
    department_summary = department_summary[['ID', 'DEPARTMENT', 'total_hired']]

    # print(department_summary)

    mean_emp_hired_2021 = round(department_summary['total_hired'].mean())
    # print(mean_emp_hired_2021)
    df_2nd_requirement = department_summary[department_summary['total_hired'] > mean_emp_hired_2021]
    print(df_2nd_requirement)

    # Write data in Snowflake
    # config_tgt = targetConnection
    table_name = "section2a"
    drop_table_from_snowflake(table_name, snowflake_config)
    insert_df_to_snowflake2(quarterly_counts, table_name, config_tgt)

    table_name = "section2b"
    drop_table_from_snowflake(table_name, snowflake_config)
    insert_df_to_snowflake2(df_2nd_requirement, table_name, config_tgt)
    end_time = time.time()
    execution_time = end_time - start_time
    execution_time = f"Execution time: {execution_time:.4f} seconds"
    print(execution_time)

    return execution_time

# main(configuration, chunk_size)

@app.route('/execute', methods=['POST'])
def execute():
    data = request.json

    configuration = data.get('configuration')
    chunk_size = data.get('chunk_size')

    answer = main(configuration, chunk_size)
    return jsonify({'answer': answer})

if __name__ == '__main__':
    app.run()



