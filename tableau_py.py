import pandas as pd
import numpy as np
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName
import tableauserverclient as TSC

def df_to_extract(df, output_path):
    '''
    Converts a Pandas dataframe to a Tableau Extract.

    Parameters
    ----------
    df (pandas dataframe): Dataframe to turn into a Tableau extract
    output_path (str): Where to create the Tableau extract
    ''' 

    # Replace nan's with 0
    df = df.replace(np.nan, 0.0, regex=True)

    print('Creating Tableau data extract...')
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, output_path, CreateMode.CREATE_AND_REPLACE) as connection:
            
            # Create schema
            connection.catalog.create_schema('Extract')

            # Create list of column definitions, based on the datatypes in pandas dataframe
            dtype_map = {
                'int32': SqlType.int(),
                'int64': SqlType.big_int(),
                'float32': SqlType.double(),
                'float64': SqlType.double(),
                'datetime64[ns]': SqlType.date(),
                'object': SqlType.text() 
            }
            table_def = []

            # Get column headers to loop through them
            df_columns = list(df)

            for col_header in df_columns:
                dtype_str = str(df[col_header].dtype)

                # Use dtype_str to lookup appropiate SqlType from dtype_map and append new column to table definition
                table_def.append(TableDefinition.Column(col_header, dtype_map[dtype_str]))
                
            # Define table
            extract_table = TableDefinition(TableName('Extract', 'Extract'), table_def)

            # Create table
            connection.catalog.create_table(extract_table)

            # Insert data
            with Inserter(connection, extract_table) as inserter:
                for idx, row in df.iterrows():
                    inserter.add_row(row)
                
                inserter.execute() 


def upload_extract_to_server(extract_path, config):
    '''
    Uploads Tableau extract to Tableau Server.

    Parameters
    ----------
    extract_path (str): Path to Tableau extract
    config (dict): Dictionary containing attributes for username, password, server_url, site, and project
    ''' 
    
    print(f'Uploading {extract_path} to Tableau Server.')
    print('     Server URL: ', config['server_url'])
    print('     Site: ', config['site'])
    print('     Project: ', config['project'])

    # Connect to Tableau Server
    tableau_auth = TSC.TableauAuth(config['username'], config['password'], site_id=config['site'])
    server = TSC.Server(config['server_url'])
    server.auth.sign_in(tableau_auth)
    all_projects, pagination_item = server.projects.get()

    project_id = None

    # Find PROJECT_NAME in list of projects
    for project in all_projects:
        if project.name == config['project']:
            project_id = project.id
            break

    if (project_id == None):
        print('Project name {} not found.'.format(config['project']))
        exit()

    # Connect to datasources in project and publish
    datasource = TSC.DatasourceItem(project_id)
    datasource = server.datasources.publish(datasource, extract_path, 'Overwrite')

    print(f'Successfully uploaded {extract_path} to Tableau Server.')
