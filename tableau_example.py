'''
Creating a Tableau extract from a dataframe and uploading it to Tableau Server
'''
import pandas as pd
from tableau_py import *
# Create configuration for upload to Tableau Server

tableau_config = {
    'username': 'username',
    'password': 'password',
    'server_url': 'https://tableau.company.com',
    'site': 'site_name', 
    'project': 'project_name'
}

# Set where you want the Tableau extract to output
extract_path = 'example_data/example_output.hyper'

# Specify all dtypes
dtypes = {
    'Item': 'int32',
    'Description': str,
    'Location': 'int32',
    'Date': 'datetime64[ns]',
    'Supply': 'float64'
}

# Create example dataframe
df = pd.read_csv('example_data/input_data.csv').astype(dtypes)

# Create Tableau extract
df_to_extract(df, extract_path)

# Upload to Tableau Server
upload_extract_to_server(extract_path, tableau_config)


