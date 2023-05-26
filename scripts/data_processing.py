# import libraries
import pandas as pd
import os


# import data into python
df = pd.read_csv('data/olist_order_items_dataset.csv', index_col=False, delimiter=',')
df = df.drop_duplicates(subset=['order_id'])
df = df.where(pd.notnull(df), None)
df = df.drop(['order_item_id'], axis=1)


import os
import pandas as pd

def ingest_data(data_file):
    df = pd.read_csv(os.path.join(data_dir, data_file), index_col=False, delimiter=',')
    df = df.where(pd.notnull(df), None)
    return df

data_dir = 'data'
data_files = os.listdir(data_dir)

for data_file in data_files:
    modified_name = data_file.replace('olist_', '').replace('_dataset.csv', '')
    # use a function to read the csvv and remove null.
    modified_dataframe = ingest_data(data_file)
    #create a seperate variable dynamically using globals() function
    globals()[modified_name] = modified_dataframe
    print(f"Processed file {data_file} and saved the modified DataFrame to {modified_name}")
