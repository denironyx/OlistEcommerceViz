# import libraries
import pandas as pd
import os
import zipfile
import geopandas as gpd

zip_path = 'data/raw/ecommerce.zip'
extract_folder = 'data/extracted'
processed_folder = 'data/processed/'

# Extract the zip file
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_folder)

data_files = os.listdir(extract_folder)

# import data into python
# df = pd.read_csv('data/olist_geolocation_dataset.csv', index_col=False, delimiter=',')
# df = df.drop_duplicates(subset=['order_id'])
# df = df.where(pd.notnull(df), None)
# df = df.drop(['order_item_id'], axis=1)

def ingest_data(data_file):
    df = pd.read_csv(os.path.join(extract_folder, data_file), index_col=False, delimiter=',')
    df = df.where(pd.notnull(df), None)
    return df

for data_file in data_files:
    if data_file.endswith('.csv'):
        modified_name = data_file.replace('olist_', '').replace('_dataset.csv', '')
        # use a function to read the csvv and remove null.
        modified_dataframe = ingest_data(data_file)
        #create a seperate variable dynamically using globals() function
        globals()[modified_name] = modified_dataframe
        print(f"Processed file {data_file} and saved the modified DataFrame to {modified_name}")
    else:
        print(f'This file {data_file} is not .csv')

## Cleaning of the data

# customers
customers = customers.copy()
customers = customers.drop_duplicates(subset=['customer_id'])
customers.drop(['customer_id'], axis=1, inplace=True)
customers.rename(columns={"customer_unique_id":"customer_id", "customer_state": "state_code", "customer_city": "city", "customer_zip_code_prefix": "zip_code"}, inplace=True)
customers

# order items
order_items = order_items.copy()
#order_items = order_items.drop_duplicates(subset=['order_id'])
order_items = order_items.drop(['order_item_id', 'shipping_limit_date'], axis=1)
order_items.head()
order_items['sales_unit_price'] = order_items['price'] + order_items['freight_value'] 

# payments
payments = order_payments.copy()
payments[payments['order_id'].duplicated()]
payments[payments.order_id == '8e5148bee82a7e42c5f9ba76161dc51a']


# product category
product_category = product_category.copy()
product_category = product_category.drop_duplicates(subset=['product_category_name'])

# product
products = products.copy()
products = products.drop_duplicates(subset=['product_id'])

# join product information and category
products = pd.merge(products, product_category, on='product_category_name', how='left')
products.head()
products.drop(['product_description_lenght', 'product_name_lenght', 'product_category_name', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'], axis=1, inplace=True)

products = products.rename(columns={"product_category_name_english": "product_category_name"})


# reviews
reviews = order_reviews.copy()
print('Duplicates in order_id: ', reviews['order_id'].duplicated().sum())
print('Duplicates in review_id: ', reviews['review_id'].duplicated().sum())
reviews.drop_duplicates(subset=['order_id'], keep='last', ignore_index=True, inplace=True)
reviews.drop_duplicates(subset=['review_id'], keep=False, ignore_index=True, inplace=True)

# check missing values
reviews.isna().sum() / len(reviews)

reviews = reviews.drop(['review_id','review_comment_title', 'review_comment_message', 'review_creation_date',
                        'review_answer_timestamp'], axis=1)

# sellers
sellers = sellers.copy()
sellers = sellers.drop_duplicates(subset=['seller_id'])

# 
sellers.rename(columns={"seller_state": "state_code", "seller_city": "city", "seller_zip_code_prefix":"zip_code"}, inplace=True)

# geolocation
geolocation = pd.read_csv(os.path.join(extract_folder, 'olist_geolocation_dataset.csv'), index_col=False, delimiter=',')
geolocation = geolocation.drop(['geolocation_city', 'geolocation_state'], axis=1)
geolocation = geolocation.drop_duplicates(subset=['geolocation_zip_code_prefix'], ignore_index=True)

geo_customer = geolocation.rename(columns={"geolocation_zip_code_prefix":"zip_code",
                                            "geolocation_lat":"customer_lat",
                                            "geolocation_lng":"customer_lng"})

geo_seller = geolocation.rename(columns={"geolocation_zip_code_prefix":"zip_code",
                                         "geolocation_lat": "seller_lat",
                                         "geolocation_lng": "seller_lng"})

# orders
orders = orders.copy()
#orders = orders.drop_duplicates(subset=['order_id'])

# functions
def delivery_ontime(est_date, del_date):
    if est_date < del_date:
        return "Late"
    else:
        return "On time"
    
    

#changing dtype to date
orders.loc[:, 'order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'],
                                                            format='%Y/%m/%d').dt.date
orders.loc[:, 'order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'],
                                                            format='%Y/%m/%d').dt.date
orders.loc[:, 'order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'],
                                                            format='%Y/%m/%d').dt.date
orders['order_purchase_year'] = pd.to_datetime(orders['order_purchase_timestamp']).dt.year
orders['order_purchase_month'] = pd.to_datetime(orders['order_purchase_timestamp']).dt.month


# track the estimated and actual days of delivery
orders['estimated_days_of_delivery'] = (orders['order_estimated_delivery_date'] - orders['order_purchase_timestamp']).astype('timedelta64[D]')
orders['actual_days_of_delivery'] = (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']).astype('timedelta64[D]')
orders['delivery_performance'] = orders.apply(
    lambda x: delivery_ontime(x.estimated_days_of_delivery, x.actual_days_of_delivery), axis=1
)
orders = orders.drop([ 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'], axis = 1)
orders.head()

# state_code
# state_codes = pd.read_csv('data/state_codes.csv', index_col=False, delimiter=',')
state_codes = pd.read_excel(os.path.join(extract_folder, 'state_codes.xlsx'), usecols=[0,1], names=['state_name','state_code'])
state_codes = state_codes[state_codes['state_code'].notnull()]
state_codes.head()


# Join state codes and seller or customer data
sellers = pd.merge(sellers,state_codes, on='state_code', how='left' )
sellers = pd.merge(sellers, geo_seller, on='zip_code', how='left')

# Join state codes and seller or customer data
customers = pd.merge(customers, state_codes, on='state_code', how='left')
customers = pd.merge(customers, geo_customer, on='zip_code', how='left')

# Export the data 
customers.to_csv(f"{processed_folder}customers.csv", index=False)
sellers.to_csv(f"{processed_folder}sellers.csv", index=False)
reviews.to_csv(f"{processed_folder}reviews.csv", index=False)
products.to_csv(f"{processed_folder}products.csv", index=False)
order_items.to_csv(f"{processed_folder}order_items.csv", index=False)
orders.to_csv(f"{processed_folder}orders.csv", index=False)
payments.to_csv(f"{processed_folder}payments.csv", index=False)