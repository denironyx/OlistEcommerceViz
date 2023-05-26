# import libraries
import pandas as pd
import os


# import data into python
df = pd.read_csv('data/olist_geolocation_dataset.csv', index_col=False, delimiter=',')
df = df.drop_duplicates(subset=['order_id'])
df = df.where(pd.notnull(df), None)
df = df.drop(['order_item_id'], axis=1)

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

## Cleaning of the data

# customers
customers = customers.copy()
customers = customers.drop_duplicates(subset=['customer_id'])
customers.drop(['customer_unique_id'], axis=1, inplace=True)
customers.rename(columns={"customer_state": "state_code", "customer_city": "city"})

# order items
order_items = order_items.copy()
order_items = order_items.drop_duplicates(subset=['order_id'])
order_items = order_items.drop(['order_item_id'], axis=1)
order_items.head()

# payments
payments = order_payments.copy()

# product category
product_category = product_category.copy()
product_category = product_category.drop_duplicates(subset=['product_category_name'])

# product
products = products.copy()
products = products.drop_duplicates(subset=['product_id'])

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

# geolocation
geolocation = pd.read_csv('data/olist_geolocation_dataset.csv', index_col=False, delimiter=',')
geolocation = geolocation.drop(['geolocation_city', 'geolocation_state'], axis=1)
geolocation = geolocation.drop_duplicates(subset=['geolocation_zip_code_prefix'], ignore_index=True)

geo_customer = geolocation.rename(columns={"geolocation_zip_code_prefix":"customer_zip_code_prefix",
                                            "geolocation_lat":"customer_lat",
                                            "geolocation_lng":"customer_lng"})

geo_seller = geolocation.rename(columns={"geolocation_zip_code_prefix":"seller_zip_code_prefix",
                                         "geolocation_lat": "seller_lat",
                                         "geolocation_lng": "seller_lng"})


# orders
orders = orders.copy()
orders = orders.drop_duplicates(subset=['order_id'])

#changing dtype to date
orders.loc[:, 'order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'],
                                                            format='%Y/%m/%d').dt.date
orders.loc[:, 'order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'],
                                                            format='%Y/%m/%d').dt.date
orders.loc[:, 'order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'],
                                                            format='%Y/%m/%d').dt.date

# track the estimated and actual days of delivery
orders['estimated_days_of_delivery'] = (orders['order_estimated_delivery_date'] - orders['order_purchase_timestamp']).astype('timedelta64[D]')
orders['actual_days_of_delivery'] = (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']).astype('timedelta64[D]')

orders.head()

# state_code
state_codes = state_codes.copy()
