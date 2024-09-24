import os
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient




# Function to fetch data for a specific date
def fetch_data(date, collection):
    '''
    Searches the database using the date in Y-m-d 00:00:00
    Default is yesterday as it contains full data
    '''
    query = {"query.dateFrom": f"{date} 00:00:00"}  # Adjust based on your date format
    cursor = db[collection].find(query)
    data = list(cursor)
    
    if data:
        total_prods = data[0]['result']['totalCount']
        formatted_data = []
        
        for entry in data:
            metrics = entry['query']['metrics']
            totals = entry['result']['totals']
            results = entry['result']['data']
            
            # Prepare headers
            headers = [' '] + ["image"] + metrics + ['in_ozon', 'price']
            
            # Add totals row with 'Totals' as the first value
            totals_row = ['Totals', ' '] + totals
            formatted_data.append(totals_row)

            # Add each product's data
            for item in results:
                dimensions = item['dimensions'][0]  # Assuming there's only one dimension per result
                metrics_values = item['metrics']
                # we use aggregate to find the matching records
                pipeline = [
                    # Match the document containing the specified date
                    { '$match': { f'{date}': { '$exists': True } } },
                    # Unwind the array for the given date
                    { '$unwind': f'${date}' },
                    # Match the sellerId within the unwound array
                    { '$match': { f'{date}.sellerId': dimensions.get('sellerId') } }
                ]
                prod_data = list(db[f"{collection}_prods"].aggregate(pipeline))
                in_ozon = prod_data[0][f'{date}'].get('in_ozon') if prod_data else None
                price = prod_data[0][f'{date}'].get('price') if prod_data else None
                image = prod_data[0][f'{date}'].get('image') if prod_data else None
                row = [dimensions.get('sellerId')] + [image] + metrics_values + [in_ozon, price]
                formatted_data.append(row)
            
        # Convert to DataFrame
        df = pd.DataFrame(formatted_data, columns=headers)
        return df, total_prods
    else:
        return pd.DataFrame()
    
def fetch_all_data(collection):
    '''
    Fetches all the data
    '''
    cursor = db[collection].find()
    data = list(cursor)
    # Initialize dictionary to store data organized by date and product
    data_by_date_and_product = defaultdict(lambda: defaultdict(int))

    # Process the MongoDB response
    for document in data:
        date_from = document['query']['dateFrom'].split(' ')[0]  # Extract the date part
        metrics = document['query']['metrics']
        totals = document['result']['totals']
        
        # Store the totals data with date and product ID as keys
        data_by_date_and_product['Products Ordered'][date_from] = totals[0]
        data_by_date_and_product['TotalRevenue'][date_from] = totals[4]
        try:
            data_by_date_and_product['AveragePrice'][date_from] = int(totals[4]/totals[0])
        except ZeroDivisionError:
            data_by_date_and_product['AveragePrice'][date_from] = 0
        data_by_date_and_product['Cancelled'][date_from] = totals[5]
        # Process the product data
        for item in document['result']['data']:
            dimensions = item['dimensions'][0]
            product_id = dimensions['sellerId']
            ordered_units = item['metrics'][0]  # 'ordered_units' is always the first metric
            
            # Store the ordered units data with date and product ID as keys
            data_by_date_and_product[product_id][date_from] = ordered_units
    
    # Flatten the dictionary into a list for DataFrame creation
    flattened_data = []
    
    for product_id, dates in data_by_date_and_product.items():
        entry = {'Product ID': product_id}
        entry.update(dates)  # Add all dates as columns with their respective values
        flattened_data.append(entry)
    # Create a DataFrame
    df = pd.DataFrame(flattened_data)
    sorted_df = df.sort_index(axis=1, ascending=False)


    return sorted_df


def fetch_prod_analytics(collection):
    '''
    Fetches all the data from the given collection and returns it as a DataFrame
    '''
    cursor = db[f"{collection}_prods"].find()
    datas = list(cursor)
    print(len(datas))

    # Define metrics for the table
    metrics = [
        'Date',
        'Products ordered',
        'Unique visitors, total',
        'Unique visitors with PDP view',
        'Shopping cart conversion rate from a PDP',
        'Position in search and catalog',
        'online price',
        'In Ozon warehouse'
    ]

    full_values = []
    columns = None  # Initialize outside loop to ensure it's only set once
    num_metrics_per_seller = 8  # Define the expected number of metrics per seller

    for data in datas:
        # Extract date key dynamically
        date_key = [key for key in data.keys() if key != '_id'][0]
        print(f"Processing date: {date_key}")

        # Extract sellers from any date
        sellers_data = data[date_key]
        sellers = [item['sellerId'] for item in sellers_data]

        # Create a MultiIndex header if it's not created already
        if columns is None:
            columns = pd.MultiIndex.from_tuples([(seller, metric) for seller in sellers for metric in metrics])

        row_data = []
        for seller in sellers_data:
            # MongoDB aggregation pipeline
            pipeline = [
                {
                    "$match": {
                        "query.dateFrom": f"{date_key} 00:00:00",
                        "result.data.dimensions.sellerId": seller['sellerId']
                    }
                },
                {
                    "$unwind": "$result.data"  # Deconstructs the result.data array
                },
                {
                    "$unwind": "$result.data.dimensions"  # Deconstructs the dimensions array within result.data
                },
                {
                    "$match": {
                        "result.data.dimensions.sellerId": seller['sellerId']
                    }
                },
                {
                    "$project": {
                        "_id": 0,  # Exclude _id field
                        "metrics": "$result.data.metrics"  # Include only metrics field
                    }
                }
            ]
            prod_datas = list(db[f"{collection}"].aggregate(pipeline))

            # Extracting metrics (assuming 8 metrics per seller)
            prod_ord = prod_datas[0]['metrics'][0] if prod_datas else None
            uniq_vis = prod_datas[0]['metrics'][1] if prod_datas else None
            uniq_vis_pdp = prod_datas[0]['metrics'][2] if prod_datas else None
            shop_cart = prod_datas[0]['metrics'][3] if prod_datas else None
            pos_ = prod_datas[0]['metrics'][7] if prod_datas else None
            
            seller_metrics = [
                date_key,        # Date
                prod_ord,        # Products ordered (can be changed dynamically)
                uniq_vis,        # Unique visitors, total
                uniq_vis_pdp,    # Unique visitors with PDP view
                shop_cart,       # Shopping cart conversion rate from a PDP
                pos_,            # Position in search and catalog
                seller['price'], # online price from the data
                seller['in_ozon']# In Ozon warehouse from the data
            ]

            row_data.extend(seller_metrics)  # Add this seller's data to the row

        # Adjust the row data to match the number of columns
        expected_columns = len(columns)
        actual_columns = len(row_data)

        if actual_columns < expected_columns:
            # Add None for missing columns
            row_data.extend([None] * (expected_columns - actual_columns))
        elif actual_columns > expected_columns:
            # Truncate extra columns
            row_data = row_data[:expected_columns]

        # Add corrected row data to full_values
        full_values.append(row_data)


    # Create the DataFrame
    df = pd.DataFrame(full_values, columns=columns)

    # Display the DataFrame
    return df


def get_store_name(collection_name):
    '''Maps collection name to store name'''
    if collection_name == '1742699':
        return "Store 3", "shop"
    elif collection_name == '1495083':
        return "Store 2", "shop"
    elif collection_name == '1004262':
        return "Store 1", "shop"
    else:
        return "Unknown Store"

load_dotenv()
# MongoDB connection
# client = MongoClient(os.getenv('DB_URI'))
client = MongoClient(st.secrets['DB_URI'])
db = client['ozon']
all_collections = db.list_collection_names()
# Filter collections that do not contain an underscore '_'
collections = [collection for collection in all_collections if '_' not in collection]

# save in a dict fr easier menu access
collection_dict = {collection: get_store_name(collection) for collection in collections}

# Streamlit app
st.set_page_config(layout="wide", page_title= "Ozon analytics")
# sidebar menu
with st.sidebar:
    selected_collection = option_menu(menu_title="Ozon", 
                           menu_icon="cast", 
                           options=list(collection_dict.keys()),
                           icons=[icon for _, icon in collection_dict.values()])

# Top titles
st.title(":orange[Ozon] Analytics Viewer", anchor=False)
st.subheader(f":violet[{collection_dict[selected_collection][0]}]", divider="red")


# page menu
selected = option_menu(menu_title=None,
                       options=['Data', 'Ordered Analytics', 'Product Analysis'],
                       icons=['file-spreadsheet','receipt','award'],
                       orientation="horizontal")

# Input for date
if selected == "Data":
    yester_date = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
    date_input = st.text_input("Enter date (e.g., 2024-09-13):", yester_date)

    if date_input:
        # Fetch and display data for the given date: a tuple
        data = fetch_data(date_input, collection=str(selected_collection))
        
        if not data[0].empty:
            st.write(f"Data for :violet[{date_input}] Total products: {data[1]}")
            # display the images too
            st.dataframe(data[0],column_config={
                "image": st.column_config.ImageColumn(
                    "Images", help="Duble click to preview"
                )}, hide_index=True)  # Display data as a table
        else:
            st.write(f"No data found for {date_input}")
elif selected == 'Ordered Analytics':
    st.write(f"Ordered Units per date")
    # Define the metrics you want to display

    selected_metrics = ["ordered_units"]
    data = fetch_all_data(collection=str(selected_collection))
    st.dataframe(data, hide_index=False)
else:
    prod_data = fetch_prod_analytics(collection=selected_collection)
    st.dataframe(data=prod_data)