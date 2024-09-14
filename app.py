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
            headers = [' '] + metrics
            
            # Add totals row with 'Totals' as the first value
            totals_row = ['Totals'] + totals
            formatted_data.append(totals_row)

            # Add each product's data
            for item in results:
                dimensions = item['dimensions'][0]  # Assuming there's only one dimension per result
                metrics_values = item['metrics']
                row = [dimensions.get('sellerId')] + metrics_values
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
        data_by_date_and_product['AveragePrice'][date_from] = int(totals[4]/totals[0])
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
client = MongoClient(os.getenv('DB_URI'))
db = client['ozon']
collections = db.list_collection_names()
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
            st.dataframe(data[0], hide_index=True)  # Display data as a table
        else:
            st.write(f"No data found for {date_input}")
elif selected == 'Ordered Analytics':
    st.write(f"Ordered Units per date")
    # Define the metrics you want to display

    selected_metrics = ["ordered_units"]
    data = fetch_all_data(collection=str(selected_collection))
    st.dataframe(data, hide_index=False)
else:
    st.write(f"coming soon...")