# import cloudscraper
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def insert_to_db(coll, data, database="ozon"):
    '''
    Inserts data to mongoDB
        database : Default=ozon
        coll : collecion name which will be the store/company id
    '''
    uri = os.getenv('DB_URI')
    client = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        test = client.admin.command('ping')
        if test.get('ok') == 1:
            print("[DB] Successfully connected to MongoDB!")
            db = client[database] # select the database
            collection = db[coll]
            result = collection.insert_one(data)
            print(f"[DB] Data inserted {result.acknowledged}")
    except Exception as e:
        print(f"[DB] {e}")

def load_cookies_from_txt(file_path : str):
    '''Load cookies from a txt file'''
    cookies_dict = {}
    print(f"[COOK] Loading cookies...")
    with open(file_path, 'r') as file:
        for line in file:
            # Skip comments and empty lines
            if line.startswith('#') or not line.strip():
                continue 
            # Split the line by tabs (the typical format for cookies.txt)
            parts = line.strip().split('\t')
            if len(parts) == 7:
                # Extract the cookie name and value
                cookie_name = parts[5]
                cookie_value = parts[6]
                # Add to the dictionary
                cookies_dict[cookie_name] = cookie_value
    return cookies_dict

def get_prod_list(company_id):
    url = "https://seller.ozon.ru/api/product/list"
    payload_data = {
        "fields": [5, 2, 4, 3, 6, 11, 13, 1, 7, 16, 19, 17, 20],
        "search":"",
        "visibility": 15,
        "sort_by": "created_at",
        "sort_dir": "DESC",
        "description_category_and_type_id": [],
        "company_id": int(company_id),
        "model_id": [],
        "is_es_search": True,
        "image_absent": False,
        "page": 1,
        "page_size": 50,
        "from": 0,
        "limit": 50,
        "last_id": ""
    }

    cookies = load_cookies_from_txt(f'cookies/{company_id}.txt')
    headers = {
        "x-o3-company-id": company_id,
    }

    # Send request with cookies
    resp = requests.post(url, json=payload_data, headers=headers, cookies=cookies)
    resp_data = resp.json()['items']
    # the first object
    prod_dict = {}
    date = datetime.now().strftime('%Y-%m-%d')
    prod_list = []
    for resp_d in resp_data:
        sellerId = resp_d['offer_id']
        price = resp_d['price'].get('marketing_price')
        in_ozon = resp_d['stock']['present']
        image = resp_d['pictures'][0]['url']
        data = {
            "sellerId": sellerId,
            "price": price,
            "in_ozon": in_ozon,
            "image": image,
        }
        prod_list.append(data)
    # Assign the list to the date key in the dictionary
    prod_dict[date] = prod_list
    # insert to db
    if prod_list:
        insert_to_db(coll=f"{company_id}_prods", data=prod_dict)
    return prod_dict

def get_analytics(company_id : str, date_=datetime.now().strftime('%Y-%m-%d')):
    """
    Gets the data for the store:
    
    Args:
        company_id (str): The store's company ID (retrieved from the request headers in Chrome).
        date_ (str): The date for which to get the data (default is today's date).
                     Enter the date in 'YYYY-MM-DD' format to get the data for a specific day.
                     If not provided, it defaults to today's date.

    Cookies are saved in the 'cookies' folder with the company ID.
    """
    table_data_url = "https://seller.ozon.ru/api/site/seller-analytics/charts/table"
    print(f"[MAIN] Getting {date_} analytics for {company_id} store")
    payload_data = {
        "group_by":"SKU",
        "metrics":["ordered_units","session_view","session_view_pdp","conv_tocart_pdp","revenue","cancellations","returns","position_category"],
        "date_from":f"{date_} 00:00:00",
        "date_to":f"{date_} 23:59:59",
        "filters":[],
        "limit":"999",
        "offset":"0",
        "sort":[{"key":"ordered_units","order":"DESC"}]
    }
    cookies = load_cookies_from_txt(f'cookies/{company_id}.txt')
    headers = {
        "x-o3-company-id": company_id,
    }
    table_resp = requests.post(table_data_url, json=payload_data, cookies=cookies, headers=headers)
    print(f"[MAIN] Code {table_resp.status_code}")
    if table_resp.status_code == 200:
        # insert to db
        insert_to_db(coll=company_id, data=table_resp.json())
    else:
        print(f"[MAIN] Connection Failed: {table_resp.status_code}")

# Main
load_dotenv()
STORE_1 = "1004262"
STORE_2 = "1495083"
STORE_3 = "1742699"
yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d')

if __name__ == "__main__":
    # gets data for yesterday
    for store in [STORE_1, STORE_2, STORE_3]:
        results = get_analytics(company_id=store, date_=yesterday)
        print("[PROD]Getting the products list...")
        get_prod_list(company_id=store) # Gets the product list n todays date