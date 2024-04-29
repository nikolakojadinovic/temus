import pandas as pd 
import requests
from flask import Flask 
import time
from datetime import datetime, timedelta 
import os 
from typing import List
from io import StringIO
import numpy as np 
from sqlalchemy import text 

RAW_PATH = os.environ.get('RAW_URL') if os.environ.get('RAW_URL') else 'raw'

def get_engine():
    from sqlalchemy import create_engine, URL
    connection_url = URL.create(
        "postgresql+psycopg2",
        username="postgres",
        password="root",
        host=os.environ.get('DB_URL') if os.environ.get('DB_URL') else "localhost",
        database="postgres",
        query={}
    )
    return create_engine(connection_url)

def read_from_raw_storage(dataset):

    BASE_DIR = f"{RAW_PATH}/{dataset}"
    with open(f"{BASE_DIR}/_LATEST","r") as f:
        latest_data_path = f.read()
    return pd.read_csv(f"{latest_data_path}/data.csv")

def pre_transform():

    products_columns_renamed = dict(zip(
        ["Item","Category","Vendor","Sale Price","Stock Status"],
        ["item","category", "vendor", "sale_price", "stock_status"]
    ))
    vendors_columns_renamed = dict(zip(
        ["Vendor Name","Shipping Cost","Customer Review Score","Number of Feedbacks"],
        ["vendor", "shipping_cost","customer_review_score", "number_of_feedbacks"]
    ))  
    
    df_products = read_from_raw_storage("products")
    df_vendors = read_from_raw_storage("vendors")

    df_products.rename(columns = products_columns_renamed, inplace = True)
    df_vendors.rename(columns = vendors_columns_renamed, inplace = True)

    df_products = df_products.astype({
        "item": str,
        "category": str, 
        "vendor": str, 
        "sale_price": float, 
        "stock_status": str
    })

    df_vendors = df_vendors.astype({
        "vendor": str, 
        "shipping_cost": float,
        "customer_review_score": float,
        "number_of_feedbacks": int
    })

    df_flat = df_products.merge(df_vendors, on = "vendor", how="inner")
    df_flat.drop(["Unnamed: 0.1_x",  "Unnamed: 0_x",'Unnamed: 0.1_y', 'Unnamed: 0_y'], axis = 1, inplace=True)

    return df_flat

def load_to_db(data:pd.DataFrame):

    engine = get_engine()

    data.to_sql(
        name = "items",
        con = engine, 
        if_exists = "append",
        index = False
    )

    print(f"[TRANSFORM-LOAD] Loadaded newest data at: {time.ctime()}")
    
def run():
    data = pre_transform()
    load_to_db(data)
