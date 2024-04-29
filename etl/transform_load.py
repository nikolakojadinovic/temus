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

def get_engine():
    from sqlalchemy import create_engine, URL
    connection_url = URL.create(
        "postgresql+psycopg2",
        username="postgres",
        password="root",
        host="0.0.0.0",
        database="postgres",
        query={}
    )
    return create_engine(connection_url)

def read_from_raw_storage(dataset):

    BASE_DIR = f"raw/{dataset}"
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

    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE items"))

    data.to_sql(
        name = "items",
        con = engine, 
        if_exists = "append",
        index = False
    )

    print(f"[INFO] Loadaded newest data at: {time.ctime()}")

def update_aggregated_views():

    engine = get_engine()

    with open("etl/views.sql", "r") as r:
        queries = [q for q in r.read().split(";") if q and q != "\n"]

    with engine.connect() as conn: 
        transaction = conn.begin()
        try:
            for query in queries:
                if query:
                    conn.execute(text(query))
            transaction.commit()
        except Exception as e:
            print(f"Failed updating materialized views: {e}.\n Performing rollback")
            transaction.rollback()
                    
    
data = pre_transform()
load_to_db(data)
update_aggregated_views()