import pandas as pd 
import requests
from flask import Flask 
import time
from datetime import datetime, timedelta 
import os 
from typing import List
from io import StringIO
import numpy as np 

def read_from_raw_storage(dataset):

    BASE_DIR = f"raw/{dataset}"
    with open(f"{BASE_DIR}/_LATEST","r") as f:
        latest_data_path = f.read()
    return pd.read_csv(f"{latest_data_path}/data.csv")


def transform():
    
    
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
        "item": np.string_,
        "category": np.string_, 
        "vendor": np.string_, 
        "sale_price": np.float64, 
        "stock_status": np.string_
    })

    df_vendors = df_vendors.astype({
        "vendor": np.string_, 
        "shipping_cost": np.float64,
        "customer_review_score": np.float64,
        "number_of_feedbacks": np.int32
    })

    df_flat = df_products.merge(df_vendors, on = "vendor", how="inner")
    df_flat.drop(["Unnamed: 0.1_x",  "Unnamed: 0_x"], axis = 1,inplace=True)
    return df_flat

def load_to_db():
    pass

data = transform()

print(data.head())