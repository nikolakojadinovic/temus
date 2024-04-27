import pandas as pd 
import requests
from flask import Flask 
import time
from datetime import datetime, timedelta 
import os 
from typing import List
from io import StringIO

def read_from_raw_storage(dataset):

    BASE_DIR = f"raw/{dataset}"
    with open(f"{BASE_DIR}/_LATEST","r") as f:
        latest_data_path = f.read()
    return pd.read_csv(f"{latest_data_path}/data.csv"), latest_data_path

def _transform_product(df: pd.DataFrame, date:str, hour:str, minute:str):
    df["updated_at"] = date 
    df["hour"] = hour
    df["minute"] = minute 

    return df

def _transform_vendors(df:pd.DataFrame, date:str, hour:str, minute:str):
    df["updated_at"] = date 
    df["hour"] = hour
    df["minute"] = minute 

    return df

def transform(dataset):
    
    
    df, metadata = read_from_raw_storage(dataset)
    date = metadata.split("/")[2]
    hour = metadata.split("/")[3].split("-")[0]
    minute = metadata.split("/")[3].split("-")[1]

    if dataset == "products":
        return _transform_product(df,date,hour,minute)
    if dataset == "vendors":
        return _transform_vendors(df,date,hour, minute)


def load_to_db():
    pass

t_products = transform("products")
t_vendors = transform("vendors")

print(t_products.head())
print(t_vendors.head())