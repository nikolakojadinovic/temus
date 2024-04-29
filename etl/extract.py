import pandas as pd 
import requests
from flask import Flask 
import time
from datetime import datetime, timedelta 
import os 
from io import StringIO
import sys 

datasets = {
    "products":"https://temus-northstar.github.io/data_engineering_case_study_public/product_data.html",
    "vendors" :"https://temus-northstar.github.io/data_engineering_case_study_public/vendor_data.html"
}

RAW_PATH = os.environ.get('RAW_URL') if os.environ.get('RAW_URL') else 'raw'

def write_to_raw_storage(data: pd.DataFrame, dataset: str):

    BASE_DIR = f"{RAW_PATH}/{dataset}"
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    date_partition = now.split(" ")[0]
    hour_partition = now.split(" ")[1].replace(":","-")
    DATA_PATH = f"{BASE_DIR}/{date_partition}/{hour_partition}"

    if not os.listdir(BASE_DIR).__contains__(date_partition):
        os.mkdir(f"{BASE_DIR}/{date_partition}")

    if not os.listdir(f"{BASE_DIR}/{date_partition}").__contains__(hour_partition):
        os.mkdir(DATA_PATH)

    data.to_csv(f"{DATA_PATH}/data.csv")
    with open(f"{BASE_DIR}/_LATEST", "w+") as f:
        f.write(DATA_PATH)

def get_data(dataset):

    LAST_MODIFIED = None

    if not os.listdir(f"{RAW_PATH}/{dataset}").__contains__("_LAST_MODIFIED"):
        with open(f"{RAW_PATH}/{dataset}/_LAST_MODIFIED", "w+") as f: 
            f.write("Thu, 01 Jan 1970 00:00:00 GMT")

    with open(f"{RAW_PATH}/{dataset}/_LAST_MODIFIED", "r") as f:
        LAST_MODIFIED = f.read()

    url = datasets[dataset]
    
    response = requests.get(url,headers={"If-Modified-Since":LAST_MODIFIED})

    LAST_MODIFIED = response.headers.get("Last-Modified")
    if LAST_MODIFIED is not None:
        with open(f"{RAW_PATH}/{dataset}/_LAST_MODIFIED", "w+") as f: 
            f.write(LAST_MODIFIED)

    data = pd.read_html(url)    
    if response.status_code == 304:
        print(f"[EXTRACT] Status code: {response.status_code}")
    if response.status_code == 200:
        write_to_raw_storage(data[0], dataset)   
    return response.status_code
    


    