import pandas as pd 
import requests
from flask import Flask 
import time
from datetime import datetime, timedelta 
import os 
from io import StringIO

datasets = {
    "products":"https://temus-northstar.github.io/data_engineering_case_study_public/product_data.html",
    "vendors" :"https://temus-northstar.github.io/data_engineering_case_study_public/vendor_data.html"
}

LAST_MODIFIED = "" 

def write_to_raw_storage(data: pd.DataFrame, dataset: str):

    BASE_DIR = f"raw/{dataset}"
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

def load_data(dataset):

    url = datasets[dataset]

    def get_last_modified():
        return requests.get(url).headers.get("Last-Modified")
    
    global LAST_MODIFIED
    LAST_MODIFIED = get_last_modified()
    
    response = requests.get(url,headers={"If-Modified-Since":LAST_MODIFIED})
    data = pd.read_html(url)    
    # print(response.text)
    write_to_raw_storage(data[0], dataset)   
        

load_data("products")
load_data("vendors")


