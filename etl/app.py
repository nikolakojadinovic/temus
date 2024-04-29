from flask import Flask
from flask_apscheduler import APScheduler
from extract import get_data 
from transform_load import run as tl_run
import time

app = Flask(__name__)
scheduler = APScheduler()

def etl_job():
    print(f"[INFO] Running extract job. Pinging server for any data changes...Time: {time.ctime()}")

    products_status_code = get_data("products")
    vendors_status_code = get_data("vendors")

    if products_status_code == 200 or vendors_status_code == 200:
        print(f"[INFO] Changes captured. Running transform-load job. Extracting from raw storage and writing to database")
        tl_run()
    else:
        print("No changes to the previous state of both datasets at source. Skipping transform")
    
if __name__ == '__main__':
    scheduler.add_job(id='etl_job', func=etl_job, trigger='interval', seconds=5)
    scheduler.init_app(app)
    scheduler.start()
    app.run(port=3001, threaded= True, debug = True)





