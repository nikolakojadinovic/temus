from flask import Flask
from flask_apscheduler import APScheduler
from extract import get_data 
from transform_load import run as tl_run

app = Flask(__name__)
scheduler = APScheduler()

def etl_job():
    print(f"[INFO] Running extract job. Writing raw data to raw data storage.")
    get_data("products")
    get_data("vendors")
    print(f"[INFO] Running transform-load job. Extracting from raw storage and writing to database")
    tl_run()
    
if __name__ == '__main__':
    scheduler.add_job(id='etl_job', func=etl_job, trigger='interval', seconds=5)
    scheduler.init_app(app)
    scheduler.start()
    app.run(port=3001, threaded= True, debug = True)





