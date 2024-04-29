from flask import Flask, jsonify, request
import time

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

engine = get_engine()

app = Flask(__name__)

@app.route("/stats", methods = ['GET'])
def get_stats():
    pass
    
app.run(debug=True)