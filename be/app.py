from flask import Flask, jsonify, request
import time
from sqlalchemy import text
import json

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

@app.route("/categories", methods = ['GET'])
def get_categories():
    
    sql_query = f"select distinct category from items" 
    with engine.connect() as conn:
        result = [elem[0] for elem in conn.execute(text(sql_query)).fetchall()]
    return {"data":result}

@app.route("/vendors", methods = ['GET'])
def get_vendors():
    sql_query = f"select distinct vendor from items"
    with engine.connect() as conn:
        result = [elem[0] for elem in conn.execute(text(sql_query)).fetchall()]
    return {"data":result}

@app.route("/stockstatuses", methods = ['GET'])
def get_stock_statuses():
    sql_query = f"select distinct stock_status from items"
    with engine.connect() as conn:
        result = [elem[0] for elem in conn.execute(text(sql_query)).fetchall()]
    return {"data":result}

@app.route("/latestIngest")
def get_latest_ingest():
    
    with open(f"raw/products/_LAST_MODIFIED") as f: 
        products = f.read()

    with open(f"raw/vendors/_LAST_MODIFIED") as f: 
        vendors = f.read()

    return {"products": products, "vendors":vendors}

@app.route("/analytics/stats", methods = ["GET"])
def get_analytics():
    category = request.args.get("category")
    stock_status = request.args.get("stock_status")
    vendor = request.args.get("vendor")
    sql_query = f"""
        select 
            avg(sale_price) as avg_sale_price, 
            avg(sale_price + shipping_cost) as avg_total_price,
            avg(shipping_cost) as avg_shipping_cost 
        from items 
        where 
            category = '{category}' and 
            stock_status = '{stock_status}' and 
            vendor = '{vendor}'
    """

    with engine.connect() as conn:
        response = conn.execute(text(sql_query))
        columns = response.keys()
        data = {col: [] for col in columns}
        for row in response:
            for col, val in zip(columns, row):
                data[col].append(round(val,2))
        
        return data

@app.route("/analytics/compareVendors", methods = ["GET"])
def compare_vendors():
    category = request.args.get("category")
    stock_status = request.args.get("stock_status")
    sql_query = f"""
        with lowest_price_by_vendor as (
            select vendor, min(sale_price) as price, count(items) as num_items
            from items 
            where stock_status = '{stock_status}' and category = '{category}'
            group by 1
        )

        select distinct
            lpbv.vendor, lpbv.num_items,
            i.item || ' - $' || lpbv.price as cheapest_item
        from lowest_price_by_vendor lpbv
        inner join items i
        on lpbv.vendor = i.vendor and lpbv.price = i.sale_price
        where stock_status = '{stock_status}' and category = '{category}'
        order by num_items asc;

    """

    with engine.connect() as conn:
        response = conn.execute(text(sql_query))
        columns = response.keys()
        data = {col: [] for col in columns}
        for row in response:
            for col, val in zip(columns, row):
                data[col].append(val)
        return data

if __name__ == "__main__":
    app.run(port = 3000, debug=True)