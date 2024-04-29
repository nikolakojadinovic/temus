import streamlit as st 
import requests 
import pandas as pd 
import json

BACKEND_URL = "http://127.0.0.1:3000"

def get_filters():
    
    
    categories = requests.get(f"{BACKEND_URL}/categories")
    vendors = requests.get(f"{BACKEND_URL}/vendors")
    stock_statuses = requests.get(f"{BACKEND_URL}/stockstatuses")

    categories=list(categories.json()["data"])
    vendors=list(vendors.json()["data"])
    statuses=list(stock_statuses.json()["data"])
    return categories, vendors, statuses

def get_etl_metadata():
    prod_payload ={"dataset":"products"}
    vend_payload ={"dataset":"vendors"}
    etl_metadata = requests.get(f"{BACKEND_URL}/latestIngest")
    return etl_metadata.json()

def get_stats(category, vendor,stock_status):
    params = {"category":category,"vendor":vendor, "stock_status":stock_status}
    response = requests.get(f"{BACKEND_URL}/analytics/stats", params = params)
    return response.json()

def get_vendor_comparison(category, stock_status):
    params = {"category":category, "stock_status":stock_status}
    response = requests.get(f"{BACKEND_URL}/analytics/compareVendors", params = params)
    return response.json()

def main():

    st.title("ElectroWorld - Real Time Analytics")
    head1, head2, head3 = st.columns(3)
    filter_categories, filter_vendors, filter_statuses = get_filters()
    DEFAULT_VENDOR = filter_vendors.index("ElectroWorld")
    DEFAULT_STOCKSTATUS = filter_statuses.index("In Stock")
    etl_metadata = get_etl_metadata()
    with st.sidebar:
        st.write("Apply filters")

        add_category = st.selectbox(
            "Category", filter_categories
        )
        add_vendors = st.selectbox(
            "Vendors", filter_vendors, index=DEFAULT_VENDOR
        )
        add_stock = st.selectbox(
            "Status", filter_statuses, index=DEFAULT_STOCKSTATUS
        )

        st.write("ETL Info - Latest refresh")
        st.caption(f"Products dataset:{etl_metadata['products']}")
        st.caption(f"Vendors dataset:{etl_metadata['products']}")
        stats = get_stats(add_category, add_vendors, add_stock)
        vendor_comparison = pd.DataFrame(get_vendor_comparison(add_category, add_stock))
    with head1:
        st.metric("AVG Sale price","$"+str(stats["avg_sale_price"][0]))
    with head2:
        st.metric("AVG Ship. cost", "$"+str(stats["avg_shipping_cost"][0]))
    with head3:
        st.metric("AVG Total price", "$"+str(stats["avg_total_price"][0]))

    st.write("Vendor Comparison")
    st.dataframe(vendor_comparison)
    
    
if __name__ == "__main__":
    main()
    