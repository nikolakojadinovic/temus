#  A small ETL example using python, postgres, streamlit and flask 

## This application reads data from the URL by pinging it every 10 seconds, ensuring near real time ETL. If new data is present, it will be loaded into raw storage and then into Postgres with  the ETL service. On top of that, a Flask API is built to get the data from the database and serve it to the frontend application, which is build in streamlit. Streamlit visualises the data, shows statistic, tables, and let's the user filter the data and get dynamic reports

## the application is deployed using docker compose containg 4 services (temus-be, temus-fe, temus-etl, postgres), which correspond to the directories of this project. 

## Raw storage mocks S3 logical hierarchy by partitioning the data by date and time, which is present in .gitignored directory "raw" 

## Running the app: 
### 1. clone the repo 
### 2. mkdir raw 
### 3. mkdir raw/products 
### 4. mkdir raw/vendors 
### docker compose up -d 
### Go to localhost:8501 to access the fronted 

## The app is already deployed and accessible on: http://93.127.203.191:8501/