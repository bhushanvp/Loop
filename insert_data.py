import pandas as pd
from pymongo import MongoClient

# MongoDB connection settings
mongodb_uri = "mongodb://localhost:27017/"
database_name = "loop_report_db"

# Connect to MongoDB
client = MongoClient(mongodb_uri)

def insert_data(collection_name, csv_file):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    data = df.to_dict(orient='records')
    db = client[database_name]
    collection = db[collection_name]

    # Insert the data into the MongoDB collection
    collection.insert_many(data)

    # Close the MongoDB connection
    client.close()

    print(f"{len(data)} documents inserted into the '{collection_name}' collection of the '{database_name}' database in MongoDB.")

# insert_data("store", "./data/store status.csv")
# insert_data("business_hours", "./data/Menu hours.csv")
# insert_data("timezones", "./data/bq-results-20230125-202210-1674678181880.csv")