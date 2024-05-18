import json
from pymongo import MongoClient, UpdateOne
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_json(file_path):
    """ Load JSON data from a file """
    with open(file_path, 'r') as file:
        return json.load(file)

def validate_record(record):
    """ Validate record to ensure necessary fields are present """
    required_fields = [ "name", "extensions"]
    for field in required_fields:
        if field not in record or not record[field]:
            return False
    return True

def connect_mongodb(uri, db_name, collection_name):
    """ Connect to MongoDB and return the collection """
    client = MongoClient(uri)
    db = client[db_name]
    return db[collection_name]

def process_data(file_path, uri, db_name, collection_name):
    """ Process data from JSON file and store in MongoDB """
    # Load data from dataset
    data = load_json(file_path)
    batchNumber = 0

    # Connect to MongoDB
    try:
        collection = connect_mongodb(uri, db_name, collection_name)
    except:
        logging.warning("Unable to connect to db")
        sys.exit(1)

    # Prepare bulk operations
    operations = []

    for record in data:
        if validate_record(record):
            # Create an upsert operation to handle duplicates
            operations.append(UpdateOne(
                {"name": record["name"]},  # Filter by name
                {"$set": record},
                upsert=True
            ))
        else:
            logging.warning(f"Invalid record: {record}")

        # Execute operations in batches of 50
        if len(operations) == 50:
            retryCount = 0
            while retryCount < 3:
                try:
                    collection.bulk_write(operations)
                    logging.info(f"Processed 50 records, Batch " + str(batchNumber))
                    break
                except:
                    retryCount += 1
                    logging.info(f"Failed process Retrying Batch Operation -- "+ str(batchNumber))
        
            #Reset operations queue 
            operations = []

    # Process remaining operations
    if operations:
        collection.bulk_write(operations)
    
    
    logging.info(f"Processed {len(operations)} records")
    batchNumber += 1

if __name__ == "__main__":
    # Path to your JSON file
    json_file_path = "dataset.json"

    # MongoDB connection parameters
    mongo_uri = "mongodb://localhost:27017/"
    database_name = "ransomware_db"
    collection_name = "ransomware_data"

    # Process the data and store it in MongoDB
    process_data(json_file_path, mongo_uri, database_name, collection_name)
    logging.info("Data processing completed successfully.")