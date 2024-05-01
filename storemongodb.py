from dagster import op, get_dagster_logger, In, Out, io_manager
import pymongo
from pandas import DataFrame

# Define a custom IO manager for MongoDB
class MongoDBIOManager:
    def __init__(self, mongo_uri, db_name):
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[db_name]

    def handle_output(self, context, obj):
        collection_name = context.name
        self.db[collection_name].insert_many(obj.to_dict('records'))

    def load_input(self, context):
        collection_name = context.upstream_output.name
        data = self.db[collection_name].find()
        return DataFrame(list(data))

# MongoDB connection configurations
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "StockData"

@io_manager
def mongo_io_manager():
    return MongoDBIOManager(MONGO_URI, DB_NAME)

@op(required_resource_keys={"io_manager"})
def store_apple_data(context, apple_data: DataFrame):
    logger = get_dagster_logger()
    if not apple_data.empty:
        context.resources.io_manager.handle_output(context, apple_data)
        logger.info("Apple stock data stored successfully in MongoDB")
    else:
        logger.error("No data to store for Apple stocks")

@op(required_resource_keys={"io_manager"})
def store_nasdaq_data(context, nasdaq_data: DataFrame):
    logger = get_dagster_logger()
    if not nasdaq_data.empty:
        context.resources.io_manager.handle_output(context, nasdaq_data)
        logger.info("NASDAQ stock data stored successfully in MongoDB")
    else:
        logger.error("No data to store for NASDAQ stocks")
