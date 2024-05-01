from dagster import job
from extract1 import extract_apple_data
from extract2 import extract_nasdaq_data
from storemongodb import store_apple_data, store_nasdaq_data
from load_preprocess_transform import load_preprocess_apple_data, load_preprocess_nasdaq_data
from storepostgresql import store_apple_data_postgres, store_nasdaq_data_postgres
from mergedb import merge_postgresql_databases
from load_and_analysis import full_stock_analysis
from visualisation import visualization_op

@job
def full_etl_pipeline():
    # Extract data
    apple_raw_data = extract_apple_data()
    nasdaq_raw_data = extract_nasdaq_data()

    # Store in MongoDB
    store_apple_data(apple_raw_data)
    store_nasdaq_data(nasdaq_raw_data)

    # Load from MongoDB, preprocess and transform
    processed_apple_data = load_preprocess_apple_data(apple_raw_data)
    processed_nasdaq_data = load_preprocess_nasdaq_data(nasdaq_raw_data)

    # Store processed data in PostgreSQL
    store_apple_data_postgres(processed_apple_data)
    store_nasdaq_data_postgres(processed_nasdaq_data)

    # Analysis
    analysis_results = full_stock_analysis()

    # Visualization
    visualization_op()

    # Merge data in PostgreSQL
    merged_data = merge_postgresql_databases()

