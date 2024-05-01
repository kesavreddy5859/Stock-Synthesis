from dagster import op, get_dagster_logger, OutputDefinition
import pandas as pd
import sqlalchemy

POSTGRES_URI = 'postgresql://username:password@localhost:5432/stockdata'

# Helper function to create a SQLAlchemy engine
def get_postgres_engine():
    return sqlalchemy.create_engine(POSTGRES_URI)

@op(output_defs=[OutputDefinition(name='merged_data', dagster_type=pd.DataFrame)])
def merge_postgresql_databases():
    logger = get_dagster_logger()
    engine = get_postgres_engine()

    # SQL query to load Apple and NASDAQ data
    query_apple = "SELECT * FROM apple_stock;"
    query_nasdaq = "SELECT * FROM nasdaq_stock;"

    apple_data = pd.read_sql(query_apple, con=engine)
    nasdaq_data = pd.read_sql(query_nasdaq, con=engine)

    # Ensure both dataframes have a 'Date' column to join on
    if 'Date' not in apple_data.columns or 'Date' not in nasdaq_data.columns:
        logger.error("Both tables must have a 'Date' column for merging.")
        return pd.DataFrame()

    # Merging the dataframes on the 'Date' column
    merged_data = pd.merge(apple_data, nasdaq_data, on='Date', suffixes=('_apple', '_nasdaq'))

    # Store the merged data in a new PostgreSQL table
    merged_data.to_sql('merged_stock_data', con=engine, if_exists='replace', index=False)
    logger.info("Merged data stored successfully in PostgreSQL under the table 'merged_stock_data'.")

    return merged_data
