from dagster import op, get_dagster_logger, InputDefinition
import pandas as pd
import sqlalchemy

POSTGRES_URI = 'postgresql://username:password@localhost:5432/stockdata'

# Helper function to create a SQLAlchemy engine
def get_postgres_engine():
    return sqlalchemy.create_engine(POSTGRES_URI)

@op(input_defs=[InputDefinition(name="apple_data", dagster_type=pd.DataFrame)])
def store_apple_data_postgres(apple_data: pd.DataFrame):
    logger = get_dagster_logger()
    engine = get_postgres_engine()
    if not apple_data.empty:
        apple_data.to_sql('apple_stock', con=engine, if_exists='replace', index=True)
        logger.info("Apple stock data stored successfully in PostgreSQL")
    else:
        logger.error("No data to store for Apple stocks in PostgreSQL")

@op(input_defs=[InputDefinition(name="nasdaq_data", dagster_type=pd.DataFrame)])
def store_nasdaq_data_postgres(nasdaq_data: pd.DataFrame):
    logger = get_dagster_logger()
    engine = get_postgres_engine()
    if not nasdaq_data.empty:
        nasdaq_data.to_sql('nasdaq_stock', con=engine, if_exists='replace', index=True)
        logger.info("NASDAQ stock data stored successfully in PostgreSQL")
    else:
        logger.error("No data to store for NASDAQ stocks in PostgreSQL")
