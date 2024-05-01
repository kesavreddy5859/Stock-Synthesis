from dagster import op, job, get_dagster_logger, InputDefinition, OutputDefinition
import pandas as pd
from sklearn.preprocessing import StandardScaler

@op(input_defs=[InputDefinition(name="apple_data", dagster_type=pd.DataFrame)], output_defs=[OutputDefinition(pd.DataFrame)])
def load_preprocess_apple_data(apple_data):
    logger = get_dagster_logger()

    # Data validation
    if apple_data.empty:
        logger.error("Apple data is empty.")
        raise ValueError("No data loaded from MongoDB.")
    
    # Cleaning: Handle missing values and remove duplicates
    apple_data.dropna(inplace=True)
    apple_data.drop_duplicates(inplace=True)

    # Data type correction
    apple_data['Open'] = apple_data['Open'].astype(float)
    apple_data['High'] = apple_data['High'].astype(float)
    apple_data['Low'] = apple_data['Low'].astype(float)
    apple_data['Close'] = apple_data['Close'].astype(float)
    apple_data['Volume'] = apple_data['Volume'].astype(int)

    # Normalization/Standardization
    scaler = StandardScaler()
    apple_data[['Open', 'High', 'Low', 'Close']] = scaler.fit_transform(apple_data[['Open', 'High', 'Low', 'Close']])

    # Date Handling
    apple_data['Date'] = pd.to_datetime(apple_data['Date'])
    apple_data.set_index('Date', inplace=True)

    logger.info("Apple data preprocessed successfully.")
    return apple_data

@op(input_defs=[InputDefinition(name="nasdaq_data", dagster_type=pd.DataFrame)], output_defs=[OutputDefinition(pd.DataFrame)])
def load_preprocess_nasdaq_data(nasdaq_data):
    logger = get_dagster_logger()

    # Data validation
    if nasdaq_data.empty:
        logger.error("NASDAQ data is empty.")
        raise ValueError("No data loaded from MongoDB.")
    
    # Cleaning: Handle missing values and remove duplicates
    nasdaq_data.dropna(inplace=True)
    nasdaq_data.drop_duplicates(inplace=True)

    # Data type correction
    nasdaq_data['Open'] = nasdaq_data['Open'].astype(float)
    nasdaq_data['High'] = nasdaq_data['High'].astype(float)
    nasdaq_data['Low'] = nasdaq_data['Low'].astype(float)
    nasdaq_data['Close'] = nasdaq_data['Close'].astype(float)
    nasdaq_data['Volume'] = nasdaq_data['Volume'].astype(int)

    # Normalization/Standardization
    scaler = StandardScaler()
    nasdaq_data[['Open', 'High', 'Low', 'Close']] = scaler.fit_transform(nasdaq_data[['Open', 'High', 'Low', 'Close']])

    # Date Handling
    nasdaq_data['Date'] = pd.to_datetime(nasdaq_data['Date'])
    nasdaq_data.set_index('Date', inplace=True)

    logger.info("NASDAQ data preprocessed successfully.")
    return nasdaq_data
