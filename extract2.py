from dagster import op, get_dagster_logger
import requests
import pandas as pd
from datetime import datetime

API_KEY = "OG1G8BMZRGYUJPES"
BASE_URL = "https://www.alphavantage.co/query"

@op
def extract_nasdaq_data():
    logger = get_dagster_logger()
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": "IXIC",
        "apikey": API_KEY,
        "datatype": "json",
        "outputsize": "full"
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        logger.info("Data extraction successful")
        data = response.json()['Time Series (Daily)']
        df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adjusted Close', 'Volume'])
        for date, values in data.items():
            if datetime.strptime(date, '%Y-%m-%d') >= datetime(1980, 12, 12) and datetime.strptime(date, '%Y-%m-%d') <= datetime(2024, 4, 20):
                df = df.append({
                    "Date": date,
                    "Open": float(values['1. open']),
                    "High": float(values['2. high']),
                    "Low": float(values['3. low']),
                    "Close": float(values['4. close']),
                    "Adjusted Close": float(values['5. adjusted close']),
                    "Volume": int(values['6. volume'])
                }, ignore_index=True)
        return df
    else:
        logger.error("Failed to extract data")
        return pd.DataFrame()
