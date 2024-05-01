from dagster import op, job, get_dagster_logger
import pandas as pd
import sqlalchemy
import statsmodels.api as sm
from statsmodels.tsa.holtwinters import SimpleExpSmoothing, ExponentialSmoothing
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error
import numpy as np

POSTGRES_URI = 'postgresql://username:password@localhost:5432/stockdata'

def load_data_from_postgres(table_name):
    engine = sqlalchemy.create_engine(POSTGRES_URI)
    return pd.read_sql(table_name, engine)

@op
def full_stock_analysis():
    logger = get_dagster_logger()
    apple_data = load_data_from_postgres('apple_stock')
    nasdaq_data = load_data_from_postgres('nasdaq_stock')

    # Descriptive Statistics
    apple_desc = apple_data.describe()
    nasdaq_desc = nasdaq_data.describe()
    logger.info(f"Apple Descriptive Stats: {apple_desc}")
    logger.info(f"NASDAQ Descriptive Stats: {nasdaq_desc}")

    # Correlation Analysis
    combined_data = apple_data.join(nasdaq_data, lsuffix='_apple', rsuffix='_nasdaq')
    correlation_matrix = combined_data.corr()
    logger.info(f"Correlation Matrix: {correlation_matrix}")

    # Time Series Analysis
    apple_model = sm.tsa.seasonal_decompose(apple_data['Close'], model='additive', period=252)
    nasdaq_model = sm.tsa.seasonal_decompose(nasdaq_data['Close'], model='additive', period=252)
    apple_trend = apple_model.trend.dropna()
    nasdaq_trend = nasdaq_model.trend.dropna()
    apple_residual = apple_model.resid.dropna()
    nasdaq_residual = nasdaq_model.resid.dropna()

    # Predictive Modeling
    models = {
        'SES_Apple': SimpleExpSmoothing(apple_data['Close']).fit(),
        'SES_NASDAQ': SimpleExpSmoothing(nasdaq_data['Close']).fit(),
        'Holt_Apple': ExponentialSmoothing(apple_data['Close'], trend='add').fit(),
        'Holt_NASDAQ': ExponentialSmoothing(nasdaq_data['Close'], trend='add').fit(),
        'Holt-Winters_Apple': ExponentialSmoothing(apple_data['Close'], trend='add', seasonal='mul', seasonal_periods=252).fit(),
        'Holt-Winters_NASDAQ': ExponentialSmoothing(nasdaq_data['Close'], trend='add', seasonal='mul', seasonal_periods=252).fit(),
        'ARIMA_Apple': ARIMA(apple_data['Close'], order=(1,1,1)).fit(),
        'ARIMA_NASDAQ': ARIMA(nasdaq_data['Close'], order=(1,1,1)).fit(),
        'SARIMA_Apple': SARIMAX(apple_data['Close'], order=(1,1,1), seasonal_order=(1,1,1,252)).fit(),
        'SARIMA_NASDAQ': SARIMAX(nasdaq_data['Close'], order=(1,1,1), seasonal_order=(1,1,1,252)).fit()
    }

    # Evaluating models and selecting the best based on AIC
    best_model = min(models.items(), key=lambda x: x[1].aic)
    logger.info(f"Best model: {best_model[0]} with AIC: {best_model[1].aic}")

    return {
        'descriptive_statistics': (apple_desc, nasdaq_desc),
        'correlation': correlation_matrix,
        'trends': (apple_trend, nasdaq_trend),
        'residuals': (apple_residual, nasdaq_residual),
        'best_model': best_model
    }

@job
def etl_and_analysis_job():
    full_stock_analysis()
