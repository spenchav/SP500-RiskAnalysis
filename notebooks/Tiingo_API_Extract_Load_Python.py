# Import necessary modules
import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time
from pymysql.err import IntegrityError, OperationalError

# Load environment variables from .env
load_dotenv()

# Establish a database connection using SQLAlchemy and credentials from the .env file
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

if None in (DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME):
    raise Exception("Database credentials are not fully set in the .env file.")

engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')



# --- Query tickers from raw_wikipedia_sp500 ---
with engine.connect() as conn:
    query_wiki = text("SELECT DISTINCT symbol FROM raw_wikipedia_sp500")
    df_wiki = pd.read_sql(query_wiki, conn)



# Standardize ticker symbols: strip any extra whitespace and convert to uppercase
df_wiki['symbol'] = df_wiki['symbol'].str.strip().str.upper()



# --- Query tickers already in raw_prices ---
with engine.connect() as conn:
    query_prices = text("SELECT DISTINCT symbol FROM raw_prices")
    df_prices = pd.read_sql(query_prices, conn)

if not df_prices.empty:
    processed_tickers = set(df_prices['symbol'].str.strip().str.upper())
else:
    processed_tickers = set()

all_tickers = set(df_wiki['symbol'])
new_tickers = list(all_tickers - processed_tickers)
new_tickers.sort()


# Define batch size (e.g., 50 tickers per execution)
batch_size = 50
tickers_to_process = new_tickers[:batch_size]

print(f"Total tickers in Wikipedia table: {len(all_tickers)}")
print(f"Tickers already processed in raw_prices: {len(processed_tickers)}")
print(f"New tickers to process in this batch: {len(tickers_to_process)}")
print("Tickers in current batch:", tickers_to_process)



def get_eod_prices(symbol, start="2019-01-01", end=None, resample_freq="daily"):
    """
    Fetch historical EOD price data for a given symbol from the Tiingo API.
    Returns a DataFrame with columns: date, open, high, low, close, volume, symbol.
    """
    TIINGO_KEY = os.getenv('TIINGO_KEY')
    if not TIINGO_KEY:
        raise Exception("TIINGO_KEY not set in the .env file.")
    
    # Convert symbol format for Tiingo API (replace periods with hyphens)
    tiingo_symbol = symbol.replace('.', '-')
    
    base_url = f"https://api.tiingo.com/tiingo/daily/{tiingo_symbol}/prices"
    params = {
        'startDate': start,
        'format': 'json',
        'token': TIINGO_KEY,
        'resampleFreq': resample_freq
    }
    if end:
        params['endDate'] = end
    
    response = requests.get(base_url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not data:
        raise Exception(f"No data returned from Tiingo for symbol: {symbol}")
    df = pd.DataFrame(data)
    columns_to_keep = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = df[[col for col in columns_to_keep if col in df.columns]]
    df['date'] = pd.to_datetime(df['date'])
    for col in ['open', 'high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    if 'volume' in df.columns:
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    # Store the original symbol in the dataframe (not the Tiingo-formatted one)
    df['symbol'] = symbol
    df = df.sort_values('date').reset_index(drop=True)
    return df



for symbol in tickers_to_process:
    try:
        print(f"Processing symbol: {symbol}")
        df_symbol = get_eod_prices(symbol)
        # Wrap the insertion in a transaction so that each is rolled back if an error occurs.
        with engine.begin() as connection:
            df_symbol.to_sql(name="raw_prices", con=connection, if_exists="append", index=False)
        print(f"Symbol {symbol} processed successfully.")
    except IntegrityError as ie:
        # Duplicate entry (error code 1062) likely means data for those dates already exists.
        if "Duplicate entry" in str(ie):
            print(f"Duplicate entry error for {symbol}. Skipping insertion.")
        else:
            print(f"IntegrityError processing symbol {symbol}: {ie}")
    except OperationalError as oe:
        print(f"OperationalError processing symbol {symbol}: {oe}")
    except Exception as e:
        print(f"Error processing symbol {symbol}: {e}")
    time.sleep(1)  # Delay between requests

print("Batch processing complete. Run this cell again after one hour for the next batch.")