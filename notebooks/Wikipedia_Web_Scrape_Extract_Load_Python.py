''' Install necessary packages if not already installed
pip install BeautifulSoup
pip install pymysql
pip install python-dotenv
pip install pandas
pip install bs4
'''

# Import necessary modules
import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from dotenv import load_dotenv


# Load environment variables from .env (data base credentials)
load_dotenv()


# URL for the Wikipedia page for the S&P 500 companies.
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'


# Fetch the webpage
response = requests.get(url)
response.raise_for_status()  # Raise an exception for HTTP errors



# Parse the HTML content with BeautifulSoup
soup = BeautifulSoup(response.content, "html.parser")

# Find the table with id "constituents"
table = soup.find("table", {"id": "constituents"})
if table is None:
    raise Exception("Could not find the 'constituents' table on the page.")



# Attempt to extract headers using <thead> if available, otherwise use the first row of the table.
thead = table.find("thead")
if thead:
    headers = [th.get_text(strip=True) for th in thead.find_all("th")]
else:
    # Fallback: Use the first row (assumed to be header) from the table
    first_row = table.find("tr")
    if first_row:
        headers = [cell.get_text(strip=True) for cell in first_row.find_all(["th", "td"])]
    else:
        raise Exception("No header row found in the table.")
    


    # Extract all data rows (skip header row if it's within <tbody>)
tbody = table.find("tbody")
if tbody:
    data_rows = tbody.find_all("tr")
else:
    # If there is no <tbody>, get all <tr> elements and skip the first row (headers)
    data_rows = table.find_all("tr")[1:]



    rows = []
for tr in data_rows:
    # Get all cell values from the row (using <td> elements)
    cells = tr.find_all("td")
    row = [cell.get_text(strip=True) for cell in cells]
    if row:
        rows.append(row)


# Create a DataFrame from the scraped data
df_sp500 = pd.DataFrame(rows, columns=headers)
print("Sample scraped data from Wikipedia:")
print(df_sp500.head())


# Establish a database connection using credentials from your .env file
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

if None in (DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME):
    raise Exception("One or more database credentials are not set in the .env file.")

# Create a SQLAlchemy engine for MySQL
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")



# Write the DataFrame to a MySQL table named 'raw_wikipedia_sp500'
df_sp500.to_sql(name="raw_wikipedia_sp500", con=engine, if_exists="replace", index=False)
print("Scraped Wikipedia data loaded successfully into 'raw_wikipedia_sp500' table.")