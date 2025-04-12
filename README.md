# Sector Shockwaves: Analyzing Volatility in the S&P 500

## 1. Tech Stack

*   **Languages:** Python, SQL
*   **Database:** AWS RDS MySQL
*   **Data Transformation:** dbt (Data Build Tool)
*   **Data Sources:** Wikipedia (Web Scraping), Tiingo API
*   **Data Extraction/Loading:** Python (`requests`, `BeautifulSoup`, `pandas`, `SQLAlchemy`)
*   **Analysis Environment:** Jupyter Notebooks
*   **CI/CD & Version Control:** GitHub, GitHub Actions
*   **Visualization:** Excel

## 2. Project Objective

*   **Audience:** Investment analysts, portfolio managers, and risk analysts.
*   **Problem:** Effectively understanding and navigating market volatility is crucial for investment decisions. This project addresses the need to identify which sectors exhibit significant changes in risk profiles (volatility) and performance momentum (Sharpe ratio), and to diagnose the underlying drivers (sub-industries or specific companies) behind these shifts.
*   **Solution:** This project provides a framework for analyzing S&P 500 sector and company-level volatility and risk-adjusted returns. It leverages automated data extraction from Wikipedia and the Tiingo API, robust data transformation using dbt, and SQL-based analysis within a Jupyter Notebook to:
    *   Calculate and compare sector-level volatility trends and year-over-year changes.
    *   Identify sectors with the strongest momentum using rolling Sharpe ratios compared to historical benchmarks.
    *   Diagnose the drivers of significant volatility changes by examining sub-industry and company-level contributions.
    *   Provide actionable insights and visualization suggestions to support investment strategy and risk management.

## 3. Job Description

**Company:** TCW (Trust Company of the West)
**Title:** Investment Risk Analyst
**Job Description Summary:** TCW is seeking an Investment Risk Analyst to join their dynamic Investment Risk team, which works across various departments including Portfolio Management and Client Services. The role involves the ongoing development and research of risk modeling and management processes across multiple asset classes, collaborating with internal teams and vendors on risk analytics, supporting the derivative risk management program, improving and automating processes, and contributing to projects focused on counterparty and liquidity risk. The position requires strong analytical and technical skills, investment management domain expertise, and proficiency in quantitative analysis and data handling.

**Project Relevance:** This project directly aligns with the core responsibilities of the Investment Risk Analyst position at TCW. It demonstrates the ability to conduct **quantitative and data-driven risk analysis** by calculating and interpreting volatility and Sharpe ratios for S&P 500 sectors and companies. The use of **SQL** for complex data aggregation and analysis, **Python** for data extraction (API interaction, web scraping), and **dbt** for data modeling showcases the required **technical background** and **analytical skills**. Specifically, the diagnostic analysis identifying drivers of sector volatility provides practical experience in generating the **risk insights** needed to support investment teams, mirroring the essential duties described in the job posting.

**Link to Job Description:** [Link to Job_Description.pdf](./Proposal/Job_Description.pdf)
## 4. Data

*   **Sources:**
    *   **S&P 500 Constituents:** [Wikipedia List of S&P 500 companies](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies) (Scraped for company names, tickers, sectors, industries).
    *   **End-of-Day Stock Prices:** [Tiingo API](https://www.tiingo.com/) (Used for historical daily open, high, low, close, volume, adjusted prices, dividends, and splits).
*   **Characteristics:**
    *   Covers constituents of the S&P 500 index.
    *   Historical daily financial market data, primarily from 2014/2015 through Q1 2025 based on the analysis performed.
    *   Includes adjusted prices to account for corporate actions like dividends and splits, crucial for accurate return and volatility calculations.

## 5. Notebooks/Python Scripts

*   **[`notebooks/Wikipedia_Web_Scrape_Extract_Load_Python.py`](./notebooks/Wikipedia_Web_Scrape_Extract_Load_Python.py)**: Python script that scrapes the list of S&P 500 companies, their sectors, industries, and other metadata from the relevant Wikipedia page and loads this information into the `raw_wikipedia_sp500` table in the MySQL database.
*   **[`notebooks/Tiingo_API_Extract_Load_Python.py`](./notebooks/Tiingo_API_Extract_Load_Python.py)**: Python script that fetches historical end-of-day stock price data (including adjusted prices) from the Tiingo API for the S&P 500 tickers. It loads this data into the `raw_prices` table, handling batching and basic error checking to manage API rate limits and data integrity.
*   **[`notebooks/Tiingo_API_SQL_Analysis.ipynb`](./notebooks/Tiingo_API_SQL_Analysis.ipynb)**: The primary Jupyter Notebook for the analysis phase. It connects to the database (using credentials from `.env`), executes SQL queries against the dbt-transformed warehouse tables (`fact_price`, `dim_symbol`, `dim_date`) to perform descriptive (e.g., Sharpe ratio momentum) and diagnostic (e.g., identifying drivers of volatility spikes) analytics, presents the results in pandas DataFrames, and includes interpretations, insights, and visualization suggestions.
*   **[`notebooks/Wikipedia_Web_Scrape_Analysis.ipynb`](./notebooks/Wikipedia_Web_Scrape_Analysis.ipynb)**: *(Currently empty)* Intended as a space for potential preliminary analysis or validation of the data scraped directly from Wikipedia, such as examining sector distributions or company counts before loading into the database.

## 6. Future Improvements

1.  **Integrate Macroeconomic Data:** Incorporate key macroeconomic indicators (e.g., VIX index, interest rates, GDP growth, inflation rates) into the analysis to investigate their correlation with sector volatility and potentially build predictive models.
2.  **Develop Interactive Dashboard:** Create a web-based interactive dashboard (using tools like Streamlit, Plotly Dash, or Power BI) allowing users to dynamically select sectors, time periods, and metrics for real-time analysis and visualization, enhancing usability for analysts.
