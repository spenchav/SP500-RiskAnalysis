-- Web‑scrape source
CREATE TABLE sql_project.raw_wikipedia_sp500 (
    symbol         VARCHAR(10) PRIMARY KEY,
    security       VARCHAR(100),
    gics_sector    VARCHAR(50),
    gics_sub_industry VARCHAR(60),
    headquarters   VARCHAR(100),
    date_added     DATE,
    cik            VARCHAR(10)
);

-- Drop and recreate raw_prices table with adjusted columns
DROP TABLE IF EXISTS raw_prices;
CREATE TABLE raw_prices (
  symbol VARCHAR(10),
  date DATE,
  open DECIMAL(10,4),
  high DECIMAL(10,4),
  low DECIMAL(10,4),
  close DECIMAL(10,4),
  volume BIGINT,
  adj_open DECIMAL(10,4),
  adj_high DECIMAL(10,4),
  adj_low DECIMAL(10,4),
  adj_close DECIMAL(10,4),
  adj_volume BIGINT,
  div_cash DECIMAL(10,4),
  split_factor DECIMAL(10,4),
  PRIMARY KEY (symbol, date)
);

-- Dimension tables
CREATE TABLE dim_symbol (
  symbol_id   INT AUTO_INCREMENT PRIMARY KEY,
  symbol      VARCHAR(10) UNIQUE,
  security    VARCHAR(100),
  gics_sector VARCHAR(50),
  gics_industry VARCHAR(60)
);

CREATE TABLE dim_date (
  date_id  INT AUTO_INCREMENT PRIMARY KEY,
  trade_date DATE UNIQUE,
  year SMALLINT,
  quarter TINYINT,
  month TINYINT,
  day TINYINT,
  weekday TINYINT
);

-- Fact table
CREATE TABLE fact_price (
  symbol_id INT,
  date_id   INT,
  open_price  DECIMAL(10,4),
  close_price DECIMAL(10,4),
  high_price  DECIMAL(10,4),
  low_price   DECIMAL(10,4),
  volume      BIGINT,
  PRIMARY KEY (symbol_id, date_id),
  FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id),
  FOREIGN KEY (date_id)   REFERENCES dim_date(date_id)
);

SELECT 
    TABLE_SCHEMA as 'Database',
    TABLE_NAME as 'Table',
    COLUMN_NAME as 'Column',
    DATA_TYPE as 'Data Type'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;



SELECT
    TABLE_NAME,
    COLUMN_NAME,
    COLUMN_TYPE,         -- Data type (e.g., bigint, varchar(255), date, decimal(10,2))
    IS_NULLABLE,         -- YES or NO
    COLUMN_KEY,          -- Indicates key type: PRI (Primary), UNI (Unique), MUL (Indexed - often a Foreign Key)
    COLUMN_DEFAULT,      -- Default value, if any
    EXTRA                -- Extra info like 'auto_increment'
FROM
    information_schema.COLUMNS
WHERE
    TABLE_SCHEMA = 'sql_project' -- Replace 'sql_project' if your DB name is different
ORDER BY
    TABLE_NAME,          -- Group columns by table
    ORDINAL_POSITION;    -- Keep columns in their original defined order


    WITH daily_returns AS (
  SELECT
    s.symbol,
    d.trade_date as date,
    fp.close_price as adj_close,
    -- Get the previous day's closing price for the same symbol
    LAG(fp.close_price, 1) OVER (PARTITION BY s.symbol ORDER BY d.trade_date) AS prev_adj_close,
    s.gics_sector,
    d.year,
    d.month
  FROM fact_price fp
  JOIN dim_symbol s ON fp.symbol_id = s.symbol_id
  JOIN dim_date d ON fp.date_id = d.date_id
  WHERE d.trade_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
),
-- Calculate return based on close-to-close change
monthly_stats AS (
    SELECT 
        gics_sector,
        year,
        month,
        -- Calculate monthly volatility (standard deviation of daily returns)
        STDDEV(
            CASE
                WHEN prev_adj_close IS NOT NULL AND prev_adj_close != 0 
                THEN (adj_close - prev_adj_close) / prev_adj_close
                ELSE NULL
            END
        ) AS monthly_volatility,
        -- Calculate average monthly return
        AVG(
            CASE
                WHEN prev_adj_close IS NOT NULL AND prev_adj_close != 0 
                THEN (adj_close - prev_adj_close) / prev_adj_close
                ELSE NULL
            END
        ) AS avg_monthly_return,
        -- Count number of trading days in month
        COUNT(*) as trading_days
    FROM daily_returns
    WHERE prev_adj_close IS NOT NULL
    GROUP BY gics_sector, year, month
),
sector_baselines AS (
    SELECT
        gics_sector,
        AVG(monthly_volatility) as avg_sector_volatility,
        STDDEV(monthly_volatility) as volatility_std
    FROM monthly_stats
    GROUP BY gics_sector
)
SELECT 
    ms.gics_sector,
    ms.year,
    ms.month,
    ms.monthly_volatility,
    ms.avg_monthly_return,
    sb.avg_sector_volatility as baseline_volatility,
    -- Calculate how many standard deviations away from the sector average
    (ms.monthly_volatility - sb.avg_sector_volatility) / sb.volatility_std as volatility_zscore,
    ms.trading_days
FROM monthly_stats ms
JOIN sector_baselines sb ON ms.gics_sector = sb.gics_sector
-- Filter for months with significantly higher volatility (e.g., > 1.5 standard deviations)
WHERE (ms.monthly_volatility - sb.avg_sector_volatility) / sb.volatility_std > 1.5
ORDER BY 
    ms.gics_sector,
    volatility_zscore DESC





-- First, let's check our date range and data quality
WITH date_check AS (
    SELECT 
        s.gics_sector,
        MIN(d.trade_date) as earliest_date,
        MAX(d.trade_date) as latest_date,
        COUNT(DISTINCT d.trade_date) as total_trading_days,
        COUNT(DISTINCT s.symbol) as unique_symbols
    FROM fact_price fp
    JOIN dim_symbol s ON fp.symbol_id = s.symbol_id
    JOIN dim_date d ON fp.date_id = d.date_id
    GROUP BY s.gics_sector
)
SELECT *
FROM date_check
ORDER BY gics_sector;