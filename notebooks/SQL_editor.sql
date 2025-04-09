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


SELECT *
FROM raw_alpha_vantage_prices;


CREATE TABLE raw_prices (
  symbol      VARCHAR(10),
  trade_date  DATE,
  open_price  DECIMAL(10,4),
  high_price  DECIMAL(10,4),
  low_price   DECIMAL(10,4),
  close_price DECIMAL(10,4),
  volume      BIGINT,
  PRIMARY KEY (symbol, trade_date)
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
