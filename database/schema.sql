-- Drop table if it already exists
DROP TABLE IF EXISTS stocks;

-- Create stocks table
CREATE TABLE stocks (
    id SERIAL PRIMARY KEY,          -- auto-incrementing ID
    date DATE NOT NULL,             -- trading date
    ticker VARCHAR(10) NOT NULL,    -- stock symbol, e.g., AAPL
    open NUMERIC(10, 2),            -- opening price
    high NUMERIC(10, 2),            -- highest price of the day
    low NUMERIC(10, 2),             -- lowest price of the day
    close NUMERIC(10, 2),           -- closing price
    volume BIGINT,                  -- traded volume
    UNIQUE(date, ticker)            -- prevent duplicate entries
);