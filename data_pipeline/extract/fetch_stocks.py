import yfinance as yf
import pandas as pd
import os

def fetch_stocks(tickers=["AAPL", "MSFT", "TSLA"]):
    os.makedirs("data", exist_ok=True)
    data = yf.download(tickers, period="1y", interval="1d")
    data.to_csv("data/stocks_raw.csv")
    return data

if __name__ == "__main__":
    fetch_stocks()