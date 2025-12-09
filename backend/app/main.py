from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow frontend (React) to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Database connection function
def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="stocks_db",
        user="postgres",
        password="pass",
        port=5432
    )
    return conn

@app.get("/")
def root():
    return {"message": "FastAPI backend is running!"}


# Example endpoint: return latest 5 stock records
@app.get("/stocks/latest")
def get_latest_stocks():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM stocks
        ORDER BY date DESC
        LIMIT 5;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# Endpoint to get latest stocks for a specific ticker
@app.get("/stocks/latest/{ticker}")
def get_stocks_by_ticker(ticker: str):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM stocks
        WHERE ticker = %s
        ORDER BY date DESC
        LIMIT 5;
    """, (ticker.upper(),))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@app.get("/stocks/latest/AAPL")
def get_apple_stocks():
    return get_stocks_by_ticker("AAPL")


@app.get("/stocks/latest/TSLA")
def get_tesla_stocks():
    return get_stocks_by_ticker("TSLA")


@app.get("/stocks/latest/MSFT")
def get_microsoft_stocks():
    return get_stocks_by_ticker("MSFT")