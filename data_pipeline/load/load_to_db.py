import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

def load_to_postgres(parquet_path):
    # Load parquet file into DataFrame
    df = pd.read_parquet(parquet_path)

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        database="stocks_db",
        user="postgres",
        password="pass",
        port=5432
    )

    cursor = conn.cursor()

    # Prepare insert query
    insert_query = """
        INSERT INTO stocks (date, ticker, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, ticker) DO NOTHING;
    """

    # Convert DataFrame rows → tuples
    data = [
        (
            row["date"],
            row["ticker"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"]
        )
        for _, row in df.iterrows()
    ]

    execute_batch(cursor, insert_query, data)
    conn.commit()

    cursor.close()
    conn.close()

    print("Data successfully loaded into PostgreSQL.")

if __name__ == "__main__":
    load_to_postgres("data/cleaned/stocks_clean.parquet")
