from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array, struct

def run_clean_stocks_etl():
    spark = SparkSession.builder.appName("CleanStocksETL").getOrCreate()

    # --- 1. READ RAW CSV WITH MULTIPLE HEADER ROWS ---
    raw_path = "data/raw/stocks.csv"

    # Read file without treating first two rows as real headers
    df_raw = spark.read.option("header", False).csv(raw_path)

    # Extract header rows
    metric_row = df_raw.first()
    ticker_row = df_raw.collect()[1]

    # Remove header rows from data
    df_data = df_raw.rdd.zipWithIndex().filter(lambda x: x[1] > 1).map(lambda x: x[0]).toDF()

    # Assign proper column names (metric_ticker)
    new_cols = []
    for metric, ticker in zip(metric_row, ticker_row):
        if metric == "Date":
            new_cols.append("Date")
        else:
            new_cols.append(f"{metric}_{ticker}")  # e.g. Close_AAPL

    df_data = df_data.toDF(*new_cols)

    # --- 2. CAST COLUMNS ---
    from pyspark.sql.functions import to_date

    df_data = df_data.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

    # Convert numeric columns
    for colname in df_data.columns:
        if colname != "Date":
            df_data = df_data.withColumn(colname, col(colname).cast("double"))

    # --- 3. RESHAPE: WIDE → LONG FORMAT ---
    tickers = ["AAPL", "MSFT", "TSLA"]
    metrics = ["Open", "High", "Low", "Close", "Volume"]

    long_rows = []

    for t in tickers:
        long_rows.append(
            struct([
                col("Date").alias("date"),
                col(f"Open_{t}").alias("open"),
                col(f"High_{t}").alias("high"),
                col(f"Low_{t}").alias("low"),
                col(f"Close_{t}").alias("close"),
                col(f"Volume_{t}").alias("volume"),
                col(lit(t)).alias("ticker")
            ])
        )

    df_long = df_data.select(explode(array(*long_rows)).alias("row")).select("row.*")

    # --- 4. WRITE CLEAN PARQUET ---
    df_long.write.mode("overwrite").parquet("data/cleaned/stocks_clean.parquet")

    print("Clean stock data saved to data/cleaned/stocks_clean.parquet")

    spark.stop()

if __name__ == "__main__":
    run_clean_stocks_etl()
