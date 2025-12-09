from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, explode, array, struct, lit

spark = SparkSession.builder.appName("CleanStocksETL").getOrCreate()

# --- 1. Read CSV without header ---
raw_path = "data/raw/stocks.csv"
df_raw = spark.read.option("header", False).csv(raw_path)

# --- 2. Extract the first three rows as headers ---
rows = df_raw.limit(3).collect()
metric_row = rows[0]
ticker_row = rows[1]
date_row = rows[2]  # mostly empty except first column

# --- 3. Skip the first three rows to get the actual data ---
df_data = df_raw.rdd.zipWithIndex().filter(lambda x: x[1] > 2).map(lambda x: x[0]).toDF()

# --- 4. Create proper column names ---
new_cols = []
for i, (metric, ticker) in enumerate(zip(metric_row, ticker_row)):
    if i == 0:  # first column is Date
        new_cols.append("Date")
    else:
        new_cols.append(f"{metric}_{ticker}")  # e.g., Close_AAPL

df_data = df_data.toDF(*new_cols)

# --- 5. Cast Date column ---
df_data = df_data.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

# --- 6. Cast numeric columns ---
for c in df_data.columns:
    if c != "Date":
        df_data = df_data.withColumn(c, col(c).cast("double"))

# --- 7. Reshape to long format ---
tickers = ["AAPL", "MSFT", "TSLA"]
metrics = ["Open", "High", "Low", "Close", "Volume"]

long_rows = []
for t in tickers:
    long_rows.append(
        struct(
            col("Date").alias("date"),
            col(f"Open_{t}").alias("open"),
            col(f"High_{t}").alias("high"),
            col(f"Low_{t}").alias("low"),
            col(f"Close_{t}").alias("close"),
            col(f"Volume_{t}").alias("volume"),
            lit(t).alias("ticker")
        )
    )

df_long = df_data.select(explode(array(*long_rows)).alias("row")).select("row.*")

# --- 8. Write to parquet ---
df_long.write.mode("overwrite").parquet("data/cleaned/stocks_clean.parquet")
print("Clean stock data saved to data/cleaned/stocks_clean.parquet")

spark.stop()
