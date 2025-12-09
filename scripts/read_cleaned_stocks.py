from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("STOCKFROG").getOrCreate()

df = spark.read.parquet("data/cleaned/stocks_clean.parquet")
df.show()
df.printSchema()

spark.stop()
