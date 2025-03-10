import yfinance as yf
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

### Extract Data from Yahoo Finance
def extract_stock_data(ticker, start_date, end_date):
    stock = yf.Ticker(ticker)
    stock_data = stock.history(start=start_date, end=end_date)
    stock_data.to_csv("raw_stock_data.csv", index=False)
    return stock_data

stock_df = extract_stock_data('AAPL', '2024-12-01', '2024-12-31')
#print(df.head())

### Transform Data
def transform_stock_data(df):
    spark = SparkSession.builder.appName("StockDataETL").getOrCreate()

    ### Convert to Spark DataFrame
    df = spark.read.csv("raw_stock_data.csv", header=True, inferSchema=True)

    df_transformed = df.select(
        col("Open").alias("open_price"),
        col("High").alias("high_price"),
        col("Low").alias("low_price"),
        col("Close").alias("close_price"),
        col("Volume").alias("volume"),
    ).withColumn("open_price", round(col("open_price"), 2)) \
        .withColumn("high_price", round(col("high_price"), 2)) \
        .withColumn("low_price", round(col("low_price"), 2)) \
        .withColumn("close_price", round(col("close_price"), 2))

    df_transformed = df.drop("Dividends", "Stock Splits")

    ### Save Transformed Data
    df_transformed.write.format("csv").mode("overwrite").save("stock_data_transformed.csv")


transform_stock_data(stock_df)



