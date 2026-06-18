from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def run_streaming(spark: SparkSession):
    print("=" * 50)
    print("  SPARK STREAMING - SIMULATION TEMPS REEL")
    print("=" * 50)

    schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("TotalPrice", DoubleType(), True),
    ])

    df_stream = spark.readStream \
        .schema(schema) \
        .option("sep", ";") \
        .option("header", "true") \
        .csv("hdfs://namenode:9000/data/")

    df_filtered = df_stream.filter(
        col("TotalPrice").isNotNull() & col("Country").isNotNull()
    ).select("InvoiceNo", "Country", "TotalPrice")

    query = df_filtered.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("[INFO] Streaming démarré. En attente de données...")
    query.awaitTermination(30)
    print("[INFO] Streaming terminé.")