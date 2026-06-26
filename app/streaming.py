from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, round as spark_round, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType
)


def run_streaming(spark: SparkSession, timeout: int = 120) -> None:
    """Lance un job Structured Streaming surveillant /data/stream/ et agrège le CA par pays."""
    print("=" * 50)
    print("  SPARK STREAMING - SIMULATION TEMPS REEL")
    print("=" * 50)

    # Schéma brut du CSV source (identique au fichier online_retail.csv)
    schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", StringType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
    ])

    df_stream = spark.readStream \
        .schema(schema) \
        .option("sep", ";") \
        .option("header", "true") \
        .csv("hdfs://namenode:9000/data/stream/")

    df_ca = df_stream \
        .withColumn("UnitPrice_num",
                    regexp_replace(col("UnitPrice"), ",", ".").cast("double")) \
        .withColumn("Quantity_num", col("Quantity").cast("integer")) \
        .filter(
            col("Country").isNotNull()
            & col("UnitPrice_num").isNotNull()
            & col("Quantity_num").isNotNull()
            & (col("Quantity_num") > 0)
            & (col("UnitPrice_num") > 0)
        ) \
        .withColumn("CA", col("Quantity_num") * col("UnitPrice_num")) \
        .groupBy("Country") \
        .agg(
            spark_round(spark_sum("CA"), 2).alias("CA_Cumule"),
            count("*").alias("Nb_Transactions")
        )

    query = df_ca.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 30) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("[INFO] Streaming demarre. En attente de donnees dans /data/stream/...")
    print("[INFO] Lancez 'python3 /app/producer.py' dans un autre terminal pour envoyer des donnees.")
    query.awaitTermination(timeout)
    print("[INFO] Streaming termine.")
