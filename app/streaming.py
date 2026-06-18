from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as spark_sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def run_streaming(spark: SparkSession):
    print("=" * 50)
    print("  SPARK STREAMING - SIMULATION TEMPS REEL")
    print("=" * 50)

    # Schema du dataset
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

    # Lecture en streaming depuis HDFS
    df_stream = spark.readStream \
        .schema(schema) \
        .option("sep", ";") \
        .option("header", "true") \
        .csv("hdfs://namenode:9000/data/")

    # Calcul du chiffre d'affaires en temps réel par pays
    df_agg = df_stream \
        .filter(col("TotalPrice").isNotNull()) \
        .groupBy("Country") \
        .agg(
            spark_sum("TotalPrice").alias("CA_Total"),
            count("InvoiceNo").alias("Nb_Transactions")
        )

    # Affichage dans la console
    query = df_agg.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("[INFO] Streaming démarré. En attente de données...")
    query.awaitTermination(60)
    print("[INFO] Streaming terminé.")