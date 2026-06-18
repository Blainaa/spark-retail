from pyspark.sql import SparkSession
from cleaning import load_and_clean
from streaming import run_streaming

def main():
    # Initialisation de la session Spark connectée au cluster + HDFS
    spark = SparkSession.builder \
        .appName("Spark Retail Analytics") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 50)
    print("  SPARK RETAIL - CHARGEMENT DES DONNEES")
    print("=" * 50)

    # Chargement et nettoyage depuis HDFS
    df = load_and_clean(spark, "hdfs://namenode:9000/data/online_retail.csv")

    print("\nApercu des donnees nettoyees :")
    df.show(5, truncate=False)

    print(f"\nSchema final :")
    df.printSchema()

    # Lancement du streaming
    run_streaming(spark)

    spark.stop()

if __name__ == "__main__":
    main()