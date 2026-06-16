from pyspark.sql import SparkSession
from cleaning import load_and_clean

def main():
    spark = SparkSession.builder \
        .appName("Spark Retail Analytics") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("="*50)
    print("  SPARK RETAIL - CHARGEMENT DES DONNEES")
    print("="*50)

    df = load_and_clean(spark, "/data/online_retail.csv")

    print("\nApercu des donnees nettoyees :")
    df.show(5, truncate=False)

    print(f"\nSchema final :")
    df.printSchema()

    spark.stop()

if __name__ == "__main__":
    main()