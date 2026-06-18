from pyspark.sql import SparkSession
from cleaning import load_and_clean
from analysis import run_analysis
from streaming import run_streaming

def main():
    spark = SparkSession.builder \
        .appName("Spark Retail Analytics") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 50)
    print("  SPARK RETAIL - CHARGEMENT DES DONNEES")
    print("=" * 50)

    df = load_and_clean(spark, "hdfs://namenode:9000/data/online_retail.csv")

    print("\nApercu des donnees nettoyees :")
    df.show(5, truncate=False)
    df.printSchema()

    # Cache du DataFrame pour éviter de relire HDFS à chaque analyse
    df.cache()

    # Analyses métier et KPIs
    run_analysis(df)

    # Streaming temps réel
    run_streaming(spark)

    spark.stop()

if __name__ == "__main__":
    main()
