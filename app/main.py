import argparse
from pyspark.sql import SparkSession
from cleaning import load_and_clean
from analysis import run_analysis
from streaming import run_streaming


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Retail Analytics")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--analysis-only", action="store_true",
                       help="Lancer uniquement l'analyse batch")
    group.add_argument("--streaming-only", action="store_true",
                       help="Lancer uniquement le streaming")
    parser.add_argument("--no-charts", action="store_true",
                        help="Desactiver la generation des graphiques")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder \
        .appName("Spark Retail Analytics") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    if not args.streaming_only:
        print("=" * 50)
        print("  SPARK RETAIL - CHARGEMENT DES DONNEES")
        print("=" * 50)

        df = load_and_clean(spark, "hdfs://namenode:9000/data/batch/online_retail.csv")

        print("\nApercu des donnees nettoyees :")
        df.show(5, truncate=False)
        df.printSchema()

        df.cache()

        run_analysis(df, generate_charts=not args.no_charts)

    if not args.analysis_only:
        run_streaming(spark)

    spark.stop()


if __name__ == "__main__":
    main()
