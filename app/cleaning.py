from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round as spark_round, regexp_replace, when
from pyspark.sql.types import DoubleType

def load_and_clean(spark: SparkSession, path: str):
    # Chargement avec separateur point-virgule
    df = spark.read.csv(path, header=True, inferSchema=False, sep=";")
    print(f"[INFO] Lignes brutes : {df.count()}")

    # Correction decimales : remplacer virgule par point pour UnitPrice
    df = df.withColumn("UnitPrice", regexp_replace(col("UnitPrice"), ",", ".").cast(DoubleType()))
    df = df.withColumn("Quantity", col("Quantity").cast("integer"))

    # Conversion de la date
    df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm"))

    # Identifier et filtrer les annulations (InvoiceNo commencant par C)
    df = df.withColumn("IsCancelled", when(col("InvoiceNo").startswith("C"), True).otherwise(False))
    df = df.filter(col("IsCancelled") == False).drop("IsCancelled")
    print(f"[INFO] Lignes apres suppression des annulations : {df.count()}")

    # Suppression des lignes sans CustomerID ou Description
    df = df.dropna(subset=["CustomerID", "Description"])

    # Suppression des quantites et prix negatifs ou nuls
    df = df.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))

    # Colonne TotalPrice
    df = df.withColumn("TotalPrice", spark_round(col("Quantity") * col("UnitPrice"), 2))

    # Suppression des doublons
    df = df.dropDuplicates()

    print(f"[INFO] Lignes apres nettoyage : {df.count()}")
    df.printSchema()

    return df