from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct, avg,
    month, year, date_format, round as spark_round,
    desc, rank, datediff, max as spark_max, ntile, when, lit
)
from pyspark.sql.window import Window

HDFS_OUTPUT = "hdfs://namenode:9000/data/output"


# ─── KPIs globaux ────────────────────────────────────────────────────────────

def print_global_kpis(df: DataFrame) -> None:
    """Affiche les KPIs globaux du dataset (CA, commandes, clients, produits)."""
    print("=" * 50)
    print("  KPIs GLOBAUX")
    print("=" * 50)

    kpis = df.agg(
        spark_round(spark_sum("TotalPrice"), 2).alias("Chiffre_dAffaires_Total"),
        countDistinct("InvoiceNo").alias("Nombre_Commandes"),
        countDistinct("CustomerID").alias("Nombre_Clients"),
        countDistinct("StockCode").alias("Nombre_Produits"),
        spark_round(avg("TotalPrice"), 2).alias("Panier_Moyen_par_Ligne"),
    ).collect()[0]

    avg_order_value = df.groupBy("InvoiceNo") \
        .agg(spark_sum("TotalPrice").alias("order_total")) \
        .agg(spark_round(avg("order_total"), 2).alias("avg")).collect()[0]["avg"]

    print(f"  Chiffre d'affaires total : £{kpis['Chiffre_dAffaires_Total']:,.2f}")
    print(f"  Nombre de commandes      : {kpis['Nombre_Commandes']:,}")
    print(f"  Nombre de clients        : {kpis['Nombre_Clients']:,}")
    print(f"  Nombre de produits       : {kpis['Nombre_Produits']:,}")
    print(f"  Valeur moyenne commande  : £{avg_order_value:,.2f}")
    print()


# ─── Analyses ventes ─────────────────────────────────────────────────────────

def analyze_monthly_revenue(df: DataFrame) -> DataFrame:
    """Calcule le chiffre d'affaires et le nombre de commandes par mois."""
    print("=" * 50)
    print("  EVOLUTION DU CHIFFRE D'AFFAIRES MENSUEL")
    print("=" * 50)

    monthly = df.withColumn("Annee", year("InvoiceDate")) \
                .withColumn("Mois", month("InvoiceDate")) \
                .groupBy("Annee", "Mois") \
                .agg(
                    spark_round(spark_sum("TotalPrice"), 2).alias("CA_Mensuel"),
                    countDistinct("InvoiceNo").alias("Nb_Commandes")
                ) \
                .orderBy("Annee", "Mois")

    monthly.show(24, truncate=False)
    return monthly


def analyze_revenue_by_day(df: DataFrame) -> DataFrame:
    """Calcule le chiffre d'affaires total par jour de la semaine."""
    print("=" * 50)
    print("  CHIFFRE D'AFFAIRES PAR JOUR DE LA SEMAINE")
    print("=" * 50)

    by_day = df.withColumn("Jour", date_format("InvoiceDate", "EEEE")) \
               .groupBy("Jour") \
               .agg(spark_round(spark_sum("TotalPrice"), 2).alias("CA_Total")) \
               .orderBy(desc("CA_Total"))

    by_day.show(truncate=False)
    return by_day


# ─── Analyses produits ────────────────────────────────────────────────────────

def analyze_top_products_by_revenue(df: DataFrame, n: int = 10) -> DataFrame:
    """Calcule le top N produits par chiffre d'affaires total."""
    print("=" * 50)
    print(f"  TOP {n} PRODUITS PAR CHIFFRE D'AFFAIRES")
    print("=" * 50)

    top = df.groupBy("StockCode", "Description") \
            .agg(
                spark_round(spark_sum("TotalPrice"), 2).alias("CA_Total"),
                spark_sum("Quantity").alias("Quantite_Vendue")
            ) \
            .orderBy(desc("CA_Total")) \
            .limit(n)

    top.show(truncate=False)
    return top


def analyze_top_products_by_quantity(df: DataFrame, n: int = 10) -> DataFrame:
    """Calcule le top N produits par quantité vendue."""
    print("=" * 50)
    print(f"  TOP {n} PRODUITS PAR QUANTITE VENDUE")
    print("=" * 50)

    top = df.groupBy("StockCode", "Description") \
            .agg(
                spark_sum("Quantity").alias("Quantite_Vendue"),
                spark_round(spark_sum("TotalPrice"), 2).alias("CA_Total")
            ) \
            .orderBy(desc("Quantite_Vendue")) \
            .limit(n)

    top.show(truncate=False)
    return top


def analyze_top_products_by_country(df: DataFrame, n: int = 5) -> DataFrame:
    """Calcule le top N produits par CA pour chaque pays via une window function rank()."""
    print("=" * 50)
    print(f"  TOP {n} PRODUITS PAR PAYS (CA)")
    print("=" * 50)

    product_country = df.groupBy("Country", "StockCode", "Description") \
        .agg(spark_round(spark_sum("TotalPrice"), 2).alias("CA_Total"))

    window_spec = Window.partitionBy("Country").orderBy(desc("CA_Total"))
    ranked = product_country \
        .withColumn("Rang", rank().over(window_spec)) \
        .filter(col("Rang") <= n) \
        .orderBy("Country", "Rang")

    ranked.show(100, truncate=False)
    return ranked


# ─── Analyses clients ─────────────────────────────────────────────────────────

def analyze_top_customers(df: DataFrame, n: int = 10) -> DataFrame:
    """Calcule le top N clients par chiffre d'affaires total."""
    print("=" * 50)
    print(f"  TOP {n} CLIENTS PAR CHIFFRE D'AFFAIRES")
    print("=" * 50)

    top = df.groupBy("CustomerID") \
            .agg(
                spark_round(spark_sum("TotalPrice"), 2).alias("CA_Total"),
                countDistinct("InvoiceNo").alias("Nb_Commandes"),
                spark_round(avg("TotalPrice"), 2).alias("Panier_Moyen")
            ) \
            .orderBy(desc("CA_Total")) \
            .limit(n)

    top.show(truncate=False)
    return top


def analyze_customer_segments(df: DataFrame) -> DataFrame:
    """Segmente les clients avec la méthode RFM (Recency, Frequency, Monetary)."""
    print("=" * 50)
    print("  SEGMENTATION CLIENTS RFM")
    print("=" * 50)

    max_date = df.agg(spark_max("InvoiceDate").alias("max_date")).collect()[0]["max_date"]

    rfm = df.groupBy("CustomerID").agg(
        datediff(lit(max_date), spark_max("InvoiceDate")).alias("Recency"),
        countDistinct("InvoiceNo").alias("Frequency"),
        spark_round(spark_sum("TotalPrice"), 2).alias("Monetary")
    )

    # Recency DESC → ntile 1 = client le moins récent (mauvais), 4 = le plus récent (bon)
    w_r = Window.orderBy(desc("Recency"))
    # Frequency/Monetary ASC → ntile 1 = plus faible (mauvais), 4 = plus élevé (bon)
    w_f = Window.orderBy("Frequency")
    w_m = Window.orderBy("Monetary")

    scored = rfm \
        .withColumn("R_Score", ntile(4).over(w_r)) \
        .withColumn("F_Score", ntile(4).over(w_f)) \
        .withColumn("M_Score", ntile(4).over(w_m)) \
        .withColumn("RFM_Score", col("R_Score") + col("F_Score") + col("M_Score"))

    segmented = scored.withColumn(
        "Segment",
        when(col("RFM_Score") >= 10, "Champions")
        .when(
            (col("F_Score") >= 3) & (col("M_Score") >= 3) & (col("R_Score") <= 2),
            "Clients a risque"
        )
        .when(col("RFM_Score") <= 5, "Clients perdus")
        .otherwise("Clients reguliers")
    )

    print("  Répartition des segments :")
    segmented.groupBy("Segment") \
        .agg(
            count("CustomerID").alias("Nb_Clients"),
            spark_round(avg("Monetary"), 2).alias("CA_Moyen")
        ) \
        .orderBy(desc("Nb_Clients")) \
        .show(truncate=False)

    return segmented


# ─── Analyses géographiques ───────────────────────────────────────────────────

def analyze_revenue_by_country(df: DataFrame, n: int = 15) -> DataFrame:
    """Calcule le chiffre d'affaires, le nombre de clients et commandes par pays."""
    print("=" * 50)
    print(f"  CHIFFRE D'AFFAIRES PAR PAYS (top {n})")
    print("=" * 50)

    by_country = df.groupBy("Country") \
                   .agg(
                       spark_round(spark_sum("TotalPrice"), 2).alias("CA_Total"),
                       countDistinct("CustomerID").alias("Nb_Clients"),
                       countDistinct("InvoiceNo").alias("Nb_Commandes")
                   ) \
                   .orderBy(desc("CA_Total")) \
                   .limit(n)

    by_country.show(truncate=False)
    return by_country


# ─── Point d'entrée ───────────────────────────────────────────────────────────

def run_analysis(df: DataFrame, generate_charts: bool = True) -> None:
    """Exécute toutes les analyses et exporte les résultats vers HDFS en Parquet."""
    print_global_kpis(df)

    monthly = analyze_monthly_revenue(df)
    monthly.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/monthly_revenue/")
    print(f"[INFO] Export Parquet : {HDFS_OUTPUT}/monthly_revenue/")

    analyze_revenue_by_day(df)

    top_products = analyze_top_products_by_revenue(df)
    top_products.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/top_products/")
    print(f"[INFO] Export Parquet : {HDFS_OUTPUT}/top_products/")

    analyze_top_products_by_quantity(df)

    top_by_country = analyze_top_products_by_country(df)
    top_by_country.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/top_products_by_country/")
    print(f"[INFO] Export Parquet : {HDFS_OUTPUT}/top_products_by_country/")

    analyze_top_customers(df)

    rfm_segments = analyze_customer_segments(df)
    rfm_segments.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/rfm_segments/")
    print(f"[INFO] Export Parquet : {HDFS_OUTPUT}/rfm_segments/")

    by_country = analyze_revenue_by_country(df)
    by_country.write.mode("overwrite").parquet(f"{HDFS_OUTPUT}/revenue_by_country/")
    print(f"[INFO] Export Parquet : {HDFS_OUTPUT}/revenue_by_country/")

    if generate_charts:
        print("=" * 50)
        print("  GENERATION DES VISUALISATIONS")
        print("=" * 50)
        try:
            from visualizations import generate_all_charts
            generate_all_charts(
                monthly_df=monthly,
                top_products_df=top_products,
                country_df=by_country,
                rfm_df=rfm_segments,
            )
        except ImportError as e:
            print(f"[INFO] Visualisations ignorees (matplotlib manquant) : {e}")
