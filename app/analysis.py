from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct, avg,
    month, year, date_format, round as spark_round,
    desc, rank
)
from pyspark.sql.window import Window


# ─── KPIs globaux ────────────────────────────────────────────────────────────

def print_global_kpis(df: DataFrame):
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

def analyze_monthly_revenue(df: DataFrame):
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


def analyze_revenue_by_day(df: DataFrame):
    print("=" * 50)
    print("  CHIFFRE D'AFFAIRES PAR JOUR DE LA SEMAINE")
    print("=" * 50)

    by_day = df.withColumn("Jour", date_format("InvoiceDate", "EEEE")) \
               .groupBy("Jour") \
               .agg(spark_round(spark_sum("TotalPrice"), 2).alias("CA_Total")) \
               .orderBy(desc("CA_Total"))

    by_day.show(truncate=False)


# ─── Analyses produits ────────────────────────────────────────────────────────

def analyze_top_products_by_revenue(df: DataFrame, n: int = 10):
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


def analyze_top_products_by_quantity(df: DataFrame, n: int = 10):
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


# ─── Analyses clients ─────────────────────────────────────────────────────────

def analyze_top_customers(df: DataFrame, n: int = 10):
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


def analyze_customer_segments(df: DataFrame):
    """Segmente les clients en 3 groupes selon leur dépense totale."""
    print("=" * 50)
    print("  SEGMENTATION CLIENTS (depense totale)")
    print("=" * 50)

    from pyspark.sql.functions import when

    customer_spend = df.groupBy("CustomerID") \
        .agg(spark_round(spark_sum("TotalPrice"), 2).alias("CA_Total"))

    segmented = customer_spend.withColumn(
        "Segment",
        when(col("CA_Total") >= 5000, "Premium")
        .when(col("CA_Total") >= 1000, "Standard")
        .otherwise("Occasionnel")
    )

    segmented.groupBy("Segment") \
             .agg(
                 count("CustomerID").alias("Nb_Clients"),
                 spark_round(avg("CA_Total"), 2).alias("CA_Moyen")
             ) \
             .orderBy(desc("Nb_Clients")) \
             .show(truncate=False)


# ─── Analyses géographiques ───────────────────────────────────────────────────

def analyze_revenue_by_country(df: DataFrame, n: int = 15):
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


# ─── Point d'entrée ───────────────────────────────────────────────────────────

def run_analysis(df: DataFrame):
    print_global_kpis(df)
    analyze_monthly_revenue(df)
    analyze_revenue_by_day(df)
    analyze_top_products_by_revenue(df)
    analyze_top_products_by_quantity(df)
    analyze_top_customers(df)
    analyze_customer_segments(df)
    analyze_revenue_by_country(df)
