"""Génère des graphiques matplotlib à partir des DataFrames Spark agrégés."""

import os

import matplotlib
matplotlib.use("Agg")  # backend non-interactif, compatible Docker
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, avg

OUTPUT_DIR = "/app/output/charts"


def _ensure_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def chart_monthly_revenue(monthly_df: DataFrame) -> None:
    """Génère un line chart de l'évolution du CA mensuel."""
    _ensure_dir()
    pdf = monthly_df.orderBy("Annee", "Mois").toPandas()
    pdf["label"] = (
        pdf["Annee"].astype(str) + "-" + pdf["Mois"].astype(str).str.zfill(2)
    )

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(pdf["label"], pdf["CA_Mensuel"], marker="o", linewidth=2)
    ax.set_title("Evolution du CA mensuel")
    ax.set_xlabel("Mois")
    ax.set_ylabel("CA (£)")
    ax.tick_params(axis="x", rotation=45)
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "monthly_revenue.png")
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"[INFO] Chart sauvegarde : {path}")


def chart_top_products(top_products_df: DataFrame) -> None:
    """Génère un bar chart horizontal des top 10 produits par CA."""
    _ensure_dir()
    pdf = top_products_df.toPandas().head(10).sort_values("CA_Total")

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(pdf["Description"], pdf["CA_Total"])
    ax.set_title("Top 10 produits par CA")
    ax.set_xlabel("CA (£)")
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "top_products.png")
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"[INFO] Chart sauvegarde : {path}")


def chart_revenue_by_country(country_df: DataFrame) -> None:
    """Génère un bar chart horizontal du CA par pays (top 10)."""
    _ensure_dir()
    pdf = country_df.toPandas().head(10).sort_values("CA_Total")

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(pdf["Country"], pdf["CA_Total"])
    ax.set_title("CA par pays (top 10)")
    ax.set_xlabel("CA (£)")
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "revenue_by_country.png")
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"[INFO] Chart sauvegarde : {path}")


def chart_rfm_segments(rfm_df: DataFrame) -> None:
    """Génère un bar chart de la répartition des segments RFM."""
    _ensure_dir()
    from pyspark.sql.functions import count as spark_count
    summary = (
        rfm_df.groupBy("Segment")
        .agg(spark_count("CustomerID").alias("Nb_Clients"))
        .toPandas()
        .sort_values("Nb_Clients", ascending=False)
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(summary["Segment"], summary["Nb_Clients"])
    ax.set_title("Repartition des segments RFM")
    ax.set_xlabel("Segment")
    ax.set_ylabel("Nombre de clients")
    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "rfm_segments.png")
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"[INFO] Chart sauvegarde : {path}")


def generate_all_charts(
    monthly_df: DataFrame,
    top_products_df: DataFrame,
    country_df: DataFrame,
    rfm_df: DataFrame,
) -> None:
    """Génère tous les graphiques pour la soutenance et les sauvegarde dans OUTPUT_DIR."""
    chart_monthly_revenue(monthly_df)
    chart_top_products(top_products_df)
    chart_revenue_by_country(country_df)
    chart_rfm_segments(rfm_df)
    print(f"[INFO] Tous les charts sauvegardes dans {OUTPUT_DIR}/")
