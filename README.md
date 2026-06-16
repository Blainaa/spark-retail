# Spark Retail — Sales Data Analytics

Projet Big Data — EFREI ING2
Analyse des ventes e-commerce avec Apache Spark, PySpark et Docker.

## Dataset
UCI Online Retail Dataset (~540k transactions)
https://archive.ics.uci.edu/dataset/352/online+retail

## Stack
- Apache Spark 3.5 (PySpark)
- Docker / Docker Compose
- Hadoop HDFS
- Spark Structured Streaming (extension)

## Structure
spark-retail/
+-- docker-compose.yml
+-- app/
¦   +-- main.py         # Point d'entrée
¦   +-- cleaning.py     # Nettoyage des données
¦   +-- analysis.py     # Analyses métier & KPIs
¦   +-- streaming.py    # Extension streaming
+-- hdfs/
¦   +-- setup.sh        # Configuration HDFS
+-- data/               # Dataset local (non versionné)

## Lancer le projet
docker-compose up -d
docker exec spark-master spark-submit /app/main.py
