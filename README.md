# Spark Retail � Sales Data Analytics

Projet Big Data � EFREI ING2
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
```bash
spark-retail/
+-- docker-compose.yml
+-- app/
�   +-- main.py         # Point d'entr�e
�   +-- cleaning.py     # Nettoyage des donn�es
�   +-- analysis.py     # Analyses m�tier & KPIs
�   +-- streaming.py    # Extension streaming
+-- hdfs/
�   +-- setup.sh        # Configuration HDFS
+-- data/               # Dataset local (non versionn�)
```

## Lancer le projet
```bash
docker-compose up -d
docker exec spark-master spark-submit /app/main.py
```