# Spark Retail - Sales Data Analytics

Projet Big Data - EFREI ING2  
Analyse des ventes e-commerce avec Apache Spark, PySpark, HDFS et Docker.

## Dataset

UCI Online Retail Dataset (~540k transactions)  
https://archive.ics.uci.edu/dataset/352/online+retail

Le CSV (`online_retail.csv`) utilise `;` comme séparateur et la virgule comme décimal pour les prix.

## Stack

- Apache Spark 3.5 (PySpark)
- Hadoop HDFS 3.2.1 avec **3 DataNodes** (réplication de blocs x3)
- Docker / Docker Compose
- Spark Structured Streaming

## Structure

```
spark-retail/
├── docker-compose.yml       # 1 NameNode + 3 DataNodes + Spark master/worker
├── app/
│   ├── main.py              # Point d'entrée (argparse : --analysis-only / --streaming-only)
│   ├── cleaning.py          # Nettoyage des données
│   ├── analysis.py          # Analyses métier, KPIs, segmentation RFM, exports Parquet
│   ├── streaming.py         # Structured Streaming sur /data/stream/
│   ├── producer.py          # Producteur de chunks CSV → HDFS (démo temps réel)
│   └── visualizations.py   # Génération de graphiques matplotlib (PNG)
├── hdfs/
│   └── setup.sh             # Init HDFS : répertoires batch/stream/output + rapport réplication
├── data/
│   └── online_retail.csv    # Dataset local (non versionné)
```

### Chemins HDFS

| Chemin HDFS                     | Usage                                   |
|----------------------------------|-----------------------------------------|
| `/data/batch/online_retail.csv` | Données complètes pour l'analyse batch  |
| `/data/stream/`                 | Dossier surveillé par le streaming      |
| `/data/output/<analyse>/`       | Résultats exportés en Parquet           |

## Lancer le projet

### 1. Démarrer les conteneurs

```bash
docker-compose up -d
```

Vérifie que les **3 DataNodes** sont actifs dans l'UI NameNode : http://localhost:9870

### 2. Initialiser HDFS

```bash
docker exec -it namenode bash /data/hdfs/setup.sh
```

Ce script :
- Crée les répertoires `/data/batch/`, `/data/stream/`, `/data/output/`
- Upload le dataset dans `/data/batch/online_retail.csv`
- Affiche le rapport de réplication (`hdfs dfsadmin -report`)
- Affiche la localisation des blocs (`hdfs fsck ... -blocks -locations`)

### 3. Lancer l'analyse batch complète

```bash
docker exec spark-master spark-submit /app/main.py
```

Options disponibles :
```bash
# Analyse batch uniquement (sans streaming)
docker exec spark-master spark-submit /app/main.py --analysis-only

# Streaming uniquement
docker exec spark-master spark-submit /app/main.py --streaming-only

# Sans génération de graphiques
docker exec spark-master spark-submit /app/main.py --no-charts
```

## Démo streaming temps réel

Ouvrir **deux terminaux** :

**Terminal 1** — Lancer le streaming Spark :
```bash
docker exec spark-master spark-submit /app/main.py --streaming-only
```

**Terminal 2** — Lancer le producteur de données :
```bash
docker exec spark-master python3 /app/producer.py
```

Le producteur envoie un chunk de ~200 lignes toutes les 10 secondes dans `/data/stream/`.  
Le streaming affiche en console le CA cumulé par pays, mis à jour à chaque micro-batch.

Options du producteur :
```bash
python3 /app/producer.py --chunk-size 300 --interval 5 --duration 180
```

## Résultats exportés

Après l'analyse batch, les fichiers Parquet sont disponibles dans HDFS :

```bash
hdfs dfs -ls /data/output/
# monthly_revenue/
# top_products/
# top_products_by_country/
# rfm_segments/
# revenue_by_country/
```

Les graphiques PNG sont générés dans `/app/output/charts/` (monté sur `./app/output/charts/`) :
- `monthly_revenue.png` — Évolution du CA mensuel
- `top_products.png` — Top 10 produits par CA
- `revenue_by_country.png` — CA par pays top 10
- `rfm_segments.png` — Répartition des segments RFM

## Architecture HDFS — 3 DataNodes

```
NameNode (namenode:9000)
├── DataNode 1 (datanode1)
├── DataNode 2 (datanode2)
└── DataNode 3 (datanode3)

Réplication : 3 copies de chaque bloc
```

Chaque fichier uploadé dans HDFS est automatiquement répliqué sur les 3 DataNodes.  
La commande `hdfs fsck /data/batch/online_retail.csv -blocks -locations` permet de visualiser  
sur quels DataNodes chaque bloc est stocké.
