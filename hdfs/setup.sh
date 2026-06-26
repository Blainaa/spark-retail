#!/bin/bash

echo "Attente du demarrage du namenode..."
sleep 20

echo "Creation des repertoires HDFS..."
hdfs dfs -mkdir -p /data/batch
hdfs dfs -mkdir -p /data/stream
hdfs dfs -mkdir -p /data/output

echo "Upload du dataset dans HDFS (batch)..."
hdfs dfs -put -f /data/online_retail.csv /data/batch/online_retail.csv

echo "Verification..."
hdfs dfs -ls /data
hdfs dfs -ls /data/batch

echo ""
echo "=== Rapport de replication HDFS ==="
hdfs dfsadmin -report

echo ""
echo "=== Localisation des blocs du dataset ==="
hdfs fsck /data/batch/online_retail.csv -files -blocks -locations

echo "Setup HDFS termine."
