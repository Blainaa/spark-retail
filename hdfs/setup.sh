#!/bin/bash

echo "Attente du démarrage du namenode..."
sleep 15

echo "Création du répertoire HDFS /data..."
hdfs dfs -mkdir -p /data

echo "Upload du dataset dans HDFS..."
hdfs dfs -put /data/online_retail.csv /data/online_retail.csv

echo "Vérification..."
hdfs dfs -ls /data

echo "Setup HDFS terminé."