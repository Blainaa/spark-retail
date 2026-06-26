#!/usr/bin/env python3
"""Simule l'arrivee de nouvelles transactions en ecrivant des chunks CSV dans HDFS /data/stream/."""

import argparse
import os
import random
import shutil
import subprocess
import sys
import time


def _hdfs_cmd() -> list:
    """Retourne la commande hdfs disponible dans l'environnement."""
    if shutil.which("hdfs"):
        return ["hdfs", "dfs"]
    hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
    candidate = os.path.join(hadoop_home, "bin", "hdfs")
    if os.path.exists(candidate):
        return [candidate, "dfs"]
    spark_home = os.environ.get("SPARK_HOME", "/opt/spark")
    for sub in ["hadoop", "../hadoop"]:
        candidate = os.path.join(spark_home, sub, "bin", "hdfs")
        if os.path.exists(candidate):
            return [candidate, "dfs"]
    print("[WARN] Commande hdfs introuvable — verifiez HADOOP_HOME ou PATH.", file=sys.stderr)
    return ["hdfs", "dfs"]


def load_data(local_path: str) -> tuple:
    """Charge le CSV source et retourne (header, lignes de donnees)."""
    with open(local_path, "r", encoding="latin-1") as f:
        lines = f.readlines()
    return lines[0], lines[1:]


def push_chunk(
    data_lines: list,
    header: str,
    chunk_size: int,
    chunk_num: int,
    hdfs_cmd: list,
) -> None:
    """Ecrit un chunk aleatoire dans /data/stream/ sur HDFS."""
    sample = random.choices(data_lines, k=min(chunk_size, len(data_lines)))
    content = (header + "".join(sample)).encode("latin-1")

    tmp_file = f"/tmp/stream_chunk_{chunk_num:04d}.csv"
    with open(tmp_file, "wb") as f:
        f.write(content)

    hdfs_path = f"/data/stream/chunk_{chunk_num:04d}.csv"
    subprocess.run(hdfs_cmd + ["-put", "-f", tmp_file, hdfs_path], check=True)
    os.remove(tmp_file)
    print(f"[INFO] Chunk {chunk_num:04d} envoye ({len(sample)} lignes) → {hdfs_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Producteur de donnees pour la demo streaming Spark"
    )
    parser.add_argument(
        "--input", default="/data/online_retail.csv",
        help="Chemin local du CSV source (defaut : /data/online_retail.csv)"
    )
    parser.add_argument(
        "--chunk-size", type=int, default=200,
        help="Nombre de lignes par chunk (defaut : 200)"
    )
    parser.add_argument(
        "--interval", type=int, default=10,
        help="Intervalle entre deux chunks en secondes (defaut : 10)"
    )
    parser.add_argument(
        "--duration", type=int, default=120,
        help="Duree totale de production en secondes (defaut : 120)"
    )
    args = parser.parse_args()

    print("=" * 50)
    print("  PRODUCER - SIMULATION STREAMING TEMPS REEL")
    print("=" * 50)
    print(f"[INFO] Chargement des donnees depuis {args.input}...")

    header, data_lines = load_data(args.input)
    hdfs_cmd = _hdfs_cmd()

    print(f"[INFO] {len(data_lines)} lignes disponibles.")
    print(
        f"[INFO] Envoi de {args.chunk_size} lignes "
        f"toutes les {args.interval}s pendant {args.duration}s..."
    )

    chunk_num = 0
    elapsed = 0

    while elapsed < args.duration:
        push_chunk(data_lines, header, args.chunk_size, chunk_num, hdfs_cmd)
        chunk_num += 1
        time.sleep(args.interval)
        elapsed += args.interval

    print(f"[INFO] Production terminee. {chunk_num} chunks envoyes.")


if __name__ == "__main__":
    main()
