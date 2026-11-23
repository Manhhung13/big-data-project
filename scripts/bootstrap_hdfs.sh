#!/usr/bin/env sh
set -eu
hdfs dfs -mkdir -p /data/_system /data/bronze /data/silver /data/gold
# ONLY for disposable local Docker pilot. Never chmod 777 in production.
hdfs dfs -chmod -R 777 /data
hdfs dfs -ls /data
