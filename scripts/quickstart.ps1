$ErrorActionPreference = 'Stop'
docker compose up -d namenode datanode
if ($LASTEXITCODE -ne 0) { throw 'Docker HDFS failed' }
docker compose exec namenode hdfs dfs -mkdir -p /data
if ($LASTEXITCODE -ne 0) { throw 'HDFS mkdir failed' }
docker compose exec namenode hdfs dfs -chmod -R 777 /data
if ($LASTEXITCODE -ne 0) { throw 'HDFS permissions failed' }
docker compose build spark-job
if ($LASTEXITCODE -ne 0) { throw 'Spark Docker build failed' }
docker compose --profile tools run --rm spark-job --env dev seed-demo
if ($LASTEXITCODE -ne 0) { throw 'Seed failed' }
docker compose --profile tools run --rm spark-job --env dev run --job all --start 2026-10-01 --end 2026-10-07
if ($LASTEXITCODE -ne 0) { throw 'Pipeline failed' }
docker compose --profile tools run --rm spark-job --env dev show-current
if ($LASTEXITCODE -ne 0) { throw 'Catalog inspect failed' }
