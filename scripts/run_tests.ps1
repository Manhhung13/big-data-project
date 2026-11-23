# Runs every pytest suite inside the Spark 3.5.9 image (repo mounted read-only; tests write only to /tmp).
# The host may have another PySpark version (e.g. 4.x), so Spark tests are never run with the host Python.
param([string]$Image = 'stock-batch-layer-spark-job:latest', [string[]]$Tests = @('tests'))
$Repo = (Split-Path -Parent $PSScriptRoot) -replace '\\', '/'
$PyPath = '/opt/project/src:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip'
docker run --rm --user 185 -e HOME=/tmp -e SPARK_LOCAL_IP=127.0.0.1 -e PYTHONDONTWRITEBYTECODE=1 -e "PYTHONPATH=$PyPath" -v "${Repo}:/opt/project:ro" -w /opt/project --entrypoint python3 $Image -m pytest -q -p no:cacheprovider $Tests --tb=short
if ($LASTEXITCODE -ne 0) { throw 'tests failed' }
