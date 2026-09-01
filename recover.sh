#!/usr/bin/env bash
set -euo pipefail

echo "1) Parando e removendo containers orfãos..."
docker compose down --remove-orphans

echo "2) Garantindo jars S3A..."
mkdir -p jars
[ -f jars/hadoop-aws-3.3.4.jar ] || curl -sSL -o jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
[ -f jars/aws-java-sdk-bundle-1.12.262.jar ] || curl -sSL -o jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

echo "3) Build das imagens Spark (no-cache)..."
docker compose build --no-cache spark-master spark-worker-1 spark-worker-2

echo "4) Subindo serviços..."
docker compose up -d

echo "5) Aguardando 6s e mostrando logs do master..."
sleep 6
docker logs docker-spark-master-1 --tail 200 || true

echo "6) Status dos containers Spark:"
docker ps --filter "name=docker-spark" --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"

