#!/usr/bin/env bash
set -euo pipefail

mkdir -p jars
cd jars

# Versões compatíveis com Spark 3.5.1 / Hadoop 3.x
HADOOP_AWS_VERSION="3.3.4"
AWS_SDK_VERSION="1.12.262"

echo "Baixando hadoop-aws ${HADOOP_AWS_VERSION}..."
curl -sSL -O "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"

echo "Baixando aws-java-sdk-bundle ${AWS_SDK_VERSION}..."
curl -sSL -O "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

echo "JARs baixados em $(pwd)"

