from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "Receita_Spark") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "2g")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )