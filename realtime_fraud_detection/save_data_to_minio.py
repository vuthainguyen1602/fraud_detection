import logging
from pyspark.sql import SparkSession
import boto3
import sys
import json
import os
from datetime import datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG = {
    'spark_app_name': "KafkaElasticsearchIntegration",
    'spark_master': "local[*]",
    'kafka_bootstrap_servers': "localhost:9092",
    'kafka_topic': "financial_transactions",
    'postgres_jdbc_url': "jdbc:postgresql://localhost:5432/postgres",
    'postgres_user': "postgres",
    'postgres_password': "0972131434",
    'postgres_table': "financial_transactions_prediction1",
    'model_path': "pre_trained_model22",
    'spark_jars': "/Users/thainguyenvu/Downloads/postgresql-42.7.2.jar",
    'spark_home': '/Users/thainguyenvu/Downloads/spark-3.5.1-bin-hadoop3-scala2.13',
    'python_executable': sys.executable
}

os.environ['PYSPARK_PYTHON'] = CONFIG['python_executable']
os.environ['PYSPARK_DRIVER_PYTHON'] = CONFIG['python_executable']
os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,'
                                     'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 pyspark-shell')
os.environ['SPARK_HOME'] = CONFIG['spark_home']
os.environ['es.set.netty.runtime.available.processors'] = 'false'
logger.debug("Environment variables set successfully.")


def save_to_minio(partition):
    try:
        minio_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='K99Bf2An11ZMkqg4puLK',
            aws_secret_access_key='Y1pnMLXzHsvRQ9GE278mkBuy6rNSiiJamaxzO964'
        )

        bucket_name = 'financial-transactions-prediction'
        if not minio_client.list_buckets().get('Buckets') or \
                not any(bucket['Name'] == bucket_name for bucket in minio_client.list_buckets()['Buckets']):
            logger.info(f"Creating bucket: {bucket_name}")
            minio_client.create_bucket(Bucket=bucket_name)

        for row in partition:
            key = row.key if row.key else 'default-key'

            now = datetime.now()
            # Convert the timestamp to a string
            timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")
            unique_key = f"{key}_{timestamp_str}"
            data = json.loads(row.value)
            logger.info(f"Saving data to MinIO: {unique_key}")
            minio_client.put_object(
                Bucket=bucket_name,
                Key=unique_key,
                Body=json.dumps(data)
            )
    except Exception as e:
        logger.error(f"Error saving data to MinIO: {e}", exc_info=True)


def main():
    try:
        # Create SparkSession
        spark = SparkSession.builder \
            .appName("KafkaToMinIO") \
            .getOrCreate()

        logger.info("Spark session started")

        # Kafka
        kafka_topic = "financial_transactions"

        # Read stream data from Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", CONFIG['kafka_bootstrap_servers']) \
            .option("subscribe", CONFIG['kafka_topic']) \
            .load()

        logger.info(f"Subscribed to Kafka topic: {kafka_topic}")

        # Convert data to string
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        # Write data to MinIO
        query = df.writeStream \
            .foreachBatch(lambda df, epoch_id: df.foreachPartition(save_to_minio)) \
            .start()

        logger.info("Starting streaming query")

        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
