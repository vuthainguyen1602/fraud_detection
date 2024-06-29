import findspark
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
import os
import sys

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configuration for the Spark job
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


def set_environment_variables():
    logger.debug("Setting environment variables.")
    os.environ['PYSPARK_PYTHON'] = CONFIG['python_executable']
    os.environ['PYSPARK_DRIVER_PYTHON'] = CONFIG['python_executable']
    os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,'
                                         'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 pyspark-shell')
    os.environ['SPARK_HOME'] = CONFIG['spark_home']
    os.environ['es.set.netty.runtime.available.processors'] = 'false'
    logger.debug("Environment variables set successfully.")


def initialize_spark_session():
    logger.debug("Initializing Spark session.")
    findspark.init(CONFIG['spark_home'])
    spark = SparkSession.builder \
        .appName(CONFIG['spark_app_name']) \
        .master(CONFIG['spark_master']) \
        .config("spark.jars", CONFIG['spark_jars']) \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()
    logger.info("Spark session initialized with app name: %s", CONFIG['spark_app_name'])
    return spark


def read_stream_from_kafka(spark):
    logger.debug("Reading stream from Kafka topic: %s", CONFIG['kafka_topic'])
    streaming_data = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG['kafka_bootstrap_servers']) \
        .option("subscribe", CONFIG['kafka_topic']) \
        .load()
    logger.info("Stream read successfully from Kafka.")
    return streaming_data


def parse_json_data(streaming_data, schema):
    logger.debug("Parsing JSON data from Kafka stream.")
    json_data = streaming_data.selectExpr("CAST(value AS STRING)")
    parsed_data = json_data.select(from_json(json_data.value, schema).alias("data")).select("data.*")
    logger.info("JSON data parsed successfully.")
    return parsed_data


def load_and_apply_model(parsed_data):
    logger.debug("Loading and applying the pre-trained model.")
    try:
        model = PipelineModel.load(CONFIG['model_path'])
        logger.info("Model loaded successfully from path: %s", CONFIG['model_path'])
    except Exception as e:
        logger.error("Failed to load model: %s", e)
        raise

    try:
        transformed_data = model.transform(parsed_data)
        logger.info("Model applied successfully to the data.")
    except Exception as e:
        logger.error("Failed to apply model: %s", e)
        raise
    return transformed_data


def prepare_output_data(transformed_data):
    logger.debug("Preparing output data for writing to PostgreSQL.")

    def vector_to_string(vector):
        try:
            return str(vector)
        except Exception as e:
            logger.error("Error in vector_to_string UDF: %s", e)
            return None

    vector_to_string_udf = udf(vector_to_string, StringType())

    selected_columns = [
        "transaction_id",
        "type",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "prediction"
    ]

    if "probability" in transformed_data.columns:
        transformed_data = transformed_data.withColumn("probability_str", vector_to_string_udf(col("probability")))
        selected_columns.append("probability_str")

    selected_columns_df = transformed_data.select(*selected_columns)
    logger.info("Output data prepared successfully.")
    return selected_columns_df


def write_to_postgres(batch_df, batch_id):
    logger.debug("Writing batch %s to PostgreSQL.", batch_id)
    jdbc_properties = {
        "user": CONFIG['postgres_user'],
        "password": CONFIG['postgres_password'],
        "driver": "org.postgresql.Driver"
    }
    try:
        batch_df.write.jdbc(url=CONFIG['postgres_jdbc_url'], table=CONFIG['postgres_table'], mode="append",
                            properties=jdbc_properties)
        logger.info("Batch %s written to PostgreSQL successfully.", batch_id)
    except Exception as e:
        logger.error("Failed to write batch %s to PostgreSQL: %s", batch_id, e)


def main():
    logger.debug("Starting main process.")
    set_environment_variables()

    spark = initialize_spark_session()

    schema = StructType([
        StructField("transaction_id", IntegerType()),
        StructField("type", StringType()),
        StructField("amount", DoubleType()),
        StructField("oldbalanceOrg", DoubleType()),
        StructField("newbalanceOrig", DoubleType()),
        StructField("oldbalanceDest", DoubleType()),
        StructField("newbalanceDest", DoubleType()),
    ])

    streaming_data = read_stream_from_kafka(spark)

    parsed_data = parse_json_data(streaming_data, schema)

    transformed_data = load_and_apply_model(parsed_data)

    selected_columns_df = prepare_output_data(transformed_data)

    query = selected_columns_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .start()

    logger.info("Streaming query started successfully.")
    query.awaitTermination()
    logger.debug("Main process terminated.")


if __name__ == "__main__":
    main()