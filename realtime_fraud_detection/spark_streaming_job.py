import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
import os
import sys

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
    os.environ['PYSPARK_PYTHON'] = CONFIG['python_executable']
    os.environ['PYSPARK_DRIVER_PYTHON'] = CONFIG['python_executable']
    os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,'
                                         'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 pyspark-shell')
    os.environ['SPARK_HOME'] = CONFIG['spark_home']
    os.environ['es.set.netty.runtime.available.processors'] = 'false'


def initialize_spark_session():
    findspark.init(CONFIG['spark_home'])
    spark = SparkSession.builder \
        .appName(CONFIG['spark_app_name']) \
        .master(CONFIG['spark_master']) \
        .config("spark.jars", CONFIG['spark_jars']) \
        .getOrCreate()
    return spark


def read_stream_from_kafka(spark):
    streaming_data = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG['kafka_bootstrap_servers']) \
        .option("subscribe", CONFIG['kafka_topic']) \
        .load()
    return streaming_data


def parse_json_data(streaming_data, schema):
    json_data = streaming_data.selectExpr("CAST(value AS STRING)")
    parsed_data = json_data.select(from_json(json_data.value, schema).alias("data")).select("data.*")
    return parsed_data


def load_and_apply_model(parsed_data):
    model = PipelineModel.load(CONFIG['model_path'])
    transformed_data = model.transform(parsed_data)
    return transformed_data


def prepare_output_data(transformed_data):
    def vector_to_string(vector):
        return str(vector)

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
    return selected_columns_df


def write_to_postgres(batch_df, batch_id):
    jdbc_properties = {
        "user": CONFIG['postgres_user'],
        "password": CONFIG['postgres_password'],
        "driver": "org.postgresql.Driver"
    }
    print(f"Writing batch {batch_id} to PostgreSQL")
    batch_df.write.jdbc(url=CONFIG['postgres_jdbc_url'], table=CONFIG['postgres_table'], mode="append",
                        properties=jdbc_properties)


def main():
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

    query.awaitTermination()


if __name__ == "__main__":
    main()
