
import findspark
from pyspark.sql.functions import from_json, udf, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

import os
import sys
from pyspark.sql import SparkSession

# Set environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,'
                                     'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 pyspark-shell')
os.environ['SPARK_HOME'] = '/Users/thainguyenvu/Downloads/spark-3.5.1-bin-hadoop3-scala2.13'
os.environ['es.set.netty.runtime.available.processors'] = 'false'

# Set the path to your Java cacerts file

findspark.init('/Users/thainguyenvu/Downloads/spark-3.5.1-bin-hadoop3-scala2.13')

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaElasticsearchIntegration") \
    .master("local[*]") \
    .config("spark.jars", "/Users/thainguyenvu/Downloads/postgresql-42.7.2.jar") \
    .getOrCreate()


sc = spark.sparkContext
print("Updated Scala Version:", sc._jvm.scala.util.Properties.versionString())

# Schema for the incoming JSON data
schema = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
])

# Read data from Kafka
streaming_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial_transactions") \
    .load()

# Convert the value column from Kafka into a string
json_data = streaming_data.selectExpr("CAST(value AS STRING)")

# Parse the JSON data
parsed_data = json_data.select(from_json(json_data.value, schema).alias("data")).select("data.*")

from pyspark.ml import PipelineModel

model = PipelineModel.load("pre_trained_model11")
real_time_predictions = model.transform(parsed_data)

# Apply the model transformation
transformed_data = model.transform(parsed_data)

# Print the schema of the transformed data to check available columns
transformed_data.printSchema()

# Function to convert probability vector to string
def vector_to_string(vector):
    return str(vector)

# UDF to convert vector to string
vector_to_string_udf = udf(vector_to_string, StringType())

# Select relevant columns, including the "probability" column if it exists
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

# Add "probability" column if it exists in the transformed data
if "probability" in transformed_data.columns:
    transformed_data = transformed_data.withColumn("probability_str", vector_to_string_udf(col("probability")))
    selected_columns.append("probability_str")

selected_columns_df = transformed_data.select(*selected_columns)


# PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
jdbc_properties = {
    "user": "postgres",
    "password": "0972131434",
    "driver": "org.postgresql.Driver"
}

# Function to write each micro-batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    print(f"Writing batch {batch_id} to PostgreSQL")
    batch_df.printSchema()
    batch_df.write \
        .jdbc(url=jdbc_url, table="financial_transactions_prediction1", mode="append", properties=jdbc_properties)

# Write the selected data to PostgreSQL using foreachBatch
query = selected_columns_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
