import logging
import json
import time
import random
import pandas as pd
from kafka3 import KafkaProducer
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("KafkaProducerScript")


def produce_messages(producer, topic, dataframe):
    """Function to produce messages to a Kafka topic from a DataFrame."""
    logger.info(f"Starting to produce messages to topic '{topic}'")

    time.sleep(5)  # Initial delay
    for index, row in dataframe.iterrows():
        message = {
            'transaction_id': int(row['transaction_id']),
            'type': row['type'],
            'amount': float(row['amount']),
            'oldbalanceOrg': float(row['oldbalanceOrg']),
            'newbalanceOrig': float(row['newbalanceOrig']),
            'oldbalanceDest': float(row['oldbalanceDest']),
            'newbalanceDest': float(row['newbalanceDest']),
        }
        logger.debug(f"Sending message: {message}")
        producer.send(topic, json.dumps(message).encode('utf-8'))
        time.sleep(5)  # Delay between messages

    logger.info("Finished producing messages")


if __name__ == "__main__":
    # Configuration
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'financial_transactions')
    csv_file_path = os.getenv('CSV_FILE_PATH', '/Users/thainguyenvu/Downloads/PS_20174392719_1491204439457_log.csv')
    sample_size = int(os.getenv('SAMPLE_SIZE', 6000))

    logger.info("Starting Kafka producer script")
    logger.info(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")
    logger.info(f"Kafka topic: {kafka_topic}")
    logger.info(f"CSV file path: {csv_file_path}")
    logger.info(f"Sample size: {sample_size}")

    try:
        # Kafka producer instance
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
        logger.info("Kafka producer created")

        # Read data from CSV file
        logger.info("Reading data from CSV file")
        df = pd.read_csv(csv_file_path, skiprows=lambda i: i > 0 and random.random() > sample_size / 6362620)
        df['transaction_id'] = range(1, len(df) + 1)
        logger.info(f"Data read successfully, total records: {len(df)}")

        # Produce messages to Kafka
        produce_messages(producer, kafka_topic, df)

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        producer.close()
        logger.info("Kafka producer closed")
