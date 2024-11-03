import logging
import json
import time
import random
import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("KafkaProducerScript")

def on_send_success(record_metadata):
    """Callback for successful message sending."""
    logger.info(f"Message sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")

def on_send_error(excp):
    """Callback for message sending errors."""
    logger.error('Error sending message:', exc_info=excp)

def check_topic_exists(bootstrap_servers, topic_name):
    """Check if the specified topic exists."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics = admin_client.list_topics()
        return topic_name in topics
    except Exception as e:
        logger.error(f"Error checking topic: {e}")
        return False
    finally:
        admin_client.close()

def create_producer(bootstrap_servers):
    """Create and return a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        api_version=(0, 10),
        retries=5,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        security_protocol='PLAINTEXT'
    )

def read_data(file_path, sample_size, total_records=6362620):
    """Read and sample data from CSV file."""
    try:
        df = pd.read_csv(file_path, skiprows=lambda i: i > 0 and random.random() > sample_size / total_records)
        df['transaction_id'] = range(1, len(df) + 1)
        logger.info(f"Data read successfully, total records: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise

def produce_messages(producer, topic, dataframe):
    """Function to produce messages to a Kafka topic from a DataFrame."""
    logger.info(f"Starting to produce messages to topic '{topic}'")
    messages_sent = 0
    messages_failed = 0

    try:
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

            try:
                future = producer.send(topic, message)
                future.add_callback(on_send_success).add_errback(on_send_error)
                producer.flush()
                messages_sent += 1

                if messages_sent % 100 == 0:  # Log every 100 messages
                    logger.info(f"Progress: {messages_sent} messages sent successfully")

                time.sleep(1)  # Delay between messages

            except Exception as e:
                messages_failed += 1
                logger.error(f"Error sending message {message['transaction_id']}: {e}")

    except Exception as e:
        logger.error(f"Error in message production loop: {e}")
    finally:
        logger.info(f"Message production completed. Sent: {messages_sent}, Failed: {messages_failed}")

    return messages_sent, messages_failed

def main():
    """Main function to run the Kafka producer."""
    # Configuration
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'financial_transactions')
    csv_file_path = os.getenv('CSV_FILE_PATH', '/Users/thainguyenvu/Downloads/PS_20174392719_1491204439457_log.csv')
    sample_size = int(os.getenv('SAMPLE_SIZE', 6000))

    logger.info("Starting Kafka producer script")
    logger.info(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")
    logger.info(f"Kafka topic: {kafka_topic}")
    logger.info(f"CSV file path: {csv_file_path}")
    logger.info(f"Sample size: {sample_size}")

    producer = None
    try:
        # Check if topic exists
        if not check_topic_exists(kafka_bootstrap_servers, kafka_topic):
            logger.error(f"Topic {kafka_topic} does not exist")
            return

        # Create Kafka producer
        producer = create_producer(kafka_bootstrap_servers)

        # Test connection
        producer.bootstrap_connected()
        logger.info("Successfully connected to Kafka")

        # Read data
        df = read_data(csv_file_path, sample_size)

        # Produce messages
        messages_sent, messages_failed = produce_messages(producer, kafka_topic, df)

        logger.info(f"Producer completed. Total messages sent: {messages_sent}, failed: {messages_failed}")

    except Exception as e:
        logger.error(f"An error occurred in main process: {e}", exc_info=True)
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    main()