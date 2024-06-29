# Real-Time Fraud Detection Project

## Overview

This project focuses on real-time fraud detection in financial transactions using Apache Spark, PySpark's MLlib, Kafka, Postgres and Superset. The system processes streaming data, trains a Gradients Boosting Trees model, then using this model to predict fraud then stores data in the Postgres database. The data will be visualization on Apache Superset dashboard.

## Table of Contents

- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Architecture & Workflow](#architecture--workflow)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Usage](#usage)
- [Contributing](#contributing)

## Key Features

1. **Gradient Boosting Trees Model:**
   - Trained a model using PySpark's MLlib for accurate fraud detection.

2. **Real-Time Data Flow:**
   - Simulated real-time data with Kafka.
   - Spark Streaming for real-time predictions.

3. **Postgres database Integration:**
   - Stored data and predictions in Postgres database.
   - 
4. **Apache Superset dasboard:**
   - Retrieves and visualizes fraud detection insights from Postgres database.

## Tech Stack
- Apache Spark
- PySpark's MLlib
- Apache Kafka
- Postgres database
- Apache Superset
## Architecture & Workflow

<img width="628" alt="Screenshot 2024-06-28 at 11 54 12" src="https://github.com/vuthainguyen1602/fraud_detection/assets/39717457/a26b32ae-6729-4cf8-ae37-aeb49945f1e8">


## Project Structure
```
├── offline_fraud_detetction/ 
│ ├── Origional_Fraud_Detection.ipynb/ # Traning model with original data
│ ├── Random_Undersampling_Fraud_Detection_1_1.ipynb/ # Training model with undersampling 1/1 ratio 
│ └── Random_Undersampling_Fraud_Detection_1_10.ipynb # Training model with undersampling 1/10 ratio
├── realtime_fraud_detection/
│ ├── kafka_producer_job.py/ # Ingest data to kafka server 
│ ├── spark_streaming_job.py/ # Read streaming data from kafka then save data to Postgres database
│ └── train_model_job.py.ipynb # Pretrained model 
├── README.md # Project documentation
```


## Setup

0. **Download Data**
   - Link: [Dataset](https://www.kaggle.com/datasets/ealaxi/paysim1).

1. **Environment Setup:**
   - Install dependencies: `pip install -r requirements.txt`.

2. **Project Configuration:**
   - Set up Postgres database.
   - Set up Superset.
   - Configure connect superset to Postgres database.

3. **Model Training:**
   - Run `train_model_job.py` to train the Gradient Boosting Trees model.


## Usage

1. **Start Kafka Producer:**
   ```bash
   python kafka_producer_job.py

2. **Run Spark Streaming Job:**
   ```bash
   spark-submit --packages --packages org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 pyspark-shell spark_streaming_job.py

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.
