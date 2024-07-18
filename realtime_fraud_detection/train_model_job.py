import logging
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from common_utils.evaluate_model import compute_metrics

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FraudDetectionJob")


def initialize_spark(app_name="FraudDetection"):
    logger.info("Initializing Spark session.")
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    logger.info("Spark session initialized.")
    return spark


def load_and_preprocess_data(spark, data_path):
    logger.info(f"Loading data from {data_path}")
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    logger.info(f"Data loaded. Schema: {df.printSchema()}")

    cols_to_remove = ["step", "nameOrig", "nameDest", "isFlaggedFraud"]
    df = df.drop(*cols_to_remove)
    logger.info(f"Columns removed: {cols_to_remove}")

    return df


def build_pipeline():
    logger.info("Building machine learning pipeline.")

    string_indexer = StringIndexer(inputCol="type", outputCol="type_index")
    logger.info("StringIndexer created for column 'type'.")

    feature_cols = ["amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "type_index"]
    vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    logger.info(f"VectorAssembler created for columns: {feature_cols}")

    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    logger.info("StandardScaler created to standardize features.")

    gbt = GBTClassifier(labelCol="isFraud", featuresCol="scaled_features", maxDepth=30, maxBins=1000)
    logger.info("Gradient-Boosted Trees classifier created.")

    pipeline = Pipeline(stages=[string_indexer, vector_assembler, scaler, gbt])
    logger.info("Pipeline created with all stages.")

    return pipeline


def train_model(pipeline, train_data):
    logger.info("Training model.")
    model = pipeline.fit(train_data)
    logger.info("Model training complete.")
    return model


def evaluate_model(model, test_data):
    logger.info("Evaluating model on test data.")
    predictions = model.transform(test_data)

    evaluator = BinaryClassificationEvaluator(labelCol="isFraud")
    area_under_curve = evaluator.evaluate(predictions)
    logger.info(f"Evaluation complete. Area Under ROC Curve (AUC): {area_under_curve}")

    auc_roc, accuracy, precision, recall, f1_score = compute_metrics(predictions)
    logger.info("Additional metrics calculated:")
    logger.info(
        f"AUC ROC: {auc_roc}, Accuracy: {accuracy}, Precision: {precision}, Recall: {recall}, F1-score: {f1_score}")


def save_model(model, model_path):
    logger.info(f"Saving model to {model_path}")
    model.save(model_path)
    logger.info("Model saved successfully.")


def main():
    data_path = "/Users/thainguyenvu/Downloads/PS_20174392719_1491204439457_log.csv"
    model_path = "pre_trained_model1"

    logger.info("Starting Fraud Detection job.")

    try:
        spark = initialize_spark()

        df = load_and_preprocess_data(spark, data_path)

        train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
        logger.info("Data split into training and testing sets.")

        pipeline = build_pipeline()

        model = train_model(pipeline, train_data)

        evaluate_model(model, test_data)

        save_model(model, model_path)

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
