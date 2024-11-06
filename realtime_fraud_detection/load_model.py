from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, col, when
from pyspark.ml import PipelineModel
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_models(n_models):
    """
    Load pre-trained models from specified paths.

    :param n_models: Number of models to load.
    :return: List of loaded models.
    """
    models = []
    for i in range(n_models):
        model_path = f"boosting/pre_trained_model_{i}"
        try:
            logger.info(f"Loading model from {model_path}")
            model = PipelineModel.load(model_path)
            models.append(model)
            logger.info(f"Model {i} loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load model {i} from {model_path}: {e}")
            # Optionally, you can choose to continue loading other models or raise the error
            continue

    return models

def make_predictions_with_loaded_models(test_df, models):
    """
    Make predictions using the loaded models on the provided test DataFrame.

    :param test_df: Input DataFrame for predictions.
    :param models: List of models to use for predictions.
    :return: DataFrame with final predictions.
    """
    predictions = test_df
    logger.info("Starting predictions with loaded models.")

    for i, model in enumerate(models):
        try:
            logger.info(f"Transforming data with model {i}.")
            predictions = model.transform(predictions) \
                .withColumnRenamed("prediction", f"prediction_{i}") \
                .withColumnRenamed("rawPrediction", f"rawPrediction_{i}") \
                .withColumnRenamed("probability", f"probability_{i}")
            logger.info(f"Model {i} predictions added.")
        except Exception as e:
            logger.error(f"Failed to transform data with model {i}: {e}")
            # Optionally, continue processing other models or raise the error
            continue

    prediction_columns = [f"prediction_{i}" for i in range(len(models))]
    combine_udf = udf(lambda *predictions: float(sum(predictions) / len(predictions)), DoubleType())

    logger.info("Combining predictions.")
    final_predictions = predictions.withColumn("prediction",
                                               combine_udf(*[col(c) for c in prediction_columns]))
    final_predictions = final_predictions.withColumn("prediction",
                                                     when(col("prediction") >= 0.5, 1.0).otherwise(0.0))

    logger.info("Final predictions calculated.")
    return final_predictions
