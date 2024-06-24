from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col


def compute_metrics(predictions):
    # Let's start by evaluating the model with BinaryClassificationEvaluator
    evaluator = BinaryClassificationEvaluator(labelCol="isFraud")

    # First, let's check out the AUC-ROC
    auc_roc = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})

    # Extract values from the confusion matrix DataFrame

    TP = predictions.filter((col('isFraud') == 1) & (col('prediction') == 1)).count()
    TN = predictions.filter((col('isFraud') == 0) & (col('prediction') == 0)).count()
    FP = predictions.filter((col('isFraud') == 0) & (col('prediction') == 1)).count()
    FN = predictions.filter((col('isFraud') == 1) & (col('prediction') == 0)).count()

    # Compute accuracy
    accuracy = (TP + TN) / (TP + TN + FP + FN)

    # Compute precision
    precision = TP / (TP + FP)

    # Compute recall
    recall = TP / (TP + FN)

    # Compute F1-score
    f1_score = 2 * (precision * recall) / (precision + recall)

    return auc_roc, accuracy, precision, recall, f1_score
