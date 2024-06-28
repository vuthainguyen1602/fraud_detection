from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession

from common_utils.evaluate_model import compute_metrics

# Create a Spark session
spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Load the dataset
df = spark.read.csv("/Users/thainguyenvu/Downloads/PS_20174392719_1491204439457_log.csv", header=True, inferSchema=True)

# Remove unnecessary columns
cols_to_remove = ["step", "nameOrig", "nameDest", "isFlaggedFraud"]
df = df.drop(*cols_to_remove)

# Convert categorical variables to numerical representations
string_indexer = StringIndexer(inputCol="type", outputCol="type_index")

# Vectorize features
feature_cols = ["amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "type_index"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
# df_oversampled = vector_assembler.transform(df_oversampled)

# Scale features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
# scaler_model = scaler.fit(df_oversampled)
# df_oversampled_scaled = scaler_model.transform(df_oversampled)

# Split the data into training and testing sets
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# Train the model (Logistic Regression)
gbt = GBTClassifier(labelCol="isFraud", featuresCol="scaled_features", maxDepth=30, maxBins=1000)
pipeline = Pipeline(stages=[string_indexer, vector_assembler, scaler, gbt])
model = pipeline.fit(train_data)

# Make predictions on the test set
predictions = model.transform(test_data)

# Evaluate the model
evaluator = BinaryClassificationEvaluator(labelCol="isFraud")
area_under_curve = evaluator.evaluate(predictions)
print(f"Area Under ROC Curve (AUC): {area_under_curve}")

auc_roc, accuracy, precision, recall, f1_score = compute_metrics(predictions)

# Print the value evaluation
print("AUC ROC:", auc_roc)
print("Accuracy:", accuracy)
print("Precision:", precision)
print("Recall:", recall)
print("F1-score:", f1_score)
# Save the trained model
model_path = "pre_trained_model22"
model.save(model_path)

# Cleanup
spark.stop()