from pyspark.sql import SparkSession, Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, log, row_number, udf
from pyspark.sql import functions as F
from pyspark.ml.classification import RandomForestClassificationModel, GBTClassificationModel, LogisticRegressionModel
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, DoubleType
from pyspark.sql.functions import expr
from pyspark.ml.linalg import SparseVector, DenseVector
from pyspark.ml.evaluation import RankingEvaluator

MODEL_NAME = "lr"
EVALUATE_DF = "test"

spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .appName("Product Recommendation") \
    .getOrCreate()

if EVALUATE_DF == "train":
    df = spark.read.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess"
                            "/train_df_include_prev_month")
else:
    df = spark.read.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess"
                            "/test_df_include_prev_month")

df = df.drop("new_product_vec")

feat_cols = ["product_vec", "feat_vec"]
feat_vec = "feature"
target_col = "target_product_vec"

vecAssembler = VectorAssembler(inputCols=feat_cols, outputCol=feat_vec)

df = vecAssembler.transform(df)

num_classes = 24

models = {}
pred_df = df


@udf(DoubleType())
def extract_class_1_prob(probability):
    if isinstance(probability, SparseVector):
        probability = probability.toArray()
    answer = float(probability[1]) if len(probability) > 1 else 0.0
    return answer


for i in range(num_classes):
    model_path = ("/home/m1nhd3n/Works/DataEngineer"
                  "/product_recommendations/models"
                  f"/spark/only_curr_month/model_{i}_{MODEL_NAME}")
    match MODEL_NAME:
        case "rf":
            models[f"model_{i}"] = RandomForestClassificationModel.load(model_path)
        case "gbt":
            models[f"model_{i}"] = GBTClassificationModel.load(model_path)
        case "lr":
            models[f"model_{i}"] = LogisticRegressionModel.load(model_path)
    pred_df = models[f"model_{i}"].transform(pred_df)
    pred_df = pred_df.drop("rawPrediction", "prediction")
    pred_df = pred_df.withColumn("probability", extract_class_1_prob(col("probability")))
    pred_df = pred_df.withColumnRenamed("probability", f"prob_{i}")

predAssembler = VectorAssembler(inputCols=[f"prob_{i}" for i in range(num_classes)], outputCol="prediction_vec")
pred_df = predAssembler.transform(pred_df)


@udf(returnType=ArrayType(IntegerType()))
def binary_to_indices(binary_labels):
    if isinstance(binary_labels, SparseVector):
        return list(binary_labels.indices)
    else:
        return [j for j, v in enumerate(binary_labels) if v == 1]


@udf(returnType=ArrayType(IntegerType()))
def ranked_indices(pred_scores):
    if isinstance(pred_scores, DenseVector):
        pred_list = pred_scores.toArray().tolist()
        return [idx for idx, _ in sorted(enumerate(pred_list), key=lambda x: x[1], reverse=True)]
    else:
        return []


@udf(ArrayType(DoubleType()))
def cast_to_double_array(arr):
    return [float(x) for x in arr] if arr is not None else []


pred_df = pred_df.withColumn("truth_idx", binary_to_indices(col(target_col)))
pred_df = pred_df.withColumn("pred_idx", ranked_indices(col("prediction_vec")))

pred_df = pred_df.withColumn("truth_idx", cast_to_double_array(col("truth_idx")))
pred_df = pred_df.withColumn("pred_idx", cast_to_double_array(col("pred_idx")))

pred_df.show()

evaluator = RankingEvaluator(predictionCol="pred_idx", labelCol="truth_idx", k=3)

result = evaluator.evaluate(pred_df)
print(result)
