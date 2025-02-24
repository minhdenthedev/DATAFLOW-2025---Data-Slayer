import gc

from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.evaluation import RankingEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import SparseVector, DenseVector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, DoubleType

EVALUATE_DF = "test"

spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .appName("Product Recommendation") \
    .master("local[4]") \
    .getOrCreate()

if EVALUATE_DF == "train":
    pred_df = spark.read.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess"
                                 "/train_df_include_prev_month")
else:
    pred_df = spark.read.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess"
                                 "/test_df_include_prev_month")

pred_df = pred_df.drop("new_product_vec")

feat_cols = ["product_vec", "feat_vec", "prev_product_vec"]
feat_vec = "feature"
target_col = "target_product_vec"

vecAssembler = VectorAssembler(inputCols=feat_cols, outputCol=feat_vec)

pred_df = vecAssembler.transform(pred_df)

num_classes = 24


@udf(DoubleType())
def extract_class_1_prob(probability):
    if isinstance(probability, SparseVector):
        probability = probability.toArray()  # Convert SparseVector to a NumPy array
    answer = float(probability[1]) if len(probability) > 1 else 0.0  # Extract class 1 prob
    return answer


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


def inference(df):
    for i in range(num_classes):
        model_path = ("/home/m1nhd3n/Works/DataEngineer"
                      "/product_recommendations/models"
                      f"/spark/bests/gbt_{i}")
        model = GBTClassificationModel.load(model_path)
        df = model.transform(df)
        df = df.drop("rawPrediction", "prediction")
        df = df.drop("raw_prediction")
        df = df.withColumn("probability", extract_class_1_prob(col("probability")))
        df = df.withColumnRenamed("probability", f"prob_{i}")
        del model
        gc.collect()
    predAssembler = VectorAssembler(inputCols=[f"prob_{i}" for i in range(num_classes)], outputCol="prediction_vec")
    df = predAssembler.transform(df)
    df = df.drop(*[f"prob_{i}" for i in range(num_classes)])
    return df


def score(df):
    df = df.withColumn("truth_idx", binary_to_indices(col(target_col)))
    df = df.withColumn("pred_idx", ranked_indices(col("prediction_vec")))

    df = df.withColumn("truth_idx", cast_to_double_array(col("truth_idx")))
    df = df.withColumn("pred_idx", cast_to_double_array(col("pred_idx")))

    df = df.select("truth_idx", "pred_idx")

    evaluator = RankingEvaluator(predictionCol="pred_idx", labelCol="truth_idx", k=3)

    return evaluator.evaluate(df)


pred_df = inference(pred_df)
result = score(pred_df)
print(f"MAP {EVALUATE_DF} at k=3: {result}")

