import gc

from pyspark.ml.classification import RandomForestClassificationModel, GBTClassificationModel, LogisticRegressionModel
from pyspark.ml.evaluation import RankingEvaluator, BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import SparseVector, DenseVector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, DoubleType
from xgboost.spark import SparkXGBClassifierModel

MODEL_NAME = "gbt"
EVALUATE_DF = "train"

spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .appName("Product Recommendation") \
    .master("local[4]") \
    .getOrCreate()

if EVALUATE_DF == "train":
    df = spark.read.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess"
                            "/train_df_include_prev_month")
else:
    df = spark.read.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess"
                            "/test_df_include_prev_month")

df = df.drop("new_product_vec")

feat_cols = ["product_vec", "feat_vec", "prev_product_vec"]
feat_vec = "feature"
target_col = "target_product_vec"


def extract_label(index):
    return udf(lambda v: int(v[index]), IntegerType())


vecAssembler = VectorAssembler(inputCols=feat_cols, outputCol=feat_vec)

df = vecAssembler.transform(df)

num_classes = 24
for i in range(num_classes):
    df = df.withColumn(f"label_{i}", extract_label(i)(col(target_col)))

for i in range(num_classes):
    model_path = ("/home/m1nhd3n/Works/DataEngineer"
                  "/product_recommendations/models"
                  f"/spark/include_prev_month/model_{i}_{MODEL_NAME}")
    model = GBTClassificationModel.load(model_path)
    pred_df = model.transform(df)
    evaluator = BinaryClassificationEvaluator(labelCol=f"label_{i}", metricName="areaUnderPR")
    result = evaluator.evaluate(pred_df)
    print(f"AP for model {i}: {result}")
