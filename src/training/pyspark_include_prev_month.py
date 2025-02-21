from pyspark.sql import SparkSession, Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, log, row_number
from pyspark.sql import functions as F
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, LogisticRegression
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql.functions import expr

MODEL_TYPE = "lr"

spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .appName("Product Recommendation") \
    .getOrCreate()

test_df = spark.read.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess"
                             "/test_df_include_prev_month")
train_df = spark.read.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess"
                              "/train_df_include_prev_month")

train_df = train_df.drop("new_product_vec")
test_df = test_df.drop("new_product_vec")

feat_cols = ["product_vec", "feat_vec", "prev_product_vec"]
feat_vec = "feature"
target_col = "target_product_vec"

vecAssembler = VectorAssembler(inputCols=feat_cols, outputCol=feat_vec)

train_df = vecAssembler.transform(train_df)

num_classes = 24


def extract_label(index):
    return udf(lambda v: int(v[index]), IntegerType())


# Apply UDF to create separate binary columns
for i in range(num_classes):
    train_df = train_df.withColumn(f"label_{i}", extract_label(i)(col(target_col)))

train_df.show()

models = {}
for i in range(num_classes):
    match MODEL_TYPE:
        case "gbt":
            rf = GBTClassifier(featuresCol=feat_vec, labelCol=f"label_{i}")
        case "rf":
            rf = RandomForestClassifier(featuresCol=feat_vec, labelCol=f"label_{i}", numTrees=10)
        case "lr":
            rf = LogisticRegression(featuresCol=feat_vec, labelCol=f"label_{i}")
        case _:
            rf = GBTClassifier(featuresCol=feat_vec, labelCol=f"label_{i}")

    models[f"model_{i}"] = rf.fit(train_df)
    models[f"model_{i}"].save(f"/home/m1nhd3n/Works/DataEngineer/product_recommendations/models/spark"
                              f"/include_prev_month/model_{i}_{MODEL_TYPE}")

