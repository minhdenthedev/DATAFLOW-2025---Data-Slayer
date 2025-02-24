from pyspark.ml.classification import RandomForestClassificationModel, GBTClassificationModel, LogisticRegressionModel, \
    GBTClassifier
from pyspark.ml.evaluation import RankingEvaluator, BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import SparseVector, DenseVector
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
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

train_df = df.drop("new_product_vec")

feat_cols = ["product_vec", "feat_vec", "prev_product_vec"]
feat_vec = "feature"
target_col = "target_product_vec"

vecAssembler = VectorAssembler(inputCols=feat_cols, outputCol=feat_vec)

train_df = vecAssembler.transform(train_df)

num_classes = 24

ft_list = [3, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 19]


def extract_label(index):
    return udf(lambda v: int(v[index]), IntegerType())


# Apply UDF to create separate binary columns
for i in range(num_classes):
    train_df = train_df.withColumn(f"label_{i}", extract_label(i)(col(target_col)))

train_df = train_df.drop(*feat_cols, target_col, "month_idx", "ncodpers")

train_df.show()

for i in range(num_classes):
    if i == 0:
        continue
    print(f"Counting labels for class {i}")
    # Compute class distribution
    class_counts = train_df.groupBy(f"label_{i}").count().collect()
    total_count = sum(row["count"] for row in class_counts)

    # Compute class weights
    class_weights = {row[f"label_{i}"]: total_count / row["count"] for row in class_counts}
    train_df = train_df.withColumn("weight", when(col(f"label_{i}") == 1, class_weights[1])
                                   .otherwise(class_weights[0]))
    evaluator = BinaryClassificationEvaluator(labelCol=f"label_{i}", metricName="areaUnderPR")
    if i in ft_list:
        print(f"Fine-tuning {i}th class model.")
        gbt = GBTClassifier(featuresCol="feature", labelCol=f"label_{i}")
        paramGrid = ParamGridBuilder() \
            .addGrid(gbt.maxDepth, [3, 5, 7]) \
            .addGrid(gbt.maxBins, [16, 32, 64]) \
            .addGrid(gbt.maxIter, [10, 20, 50]) \
            .build()

        crossVal = CrossValidator(estimator=gbt,
                                  estimatorParamMaps=paramGrid,
                                  evaluator=evaluator,
                                  numFolds=3)
        cvModel = crossVal.fit(train_df)
        bestModel = cvModel.bestModel
        aupr = evaluator.evaluate(bestModel.transform(train_df))
        print(f"AUPR class {i}: {aupr}")
        bestModel.write().overwrite().save(
            f"/home/m1nhd3n/Works/DataEngineer/product_recommendations/models/spark/bests"
            f"/gbt_{i}")
    else:
        print(f"Skip fine-tuning for class {i}")
        gbt = GBTClassifier(labelCol=f"label_{i}", featuresCol="feature", maxIter=100)
        model = gbt.fit(train_df)
        aupr = evaluator.evaluate(model.transform(train_df))
        print(f"AUPR class {i}: {aupr}")
        model.write().overwrite().save(
            f"/home/m1nhd3n/Works/DataEngineer/product_recommendations/models/spark/bests"
            f"/gbt_{i}"
        )
