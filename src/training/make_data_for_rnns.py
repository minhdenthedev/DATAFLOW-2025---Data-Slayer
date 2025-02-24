from pyspark.sql import SparkSession, Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, log, row_number
from pyspark.sql import functions as F
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, LogisticRegression
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql.functions import expr
from xgboost.spark import SparkXGBClassifier


spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .appName("Product Recommendation") \
    .master("local[4]") \
    .getOrCreate()
df = spark.read.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess/created_target_col")
df.show()
