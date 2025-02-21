from pyspark.sql import SparkSession, Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, log, row_number

spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .appName("Product Recommendation") \
    .getOrCreate()

df = spark.read.csv("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess/preprocessed.csv", header=True, inferSchema=True)
df.printSchema()

# Transform date column
date_df = df.select("fecha_dato").distinct()
date_window = Window.orderBy(date_df.fecha_dato)
date_df = date_df.withColumn("month_idx", row_number().over(date_window))
df = df.join(date_df, on="fecha_dato")
df = df.drop("fecha_dato", "pais_residencia", "cod_prov")

# Log transform gross income to minimize skewness
df = df.withColumn("renta", log(col("renta")))

product_cols = [
    'ind_ahor_fin_ult1',
    'ind_aval_fin_ult1',
    'ind_cco_fin_ult1',
    'ind_cder_fin_ult1',
    'ind_cno_fin_ult1',
    'ind_ctju_fin_ult1',
    'ind_ctma_fin_ult1',
    'ind_ctop_fin_ult1',
    'ind_ctpp_fin_ult1',
    'ind_deco_fin_ult1',
    'ind_deme_fin_ult1',
    'ind_dela_fin_ult1',
    'ind_ecue_fin_ult1',
    'ind_fond_fin_ult1',
    'ind_hip_fin_ult1',
    'ind_plan_fin_ult1',
    'ind_pres_fin_ult1',
    'ind_reca_fin_ult1',
    'ind_tjcr_fin_ult1',
    'ind_valo_fin_ult1',
    'ind_viv_fin_ult1',
    'ind_nomina_ult1',
    'ind_nom_pens_ult1',
    'ind_recibo_ult1'
]

vecAssembler = VectorAssembler(inputCols=product_cols, outputCol="product_vec")

indexer_cols = [
    "sexo",
    "indresi",
    "indext",
    "segmento",
    "ind_empleado",
    "tiprel_1mes",
    "canal_entrada",
    "indrel_1mes",
    "indrel",
    "ind_nuevo",
    "indfall",
    "ind_actividad_cliente"
]

strIndexer = StringIndexer(
    inputCols=indexer_cols, 
    outputCols=["e_" + c for c in indexer_cols]
)

onehotEncoder = OneHotEncoder(inputCols=["e_canal_entrada"], outputCols=["o_canal_entrada"])

featAssembler = VectorAssembler(
    inputCols=[*["e_" + c for c in indexer_cols if "canal_entrada" not in c], "o_canal_entrada", "age", "renta", "antiguedad"], 
    outputCol="feat_vec"
)

pipeline = Pipeline(stages=[
    vecAssembler,
    strIndexer,
    onehotEncoder,
    featAssembler
])

pipelineModel = pipeline.fit(df)

df = pipelineModel.transform(df)

keep_cols = ["feat_vec", "product_vec", "month_idx", "ncodpers"]

df = df.drop(*[c for c in df.columns if c not in keep_cols])

df.show()

df.write.parquet("/home/m1nhd3n/Works/DataEngineer/product_recommendations/data/preprocess/transformed_data.parquet")
