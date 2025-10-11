# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count

spark = SparkSession.builder.appName("TitanicAnalysis").getOrCreate()
print("SparkSession created successfully.")
# %%
file_path = 'titanic_cleaned.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("DataFrame loaded.")
df.printSchema()
df.show(5)
# %%
print("--- Starting Analysis ---")

print("\nGender distribution:")
df.groupBy("sex").count().show()

print("\nSurvival rate by class:")
df.groupBy("pclass").agg(avg("survived").alias("survival_rate")).orderBy("pclass").show()

print("\nAverage fare by class:")
df.groupBy("pclass").agg(avg("fare").alias("average_fare")).orderBy("pclass").show()
# %%
spark.stop()

print("\nSparkSession stopped. All resources released.")
# %%
