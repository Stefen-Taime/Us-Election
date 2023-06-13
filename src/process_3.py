from pyspark.sql import SparkSession
from pyspark.sql.functions import count, expr, col

# Créer une SparkSession
spark = SparkSession.builder \
    .appName('MongoDBIntegration') \
    .config("spark.mongodb.input.uri", "mongodb://root:example@mongo:27017/admin.votes") \
    .getOrCreate()

# Charger les données MongoDB dans un DataFrame
df = spark.read.format("mongo").load()

# Calculer le nombre de votants hommes et femmes par État
gender_counts_by_state = df.groupBy(df["voter.address.state"].alias("state"), df["voter.gender"].alias("gender")).agg(count("voter.gender").alias("count"))

# Calculer le nombre total de votants par État
total_counts_by_state = df.groupBy(df["voter.address.state"].alias("state")).agg(count("voter.gender").alias("total"))

# Joindre les deux DataFrames et calculer le pourcentage
gender_percentages_by_state = gender_counts_by_state.join(total_counts_by_state, "state").withColumn("percentage", (col("count") / col("total")) * 100)

# Écrire le résultat dans MongoDB
gender_percentages_by_state.write.format("mongo").mode("overwrite").option("spark.mongodb.output.uri", "mongodb://root:example@mongo:27017/admin.gender_percentages_by_state").save()

# Calculer le nombre de votants hommes et femmes pour l'ensemble de l'élection
gender_counts_total = df.groupBy(df["voter.gender"].alias("gender")).agg(count("voter.gender").alias("count"))

# Calculer le nombre total de votants pour l'ensemble de l'élection
total_counts_total = df.agg(count("voter.gender").alias("total"))

# Calculer le pourcentage
gender_percentages_total = gender_counts_total.withColumn("percentage", (col("count") / total_counts_total.collect()[0]["total"]) * 100)

# Écrire le résultat dans MongoDB
gender_percentages_total.write.format("mongo").mode("overwrite").option("spark.mongodb.output.uri", "mongodb://root:example@mongo:27017/admin.gender_percentages_total").save()
