from pyspark.sql import SparkSession
from pyspark.sql.functions import count, expr, col, datediff, to_date, year
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Create a SparkSession
spark = SparkSession.builder \
    .appName('MongoDBIntegration') \
    .config("spark.mongodb.input.uri", "mongodb://root:example@mongo:27017/admin.votes") \
    .getOrCreate()

# Load the MongoDB data into a DataFrame
df = spark.read.format("mongo").load()

# Convert the date strings to date type and calculate the age
df = df.withColumn("voting_date", to_date(col("vote.voting_date"), "yyyy-MM-dd"))
df = df.withColumn("birth_date", to_date(col("voter.birth_date"), "yyyy-MM-dd"))
df = df.withColumn("age", ((datediff(col("voting_date"), col("birth_date")) / 365)).cast("int"))

# Calculate the average age by state and party
avg_age_by_state_and_party = df.groupBy(df["voter.address.state"].alias("state"), df["vote.voting_choice.party"].alias("party")).agg(F.avg("age").alias("average_age"))

# Write the result to MongoDB
avg_age_by_state_and_party.write.format("mongo").mode("overwrite").option("spark.mongodb.output.uri", "mongodb://root:example@mongo:27017/admin.avg_age_by_state_and_party").save()

# Calculate the national average age
national_avg_age = df.agg(F.avg("age").alias("national_average_age"))

# Write the result to MongoDB
national_avg_age.write.format("mongo").mode("overwrite").option("spark.mongodb.output.uri", "mongodb://root:example@mongo:27017/admin.national_avg_age").save()

# Calculate the average age of all voters
avg_age_all_voters = df.agg(F.avg("age").alias("average_age_all_voters"))

# Write the result to MongoDB
avg_age_all_voters.write.format("mongo").mode("overwrite").option("spark.mongodb.output.uri", "mongodb://root:example@mongo:27017/admin.avg_age_all_voters").save()
