from pyspark.sql import SparkSession
from pyspark.sql.functions import count, expr, col
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Create a SparkSession
spark = SparkSession.builder \
    .appName('MongoDBIntegration') \
    .config("spark.mongodb.input.uri", "mongodb://root:example@mongo:27017/admin.votes") \
    .getOrCreate()

# Load the MongoDB data into a DataFrame
df = spark.read.format("mongo").load()

# Extract relevant fields and group by state and party
result = df.select(
    df["voter.address.state"].alias("state"),
    df["vote.voting_choice.party"].alias("party"),
    df["vote.vote_valid"].alias("validity")
).where(col("validity") == "Yes").groupby("state", "party").agg(count("validity").alias("votes"))

# Find the party with the most votes in each state
winners = result.withColumn("rn", F.row_number().over(Window.partitionBy("state").orderBy(F.desc("votes")))).filter(col("rn") == 1).drop("rn")

# Calculate the percentage of victory
total_votes = result.groupby("state").agg(F.sum("votes").alias("total_votes"))
winners_with_percentage = winners.join(total_votes, "state").withColumn("percentage", (col("votes") / col("total_votes")) * 100)

# Write the result to MongoDB
winners_with_percentage.write.format("mongo").mode("overwrite").option("spark.mongodb.output.uri", "mongodb://root:example@mongo:27017/admin.election_results").save()