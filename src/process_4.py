from pyspark.sql import SparkSession
from pyspark.sql.functions import max
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from twilio.rest import Client

# create SparkSession
spark = SparkSession.builder \
    .appName("ElectionResults") \
    .config("spark.mongodb.input.uri", "mongodb://root:example@mongo:27017/admin.election_results") \
    .getOrCreate()

# read data from MongoDB
df = spark.read.format("mongo").load()
# create a dictionary of grand electors by state
electors_dict = {
    "Alabama": 9,
    "Alaska": 3,
    "Arizona": 11,
    "Arkansas": 6,
    "California": 55,
    "Colorado": 9,
    "Connecticut": 7,
    "Delaware": 3,
    "Florida": 29,
    "Georgia": 16,
    "Hawaii": 4,
    "Idaho": 4,
    "Illinois": 20,
    "Indiana": 11,
    "Iowa": 6,
    "Kansas": 6,
    "Kentucky": 8,
    "Louisiana": 8,
    "Maine": 4,
    "Maryland": 10,
    "Massachusetts": 11,
    "Michigan": 16,
    "Minnesota": 10,
    "Mississippi": 6,
    "Missouri": 10,
    "Montana": 3,
    "Nebraska": 5,
    "Nevada": 6,
    "New Hampshire": 4,
    "New Jersey": 14,
    "New Mexico": 5,
    "New York": 29,
    "North Carolina": 15,
    "North Dakota": 3,
    "Ohio": 18,
    "Oklahoma": 7,
    "Oregon": 7,
    "Pennsylvania": 20,
    "Rhode Island": 4,
    "South Carolina": 9,
    "South Dakota": 3,
    "Tennessee": 11,
    "Texas": 38,
    "Utah": 6,
    "Vermont": 3,
    "Virginia": 13,
    "Washington": 12,
    "West Virginia": 5,
    "Wisconsin": 10,
    "Wyoming": 3,
    "District of Columbia": 3
}


# Convert dictionary to DataFrame
electors_df = spark.createDataFrame([(k, v) for k, v in electors_dict.items()], ["state", "electors"])

# Join the electors DataFrame with the election results DataFrame
df = df.join(electors_df, on="state", how="inner")

# Compute total votes for each party nationwide
nationwide_df = df.groupBy("party").agg(F.sum("votes").alias("total_votes"))

# Determine the party that won the most votes nationwide
nationwide_winner = nationwide_df.orderBy(F.desc("total_votes")).first()[0]

# Identify maximum votes in each state
state_max_df = df.groupBy("state").agg(max("votes").alias("max_votes"))

# Join original dataframe with state_max_df to find ties
df = df.join(state_max_df, on="state", how="inner")

# Create a window partitioned by state
window = Window.partitionBy(df['state'])

from pyspark.sql.functions import when

# Calculate number of winners per state (this will be >1 in case of a tie)
df = df.withColumn('winners', F.sum(when(df.votes == df.max_votes, 1).otherwise(0)).over(window))


# In case of tie, assign electors to the nationwide winner, otherwise to the state winner
df = df.withColumn('final_party', when(df.winners > 1, nationwide_winner).otherwise(df.party))

# Compute total grand electors for each final party
result_df = df.groupBy("final_party").sum("electors")

# Save the result to MongoDB
result_df.write.format("mongo").option("uri", "mongodb://root:example@mongo:27017/admin.election_results_out").mode("overwrite").save()

# Your Twilio account sid and auth token
account_sid = 'ACef65bb78dd4d08cbabbed5bc0c5c8a42'
auth_token = '37d27c1c6c6f2ee6b8bde3d239a76c0e'
client = Client(account_sid, auth_token)

# Collect results from DataFrame
result = result_df.collect()

# Format the results into a string
result_str = "\n".join([f"{row['final_party']}: {row['sum(electors)']} electors" for row in result])

# Create a professional message
message_body = f"Dear recipient, \n\nWe are pleased to share with you the final election results:\n\n{result_str}\n\nWe would like to express our gratitude for your patience and interest in our democratic process. For more detailed results, please visit our official website.\n\nBest regards,\n[Election Committee]"

message = client.messages.create(
    from_='+13613265649',
    body=message_body,
    to='+14385064453'
)

print(f"Message sent with id {message.sid}")

# Stop the SparkSession
spark.stop()