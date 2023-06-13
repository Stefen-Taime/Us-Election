
## Creating a Election Monitoring System Using MongoDB, Spark, Twilio SMS Notifications, and Dash

![](https://cdn-images-1.medium.com/max/3720/1*sjwZvn23nt0X6cuFt8pM0w.png)

In this article, we present a proof-of-concept (POC) for an innovative solution that tackles this challenge in the context of election monitoring. This solution was devised for a government that approached a young digital company specializing in data, with a desire to make election results more transparent, accessible, and real-time.

The system proposed is designed to ingest voter data, process and analyze it, alert the media and concerned parties via SMS once the results are ready, and finally display the results on an interactive map via a Dash application.

## The Data Pipeline

In this context, the Spark cluster is set up with a worker node, which will execute the tasks assigned by the Spark master. This setup allows for efficient handling of data processing tasks, which can be split among multiple worker nodes if necessary.

The data the system processes come from an intriguing source: a synthetic dataset of voting records. A script using the Python library Faker generates this data, imitating realistic voting behavior across all US states and the District of Columbia. The synthetic data is stored in MongoDB, a popular NoSQL database known for its flexibility and scalability, making it an excellent choice for handling large datasets like voting records.

    from datetime import datetime
    from faker import Faker
    from pymongo import MongoClient
    
    # Init Faker
    fake = Faker()
    
    ## Init MongoDB
    client = MongoClient('mongodb://root:example@localhost:27017/')
    db = client['admin']
    collection = db['votes']
    
    state_weights = {
        "Alabama": (0.60, 0.40),
        "Alaska": (0.55, 0.45),
        "Arizona": (0.15, 0.85),
        "Arkansas": (0.20, 0.80),
        "California": (0.15, 0.85),
        "Colorado": (0.70, 0.30),
        "Connecticut": (0.10, 0.90),
        "Delaware": (0.34, 0.66),
        "Florida": (0.82, 0.18),
        "Georgia": (0.95, 0.05),
        "Hawaii": (0.50, 0.50),
        "Idaho": (0.67, 0.33),
        "Illinois": (0.60, 0.40),
        "Indiana": ((0.15, 0.85)),
        "Iowa": (0.45, 0.55),
        "Kansas": (0.40, 0.60),
        "Kentucky": (0.62, 0.38),
        "Louisiana": (0.58, 0.42),
        "Maine": (0.60, 0.40),
        "Maryland": (0.55, 0.45),
        "Massachusetts": (0.63, 0.37),
        "Michigan": (0.62, 0.38),
        "Minnesota": (0.61, 0.39),
        "Mississippi": (0.41, 0.59),
        "Missouri": (0.60, 0.40),
        "Montana": (0.57, 0.43),
        "Nebraska": (0.56, 0.44),
        "Nevada": (0.55, 0.45),
        "New Hampshire": (0.54, 0.46),
        "New Jersey": (0.53, 0.47),
        "New Mexico": (0.52, 0.48),
        "New York": (0.51, 0.49),
        "North Carolina": (0.50, 0.50),
        "North Dakota": (0.05, 0.95),
        "Ohio": (0.58, 0.42),
        "Oklahoma": (0.57, 0.43),
        "Oregon": (0.56, 0.44),
        "Pennsylvania": (0.55, 0.45),
        "Rhode Island": (0.50, 0.50),
        "South Carolina": (0.53, 0.47),
        "South Dakota": (0.48, 0.52),
        "Tennessee": (0.51, 0.49),
        "Texas": (0.60, 0.40),
        "Utah": (0.59, 0.41),
        "Vermont": (0.58, 0.42),
        "Virginia": (0.57, 0.43),
        "Washington": (0.44, 0.56),
        "West Virginia": (0.55, 0.45),
        "Wisconsin": (0.46, 0.54),
        "Wyoming": (0.53, 0.47),
        "District of Columbia": ((0.15, 0.85))  
    }
    
    def generate_vote(state):
        weights = state_weights.get(state, (0.50, 0.50))  # Get the weights for the state, or use (0.50, 0.50) as a default
        vote = {
            "voting_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),  # Updated line
            "voter": {
                "voter_id": str(fake.unique.random_number(digits=9)),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "address": {
                    "street": fake.street_address(),
                    "city": fake.city(),
                    "state": state,
                    "zip_code": fake.zipcode()
                },
                "birth_date": str(fake.date_of_birth(minimum_age=18, maximum_age=90)),
                "gender": fake.random_element(elements=('Male', 'Female')),
            },
            "vote": {
                "voting_date": "2023-06-06",
                "voting_location": fake.address(),
                "election": {
                    "type": "Presidential Election",
                    "year": "2023"
                },
                "vote_valid": "Yes",
                "voting_choice": {
                    "party": fake.random_element(elements=('Republican', 'Democrat')),
                }
            }
        }
    
        return vote
    
    # List of states
    states = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
              "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois",
              "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
              "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
              "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
              "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania",
              "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
              "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
              "District of Columbia"]
    
    
    # Generate fake voting data for each state and insert into MongoDB
    for state in states:
        for i in range(1, 61):
            vote = generate_vote(state)
            collection.insert_one(vote)
    
    print(f"All votes saved to MongoDB")

For each state, the synthetic data simulates voter choices based on predefined probabilities, reflecting historical voting patterns. This data, consisting of 60 voters for each state, serves as the input for the Spark processing system.

The Spark system processes the data, determining the winning party in each state. It then calculates the percentage of votes each party has won. This critical information is then fed into an SMS notification system, alerting media outlets and the relevant parties with real-time election results.

    version: '3.1'
    services:
    
      # ===================== #
      #     Apache Spark      #
      # ===================== #
      spark:
        image: bitnami/spark:3.3.1
        environment:
          - SPARK_MODE=master
        ports:
          - '8080:8080'
          - '7077:7077'
        volumes:
          - ./data:/data
          - ./src:/src
      spark-worker:
        image: bitnami/spark:3.3.1
        environment:
          - SPARK_MODE=worker
          - SPARK_MASTER_URL=spark://spark:7077
          - SPARK_WORKER_MEMORY=4G
          - SPARK_EXECUTOR_MEMORY=4G
          - SPARK_WORKER_CORES=4
        ports:
          - '8081:8081'
        volumes:
          - ./data:/data
          - ./src:/src
    
      # ===================== #
      #        MongoDB        #
      # ===================== #
      mongo:
        image: mongo:4.4
        volumes:
          - ./mongo:/data/db
        ports:
          - '27017:27017'
        environment:
          - MONGO_INITDB_ROOT_USERNAME=root
          - MONGO_INITDB_ROOT_PASSWORD=example
      mongo-express:
        image: mongo-express
        ports:
          - '8091:8081'
        environment:
          - ME_CONFIG_MONGODB_ADMINUSERNAME=root
          - ME_CONFIG_MONGODB_ADMINPASSWORD=example
          - ME_CONFIG_MONGODB_SERVER=mongo
          - ME_CONFIG_MONGODB_PORT=27017

## Data Processing with PySpark (Job 1)

 1. Create a SparkSession: The code initiates a SparkSession, which is an entry point to any Spark functionality. When it starts, it connects to the MongoDB database where the data is stored.

 2. Load Data: The code then reads the data from MongoDB and loads it into a DataFrame, which is a distributed collection of data organized into named columns. It’s similar to a table in a relational database and can be manipulated in similar ways.

 3. Data Processing: The code selects the relevant fields from the DataFrame (state, party, and validity of the vote), groups them by state and party, and counts the number of valid votes for each party in each state.

 4. Find Winners: Next, the code finds the party with the most votes in each state. It does this by ranking the parties within each state based on the number of votes they got and then selecting the one with the highest rank (i.e., the one with the most votes).

 5. Calculate Percentages: The code then calculates the percentage of votes each winning party got in its state. It does this by dividing the number of votes the winning party got by the total votes in that state and multiplying by 100.

 6. Write Results: Finally, the code saves the results, which include the winning party and their vote percentage in each state, back to MongoDB but in a different collection called ‘election_results’.

So in essence, this code processes voting records to determine the party that won the most votes in each state and calculates what percentage of the total votes in that state the winning party received. This analysis can give a clear picture of the distribution of votes in an election.

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

Output:

    {
        _id: ObjectId('64873b3df42ba41d32f3d1a6'),
        state: 'Utah',
        party: 'Republican',
        votes: 127,
        total_votes: 240,
        percentage: 52.916666666666664
    }

## Data Processing with PySpark (Job 2)

 1. Create a SparkSession and Load Data: The script starts a SparkSession and then loads data from a MongoDB collection.

 2. Set Electoral Votes by State: The United States uses a system called the Electoral College to decide the outcome of presidential elections. Each state has a number of votes in the Electoral College that is largely proportional to its population. This script creates a dictionary that maps each state to its number of electoral votes. Then it converts this dictionary into a DataFrame.

 3. Join Electoral Votes with Election Data: The script combines the election results data with the electoral votes data, based on the state name. This gives us a DataFrame where each row has the state name, the party, the votes that party received, and the number of electoral votes that state has.

 4. Calculate Nationwide Votes: The script calculates the total votes received by each party nationwide.

 5. Identify the Nationwide Winner: The script determines the party that got the most votes nationwide.

 6. Calculate Maximum State Votes and Handle Ties: The script identifies the maximum number of votes received in each state and handles ties by giving the electoral votes to the nationwide winner.

 7. Calculate Total Grand Electors for Each Party: The script then calculates the total number of electoral votes (“grand electors”) each party received nationwide, considering the rule of tie-breaking.

 8. Save the Results: The script saves the electoral votes results back to MongoDB.

 9. Notify the Results via SMS: Using Twilio, an online messaging service, the script then sends an SMS with the election results. The results are formatted as a string which includes each party and the number of electoral votes they won.

 10. Stop the SparkSession: Lastly, the script stops the SparkSession, releasing its resources.

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import max
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from twilio.rest import Client
    
    spark = SparkSession.builder \
        .appName("ElectionResults") \
        .config("spark.mongodb.input.uri", "mongodb://root:example@mongo:27017/admin.election_results") \
        .getOrCreate()
    
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
    
    df = df.join(electors_df, on="state", how="inner")
    
    nationwide_df = df.groupBy("party").agg(F.sum("votes").alias("total_votes"))
    
    nationwide_winner = nationwide_df.orderBy(F.desc("total_votes")).first()[0]
    
    # Identify maximum votes in each state
    state_max_df = df.groupBy("state").agg(max("votes").alias("max_votes"))
    
    df = df.join(state_max_df, on="state", how="inner")
    
    window = Window.partitionBy(df['state'])
    
    from pyspark.sql.functions import when
    
    df = df.withColumn('winners', F.sum(when(df.votes == df.max_votes, 1).otherwise(0)).over(window))
    
    
    df = df.withColumn('final_party', when(df.winners > 1, nationwide_winner).otherwise(df.party))
    
    result_df = df.groupBy("final_party").sum("electors")
    
    # Save the result to MongoDB
    result_df.write.format("mongo").option("uri", "mongodb://root:example@mongo:27017/admin.election_results_out").mode("overwrite").save()
    
    account_sid = ''
    auth_token = ''
    client = Client(account_sid, auth_token)
    
    result = result_df.collect()
    
    result_str = "\n".join([f"{row['final_party']}: {row['sum(electors)']} electors" for row in result])
    
    message_body = f"Dear recipient, \n\nWe are pleased to share with you the final election results:\n\n{result_str}\n\nWe would like to express our gratitude for your patience and interest in our democratic process. For more detailed results, please visit our official website.\n\nBest regards,\n[Election Committee]"
    
    message = client.messages.create(
        from_='',
        body=message_body,
        to=''
    )
    
    print(f"Message sent with id {message.sid}")
    
    # Stop the SparkSession
    spark.stop()

Output:

    {
        _id: ObjectId('6487445c358709227a7e9c71'),
        final_party: 'Republican',
        'sum(electors)': 201
    }
    
    
    {
        _id: ObjectId('6487445c358709227a7e9c72'),
        final_party: 'Democrat',
        'sum(electors)': 337
    }

## Notification of Results

![](https://cdn-images-1.medium.com/max/2160/1*DXpFgmETQw5WMKybNM4V_w.png)

## Visualization with Dash

The final step involves visualizing the results using Dash, a productive Python framework for building web analytic applications. It allows us to construct an interactive map of the United States, where each state is colored according to the party that won the majority of votes: blue for Democrats and red for Republicans. This enables users to easily and intuitively understand the election results.

 1. Connect to a Database: The script connects to a database (specifically MongoDB) where the election results are stored.

 2. Define the Geographic Data: The script contains a list of states with their latitude and longitude coordinates. This data will help to plot each state accurately on the map.

 3. Create a State Name to Abbreviation Dictionary: This dictionary is used to map full state names to their abbreviations (like “New York” to “NY”), because the map uses abbreviations.

 4. Set Up the Application: The script sets up an app using a framework called Dash, which helps in building interactive web applications.

 5. Define the Application Layout: The layout of the app is defined to include a graphical element (a map in this case).

 6. Update the Map: A function is defined that updates the map each time it’s called. This function does a few things:

 7. a. Get Election Results: The function fetches the election results from the database.

 8. b. Process Results: It processes these results to extract the necessary data. For each state, it gets the party that won and the percentage of votes that party received. Parties are assigned a numerical value to color-code them later (0 for Republican and 1 for Democrat).

 9. c. Prepare Hover Text: This is the text that appears when you hover over a state on the map. It shows the party that won and the percentage of votes they received.

 10. d. Create the Map: The function creates a map of the United States, with each state color-coded based on the party that won there (blue for Democrats and red for Republicans).

 11. e. Add Legends: Legends are added to the map to indicate which color corresponds to which party.

 12. f. Adjust the Layout: Finally, the function adjusts the layout of the map and returns it. The map is displayed in the web application.

![](https://cdn-images-1.medium.com/max/3720/1*sjwZvn23nt0X6cuFt8pM0w.png)

I hope this guide will give you a better understanding of how MongoDB, PySpark, Twilio and Dash can be used to build an efficient, high-performance data pipeline.

[Medium](https://medium.com/@stefentaime_10958/creating-a-real-time-election-monitoring-system-using-mongodb-spark-sms-notifications-and-dash-e3b276180dcb)
