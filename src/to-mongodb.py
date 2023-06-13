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
