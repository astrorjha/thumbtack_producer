"""
### Thumbtack POC: Synthetic Traffic Producer (Airflow 3.1)

**Purpose:** This DAG acts as a high-fidelity simulator for the Thumbtack website. It generates 
synthetic, "messy" user service requests and drops them into an S3 staging bucket. 
This simulates the raw data source that the downstream GPU-accelerated inference 
pipeline (the Consumer) will process.

**Key Features:**
1. **Business Hour Constraints:** Using the Airflow 3.1 `MultipleCronTriggerTimetable`, 
   this DAG only runs between 8:00 AM and 7:00 PM EST.
2. **Realistic "Messy" Text:** Employs a combinatorial generator to mimic human 
   typing quirks (typos, mixed casing, varying urgency, and filler words).
3. **Geographical Diversity:** Randomly assigns requests to 25+ major US metro 
   zip codes to provide rich data for Databricks transformations.
4. **Traffic Spiking:** Uses a sinusoidal wave to simulate mid-day traffic peaks 
   centered around 2:00 PM EST.

**Infrastructure Dependencies:**
- **Connection ID:** `thumbtack` (Requires S3 write permissions).
- **Target Bucket:** `thumbtack-poc-staging`
"""

import os
import uuid
import random
import io
import numpy as np
import pendulum
from airflow.sdk import DAG
from airflow.providers.python.operators.python import PythonOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

# --- CONFIGURATION & CONSTANTS ---
EST = pendulum.timezone("America/New_York")
S3_BUCKET = "thumbtack-poc-staging"
AWS_CONN_ID = "aws_fe_srvc_account"

# Diverse US Zip Codes covering Northeast, South, Midwest, Southwest, and West
ZIP_CODES = [
    "10001", "10012", "11201", "02108", "19103", # Northeast (NYC, Boston, Philly)
    "30303", "33130", "20001", "28202", "37201", # South (Atlanta, Miami, DC, Charlotte, Nashville)
    "60601", "48226", "55401", "63101", "43215", # Midwest (Chicago, Detroit, Indy, St. Louis, Columbus)
    "75201", "77002", "78701", "80202", "85004", # Southwest/Mountain (Dallas, Houston, Austin, Denver, Phoenix)
    "94105", "90012", "98101", "97201", "92101"  # West Coast (SF, LA, Seattle, Portland, San Diego)
]

# Core Intents for the 27 Thumbtack Categories mapped in the model
INTENTS = {
    "Appliance Repair": ["fridge not cold", "washer leaking", "dryer making noise", "oven wont heat"],
    "Carpet Cleaning": ["stains in living room", "dog pee on rug", "deep clean carpets", "steam clean"],
    "Concrete & Masonry": ["crack in driveway", "patio bricks loose", "retaining wall repair", "paving"],
    "Duct & Vent Cleaning": ["air smells dusty", "clean dryer vent", "vent cleaning", "ac ducts"],
    "Electrical": ["outlet sparking", "lights flickering", "breaker keeps tripping", "fan install"],
    "Flooring Installation": ["laminate in kitchen", "hardwood floor", "tile bathroom", "vinyl planks"],
    "Furniture Assembly": ["ikea desk", "wayfair bed frame", "dresser assembly", "mounting shelf"],
    "Garage Door Repair": ["door stuck", "broken spring", "opener not working", "remote broken"],
    "General Contracting": ["home addition", "basement finish", "structural work", "full renovation"],
    "Gutter Cleaning": ["leaves in gutters", "downspout clogged", "overflowing gutters", "clean roof"],
    "HVAC": ["ac broken", "furnace clicking", "no heat", "hvac maintenance", "thermostat issue"],
    "Handyman": ["patch drywall", "fix door handle", "hang pictures", "general repairs"],
    "House Cleaning": ["weekly clean", "deep clean", "move out cleaning", "maid service"],
    "House Painting": ["exterior paint", "fence staining", "deck painting", "whole house paint"],
    "Interior Painting": ["paint kitchen", "bedroom walls", "ceiling touch up", "trim painting"],
    "Junk Removal": ["haul away old couch", "garage cleanout", "trash removal", "heavy debris"],
    "Kitchen Remodeling": ["new cabinets", "countertop install", "backsplash", "island design"],
    "Landscaping Design": ["new garden layout", "hardscape plan", "backyard redesign", "modern yard"],
    "Lawn Care": ["mow the grass", "fertilizing", "weed control", "yard cleanup", "aeration"],
    "Local Moving": ["1 bedroom move", "moving house", "apartment relocation", "local haul"],
    "Packing & Unpacking": ["pack boxes", "unpack kitchen", "help wrapping fragile items"],
    "Pest Control": ["ants in kitchen", "spider problem", "roach treatment", "bug spray"],
    "Plumbing": ["leaky faucet", "toilet clogged", "drain slow", "hot water heater", "pipe leak"],
    "Roofing": ["shingle missing", "roof leak", "new roof estimate", "storm damage"],
    "TV Mounting": ["hang tv on wall", "65 inch tv mount", "hide wires", "soundbar install"],
    "Termite Treatment": ["termite inspection", "wood damage", "termite protection"],
    "Tree Trimming": ["cut branches", "prune oak tree", "stump removal", "tree falling"]
}

# --- HELPER FUNCTIONS ---

def generate_messy_text():
    """Generates a random, messy text string to simulate human user input."""
    category = random.choice(list(INTENTS.keys()))
    intent = random.choice(INTENTS[category])
    
    fillers = ["hi", "hello", "need help", "looking for", "can someone", "asap", "plz", "emergency", "hey there"]
    modifiers = ["in my house", "at my condo", "today", "this week", "urgently", "near me", "immediately"]
    closings = ["thx", "thanks", "call me", "how much?", "quote plz", "!!", "best regards"]
    
    parts = []
    if random.random() > 0.3: parts.append(random.choice(fillers))
    parts.append(intent)
    if random.random() > 0.4: parts.append(random.choice(modifiers))
    if random.random() > 0.5: parts.append(random.choice(closings))
    
    text = " ".join(parts)
    
    # Random casing: lower, upper, sentence, or chaotic random
    case = random.choice(['lower', 'upper', 'sentence', 'random'])
    if case == 'lower': text = text.lower()
    elif case == 'upper': text = text.upper()
    elif case == 'random': text = "".join(c.upper() if random.random() > 0.5 else c.lower() for c in text)
    
    return text

def simulate_traffic(bucket_name, logical_date, **kwargs):
    """Calculates spike volume and uploads a Parquet batch to S3."""
    now_est = pendulum.now("America/New_York")
    hour = now_est.hour
    
    # Sinusoidal logic peaking at 2 PM (Hour 14) during the 8am-7pm window
    multiplier = 1.2 + np.sin(np.pi * (hour - 8) / 11) 
    batch_size = int(random.randint(500, 1200) * multiplier)
    
    data = [{
        "request_id": str(uuid.uuid4()),
        "request_text": generate_messy_text(),
        "zip_code": random.choice(ZIP_CODES),
        "created_at": now_est.isoformat(),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "property_type": random.choice(["house", "townhouse", "apartment", "condo", "commercial"]),
        "urgency": random.choice(["flexible", "within_48_hours", "within_a_week", "asap"])
    } for _ in range(batch_size)]
    
    df = pd.DataFrame(data)
    
    # Memory-efficient buffer for S3 upload
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    ds_str = logical_date.strftime('%Y-%m-%d')
    key = f"inbound/{ds_str}/batch_{now_est.strftime('%H%M')}.parquet"
    
    s3.load_file_obj(buffer, key=key, bucket_name=bucket_name, replace=True)
    print(f"Uploaded {batch_size} messy requests from across the US to {key}")

# --- DAG DEFINITION (Airflow 3.1) ---

with DAG(
    dag_id='thumbtack_step0_producer',
    start_date=pendulum.datetime(2026, 3, 11, tz="America/New_York"),
    # Strictly 8 AM to 7 PM EST per Thumbtack Business Hour requirements
    schedule=MultipleCronTriggerTimetable(
        ["0 8-19 * * *"], 
        timezone="America/New_York"
    ),
    catchup=False,
    doc_md=__doc__,
    tags=['airflow_3.1', 'producer', 'thumbtack', 'geography', 'messy-data']
) as dag:

    generate_spike = PythonOperator(
        task_id='simulate_user_traffic',
        python_callable=simulate_traffic,
        op_kwargs={'bucket_name': S3_BUCKET}
    )