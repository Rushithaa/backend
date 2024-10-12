import requests
import json
import time
from py5paisa import FivePaisaClient
from tabulate import tabulate
from datetime import datetime, timedelta
import pytz
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import urllib.parse
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, ConfigurationError
from apscheduler.schedulers.background import BackgroundScheduler

# MongoDB connection setup
username = urllib.parse.quote_plus("mrushithaa")
password = urllib.parse.quote_plus("Hical@1767")
uri = f"mongodb+srv://{username}:{password}@nifty-banknifty-tide.1kwpe.mongodb.net/?retryWrites=true&w=majority&appName=nifty-banknifty-tide"

# 5paisa credentials
cred = {
    "APP_NAME": "5P52036093",
    "APP_SOURCE": "20116",
    "USER_ID": "KGg2CSG41Dv",
    "PASSWORD": "t7i3djwjDha",
    "USER_KEY": "VJ5LQlKmuh8MTviP72WeSQeTrmIj3lN7",
    "ENCRYPTION_KEY": "Cy0fWzhmQMWrETwDareSVaIKyom6HEhq"
}

five_paisa_client = FivePaisaClient(cred=cred)

# Global variables to store the current database names
current_nifty_db = None
current_banknifty_db = None

def connect_to_mongodb(db_name):
    max_retries = 5
    retry_delay = 1

    for attempt in range(max_retries):
        try:
            client = MongoClient(uri, server_api=ServerApi('1'))
            db = client[db_name]
            collection = db['data']
            client.admin.command('ping')
            print(f"Successfully connected to MongoDB database: {db_name}")
            return collection
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            if attempt < max_retries - 1:
                print(f"Connection attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"Failed to connect after {max_retries} attempts: {e}")
                return None
        except ConfigurationError as e:
            print(f"MongoDB configuration error: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

def create_daily_databases():
    global current_nifty_db, current_banknifty_db
    ist = pytz.timezone('Asia/Kolkata')
    current_date = datetime.now(ist).strftime("%d%m%Y")
    
    current_nifty_db = f"niftydata{current_date}"
    current_banknifty_db = f"bankniftydata{current_date}"
    
    # Create new collections in these databases
    connect_to_mongodb(current_nifty_db)
    connect_to_mongodb(current_banknifty_db)
    
    print(f"Created new databases: {current_nifty_db} and {current_banknifty_db}")

def fetch_and_store_data(index):
    global current_nifty_db, current_banknifty_db
    
    # Check if it's a weekday (Monday = 0, Friday = 4)
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    if current_time.weekday() > 4:
        print(f"It's weekend. Skipping data collection for {index}.")
        return

    db_name = current_nifty_db if index == 'NIFTY' else current_banknifty_db
    collection = connect_to_mongodb(db_name)
    if collection is None:
        print(f"Failed to connect to MongoDB for {index}. Skipping this data fetch.")
        return

    try:
        indian_date = current_time.strftime("%d-%m-%Y")
        indian_timestamp = current_time.strftime("%I:%M:%S %p")

        # Get expiry dates
        expiry_result = five_paisa_client.get_expiry("N", index)

        # Extract the first expiry code
        first_expiry = expiry_result['Expiry'][0]['ExpiryDate']
        expiry_code = first_expiry.split('(')[1].split('+')[0]

        # Get option chain
        option_chain = five_paisa_client.get_option_chain("N", index, expiry_code)

        # Calculate metrics
        Total_Call_OI = sum(option['OpenInterest'] for option in option_chain['Options'] if option['CPType'] == 'CE')
        Total_Put_OI = sum(option['OpenInterest'] for option in option_chain['Options'] if option['CPType'] == 'PE')
        Call_OI_Change = sum(option['ChangeInOI'] for option in option_chain['Options'] if option['CPType'] == 'CE')
        Put_OI_Change = sum(option['ChangeInOI'] for option in option_chain['Options'] if option['CPType'] == 'PE')
        Bull_Strength = sum(option['OpenInterest'] * option['LastRate'] for option in option_chain['Options'] if option['CPType'] == 'CE')
        Bear_Strength = sum(option['OpenInterest'] * option['LastRate'] for option in option_chain['Options'] if option['CPType'] == 'PE')
        Bull_Power = Bull_Strength / Total_Call_OI if Total_Call_OI != 0 else 0
        Bear_Power = Bear_Strength / Total_Put_OI if Total_Put_OI != 0 else 0
        PCR_OI = Total_Put_OI / Total_Call_OI if Total_Call_OI != 0 else 0
        Total_Call_Volume = sum(option['Volume'] for option in option_chain['Options'] if option['CPType'] == 'CE')
        Total_Put_Volume = sum(option['Volume'] for option in option_chain['Options'] if option['CPType'] == 'PE')
        PCR_Volume = Total_Put_Volume / Total_Call_Volume if Total_Call_Volume != 0 else 0

        # Prepare data for MongoDB
        data = {
            "indian_date": indian_date,
            "indian_timestamp": indian_timestamp,
            "Total_Call_OI": Total_Call_OI,
            "Total_Put_OI": Total_Put_OI,
            "Call_OI_Change": Call_OI_Change,
            "Put_OI_Change": Put_OI_Change,
            "Bull_Power": Bull_Power,
            "Bear_Power": Bear_Power,
            "PCR_OI": PCR_OI,
            "PCR_Volume": PCR_Volume
        }

        # Insert data into MongoDB
        collection.insert_one(data)
        print(f"Data for {index} inserted successfully at {indian_timestamp}")

    except Exception as e:
        print(f"An error occurred while fetching or storing data for {index}: {e}")

def is_weekday():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist).weekday() < 5

def run_weekday_job():
    if is_weekday():
        fetch_and_store_data('NIFTY')
        fetch_and_store_data('BANKNIFTY')

def create_databases_if_weekday():
    if is_weekday():
        create_daily_databases()

if __name__ == "__main__":
    # Initialize scheduler
    scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Kolkata'))

    # Schedule jobs
    scheduler.add_job(create_databases_if_weekday, 'cron', hour=9, minute=13)
    scheduler.add_job(run_weekday_job, 'interval', minutes=1)

    # Start the scheduler
    scheduler.start()

    print("Scheduler started. Press Ctrl+C to exit.")

    try:
        # Keep the script running
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()