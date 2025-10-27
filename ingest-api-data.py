import requests
import json
from datetime import datetime
from snowflake.snowpark import Session
import sys
import pytz
import logging

# ------------------------------------------------------
# CONFIGURATION & LOGGING
# ------------------------------------------------------

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# Set IST time zone
ist_timezone = pytz.timezone('Asia/Kolkata')

# Get current time in IST
current_time_ist = datetime.now(ist_timezone)

# Format timestamp
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')
today_string = current_time_ist.strftime('%Y_%m_%d')

# Local JSON filename
file_name = f'air_quality_data_{timestamp}.json'


# ------------------------------------------------------
# SNOWFLAKE CONNECTION FUNCTION
# ------------------------------------------------------

def snowpark_basic_auth() -> Session:
    """Create Snowpark session with basic authentication."""
    connection_parameters = {
        "ACCOUNT": "MYDLFCK-IN11716",     # ✅ your actual Snowflake account
        "USER": "SURESH",                 # ✅ your login username
        "PASSWORD": "<your-password>",    # 🔒 replace with your actual password
        "ROLE": "SYSADMIN",
        "DATABASE": "DEV_DB",
        "SCHEMA": "STAGE_SCH",
        "WAREHOUSE": "LOAD_WH"
    }

    session = Session.builder.configs(connection_parameters).create()
    logging.info("✅ Successfully connected to Snowflake")
    return session


# ------------------------------------------------------
# AIR QUALITY API FETCH + SNOWFLAKE UPLOAD
# ------------------------------------------------------

def get_air_quality_data(api_key, limit):
    """Fetch Air Quality Data and upload to Snowflake stage."""
    api_url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'

    params = {
        'api-key': api_key,
        'format': 'json',
        'limit': limit
    }

    headers = {
        'accept': 'application/json'
    }

    try:
        logging.info('Sending API request...')
        response = requests.get(api_url, params=params, headers=headers)
        logging.info(f'Response Status: {response.status_code}')

        if response.status_code == 200:
            # Log response sample
            logging.info(f"Response preview: {response.text[:200]}")

            if not response.text.strip():
                logging.error("❌ Empty API response")
                sys.exit(1)

            try:
                json_data = response.json()
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error: {e}")
                logging.error(f"Response sample: {response.text[:200]}")
                sys.exit(1)

            if "records" not in json_data or not json_data["records"]:
                logging.error("❌ No 'records' found in API response.")
                sys.exit(1)

            # Write to local file
            with open(file_name, 'w') as json_file:
                json.dump(json_data, json_file, indent=2)

            logging.info(f"✅ JSON file saved locally: {file_name}")

            # Snowflake stage path
            stg_location = f'@DEV_DB.STAGE_SCH.RAW_STG/india/{today_string}/'
            sf_session = snowpark_basic_auth()

            logging.info(f'Uploading file to Snowflake stage: {stg_location}')
            sf_session.file.put(file_name, stg_location, auto_compress=True)
            logging.info("✅ File uploaded to Snowflake stage successfully")

            # Verify upload
            lst_query = f'LIST {stg_location}{file_name}.gz'
            result_lst = sf_session.sql(lst_query).collect()
            logging.info(f"Stage verification result: {result_lst}")

            logging.info("🎯 Job completed successfully")
            return json_data

        else:
            logging.error(f"❌ API Error {response.status_code}: {response.text}")
            sys.exit(1)

    except Exception as e:
        logging.error(f"❌ An error occurred: {e}")
        sys.exit(1)

    return None


# ------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------

if __name__ == "__main__":
    api_key = "579b464db66ec23bdd000001b13b7a61324e49204e22851247dadc91"  # ✅ your API key
    limit_value = 4000

    logging.info("🚀 Starting Air Quality Data Pipeline Job")
    air_quality_data = get_air_quality_data(api_key, limit_value)
