import requests
import json
import psycopg2
import os
import logging
from dotenv import load_dotenv
import time
from datetime import datetime

load_dotenv()

# Generator schedule data URL
GENERATOR_URL = "https://wrldc.in/GeneratorSchedule_data.aspx/Get_GeneratorScheduleData_state_Wise"
DEMAND_URL = "https://wrldc.in/OnlinestateTest1.aspx/GetRealTimeData_state_Wise"
INTERSTATE_FLOW_URL = "https://wrldc.in/InterRegionalLinks_Data.aspx/Get_InterRegionalLinks_Region_Wise"

GENERATOR_COLUMNS = ["state_id", "state_name", "dec_capacity",
                     "schedule", "actual", "deviation", "last_update"]
DEMAND_COLUMNS = ["state_id", "state_name", "sch_drawal", "act_drawal", "current_datetime",
                  "frequency", "deviation", "generation", "demand", "act_data", "sch_data"]
INTERLINK_COLUMNS = ["region_id", "region_name", "export_ttc", "import_ttc", "long_term",
                     "short_term", "px_import", "px_export", "total", "current_loading", "last_update"]

ERROR_LOG_PATH = "logs/error_dates.txt"

# Database connection details
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")
user = os.getenv("USER")
password = os.getenv("PASSWORD")

# Set up logging
logging.basicConfig(filename='logs/scrapper.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.info("Starting the scrapper")
logging.info("Getting the schedule data")

# Get the data from the API


def transform_api_data(data, columns):
    rows = json.loads(data["d"])
    transformed_data = []
    for row in rows:
        converted_data = {
            key.lower().replace('_', ''): value
            if key.lower().replace('_', '') != 'lastupdate'
            else datetime.strptime(value, '%Y-%d-%m %H:%M:%S')
            for key, value in row.items()
        }
        transformed_data.append([converted_data.get(
            column.lower().replace('_', ''), None) for column in columns])
    return transformed_data


def get_data(url, payload):
    response = requests.post(url, json=payload, timeout=30)
    return json.loads(response.text)

# Load data into database


def load_data(table_name, column_names, data):
    logging.info(f"Loading {table_name} data into database")
    try:
        with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
            with conn.cursor() as cursor:
                query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))})"
                # Execute the query with multiple rows
                cursor.executemany(query, data)
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error in loading {table_name} data into database")
        logging.error(error)
    logging.info(f"Loaded {table_name} data into database")


# Iterate over scheduled dates, fetch value for demand, generation, and interlink, and store in the database
def handle_error(date, data_type, exception):
    error_message = f"Error in getting the {data_type} data for date: {date}\n"
    logging.error(error_message)
    logging.error(exception)
    with open(ERROR_LOG_PATH, "a") as f:
        f.write(f"{date.strip()}- {data_type}\n")


for file_name in sorted(os.listdir("data/schedule_dates")):
    logging.info("Getting the schedule data for file: " + file_name)
    with open(f"data/schedule_dates/{file_name}") as f:
        for date in f.readlines():
            payload = {"date": date.strip()}

            try:
                generator_data = get_data(GENERATOR_URL, payload)
                logging.info("Generator data fetched for date: " + date)
                generator_data = transform_api_data(
                    generator_data, GENERATOR_COLUMNS)
            except Exception as e:
                handle_error(date, "generator", e)
            else:
                load_data("generator", GENERATOR_COLUMNS, generator_data)

            try:
                demand_data = get_data(DEMAND_URL, payload)
                logging.info("Demand data fetched for date: " + date)
                demand_data = transform_api_data(demand_data, DEMAND_COLUMNS)
            except Exception as e:
                handle_error(date, "demand", e)
            else:
                load_data("demand", DEMAND_COLUMNS, demand_data)

            try:
                interlink_data = get_data(INTERSTATE_FLOW_URL, payload)
                logging.info("Interlink data fetched for date: " + date)
                interlink_data = transform_api_data(
                    interlink_data, INTERLINK_COLUMNS)
            except Exception as e:
                handle_error(date, "interlink", e)
            else:
                load_data("interlink", INTERLINK_COLUMNS, interlink_data)

            logging.info("Data for date: " + date.strip() +
                         " loaded into database")

