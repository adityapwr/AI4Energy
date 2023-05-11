# create an python program to call api, transform it and load it into a database

from data_transformation import transform_api_data
import requests
import json
import pandas as pd
import psycopg2
import os
import logging
from dotenv import load_dotenv
load_dotenv()


# Generator schedule data url
GENERATOR_URL = "https://wrldc.in/GeneratorSchedule_data.aspx/Get_GeneratorScheduleData_state_Wise"
DEMAND_URL = "https://wrldc.in/OnlinestateTest1.aspx/GetRealTimeData_state_Wise"
INTERSTATE_FLOW_URL = "https://wrldc.in/InterRegionalLinks_Data.aspx/Get_InterRegionalLinks_Region_Wise"


# import schedule dates batches from data/schedule_dates/*
file_path = "data/schedule_dates/"
file_names = os.listdir(file_path)
file_names.sort()
file_names = [file_path + file_name + '.txt' for file_name in file_names]


# get the data from the api
def get_data(url, payload):
    response = requests.post(url, data=payload)
    data = json.loads(response.text)
    return data


if __name__ == "__main__":
    # set up logging
    logging.basicConfig(filename='logs/scrapper.log', filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Starting the scrapper")
    logging.info("Getting the schedule data")

    logging.info("Schedule data fetched")
    logging.info("Getting the demard data")
    # create database connection pool

    # iterate over scheduled dates, fetch value for demand, generation and interlink and store in database
    for file_name in file_names:
        logging.info("Getting the schedule data for file: " + file_name)
        with open(file_name) as f:
            generator_data = []
            demand_data = []
            interlink_data = []
            for date in f.readlines():
                logging.info("Getting the generator data for date: " + date)
                payload = {"date": date}
                # get the generator data
                try:
                    generator_data.append(transform_api_data(
                        get_data(GENERATOR_URL, payload)))
                except:
                    logging.error(
                        "Error in getting the schedule data for date: " + date)
                    # write date to file
                    with open("data/schedule_dates/error_dates.txt", "a") as f:
                        f.write(date + '- generator')
                    continue
                # get the demand data 
                logging.info("Getting the demand data for date: " + date)
                try:
                    demand_data.append(transform_api_data(
                        get_data(DEMAND_URL, payload)))
                except:
                    logging.error(
                        "Error in getting the demand data for date: " + date)
                    # write date to file
                    with open("data/schedule_dates/error_dates.txt", "a") as f:
                        f.write(date + '- demand')
                    continue
                # get the interlink data
                logging.info("Getting the interlink data for date: " + date)
                try:
                    interlink_data.append(transform_api_data(
                        get_data(INTERSTATE_FLOW_URL, payload)))
                except:
                    logging.error(
                        "Error in getting the interlink data for date: " + date)
                    # write date to file
                    with open("data/schedule_dates/error_dates.txt", "a") as f:
                        f.write(date + '- interlink')
                    continue
            
            # load datainto database
            logging.info("Loading the schedule data into database")
            # Database connection details
            # load details from .env file
            host = os.getenv("HOST")
            port = os.getenv("PORT")
            database = os.getenv("DATABASE")
            user = os.getenv("USER")
            password = os.getenv("PASSWORD")

            try:
                # Establish a connection
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password
                )

                # Create a cursor
                cursor = conn.cursor()

                # Inject generator data into database
                for data in generator_data:
                    for row in data:
                        cursor.execute(
                            #   id SERIAL PRIMARY KEY,
                            # state_id INTEGER NOT NULL,
                            # state_name VARCHAR(255) NOT NULL,
                            # dec_capacity FLOAT NOT NULL,
                            # schedule FLOAT NOT NULL,
                            # actual FLOAT NOT NULL,
                            # deviation FLOAT NOT NULL,
                            # last_update TIMESTAMP NOT NULL
                            "INSERT INTO generator_schedule (state_id, state_name, dec_capacity, schedule, actual, deviation, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s)", row)
                # Inject demand data into database
                for data in demand_data:
                    for row in data:
                        cursor.execute(
                            # id SERIAL PRIMARY KEY,
                            # state_id INTEGER NOT NULL,
                            # state_name VARCHAR(255) NOT NULL,
                            # sch_drawal FLOAT NOT NULL,
                            # act_drawal FLOAT NOT NULL,
                            # current_datetime TIMESTAMP NOT NULL,
                            # frequency FLOAT NOT NULL,
                            # deviation FLOAT NOT NULL,
                            # generation FLOAT NOT NULL,
                            # demand FLOAT NOT NULL,
                            # act_data FLOAT NOT NULL,
                            # sch_data FLOAT NOT NULL
                            "INSERT INTO demand (state_id, state_name, sch_drawal, act_drawal, current_datetime, frequency, deviation, generation, demand, act_data, sch_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

                for data in interlink_data:
                    for row in data:
                        cursor.execute(
                            # id SERIAL PRIMARY KEY,
                            # region_id INTEGER NOT NULL,
                            # region_name VARCHAR(255) NOT NULL,
                            # export_ttc FLOAT NOT NULL,
                            # import_ttc FLOAT NOT NULL,
                            # long_term FLOAT NOT NULL,
                            # short_term FLOAT NOT NULL,
                            # px_import FLOAT NOT NULL,
                            # px_export FLOAT NOT NULL,
                            # total FLOAT NOT NULL,
                            # current_loading FLOAT NOT NULL,
                            # last_update TIMESTAMP NOT NULL
                            "INSERT INTO interlink (region_id, region_name, export_ttc, import_ttc, long_term, short_term, px_import, px_export, total, current_loading, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
                # Close the cursor and connection
                cursor.close()
                conn.close()
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(
                    "Error in loading the schedule data into database")
                logging.error(error)
                # write date to file
                with open("data/schedule_dates/error_dates.txt", "a") as f:
                    f.write(date + '- database')
                continue
            logging.info("Schedule data loaded into database")

        logging.info("Data for file: " + file_name + " loaded into database")
    logging.info("Scrapper completed")
        
            