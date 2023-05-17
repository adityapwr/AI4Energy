# transfrom csv and inject into postgres

import psycopg2
import logging
import os
import datetime
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CSV_PATH = "data/re-data/Dayahead"

host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")
user = os.getenv("DB_USER")
password = os.getenv("PASSWORD")


# write a program to read all files from the directory and store in a dataframe, apply transformation, and store in the database

# function to read all files from the directory and store in a dataframe

def read_files(directory_path):
    data = pd.DataFrame()
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path, filename)
            # read csv file and apply transformation
            df = pd.read_csv(file_path)
            df = transform_data(df, filename)
            data = data.append(df)
    return data

# function to transform data


def transform_data(df, filename):
    # get date from filename
    date = filename.split('_')[1].split('.')[0]
    # convert date to datetime format
    date = pd.to_datetime(date, format='%d%m%y')

    # function to create datetime object from date and time
    def create_datetime(date, time):
        date = pd.to_datetime(date, format='%m%d%y')
        time = pd.to_datetime(time, format='hr%H%M')
        return datetime.datetime.combine(date, time.time())
    # Transpose the dataframe
    data = df.copy()
    data = data.T
    # reset index
    data.reset_index(inplace=True)

    # set column names
    data.columns = data.iloc[0]
    data.drop(data.index[0], inplace=True)

    # Transform timestamp column to datetime format
    data['timestamp'] = data['timestamp'].apply(
        lambda x: create_datetime(date, x))

    # Melt the dataframe
    data = data.melt(id_vars=['timestamp'],
                     var_name='plant', value_name='value')

    return data


# function to store data in the database
# function to store data in the database
def store_data(data):
    # create psycopg2 connection
    try:
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)
        cursor = connection.cursor()
        # store data in the database
        for index, row in data.iterrows():
            cursor.execute(
                "INSERT INTO renewable (generation_timestamp, plant_pos, plant_value) VALUES (%s, %s, %s)", (row['timestamp'], row['plant'], row['value']))
            connection.commit()
        logging.info("Data stored in the database")
    except (Exception, psycopg2.Error) as error:
        logging.error("Error in storing data in the database")
        logging.error(error)


if __name__ == "__main__":
    logging.basicConfig(filename='logs/re-scrapper.log', filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    logging.info("Starting the re-scrapper")

    # read all files from the directory
    data = read_files(CSV_PATH)
    # store data in the database
    store_data(data)
    logging.info("Ending the re-scrapper")
