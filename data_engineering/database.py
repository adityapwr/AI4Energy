import psycopg2
import os
from dotenv import load_dotenv
import time
import datetime

load_dotenv()

# Database connection details
host = os.getenv("HOST")
port = os.getenv("PORT")
database = os.getenv("DATABASE")
user = os.getenv("USER")
password = os.getenv("PASSWORD")

GENERATOR_COLUMNS = ["state_id", "state_name", "dec_capacity",
                     "schedule", "actual", "deviation", "last_update"]
DEMAND_COLUMNS = ["state_id", "state_name", "sch_drawal", "act_drawal", "current_datetime",
                  "frequency", "deviation", "generation", "demand", "act_data", "sch_data"]
INTERLINK_COLUMNS = ["region_id", "region_name", "export_ttc", "import_ttc", "long_term",
                     "short_term", "px_import", "px_export", "total", "current_loading", "last_update"]

def load_data(table_name, column_names, data):
    try:
        with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
            with conn.cursor() as cursor:
                for row in data:
                    cursor.execute(
                        f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))})", row)
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == "__main__":
    demand_data = [[0, 'Gujrat', 5513.0, 5571.0, '2018-07-02 02:41:55', 49.0, 58.0, 6910.0, 12482.0, 0.0, 0.0],
                   [0, 'Madhya Pradesh', 4928.0, 5346.0, '2018-07-02 02:41:55', 49.0, 418.0, 3722.0, 9069.0, 0.0, 0.0]]
    generator_data = [[1, 'Korba I', 1930.0, 1930.0, 1987.0, 57.0, datetime.datetime(2018, 7, 2, 4, 43, 32)], [2, 'Korba III', 468.0, 468.0, 464.0, -4.0, datetime.datetime(2018, 7, 2, 4, 43, 32)], [
        3, 'VSTPS-I', 940.0, 940.0, 984.0, 44.0, datetime.datetime(2018, 7, 2, 4, 43, 32)], [4, 'VSTPS-II', 942.0, 942.0, 931.0, -11.0, datetime.datetime(2018, 7, 2, 4, 43, 32)]]
    load_data("generator", GENERATOR_COLUMNS, generator_data)
