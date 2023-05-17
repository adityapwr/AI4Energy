-- # generator data looks like {'Id': 197, 'State_Id': 1, 'State_Name': 'Korba I', 'Dec_Capacity': 1930.0, 'Schedule': 1930.0, 'Actual': 1987.0, 'Deviation': 57.0, 'lastUpdate': '2018-07-02 04: 43: 32'}
-- # demand data looks like {'stateid': 0, 'StateName': 'Gujrat', 'Sch_Drawal': 5513.0, 'Act_Drawal': 5571.0, 'current_datetime': '2018-07-02 02: 41: 55', 'Frequency': 49.0, 'Deviation': 58.0, 'Generation': 6910.0, 'Demand': 12482.0, 'Act_Data': 0.0, 'Sch_Data': 0.0}
-- # interlink data looks like {'Region_Id': 1, 'Region_Name': 'WR-SR', 'Export_Ttc': 5700.0, 'Import_Ttc': 10000.0, 'Long_Term': -329.0, 'Short_Term': -1085.0, 'Px_Import': 0.0, 'Px_Export': 0.0, 'Total': -3890.0, 'Current_Loading': -3235.0, 'lastUpdate': '2018-07-02 02: 41: 55'}

-- # create tables on postgressql database for generator, demand and interlink data

-- # generator table
CREATE TABLE generator (
    id SERIAL PRIMARY KEY,
    state_id INTEGER NOT NULL,
    state_name VARCHAR(255) NOT NULL,
    dec_capacity FLOAT NOT NULL,
    schedule FLOAT NOT NULL,
    actual FLOAT NOT NULL,
    deviation FLOAT NOT NULL,
    last_update TIMESTAMP NOT NULL
);

-- # demand table
CREATE TABLE demand (
    id SERIAL PRIMARY KEY,
    state_id INTEGER NOT NULL,
    state_name VARCHAR(255) NOT NULL,
    sch_drawal FLOAT NOT NULL,
    act_drawal FLOAT NOT NULL,
    current_datetime TIMESTAMP NOT NULL,
    frequency FLOAT NOT NULL,
    deviation FLOAT NOT NULL,
    generation FLOAT NOT NULL,
    demand FLOAT NOT NULL,
    act_data FLOAT NOT NULL,
    sch_data FLOAT NOT NULL
);

-- # interlink table
CREATE TABLE interlink (
    id SERIAL PRIMARY KEY,
    region_id INTEGER NOT NULL,
    region_name VARCHAR(255) NOT NULL,
    export_ttc FLOAT NOT NULL,
    import_ttc FLOAT NOT NULL,
    long_term FLOAT NOT NULL,
    short_term FLOAT NOT NULL,
    px_import FLOAT NOT NULL,
    px_export FLOAT NOT NULL,
    total FLOAT NOT NULL,
    current_loading FLOAT NOT NULL,
    last_update TIMESTAMP NOT NULL
);

-- # renewable table
CREATE TABLE renewable (
    id SERIAL PRIMARY KEY,
    generation_timestamp TIMESTAMP NOT NULL,
    plant_pos VARCHAR(255) NOT NULL,
    plant_value FLOAT NOT NULL
);

CREATE TABLE aggregated_demand (
  agg_timestamp TIMESTAMP,
  state_name VARCHAR(255),
  average_freq FLOAT,
  average_demand FLOAT,
  PRIMARY KEY (agg_timestamp, state_name)
);


CREATE TABLE aggregated_generator (
  agg_timestamp TIMESTAMP,
  state_name VARCHAR,
  average_actual NUMERIC,
  average_schedule NUMERIC,
  average_dec_capacity NUMERIC
);

CREATE TABLE aggregated_interlink (
  agg_timestamp TIMESTAMP,
  region_name VARCHAR(255) NOT NULL,
  average_export_ttc FLOAT NOT NULL,
  average_import_ttc FLOAT NOT NULL,
  average_long_term FLOAT NOT NULL,
  average_short_term FLOAT NOT NULL,
  average_px_import FLOAT NOT NULL,
  average_px_export FLOAT NOT NULL,
  average_total FLOAT NOT NULL,
  average_current_loading FLOAT NOT NULL,
  PRIMARY KEY (agg_timestamp, region_name)
);
