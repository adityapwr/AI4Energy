# convert data into clean json objects
# input data format is { "d": "[{\"Id\": 197, \"State_Id\": 1, \"State_Name\": \"Korba I\", \"Dec_Capacity\": 1930.0, \"Schedule\": 1930.0, \"Actual\": 1987.0, \"Deviation\": 57.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}, {\"Id\": 198, \"State_Id\": 2, \"State_Name\": \"Korba III\", \"Dec_Capacity\": 468.0, \"Schedule\": 468.0, \"Actual\": 464.0, \"Deviation\": -4.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}, {\"Id\": 199, \"State_Id\": 3, \"State_Name\": \"VSTPS-I\", \"Dec_Capacity\": 940.0, \"Schedule\": 940.0, \"Actual\": 984.0, \"Deviation\": 44.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}, {\"Id\": 200, \"State_Id\": 4, \"State_Name\": \"VSTPS-II\", \"Dec_Capacity\": 942.0, \"Schedule\": 942.0, \"Actual\": 931.0, \"Deviation\": -11.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}]"}
from datetime import datetime
import json

GENERATOR_COLUMNS = ["state_id", "state_name", "dec_capacity",
                     "schedule", "actual", "deviation", "last_update"]
DEMAND_COLUMNS = ["state_id", "state_name", "sch_drawal", "act_drawal", "current_datetime",
                  "frequency", "deviation", "generation", "demand", "act_data", "sch_data"]
INTERLINK_COLUMNS = ["region_id", "region_name", "export_ttc", "import_ttc", "long_term",
                     "short_term", "px_import", "px_export", "total", "current_loading", "last_update"]


def convert_keys_to_lowercase(data):
    return {key.lower(): value for key, value in data.items()}


# def transform_api_data(data, columns):
#     converted_data = {
#         key.lower().replace('_', ''): value
#         if key.lower().replace('_', '') != 'lastupdate'
#         else datetime.datetime.strptime(value, '%Y-%m-%d %H: %M: %S')
#         for key, value in json.loads(data["d"])[0].items()
#     }
#     return [converted_data.get(column.lower().replace('_', ''), None) for column in columns]


# def transform_api_data(data, columns):
#     rows = json.loads(data["d"])
#     transformed_data = []
#     for row in rows:
#         converted_data = {
#             key.lower().replace('_', ''): value
#             if key.lower().replace('_', '') != 'currentdatetime' or key.lower().replace('_', '') != 'lastupdate'
#             else datetime.strptime(value, '%Y-%d-%m %H:%M:%S')
#             for key, value in row.items()
#         }
#         transformed_data.append([converted_data.get(
#             column.lower().replace('_', ''), None) for column in columns])
#     return transformed_data

def transform_api_data(data, columns):
    rows = json.loads(data["d"])
    transformed_data = []
    
    for row in rows:
        converted_data = {}
        
        for key, value in row.items():
            key = key.lower().replace('_', '')
            
            if key != 'currentdatetime' and key != 'lastupdate':
                converted_data[key] = value
            else:
                converted_data[key] = datetime.strptime(value, '%Y-%d-%m %H:%M:%S')
        
        transformed_row = [converted_data.get(column.lower().replace('_', ''), None) for column in columns]
        transformed_data.append(transformed_row)
    
    return transformed_data


if __name__ == "__main__":
    generator_data = {"d": "[{\"Id\":197,\"State_Id\":1,\"State_Name\":\"Korba I\",\"Dec_Capacity\":1930.0,\"Schedule\":1930.0,\"Actual\":1987.0,\"Deviation\":57.0,\"lastUpdate\":\"2018-07-02 04:43:32\"},{\"Id\":198,\"State_Id\":2,\"State_Name\":\"Korba III\",\"Dec_Capacity\":468.0,\"Schedule\":468.0,\"Actual\":464.0,\"Deviation\":-4.0,\"lastUpdate\":\"2018-07-02 04:43:32\"},{\"Id\":199,\"State_Id\":3,\"State_Name\":\"VSTPS-I\",\"Dec_Capacity\":940.0,\"Schedule\":940.0,\"Actual\":984.0,\"Deviation\":44.0,\"lastUpdate\":\"2018-07-02 04:43:32\"},{\"Id\":200,\"State_Id\":4,\"State_Name\":\"VSTPS-II\",\"Dec_Capacity\":942.0,\"Schedule\":942.0,\"Actual\":931.0,\"Deviation\":-11.0,\"lastUpdate\":\"2018-07-02 04:43:32\"}]"}
    demand_data = {"d": "[{\"stateid\":0,\"StateName\":\"Gujrat\",\"Sch_Drawal\":3568.0,\"Act_Drawal\":3478.0,\"current_datetime\":\"2021-15-01 12:01:52\",\"Frequency\":50.0,\"Deviation\":-90.0,\"Generation\":6323.0,\"Demand\":9801.0,\"Act_Data\":0.0,\"Sch_Data\":0.0},{\"stateid\":0,\"StateName\":\"Madhya Pradesh\",\"Sch_Drawal\":5017.0,\"Act_Drawal\":5121.0,\"current_datetime\":\"2021-15-01 12:01:52\",\"Frequency\":50.0,\"Deviation\":104.0,\"Generation\":3945.0,\"Demand\":9065.0,\"Act_Data\":0.0,\"Sch_Data\":0.0},{\"stateid\":0,\"StateName\":\"Maharashtra\",\"Sch_Drawal\":5731.0,\"Act_Drawal\":5838.0,\"current_datetime\":\"2021-15-01 12:01:52\",\"Frequency\":50.0,\"Deviation\":107.0,\"Generation\":11442.0,\"Demand\":17279.0,\"Act_Data\":0.0,\"Sch_Data\":0.0},{\"stateid\":0,\"StateName\":\"Chattishgarh\",\"Sch_Drawal\":1610.0,\"Act_Drawal\":1634.0,\"current_datetime\":\"2021-15-01 12:01:52\",\"Frequency\":50.0,\"Deviation\":24.0,\"Generation\":1668.0,\"Demand\":3302.0,\"Act_Data\":0.0,\"Sch_Data\":0.0}]"}
    interlink_data = {"d": "[{\"Region_Id\": 1, \"Region_Name\": \"WR-SR\", \"Export_Ttc\": 5700.0, \"Import_Ttc\": 10000.0, \"Long_Term\": -329.0, \"Short_Term\": -1085.0, \"Px_Import\": 0.0, \"Px_Export\": 0.0, \"Total\": -3890.0, \"Current_Loading\": -3235.0, \"lastUpdate\": \"2018-07-02 02: 41: 55\"}]"}
    print(transform_api_data(generator_data, GENERATOR_COLUMNS))
