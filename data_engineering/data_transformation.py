# convert data into clean json objects
# input data format is { "d": "[{\"Id\": 197, \"State_Id\": 1, \"State_Name\": \"Korba I\", \"Dec_Capacity\": 1930.0, \"Schedule\": 1930.0, \"Actual\": 1987.0, \"Deviation\": 57.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}, {\"Id\": 198, \"State_Id\": 2, \"State_Name\": \"Korba III\", \"Dec_Capacity\": 468.0, \"Schedule\": 468.0, \"Actual\": 464.0, \"Deviation\": -4.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}, {\"Id\": 199, \"State_Id\": 3, \"State_Name\": \"VSTPS-I\", \"Dec_Capacity\": 940.0, \"Schedule\": 940.0, \"Actual\": 984.0, \"Deviation\": 44.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}, {\"Id\": 200, \"State_Id\": 4, \"State_Name\": \"VSTPS-II\", \"Dec_Capacity\": 942.0, \"Schedule\": 942.0, \"Actual\": 931.0, \"Deviation\": -11.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}]"}
import json


def transform_api_data(data):
    # take input data and convert it into a list of json objects
    data = data["d"]
    data = json.loads(data)
    return data

if __name__ == "__main__":
    generator_data = { "d": "[{\"Id\": 197, \"State_Id\": 1, \"State_Name\": \"Korba I\", \"Dec_Capacity\": 1930.0, \"Schedule\": 1930.0, \"Actual\": 1987.0, \"Deviation\": 57.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}, {\"Id\": 198, \"State_Id\": 2, \"State_Name\": \"Korba III\", \"Dec_Capacity\": 468.0, \"Schedule\": 468.0, \"Actual\": 464.0, \"Deviation\": -4.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}, {\"Id\": 199, \"State_Id\": 3, \"State_Name\": \"VSTPS-I\", \"Dec_Capacity\": 940.0, \"Schedule\": 940.0, \"Actual\": 984.0, \"Deviation\": 44.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}, {\"Id\": 200, \"State_Id\": 4, \"State_Name\": \"VSTPS-II\", \"Dec_Capacity\": 942.0, \"Schedule\": 942.0, \"Actual\": 931.0, \"Deviation\": -11.0, \"lastUpdate\": \"2018-07-02 04: 43: 32\"}]"}
    demand_data = { "d": "[{\"stateid\": 0, \"StateName\": \"Gujrat\", \"Sch_Drawal\": 5513.0, \"Act_Drawal\": 5571.0, \"current_datetime\": \"2018-07-02 02: 41: 55\", \"Frequency\": 49.0, \"Deviation\": 58.0, \"Generation\": 6910.0, \"Demand\": 12482.0, \"Act_Data\": 0.0, \"Sch_Data\": 0.0}, {\"stateid\": 0, \"StateName\": \"Madhya Pradesh\", \"Sch_Drawal\": 4928.0, \"Act_Drawal\": 5346.0, \"current_datetime\": \"2018-07-02 02: 41: 55\", \"Frequency\": 49.0, \"Deviation\": 418.0, \"Generation\": 3722.0, \"Demand\": 9069.0, \"Act_Data\": 0.0, \"Sch_Data\": 0.0}, {\"stateid\": 0, \"StateName\": \"Maharashtra\", \"Sch_Drawal\": 6079.0, \"Act_Drawal\": 6263.0, \"current_datetime\": \"2018-07-02 02: 41: 55\", \"Frequency\": 49.0, \"Deviation\": 184.0, \"Generation\": 12362.0, \"Demand\": 18625.0, \"Act_Data\": 0.0, \"Sch_Data\": 0.0}]"}
    interlink_data = {"d": "[{\"Region_Id\": 1, \"Region_Name\": \"WR-SR\", \"Export_Ttc\": 5700.0, \"Import_Ttc\": 10000.0, \"Long_Term\": -329.0, \"Short_Term\": -1085.0, \"Px_Import\": 0.0, \"Px_Export\": 0.0, \"Total\": -3890.0, \"Current_Loading\": -3235.0, \"lastUpdate\": \"2018-07-02 02: 41: 55\"}]"}
    print(transform_api_data(interlink_data))
