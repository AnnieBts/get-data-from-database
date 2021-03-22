import sqlite3
import pandas as pd


class Database:
    def __init__(self):
        pass




    def get_connection_to_db(self):
        try:
            # connect to the database
            cnx = sqlite3.connect('C:\Users\Desktop\database.db')
            return cnx

        except Exception as e:
            print("Error 1 @ Database.get_connection_to_db(), Error establishing connection to database")
            print(e)
            return False

    def get_inverter_settings(self):
        try:
          df = pd.read_sql_query("SELECT * FROM Inverters", self.get_connection_to_db())
          df1 = df[['Serial', 'Name']]
          df1.rename(columns={'Name':'Inverter Name', 'Serial':'Inverter Serial Number'})
          return df1
        except Exception as e:
          print("Error 1 @ Database.get_inverter_settings()")
          print(e)
          return False

    def get_spot_data(self):
        try:
          df2 = pd.read_sql_query("SELECT * FROM SpotData", self.get_connection_to_db())
          df3 = df2.drop(columns =['Serial', 'Frequency', 'OperatingTime', 'FeedInTime', 'BT_Signal', 'Status', 'GridRelay', 'Temperature'])
          df3.rename(columns={'Pdc1':'DC Power 1', 'Pdc2':'DC Power 2', 'Idc1':'DC Amperage 1', 'Idc2':'DC Amperage 2', 'Udc1':'DC Voltage 1', 'Udc2':'DC Voltage 2', 'Pac1':'AC Power 1', 'Pac2':'AC Power 2', 'Pac3':'AC Power 3', 'Iac1':'AC Amperage 1', 'Iac2':'AC Amperage 2', 'Iac3':'AC Amperage 3', 'Uac1':'AC Voltage 1', 'Uac2':'AC Voltage 2', 'Uac3':'AC Voltage 3', 'EToday':'Today Energy', 'ETotal':'Total Energy'}, inplace=True)
          return df3
        except Exception as e:
          print("Error 1 @ Database.get_spot_data()")
          print(e)
          return False

    def get_events(self):
        try:
          df2 = pd.read_sql_query("SELECT * FROM EventData", self.get_connection_to_db())
          df3 = df2.drop(columns =['EventCode', 'Category', 'Tag'])
          df3.rename(columns={'EventCode':'Event Code', 'Category':'Event Category', 'Tag':'Event Tag'}, inplace=True)
          return df3
        except Exception as e:
          print("Error 1 @ Database.get_events()")
          print(e)
          return False


    def connect_telemetry_buffer(self):
        # Connect to the database
        connection = self.get_connection_to_db()

        if not connection:
            return False

        try:
            d1 = self.get_inverter_settings()
            d2 = self.get_buffered_telemetry()
            data = pd.concat([d1, d2], ignore_index=True, sort=False)
            return data

        except Exception as e:
            print("Error 1 @ Database.connect_telemetry_buffer()")
            print(e)
            return False



    def write_json_file(self):
        try:
           df = self.connect_telemetry_buffer()
           with open('temp.json', 'w') as f:
              f.write(df.to_json(orient='records', lines=True))
        except Exception as e:
            print("Error 1 @ Database.write_json_file()")
            print(e)


if __name__ == "__main__":

    db = Database()

    buffer = db.connect_telemetry_buffer()
    print(buffer)
