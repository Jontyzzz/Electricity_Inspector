import paho.mqtt.client as mqtt
import json
import mysql.connector
from datetime import datetime
from pytz import timezone

# Define the MQTT broker's address and port
broker_address = "43.225.52.207"
broker_port = 1883
# Replace these with your actual database credentials
db_config = {
    'host': '103.195.185.168',
    'user': 'indiscpx_BLVL',
    'password': 'indiscpx_BLVL@123',
    'database': 'indiscpx_BLVL',
}

# Global variable to track the number of records inserted
records_inserted = 0

# Mapping PLC addresses to column names
plc_address_mapping = {
    40001: 'PT1',
    40003: 'PT2',
    40005: 'PT3',
    40007: 'PT4',
    40009: 'PT5',
    40011: 'CM1',
    40013: 'CM2',
    40015: 'GFM',
    40017: 'OFM',
    40019: 'SFM_1',
    40021: 'SFM_2',
    40023: 'WFM',
    40025: 'DI',
    40027: 'TT1',
    40029: 'TT2',
    40031: 'TT3',
    40033: 'TT4',
    40035: 'TT5',
    40037: 'TT6',
    40039: 'TT7',
    40041: 'TT8',
    40043: 'Boiler_Steam_Quality_Dryness',
    40045: 'Totalizer_GFM',
    40047: 'Totalizer_OFM',
    40049: 'Totalizer_SFM_1',
    40051: 'Totalizer_SFM_2',
    40053: 'Totalizer_WFM',
    40055: 'TWCVFB_S',
    40057: 'TWCVFB_W',
    40059: 'Running_Hour',
    40061: 'Running_Minute'
}

# Connect to the database
def connect_to_database():
    return mysql.connector.connect(**db_config)

def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")

    try:
        data = json.loads(payload)

        if isinstance(data, list):
            for item in data:
                process_json_object(item)
        elif isinstance(data, dict):
            process_json_object(data)
        else:
            print("Error: Received unrecognized JSON structure.")

    except Exception as e:
        print(f"Error processing JSON message: {e}")

def process_json_object(data):
    global records_inserted
    date_str = data.get("Date")

    if date_str:
        date_str = data["Date"]
        sensor_values = data.get("Data", [])
        ip_address = data.get("IP")

        current_datetime = datetime.now()

        # Format the date as "YYYY-MM-DD"
        formatted_date = current_datetime.strftime("%Y-%m-%d")

        # Connect to the MySQL database
        with connect_to_database() as conn, conn.cursor(buffered=True) as cursor:
            try:
                # Process HRN_ONGC_TEST table
                # Check if it's a new day
                select_last_date_query = """
                    SELECT MAX(Date) FROM HRN_ONGC_TEST
                """
                cursor.execute(select_last_date_query)
                last_processed_date = cursor.fetchone()[0]

                if last_processed_date is None or last_processed_date < current_datetime.date():
                    # If it's a new day, reset the PID
                    current_pid = 1
                else:
                    # Get the last PID for the current date
                    select_last_pid_query = """
                        SELECT MAX(PID) FROM HRN_ONGC_TEST WHERE Date = %s
                    """
                    cursor.execute(select_last_pid_query, (formatted_date,))
                    last_pid = cursor.fetchone()[0]
                    current_pid = last_pid + 1 if last_pid is not None else 1

                for plc_key, plc_value in enumerate(sensor_values):
                    plc_address = 40001 + plc_key * 2
                    column_name = plc_address_mapping.get(plc_address)
                    print("plc_value : ",plc_value)

                    if column_name:
                        print(f"Processing PLC Address {plc_address} - Column Name: {column_name}")

                        # Check if the date and PLC address combination is already present in the table
                        select_query = """
                            SELECT PID FROM HRN_ONGC_TEST
                            WHERE `Date` = %s AND `ParameterName` = %s
                        """
                        cursor.execute(select_query, (formatted_date, column_name))
                        existing_pid = cursor.fetchone()

                        if existing_pid:
                            print("Existing PID found:", existing_pid[0])
                            update_query_parameter_colln = """
                                UPDATE HRN_ONGC_TEST
                                SET ParameterValue = %s, UPtime = %s
                                WHERE `Date` = %s AND `ParameterName` = %s
                            """
                            cursor.execute(update_query_parameter_colln, (plc_value, current_datetime, formatted_date, column_name))
                            print("Data successfully updated in HRN_ONGC_TEST table.")
                        else:
                            # If we haven't reached the insertion limit
                            if records_inserted < 31:
                                # Insert the record
                                insert_query_parameter_colln = """
                                    INSERT INTO HRN_ONGC_TEST (UPtime, Date, IP, PID, ParameterName, ParameterValue)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                """
                                cursor.execute(insert_query_parameter_colln, (current_datetime, formatted_date, ip_address, current_pid, column_name, plc_value))
                                print("Data successfully inserted in HRN_ONGC_TEST table.")
                                records_inserted += 1
                                current_pid += 1  # Increment PID for the next record
                            else:
                                # If we've reached the limit, update instead of insert
                                update_query_parameter_colln = """
                                    UPDATE HRN_ONGC_TEST
                                    SET ParameterValue = %s, UPtime = %s
                                    WHERE `Date` = %s AND `ParameterName` = %s
                                """
                                cursor.execute(update_query_parameter_colln, (plc_value, current_datetime, formatted_date, column_name))
                                print("Data successfully updated in HRN_ONGC_TEST table.")

                conn.commit()  # Commit changes after processing the loop
            except Exception as e:
                print("Error :", e)
                plc_data = {}
                for index, value in enumerate(sensor_values):
                    plc_address = 40001 + index * 2
                    column_name = plc_address_mapping.get(plc_address)
                    if column_name:
                        plc_data[column_name] = value

                if plc_data:
                    query_ongc_iot = f"""
                        INSERT INTO ONGC_IOT_INSERT ( ConDate, IP, {', '.join(plc_data.keys())})
                        VALUES (%s, %s, {', '.join(['%s'] * len(plc_data))})
                    """
                    values_ongc_iot = (current_datetime, ip_address, *plc_data.values())
                    cursor.execute(query_ongc_iot, values_ongc_iot)
                    conn.commit()
                    print("Data successfully inserted into ONGC_IOT_INSERT table.")

            print("Data successfully committed to the database.")



# Create an MQTT client
client = mqtt.Client()

# Set the callback function for message reception
client.on_message = on_message

# Connect to the MQTT broker
client.connect(broker_address, broker_port)

# Subscribe to the "Parameters" topic
client.subscribe("Parameters")

# Connect to the database
conn = connect_to_database()

try:
    print("Program started")
    client.loop_forever()
finally:
    conn.close()
    print("Done")