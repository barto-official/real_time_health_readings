import azure.functions as func
import logging
import mysql.connector
import os
import requests
import json

app = func.FunctionApp()

@app.function_name(name=NAME)
@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name=NAME,
                               connection="My_connection") 
def device_readings(azeventhub: func.EventHubEvent):
    event_data = azeventhub.get_body().decode('utf-8')
    logging.info('Event Hub trigger processed an event: %s', event_data)
    data = json.loads(event_data) 

    # Define MySQL database connection parameters
    config = {
        'host': os.environ['MYSQL_HOST'],
        'user': os.environ['MYSQL_USER'],
        'password': os.environ['MYSQL_PASSWORD'],
        'database': os.environ['MYSQL_DATABASE'],
        'port': int(os.environ.get('MYSQL_PORT', 3306))
    }

    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # Insert data into the MySQL database
        insert_query = """
        INSERT INTO DeviceOperationalData (device_id, timestamp, battery_level, firmware_version, connectivity_status, error_codes)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            data['device_id'],
            data['timestamp'],
            data['battery_level'],
            data['firmware_version'],
            data['connectivity_status'],
            data['error_codes']
        ))
        # Commit the transaction
        conn.commit()
        logging.info("Data inserted successfully into MySQL.")
        
    except mysql.connector.Error as e:
        logging.error(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

@app.function_name(name="glucose_readings")
@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="glucose_monitoring",
                               connection="My_connection_glucose") 

def glucose_readings(azeventhub: func.EventHubEvent):
    # Parse the event data
    event_data = azeventhub.get_body().decode('utf-8')
    logging.info('Event Hub trigger processed an event: %s', event_data)
    data = json.loads(event_data)  # Assuming data is in JSON format

    # Define MySQL database connection parameters
    config = {
        'host': os.environ['MYSQL_HOST'],
        'user': os.environ['MYSQL_USER'],
        'password': os.environ['MYSQL_PASSWORD'],
        'database': os.environ['MYSQL_DATABASE'],
        'port': int(os.environ.get('MYSQL_PORT', 3306))
    }

    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # Insert data into the MySQL database
        insert_query = """
        INSERT INTO GlucoseReadings (patient_id, device_id, glucose_level, timestamp, location)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            data['patient_id'],
            data['device_id'],
            data['glucose_level'],
            data['timestamp'],
            data['location']
        ))

        # Commit the transaction
        conn.commit()
        logging.info("Data inserted successfully into MySQL.")
        
    except mysql.connector.Error as e:
        logging.error(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

@app.function_name(name="alert_bot")
@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="alerts", connection="My_connection") 
def alert_bot(azeventhub: func.EventHubEvent):
    # Parse the event data
    event_data = azeventhub.get_body().decode('utf-8')
    logging.info('Event Hub trigger processed an event: %s', event_data)
    data = json.loads(event_data)  # Assuming data is in JSON format
    
    # Send the custom message to the Telegram channel
    telegram_url = f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/sendMessage"
    telegram_params = {
        "chat_id": os.environ["TELEGRAM_CHANNEL_ID"],
        "text":"Glucose level outside of the normal range for Patient_id: " + str(data['patient_id']) + 
            ";\nglucose_level: " + str(data['glucose_level']) + 
            "\ntimestamp: " + str(data['timestamp']) + 
            ", \nlocation: " + str(data['location']) + 
            "\n\nPlease check the patient's condition."
    }
    response = requests.post(telegram_url, data=telegram_params)
    
    if response.status_code != 200:
        logging.error(f"Failed to send message to Telegram. Response: {response.text}")


    
