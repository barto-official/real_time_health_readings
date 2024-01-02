import streamlit as st
import pandas as pd
import mysql.connector
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to MySQL database
conn_mysql = mysql.connector.connect(
    host= BD_HOST,
    user= BD_USER,
    port= BD_PORT,
    password= BD_PASSWORD,
    database= BD_DATABASE
)

cursor = conn_mysql.cursor()

# Fetch data from MySQL
cursor.execute("SELECT * FROM Patient")
patients = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

cursor.execute("SELECT * FROM GlucoseReadings")
readings = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

# Close the MySQL connection
conn_mysql.close()

# Convert to DuckDB (assuming you're using Pandas DataFrames)
conn_duckdb = duckdb.connect(database=':memory:')
patients.to_sql('Patient', conn_duckdb, if_exists='replace', index=False)
readings.to_sql('GlucoseReadings', conn_duckdb, if_exists='replace', index=False)

# Aggregate data in DuckDB
query = """
SELECT p.patient_id, p.full_name, COUNT(g.reading_id) AS reading_count, AVG(g.glucose_level) AS average_glucose
FROM Patient p
JOIN GlucoseReadings g ON p.patient_id = g.patient_id
GROUP BY p.patient_id, p.full_name
"""
aggregated_data = conn_duckdb.execute(query).fetchdf()
# Close the DuckDB connection
conn_duckdb.close()

# Set the index of the DataFrame to match the patient_id
aggregated_data.set_index('patient_id', inplace=True)

# Function to calculate moving average
def calculate_moving_average(data, window=1):
    return data.rolling(window=window).mean()

# Function to plot the glucose readings
def plot_glucose_readings(patient_id, patient_name):
    patient_readings = readings[readings['patient_id'] == patient_id]
    patient_readings = patient_readings.sort_values('timestamp')
    patient_readings['moving_average'] = calculate_moving_average(patient_readings['glucose_level'])

    plt.figure(figsize=(10, 5))
    plt.plot(patient_readings['timestamp'], patient_readings['glucose_level'], label='Average Glucose Reading')
    plt.plot(patient_readings['timestamp'], patient_readings['moving_average'], label='Moving Average', linestyle='--')
    plt.title(f'Glucose Readings for {patient_name}')
    plt.xlabel('Date')
    plt.ylabel('Glucose Level')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(plt)

# Function to plot daily average glucose readings for each patient
def plot_daily_average_glucose_per_patient():
    # Group by patient_id and date, then calculate the mean of the glucose_level
    daily_avg_per_patient = readings.copy()
    daily_avg_per_patient['date'] = daily_avg_per_patient['timestamp'].dt.date
    daily_avg_per_patient = daily_avg_per_patient.groupby(['patient_id', 'date'])['glucose_level'].mean().reset_index()

    # Create a mapping from patient_id to full_name
    id_to_name = patients.set_index('patient_id')['full_name'].to_dict()

    # Replace patient_id with full_name in the DataFrame
    daily_avg_per_patient['patient_id'] = daily_avg_per_patient['patient_id'].map(id_to_name)

    # Ensure patients are sorted by their original IDs (if not already sorted)
    daily_avg_per_patient.sort_values('patient_id', inplace=True)

    # Pivot the data to have dates as the index and each patient's averages as columns
    pivot_data = daily_avg_per_patient.pivot(index='date', columns='patient_id', values='glucose_level')

    # Plot using seaborn's lineplot for multi-line plots, with one line per patient
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=pivot_data)
    plt.title('Daily Average Glucose Levels per Patient')
    plt.xlabel('Date')
    plt.ylabel('Average Glucose Level (mg/dL)')
    plt.legend(title='Patient Name', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(plt)

st.title('Patient Glucose Monitoring')

# Display aggregated data with index matching patient ID
# Ensure the DataFrame is sorted by patient_id before displaying
aggregated_data.sort_index(inplace=True)
st.write("Patient Glucose Readings Summary")
st.dataframe(aggregated_data)

# Select a patient to visualize
patient_id = st.selectbox('Select a patient to view details', aggregated_data.index)
selected_patient = aggregated_data.loc[[patient_id]]

if not selected_patient.empty:
    patient_name = selected_patient.iloc[0]['full_name']
    plot_glucose_readings(patient_id, patient_name)

st.write('Daily Average Glucose Readings for Each Patient')
plot_daily_average_glucose_per_patient()