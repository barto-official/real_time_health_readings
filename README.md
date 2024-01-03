**Overview**

This projects aims at creating a real-time system for healh data monitoring using Azure Cloud. <be> 

The system works in the following way: patients use a connected device to measure their glucose levels several times per day, as prompted by the device. The reading is then sent to a centralized system over the internet. From there, a data pipeline transforms and analyzes the data to provide alerts for patients, doctors, and any other authorized user or application.

We simulate real-life readings from device with synthetic data generation in Google Collab. 

The potential application of this architecture is the private IoT (e.g. home-based) system that takes data from wearable devices for further processing. The architecture can be accompanied by more advanced data processing and analytics (e.g machine learning) implemented on top.  

---
**Goals**

1. Create a template for real-time architecture to stream data using cloud services
2. Build a backbone for further IoT system
3. Minimize latency of sending/receiving/analyzing data
4. Automate the data flow without human interaction
---
**Data**

Two types of data are added: glucose readings (responsible for pure health data) and device data (device readings). Moreover, static (dimension) data are pre-specified and stored in MySQL. 

To better mimic the real-life data, synthetic data is generated using pre-defiend patient profiles (based on possible glucose levels): 
* Diabetic: Higher readings.
* Athlete: Lower post-exercise readings.
* Party-goer: Variable readings.
* Low-glucose: Dangerously low readings.
* Elderly: Steady/higher readings.

Then using TimeSynth, a library used for generating synthetic time series data, is used to sample a regular time point and creates a sinusoidal signal with added Gaussian noise. This setup is used to simulate the natural fluctuations in glucose levels over time.

![Glucose2](https://github.com/barto-official/real_time_health_readings/assets/125658269/6e1775a3-903f-49d1-b611-cae925c44896)


---
**Architecture** 


![first](https://github.com/barto-official/real_time_health_readings/assets/125658269/c734ed1d-8bfe-49b9-a71c-0482cb8a2015) 

1. Data Source:
   - Synthetic Data Generation in Google Collab
   - Data sent using Spark/Python to Eventhub
2. Data Ingestion
   - Azure Eventhub Namespace with three eventhubs: alerts, glucose_readings, device_readings
4. Data Processing:
   - Fetch data from Eventhub for processing using Spark in Google Collab
   - Spark Streaming that filters data based on pre-defined logic to check for possible breaches that generate alerts
   - Current business logic for generating alerts: Condition: **Condition: (Glucose Reading > 115) OR (Rolling Window Average of 10 minutes > 105)**
   - If either condition is met, send to special Evnethub. From there, Azure Function picks the data and sends to Telegram Channel.
   - Three Azure Functions: two for moving glucose and device data from Eventhub and write to MySQL every time new data is coming, and one for taking filtereed data and posting on Telegram channel (managing Telegram Bot)
  <img width="350" alt="Screenshot 2024-01-03 at 10 42 45" src="https://github.com/barto-official/real_time_health_readings/assets/125658269/8672bf90-60dc-485f-b8df-d3944a1222de">

5. Data Storage
   - MySQL (OLTP): transactional data
   - DuckDB: for fast in-memory analytical purposes
   - Blob Storage: long-term retention
6. Data Visualization
   - Importing data from Mysql to DuckDb for aggregation and analytics
   - Visualization using Streamlit (Dashboard): showing moving average (Window=2 days) for each patient, average glucose readings, and comparison between patients.
  
     
<img width="443" alt="Screenshot 2024-01-02 at 21 09 38" src="https://github.com/barto-official/real_time_health_readings/assets/125658269/3ce43ca2-052f-498d-ad14-253704ecef2e">


---
**Components**

1. Spark 3.4.1
2. Faker — Synthetic Data Generation
3. Matplotlib
4. Streamlit
5. Seaborn
6. Mysql Conenctor
7. Pyspark/Pyspark.sql
8. azure-storage-blob
9. DuckDB
10. azure.eventhub
11. timesynth


---
**Repository**
1. Azure Components are implemented directly in the platform (Eventhub, Blob Storage, Azure Function) so no code is maintained here (except for Azure Functions)
2. Send/Receiving data in streaming mode should ideally be implemented in Spark, but in some cases (due to compatibility errors) pure Python is used. For production, this should be rather avoided.
3. In 'main' folder, you can find code to generate data and send it to eventhub (generate_data notebook) and then separete notebook to read this data and filter for alerts. You can also do it in one file and directly after generating them (spark_eventhub.ipynb notebook). The code serves as a template because it needs to be adjusted to the way you generate/send data to eventhub (schema, flattening through 'explode' function etc.)
4. I show connection to Blob storage mainly for educational purposes, the real application and type of file to be sent will vary.
       
---
**Pitfalls / Limitations**

1. Current implementation is based on synthetic data generation. Ideally, data should be taken from real sources. Recommendation: Apple Health kit, Huawei Health App. In either of those two, MQTT can be used to stream data (real-time is almost impossible due to the vendor's limitations) to Azure IoT Hub which can route messages to Eventhub.
2. Data should be generated as 1 JSON = 1 Reading = 1 Patient to allow for a rolling average in streaming mode.
3. In some cases, Spark Streaming is replaced by Python due to compatibility issues with other libraries/Azure.
---
**Possible Improvements**

- [ ] Connect the system to Apple Health Kit using MQTT messages
- [ ] Connect the system to Huawei Health App
- [ ] Replace Google Collab with local environment or Databricks
- [ ] (Only for Synthetic Data Generation): implement better data generation algorithm to mimic the real-life health data context (time-dependency etc)
- [ ] Replace Python Code with Spark code for importing/exporting data for DuckDB and Azure Blob Storage
- [ ] Implement Redis database for quick lookup of patient data (data enrichment)
- [ ] Implement further security mechanisms for data at rest/in transit
- [ ] Create a Dedicated App for Doctors
- [ ] Implement Dead-Letter Queue
- [ ] Improve Visualization (potentially PowerBI)
- [ ] Implement Jars for working with DuckDB using Spark


