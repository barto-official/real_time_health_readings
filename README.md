This projects aims at creating a real-time system for healh data monitoring using Azure Cloud. The potential application of this architecture is the private IoT (e.g. home-based) system that takes data from wearable devices for further processing. The architecture can be accompanied by more advanced data processing and analytics (e.g machine learning) implemented on top. 

** Goals **
1. Create a template for real-time architecture to stream data using cloud services
2. Build a backbone for further IoT system
3. Minimize latency of sending/receiving/analyzing data
4. Automate the data flow without human interaction

** Architecture **
The Architecture comprises of 

** Components **
1. Data Source:
   - Synthetic Data Generation in Google Collab
2. Data Ingestion
   - D
4. Data Processing:
   - Spark Streaming
   - DuckDB
5. Data Storage
   - MySQL (OLTP): transactional data
   - DuckDB: for fast in-memory analytical purposes
   - Blob Storage: long-term retention
6. Data Visualization

**Pitfalls / Limitations**
1. Current implementation is based on synthetic data generation. Ideally, data should be taken from real sources. Recommendation: Apple Health kit, Huawei Health App. In either of those two, MQTT can be used to stream data (real-time is almost impossible due to the vendor's limitations) to Azure IoT Hub which can route messages to Eventhub.

**Possible Improvements**
- [ ] Connect the system to Apple Health Kit using MQTT messages
- [ ] Connect the system to Huawei Health App
- [ ] Replace Google Collab with local environment or Databricks
- [ ] (Only for Synthetic Data Generation): implement better data generation algorithm to mimic the real-life health data context (time-dependency etc)
- [ ] Replace Python Code with Spark code for importing/exporting data for DuckDB and Azure Blob Storage
- [ ] Implement Redis database for quick lookup of patient data (data enrichment)
- [ ] Implement further security mechanisms for data at rest/in transit
- [ ] Create a Dedicated App for Doctors


