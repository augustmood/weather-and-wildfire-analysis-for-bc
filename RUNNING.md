
Launch Web App:
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 webapp/app.py
```
Initialize Database:
```
# Initialize weather data tables
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/initialize.py

# Initialize wildfire data table
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/wildfire_initialize.py
```
Scheduled Automation Backend Script:
```
# Update Database every hour at :03
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/import_data_c.py

# Update Database every hour at :03
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/wildfire_spark_load.py

# Update Database every day at 12:13 AM
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/import_data_hf.py

# Update Database every day at 12:13 AM
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/load_data_hf.py
```

