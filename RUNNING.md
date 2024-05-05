
Launch Web App:
```
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 app.py
```
Initialize Database:
```
# Initialize weather data tables
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/initialize.py

# Initialize wildfire data table
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/wildfire_initialize.py
```
Scheduled Automation Backend Script (in tmux sessions):
```
# Update table current_weather every hour at :03
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/import_data_c.py

# Update table wildfire every hour at :03
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/wildfire_spark_load.py

# Update tables history_weather, forecast_weather every day at 12:10 AM
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/import_data_hf.py

# Load data from history_weather, forecast_weather every day at 12:15 AM
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/load_data_hf.py
```
Note
* Please run the commands in an environment with spark configured.
* Please run the commands at root directory.
