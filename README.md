# CMPT 732 Project: Weather Analysis for British Columbia

## Introduction

In response to the growing need for comprehensive weather analysis in British Columbia, our project aims to leverage advanced data analytics and visualization techniques to provide a detailed understanding of key environmental factors. Focused on weather, temperature, air pollution, and wildfire data, our project culminates in the development of a web application designed to offer accessible insights to the residents and authorities of British Columbia.

## Contributors

* Chengkun He
* Binming Li
* Dexin Yang

## Repository Structure

```
.
└── cmpt-732-final-project
    ├── assets/                                <- picture assets of web app
    │   ├── 128x128/...
    │   ├── 64x64/...
    │   ├── Logo.png
    │   ├── arrow-down-sign-to-navigate.png
    │   └── dashExtensions_default.js
    ├── config/                                <- main config
    │   └── config.yaml
    ├── src/                                   <- backend utils & scripts for automation
    │   ├── data_fetch.py
    │   ├── data_provider.py
    │   ├── import_data_c.py
    │   ├── import_data_hf.py
    │   ├── initialize.py
    │   ├── load_data_hf.py
    │   ├── wildfire_data_scraping.py
    │   ├── wildfire_initialize.py
    │   └── wildfire_spark_load.py
    ├── data/                                  <- sample data for illustration
    │   ├── prot_current_fire_polys/...
    │   ├── prot_current_fire_polys.zip
    │   ├── forecast_weather.csv
    │   └── history_weather.csv
    ├── pages/                                 <- pages displayed in app
    │   ├── weather_map.py
    │   ├── weather_chart.py
    │   ├── wildfire_graphs.py
    │   ├── wildfire_list.py
    │   └── wildfire_map.py
    ├── app.py                                 <- main app
    ├── requirements.txt                       <- requirements to be installed
    ├── README.md                              <- deployment & launch instruction
    ├── RUNNING.md                             <- introduction to the project
    └── .gitignore
```

## Demo

* http://52.26.1.108:8050/

## Tools/Technologies

* Data Fetch Automation: Python Schedule, tmux
* ETL operations: Apache Spark, Pandas
* Backend Database: Amazon Keyspace (Cassandra)
* Visualization: Plotly, Leaflet, Mapbox
* Deployment: Amazon EC2
* Frontend: Dash by Plotly

## Data source

* [Free Weather API](https://www.weatherapi.com/)
* [British Columbia Government's Open Data Portal](https://catalogue.data.gov.bc.ca/dataset/fire-perimeters-current)
