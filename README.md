# YouTube Data Harvesting and Warehousing

## Overview

YouTube Data Harvesting and Warehousing is a project designed to extract valuable information from YouTube channels using the Google API. The extracted data is stored in a MongoDB database and later migrated to a SQL data warehouse for analysis and exploration. The project includes a user-friendly Streamlit application for easy interaction.

## Key Features

- Streamlit application for user interaction
- Google API for YouTube data extraction
- MongoDB for initial data storage
- MySQL for SQL data warehouse
- Data visualization using Streamlit

## Installation

To run this project, install the required packages:

```bash
pip install google-api-python-client pymongo pandas mysql-connector-python streamlit

Getting Started
1.Set up the Streamlit application using the provided Python library.
2.Connect to the YouTube API V3 using the Google API client library.
3.Store extracted data in a MongoDB data lake.
4.Migrate data to a MySQL data warehouse for analysis.
5.Utilize SQL queries for data exploration and user-specific retrievals.
6.Display results using Streamlit's data visualization capabilities.

**Run the Streamlit application:**
streamlit run app.py
Enter YouTube channel IDs, view details, and select channels for migration.

Explore the data within the Streamlit application with visualizations.

**Project Structure**
YouTube-Data-Harvesting-/
│
├── app.py                # Main Streamlit application
├── data_processing.py    # Data processing scripts
├── youtube_api.py        # YouTube API interaction
├── mongo_handler.py      # MongoDB data storage
├── sql_handler.py        # SQL data warehouse migration
└── requirements.txt      # List of required packages
