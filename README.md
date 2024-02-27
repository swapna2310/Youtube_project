**YouTube Data Harvesting and Warehousing project**

**Problem Statement**
     YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable   
     information from YouTube channels. The extracted data is then stored in a MongoDB database and allow users to collect data from up to 10 different channels. Subsequently migrated 
     to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.

Key Technologies and Skills:
    * Python
    * MySQL
    * MongoDB
    * Google Client Library
    * Streamlit

Installation

To run this project, you need to install the following packages:

pip install google-api-python-client
pip install pymongo
pip install pandas
pip install mysql.connector
pip install streamlit    

Approach
1.Start by setting up a Streamlit application using the python library "streamlit", which provides an easy-to-use interface for users to enter a YouTube channel ID, view channel     
  details, and select channels to migrate.
2.Establish a connection to the YouTube API V3, which allows me to retrieve channel and video data by utilizing the Google API client library for Python.
3.Store the retrieved data in a MongoDB data lake, as MongoDB is a suitable choice for handling unstructured and semi-structured data. This is done by firstly writing a method to 
  retrieve the previously called api call and storing the same data in the database in 3 different collections.
4.Transferring the collected data from multiple channels namely the channels,videos and comments to a SQL data warehouse, utilizing a SQL database like MySQL for this 
  purpose.
5.Utilize SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input. For that the SQL table previously made has to be properly 
  given the the foreign and the primary key.
6.The retrieved data is displayed within the Streamlit application, leveraging Streamlit's data visualization capabilities to create charts and graphs for users to analyze the data.
  YouTube-Data-Harvesting-
