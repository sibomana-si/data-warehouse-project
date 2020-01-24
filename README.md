# SPARKIFY SONG PLAY ANALYSIS

## Description
This codebase implements a [star schema](https://en.wikipedia.org/wiki/Star_schema) design on an [Amazon Redshift](https://aws.amazon.com/redshift/) database, as well as an [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) pipeline for loading data into the analytics tables.
The goal is to facilitate the Sparkify analytics team's transfer of their analytics processes to the cloud, in light of the growth of their user base and song database.  

Song and Event data will be loaded from files stored on [Amazon S3](https://aws.amazon.com/s3/) into staging tables in the Redshift database, and finally from the staging tables into the analytics (fact and dimension) tables for analysis.


## Database Design and ETL Pipeline
Our design consists of:
- **Staging** tables (staging_songs and staging_events): song and event data from the files stored in S3 will be loaded into these tables.
- **Analytics** tables: one fact table (song_plays), and four dimension tables (users, songs, artists, and time).  


#### STAGING Tables

##### 1. staging_events table

|   **staging_events**      |		
|:-------------------------:|
| artist VARCHAR  	        |
| auth VARCHAR 	            |
| first_name VARCHAR   	    |
| gender CHAR               |
| item_in_session INTEGER   |
| last_name VARCHAR         |
| length DECIMAL            |
| level VARCHAR             |
| location VARCHAR          |
| method VARCHAR            |
| page VARCHAR              |
| registration DECIMAL      |
| session_id INTEGER        |
| song VARCHAR              |
| status INTEGER            |
| ts BIGINT                 |
| user_agent VARCHAR        |
| user_id INTEGER           |


##### 2. staging_songs table

|    **staging_songs**          |		
|:-----------------------------:|
| num_songs INTEGER  	        |
| artist_id VARCHAR 	        |
| artist_latitude DECIMAL   	|
| artist_longitude DECIMAL      |
| artist_location VARCHAR       |
| artist_name VARCHAR           |
| song_id VARCHAR               |
| title VARCHAR                 |
| duration DECIMAL              |
| year INTEGER                  |
        


#### FACT Table

|            **song_plays**                         |
|:-------------------------------------------------:|
| songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY 	|
| start_time BIGINT NOT NULL     	                |
| user_id INTEGER NOT NULL           	            |
| level VARCHAR                     	            |
| song_id VARCHAR NOT NULL          	            |
| artist_id VARCHAR NOT NULL        	            |
| session_id INTEGER NOT NULL        	            |
| location VARCHAR                  	            |
| user_agent VARCHAR                	            |


#### DIMENSION Tables

##### 1. users table

|           **users**           |		
|:-----------------------------:|
| user_id INTEGER PRIMARY KEY  	|
| first_name VARCHAR NOT NULL 	|
| last_name VARCHAR NOT NUL   	|
| gender CHAR              	    |
| level VARCHAR               	|


##### 2. songs table

|           **songs**           |
|:-----------------------------:|
| song_id VARCHAR PRIMARY KEY 	|
| title VARCHAR NOT NULL      	|
| artist_id VARCHAR NOT NULL  	|
| year INTEGER                  |
| duration DECIMAL         	    |


##### 3. artists table

|           **artists**          |
|:------------------------------:|
| artist_id VARCHAR PRIMARY KEY  |
| name VARCHAR NOT NULL          |
| location VARCHAR               |
| latitude DECIMAL           	 |
| longitude DECIMAL          	 |


##### 4. time table

|              **time**             |
|:---------------------------------:|
| start_time BIGINT PRIMARY KEY 	|
| hour INTEGER                      |
| day INTEGER                       |
| week INTEGER                      |
| month INTEGER                     |
| year INTEGER                      |
| weekday INTEGER                   |


#### ETL Pipeline

The ETL pipeline was designed in a modular way and consists of three modules:
- **sql_queries.py:** contains all the sql queries used during the ETL process. This includes the queries for creating/dropping the staging and analytics tables, as well as loading data into the staging tables, and inserting data into the analytics (fact and dimension) tables.
- **create_tables.py:** creates the staging and analytics tables.
- **etl.py:** loads data from S3 files into the staging tables, and then inserts the relevant song and event data from the staging tables into the analytics tables.


##### Usage
1. creates the staging and analytics tables

	`$ python create_tables.py`
	
2. loads the song and log data into the staging tables, and finally inserts the data into the analytics tables

	`$ python etl.py`
	
	