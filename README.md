# Data Lakes with Spark on AWS

### Summary 

The Data is present in public S3 buckets. The bucket contains data that a startup called Sparkify is collecting. This data contains info about songs, artists and activities done by users (log/events). All of the data is present in JSON formats. Sparkify wants to get more insights from thier users and thier own platform about how thier services are being used.

### Objective
The goal is to build an ETL Pipeline that will extract data from a S3, perform in memory data manipulation and wrangling tasks with Spark and write it back to S3 for Sparify to perform analysis.

### Schema

A **star schema** model has been chosen where :- 

**Fact Table - songplays-**

and **4 dimension tables** :
**Songs** table with data about songs
**Artists** table
**Users** table
**Time** table

#### Project structure

* <i> data </i> - Folder containing two zip files of song and log data for users to see what data they are working with
* <i> etl.py </i> - This is the file that needs to be run by the user. It includes the whole ETL process.
* <i> dl.cfg </i> - Configuration file that contains info about AWS credentials