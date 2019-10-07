import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ShortType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
       This function is for getting the Spark components
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    
    
    """
       This function loads JSON format songs data from S3 Bucket and extracts the songs and artist tables
        to write it back to S3 parquet files

        The Parameters included are:
            spark       : Spark Session
            input_data  : S3 song_data's location
            output_data : S3 output's location
   """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # Providing manually defined structure to Spark
    schema_sdata = StructType([
        StructField("song_id", StringType(), True),
        StructField("year", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_location", StringType(), True),
         StructField("artist_name", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
    ])
    # read song data file
    df = spark.read.json(song_data,schema=schema_sdata)

    df.createOrReplaceTempView("song_view_table")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
                    SELECT 
                    distinct song_id,
                    artist_id,
                    year,
                    duration,
                    FROM song_view_table
                    WHERE song_id IS NOT NULL
                  """)

    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT 
                                    artist_id, 
                                    artist_name,
                                    artist_location,
                                    artist_latitude,
                                    artist_longitude
                                FROM song_view_table
                                WHERE artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    
    """
       This function loads JSON format songs data from S3 Bucket and extracts the songs and artist tables
        to write it back to S3 parquet files

        The Parameters included are:
            spark       : Spark Session
            input_data  : S3 song_data's location
            output_data : S3 output's location

   """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    
    # Providing manually defined structure to Spark
    schema_ldata = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", ShortType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", ShortType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ])
    # read log data file
    df = spark.read.json(song_data,schema=schema_ldata)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    df.createOrReplaceTempView("log_view_table")
    
    # extract columns for users table    
    users_table = spark.sql("""
                                SELECT 
                                    DISTINCT (log.userId) AS user_id, 
                                    log.firstName AS first_name,
                                    log.lastName AS last_name,
                                    log.gender AS gender,
                                    log.level AS level
                                FROM log_view_table log
                                WHERE log.userId IS NOT NULL
                            """)
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"users")


    # extract columns to create time table
    
    # Refereed from https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
    time_table = spark.sql("""
                    SELECT DISTINCT start_time,
                    EXTRACT(hour From start_time) as hour,
                    EXTRACT(day From start_time) as day,
                    EXTRACT(week From start_time) as week,
                    EXTRACT(month From start_time) as month,
                    EXTRACT(year From start_time) as year,
                    EXTRACT(dayofweek From start_time) as weekday
                    FROM (SELECT
                          to_timestamp(ts/1000.0) as start_time
                          FROM log_view_table
                    )
                    
    """)
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data+"time")

    # read in song data to use for songplays table
    #song_df = 

    # extract columns from joined song and log datasets to create songplays table
    
        # Reference https://stackoverflow.com/questions/48209667/using-monotonically-increasing-id-for-assigning-row-number-to-pyspark-datafram
    songplays_table = spark.sql("""
                                    SELECT 
                                        ROW_NUMBER() OVER(Order by l.ts) AS songplay_id,
                                        to_timestamp(l.ts/1000.0) AS start_time,
                                        l.userId AS user_id,
                                        l.level ,
                                        s.song_id ,
                                        s.artist_id ,
                                        l.sessionId AS session_id,
                                        l.location ,
                                        l.userAgent as user_agent,
                                        month(to_timestamp(l.ts/1000.0)) AS month,
                                        year(to_timestamp(l.ts/1000.0)) AS year
                                    FROM log_view_table l
                                    JOIN song_view_table s
                                        ON l.artist = s.artist_name 
                                        AND 
                                        l.song = s.title
                                """)
    # Reference 2 https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.functions.month
    
    # write songplays table to parquet files partitioned by year and month
     songplays_table.write.partitionBy("year","month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
