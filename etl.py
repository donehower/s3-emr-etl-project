from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def create_spark_session():
    """
    Creates spark session.

    :returns: Spark session object.
    """
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads song data from S3 and writes results back to S3 as parquet files.
    :param spark: a sparkSession object
    :param input_data: S3 bucket/directory for song data json files
    :param output_data: S3 bucket where result parquet files are written

    :returns: Nothing
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    # write songs table to parquet files partitioned by year and artist
    songs_table = df.select('song_id',
                            'title',
                            'artist_id',
                            'year',
                            'duration').distinct()

    songs_table\
        .write\
        .partitionBy('year', 'artist_id')\
        .format("parquet")\
        .save(output_data + "song_table.parquet")

    # extract columns to create artists table
    # write artists table to parquet files
    artist_table = df.select('artist_id',
                             'artist_name',
                             'artist_location',
                             'artist_latitude',
                             'artist_longitude').distinct()

    artist_table\
        .write\
        .format('parquet')\
        .save(output_data + "artist_table.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Reads log data from S3 and writes results back to S3 as parquet files.
    :param spark: a sparkSession object
    :param input_data: S3 bucket/directory for song data json files
    :param output_data: S3 bucket where result parquet files are written

    :returns: Nothing
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file and filter by actions for songplays
    df = spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    # write users table to parquet files
    window = Window\
        .partitionBy('userId')\
        .orderBy(df['ts'].desc())

    users_table = df.withColumn('order_users', F.rank().over(window))\
        .filter('order_users=1')\
        .select('userId', 'firstName', 'lastName', 'gender', 'level')

    users_table\
        .write\
        .format('parquet')\
        .save(output_data + "users_table.parquet")

    # create timestamp column from original timestamp column
    # extract columns to create time table
    # write time table to parquet files partitioned by year and month
    time_table = df.select('ts').distinct()
    time_table = time_table.withColumn('start_time',
                                        F.from_unixtime(time_table['ts']/1000))\
        .withColumn('year', F.year('start_time'))\
        .withColumn('month', F.month('start_time'))\
        .withColumn('day_of_month', F.dayofmonth('start_time'))\
        .withColumn('day_of_week', F.dayofweek('start_time'))\
        .withColumn('week', F.weekofyear('start_time'))\
        .withColumn('hour', F.hour('start_time'))

    time_table\
        .write\
        .partitionBy('year', 'month')\
        .format("parquet")\
        .save(output_data + "time_table.parquet")

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    songs_df = spark.read.json(song_data)

    # join song and log datasets to create songplays table and extract columns
    # write songplays table to parquet files partitioned by year and month
    joinExpression = [df.song == songs_df.title,
                      df.length == songs_df.duration,
                      df.artist == songs_df.artist_name]
    joinType = "inner"
    songplays_table = df.join(songs_df, joinExpression, joinType)\
        .select('ts',
                'userId',
                'level',
                'song_id',
                'artist_id',
                'sessionId',
                'location',
                'userAgent')
    songplays_table = songplays_table.withColumn('year', F.year(F.to_timestamp(time_table['ts']/1000)))\
        .withColumn('month', F.month(F.to_timestamp(time_table['ts']/1000)))\
        .withColumn('songplay_id', F.monotonically_increasing_id())

    songplays_table\
        .write\
        .partitionBy('year', 'month')\
        .format('parquet')\
        .save(output_data + 'songplays_table.parquet')


def main():
    spark = create_spark_session()
    input_data = 's3://udacity-dend/'
    output_data = 's3://data-lake-skd/app-data/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
