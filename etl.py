from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data to create songs and artists tables"""
    staging_songs = spark.read.json(f"{input_data}/song-data/*/*/*/*.json")
    staging_songs.createOrReplaceTempView("staging_songs")
    
    # Songs table
    songs = spark.sql(
    '''
        select
            song_id,
            title,
            artist_id,
            year,
            duration
        from
            staging_songs
    '''
    )
    songs.write \
         .partitionBy('year', 'artist_id') \
         .mode('overwrite') \
         .parquet(f'{output_data}/songs')

    # Artists table
    artists = spark.sql(
    '''
        select
            artist_id,
            max(artist_name) as name,
            max(artist_location) as location,
            max(artist_latitude) as latitude,
            max(artist_longitude) as longitude
        from
            staging_songs
        group by 1
    '''
    )
    artists.write \
           .mode('overwrite') \
           .parquet(f'{output_data}/artists')


def process_log_data(spark, input_data, output_data):
    """Process events data from logs to create users, songplays and time tables"""
    base_events = spark.read.json(f"{input_data}/log-data/*/*/*.json")
    base_events.createOrReplaceTempView("base_events")

    # Prepare staging table (rename columns)
    staging_events = spark.sql(
    '''
        select
            artist as artist,
            auth as auth,
            firstName as first_name,
            gender as gender,
            itemInSession as item_in_session,
            lastName as last_name,
            length as length,
            level as level,
            location as location,
            method as method,
            page as page,
            registration as registration,
            sessionId as session_id,
            song as song,
            status as status,
            ts as ts,
            userAgent as user_agent,
            userId as user_id
        from
            base_events
    '''
    )
    staging_events.createOrReplaceTempView("staging_events")

    # Users table
    users = spark.sql(
    '''
        with events as
        (
            select * from staging_events
        ),

        add_row_number_and_remove_null_ids as
        (
            select
                *,
                row_number() over (partition by user_id order by ts desc) as row_number_by_user
            from
                events
            where
                user_id is not null
        ),
        
        latest_user_events as
        (
            -- this is done to get latest user event since `level` can change over time
            select
                *
            from
                add_row_number_and_remove_null_ids
            where
                row_number_by_user = 1
        )

        select
            user_id,
            first_name,
            last_name,
            gender,
            level
        from
            latest_user_events
    '''
    )
    users.write \
         .mode('overwrite') \
         .parquet(f'{output_data}/users')
    
    # Songplays table
    songplays = spark.sql(
    '''
        with events as
        (
            select * from staging_events
        ),

        songs as
        (
            select * from staging_songs
        ),

        only_next_song_events as
        (
            select
                *
            from
                events
            where
                page = 'NextSong'
        ),

        events_with_converted_timestamp as
        (
            select
                *,
                from_unixtime(ts / 1000) as start_time
            from
                only_next_song_events
        ),

        joined as
        (
            select
                e.start_time,
                e.user_id,
                e.level,
                s.song_id,
                s.artist_id,
                e.session_id,
                e.location,
                e.user_agent
            from
                events_with_converted_timestamp e
                left outer join songs s on e.song = s.title
        ),

        final as (
            select
                -- create songplay id based on other columns that form a unique combination
                md5(
                    coalesce(cast(user_id as string), '') || coalesce(song_id, '') || coalesce(cast(start_time as string), '')
                ) as songplay_id,
                *
            from
                joined
        )

        select * from final
    '''
    )
    # Register as temp view to be used by time table query
    songplays.createOrReplaceTempView("songplays")
    songplays.write \
             .partitionBy('user_id') \
             .mode('overwrite') \
             .parquet(f'{output_data}/songplays')

    # Time table
    time = spark.sql(
    '''
        select
            start_time,
            extract(hour from start_time) as hour,
            extract(day from start_time) as day,
            extract(week from start_time) as week,
            extract(month from start_time) as month,
            extract(year from start_time) as year,
            dayofweek(start_time) as weekday
        from
            songplays
    '''
    )
    time.write \
        .partitionBy('year', 'month') \
        .mode('overwrite') \
        .parquet(f'{output_data}/time')

    # Drop temp views
    spark.catalog.dropTempView("base_events")
    spark.catalog.dropTempView("staging_events")
    spark.catalog.dropTempView("songplays")
    spark.catalog.dropTempView("staging_songs")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "hdfs:///processed"  # Writing to HDFS before copying to S3 using s3-dist-cp
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
