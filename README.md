# udacity-data-lake-s3-spark

## Steps

- load from S3 using Spark
- transform using SQL API
- write processed data to HDFS
- use s3-dist-cp to copy from HDFS to S3

TODOs:

- add partition by clauses to `write.parquet` commands, as requested in the instructions
- move SQL queries to a new file, similar to previous projects (`sql_queries.py`)
- move commands from notebook to `etl.py`
- create script to submit job to cluster
- rubric check