# udacity-data-lake-s3-spark

## Steps

- load from S3 using spark
- transform using SQL API
- write to hdfs, then use s3-dist-cp to copy to s3
- create script to launch jobs from local onto the cluster

Other todos:

- Add partition by clauses to `write.parquet` commands, as requested in the instructions