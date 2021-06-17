aws emr ssh \
--profile udacity-emr \
--cluster-id j-2E1F23U8OV9FD \
--key-pair-file ~/.ssh/spark-cluster.pem \
--command 's3-dist-cp --src hdfs:///processed --dest s3a://udacity-data-lake-spark/processed'
