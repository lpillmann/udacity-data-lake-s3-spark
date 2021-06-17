aws emr ssh \
--profile udacity-emr \
--cluster-id j-1S64NLYROIV4M \
--key-pair-file ~/.ssh/spark-cluster.pem \
--command 's3-dist-cp --src hdfs:///processed --dest s3a://udacity-data-lake-spark/processed'
