aws emr add-steps \
--profile udacity-emr \
--cluster-id j-1S64NLYROIV4M \
--steps Type=spark,Name=SparkifyEtl,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,s3://udacity-data-lake-spark/scripts/etl.py],ActionOnFailure=CONTINUE
