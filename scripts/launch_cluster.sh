aws emr create-cluster \
--name sparkify_cluster \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 3 \
--applications Name=Spark Name=Hive \
--ec2-attributes KeyName=spark-cluster,SubnetId=subnet-37098f4f \
--instance-type m5.xlarge \
--profile udacity-emr
