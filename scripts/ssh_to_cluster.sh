aws emr ssh \
--profile udacity-emr \
--cluster-id j-1S64NLYROIV4M \
--key-pair-file ~/.ssh/spark-cluster.pem

# Then, run:
# sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
